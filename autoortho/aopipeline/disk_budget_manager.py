"""
disk_budget_manager.py - Unified disk space management for AutoOrtho

Provides centralized disk accounting and eviction across cache types:
- DDS cache (.dds + .ddm) - compiled textures
- JPEGs (.jpg) - source tile images

Budget enforcement is soft: writes are never blocked. Instead, when a
category exceeds its allocation, background eviction reclaims space by
deleting the least-recently-accessed entries.
"""

import json
import logging
import os
import subprocess
import threading
import time
from typing import Optional

log = logging.getLogger(__name__)


class DiskUsageReport:
    """Snapshot of disk usage across all cache categories."""
    __slots__ = ("dds_bytes", "jpeg_bytes",
                 "total_bytes", "budget_bytes", "scan_time_ms")

    def __init__(self):
        self.dds_bytes = 0
        self.jpeg_bytes = 0
        self.total_bytes = 0
        self.budget_bytes = 0
        self.scan_time_ms = 0.0

    def __repr__(self):
        return (f"DiskUsage(dds={self.dds_bytes/(1024**2):.0f}MB, "
                f"jpeg={self.jpeg_bytes/(1024**2):.0f}MB, "
                f"total={self.total_bytes/(1024**2):.0f}MB / "
                f"{self.budget_bytes/(1024**2):.0f}MB)")


class DiskBudgetManager:
    """
    Unified disk space management for AutoOrtho caches.
    
    Tracks disk usage across DDS cache and JPEG files.
    Enforces per-category budgets through LRU eviction.
    
    Budget allocation (configurable, % of ``total_budget_mb``):
    - DDS cache: 80% (primary persistent storage)
    - JPEGs: 20% (source images retained until DDS is complete)
    
    Thread Safety:
        All public methods are thread-safe. Eviction runs in background
        threads to avoid blocking callers.
    """

    def __init__(self, cache_dir: str, total_budget_mb: int,
                 dds_budget_pct: int = 80,
                 jpeg_budget_pct: int = 20,
                 dds_cache=None):
        """
        Args:
            cache_dir: Base cache directory.
            total_budget_mb: Total disk budget in MB across all categories.
            dds_budget_pct: Percentage allocated to DDS cache (10-90).
            jpeg_budget_pct: Percentage allocated to JPEGs (5-50).
            dds_cache: Optional DynamicDDSCache instance for DDS eviction.
        """
        self._cache_dir = cache_dir
        self._total_budget = total_budget_mb * 1024 * 1024  # bytes

        # Clamp percentages to valid ranges
        dds_budget_pct = max(10, min(90, dds_budget_pct))
        jpeg_budget_pct = max(5, min(50, jpeg_budget_pct))

        # Normalize percentages to sum to 100
        total_pct = dds_budget_pct + jpeg_budget_pct
        self._dds_budget = int(self._total_budget * dds_budget_pct / total_pct)
        self._jpeg_budget = int(self._total_budget * jpeg_budget_pct / total_pct)

        # Current usage tracking (updated by scan and accounting calls)
        self._dds_usage = 0
        self._jpeg_usage = 0

        self._dds_cache = dds_cache  # Reference to DynamicDDSCache for eviction

        self._lock = threading.Lock()
        self._scan_complete = threading.Event()
        self._last_scan_time = 0.0
        self._eviction_in_progress = False

        self._state_file = os.path.join(cache_dir, '.disk_usage.json')
        self._jpeg_usage = self._load_jpeg_state()
        self._last_state_save = 0.0

        log.info(f"DiskBudgetManager initialized: total={total_budget_mb}MB "
                 f"(DDS={self._dds_budget/(1024**2):.0f}MB, "
                 f"JPEGs={self._jpeg_budget/(1024**2):.0f}MB)")

    # ------------------------------------------------------------------
    # Accounting (called after writes)
    # ------------------------------------------------------------------

    def account_dds(self, size_bytes: int) -> None:
        """Account for a DDS cache write.

        Args:
            size_bytes: Size of the DDS file written (positive for add,
                        negative for removal).
        """
        with self._lock:
            self._dds_usage += size_bytes
            self._dds_usage = max(0, self._dds_usage)

        if self._dds_usage > self._dds_budget:
            self._schedule_eviction("dds")

    def account_jpeg(self, size_bytes: int) -> None:
        """Account for a JPEG cache write or deletion.

        Args:
            size_bytes: Bytes added (positive) or removed (negative).
        """
        save_value = None
        with self._lock:
            self._jpeg_usage += size_bytes
            self._jpeg_usage = max(0, self._jpeg_usage)
            now = time.time()
            if now - self._last_state_save >= 60:
                self._last_state_save = now
                save_value = self._jpeg_usage
        if save_value is not None:
            self._save_jpeg_state(save_value)

    # ------------------------------------------------------------------
    # Eviction
    # ------------------------------------------------------------------

    def check_and_evict(self) -> None:
        """
        Check all categories and evict if over budget.
        
        Called periodically (e.g., from TileCacher.clean loop) and
        after accounting calls when a budget is exceeded.
        """
        # DDS eviction
        if self._dds_usage > self._dds_budget and self._dds_cache is not None:
            excess = self._dds_usage - int(self._dds_budget * 0.9)
            if excess > 0:
                freed = self._dds_cache.evict_lru(excess)
                with self._lock:
                    self._dds_usage -= freed

    def _schedule_eviction(self, category: str) -> None:
        """Schedule a background eviction check for the given category."""
        with self._lock:
            if self._eviction_in_progress:
                return
            self._eviction_in_progress = True

        def _run():
            try:
                self.check_and_evict()
            finally:
                with self._lock:
                    self._eviction_in_progress = False

        t = threading.Thread(target=_run, daemon=True, name=f"disk_evict_{category}")
        t.start()

    # ------------------------------------------------------------------
    # Disk scanning
    # ------------------------------------------------------------------

    def scan_disk_usage(self) -> DiskUsageReport:
        """
        Scan the cache directory tree and compute actual disk usage.

        Uses 'du' for fast kernel-level traversal (~1s vs 600s for 4M+ files).
        Falls back to os.scandir if du is unavailable.

        This is I/O intensive and should be called from a background thread.

        Returns:
            DiskUsageReport with per-category byte counts.
        """
        report = DiskUsageReport()
        report.budget_bytes = self._total_budget
        start = time.monotonic()

        try:
            dds_dir = os.path.join(self._cache_dir, "dds_cache")
            dds_bytes = self._du_size(dds_dir) if os.path.isdir(dds_dir) else 0
            report.dds_bytes = dds_bytes

            # Use persisted JPEG count when available — scanning 4.7M files
            # with du takes ~8 minutes on macOS vs ~2s for dds_cache alone.
            with self._lock:
                saved_jpeg = self._jpeg_usage
            if saved_jpeg > 0:
                report.jpeg_bytes = saved_jpeg
            else:
                total_bytes = self._du_size(self._cache_dir)
                report.jpeg_bytes = max(0, total_bytes - dds_bytes)

        except Exception as e:
            log.warning(f"Disk usage scan error: {e}")

        report.total_bytes = report.dds_bytes + report.jpeg_bytes
        report.scan_time_ms = (time.monotonic() - start) * 1000

        # Update tracked usage
        with self._lock:
            self._dds_usage = report.dds_bytes
            self._jpeg_usage = report.jpeg_bytes
            self._last_scan_time = time.time()

        self._scan_complete.set()
        self._save_jpeg_state(report.jpeg_bytes)

        log.info(f"Disk scan complete in {report.scan_time_ms:.0f}ms: {report}")
        return report

    def initial_scan(self) -> None:
        """
        Run initial disk scan and cleanup. Intended for background thread at startup.
        
        Performs:
        1. Full disk usage scan
        2. Budget enforcement (eviction if needed)
        """
        try:
            self.scan_disk_usage()
            self.check_and_evict()
        except Exception as e:
            log.warning(f"Initial disk scan error: {e}")
        finally:
            self._scan_complete.set()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def usage_report(self) -> dict:
        """Return current usage statistics."""
        with self._lock:
            return {
                "dds_usage_mb": self._dds_usage / (1024 ** 2),
                "dds_budget_mb": self._dds_budget / (1024 ** 2),
                "jpeg_usage_mb": self._jpeg_usage / (1024 ** 2),
                "jpeg_budget_mb": self._jpeg_budget / (1024 ** 2),
                "total_usage_mb": (self._dds_usage + self._jpeg_usage) / (1024 ** 2),
                "total_budget_mb": self._total_budget / (1024 ** 2),
                "last_scan": self._last_scan_time,
            }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_jpeg_state(self) -> int:
        """Load persisted JPEG usage from state file. Returns 0 if absent."""
        try:
            with open(self._state_file) as f:
                data = json.load(f)
            return int(data.get("jpeg_bytes", 0))
        except Exception:
            return 0

    def _save_jpeg_state(self, jpeg_bytes: int) -> None:
        """Persist JPEG usage to state file (best-effort)."""
        try:
            tmp = self._state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"jpeg_bytes": jpeg_bytes}, f)
            os.replace(tmp, self._state_file)
        except Exception as e:
            log.debug(f"Could not save disk usage state: {e}")

    @staticmethod
    def _du_size(path: str) -> int:
        """Return total byte size of a directory using 'du'.

        'du' uses kernel-level directory traversal and is orders of magnitude
        faster than Python's os.walk for large caches (4M+ files: ~1s vs ~600s).
        Falls back to os.scandir if du is unavailable or fails.
        """
        try:
            # -s: summarize, -k: output in 1K blocks (portable across macOS/Linux)
            result = subprocess.check_output(
                ['du', '-sk', path],
                timeout=120,
                stderr=subprocess.DEVNULL,
            )
            kb = int(result.split()[0])
            return kb * 1024
        except Exception:
            return DiskBudgetManager._scandir_size(path)

    @staticmethod
    def _scandir_size(root: str) -> int:
        """Fallback: sum all file sizes under root using os.scandir."""
        total = 0
        stack = [root]
        while stack:
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for entry in it:
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                stack.append(entry.path)
                            elif entry.is_file(follow_symlinks=False):
                                total += entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            pass
            except OSError:
                pass
        return total

    @staticmethod
    def _safe_remove(path: str) -> None:
        """Remove a file, ignoring errors."""
        try:
            os.remove(path)
        except OSError:
            pass
