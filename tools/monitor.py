#!/usr/bin/env python3
"""Live health monitor for AutoOrtho.

Tails ~/.autoortho-data/logs/autoortho.log and renders a real-time TUI
showing FUSE read performance, instrumentation firings, and health status.

Usage:
    python3 monitor.py [--log PATH] [--window-sec 300]
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


DEFAULT_LOG = Path.home() / ".autoortho-data" / "logs" / "autoortho.log"
LOG_TS_FMT = "%Y-%m-%d %H:%M:%S,%f"


# ─────────────────────────── parsers ───────────────────────────

_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

_FUSE_PERF_RE = re.compile(
    r"FUSE_PERF_SUMMARY window=(\d+)s reads=(\d+) "
    r"slow_16ms=(\d+) slow_50ms=(\d+) slow_100ms=(\d+) slow_500ms=(\d+) "
    r"avg_ms=([\d.]+) max_ms=([\d.]+) by_class=\[([^\]]*)\]"
)

_READ_SLOW_RE = re.compile(
    r"READ_DDS_BYTES VERY_SLOW total_ms=(\d+) branch=(\S+) mm_idx=\d+ "
    r"fetch_ms=[\d.]+ seek_ms=[\d.]+ tile=(\S+) "
    r"mm_retrieved=(\d+)/(\d+)"
)

_BUILD_STUCK_RE = re.compile(
    r"PIPELINE_TRACE build_stuck pid=(\d+) tile=(\S+) thread=\d+ "
    r"age_s=([\d.]+) stage='([^']+)' native_inflight=\[([^\]]*)\]"
)

_FINALIZE_FILE_SLOW_RE = re.compile(
    r"PIPELINE_TRACE finalize_to_file SLOW pid=(\d+) tile=(\S+) "
    r"elapsed_ms=(\d+) bytes_written=(\d+)"
)

_FINALIZE_BUFFER_SLOW_RE = re.compile(
    r"PIPELINE_TRACE finalize_to_buffer SLOW pid=(\d+) tile=(\S+) "
    r"elapsed_ms=(\d+)"
)

_FINALIZE_PROGRESS_RE = re.compile(
    r"PIPELINE_TRACE finalize_progress pid=(\d+) tile=(\S+) "
    r"elapsed=([\d.]+)s staging_size=(-?\d+) growth_since_last=(-?\d+)"
)

_STORE_FILE_SLOW_RE = re.compile(
    r"PIPELINE_TRACE store_from_file SLOW pid=(\d+) tile=(\S+) "
    r"elapsed_ms=(\d+) bytes=(\d+)"
)

_DDS_CACHE_LOAD_SLOW_RE = re.compile(
    r"DDS_CACHE_LOAD VERY_SLOW total_ms=(\d+) read_ms=([\d.]+) "
    r"decompress_ms=([\d.]+) raw_bytes=(\d+) dds_bytes=(\d+) "
    r"compressed=(\S+) tile=(\S+)"
)

_NATIVE_BUILD_SLOW_RE = re.compile(
    r"NATIVE_BUILD_EXIT slow duration_ms=([\d.]+)"
)

_EVICTION_RE = re.compile(
    r"Eviction futile_streak=\d+:.*cur=(\d+)MB limit=([\d.]+)MB"
)

_TC_LOCK_WAIT_RE = re.compile(r"tc_lock_wait .* wait_ms=([\d.]+)")

_DELETED_BAD_TILE_RE = re.compile(
    r"DDS passthrough: deleted bad tile (\S+) — (.+)"
)

_MOUNT_RE = re.compile(r"mount_worker - INFO - MOUNT: (.+)$")
_FAILURES_RE = re.compile(r"FAILURES DETECTED")
_FORCE_EXIT_RE = re.compile(r"Force exiting")
_AO_VERSION_RE = re.compile(r"AutoOrtho version: ref: (.+)$")
_TERRAIN_READY_RE = re.compile(r"TerrainTileLookup: Ready for (\S+)")

# Pool health — see autoortho/aopipeline/aodecode.c.  Each STATS log line is a
# Python dict printed by one worker process containing per-PID pool counters.
# A monotonically-rising overflow_mb that never drops is the signature of a
# counter leak; sustained high waiters per PID is the lead indicator of a
# pool-exhaustion deadlock (build_stuck appears 1-2 min later).
_STATS_LINE_RE = re.compile(r"STATS:")
_POOL_OVERFLOW_MB_RE = re.compile(r"'decode_pool_overflow_mb'\s*:\s*(\d+)")
_POOL_WAITERS_RE = re.compile(r"'decode_pool_waiters:(\d+)'\s*:\s*(\d+)")
_POOL_INIT_RE = re.compile(
    r"Global decode pool initialized: \d+ fixed buffers, (\d+) MB limit"
)

# Network / CDN health — upstream signal for build degradation.  Fallback
# chunks driven by these failures are the typical trigger for pool-exhaustion
# under the previous leak; even with the leak fixed, sustained CDN failures
# produce missing_color tiles, slow reads, and green terrain.
_NETWORK_ERROR_RATE_RE = re.compile(
    r"Very high network error rate detected\s*:\s*([\d.]+)\s*%"
)
_CHUNK_HTTP_FAIL_RE = re.compile(
    r"Failed with status (\d+) to get chunk Chunk\(([^)]+)\)"
)
_CHUNK_RESUBMIT_RE = re.compile(r"Failed getting: Chunk\([^)]+\).*re-submit")
_CHUNK_INVALID_JPEG_RE = re.compile(r"chunk_invalid_jpeg_sample")

# Build pipeline pressure — coordinator_heartbeat is emitted by every worker
# every few seconds with its current queue depth, active build slots, and
# completed/failed counts.  Visible backpressure before build_stuck fires.
_COORDINATOR_RE = re.compile(
    r"coordinator_heartbeat pid=(\d+) queue_size=(\d+) "
    r"active_builds=(\d+)/(\d+) completed=(\d+)"
)

# Live/BG builder inflight counters — embedded in STATS log lines as
# 'native_inflight:streaming_builder_held_{live,bg}:<pid>': N.  Live builds
# are FUSE-driven and bypass the coordinator queue entirely; BG builds are
# the predictive ones tracked by coordinator_heartbeat.  Counting both gives
# total real-time builder pressure.
_INFLIGHT_BUILDERS_RE = re.compile(
    r"'native_inflight:streaming_builder_held_(live|bg):(\d+)'\s*:\s*(\d+)"
)

# Extract per-class read counts from FUSE_PERF by_class strings like
# "disk_passthrough=1672(0slow,0.0ms_avg),tile=178(30slow,9.3ms_avg)"
_BY_CLASS_KV_RE = re.compile(r"(\w+)=(\d+)")


def parse_ts(line: str) -> Optional[datetime]:
    m = _TS_RE.match(line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), LOG_TS_FMT)
    except ValueError:
        return None


# ─────────────────────────── state ───────────────────────────

@dataclass
class FusePerfSample:
    ts: datetime
    reads: int
    slow_16ms: int
    slow_50ms: int
    slow_100ms: int
    slow_500ms: int
    avg_ms: float
    max_ms: float
    by_class: str


@dataclass
class Event:
    ts: datetime
    level: str  # info | warn | crit
    category: str
    text: str


@dataclass
class MonitorState:
    window_sec: int
    log_path: Path
    fuse_samples: deque = field(default_factory=lambda: deque(maxlen=120))
    events: deque = field(default_factory=lambda: deque(maxlen=200))
    counters: dict = field(default_factory=lambda: {
        "read_slow": 0,
        "build_stuck": 0,
        "finalize_file_slow": 0,
        "finalize_buffer_slow": 0,
        "store_file_slow": 0,
        "dds_cache_load_slow": 0,
        "native_build_slow": 0,
        "tc_lock_wait": 0,
        "deleted_bad_tile": 0,
        "force_exits": 0,
        "failures_detected": 0,
        "finalize_progress_polls": 0,
        "finalize_progress_no_growth": 0,
        "pool_high_waiters_crit": 0,
        "pool_leak_crit": 0,
        "pool_sustained_high_crit": 0,
        "chunk_http_fail_5xx": 0,
        "chunk_http_fail_4xx": 0,
        "chunk_resubmit": 0,
        "chunk_invalid_jpeg": 0,
        "network_high_error_crit": 0,
    })
    # Per-(pid, tile) tracking for finalize_progress: how many consecutive
    # zero-growth polls we've seen.  Cleared when finalize ends (we don't get
    # a direct end signal, so we only ever grow this dict; periodic trim
    # would be nice but isn't urgent for v1).
    finalize_zero_growth_streak: dict = field(default_factory=dict)
    last_rss_mb: Optional[int] = None
    rss_limit_mb: Optional[float] = None
    last_max_read_ms: float = 0.0
    last_max_read_tile: str = ""
    ao_version: str = "?"
    mounts: list = field(default_factory=list)
    terrain_ready: list = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    # Decode pool health — see _STATS_LINE_RE comment above.  overflow_samples
    # is a rolling window of the most recent overflow_mb readings used to
    # detect both spikes (current value) and leaks (sustained high without
    # any recovery).  60 samples ≈ 10 min at the ~10s STATS cadence.
    pool_overflow_samples: deque = field(default_factory=lambda: deque(maxlen=60))
    pool_overflow_peak: int = 0
    pool_overflow_current: int = 0
    pool_memory_limit_mb: int = 1024  # default; replaced by _POOL_INIT_RE if seen
    # Per-PID waiter tracking: pid → (last_ts, count, high_streak).
    # high_streak counts consecutive STATS samples with waiters >= 5; once
    # that hits 6 samples (~60s) we emit a crit (pool-exhaustion deadlock).
    pool_waiters: dict = field(default_factory=dict)
    # Single-fire flags so the leak/sustained-high crit events don't spam the
    # ring buffer once detected — cleared when the condition resolves.
    pool_leak_warned: bool = False
    pool_sustained_high_warned: bool = False
    # Network / CDN health
    network_error_rate: float = 0.0          # latest "very high network error rate" % seen
    network_error_rate_ts: Optional[datetime] = None
    chunk_http_failures: dict = field(default_factory=dict)  # status_code → count
    chunk_resubmits: int = 0
    chunk_invalid_jpeg: int = 0
    network_high_streak: int = 0             # consecutive samples with >=25% rate
    network_high_warned: bool = False
    # Build pipeline pressure — pid → (ts, queue_size, active, max_active)
    coordinator_state: dict = field(default_factory=dict)
    # Session peaks — coordinator_state holds the latest values per PID, but
    # the queue can fill and drain between samples, so we track the highest
    # depth ever seen (and when) for display.
    bg_queue_peak: int = 0
    bg_queue_peak_ts: Optional[datetime] = None
    bg_queue_peak_pid: str = ""
    # Per-PID inflight builders from latest STATS (separate from coordinator
    # which only tracks BG).  Live builders are the FUSE-driven on-demand
    # builds invisible to coordinator_heartbeat.
    live_builders: dict = field(default_factory=dict)  # pid → count
    bg_builders: dict = field(default_factory=dict)    # pid → count

    def trim(self) -> None:
        cutoff = datetime.now() - timedelta(seconds=self.window_sec)
        while self.events and self.events[0].ts < cutoff:
            self.events.popleft()

    def record_event(self, ev: Event) -> None:
        self.events.append(ev)

    def health(self) -> tuple[str, str]:
        """Return (color, label) for overall health."""
        recent_cutoff = datetime.now() - timedelta(seconds=60)
        recent_crit = sum(1 for e in self.events if e.ts >= recent_cutoff and e.level == "crit")
        if recent_crit:
            return "red", f"CRITICAL ({recent_crit} crit events <60s)"

        # Stale-data check: if our most recent FUSE sample is more than 2 min
        # old, X-Plane probably isn't talking to AO (paused, hung, crashed).
        # Calling that "OK" would be misleading.  FUSE_PERF fires every 60s
        # when there's traffic, so >120s of silence means real silence.
        if self.fuse_samples:
            last_sample_age = (datetime.now() - self.fuse_samples[-1].ts).total_seconds()
            if last_sample_age > 120:
                mins = int(last_sample_age // 60)
                return "yellow", f"NO RECENT DATA (last FUSE sample {mins} min ago — X-Plane paused/hung?)"

        if self.fuse_samples:
            last = self.fuse_samples[-1]
            if last.max_ms > 5000 or last.avg_ms > 500:
                return "red", f"SLOW (max={last.max_ms:.0f}ms avg={last.avg_ms:.0f}ms)"
            if last.max_ms > 1000 or last.avg_ms > 100:
                return "yellow", f"DEGRADED (max={last.max_ms:.0f}ms avg={last.avg_ms:.0f}ms)"
            return "green", f"OK (max={last.max_ms:.0f}ms avg={last.avg_ms:.0f}ms)"
        return "white", "WAITING for FUSE_PERF data"


# ─────────────────────────── parsing dispatch ───────────────────────────

def parse_line(state: MonitorState, line: str) -> None:
    ts = parse_ts(line)
    if ts is None:
        return

    m = _FUSE_PERF_RE.search(line)
    if m:
        sample = FusePerfSample(
            ts=ts,
            reads=int(m.group(2)),
            slow_16ms=int(m.group(3)),
            slow_50ms=int(m.group(4)),
            slow_100ms=int(m.group(5)),
            slow_500ms=int(m.group(6)),
            avg_ms=float(m.group(7)),
            max_ms=float(m.group(8)),
            by_class=m.group(9),
        )
        state.fuse_samples.append(sample)
        return

    m = _READ_SLOW_RE.search(line)
    if m:
        total_ms = int(m.group(1))
        tile = m.group(3)
        state.counters["read_slow"] += 1
        if total_ms > state.last_max_read_ms:
            state.last_max_read_ms = total_ms
            state.last_max_read_tile = tile
        if total_ms >= 5000:
            state.record_event(Event(ts, "warn", "read_slow",
                                     f"{total_ms}ms branch={m.group(2)} tile={tile}"))
        return

    m = _BUILD_STUCK_RE.search(line)
    if m:
        state.counters["build_stuck"] += 1
        state.record_event(Event(ts, "crit", "build_stuck",
                                 f"pid={m.group(1)} tile={m.group(2)} age={m.group(3)}s "
                                 f"stage={m.group(4)} inflight=[{m.group(5)}]"))
        return

    m = _FINALIZE_FILE_SLOW_RE.search(line)
    if m:
        state.counters["finalize_file_slow"] += 1
        state.record_event(Event(ts, "crit", "finalize_file_slow",
                                 f"pid={m.group(1)} tile={m.group(2)} "
                                 f"elapsed={m.group(3)}ms bytes={m.group(4)}"))
        return

    m = _FINALIZE_PROGRESS_RE.search(line)
    if m:
        pid = m.group(1)
        tile = m.group(2)
        elapsed = float(m.group(3))
        size = int(m.group(4))
        growth = int(m.group(5))
        state.counters["finalize_progress_polls"] += 1
        key = (pid, tile)
        if growth <= 0:
            streak = state.finalize_zero_growth_streak.get(key, 0) + 1
            state.finalize_zero_growth_streak[key] = streak
            state.counters["finalize_progress_no_growth"] += 1
            # First zero-growth = warn; sustained = crit (true hang signal)
            level = "crit" if streak >= 2 else "warn"
            state.record_event(Event(ts, level, "finalize_stuck",
                                     f"pid={pid} tile={tile} elapsed={elapsed:.0f}s "
                                     f"staging_size={size} no_growth_streak={streak}"))
        else:
            # Growth: reset streak; emit info-level so user sees disk IS progressing
            state.finalize_zero_growth_streak.pop(key, None)
            mb_per_s = (growth / 1024 / 1024) / 10.0  # 10s poll interval
            state.record_event(Event(ts, "warn", "finalize_progress",
                                     f"pid={pid} tile={tile} elapsed={elapsed:.0f}s "
                                     f"size={size} +{growth}B ({mb_per_s:.2f} MB/s)"))
        return

    m = _FINALIZE_BUFFER_SLOW_RE.search(line)
    if m:
        state.counters["finalize_buffer_slow"] += 1
        state.record_event(Event(ts, "warn", "finalize_buffer_slow",
                                 f"pid={m.group(1)} tile={m.group(2)} elapsed={m.group(3)}ms"))
        return

    m = _STORE_FILE_SLOW_RE.search(line)
    if m:
        state.counters["store_file_slow"] += 1
        state.record_event(Event(ts, "warn", "store_file_slow",
                                 f"pid={m.group(1)} tile={m.group(2)} elapsed={m.group(3)}ms"))
        return

    m = _DDS_CACHE_LOAD_SLOW_RE.search(line)
    if m:
        state.counters["dds_cache_load_slow"] += 1
        state.record_event(Event(ts, "warn", "dds_cache_load_slow",
                                 f"total={m.group(1)}ms read={m.group(2)}ms "
                                 f"decompress={m.group(3)}ms tile={m.group(7)}"))
        return

    m = _NATIVE_BUILD_SLOW_RE.search(line)
    if m:
        state.counters["native_build_slow"] += 1
        return

    m = _EVICTION_RE.search(line)
    if m:
        state.last_rss_mb = int(m.group(1))
        state.rss_limit_mb = float(m.group(2))
        return

    m = _TC_LOCK_WAIT_RE.search(line)
    if m:
        state.counters["tc_lock_wait"] += 1
        wait_ms = float(m.group(1))
        if wait_ms >= 100:
            state.record_event(Event(ts, "warn", "tc_lock_wait", f"{wait_ms:.1f}ms"))
        return

    m = _DELETED_BAD_TILE_RE.search(line)
    if m:
        state.counters["deleted_bad_tile"] += 1
        return

    m = _MOUNT_RE.search(line)
    if m:
        mount = m.group(1).strip()
        if mount not in state.mounts:
            state.mounts.append(mount)
            state.record_event(Event(ts, "info", "mount", f"mounted {os.path.basename(mount)}"))
        return

    m = _TERRAIN_READY_RE.search(line)
    if m:
        region = m.group(1)
        if region not in state.terrain_ready:
            state.terrain_ready.append(region)
            state.record_event(Event(ts, "info", "ready", f"terrain ready: {region}"))
        return

    m = _AO_VERSION_RE.search(line)
    if m:
        state.ao_version = m.group(1).strip()
        state.record_event(Event(ts, "info", "startup", f"AO {state.ao_version}"))
        return

    if _FAILURES_RE.search(line):
        state.counters["failures_detected"] += 1
        state.record_event(Event(ts, "crit", "failures_detected", line.strip()[-120:]))
        return

    if _FORCE_EXIT_RE.search(line):
        state.counters["force_exits"] += 1
        state.record_event(Event(ts, "crit", "force_exit", "unclean shutdown"))
        return

    m = _POOL_INIT_RE.search(line)
    if m:
        state.pool_memory_limit_mb = int(m.group(1))
        state.record_event(Event(ts, "info", "pool_init",
                                 f"decode pool limit: {state.pool_memory_limit_mb} MB"))
        return

    if _STATS_LINE_RE.search(line):
        _parse_pool_stats(state, line, ts)
        return

    m = _NETWORK_ERROR_RATE_RE.search(line)
    if m:
        rate = float(m.group(1))
        state.network_error_rate = rate
        state.network_error_rate_ts = ts
        # Crit when error rate stays ≥25% for 6 consecutive announcements;
        # AO emits this line periodically when its rolling stat is bad.
        if rate >= 25.0:
            state.network_high_streak += 1
            if state.network_high_streak >= 6 and not state.network_high_warned:
                state.network_high_warned = True
                state.counters["network_high_error_crit"] += 1
                state.record_event(Event(
                    ts, "crit", "network_high",
                    f"error rate {rate:.0f}% sustained "
                    f"({state.network_high_streak} consecutive samples) — "
                    f"upstream CDN degraded, expect missing_color tiles"))
        else:
            state.network_high_streak = 0
            state.network_high_warned = False
        return

    m = _CHUNK_HTTP_FAIL_RE.search(line)
    if m:
        status = int(m.group(1))
        state.chunk_http_failures[status] = state.chunk_http_failures.get(status, 0) + 1
        if 500 <= status < 600:
            state.counters["chunk_http_fail_5xx"] += 1
        elif 400 <= status < 500:
            state.counters["chunk_http_fail_4xx"] += 1
        return

    if _CHUNK_RESUBMIT_RE.search(line):
        state.counters["chunk_resubmit"] += 1
        return

    if _CHUNK_INVALID_JPEG_RE.search(line):
        state.counters["chunk_invalid_jpeg"] += 1
        return

    m = _COORDINATOR_RE.search(line)
    if m:
        pid = m.group(1)
        queue_size = int(m.group(2))
        state.coordinator_state[pid] = (
            ts, queue_size, int(m.group(3)), int(m.group(4))
        )
        # Track session-wide peak.  coordinator_state only holds the latest
        # per PID; if a queue fills then drains between samples, we'd lose
        # the high-water mark without this.
        if queue_size > state.bg_queue_peak:
            state.bg_queue_peak = queue_size
            state.bg_queue_peak_ts = ts
            state.bg_queue_peak_pid = pid
        return


def _parse_pool_stats(state: MonitorState, line: str, ts: datetime) -> None:
    """Extract decode-pool counters from a STATS log line.

    Each STATS line is one worker's snapshot.  overflow_mb reflects that
    worker's own pool (per-process), but the dict also includes
    decode_pool_waiters:<PID> entries for every worker the broker knows
    about, so we update those by PID.
    """
    m = _POOL_OVERFLOW_MB_RE.search(line)
    if m:
        mb = int(m.group(1))
        state.pool_overflow_current = mb
        state.pool_overflow_samples.append((ts, mb))
        if mb > state.pool_overflow_peak:
            state.pool_overflow_peak = mb

        cap = state.pool_memory_limit_mb
        # Sustained high (>=80% of cap across last 6 samples ≈ 60s): the
        # symptom that preceded today's deadlock by ~30 min.
        recent = [s for _t, s in list(state.pool_overflow_samples)[-6:]]
        if len(recent) >= 6 and min(recent) >= 0.8 * cap:
            if not state.pool_sustained_high_warned:
                state.pool_sustained_high_warned = True
                state.counters["pool_sustained_high_crit"] += 1
                state.record_event(Event(
                    ts, "crit", "pool_sustained_high",
                    f"overflow_mb stayed >= {0.8 * cap:.0f} for 6 samples "
                    f"(min={min(recent)} max={max(recent)} cap={cap})"))
        elif state.pool_sustained_high_warned and recent and min(recent) < 0.5 * cap:
            state.pool_sustained_high_warned = False

        # Leak signature: last 10 samples are monotonically non-decreasing
        # AND span > 100 MB of growth with no dip.  Normal heavy use
        # oscillates; a counter leak only goes up.  Requires a meaningful
        # span so we don't fire on quiet startups.
        last10 = [s for _t, s in list(state.pool_overflow_samples)[-10:]]
        if len(last10) >= 10:
            monotonic = all(last10[i] >= last10[i - 1] for i in range(1, 10))
            span = last10[-1] - last10[0]
            if monotonic and span >= 100 and not state.pool_leak_warned:
                state.pool_leak_warned = True
                state.counters["pool_leak_crit"] += 1
                state.record_event(Event(
                    ts, "crit", "pool_leak",
                    f"overflow_mb only went up across 10 samples "
                    f"({last10[0]} → {last10[-1]} MB, no recovery — leak signature)"))
            elif state.pool_leak_warned and not monotonic:
                state.pool_leak_warned = False

    for m in _POOL_WAITERS_RE.finditer(line):
        pid = m.group(1)
        count = int(m.group(2))
        prev = state.pool_waiters.get(pid, (ts, 0, 0))
        streak = prev[2] + 1 if count >= 5 else 0
        state.pool_waiters[pid] = (ts, count, streak)
        # Once a worker has had >=5 waiters for 6 consecutive samples (~60s),
        # finalize is about to wedge.  Fires once per PID per sustained run.
        if streak == 6:
            state.counters["pool_high_waiters_crit"] += 1
            state.record_event(Event(
                ts, "crit", "pool_high_waiters",
                f"pid={pid} waiters={count} for 6 consecutive samples "
                f"(pool exhaustion — build_stuck imminent)"))

    # Per-PID live/BG builder inflight counts — overwrite from each STATS
    # line (each line is one worker's snapshot; the dict has values for all
    # workers known to the broker).
    for m in _INFLIGHT_BUILDERS_RE.finditer(line):
        tag, pid, count = m.group(1), m.group(2), int(m.group(3))
        if tag == "live":
            state.live_builders[pid] = count
        else:
            state.bg_builders[pid] = count


# ─────────────────────────── rendering ───────────────────────────

def make_bar(value: float, max_value: float, width: int = 24,
             green_until: float = 0.5, yellow_until: float = 0.9,
             inverted: bool = False) -> str:
    """Render a fixed-width Unicode usage bar.

    Color tiers: green below `green_until` * max, yellow up to `yellow_until`,
    red beyond.  Dimmed shading for the empty portion.  Returns rich markup
    suitable for embedding in Table cells.

    When `inverted=True`, the color logic is reversed — high fill is good
    (green), low fill is bad (red).  Used for ratios where 100% is the
    desired state (e.g. cache hit ratio).  In inverted mode, `green_until`
    becomes the FLOOR below which we go red, and `yellow_until` is the
    yellow→green crossover.
    """
    if max_value <= 0:
        return "[dim]" + "░" * width + "[/dim]"
    pct = max(0.0, min(1.0, value / max_value))
    filled = int(round(pct * width))
    if inverted:
        if pct < green_until:
            color = "red"
        elif pct < yellow_until:
            color = "yellow"
        else:
            color = "green"
    else:
        if pct >= yellow_until:
            color = "red"
        elif pct >= green_until:
            color = "yellow"
        else:
            color = "green"
    return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * (width - filled)}[/dim]"


def parse_by_class(by_class: str) -> dict:
    """Extract {class_name: read_count} from a FUSE_PERF by_class string."""
    return {m.group(1): int(m.group(2)) for m in _BY_CLASS_KV_RE.finditer(by_class)}


def render_header(state: MonitorState) -> Panel:
    color, label = state.health()
    sample = state.fuse_samples[-1] if state.fuse_samples else None

    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column()
    tbl.add_row("Status:", Text(label, style=color))
    tbl.add_row("AO version:", state.ao_version)
    tbl.add_row("Mounts:", f"{len(state.mounts)} mounted, {len(state.terrain_ready)} terrain ready")
    if state.last_rss_mb is not None:
        if state.rss_limit_mb:
            bar = make_bar(state.last_rss_mb, state.rss_limit_mb)
            rss_str = f"{bar}  {state.last_rss_mb} MB / {state.rss_limit_mb:.0f} MB"
            if state.last_rss_mb > state.rss_limit_mb:
                rss_str += "  [red](OVER)[/red]"
        else:
            rss_str = f"{state.last_rss_mb} MB"
        tbl.add_row("RSS (last):", rss_str)
    if sample:
        age = (datetime.now() - sample.ts).total_seconds()
        if age <= 90:
            age_str = f"[dim]({age:.0f}s ago)[/dim]"
        else:
            age_str = f"[yellow]({age:.0f}s ago — stale)[/yellow]"
        tbl.add_row("Last FUSE window:",
                    f"{sample.reads} reads, "
                    f"slow_500ms={sample.slow_500ms}, "
                    f"avg={sample.avg_ms:.1f}ms, max={sample.max_ms:.0f}ms  {age_str}")
        tbl.add_row("by_class:", sample.by_class or "—")
    if state.pool_overflow_samples:
        cap = state.pool_memory_limit_mb
        cur = state.pool_overflow_current
        peak = state.pool_overflow_peak
        pct = (cur / cap * 100) if cap else 0
        bar = make_bar(cur, cap)
        pool_str = f"{bar}  cur={cur} peak={peak} cap={cap} MB ({pct:.0f}%)"
        tbl.add_row("Decode pool:", pool_str)
        # Per-PID waiters: show only PIDs with non-zero waiters
        active = [(pid, c, s) for pid, (_t, c, s) in state.pool_waiters.items() if c > 0]
        if active:
            parts = []
            for pid, c, s in sorted(active):
                style = "[red]" if c >= 5 else "[yellow]"
                close = "[/red]" if c >= 5 else "[/yellow]"
                streak_str = f" streak={s}" if s >= 2 else ""
                parts.append(f"{style}pid={pid}:{c}{streak_str}{close}")
            tbl.add_row("Pool waiters:", " ".join(parts))
    # Network / CDN row — always shown so the monitoring is visibly active
    # even on clean sessions.  Empty bar + "err_rate=0%" means "watching, no
    # CDN issues seen in window."
    rate = state.network_error_rate
    bar = make_bar(rate, 100.0, green_until=0.10, yellow_until=0.25)
    fail_str = ""
    if state.chunk_http_failures:
        top = sorted(state.chunk_http_failures.items(),
                     key=lambda kv: -kv[1])[:2]
        fail_str = "  " + " ".join(f"[red]{s}xx={c}[/red]"
                                   if s >= 500 else f"[yellow]{s}={c}[/yellow]"
                                   for s, c in top)
    invalid_str = (f"  invalid_jpeg={state.chunk_invalid_jpeg}"
                   if state.chunk_invalid_jpeg else "")
    tbl.add_row("Network:",
                f"{bar}  err_rate={rate:.0f}%{fail_str}{invalid_str}")

    # Cache hit ratio — disk_passthrough + file_passthrough vs total reads in
    # the last FUSE window.  Inverted color: high cached fraction is good.
    if sample and sample.by_class:
        by_class = parse_by_class(sample.by_class)
        total = sum(by_class.values())
        if total > 0:
            cached = by_class.get("disk_passthrough", 0) + by_class.get("file_passthrough", 0)
            ratio = cached / total
            bar = make_bar(cached, total, inverted=True,
                           green_until=0.7, yellow_until=0.9)
            tbl.add_row("Cache hit:",
                        f"{bar}  {ratio*100:.0f}%  ({cached}/{total} reads cached)")

    # Build pressure: combined live + BG.  Live builds are FUSE-driven and
    # bypass the coordinator queue, so the coordinator alone undercounts
    # actual builder activity during heavy fresh-load bursts.
    if state.coordinator_state or state.live_builders:
        bg_active = sum(c[2] for c in state.coordinator_state.values())
        total_bg_slots = sum(c[3] for c in state.coordinator_state.values())
        live_held = sum(state.live_builders.values())
        total_active = bg_active + live_held
        # Visual cap: 2× total BG slots (live can spill arbitrarily but
        # this scale keeps "busy but normal" visible without saturating).
        visual_cap = max(total_bg_slots * 2, 24) if total_bg_slots else max(24, total_active)
        bar = make_bar(total_active, visual_cap,
                       green_until=0.4, yellow_until=0.75)
        tbl.add_row("Build slots:",
                    f"{bar}  {total_active} active "
                    f"({live_held} live + {bg_active}/{total_bg_slots} BG)")

    # Live demand — tile-class reads in the latest FUSE window.  This is
    # the "X-Plane asking for new stuff right now" signal that bypasses
    # the BG coordinator queue entirely (each FUSE read fires its own
    # on-demand builder).  streaming_builder_held_live + BG queue both
    # undercount actual demand during cold-load bursts because builds
    # often complete between STATS samples.  Reference cap 2000 reads/min
    # covers the calm-to-saturated range:
    #   <500   = calm cruise   |   500-1500 = active   |   1500+ = heavy/cold
    if sample and sample.by_class:
        by_class = parse_by_class(sample.by_class)
        tile_reads = by_class.get("tile", 0)
        demand_cap = 2000
        bar = make_bar(min(tile_reads, demand_cap), demand_cap,
                       green_until=0.25, yellow_until=0.75)
        slow_str = (f"  [yellow]{sample.slow_500ms} slow_500ms[/yellow]"
                    if sample.slow_500ms > 0 else "")
        tbl.add_row("Live demand:",
                    f"{bar}  {tile_reads} tile reads/min{slow_str}")

    # BG queue depth — predictive backlog only.  Live builds don't have a
    # queue (each FUSE read triggers its own builder), so a separate metric.
    # Shows current max-across-workers AND session peak (queues fill and
    # drain between samples; without the peak tracker, transient backups
    # disappear from the display the next time we render).
    if state.coordinator_state:
        cur_max = max(c[1] for c in state.coordinator_state.values())
        queue_cap = 100
        bar = make_bar(min(cur_max, queue_cap), queue_cap,
                       green_until=0.25, yellow_until=0.75)
        worst = max(state.coordinator_state.items(), key=lambda kv: kv[1][1])
        worst_str = (f"  worst=pid {worst[0]}({worst[1][1]})"
                     if worst[1][1] > 0 else "")
        peak_str = ""
        if state.bg_queue_peak > 0:
            ts_str = state.bg_queue_peak_ts.strftime("%H:%M:%S") if state.bg_queue_peak_ts else "?"
            peak_str = (f"  [dim]peak={state.bg_queue_peak} "
                        f"(pid {state.bg_queue_peak_pid} @ {ts_str})[/dim]")
        tbl.add_row("BG queue:",
                    f"{bar}  now={cur_max}{worst_str}{peak_str}")

    return Panel(tbl, title=f"AutoOrtho health  ({state.log_path})", border_style=color)


def render_counters(state: MonitorState) -> Panel:
    tbl = Table.grid(padding=(0, 2))
    tbl.add_column(style="bold")
    tbl.add_column(justify="right")

    def fmt(key: str, label: str, crit_threshold: int = 0) -> None:
        n = state.counters[key]
        if n == 0:
            tbl.add_row(label, "[dim]0[/dim]")
        elif crit_threshold and n >= crit_threshold:
            tbl.add_row(label, f"[red bold]{n}[/red bold]")
        else:
            tbl.add_row(label, f"[yellow]{n}[/yellow]")

    fmt("read_slow", "READ_DDS_BYTES VERY_SLOW")
    fmt("build_stuck", "build_stuck", crit_threshold=1)
    fmt("finalize_file_slow", "finalize_to_file SLOW", crit_threshold=1)
    fmt("finalize_buffer_slow", "finalize_to_buffer SLOW")
    fmt("store_file_slow", "store_from_file SLOW")
    fmt("dds_cache_load_slow", "DDS_CACHE_LOAD VERY_SLOW")
    fmt("native_build_slow", "NATIVE_BUILD_EXIT slow")
    fmt("tc_lock_wait", "tc_lock_wait events")
    fmt("deleted_bad_tile", "deleted bad passthrough tiles")
    fmt("force_exits", "force_exit (unclean shutdown)", crit_threshold=1)
    fmt("failures_detected", "FAILURES DETECTED", crit_threshold=1)
    fmt("finalize_progress_polls", "finalize_progress polls")
    fmt("finalize_progress_no_growth", "finalize_progress no_growth polls", crit_threshold=1)
    fmt("pool_high_waiters_crit", "decode pool high-waiter events", crit_threshold=1)
    fmt("pool_sustained_high_crit", "decode pool sustained near-cap", crit_threshold=1)
    fmt("pool_leak_crit", "decode pool leak signature", crit_threshold=1)
    fmt("chunk_http_fail_5xx", "chunk HTTP 5xx", crit_threshold=20)
    fmt("chunk_http_fail_4xx", "chunk HTTP 4xx")
    fmt("chunk_resubmit", "chunk re-submits")
    fmt("chunk_invalid_jpeg", "chunk invalid_jpeg (PNG placeholder)")
    fmt("network_high_error_crit", "network high-error sustained", crit_threshold=1)

    if state.last_max_read_ms > 0:
        tbl.add_row("Slowest read seen:",
                    f"{state.last_max_read_ms:.0f}ms ({state.last_max_read_tile})")
    if state.pool_overflow_peak > 0:
        tbl.add_row("Decode pool peak:",
                    f"{state.pool_overflow_peak} MB / {state.pool_memory_limit_mb} MB cap")

    return Panel(tbl, title="Counters (since start)", border_style="cyan")


def render_events(state: MonitorState) -> Panel:
    state.trim()
    tbl = Table.grid(padding=(0, 1))
    tbl.add_column(style="dim", no_wrap=True)
    tbl.add_column(no_wrap=True)
    tbl.add_column(no_wrap=True)
    tbl.add_column()

    # Show last 25 events newest at bottom
    recent = list(state.events)[-25:]
    for ev in recent:
        ts_str = ev.ts.strftime("%H:%M:%S")
        if ev.level == "crit":
            level_style = "red bold"
        elif ev.level == "warn":
            level_style = "yellow"
        else:
            level_style = "dim"
        tbl.add_row(ts_str, Text(ev.level.upper(), style=level_style),
                    Text(ev.category, style="cyan"), ev.text)

    return Panel(tbl, title=f"Recent events (last {state.window_sec}s window, max 25 shown)",
                 border_style="blue")


def render(state: MonitorState) -> Group:
    return Group(render_header(state), render_counters(state), render_events(state))


# ─────────────────────────── tailer ───────────────────────────

def tail(path: Path, from_start: bool = False):
    """Yield lines from path. Reopen on truncate/rotate."""
    while not path.exists():
        time.sleep(0.5)

    f = open(path, "r", errors="replace")
    inode = os.fstat(f.fileno()).st_ino
    if not from_start:
        f.seek(0, os.SEEK_END)

    try:
        while True:
            line = f.readline()
            if line:
                yield line
                continue
            # No new data; check if file was rotated.
            time.sleep(0.2)
            try:
                cur_inode = os.stat(path).st_ino
            except FileNotFoundError:
                continue
            if cur_inode != inode:
                f.close()
                f = open(path, "r", errors="replace")
                inode = os.fstat(f.fileno()).st_ino
    finally:
        try:
            f.close()
        except Exception:
            pass


_STATE_ESTABLISHING_RES = (
    _AO_VERSION_RE, _MOUNT_RE, _TERRAIN_READY_RE, _POOL_INIT_RE,
)


def _is_state_establishing(line: str) -> bool:
    return any(r.search(line) for r in _STATE_ESTABLISHING_RES)


def _scan_state_only(path: Path, state: MonitorState) -> None:
    """Scan a (possibly rotated) log file for state-establishing lines only.

    Cheap regex pre-filter avoids parsing high-volume event lines.  Used as
    a fallback when log rotation has moved the AO startup banner out of the
    current log file.
    """
    try:
        with open(path, "r", errors="replace") as f:
            for line in f:
                if not _is_state_establishing(line):
                    continue
                parse_line(state, line)
    except OSError:
        pass


def backfill(path: Path, state: MonitorState, seconds: int) -> None:
    """Parse the last N seconds of the log on startup so the UI isn't empty.

    State-establishing log lines (AO version, mounts, terrain-ready, pool
    init) are parsed regardless of cutoff — those record stable facts about
    the running AO, not time-bounded events.  Without this exemption, the
    "AO version" / "Mounts" fields stay blank whenever the monitor starts
    more than `seconds` after AO did (the startup banner has aged out).

    If state still missing after the current log (e.g. log rotated mid-
    session and the banner is in `autoortho.log.1`), walk back through
    rotated logs newest → oldest scanning only for state lines, stopping
    once we've found version + at least one mount.
    """
    if not path.exists():
        return
    cutoff = datetime.now() - timedelta(seconds=seconds)
    # Read tail — capped at 5MB to avoid loading huge logs.
    size = path.stat().st_size
    start = max(0, size - 5 * 1024 * 1024)
    with open(path, "r", errors="replace") as f:
        f.seek(start)
        # Skip partial first line.
        if start > 0:
            f.readline()
        for line in f:
            ts = parse_ts(line)
            if ts is None:
                continue
            if ts < cutoff:
                # Outside event window — but still process if it's a
                # state-establishing line.  Cheap regex pre-filter keeps
                # this fast on large logs.
                if _is_state_establishing(line):
                    parse_line(state, line)
                continue
            parse_line(state, line)

    # Walk back through rotated logs only if we still don't have the basics.
    # Most recent rotation first (.1, .2, ...) — first hit wins because the
    # latest AO startup banner / mount sequence is what matters.
    if state.ao_version == "?" or not state.mounts:
        for i in range(1, 6):
            rotated = path.with_name(path.name + f".{i}")
            if not rotated.exists():
                break
            _scan_state_only(rotated, state)
            if state.ao_version != "?" and state.mounts:
                break


# ─────────────────────────── main ───────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="Live AutoOrtho health monitor")
    ap.add_argument("--log", type=Path, default=DEFAULT_LOG,
                    help=f"Log file to tail (default: {DEFAULT_LOG})")
    ap.add_argument("--window-sec", type=int, default=300,
                    help="Event display window in seconds (default: 300)")
    ap.add_argument("--backfill-sec", type=int, default=3600,
                    help="Backfill this many seconds on startup (default: 3600 = 1h)")
    ap.add_argument("--refresh-hz", type=float, default=2.0,
                    help="UI refresh rate (default: 2 Hz)")
    args = ap.parse_args()

    state = MonitorState(window_sec=args.window_sec, log_path=args.log)
    console = Console()

    if args.log.exists():
        backfill(args.log, state, args.backfill_sec)
    else:
        console.print(f"[yellow]Log not found at {args.log} — waiting…[/yellow]")

    try:
        with Live(render(state), console=console, refresh_per_second=args.refresh_hz,
                  screen=True) as live:
            for line in tail(args.log):
                parse_line(state, line)
                live.update(render(state))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
