"""X-Plane process watcher: auto-close AutoOrtho when X-Plane exits.

Detection is PID-based (not dataref/UDP-based): we locate the X-Plane process
once, then cheaply poll whether that PID is still alive. UDP/dataref silence is
*not* used as a signal because X-Plane goes quiet at the main menu / when paused
while the process is still very much alive — using that would false-close AO
between flights.

Safety properties (the failure mode to avoid is closing AO mid-flight):
  * Arms only AFTER X-Plane has been seen running at least once. Launching AO
    before X-Plane never triggers a shutdown.
  * Requires X-Plane to be absent for several consecutive polls (a debounce)
    before quitting, so a single missed/odd poll can't close AO.
  * Tolerates an X-Plane restart: if the watched PID dies but a fresh X-Plane
    process appears, it adopts the new PID instead of quitting.

Enabled by default. Disable with env AO_NO_XPLANE_WATCH=1, or config flag
[general] exit_with_xplane = False.
"""

import os
import signal
import logging
import threading

import psutil

log = logging.getLogger(__name__)

# Substring matched (case-insensitively) against the process name on every
# platform: macOS "X-Plane", Windows "X-Plane.exe", Linux "X-Plane-x86_64".
_XPLANE_NAME_HINT = "x-plane"

_POLL_INTERVAL_SEC = 5.0
_ABSENT_POLLS_TO_QUIT = 3  # ~15s of confirmed absence before auto-closing

_stop_event = threading.Event()
_thread = None


def _proc_is_xplane(pid):
    """True if `pid` is alive AND its process name still looks like X-Plane.

    The name re-check guards against PID reuse: between polls the OS may have
    recycled the PID for an unrelated process.
    """
    try:
        name = psutil.Process(pid).name() or ""
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return False
    return _XPLANE_NAME_HINT in name.lower()


def find_xplane_pid():
    """Return the PID of a running X-Plane process, or None if not found."""
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            name = proc.info.get("name") or ""
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
        if _XPLANE_NAME_HINT in name.lower():
            return proc.info.get("pid", proc.pid)
    return None


def is_xplane_running():
    """Convenience predicate kept for callers/tests."""
    return find_xplane_pid() is not None


def _trigger_shutdown():
    """Initiate a graceful AutoOrtho shutdown from the watcher thread.

    Prefers the Qt-aware path when a GUI is running (returns from app.exec()
    so the normal teardown in main() runs). Falls back to a SIGTERM to self
    on POSIX (wired to _global_shutdown), or a direct shutdown on Windows
    headless where SIGTERM is a hard kill that would orphan FUSE mounts.
    """
    log.warning(
        "X-Plane process is gone (absent for ~%ss) — auto-closing AutoOrtho",
        int(_ABSENT_POLLS_TO_QUIT * _POLL_INTERVAL_SEC),
    )

    # 1. GUI mode: ask the Qt app to quit, thread-safely. This unblocks
    #    app.exec() and lets main()'s existing teardown run.
    try:
        from PySide6.QtWidgets import QApplication
        from PySide6.QtCore import QMetaObject, Qt

        app = QApplication.instance()
        if app is not None:
            QMetaObject.invokeMethod(app, "quit", Qt.ConnectionType.QueuedConnection)
            return
    except Exception as e:  # PySide6 absent / headless
        log.debug("Qt quit path unavailable: %s", e)

    # 2. Headless POSIX: SIGTERM to self runs the wired _global_shutdown
    #    handler on the main thread (clean FUSE teardown).
    if os.name != "nt":
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            return
        except Exception as e:
            log.debug("SIGTERM self failed: %s", e)

    # 3. Windows headless (rare): SIGTERM would hard-kill and orphan mounts,
    #    so run the orchestrated shutdown directly, then force-exit.
    try:
        try:
            from autoortho.__main__ import _global_shutdown
        except ImportError:
            from __main__ import _global_shutdown
        _global_shutdown()
    except Exception as e:
        log.warning("Direct shutdown failed: %s", e)
    os._exit(0)


def _watch_loop():
    watched_pid = None
    absent = 0
    log.info(
        "X-Plane watcher started (poll=%ss, quit after ~%ss absent)",
        _POLL_INTERVAL_SEC,
        int(_ABSENT_POLLS_TO_QUIT * _POLL_INTERVAL_SEC),
    )
    while not _stop_event.is_set():
        if watched_pid is None:
            # Not armed yet: look for X-Plane.
            watched_pid = find_xplane_pid()
            if watched_pid is not None:
                absent = 0
                log.info(
                    "X-Plane detected (pid=%s); auto-close on exit is armed",
                    watched_pid,
                )
        elif _proc_is_xplane(watched_pid):
            absent = 0
        else:
            # Watched PID is gone. It may have restarted with a new PID —
            # adopt it and stay armed rather than quitting.
            new_pid = find_xplane_pid()
            if new_pid is not None:
                log.info(
                    "X-Plane PID changed %s -> %s (restart?); staying armed",
                    watched_pid, new_pid,
                )
                watched_pid = new_pid
                absent = 0
            else:
                absent += 1
                log.debug(
                    "X-Plane absent (%s/%s polls)", absent, _ABSENT_POLLS_TO_QUIT
                )
                if absent >= _ABSENT_POLLS_TO_QUIT:
                    _trigger_shutdown()
                    return
        _stop_event.wait(_POLL_INTERVAL_SEC)


def start(cfg=None):
    """Start the watcher thread unless disabled. Idempotent.

    Returns the Thread (or None if disabled / already running).
    """
    global _thread

    if os.environ.get("AO_NO_XPLANE_WATCH", "").strip().lower() in (
        "1", "true", "yes", "on",
    ):
        log.info("X-Plane watcher disabled via AO_NO_XPLANE_WATCH")
        return None

    if cfg is not None:
        try:
            if not getattr(cfg.general, "exit_with_xplane", True):
                log.info("X-Plane watcher disabled via config (exit_with_xplane=False)")
                return None
        except Exception:
            pass  # missing section/key -> default enabled

    if _thread is not None and _thread.is_alive():
        return _thread

    _stop_event.clear()
    _thread = threading.Thread(
        target=_watch_loop, name="xplane-watcher", daemon=True
    )
    _thread.start()
    return _thread


def stop(join_timeout=2.0):
    """Signal the watcher to stop (used during normal shutdown)."""
    _stop_event.set()
    t = _thread
    if t is not None and t.is_alive():
        t.join(timeout=join_timeout)
