# tools/

Dev/ops utilities for AutoOrtho.  Not shipped to end users — `tools/` lives
outside the `autoortho/` package and is excluded from the PyInstaller build.

## monitor.py

Live health monitor that tails `~/.autoortho-data/logs/autoortho.log` and
renders a real-time TUI showing FUSE perf, decode-pool state, network/CDN
health, build-pipeline pressure, and recent events.

### Setup

Use the existing AO `.venv` rather than creating a separate one:

```sh
.venv/bin/pip install -r tools/requirements.txt
```

(`rich` is the only tool-side dependency; it's intentionally not in the
main `requirements.txt` so user installs don't pull it.)

### Run

```sh
.venv/bin/python3 tools/monitor.py
```

Or activate the venv once (`source .venv/bin/activate`) and just
`python tools/monitor.py`.

Do NOT use the system `python3` — modern macOS Python installations don't
ship `rich`, and the monitor will silently fail to import.

Useful flags:

- `--log PATH` — override default log location
- `--backfill-sec N` — how many seconds of history to parse on startup
  (default 600).  Bump to a few thousand if you want to catch events from
  earlier in a long session.
- `--window-sec N` — event display window in seconds (default 300)
- `--refresh-hz X` — UI refresh rate (default 2)

### What it watches

- FUSE read perf (avg/max latency, slow buckets, per-class breakdown)
- Decode pool: current overflow, peak, waiters per worker — early warning
  for the counter-leak deadlock pattern fixed in `aodecode.c:716-725`
- Network: CDN error rate, HTTP failures by status, chunk re-submits
- Build pipeline: per-worker queue depth and active build slots
- Cache hit ratio, RSS vs limit
- `build_stuck`, `finalize_progress`, `tc_lock_wait` event firings

### Why it's here

The parsers depend on specific log line shapes emitted by
`autoortho/getortho.py`, `autoortho/autoortho_fuse.py`, and
`autoortho/aopipeline/AoDDS.py`.  Co-locating the monitor with the code
that produces those lines makes drift between emitter and parser
catch-able in the same PR.
