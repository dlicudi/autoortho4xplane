# AutoOrtho — Persistent Green Strip at Top of Tiles (Analysis & Handoff)

**Date:** 2026-05-28
**Branch context:** investigated on `fix/z18-mm0-prefill` (branched from tag
`dds-passthrough-checkpoint-2026-05-25`, commit `ead2e72`). Line numbers below
are from that tree; verify against current `develop`.

---

## Symptom

A green strip across the **top edge** of terrain tiles in X-Plane. Properties
the user has confirmed by observation:

- **Persistent**: identical strip, same place, **every X-Plane restart**.
- **Distance-dependent**: invisible zoomed out; appears when you zoom/get close.
- Common **near airports**, but reproduced on plain terrain tiles too.

---

## Root cause (VERIFIED — not hypothesis)

The strip is `missing_color` BC1 blocks (RGB 66,77,55) occupying the **top chunk-row
of mm0**, served to X-Plane and uploaded to the GPU on the tile's very first read,
then never corrected.

### Evidence chain (each step reproduced this session)

1. **The served bytes contain green at the top of mm0.**
   Read the virtual DDS straight from the FUSE mount (exactly what XP receives):
   ```
   ~/X-Plane 12/Custom Scenery/z_ao_<region>/textures/{row}_{col}_{maptype}{zoom}.dds
   ```
   An X-Plane-style **35 KB read at offset 0** returned **100% missing_color** in
   the top ~68 px (17 BC1 block-rows) of mm0. The rest of mm0 and all lower mips
   decode to **real imagery** (verified by decoding BC1 → PNG with numpy).
   So AO's *imagery* is fine; only the top of mm0 is green, and only at serve time.

2. **X-Plane's first read is always offset=0 and always reaches into mm0.**
   Counters this session:
   ```
   header_read_count      = 13377
   header_read_len_gt4096 = 13377   (100% — every header read is >4 KB)
   header_read_len_le128  = 0
   ```
   The DDS header is 128 bytes; mm0 starts at byte 128. Every XP first-read is
   ~35 KB = header + top chunk-row of mm0. There is **no pure-header probe**; XP
   always pulls the top of mm0 on first contact.

3. **The header-skip short-circuit serves that region unbuilt.**
   `Tile.get_bytes()` at **getortho.py:8209**:
   ```python
   if offset == 0:
       bump('header_read_skipped_build')
       return True        # returns WITHOUT building mm0
   ```
   For `offset == 0`, AO returns immediately. Back in `read_dds_bytes`
   (~getortho.py:8969) `self.dds.read(length)` then returns the buffer for that
   range — which for an unbuilt mm0 is `missing_color` fallback (pydds). Counter:
   `header_read_skipped_build = 11728`.

4. **mm0 only ever builds on an `offset > 0` read — which XP rarely does.**
   The mm0 build trigger is gated at **getortho.py:8077**:
   ```python
   is_pure_mipmap_request = offset > 0      # required for the build
   ```
   used in the build condition at ~getortho.py:8086-8092. Result:
   ```
   serve_x_zl16_unknown_0_25     = 17112    (z16 served with mm0 ~unbuilt)
   aopipeline_mipmap_0_triggered = 35       (mm0 build fired only 35x all session)
   ```
   17,112 incomplete serves vs **35** actual mm0 builds.

5. **Affected tiles are not in the disk cache → live-built every read → persistent.**
   `find` across `~/.autoortho-data/cache/dds_cache/` found no cached `.dds` for the
   affected tiles. The disk-cache check at **getortho.py:7959** runs *before* the
   header-skip, so cached tiles serve clean — the green only hits cold/uncached
   tiles. Because they're never cached, every session rebuilds them the same way →
   identical strip every restart.

### Why it never heals (the key question)

- The **only** thing that builds mm0 is an `offset > 0` read (step 4). XP
  overwhelmingly issues `offset == 0` reads, which hit the header-skip and return
  green without building.
- There is **no background sweep** that detects tiles served incomplete
  (the 17,112) and rebuilds/caches their mm0. The live-build heal wiring exists
  only in a commit *after* this tag (`bb62c45`), and even that **defers during
  live reads**: `network_healing_deferred_live = 9`.
- X-Plane uploads the first-read bytes to a GPU texture **once** and does not
  re-read mid-session. So a tile served green from a header read stays green for
  the whole session, and (being uncached) again next session.

---

## Ruled out (do not re-investigate these)

- **Layout-mismatch / dynamic-zoom (z18) builds.** The green tile observed was a
  **plain z16** tile (`layout_zoom=16 build_zoom=16`, `origin=unknown`,
  `partial=260/260`, `max_mm0_pct=7`). The layout-mismatch z18 tile next to it
  (`build_zoom=17 layout_zoom=18`, `origin=upscale_rebuild_native`) built to
  `max_mm0_pct=100` and was **fine**. The `streaming_prefetch_skip_layout_mismatch`
  gate (getortho.py:4250) routes those tiles to the Python BG path; it is **not**
  the cause.
- **Bing imagery gap.** Decoded tiles are real satellite imagery at every
  populated mip; a source gap would be green at all distances, not just up close.
- **dds_passthrough.** Disabled in config (`dds_passthrough_enabled = False`) and
  irrelevant to this path.
- **GPU/driver cache staleness.** It's deterministic from the served bytes, not a
  driver artifact (the served bytes literally contain the green).

---

## Relevant config (`~/.autoortho`)

```
max_zoom = 16
max_zoom_near_airports = 17
max_zoom_mode = fixed
fallback_level = full
predictive_dds_enabled = True
persistent_dds_cache_mb = 4096    (cache IS enabled)
dds_passthrough_enabled = False
```

---

## Why the obvious fixes don't work

- **Just build mm0 on every header read** → defeats the header-skip optimization
  (its whole purpose is avoiding ~16 wasted chunk downloads for the ~99% of
  header-read tiles XP never renders up close). Reintroduces wasted downloads on
  all 13,377 header reads.
- **Predict probe-vs-render from the read** → impossible. Probes and renders issue
  byte-identical offset=0 / 35 KB reads. The discriminator is not in the read.
- **Inject into X-Plane's GPU memory** → not viable: AO is a separate process with
  no GL/Metal context, no XPLM API for per-tile ortho texture replacement, and
  cross-process memory injection is blocked by macOS SIP/hardened runtime.
- **Serve low-ZL imagery for header reads instead of green** → correct idea, but on
  a *truly cold* tile there is no local low-ZL source without a network fetch.

---

## Proposed fix (location-gated mm0 build — user's design)

Since the read can't tell a probe from a render, use **aircraft position** as the
discriminator (available in STATS; `SpatialPrefetcher` already computes "near"
tiles):

- For a header read (`offset == 0`, `length > 128`, mm0 not built) where the tile
  is **near the aircraft / within render range** → **build mm0 now** (or ensure it
  was pre-built) so the served bytes are real. These are the tiles that will be
  rendered up close.
- For tiles **far from the aircraft** → keep skipping (cheap; they're probes or
  distance-only and never need mm0). Preserves the header-skip perf win.

This addresses both the green strip *and* the no-heal problem with **no tradeoff
for the tiles that matter**: the ones you'll actually see up close get built; the
distant spam stays cheap.

### CRITICAL COMPLICATION: no aircraft location during the initial load burst

At scenery load, **X-Plane hammers AO for a flood of tiles before a reliable
aircraft position is available to AO.** This is exactly the window that produces
the strip at your **spawn/start airport** — the tiles you load into are requested
during the burst, served green via the header-skip, uploaded, and (being
uncached) never corrected. A naive "gate on aircraft distance" fails here because
there is no location yet.

The fix must handle the no-location-yet case. Candidate approaches (for Codex to
evaluate):
1. **Seed location from the load request itself.** The first wave of tile requests
   *defines* a tight geographic cluster — AO can infer the load center from the
   request coordinates before any position dataref is read, and treat that cluster
   as "near → build mm0."
2. **Read the start position early.** X-Plane knows the spawn lat/lon before the
   FUSE flood (start airport / saved situation). If AO can obtain it (dataref,
   plugin bridge, or the `.prf`/situation), use it to gate the load burst.
3. **Treat the load phase specially.** During the initial burst (detectable: high
   request rate, `suspend_maxwait`/loading flag already exists in config), build
   mm0 for *all* requested tiles (accept the one-time load cost) since they are by
   definition the area around the spawn. Switch to location-gating once flying.
4. **Make it heal regardless of location** (see below) so even if a load-burst
   tile is served green, a background pass fixes + caches it shortly after, so it's
   correct before the user looks closely and on every subsequent session.

Approaches (1) and (4) are the most robust because they don't depend on a position
that isn't available yet.

**Complementary durability fix (and the most robust standalone fix):** a
**background heal sweep** that detects tiles served incomplete (mm0 below a
threshold — the 17,112 `serve_x_zl16_unknown_0_25` events name exactly these) and
**rebuilds + stores** their mm0 in the persistent dds_cache, independent of
aircraft location. Once cached, the next read (this session or next) hits the
cache at getortho.py:7959 and never re-enters the green path. This sidesteps the
no-location-during-load problem entirely. Investigate why the affected tiles are
never cached today (build never completing? store skipped? evicted?) —
`prebuilt_dds_builds = 1` vs `prebuilt_dds_builds_streaming = 301` and
`prebuilt_dds_skipped_locked = 146` are the threads to pull. Note the existing
heal path **defers during live reads** (`network_healing_deferred_live = 9`),
which likely prevents it from ever running while XP is actively reading — a prime
suspect for "why doesn't it heal."

### Existing partial attempt (incomplete — do not rely on)

Commit on `fix/z18-mm0-prefill` adds a config flag `prefill_mm0_on_header_read`
(default off) that builds mm0 on the header read — **but it is gated on
`max_zoom < layout_zoom` (layout-mismatch only)**, which we now know is the WRONG
gate (the green tiles are plain z16, not layout-mismatch). The gate must be
**replaced with a location/proximity gate**, not layout-mismatch.

---

## How to reproduce / verify (no X-Plane code needed)

1. Get aircraft lat/lon from the latest `STATS:` line in
   `~/.autoortho-data/logs/autoortho.log` (`ac_lat`, `ac_lon`).
2. Convert to web-mercator tile (standard slippy-map math) at the rendered zoom;
   tile-block = `(row//16*16)_(col//16*16)`.
3. Read the served DDS from the FUSE mount and do a 35 KB read at offset 0:
   ```
   dd if=".../z_ao_<region>/textures/{row}_{col}_BI{zoom}.dds" of=/tmp/h.bin bs=35840 count=1
   ```
4. Skip 128-byte header, scan 8-byte BC1 blocks for `6642664200000000`
   (= missing_color RGB 66,77,55). A fresh tile returns ~100% green in that slice.
5. Full decode for visual confirmation: BC1 = 8-byte blocks, 2×RGB565 endpoints +
   2-bit indices; mip offsets walk `128 + Σ(bw²·8)`. numpy is in the repo `.venv`.

---

## Key code locations

- `autoortho/getortho.py:8209` — header-skip short-circuit (serves green for offset=0)
- `autoortho/getortho.py:8077` — `is_pure_mipmap_request = offset > 0` (mm0 build gate)
- `autoortho/getortho.py:8086-8092` — mm0/aopipeline build condition
- `autoortho/getortho.py:7959` — disk-cache check (runs before header-skip)
- `autoortho/getortho.py:~8969` — `dds.read()` that returns the green buffer bytes
- `autoortho/getortho.py:8425` — `_build_all_mipmaps_from_mm0` (existing build path)
- `autoortho/getortho.py:3735` — `SpatialPrefetcher._prefetch_tile` (proximity logic)
- `autoortho/pydds.py` — `get_fallback_bytes` returns missing_color for unbuilt mm regions
