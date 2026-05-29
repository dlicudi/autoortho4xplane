#!/usr/bin/env python3
"""Summarize AutoOrtho tile request geography over a time window."""

from __future__ import annotations

import argparse
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path


LOG_TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+")
TILE_RE = re.compile(r"tile=(?P<row>\d+)_(?P<col>\d+)_(?P<maptype>[A-Za-z]+)_(?P<zoom>\d+)")


def tile_center(row: int, col: int, zoom: int) -> tuple[float, float]:
    n = 2**zoom
    lon = (col + 0.5) / n * 360.0 - 180.0
    lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (row + 0.5) / n))))
    return lat, lon


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_nm = 3440.065
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * radius_nm * math.asin(math.sqrt(a))


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
            if fmt == "%H:%M:%S":
                today = datetime.today()
                parsed = parsed.replace(year=today.year, month=today.month, day=today.day)
            return parsed
        except ValueError:
            pass
    raise SystemExit(f"Unsupported time format: {value!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "log",
        nargs="?",
        default=str(Path.home() / ".autoortho-data/logs/autoortho.log"),
        help="AutoOrtho log path",
    )
    parser.add_argument("--since", help="Start time: 'YYYY-MM-DD HH:MM:SS' or 'HH:MM:SS'")
    parser.add_argument("--until", help="End time: 'YYYY-MM-DD HH:MM:SS' or 'HH:MM:SS'")
    parser.add_argument("--last-minutes", type=float, help="Analyze only the last N minutes in the log")
    parser.add_argument("--tail-lines", type=int, help="Analyze only the last N lines")
    parser.add_argument("--top", type=int, default=10, help="Number of buckets/tiles to print")
    args = parser.parse_args()

    path = Path(args.log).expanduser()
    lines = path.read_text(errors="ignore").splitlines()
    if args.tail_lines:
        lines = lines[-args.tail_lines :]

    parsed_lines: list[tuple[datetime, str]] = []
    latest_ts: datetime | None = None
    for line in lines:
        ts_match = LOG_TS_RE.match(line)
        if not ts_match:
            continue
        ts = datetime.strptime(ts_match.group("ts"), "%Y-%m-%d %H:%M:%S")
        latest_ts = ts if latest_ts is None or ts > latest_ts else latest_ts
        parsed_lines.append((ts, line))

    since = parse_time(args.since)
    until = parse_time(args.until)
    if args.last_minutes is not None:
        if latest_ts is None:
            raise SystemExit("No timestamped lines found")
        since = latest_ts - timedelta(minutes=args.last_minutes)
        until = latest_ts if until is None else until

    tile_counts: Counter[tuple[int, int, str, int]] = Counter()
    first_ts = None
    last_ts = None
    for ts, line in parsed_lines:
        if since and ts < since:
            continue
        if until and ts > until:
            continue
        match = TILE_RE.search(line)
        if not match:
            continue
        row = int(match.group("row"))
        col = int(match.group("col"))
        maptype = match.group("maptype")
        zoom = int(match.group("zoom"))
        tile_counts[(row, col, maptype, zoom)] += 1
        first_ts = ts if first_ts is None or ts < first_ts else first_ts
        last_ts = ts if last_ts is None or ts > last_ts else last_ts

    print(f"log: {path}")
    print(f"window: {first_ts} .. {last_ts}")
    print(f"unique tiles: {len(tile_counts)}")
    if not tile_counts:
        return 0

    by_zoom: dict[int, list[tuple[float, float, int, int, str, int]]] = defaultdict(list)
    for (row, col, maptype, zoom), count in tile_counts.items():
        lat, lon = tile_center(row, col, zoom)
        by_zoom[zoom].append((lat, lon, row, col, maptype, count))

    for zoom in sorted(by_zoom):
        points = by_zoom[zoom]
        lats = [p[0] for p in points]
        lons = [p[1] for p in points]
        center_lat = sum(lats) / len(lats)
        center_lon = sum(lons) / len(lons)
        diagonal_nm = haversine_nm(min(lats), min(lons), max(lats), max(lons))
        max_radius_nm = max(haversine_nm(center_lat, center_lon, lat, lon) for lat, lon, *_ in points)
        print(
            f"\nZL{zoom}: {len(points)} unique; "
            f"lat {min(lats):.4f}..{max(lats):.4f}; "
            f"lon {min(lons):.4f}..{max(lons):.4f}; "
            f"diagonal {diagonal_nm:.1f} nm; max radius {max_radius_nm:.1f} nm"
        )

        buckets = Counter((math.floor(lat), math.floor(lon)) for lat, lon, *_ in points)
        print("  top 1deg buckets:")
        for (lat_bucket, lon_bucket), count in buckets.most_common(args.top):
            print(f"    {lat_bucket:+03d},{lon_bucket:+04d}: {count}")

        print("  most repeated tiles:")
        for lat, lon, row, col, maptype, count in sorted(points, key=lambda p: p[5], reverse=True)[: args.top]:
            print(f"    {row}_{col}_{maptype}_{zoom}: {count} reads @ {lat:.4f},{lon:.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
