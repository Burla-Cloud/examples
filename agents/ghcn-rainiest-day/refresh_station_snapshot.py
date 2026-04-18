#!/usr/bin/env python3
"""Refresh the bundled NOAA station metadata snapshot in `data/`.

Run this any time NOAA publishes a newer `ghcnd-stations.txt` and you want
the committed snapshot to match. It overwrites:

    data/ghcnd-stations.txt   (~11 MB)
    data/ghcnd-countries.txt  (~4 KB)

Usage:
    python refresh_station_snapshot.py
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import requests

URLS = {
    "ghcnd-stations.txt": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt",
    "ghcnd-countries.txt": "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt",
}


def main() -> int:
    out_dir = Path(__file__).resolve().parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, url in URLS.items():
        dst = out_dir / name
        print(f"downloading {url} -> {dst}")
        r = requests.get(url, timeout=300)
        r.raise_for_status()
        dst.write_bytes(r.content)
        h = hashlib.sha256(r.content).hexdigest()[:16]
        print(f"  wrote {dst.stat().st_size:,} bytes   sha256={h}")
    print("snapshot refreshed. don't forget to `git add data/ && git commit`.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
