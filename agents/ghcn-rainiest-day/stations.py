"""GHCN-Daily station metadata loader.

Parses NOAA's `ghcnd-stations.txt` (fixed-width) and `ghcnd-countries.txt`
into in-memory dicts, and exposes a `lookup(station_id)` that returns
a small dict with name, lat, lon, elevation, country, state.

Public data. Spec per:
https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt (section IV)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import requests

STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
COUNTRIES_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt"


def _cache_dir() -> Path:
    d = Path(os.environ.get("GHCN_CACHE_DIR", "/tmp/ghcn-cache"))
    d.mkdir(parents=True, exist_ok=True)
    return d


def _download(url: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dest


def load_countries(force_refresh: bool = False) -> Dict[str, str]:
    """Return mapping FIPS-2 country code -> country name."""
    path = _cache_dir() / "ghcnd-countries.txt"
    if force_refresh and path.exists():
        path.unlink()
    _download(COUNTRIES_URL, path)
    out: Dict[str, str] = {}
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if len(line) < 3:
                continue
            code = line[0:2]
            name = line[3:].strip()
            if code and name:
                out[code] = name
    return out


def load_stations(force_refresh: bool = False) -> Dict[str, dict]:
    """Return mapping station_id -> metadata dict (name, lat, lon, elev, country, state)."""
    path = _cache_dir() / "ghcnd-stations.txt"
    if force_refresh and path.exists():
        path.unlink()
    _download(STATIONS_URL, path)
    countries = load_countries(force_refresh=False)

    out: Dict[str, dict] = {}
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if len(line) < 72:
                continue
            sid = line[0:11].strip()
            if len(sid) != 11:
                continue
            try:
                lat = float(line[12:20].strip())
                lon = float(line[21:30].strip())
            except ValueError:
                continue
            elev_raw = line[31:37].strip()
            try:
                elev = float(elev_raw)
            except ValueError:
                elev = None
            state = line[38:40].strip()
            name = line[41:71].strip()
            country = countries.get(sid[:2], sid[:2])
            out[sid] = {
                "station_id": sid,
                "name": name,
                "lat": lat,
                "lon": lon,
                "elev_m": elev,
                "state": state or None,
                "country_code": sid[:2],
                "country": country,
            }
    return out


_CACHE: Optional[Dict[str, dict]] = None


def lookup(station_id: str) -> Optional[dict]:
    """Return station metadata for one ID, loading+caching the full table on first call."""
    global _CACHE
    if _CACHE is None:
        _CACHE = load_stations()
    return _CACHE.get(station_id)
