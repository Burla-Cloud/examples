"""Global rainiest-day-ever pipeline over NOAA GHCN-Daily.

Map:    one remote task per year-file (YYYY.csv.gz) -> top-100 PRCP rows for that year.
Reduce: one remote task that merges all per-year parts into a global top-500,
        joins station metadata (name/lat/lon/country), and writes share-ready
        artifacts to /workspace/shared/ghcn/results/.

Run locally:

    # 1) Two-year smoke test (no Burla)
    SHARED_DIR=./local_shared \
    python local_validate.py

Run on Burla (plus-dig.burla.dev, joeyper23 account):

    # 1) Ensure cluster is ready (idempotent). Uses the starter kit; the UI
    #    Start fallback will boot the cluster if grow-on-demand is unavailable.
    python ../burla-agent-starter-kit/onboard.py --email joeyper23@gmail.com

    # 2) Run the pipeline in the pinned per-account venv
    python ../burla-agent-starter-kit/run_job.py \
        --email joeyper23@gmail.com \
        ghcn_pipeline.py

Data: NOAA GHCN-Daily (Menne et al. 2012, DOI 10.7289/V5D21VHZ).
"""

from __future__ import annotations

import csv
import gzip
import heapq
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import folium  # noqa: F401  (top-level import so Burla auto-installs on workers)
import requests  # noqa: F401

BY_YEAR_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"
STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
COUNTRIES_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt"

TOP_PER_YEAR = 100
TOP_GLOBAL = 500


def _shared_dir() -> Path:
    """Resolve /workspace/shared with override for local validation."""
    root = os.environ.get("SHARED_DIR", "/workspace/shared")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _parts_dir() -> Path:
    p = _shared_dir() / "ghcn" / "parts"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _results_dir() -> Path:
    p = _shared_dir() / "ghcn" / "results"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _stream_year_rows(year: int) -> Iterable[list]:
    """Stream-download + gunzip + csv-parse YYYY.csv.gz, yielding row lists.

    Uses `requests` so SSL validation works out of the box on macOS (certifi
    CA bundle), inside the Burla worker image, and in CI.
    """
    import requests

    url = BY_YEAR_URL.format(year=year)
    headers = {"User-Agent": "ghcn-rainiest-day/1.0 (+burla demo)"}
    with requests.get(url, stream=True, timeout=300, headers=headers) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = False
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
            reader = csv.reader(text)
            for row in reader:
                yield row


def process_year(year: int) -> str:
    """Scan one YYYY.csv.gz, keep top TOP_PER_YEAR PRCP observations.

    Picklable, top-level, no closures (required by Burla).
    Writes /workspace/shared/ghcn/parts/{year}.json and returns that path.
    """
    t0 = time.time()
    rows_seen = 0
    prcp_valid = 0
    heap: List[tuple] = []

    try:
        for row in _stream_year_rows(year):
            rows_seen += 1
            if len(row) < 4:
                continue
            element = row[2]
            if element != "PRCP":
                continue
            qflag = row[5] if len(row) > 5 else ""
            if qflag:
                continue
            raw = row[3]
            if not raw or raw == "-9999":
                continue
            try:
                val_tenths = int(raw)
            except ValueError:
                continue
            if val_tenths < 0:
                continue
            prcp_mm = val_tenths / 10.0
            prcp_valid += 1
            sid = row[0]
            date = row[1]
            mflag = row[4] if len(row) > 4 else ""
            sflag = row[6] if len(row) > 6 else ""
            obs_time = row[7] if len(row) > 7 else ""
            item = (
                prcp_mm,
                sid,
                date,
                mflag or "",
                sflag or "",
                obs_time or "",
            )
            if len(heap) < TOP_PER_YEAR:
                heapq.heappush(heap, item)
            elif prcp_mm > heap[0][0]:
                heapq.heapreplace(heap, item)
    except Exception as exc:
        return _write_part(
            year,
            {
                "year": year,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "rows_seen": rows_seen,
                "prcp_valid": prcp_valid,
                "top": [],
            },
        )

    top_sorted = sorted(heap, key=lambda x: x[0], reverse=True)
    top = [
        {
            "station_id": t[1],
            "date": t[2],
            "prcp_mm": t[0],
            "mflag": t[3],
            "sflag": t[4],
            "obs_time": t[5],
        }
        for t in top_sorted
    ]
    part = {
        "year": year,
        "ok": True,
        "rows_seen": rows_seen,
        "prcp_valid": prcp_valid,
        "elapsed_s": round(time.time() - t0, 2),
        "top": top,
    }
    path = _write_part(year, part)
    print(
        f"year={year} rows_seen={rows_seen:,} prcp_valid={prcp_valid:,} "
        f"top1_mm={top[0]['prcp_mm'] if top else 'n/a'} elapsed_s={part['elapsed_s']}"
    )
    return path


def _write_part(year: int, obj: dict) -> str:
    path = _parts_dir() / f"{year}.json"
    path.write_text(json.dumps(obj))
    return str(path)


def _load_stations_inline() -> dict:
    """Download + parse ghcnd-stations.txt and ghcnd-countries.txt inline.

    Duplicated from stations.py so reduce_years is self-contained on remote
    workers (Burla only ships the pickled function, not sidecar modules).
    """
    import requests

    headers = {"User-Agent": "ghcn-rainiest-day/1.0 (+burla demo)"}

    countries: dict = {}
    with requests.get(COUNTRIES_URL, timeout=120, headers=headers) as r:
        r.raise_for_status()
        for line in r.text.splitlines():
            if len(line) < 3:
                continue
            code = line[0:2]
            name = line[3:].strip()
            if code and name:
                countries[code] = name

    stations: dict = {}
    with requests.get(STATIONS_URL, stream=True, timeout=300, headers=headers) as r:
        r.raise_for_status()
        for raw in r.iter_lines(decode_unicode=True):
            if not raw or len(raw) < 72:
                continue
            sid = raw[0:11].strip()
            if len(sid) != 11:
                continue
            try:
                lat = float(raw[12:20].strip())
                lon = float(raw[21:30].strip())
            except ValueError:
                continue
            try:
                elev = float(raw[31:37].strip())
            except ValueError:
                elev = None
            state = raw[38:40].strip()
            name = raw[41:71].strip()
            stations[sid] = {
                "station_id": sid,
                "name": name,
                "lat": lat,
                "lon": lon,
                "elev_m": elev,
                "state": state or None,
                "country_code": sid[:2],
                "country": countries.get(sid[:2], sid[:2]),
            }
    return stations


def reduce_years(part_paths: List[str]) -> str:
    """Merge per-year parts into a global top-TOP_GLOBAL leaderboard + artifacts.

    Picklable, top-level. Wrapped as a list-of-one when called via Burla so
    it runs as a single remote call. Self-contained: does not import any
    sibling modules (`stations.py` lives only for local_validate.py).

    If `part_paths` is empty, glob existing files from /workspace/shared/ghcn/parts/
    on the worker (useful for REDUCE_ONLY re-runs that reuse a prior map phase).
    """
    t0 = time.time()
    if not part_paths:
        part_paths = sorted(str(p) for p in _parts_dir().glob("*.json"))
        print(f"reduce: globbed {len(part_paths)} existing parts from {_parts_dir()}")
    total_rows = 0
    total_valid = 0
    failed_years: List[dict] = []
    heap: List[tuple] = []

    for p in part_paths:
        try:
            part = json.loads(Path(p).read_text())
        except Exception as exc:
            failed_years.append({"path": p, "error": f"read:{exc}"})
            continue
        if not part.get("ok"):
            failed_years.append(
                {"year": part.get("year"), "error": part.get("error", "unknown")}
            )
            continue
        total_rows += part.get("rows_seen", 0)
        total_valid += part.get("prcp_valid", 0)
        year = part.get("year")
        for row in part.get("top", []):
            item = (
                row["prcp_mm"],
                row["station_id"],
                row["date"],
                year,
                row.get("mflag", ""),
                row.get("sflag", ""),
                row.get("obs_time", ""),
            )
            if len(heap) < TOP_GLOBAL:
                heapq.heappush(heap, item)
            elif row["prcp_mm"] > heap[0][0]:
                heapq.heapreplace(heap, item)

    leaderboard = sorted(heap, key=lambda x: x[0], reverse=True)
    print(f"reduce: loading station metadata ({COUNTRIES_URL} + {STATIONS_URL}) ...")
    station_table = _load_stations_inline()
    print(f"reduce: stations loaded ({len(station_table):,})")

    def enrich(rank: int, t: tuple) -> dict:
        sid = t[1]
        meta = station_table.get(sid, {}) or {}
        return {
            "rank": rank,
            "station_id": sid,
            "name": meta.get("name"),
            "country": meta.get("country"),
            "country_code": meta.get("country_code"),
            "state": meta.get("state"),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "elev_m": meta.get("elev_m"),
            "date": t[2],
            "prcp_mm": t[0],
            "year_file": t[3],
            "mflag": t[4],
            "sflag": t[5],
            "obs_time": t[6],
        }

    enriched = [enrich(i + 1, t) for i, t in enumerate(leaderboard)]

    results = _results_dir()
    top_result_path = results / "top_result.json"
    top500_path = results / "top_500.csv"
    map_path = results / "map.html"
    summary_path = results / "run_summary.json"

    top_result: Optional[dict] = enriched[0] if enriched else None
    if top_result:
        top_result_out = {
            **top_result,
            "source": "NOAA GHCN-Daily (by_year CSVs, quality-filtered)",
            "citation": (
                "Menne et al. 2012, J. Atmos. Oceanic Tech. 29, 897-910; "
                "dataset DOI 10.7289/V5D21VHZ"
            ),
            "note": (
                "Largest single-day PRCP in NOAA's global station network. "
                "Station data only (not a complete gridded Earth sample). "
                "Rows with non-empty Q-FLAG and -9999 sentinels excluded. "
                "Single-day PRCP only (multiday MDPR totals excluded)."
            ),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        top_result_path.write_text(json.dumps(top_result_out, indent=2))

    _write_leaderboard_csv(top500_path, enriched)

    distinct = _best_per_station(enriched)
    distinct_path = results / "top_by_station.csv"
    _write_leaderboard_csv(distinct_path, distinct)

    _render_map(enriched[:25], map_path)

    summary = {
        "total_rows_scanned": total_rows,
        "total_prcp_valid_rows": total_valid,
        "year_files_processed": len(part_paths) - len(failed_years),
        "year_files_failed": len(failed_years),
        "failed_years": failed_years[:20],
        "top_result": top_result,
        "elapsed_s": round(time.time() - t0, 2),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    summary_path.write_text(json.dumps(summary, indent=2))

    if top_result:
        print(
            "HEADLINE: Largest daily rainfall in GHCN-Daily: "
            f"{top_result['prcp_mm']} mm at "
            f"{top_result.get('name') or top_result['station_id']}, "
            f"{top_result.get('country') or top_result.get('country_code')} "
            f"on {top_result['date']}"
        )
    print(
        f"summary: rows_scanned={total_rows:,} prcp_valid={total_valid:,} "
        f"years_ok={summary['year_files_processed']} "
        f"years_failed={summary['year_files_failed']} "
        f"elapsed_s={summary['elapsed_s']}"
    )
    return str(results)


def _write_leaderboard_csv(path: Path, rows: List[dict]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "rank",
                "station_id",
                "name",
                "country",
                "country_code",
                "state",
                "lat",
                "lon",
                "elev_m",
                "date",
                "prcp_mm",
                "year_file",
                "mflag",
                "sflag",
                "obs_time",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r["rank"],
                    r["station_id"],
                    r.get("name") or "",
                    r.get("country") or "",
                    r.get("country_code") or "",
                    r.get("state") or "",
                    "" if r.get("lat") is None else r["lat"],
                    "" if r.get("lon") is None else r["lon"],
                    "" if r.get("elev_m") is None else r["elev_m"],
                    r.get("date") or "",
                    r.get("prcp_mm"),
                    r.get("year_file") or "",
                    r.get("mflag") or "",
                    r.get("sflag") or "",
                    r.get("obs_time") or "",
                ]
            )


def _best_per_station(rows: List[dict]) -> List[dict]:
    """Keep only each station's best-ranked entry, then re-rank."""
    seen = set()
    out: List[dict] = []
    for r in rows:
        sid = r["station_id"]
        if sid in seen:
            continue
        seen.add(sid)
        out.append({**r})
    for i, r in enumerate(out):
        r["rank"] = i + 1
    return out


def _render_map(top_rows: List[dict], out_path: Path) -> None:
    """Render a single-file Leaflet map of the top rows via folium."""
    if not top_rows or top_rows[0].get("lat") is None:
        center = (0, 0)
    else:
        center = (top_rows[0]["lat"], top_rows[0]["lon"])
    m = folium.Map(location=center, zoom_start=3, tiles="OpenStreetMap")
    for r in top_rows:
        if r.get("lat") is None or r.get("lon") is None:
            continue
        color = "red" if r["rank"] == 1 else "blue"
        popup = (
            f"#{r['rank']} · {r['prcp_mm']} mm · {r['date']}<br>"
            f"{r.get('name') or r['station_id']}, {r.get('country') or ''}"
        )
        folium.CircleMarker(
            location=(r["lat"], r["lon"]),
            radius=8 if r["rank"] == 1 else 5,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup(popup, max_width=300),
        ).add_to(m)
    m.save(str(out_path))


def main() -> int:
    """Entry point for a full Burla run.

    v1.4.5 signature (per ~/.burla/joeyper23/user_config.json notes):
      remote_parallel_map(function_, inputs, func_cpu=1, func_ram=4,
                          detach=False, generator=False, spinner=True,
                          max_parallelism=None)
    There is no `grow=True` in this client version; cluster must already be ON.

    Env vars:
      GHCN_START_YEAR / GHCN_END_YEAR — narrow the year range
      REDUCE_ONLY=1 — skip map, reduce over existing /workspace/shared/ghcn/parts/
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from burla import remote_parallel_map  # type: ignore

    reduce_only = os.environ.get("REDUCE_ONLY", "").strip() not in ("", "0", "false", "False")

    if reduce_only:
        print("REDUCE_ONLY=1: skipping map phase; reduce worker will glob /workspace/shared/ghcn/parts/")
        reduce_input: list = []
    else:
        start_year = int(os.environ.get("GHCN_START_YEAR", "1750"))
        end_year = int(
            os.environ.get(
                "GHCN_END_YEAR",
                str(datetime.now(timezone.utc).year),
            )
        )
        years = list(range(start_year, end_year + 1))
        print(f"submitting {len(years)} years ({start_year}..{end_year}) to Burla")
        part_paths = remote_parallel_map(
            process_year,
            years,
            func_cpu=1,
            func_ram=4,
        )
        print(f"map done. parts returned: {len(part_paths)}")
        reduce_input = list(part_paths)

    results_dir = remote_parallel_map(
        reduce_years,
        [reduce_input],
        func_cpu=8,
        func_ram=32,
    )
    print(f"reduce done. results: {results_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
