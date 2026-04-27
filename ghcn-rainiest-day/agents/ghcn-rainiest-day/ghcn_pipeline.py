"""Global rainiest-day-ever pipeline over NOAA GHCN-Daily.

Map phase   -- one remote task per year-file (`YYYY.csv.gz`). Each worker
               streams the file, filters PRCP, and emits:
                 * top-100 single-day rainfall rows (for the leaderboard)
                 * per-country totals   (for the climatology by decade)
               into `/workspace/shared/ghcn/parts/{year}.json`.

Reduce phase -- one remote task that merges every per-year part:
                 * global top-500 + per-station best
                 * rainiest / driest country per decade
                 * polished Leaflet map (`map.html`)
                 * `country_decade_stats.csv` with every country-decade row

Run on Burla (v1.4.5 per `~/.burla/joeyper23/user_config.json`):

    python ../burla-agent-starter-kit/onboard.py --email joeyper23@gmail.com
    python ../burla-agent-starter-kit/run_job.py --email joeyper23@gmail.com ghcn_pipeline.py

Env vars:
    GHCN_START_YEAR / GHCN_END_YEAR    narrow the year range
    REDUCE_ONLY=1                      skip map, reduce over existing parts
"""

from __future__ import annotations

import os

# Disable Burla's telemetry for dev/test runs (must be set before importing burla).
os.environ.setdefault("DISABLE_BURLA_TELEMETRY", "True")

import csv
import gzip
import heapq
import io
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import folium  # noqa: F401  # top-level so Burla's dep-detector installs it on workers
import requests  # noqa: F401

BY_YEAR_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"
STATIONS_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
COUNTRIES_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-countries.txt"

TOP_PER_YEAR = 100
TOP_GLOBAL = 500
MAP_TOP_N = 100  # distinct stations (leaderboard is Koumac-heavy; dedup before plotting)
MIN_OBS_PER_COUNTRY_DECADE = 1_000  # filter micro-sampled country-decades from rankings
MIN_STATIONS_PER_COUNTRY_DECADE = 3


# ---------------------------------------------------------------------------
# shared-filesystem layout
# ---------------------------------------------------------------------------

def _shared_dir() -> Path:
    p = Path(os.environ.get("SHARED_DIR", "/workspace/shared"))
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


# ---------------------------------------------------------------------------
# MAP phase: one YYYY.csv.gz -> top-100 + per-country stats
# ---------------------------------------------------------------------------

def _stream_year_rows(year: int) -> Iterable[list]:
    """Stream-download + gunzip + csv-parse YYYY.csv.gz, yielding row lists.

    Uses `requests` so SSL validation works on macOS (certifi) and inside the
    Burla worker image.
    """
    url = BY_YEAR_URL.format(year=year)
    headers = {"User-Agent": "ghcn-rainiest-day/1.0 (+burla demo)"}
    with requests.get(url, stream=True, timeout=300, headers=headers) as resp:
        resp.raise_for_status()
        resp.raw.decode_content = False
        with gzip.GzipFile(fileobj=resp.raw) as gz:
            text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
            yield from csv.reader(text)


def process_year(year: int) -> str:
    """Scan one YYYY.csv.gz once and emit the year's part JSON.

    Part schema:
        {
          "year": int, "ok": bool,
          "rows_seen": int,       # every CSV row, all elements
          "prcp_valid": int,      # PRCP rows surviving the filter
          "elapsed_s": float,
          "top": [ {station_id, date, prcp_mm, mflag, sflag, obs_time} ],
          "country_stats": {
              "<CC>": { "total_mm": float, "obs_days": int, "n_stations": int }
          }
        }

    Picklable top-level function; no closures (required by Burla).
    """
    t0 = time.time()
    rows_seen = 0
    prcp_valid = 0
    heap: List[tuple] = []

    # per-country aggregation: {cc: [total_mm, obs_days, set_of_sids]}
    country: dict = {}

    try:
        for row in _stream_year_rows(year):
            rows_seen += 1
            if len(row) < 4 or row[2] != "PRCP":
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

            cc = sid[:2]
            agg = country.get(cc)
            if agg is None:
                agg = [0.0, 0, set()]
                country[cc] = agg
            agg[0] += prcp_mm
            agg[1] += 1
            agg[2].add(sid)

            item = (prcp_mm, sid, date, mflag or "", sflag or "", obs_time or "")
            if len(heap) < TOP_PER_YEAR:
                heapq.heappush(heap, item)
            elif prcp_mm > heap[0][0]:
                heapq.heapreplace(heap, item)
    except Exception as exc:
        return _write_part(year, {
            "year": year, "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "rows_seen": rows_seen, "prcp_valid": prcp_valid,
            "top": [], "country_stats": {},
        })

    top_sorted = sorted(heap, key=lambda x: x[0], reverse=True)
    top = [{
        "station_id": t[1], "date": t[2], "prcp_mm": t[0],
        "mflag": t[3], "sflag": t[4], "obs_time": t[5],
    } for t in top_sorted]

    country_stats = {
        cc: {
            "total_mm": round(agg[0], 1),
            "obs_days": agg[1],
            "n_stations": len(agg[2]),
        }
        for cc, agg in country.items()
    }

    part = {
        "year": year, "ok": True,
        "rows_seen": rows_seen, "prcp_valid": prcp_valid,
        "elapsed_s": round(time.time() - t0, 2),
        "top": top,
        "country_stats": country_stats,
    }
    path = _write_part(year, part)
    print(
        f"year={year} rows_seen={rows_seen:,} prcp_valid={prcp_valid:,} "
        f"countries={len(country_stats)} top1={top[0]['prcp_mm'] if top else 'n/a'} "
        f"elapsed_s={part['elapsed_s']}"
    )
    return path


def _write_part(year: int, obj: dict) -> str:
    path = _parts_dir() / f"{year}.json"
    path.write_text(json.dumps(obj))
    return str(path)


# ---------------------------------------------------------------------------
# station gazetteer: bundled snapshot -> /workspace/shared/ghcn/meta -> NOAA
# ---------------------------------------------------------------------------

def _find_local_meta(name: str) -> "Path | None":
    """Return a path to a bundled/staged copy of `name`, or None.

    Lookup order:
      1. <script_dir>/data/<name>             (committed snapshot)
      2. /workspace/shared/ghcn/meta/<name>   (staged once per Burla job)
    """
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parent / "data" / name)
    except NameError:
        pass
    candidates.append(Path("/workspace/shared/ghcn/meta") / name)
    for c in candidates:
        try:
            if c.exists() and c.stat().st_size > 0:
                return c
        except OSError:
            continue
    return None


def _stage_meta_to_shared(payload: dict) -> str:
    """Write `{filename: bytes}` into /workspace/shared/ghcn/meta/ on a worker."""
    meta_dir = Path("/workspace/shared/ghcn/meta")
    meta_dir.mkdir(parents=True, exist_ok=True)
    wrote = []
    for name, data in payload.items():
        dst = meta_dir / name
        dst.write_bytes(data)
        wrote.append(f"{name} ({dst.stat().st_size} bytes)")
    return "staged: " + ", ".join(wrote)


def _load_countries() -> dict:
    """Return {country_code: country_name}. Prefers bundled snapshot, then NOAA."""
    local = _find_local_meta("ghcnd-countries.txt")
    if local is not None:
        text = local.read_text(encoding="utf-8", errors="replace")
    else:
        r = requests.get(COUNTRIES_URL, timeout=120,
                         headers={"User-Agent": "ghcn-rainiest-day/1.0"})
        r.raise_for_status()
        text = r.text
    out = {}
    for line in text.splitlines():
        if len(line) >= 3:
            out[line[0:2]] = line[3:].strip()
    return out


def _load_stations_inline() -> dict:
    """Return {sid: {name, lat, lon, elev_m, state, country_code, country}}.

    Prefers the bundled snapshot and only streams from NOAA as a fallback.
    Self-contained so it pickles cleanly when shipped to a Burla worker.
    """
    countries = _load_countries()

    local = _find_local_meta("ghcnd-stations.txt")
    if local is not None:
        lines = local.read_text(encoding="utf-8", errors="replace").splitlines()
    else:
        resp = requests.get(
            STATIONS_URL, stream=True, timeout=300,
            headers={"User-Agent": "ghcn-rainiest-day/1.0"},
        )
        resp.raise_for_status()
        lines = resp.iter_lines(decode_unicode=True)

    out = {}
    for raw in lines:
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
        out[sid] = {
            "station_id": sid, "name": name,
            "lat": lat, "lon": lon, "elev_m": elev,
            "state": state or None,
            "country_code": sid[:2],
            "country": countries.get(sid[:2], sid[:2]),
        }
    return out


# ---------------------------------------------------------------------------
# REDUCE phase
# ---------------------------------------------------------------------------

def reduce_years(part_paths: List[str]) -> str:
    """Merge every per-year part and write the final artifacts.

    If `part_paths` is empty the worker globs existing parts off the shared FS
    (useful for REDUCE_ONLY re-runs that reuse a prior map phase).
    """
    t0 = time.time()
    if not part_paths:
        part_paths = sorted(str(p) for p in _parts_dir().glob("*.json"))
        print(f"reduce: globbed {len(part_paths)} existing parts from {_parts_dir()}")

    total_rows = 0
    total_valid = 0
    failed_years: List[dict] = []
    heap: List[tuple] = []

    # decade_stats[(cc, decade)] = [total_mm, obs_days, station_years]
    decade_stats: dict = {}

    for p in part_paths:
        try:
            part = json.loads(Path(p).read_text())
        except Exception as exc:
            failed_years.append({"path": p, "error": f"read:{exc}"})
            continue
        if not part.get("ok"):
            failed_years.append({"year": part.get("year"),
                                 "error": part.get("error", "unknown")})
            continue

        total_rows += part.get("rows_seen", 0)
        total_valid += part.get("prcp_valid", 0)
        year = part.get("year")

        for row in part.get("top", []):
            item = (row["prcp_mm"], row["station_id"], row["date"], year,
                    row.get("mflag", ""), row.get("sflag", ""), row.get("obs_time", ""))
            if len(heap) < TOP_GLOBAL:
                heapq.heappush(heap, item)
            elif row["prcp_mm"] > heap[0][0]:
                heapq.heapreplace(heap, item)

        if year is not None:
            decade = (year // 10) * 10
            for cc, stats in part.get("country_stats", {}).items():
                key = (cc, decade)
                agg = decade_stats.get(key)
                if agg is None:
                    agg = [0.0, 0, 0]
                    decade_stats[key] = agg
                agg[0] += stats.get("total_mm", 0.0)
                agg[1] += stats.get("obs_days", 0)
                agg[2] += stats.get("n_stations", 0)

    leaderboard = sorted(heap, key=lambda x: x[0], reverse=True)
    print(f"reduce: loading station metadata ...")
    station_table = _load_stations_inline()
    print(f"reduce: stations loaded ({len(station_table):,})")
    country_names = {m["country_code"]: (m.get("country") or m["country_code"])
                     for m in station_table.values()}

    enriched = [_enrich(i + 1, t, station_table) for i, t in enumerate(leaderboard)]

    results = _results_dir()
    top_result: Optional[dict] = enriched[0] if enriched else None
    if top_result:
        top_out = {
            **top_result,
            "source": "NOAA GHCN-Daily (by_year CSVs, quality-filtered)",
            "citation": ("Menne et al. 2012, J. Atmos. Oceanic Tech. 29, 897-910; "
                         "dataset DOI 10.7289/V5D21VHZ"),
            "note": ("Largest single-day PRCP in NOAA's global station network. "
                     "Quality flag (Q-FLAG) empty, -9999 sentinels dropped, single-day "
                     "PRCP only (multiday MDPR totals excluded)."),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        (results / "top_result.json").write_text(json.dumps(top_out, indent=2))

    _write_leaderboard_csv(results / "top_500.csv", enriched)
    _write_leaderboard_csv(results / "top_by_station.csv", _best_per_station(enriched))

    decade_rows = _build_decade_rows(decade_stats, country_names)
    _write_country_decade_csv(results / "country_decade_stats.csv", decade_rows)

    rainy_top = _rank_by_decade(decade_rows, descending=True, k=1)
    dry_top = _rank_by_decade(decade_rows, descending=False, k=1)
    _write_decade_markdown(results / "rainiest_by_decade.md",
                           "Rainiest country per decade (mean mm per reporting station-day)",
                           rainy_top)
    _write_decade_markdown(results / "driest_by_decade.md",
                           "Driest country per decade (mean mm per reporting station-day)",
                           dry_top)
    _write_decade_csv(results / "rainiest_by_decade.csv", rainy_top)
    _write_decade_csv(results / "driest_by_decade.csv", dry_top)

    _render_map(_best_per_station(enriched), decade_rows, results / "map.html")

    summary = {
        "total_rows_scanned": total_rows,
        "total_prcp_valid_rows": total_valid,
        "year_files_processed": len(part_paths) - len(failed_years),
        "year_files_failed": len(failed_years),
        "failed_years": failed_years[:20],
        "top_result": top_result,
        "n_country_decade_rows": len(decade_rows),
        "elapsed_s": round(time.time() - t0, 2),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    (results / "run_summary.json").write_text(json.dumps(summary, indent=2))

    if top_result:
        print(
            f"HEADLINE: {top_result['prcp_mm']} mm at "
            f"{top_result.get('name') or top_result['station_id']}, "
            f"{top_result.get('country') or top_result.get('country_code')} "
            f"on {top_result['date']}"
        )
    print(
        f"summary: rows_scanned={total_rows:,} prcp_valid={total_valid:,} "
        f"years_ok={summary['year_files_processed']} "
        f"country_decades={len(decade_rows)} elapsed_s={summary['elapsed_s']}"
    )
    return str(results)


# ---------------------------------------------------------------------------
# reduce helpers
# ---------------------------------------------------------------------------

def _enrich(rank: int, t: tuple, station_table: dict) -> dict:
    sid = t[1]
    meta = station_table.get(sid, {}) or {}
    return {
        "rank": rank, "station_id": sid,
        "name": meta.get("name"),
        "country": meta.get("country"),
        "country_code": meta.get("country_code"),
        "state": meta.get("state"),
        "lat": meta.get("lat"), "lon": meta.get("lon"),
        "elev_m": meta.get("elev_m"),
        "date": t[2], "prcp_mm": t[0],
        "year_file": t[3],
        "mflag": t[4], "sflag": t[5], "obs_time": t[6],
    }


def _best_per_station(rows: List[dict]) -> List[dict]:
    seen = set()
    out: List[dict] = []
    for r in rows:
        if r["station_id"] in seen:
            continue
        seen.add(r["station_id"])
        out.append({**r})
    for i, r in enumerate(out):
        r["rank"] = i + 1
    return out


def _write_leaderboard_csv(path: Path, rows: List[dict]) -> None:
    cols = ["rank", "station_id", "name", "country", "country_code", "state",
            "lat", "lon", "elev_m", "date", "prcp_mm", "year_file",
            "mflag", "sflag", "obs_time"]
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([
                r["rank"], r["station_id"],
                r.get("name") or "", r.get("country") or "",
                r.get("country_code") or "", r.get("state") or "",
                "" if r.get("lat") is None else r["lat"],
                "" if r.get("lon") is None else r["lon"],
                "" if r.get("elev_m") is None else r["elev_m"],
                r.get("date") or "", r.get("prcp_mm"),
                r.get("year_file") or "",
                r.get("mflag") or "", r.get("sflag") or "", r.get("obs_time") or "",
            ])


def _build_decade_rows(decade_stats: dict, country_names: dict) -> List[dict]:
    """Turn `{(cc, decade): [total_mm, obs_days, station_years]}` into sortable rows."""
    rows = []
    for (cc, decade), (total_mm, obs_days, station_years) in decade_stats.items():
        if obs_days <= 0:
            continue
        mean_mm_per_obs = total_mm / obs_days
        rows.append({
            "decade": decade,
            "country_code": cc,
            "country": country_names.get(cc, cc),
            "total_prcp_mm": round(total_mm, 1),
            "obs_days": obs_days,
            "station_years": station_years,
            "mean_mm_per_obs_day": round(mean_mm_per_obs, 3),
            "projected_annual_mm": round(mean_mm_per_obs * 365.0, 1),
        })
    rows.sort(key=lambda r: (r["decade"], -r["mean_mm_per_obs_day"]))
    return rows


def _rank_by_decade(rows: List[dict], descending: bool, k: int = 1) -> List[dict]:
    """For each decade, pick the top-k by mean_mm_per_obs_day with QC filters."""
    by_dec: dict = {}
    for r in rows:
        if r["obs_days"] < MIN_OBS_PER_COUNTRY_DECADE:
            continue
        if r["station_years"] < MIN_STATIONS_PER_COUNTRY_DECADE:
            continue
        by_dec.setdefault(r["decade"], []).append(r)
    out = []
    for decade in sorted(by_dec):
        ranked = sorted(by_dec[decade],
                        key=lambda r: r["mean_mm_per_obs_day"],
                        reverse=descending)
        out.extend(ranked[:k])
    return out


def _write_country_decade_csv(path: Path, rows: List[dict]) -> None:
    cols = ["decade", "country_code", "country", "total_prcp_mm", "obs_days",
            "station_years", "mean_mm_per_obs_day", "projected_annual_mm"]
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])


def _write_decade_csv(path: Path, rows: List[dict]) -> None:
    cols = ["decade", "country", "country_code", "mean_mm_per_obs_day",
            "projected_annual_mm", "total_prcp_mm", "obs_days", "station_years"]
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])


def _write_decade_markdown(path: Path, title: str, rows: List[dict]) -> None:
    lines = [
        f"# {title}",
        "",
        "| Decade | Country | Mean mm/reporting-station-day | Projected annual mm | Obs-days | Station-years |",
        "|---:|:---|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['decade']}s | {r['country']} | {r['mean_mm_per_obs_day']:.2f} | "
            f"{r['projected_annual_mm']:,.0f} | {r['obs_days']:,} | {r['station_years']:,} |"
        )
    lines.append("")
    lines.append(
        f"Filters: country-decade must have >= {MIN_OBS_PER_COUNTRY_DECADE:,} observation-days "
        f"and >= {MIN_STATIONS_PER_COUNTRY_DECADE} station-years."
    )
    path.write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# map rendering
# ---------------------------------------------------------------------------

_MAP_TITLE_HTML = """
<div style="
    position: fixed; top: 12px; left: 50%; transform: translateX(-50%);
    z-index: 9999;
    background: rgba(255,255,255,0.96);
    backdrop-filter: blur(6px);
    color: #0f172a;
    padding: 10px 18px; border-radius: 10px;
    box-shadow: 0 8px 20px rgba(15, 23, 42, 0.18);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    font-size: 14px;
    max-width: 90%;
">
    <div style="font-weight: 700; font-size: 15px;">Global Rainiest Days Ever</div>
    <div style="color: #475569; margin-top: 2px;">
        Top {n} distinct stations by wettest single day from NOAA GHCN-Daily
        &nbsp;&middot;&nbsp; {rows_human} PRCP observations across {years} year-files
    </div>
</div>
"""

_MAP_LEGEND_HTML = """
<div style="
    position: fixed; bottom: 18px; left: 18px; z-index: 9999;
    background: rgba(255,255,255,0.96);
    color: #0f172a;
    padding: 10px 14px; border-radius: 10px;
    box-shadow: 0 4px 14px rgba(15, 23, 42, 0.15);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    font-size: 12px; line-height: 1.6;
">
    <div style="font-weight: 700; margin-bottom: 6px;">Rank (by station)</div>
    <div><span style="display:inline-block;width:14px;height:14px;border-radius:50%;background:#7f1d1d;border:2px solid #fff;vertical-align:middle;"></span> &nbsp; #1</div>
    <div><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#dc2626;border:2px solid #fff;vertical-align:middle;"></span> &nbsp; #2-10</div>
    <div><span style="display:inline-block;width:11px;height:11px;border-radius:50%;background:#ea580c;border:2px solid #fff;vertical-align:middle;"></span> &nbsp; #11-25</div>
    <div><span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:#f59e0b;border:2px solid #fff;vertical-align:middle;"></span> &nbsp; #26-50</div>
    <div><span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:#eab308;border:2px solid #fff;vertical-align:middle;"></span> &nbsp; #51-100</div>
</div>
"""


def _rank_color(rank: int) -> str:
    if rank == 1:
        return "#7f1d1d"
    if rank <= 10:
        return "#dc2626"
    if rank <= 25:
        return "#ea580c"
    if rank <= 50:
        return "#f59e0b"
    return "#eab308"


def _rank_radius(rank: int) -> int:
    if rank == 1:
        return 18
    if rank <= 10:
        return 13
    if rank <= 25:
        return 10
    if rank <= 50:
        return 8
    return 6


def _fmt_date(yyyymmdd: str) -> str:
    if not yyyymmdd or len(yyyymmdd) < 8:
        return yyyymmdd or ""
    return f"{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"


def _clean_name(name: str | None) -> str:
    if not name:
        return "(unnamed station)"
    # Strip NOAA's fixed-width trailing paren fragments like "KOUMAC (NLLE-CALEDO"
    n = name.strip()
    if n.count("(") > n.count(")"):
        n = n[: n.rfind("(")].strip()
    return n or name.strip()


def _render_map(enriched: List[dict], decade_rows: List[dict], out_path: Path) -> None:
    """Render a polished single-file Leaflet map of the top MAP_TOP_N rows."""
    top = [r for r in enriched[:MAP_TOP_N]
           if r.get("lat") is not None and r.get("lon") is not None]

    if not top:
        out_path.write_text("<!-- no rows with coordinates to map -->")
        return

    m = folium.Map(
        location=(15, 10), zoom_start=2, min_zoom=2, max_zoom=10,
        tiles="CartoDB positron",
        prefer_canvas=True,
        control_scale=True,
    )
    folium.TileLayer("CartoDB dark_matter", name="Dark", show=False).add_to(m)
    folium.TileLayer("OpenStreetMap", name="OSM", show=False).add_to(m)

    fg = folium.FeatureGroup(
        name=f"Top {len(top)} distinct stations",
        show=True,
    ).add_to(m)

    for r in top:
        name = _clean_name(r.get("name"))
        inches = (r["prcp_mm"] or 0) / 25.4
        popup_html = f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
                    font-size: 13px; min-width: 220px;">
            <div style="color: #64748b; font-size: 11px; letter-spacing: 0.04em;">RANK</div>
            <div style="font-size: 22px; font-weight: 700; color: #0f172a;">#{r['rank']}</div>
            <div style="margin-top: 6px; font-size: 18px; font-weight: 600; color: #b91c1c;">
                {r['prcp_mm']:.1f} mm <span style="color:#64748b;font-weight:400;font-size:13px;">({inches:.1f} in)</span>
            </div>
            <div style="margin-top: 8px; color: #0f172a;"><b>{name}</b></div>
            <div style="color: #334155;">{r.get('country') or r.get('country_code') or ''}</div>
            <div style="margin-top: 6px; color: #475569; font-size: 12px;">
                {_fmt_date(r.get('date') or '')}
            </div>
            <div style="margin-top: 2px; color: #94a3b8; font-size: 11px;">
                station <code style="background:#f1f5f9;padding:1px 4px;border-radius:3px;">{r['station_id']}</code>
            </div>
        </div>
        """
        tooltip_html = (
            f"<div style=\"font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',"
            f" Helvetica, Arial, sans-serif; font-size: 12px; line-height: 1.45;\">"
            f"<div style=\"color:#64748b;font-size:10px;letter-spacing:0.05em;\">RANK</div>"
            f"<div style=\"font-weight:700;color:#0f172a;\">#{r['rank']} &nbsp; "
            f"<span style=\"color:#b91c1c;\">{r['prcp_mm']:.1f} mm</span> "
            f"<span style=\"color:#64748b;font-weight:400;\">({inches:.1f} in)</span></div>"
            f"<div style=\"color:#334155;margin-top:2px;\">{_fmt_date(r.get('date') or '')}</div>"
            f"<div style=\"color:#475569;\">{name}</div>"
            f"</div>"
        )
        folium.CircleMarker(
            location=(r["lat"], r["lon"]),
            radius=_rank_radius(r["rank"]),
            color=_rank_color(r["rank"]),
            weight=2,
            fill=True,
            fill_color=_rank_color(r["rank"]),
            fill_opacity=0.78,
            opacity=1.0,
            popup=folium.Popup(popup_html, max_width=340),
            tooltip=folium.Tooltip(tooltip_html, sticky=True),
        ).add_to(fg)

    lats = [r["lat"] for r in top]
    lons = [r["lon"] for r in top]
    m.fit_bounds(
        [[max(min(lats) - 8, -70), max(min(lons) - 8, -170)],
         [min(max(lats) + 8, 75), min(max(lons) + 8, 185)]],
        padding=(30, 30),
    )

    decades_spanned = len({r["decade"] for r in decade_rows}) if decade_rows else 0
    years_approx = decades_spanned * 10 if decades_spanned else "?"
    total_prcp_rows = sum(r["obs_days"] for r in decade_rows)
    rows_human = _short_count(total_prcp_rows) if total_prcp_rows else f"{len(enriched):,}"
    title = _MAP_TITLE_HTML.format(n=len(top), rows_human=rows_human, years=years_approx)
    m.get_root().html.add_child(folium.Element(title))
    m.get_root().html.add_child(folium.Element(_MAP_LEGEND_HTML))

    folium.LayerControl(position="topright", collapsed=True).add_to(m)
    m.save(str(out_path))


def _short_count(n: int) -> str:
    """Human-readable big-number: 1.09B, 3.2M, etc."""
    if n >= 1_000_000_000:
        return f"{n/1e9:.2f}B"
    if n >= 1_000_000:
        return f"{n/1e6:.1f}M"
    if n >= 1_000:
        return f"{n/1e3:.1f}k"
    return f"{n}"


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """Drive the full Burla run (stage -> map -> reduce)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from burla import remote_parallel_map  # type: ignore

    reduce_only = os.environ.get("REDUCE_ONLY", "").strip() not in ("", "0", "false", "False")

    bundled_dir = Path(__file__).resolve().parent / "data"
    stations_bundle = bundled_dir / "ghcnd-stations.txt"
    countries_bundle = bundled_dir / "ghcnd-countries.txt"
    if stations_bundle.exists() and countries_bundle.exists():
        try:
            print(
                f"staging bundled station snapshot "
                f"({stations_bundle.stat().st_size:,} + {countries_bundle.stat().st_size:,} bytes) "
                f"to /workspace/shared/ghcn/meta/ ..."
            )
            list(remote_parallel_map(
                _stage_meta_to_shared,
                [{
                    stations_bundle.name: stations_bundle.read_bytes(),
                    countries_bundle.name: countries_bundle.read_bytes(),
                }],
                func_cpu=1, func_ram=4,
            ))
        except Exception as exc:
            print(f"stage skipped ({exc}); reduce will fall back to NOAA download")
    else:
        print(f"no bundled snapshot at {bundled_dir}; reduce will fall back to NOAA")

    if reduce_only:
        print("REDUCE_ONLY=1: skipping map phase")
        reduce_input: list = []
    else:
        start_year = int(os.environ.get("GHCN_START_YEAR", "1750"))
        end_year = int(os.environ.get("GHCN_END_YEAR",
                                      str(datetime.now(timezone.utc).year)))
        years = list(range(start_year, end_year + 1))
        print(f"submitting {len(years)} years ({start_year}..{end_year}) to Burla")
        part_paths = remote_parallel_map(process_year, years, func_cpu=1, func_ram=4)
        print(f"map done. parts returned: {len(part_paths)}")
        reduce_input = list(part_paths)

    results_dir = remote_parallel_map(
        reduce_years, [reduce_input], func_cpu=8, func_ram=32,
    )
    print(f"reduce done. results: {results_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
