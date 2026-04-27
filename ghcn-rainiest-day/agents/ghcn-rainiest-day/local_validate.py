"""Smoke test: run the map + reduce pipeline on a couple of years locally.

Drops SHARED_DIR onto a local sandbox so nothing touches Burla. Invokes the
same top-level functions that Burla calls remotely.

Usage:
    python local_validate.py                 # defaults to 1995 2000
    python local_validate.py 1995 2000 2020
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import ghcn_pipeline as gp


def main() -> int:
    years = [int(y) for y in (sys.argv[1:] or ["1995", "2000"])]

    os.environ.setdefault("SHARED_DIR", str(HERE / "local_shared"))
    print(f"SHARED_DIR={os.environ['SHARED_DIR']}")
    print(f"years={years}")

    print("== warming station metadata (bundled snapshot) ==")
    station_table = gp._load_stations_inline()
    assert len(station_table) > 100_000, f"unexpectedly small station table: {len(station_table)}"
    sample_id = "USW00023183"
    hit = station_table.get(sample_id)
    assert hit and hit["name"].startswith("PHOENIX"), f"unexpected name for {sample_id}: {hit}"
    print(f"  ok. {len(station_table):,} stations; {sample_id} -> {hit['name']}, {hit['country']}")

    part_paths = []
    for y in years:
        print(f"== process_year({y}) ==")
        p = gp.process_year(y)
        part = json.loads(Path(p).read_text())
        assert part["ok"], f"year {y} failed: {part.get('error')}"
        assert part["rows_seen"] > 1_000, f"year {y} too few rows: {part['rows_seen']}"
        assert part["prcp_valid"] > 0, f"year {y} zero PRCP"
        assert part["top"], f"year {y} empty top"
        assert part["country_stats"], f"year {y} empty country_stats"
        top1 = part["top"][0]
        assert top1["date"].startswith(str(y)), f"year {y} top1 date: {top1['date']}"
        cs = part["country_stats"]
        biggest_cc = max(cs, key=lambda c: cs[c]["obs_days"])
        print(
            f"  ok. rows_seen={part['rows_seen']:,} prcp_valid={part['prcp_valid']:,} "
            f"countries={len(cs)} top1={top1['prcp_mm']} mm at {top1['station_id']} "
            f"biggest={biggest_cc} ({cs[biggest_cc]['obs_days']:,} obs_days)"
        )
        part_paths.append(p)

    print("== reduce_years ==")
    results_dir = Path(gp.reduce_years(part_paths))
    for name in [
        "top_result.json", "top_500.csv", "top_by_station.csv", "map.html",
        "run_summary.json", "country_decade_stats.csv",
        "rainiest_by_decade.md", "rainiest_by_decade.csv",
        "driest_by_decade.md", "driest_by_decade.csv",
    ]:
        f = results_dir / name
        assert f.exists() and f.stat().st_size > 0, f"missing or empty: {f}"
        print(f"  {name:<32} {f.stat().st_size:>8,} bytes")

    top_result = json.loads((results_dir / "top_result.json").read_text())
    assert top_result["prcp_mm"] > 0
    print(
        f"HEADLINE (local, {len(years)} yrs): {top_result['prcp_mm']} mm at "
        f"{top_result.get('name') or top_result['station_id']}, "
        f"{top_result.get('country') or top_result.get('country_code')} on {top_result['date']}"
    )
    print("LOCAL_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
