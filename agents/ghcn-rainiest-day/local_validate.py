"""Local smoke test: run the map + reduce pipeline on a couple of years,
without Burla, with SHARED_DIR pointed at a local sandbox.

Usage:
    SHARED_DIR=./local_shared python local_validate.py
    SHARED_DIR=./local_shared python local_validate.py 1995 2000
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import ghcn_pipeline as gp
import stations as st


def main() -> int:
    years_raw = sys.argv[1:] or ["1995", "2000"]
    years = [int(y) for y in years_raw]

    os.environ.setdefault("SHARED_DIR", str(HERE / "local_shared"))
    os.environ.setdefault("GHCN_CACHE_DIR", str(HERE / "local_cache"))

    print(f"SHARED_DIR={os.environ['SHARED_DIR']}")
    print(f"GHCN_CACHE_DIR={os.environ['GHCN_CACHE_DIR']}")
    print(f"years={years}")

    print("== warming station metadata (one-time ~10MB download) ==")
    sample_id = "USW00023183"
    hit = st.lookup(sample_id)
    assert hit is not None, f"expected station {sample_id} to resolve"
    assert hit["name"].startswith("PHOENIX"), f"unexpected name for {sample_id}: {hit['name']}"
    print(f"stations ok. {sample_id} -> {hit['name']}, {hit['country']}")

    part_paths = []
    for y in years:
        print(f"== process_year({y}) ==")
        p = gp.process_year(y)
        data = json.loads(Path(p).read_text())
        assert data["ok"], f"year {y} failed: {data.get('error')}"
        assert data["rows_seen"] > 1000, f"year {y} suspiciously few rows: {data['rows_seen']}"
        assert data["prcp_valid"] > 0, f"year {y} zero valid PRCP"
        assert len(data["top"]) > 0, f"year {y} empty top"
        top1 = data["top"][0]
        assert top1["prcp_mm"] > 0, f"year {y} top1 is not > 0"
        assert top1["date"].startswith(str(y)), f"year {y} top1 date out of year: {top1['date']}"
        assert len(top1["station_id"]) == 11, f"year {y} bad station id: {top1['station_id']}"
        print(
            f"  ok. rows_seen={data['rows_seen']:,} prcp_valid={data['prcp_valid']:,} "
            f"top1={top1['prcp_mm']} mm at {top1['station_id']} on {top1['date']}"
        )
        part_paths.append(p)

    print("== reduce_years ==")
    results_dir = gp.reduce_years(part_paths)
    rd = Path(results_dir)
    top_result = json.loads((rd / "top_result.json").read_text())
    assert top_result["prcp_mm"] > 0, "top_result has no rainfall"
    assert top_result["station_id"], "top_result missing station"
    assert (rd / "top_500.csv").exists(), "top_500.csv not written"
    assert (rd / "map.html").exists(), "map.html not written"
    summary = json.loads((rd / "run_summary.json").read_text())
    print(f"  summary: {json.dumps(summary, indent=2)[:500]}")
    print(
        f"HEADLINE (local, {len(years)} yrs): {top_result['prcp_mm']} mm at "
        f"{top_result.get('name') or top_result['station_id']}, "
        f"{top_result.get('country') or top_result.get('country_code')} on "
        f"{top_result['date']}"
    )
    print("LOCAL_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
