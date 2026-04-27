"""Stage 1: download + clean per-city listings into one parquet on shared FS.

Uses the validation report from Stage 0 to skip cities that didn't pass.
Each Burla worker writes its own parquet to /workspace/shared/airbnb/listings/,
then one reduce worker merges them into /workspace/shared/airbnb/listings_clean.parquet
and returns a small manifest (n_rows, schema, 50-row sample) we save locally.
The full merged parquet lives on /workspace/shared so later Burla stages can
read it without re-uploading anything.
"""
from __future__ import annotations

import re
import time

from dotenv import load_dotenv

from ..config import (
    LISTINGS_CLEAN_PATH, SHARED_LISTINGS, SHARED_ROOT,
    VALIDATION_REPORT_PATH,
)
from ..lib.budget import BudgetTracker
from ..lib.io import read_json, register_src_for_burla, write_json, ensure_dir, input_hash
from ..tasks.image_tasks import (
    DownloadCityArgs, download_and_clean_city,
    MergeListingsArgs, merge_listings_parquets,
)


def _city_slug(country: str, region: str, city: str, snapshot_date: str) -> str:
    raw = f"{country}__{region}__{city}__{snapshot_date}"
    return re.sub(r"[^A-Za-z0-9._-]+", "-", raw).strip("-").lower()


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    report = read_json(VALIDATION_REPORT_PATH)
    if not report:
        raise SystemExit(f"[s01] No validation report at {VALIDATION_REPORT_PATH}; run Stage 0 first.")
    passing = report.get("passing", [])
    if not passing:
        raise SystemExit("[s01] Validation report has zero passing cities; halting.")

    args_list = [
        DownloadCityArgs(
            city=r["city"],
            country=r["country"],
            region=r["region"],
            snapshot_date=r["snapshot_date"],
            listings_url=r["listings_url"],
            shared_root=SHARED_LISTINGS,
            city_slug=_city_slug(r["country"], r["region"], r["city"], r["snapshot_date"]),
        )
        for r in passing
    ]
    h = input_hash([a.city_slug for a in args_list])

    manifest_path = LISTINGS_CLEAN_PATH.with_suffix(".manifest.json")
    existing = read_json(manifest_path)
    if existing and existing.get("input_hash") == h and existing.get("ok"):
        print(f"[s01] manifest matches input_hash={h}; skipping (delete {manifest_path} to force).", flush=True)
        return

    from burla import remote_parallel_map

    n_cities = len(args_list)
    n_workers = min(120, n_cities)
    print(f"[s01] downloading {n_cities} city listings on {n_workers} parallel workers ...", flush=True)
    t0 = time.time()

    with BudgetTracker("s01_listings", n_inputs=n_cities, func_cpu=1) as bt:
        bt.set_workers(n_workers)
        results: list[dict] = remote_parallel_map(
            download_and_clean_city,
            args_list,
            func_cpu=1,
            func_ram=4,
            max_parallelism=n_workers,
            grow=True,
            spinner=True,
        )
        n_ok = sum(1 for r in results if r.get("ok"))
        n_rows = sum(int(r.get("n_rows", 0)) for r in results if r.get("ok"))
        bt.set_succeeded(n_ok)
        bt.set_failed(n_cities - n_ok)
        bt.note(rows_per_city=n_rows / max(1, n_ok), total_rows=n_rows)
        print(f"[s01]   wrote {n_rows:,} rows from {n_ok}/{n_cities} cities to {SHARED_LISTINGS}/", flush=True)
        if n_ok < n_cities:
            for r in results:
                if not r.get("ok"):
                    print(f"[s01]   FAIL {r.get('city')}: {r.get('error')}", flush=True)

    print("[s01] reducing per-city parquets into one ...", flush=True)
    shared_merged = f"{SHARED_ROOT}/listings_clean.parquet"
    [merge_result] = remote_parallel_map(
        merge_listings_parquets,
        [MergeListingsArgs(shared_root=SHARED_LISTINGS, output_path=shared_merged)],
        func_cpu=8,
        func_ram=32,
        max_parallelism=1,
        grow=True,
        spinner=True,
    )
    if not merge_result.get("ok"):
        raise SystemExit(f"[s01] merge failed: {merge_result.get('error')}")

    elapsed = time.time() - t0
    print(f"[s01]   merged {merge_result['n_rows']:,} rows from {merge_result['n_files']} files "
          f"in {elapsed:.1f}s", flush=True)

    ensure_dir(LISTINGS_CLEAN_PATH.parent)
    write_json(manifest_path, {
        "input_hash": h,
        "ok": True,
        "shared_path": shared_merged,
        "n_rows": merge_result["n_rows"],
        "n_cities": merge_result["n_cities"],
        "n_with_picture_url": merge_result.get("n_with_picture_url", 0),
        "schema": merge_result.get("schema", []),
        "sample_rows": merge_result.get("sample_rows", []),
        "n_workers": n_workers,
        "elapsed_seconds": elapsed,
        "completed_at": time.time(),
    })
    print(f"[s01] DONE. Manifest at {manifest_path}", flush=True)


if __name__ == "__main__":
    main()
