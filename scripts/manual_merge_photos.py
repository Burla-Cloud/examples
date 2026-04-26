"""Manually merge whatever batch_*.parquet files made it into shared FS.

Run this after the s02a orchestrator was killed early. Saves a manifest with
the partial counts so downstream stages have the path they expect.
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from dotenv import load_dotenv

from src.config import (
    PHOTO_MANIFEST_PATH, SHARED_PHOTOS, SHARED_ROOT,
)
from src.lib.io import ensure_dir, register_src_for_burla, write_json
from src.tasks.scrape_tasks import MergePhotosArgs, merge_photo_batches


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    from burla import remote_parallel_map

    shared_root = SHARED_PHOTOS
    shared_merged = f"{SHARED_ROOT}/photo_manifest.parquet"

    print(f"[manual_merge] merging {shared_root}/batch_*.parquet -> {shared_merged}", flush=True)
    t0 = time.time()
    [merge] = remote_parallel_map(
        merge_photo_batches,
        [MergePhotosArgs(shared_root=shared_root, output_path=shared_merged)],
        func_cpu=8, func_ram=32, max_parallelism=1, grow=True, spinner=True,
    )
    if not merge.get("ok"):
        raise SystemExit(f"merge failed: {merge.get('error')}")
    elapsed = time.time() - t0
    print(f"[manual_merge]   merged {merge['n_rows']:,} photo rows across "
          f"{merge['n_listings']:,} listings from {merge['n_files']:,} batches "
          f"in {elapsed:.1f}s", flush=True)

    ensure_dir(PHOTO_MANIFEST_PATH.parent)
    manifest_path = PHOTO_MANIFEST_PATH.with_suffix(".manifest.json")
    write_json(manifest_path, {
        "ok": True,
        "shared_path": shared_merged,
        "n_rows": merge["n_rows"],
        "n_listings": merge["n_listings"],
        "n_files": merge["n_files"],
        "partial_run": True,
        "completed_at": time.time(),
    })
    print(f"[manual_merge] DONE. Manifest at {manifest_path}", flush=True)


if __name__ == "__main__":
    main()
