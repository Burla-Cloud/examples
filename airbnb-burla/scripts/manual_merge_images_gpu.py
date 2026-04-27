"""Manually merge completed gpu_detect batch parquets when the orchestrator
hit the budget cap before reaching the merge step. Idempotent."""
from __future__ import annotations
import sys
import time
sys.path.insert(0, ".")

from dotenv import load_dotenv

import numpy as _np  # noqa: F401
import pandas as _pd  # noqa: F401
import pyarrow as _pa  # noqa: F401
import pyarrow.parquet as _pq  # noqa: F401

from src.config import IMAGES_GPU_PATH, SHARED_IMAGES_GPU, SHARED_ROOT
from src.lib.io import ensure_dir, register_src_for_burla, write_json
from src.tasks.image_tasks import MergeImagesGpuArgs, merge_images_gpu


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    from burla import remote_parallel_map

    shared_root = SHARED_IMAGES_GPU
    shared_merged = f"{SHARED_ROOT}/images_gpu.parquet"

    print(f"[manual_merge_images_gpu] merging {shared_root}/batch_*.parquet "
          f"-> {shared_merged}", flush=True)
    t0 = time.time()
    [merge] = remote_parallel_map(
        merge_images_gpu,
        [MergeImagesGpuArgs(shared_root=shared_root, output_path=shared_merged)],
        func_cpu=8, func_ram=64, max_parallelism=1, grow=True, spinner=True,
    )
    if not merge.get("ok"):
        print(f"[manual_merge_images_gpu] failed: {merge.get('error')}", flush=True)
        if merge.get("traceback"):
            print(merge["traceback"], flush=True)
        raise SystemExit(1)

    elapsed = time.time() - t0
    print(f"[manual_merge_images_gpu]   merged {merge['n_rows']:,} rows from "
          f"{merge['n_files']:,} batches in {elapsed:.1f}s", flush=True)

    ensure_dir(IMAGES_GPU_PATH.parent)
    manifest_path = IMAGES_GPU_PATH.with_suffix(".manifest.json")
    write_json(manifest_path, {
        "ok": True,
        "shared_path": shared_merged,
        "n_rows": int(merge["n_rows"]),
        "n_listings": int(merge["n_listings"]),
        "n_files": int(merge["n_files"]),
        "elapsed_seconds": elapsed,
        "completed_at": time.time(),
        "note": "manual_merge: orchestrator hit budget cap before built-in merge step",
    })
    print(f"[manual_merge_images_gpu] DONE. Manifest at {manifest_path}", flush=True)


if __name__ == "__main__":
    main()
