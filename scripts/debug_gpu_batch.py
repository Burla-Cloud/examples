"""Run a tiny Stage 3 GPU batch end-to-end and print results.

Used to verify the ultralytics fix before launching all 1504 batches.
"""
from __future__ import annotations
import sys
sys.path.insert(0, ".")

from dotenv import load_dotenv

import numpy as _np  # noqa: F401
import pandas as _pd  # noqa: F401
import pyarrow as _pa  # noqa: F401
import pyarrow.parquet as _pq  # noqa: F401

from src.config import (
    GPU_BATCH_SIZE, SHARED_IMAGES_GPU, SHARED_ROOT, TOP_N_PER_AXIS,
)
from src.lib.io import register_src_for_burla
from src.tasks.image_tasks import GpuImageBatchArgs, gpu_detect_image_batch
from src.stages.s03_images_gpu import select_top_k_images, TopKImagesArgs


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    from burla import remote_parallel_map

    images_cpu_path = f"{SHARED_ROOT}/images_cpu.parquet"
    [picked] = remote_parallel_map(
        select_top_k_images,
        [TopKImagesArgs(images_cpu_path=images_cpu_path, top_n_per_axis=TOP_N_PER_AXIS)],
        func_cpu=8, func_ram=64, max_parallelism=1, grow=True, spinner=True,
    )
    rows = picked["rows"][:32]
    print(f"selected {len(rows)} rows for the smoke test")

    batch = GpuImageBatchArgs(
        batch_id=999999,
        rows=rows,
        output_root="/workspace/shared/airbnb/images_gpu_debug",
    )
    [res] = remote_parallel_map(
        gpu_detect_image_batch, [batch],
        func_cpu=8, func_ram=64, func_gpu="A100_40G",
        max_parallelism=1, grow=True, spinner=True,
    )
    import json
    print(json.dumps(res, indent=2, default=str))


if __name__ == "__main__":
    main()
