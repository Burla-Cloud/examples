"""Run one cpu_score_image_batch to see its full output, including errors."""
from __future__ import annotations

import json
import sys

sys.path.insert(0, ".")

from dotenv import load_dotenv

import numpy as _np  # noqa: F401
import pandas as _pd  # noqa: F401
import pyarrow as _pa  # noqa: F401
import pyarrow.parquet as _pq  # noqa: F401

from src.config import SHARED_IMAGES_CPU, SHARED_ROOT
from src.lib.io import register_src_for_burla
from src.tasks.image_tasks import CpuImageBatchArgs, cpu_score_image_batch


def main() -> None:
    load_dotenv()
    register_src_for_burla()
    from burla import remote_parallel_map

    photo_manifest = f"{SHARED_ROOT}/photo_manifest.parquet"
    output_root = SHARED_IMAGES_CPU + "_debug"

    [res] = remote_parallel_map(
        cpu_score_image_batch,
        [CpuImageBatchArgs(
            batch_id=0,
            photo_manifest_path=photo_manifest,
            row_start=0,
            row_end=20,
            output_root=output_root,
        )],
        func_cpu=2, func_ram=8, max_parallelism=1, grow=True, spinner=True,
    )
    print("RESULT:", json.dumps({k: v for k, v in res.items() if k != "traceback"}, indent=2, default=str))
    if res.get("traceback"):
        print("TRACEBACK:")
        print(res["traceback"])


if __name__ == "__main__":
    main()
