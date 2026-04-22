"""One-time warmup: download CLIP weights to shared GCS cache.

Run this ONCE before the fan-out. A single worker downloads the 350MB
ViT-B-32 weights into /workspace/shared/wpi/_model_cache on GCSFuse, so
all subsequent fan-out workers can READ the cache concurrently (reads
are fine on GCSFuse; writes are not).
"""
from __future__ import annotations

import json
from burla import remote_parallel_map

from pipeline import warmup_clip


def main() -> None:
    print("warming CLIP model cache on /workspace/shared/wpi/_model_cache ...")
    result = remote_parallel_map(
        warmup_clip,
        [0],
        func_cpu=2,
        func_ram=4,
        grow=True,
        spinner=True,
    )
    print(json.dumps(result[0], indent=2))
    print("CLIP cache ready.")


if __name__ == "__main__":
    main()
