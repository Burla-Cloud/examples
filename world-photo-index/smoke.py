"""WPI smoke test: run process_shard on ONE cluster worker for shard "100".

Verifies the full pipeline (HF download -> CLIP embed -> reverse-geocode -> write)
on a single worker before fanning out. Passes `grow=True` so Burla will boot a
node if the cluster has idled down between runs.
"""
from __future__ import annotations

import json
from burla import remote_parallel_map

from pipeline import process_shard


def main() -> None:
    print("sending one shard to one worker for end-to-end smoke test...")
    results = remote_parallel_map(
        process_shard,
        ["100"],
        func_cpu=2,
        func_ram=4,
        grow=True,
        spinner=True,
    )
    for r in results:
        print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
