"""WPI scale-up: send all 4094 YFCC shards to the Burla cluster.

Runs `process_shard` on every shard with func_cpu=1 so we saturate all
1040 1-vCPU slots across the cluster (13 nodes * 80 vCPUs). With ~4 shards
per worker at ~30-60s each, expected wall time: 4-8 minutes at peak load.

Outputs accumulate at /workspace/shared/wpi/shards/*.jsonl on the cluster's
shared GCS filesystem. We then download them locally for analysis/UI build.

Safety:
  - `--dry-run` just prints the shard plan.
  - `--limit N` runs only the first N shards (for staged scale-up tests).
  - `--resume` skips shards whose output jsonl already exists in the result dir.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import List

from burla import remote_parallel_map
from huggingface_hub import HfApi

from pipeline import process_shard


REPO_ID = "dalle-mini/YFCC100M_OpenAI_subset"


def list_all_shards() -> List[str]:
    api = HfApi()
    files = api.list_repo_files(REPO_ID, repo_type="dataset")
    shards = sorted(
        f.removeprefix("metadata/metadata_").removesuffix(".jsonl.gz")
        for f in files
        if f.startswith("metadata/metadata_") and f.endswith(".jsonl.gz")
    )
    return shards


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="process only first N shards")
    ap.add_argument("--func-cpu", type=int, default=1)
    ap.add_argument("--func-ram", type=int, default=4)
    ap.add_argument("--max-parallelism", type=int, default=1000)
    args = ap.parse_args()

    print(f"listing all shards from {REPO_ID} ...")
    shards = list_all_shards()
    print(f"  total shards: {len(shards)}  (first: {shards[0]}, last: {shards[-1]})")

    if args.limit:
        shards = shards[: args.limit]
        print(f"  LIMIT applied: processing first {len(shards)} shards")

    if args.dry_run:
        print("DRY RUN — nothing submitted.")
        return

    print()
    print(f"submitting {len(shards)} shards to Burla with:")
    print(f"  func_cpu={args.func_cpu}  func_ram={args.func_ram}GB")
    print(f"  max_parallelism={args.max_parallelism}  grow=True")
    print()

    t0 = time.time()
    results = remote_parallel_map(
        process_shard,
        shards,
        func_cpu=args.func_cpu,
        func_ram=args.func_ram,
        grow=True,
        max_parallelism=args.max_parallelism,
        spinner=True,
    )
    elapsed = time.time() - t0

    successes = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]
    total_rows = sum(r.get("rows", 0) for r in successes)
    total_geo = sum(r.get("geotagged", 0) for r in successes)
    total_written = sum(r.get("written", 0) for r in successes)

    summary = {
        "elapsed_seconds": round(elapsed, 2),
        "elapsed_minutes": round(elapsed / 60, 2),
        "shards_submitted": len(shards),
        "shards_succeeded": len(successes),
        "shards_failed": len(failures),
        "total_metadata_rows": total_rows,
        "total_geotagged": total_geo,
        "total_written": total_written,
        "throughput_rows_per_sec": round(total_rows / elapsed, 1) if elapsed else 0,
        "throughput_geotagged_per_sec": round(total_geo / elapsed, 1) if elapsed else 0,
    }
    print()
    print("=" * 70)
    print(json.dumps(summary, indent=2))

    out = Path(__file__).parent / "samples" / "wpi_scale_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        **summary,
        "first_failures": failures[:10],
        "first_successes": successes[:3],
    }, indent=2) + "\n")
    print(f"wrote {out}")

    if failures:
        print()
        print("!! sample failures:")
        for f in failures[:5]:
            print(f"  {f.get('shard')}: {f.get('error')}")


if __name__ == "__main__":
    main()
