"""GRS scale-out: upload the README parquet to the shared filesystem, then
fan out ~600 summarize_shard jobs across the Burla cluster.

The demo's core premise: every single repository on the planet can be
scanned and classified in minutes if you have enough CPUs. We partition
1M+ READMEs into 600 stripes, each worker processes ~2000 READMEs, and
the whole pipeline finishes before coffee.

Stages:
  0. Upload parquet to /workspace/shared/grs/readmes.parquet (one-off job).
  1. Run `summarize_shard(idx, n_shards)` for idx in [0, n_shards).
  2. Locally scan shard summaries and report failures / throughput.
"""
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import List, Tuple

from burla import remote_parallel_map

from pipeline import summarize_shard

import base64, os
import gzip, os, glob

HERE = Path(__file__).parent
LOCAL_PARQUET = HERE / "samples" / "readmes.parquet"
REMOTE_PARQUET = "/workspace/shared/grs/readmes.parquet"


def _upload_parquet(local: Path, verbose: bool = True) -> dict:
    """Stream a local parquet file to the cluster-shared filesystem in chunks.

    We can't pickle a 100 MB+ local file as a closure argument, so we upload
    it by gzipping + base64-encoding in ~20 MB chunks and calling a worker
    that appends each chunk to /workspace/shared/grs/readmes.parquet.
    """
    import gzip

    # Read + gzip
    t0 = time.time()
    if verbose:
        print(f"reading {local}...")
    raw = local.read_bytes()
    if verbose:
        print(f"  {len(raw)/1e6:.1f} MB raw; gzipping...")
    gz = gzip.compress(raw, compresslevel=3)
    if verbose:
        print(f"  {len(gz)/1e6:.1f} MB gz; base64 chunking...")

    CHUNK = 20 * 1024 * 1024  # 20 MB per chunk
    chunks = []
    for i in range(0, len(gz), CHUNK):
        part = gz[i : i + CHUNK]
        chunks.append((i // CHUNK, base64.b64encode(part).decode("ascii")))
    if verbose:
        print(f"  {len(chunks)} chunks, {len(gz)/1e6:.1f} MB total")

    def write_chunk(idx: int, b64: str) -> dict:
        """Each chunk becomes its own part file, written in parallel."""
        out = f"/workspace/shared/grs/_readmes.gz.part.{idx:04d}"
        os.makedirs(os.path.dirname(out), exist_ok=True)
        data = base64.b64decode(b64)
        with open(out, "wb") as f:
            f.write(data)
        return {"idx": idx, "bytes": len(data)}

    if verbose:
        print("  uploading chunks in parallel ...")
    results = remote_parallel_map(
        write_chunk,
        chunks,
        func_cpu=1,
        func_ram=4,
        max_parallelism=len(chunks),
        spinner=verbose,
    )
    got = sum(r.get("bytes", 0) for r in results)
    if verbose:
        print(f"  uploaded {got/1e6:.1f} MB across {len(results)} part files")

    # Concatenate + decompress on the cluster. Runs on ONE worker that has
    # read access to all part files on the shared GCS-backed FS.
    def finalize(n_parts: int) -> dict:
        base_dir = "/workspace/shared/grs"
        parts = sorted(glob.glob(f"{base_dir}/_readmes.gz.part.*"))
        gz_path = f"{base_dir}/_readmes.gz"
        with open(gz_path, "wb") as f_out:
            for p in parts:
                with open(p, "rb") as f_in:
                    while True:
                        b = f_in.read(8 * 1024 * 1024)
                        if not b:
                            break
                        f_out.write(b)
        # Decompress
        dst = f"{base_dir}/readmes.parquet"
        with gzip.open(gz_path, "rb") as f_in, open(dst, "wb") as f_out:
            while True:
                chunk = f_in.read(8 * 1024 * 1024)
                if not chunk:
                    break
                f_out.write(chunk)
        # Cleanup
        for p in parts:
            try:
                os.remove(p)
            except Exception:
                pass
        try:
            os.remove(gz_path)
        except Exception:
            pass
        size = os.path.getsize(dst)
        return {"dst": dst, "size": size, "n_parts": n_parts}

    if verbose:
        print("  concatenating + decompressing on cluster...")
    [info] = remote_parallel_map(
        finalize, [len(chunks)], func_cpu=2, func_ram=8, max_parallelism=1, spinner=False,
    )
    elapsed = time.time() - t0
    if verbose:
        print(f"  decompressed: {info['dst']}, {info['size']/1e6:.1f} MB (total {elapsed:.1f}s)")
    return info


def _parquet_rowcount(local: Path) -> int:
    import pyarrow.parquet as pq
    return pq.ParquetFile(local).metadata.num_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=600,
                    help="Number of stripes; each worker processes 1/shard of all rows.")
    ap.add_argument("--parallelism", type=int, default=600)
    ap.add_argument("--func-cpu", type=int, default=1)
    ap.add_argument("--func-ram", type=int, default=4)
    ap.add_argument("--skip-upload", action="store_true",
                    help="Assume parquet already exists on the cluster.")
    args = ap.parse_args()

    if not LOCAL_PARQUET.exists():
        print(f"MISSING: {LOCAL_PARQUET}. Run prepare.py first.", file=sys.stderr)
        sys.exit(1)

    n_rows = _parquet_rowcount(LOCAL_PARQUET)
    print(f"local parquet: {LOCAL_PARQUET} ({n_rows:,} rows, "
          f"{LOCAL_PARQUET.stat().st_size/1e6:.1f} MB)")
    print(f"shards: {args.shards}, rows/shard ≈ {n_rows/args.shards:.0f}")
    print(f"parallelism: {args.parallelism}")

    if not args.skip_upload:
        print("\n[1/2] uploading parquet to cluster shared fs...")
        _upload_parquet(LOCAL_PARQUET)
    else:
        print("\n[1/2] skipping upload (using existing cluster parquet)")

    print(f"\n[2/2] fanning out {args.shards} summarize_shard jobs "
          f"({args.parallelism} max parallel)...")

    t0 = time.time()
    jobs = [(i, args.shards) for i in range(args.shards)]
    results = remote_parallel_map(
        summarize_shard,
        jobs,
        func_cpu=args.func_cpu,
        func_ram=args.func_ram,
        grow=True,
        max_parallelism=args.parallelism,
        spinner=True,
    )
    elapsed = time.time() - t0

    # Aggregate stats
    n_ok = sum(r.get("n_ok", 0) for r in results)
    n_err = sum(r.get("n_err", 0) for r in results)
    total_elapsed = sum(r.get("elapsed_s", 0) for r in results)

    print("\n" + "=" * 80)
    print(f"map done in {elapsed:.1f}s")
    print(f"  shards: {len(results)}")
    print(f"  n_ok:   {n_ok:,}")
    print(f"  n_err:  {n_err:,}")
    print(f"  worker cpu-seconds: {total_elapsed:.0f}")
    if elapsed > 0:
        print(f"  throughput: {n_ok/elapsed:,.0f} repos/s")

    # Summary of failed shards
    if n_err:
        bad = [r for r in results if r.get("n_err", 0) > 0]
        print(f"\n{len(bad)} shard(s) had errors:")
        for r in bad[:10]:
            print(f"  shard {r['shard_idx']}: {r['n_err']} errors / {r['n_ok']} ok")


if __name__ == "__main__":
    main()
