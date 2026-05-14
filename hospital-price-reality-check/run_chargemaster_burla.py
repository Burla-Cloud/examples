#!/usr/bin/env python3
"""Run chargemaster_build.py on a Burla worker (where
/workspace/shared/hpt/chargemaster_full lives), then fetch the resulting
per-hospital JSON.gz files back to local frontend/public/data/chargemaster/.

We can't return one giant tarball anymore (the uncapped dataset can be many GB
of compressed output). Instead this script runs in two passes:

  1. A single worker runs chargemaster_build.py, writing per-hospital
     .json.gz files into /workspace/shared/hpt/chargemaster_dist/ and a
     chargemaster_index.json next to it. The worker returns a manifest of
     filenames.

  2. The local driver fans out a fetch_one_hospital call per filename via
     remote_parallel_map. Each return ships one compressed bundle back as
     base64; we write it straight to frontend/public/data/chargemaster/. This
     keeps each Burla return small enough to serialize reliably and lets us
     run hundreds of fetches in parallel.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path

from burla import remote_parallel_map

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"
FRONT_DATA = REPO_ROOT / "frontend" / "public" / "data"


def build_chargemaster_on_worker(payload: dict) -> dict:
    """Worker pass 1: write demo sources, run chargemaster_build, leave the
    per-hospital .json.gz files in /workspace/shared/hpt/chargemaster_dist/
    so we can fetch them later in parallel.
    """
    import base64 as _b64
    import importlib.util as _ilu
    import os as _os
    import sys as _sys
    from pathlib import Path as _P

    src_dir = _P("/tmp/hpt_src")
    src_dir.mkdir(parents=True, exist_ok=True)
    for fname, content in payload["sources"].items():
        target = src_dir / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    for fname, b64 in payload["data_files"].items():
        target = src_dir / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(_b64.b64decode(b64))

    _sys.path.insert(0, str(src_dir))

    def _load(name: str, fname: str):
        spec = _ilu.spec_from_file_location(name, src_dir / fname)
        mod = _ilu.module_from_spec(spec)
        _sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod

    out_dir = _P("/workspace/shared/hpt/chargemaster_dist")
    if out_dir.is_dir():
        for fp in list(out_dir.glob("*.json.gz")):
            try:
                fp.unlink()
            except OSError:
                pass
    out_dir.mkdir(parents=True, exist_ok=True)
    _os.environ["CHARGEMASTER_OUT_DIR"] = str(out_dir)
    cm = _load("hpt_chargemaster_build", "chargemaster_build.py")
    cm.SAMPLES = out_dir
    cm.FRONT_DATA = out_dir.parent
    cm.main()

    files = sorted(p.name for p in out_dir.glob("*.json.gz"))
    total_bytes = sum((out_dir / f).stat().st_size for f in files)
    idx_path = out_dir.parent / "chargemaster_index.json"
    index_b64 = (
        _b64.b64encode(idx_path.read_bytes()).decode()
        if idx_path.is_file()
        else None
    )
    return {
        "n_files": len(files),
        "total_bytes": total_bytes,
        "files": files,
        "index_b64": index_b64,
    }


def fetch_one_hospital(name: str) -> dict:
    """Worker pass 2: read a single chargemaster .json.gz off shared FS and
    return its bytes as base64. Small enough per call to round-trip reliably.
    """
    import base64 as _b64
    from pathlib import Path as _P

    fp = _P(f"/workspace/shared/hpt/chargemaster_dist/{name}")
    if not fp.is_file():
        return {"name": name, "bytes": 0, "data_b64": ""}
    data = fp.read_bytes()
    return {
        "name": name,
        "bytes": len(data),
        "data_b64": _b64.b64encode(data).decode(),
    }


def main() -> None:
    src_files = [
        "chargemaster_build.py",
        "hospital_index.py",
        "pipeline.py",
        "parsers_inline.py",
        "codes.py",
        "dosage_extractor.py",
    ]
    for d in ("parsers",):
        for fp in (REPO_ROOT / d).glob("*.py"):
            src_files.append(f"{d}/{fp.name}")

    sources: dict[str, str] = {}
    for rel in src_files:
        p = REPO_ROOT / rel
        if not p.is_file():
            print(f"WARN: missing {p}, skipping")
            continue
        sources[rel] = p.read_text(encoding="utf-8")
    print(f"shipping {len(sources)} source files: {sorted(sources)}")

    data_files: dict[str, str] = {}
    data_paths: list[str] = ["data/code_seeds.csv"]
    for fp in (REPO_ROOT / "data_sources").glob("*"):
        if fp.is_file():
            data_paths.append(f"data_sources/{fp.name}")
    h_idx = REPO_ROOT / "data" / "hospital_index.json"
    if h_idx.is_file():
        data_paths.append("data/hospital_index.json")
    for rel in data_paths:
        p = REPO_ROOT / rel
        if not p.is_file():
            continue
        data_files[rel] = base64.b64encode(p.read_bytes()).decode("ascii")
    total = sum(len(v) for v in data_files.values())
    print(f"shipping {len(data_files)} data files (~{total / 1024 / 1024:.1f} MB b64)")

    payload = {"sources": sources, "data_files": data_files}

    print("Pass 1: building chargemaster bundle on cluster ...")
    [manifest] = list(
        remote_parallel_map(
            build_chargemaster_on_worker,
            [payload],
            func_cpu=4,
            func_ram=32,
            max_parallelism=1,
            spinner=True,
        )
    )
    n_files = manifest["n_files"]
    total_mb = manifest["total_bytes"] / 1024 / 1024
    print(f"  built {n_files} files, {total_mb:.1f} MB total compressed")

    target_dir = FRONT_DATA / "chargemaster"
    target_dir.mkdir(parents=True, exist_ok=True)
    if manifest.get("index_b64"):
        (FRONT_DATA / "chargemaster_index.json").write_bytes(
            base64.b64decode(manifest["index_b64"])
        )
        print(f"  wrote chargemaster_index.json")

    files = manifest["files"]
    print(f"Pass 2: fetching {len(files)} .json.gz files back to local ...")
    n_done = 0
    n_bytes = 0
    for result in remote_parallel_map(
        fetch_one_hospital,
        files,
        func_cpu=1,
        func_ram=2,
        max_parallelism=200,
        spinner=True,
    ):
        if result.get("data_b64"):
            data = base64.b64decode(result["data_b64"])
            (target_dir / result["name"]).write_bytes(data)
            n_bytes += len(data)
        n_done += 1
        if n_done % 200 == 0:
            print(f"  fetched {n_done}/{len(files)} (~{n_bytes / 1024 / 1024:.1f} MB)", flush=True)
    print(f"done: wrote {n_done} files (~{n_bytes / 1024 / 1024:.1f} MB) into {target_dir}")


if __name__ == "__main__":
    main()
