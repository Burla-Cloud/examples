#!/usr/bin/env python3
"""Run reduce.py + analysis.py on a Burla worker (where /workspace/shared/hpt/observations
lives), then pull the resulting JSON outputs back to local samples/ and frontend/public/data/.
"""
from __future__ import annotations

import json
from pathlib import Path

from burla import remote_parallel_map

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"
FRONT_DATA = REPO_ROOT / "frontend" / "public" / "data"


def reduce_and_analyze(payload: dict) -> dict[str, str]:
    """Worker-side: take demo source + data files + scale summary, run reduce +
    analysis, return all output JSONs. We ship source files explicitly because
    Burla's auto-import does not always re-resolve `__file__` to a worker-local
    path, and `import reduce` collides with the historical stdlib `reduce` symbol."""
    import base64
    import json
    import sys
    from pathlib import Path
    import importlib.util as _ilu

    src_dir = Path("/tmp/hpt_src")
    src_dir.mkdir(parents=True, exist_ok=True)
    for fname, content in payload["sources"].items():
        target = src_dir / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    for fname, b64 in payload["data_files"].items():
        target = src_dir / fname
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(base64.b64decode(b64))

    sys.path.insert(0, str(src_dir))

    def _load(name: str, fname: str):
        spec = _ilu.spec_from_file_location(name, src_dir / fname)
        mod = _ilu.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
        return mod

    out_dir = Path("/workspace/shared/hpt/output")
    out_dir.mkdir(parents=True, exist_ok=True)
    front_dir = out_dir / "frontend_public_data"
    front_dir.mkdir(parents=True, exist_ok=True)

    r = _load("hpt_reduce", "reduce.py")
    a = _load("hpt_analysis", "analysis.py")

    r.SAMPLES = out_dir
    a.SAMPLES = out_dir
    a.FRONT_DATA = front_dir

    (out_dir / "hpt_scale_summary.json").write_text(payload["scale_summary"], encoding="utf-8")

    sys.argv = ["reduce"]
    r.main()
    sys.argv = ["analysis"]
    a.main()

    results: dict[str, str] = {}
    for fp in out_dir.glob("*.json"):
        results[fp.name] = fp.read_text(encoding="utf-8")
    for fp in front_dir.glob("*.json"):
        results[f"frontend/{fp.name}"] = fp.read_text(encoding="utf-8")
    return results


def main() -> None:
    import base64

    src_files = [
        "reduce.py",
        "analysis.py",
        "pipeline.py",
        "parsers_inline.py",
        "codes.py",
        "hospital_index.py",
        "description_filter.py",
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
    data_paths: list[str] = ["config/code_seeds.csv"]
    for fp in (REPO_ROOT / "data_sources").glob("*"):
        if fp.is_file():
            data_paths.append(f"data_sources/{fp.name}")
    for h_idx in ((REPO_ROOT / "data" / "hospital_index.json"),):
        if h_idx.is_file():
            data_paths.append("data/hospital_index.json")
    for rel in data_paths:
        p = REPO_ROOT / rel
        if not p.is_file():
            print(f"WARN: missing {p}, skipping")
            continue
        data_files[rel] = base64.b64encode(p.read_bytes()).decode("ascii")
    total = sum(len(v) for v in data_files.values())
    print(f"shipping {len(data_files)} data files (~{total / 1024 / 1024:.1f} MB b64)")

    summary_path = SAMPLES / "hpt_scale_summary.json"
    if summary_path.is_file():
        scale_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        scale_summary.pop("failures_sample", None)
    else:
        scale_summary = {"mode": "UNKNOWN", "hospitals_submitted": 0}
    payload = {
        "sources": sources,
        "data_files": data_files,
        "scale_summary": json.dumps(scale_summary, indent=2),
    }

    [out] = list(
        remote_parallel_map(
            reduce_and_analyze,
            [payload],
            func_cpu=2,
            func_ram=7,
            max_parallelism=1,
            spinner=True,
        )
    )

    SAMPLES.mkdir(parents=True, exist_ok=True)
    FRONT_DATA.mkdir(parents=True, exist_ok=True)
    for fname, content in out.items():
        if fname.startswith("frontend/"):
            target = FRONT_DATA / fname.removeprefix("frontend/")
        else:
            target = SAMPLES / fname
        target.write_text(content, encoding="utf-8")
        print(f"wrote {target} ({len(content):,} bytes)")

    (SAMPLES / "hpt_scale_summary.json").write_text(payload["scale_summary"], encoding="utf-8")


if __name__ == "__main__":
    main()
