"""Pull /workspace/shared/ghcn/results/* artifacts off the Burla cluster
back onto the local disk at agents/ghcn-rainiest-day/burla_results/.

Run via the starter kit so the right venv + cluster are used:
    python ../burla-agent-starter-kit/run_job.py \
        --email joeyper23@gmail.com fetch_artifacts.py
"""

from __future__ import annotations

import base64
from pathlib import Path


def read_results(_: int) -> dict:
    """Read every file under /workspace/shared/ghcn/results/ and return bytes (b64)."""
    results = Path("/workspace/shared/ghcn/results")
    out: dict = {}
    if not results.exists():
        return {"__error__": f"no results dir at {results}"}
    for p in sorted(results.iterdir()):
        if not p.is_file():
            continue
        out[p.name] = base64.b64encode(p.read_bytes()).decode("ascii")
    return out


def main() -> int:
    from burla import remote_parallel_map  # type: ignore

    dump = remote_parallel_map(
        read_results,
        [0],
        func_cpu=1,
        func_ram=4,
    )
    payload = dump[0] if dump else {}
    if not payload or "__error__" in payload:
        print("remote returned empty or error:", payload)
        return 2

    dest = Path(__file__).resolve().parent / "burla_results"
    dest.mkdir(exist_ok=True)
    for name, b64 in payload.items():
        out_path = dest / name
        out_path.write_bytes(base64.b64decode(b64))
        print(f"wrote {out_path} ({out_path.stat().st_size:,} bytes)")
    print(f"done. {len(payload)} files downloaded to {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
