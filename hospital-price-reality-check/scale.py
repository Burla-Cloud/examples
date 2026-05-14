#!/usr/bin/env python3
"""Scale: dispatch all hospitals via Burla, or --local sequential."""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def _ensure_ca_bundle() -> None:
    """Belt and suspenders: macOS python.org Python 3.x ships without a system CA file
    (it expects /Applications/Python\\ 3.x/Install\\ Certificates.command to be run).
    When that step is skipped, every TLS call into *.burla.dev dies with
    `unable to get local issuer certificate`. If certifi is installed, point Python at it.
    Honors any pre-existing SSL_CERT_FILE / REQUESTS_CA_BUNDLE.
    """
    if os.environ.get("SSL_CERT_FILE") and os.environ.get("REQUESTS_CA_BUNDLE"):
        return
    try:
        import certifi

        bundle = certifi.where()
    except Exception:
        return
    if not bundle or not Path(bundle).is_file():
        return
    os.environ.setdefault("SSL_CERT_FILE", bundle)
    os.environ.setdefault("REQUESTS_CA_BUNDLE", bundle)


_ensure_ca_bundle()

from hospital_index import load_hospitals  # noqa: E402
from pipeline import (  # noqa: E402
    parse_hospital_mrf,
    parse_hospital_mrf_full_chargemaster,
)

REPO_ROOT = Path(__file__).resolve().parent
SAMPLES = REPO_ROOT / "samples"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--local", action="store_true", help="Run parse_hospital_mrf in-process (no Burla)")
    ap.add_argument("--func-cpu", type=int, default=1)
    ap.add_argument("--func-ram", type=int, default=2)
    ap.add_argument("--max-parallelism", type=int, default=1500)
    ap.add_argument("--local-threads", type=int, default=1, help="When --local, run this many parallel parser threads")
    ap.add_argument(
        "--wipe-shared",
        action="store_true",
        help="Before submission, wipe /workspace/shared/hpt/observations on the cluster (remote runs only).",
    )
    ap.add_argument(
        "--include-tpafs",
        action="store_true",
        help="Also include hospitals from the TPAFS public MRF index (data_sources/tpafs_machine_readable_links.csv).",
    )
    ap.add_argument(
        "--include-dolthub",
        action="store_true",
        help="Also include hospitals from the dolthub transparency-in-pricing snapshot (data_sources/dolthub_v4_mrfs.json).",
    )
    ap.add_argument(
        "--include-oria",
        action="store_true",
        help="Also include hospitals from the Trilliant Health Oria public directory (data_sources/oria_v3_mrfs.json).",
    )
    ap.add_argument(
        "--per-state-cap",
        type=int,
        default=0,
        help="If > 0, cap the number of hospitals per state for a balanced sample.",
    )
    ap.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip hospitals that already have an observations jsonl on disk.",
    )
    ap.add_argument(
        "--full-chargemaster",
        action="store_true",
        help=(
            "Run parse_hospital_mrf_full_chargemaster instead of the curated parser. "
            "Emits EVERY priced row per hospital (capped at 50K) into "
            "/workspace/shared/hpt/chargemaster/*.jsonl. Used to power the "
            "per-hospital 'Full chargemaster' search view."
        ),
    )
    args = ap.parse_args()
    worker_fn = (
        parse_hospital_mrf_full_chargemaster if args.full_chargemaster else parse_hospital_mrf
    )
    obs_subdir = "chargemaster_full" if args.full_chargemaster else "observations"

    idx = REPO_ROOT / "data" / "hospital_index.json"
    hospitals = load_hospitals(
        idx if idx.is_file() else None,
        include_tpafs=args.include_tpafs,
        include_dolthub=args.include_dolthub,
        include_oria=args.include_oria,
    )
    if args.per_state_cap and args.per_state_cap > 0:
        from collections import defaultdict
        capped: list[dict] = []
        per_state: dict[str, int] = defaultdict(int)
        for h in hospitals:
            st = h.get("state") or "??"
            if per_state[st] < args.per_state_cap:
                capped.append(h)
                per_state[st] += 1
        hospitals = capped
    if args.skip_existing:
        from pipeline import shared_hpt_root
        obs_dir = shared_hpt_root() / obs_subdir
        kept = []
        skipped_ok = 0
        skipped_failed = 0
        for h in hospitals:
            jsonl = obs_dir / f"{h['hospital_id']}.jsonl"
            fail = obs_dir / f"{h['hospital_id']}.fail"
            if jsonl.is_file() and jsonl.stat().st_size > 0:
                skipped_ok += 1
                continue
            if fail.is_file():
                skipped_failed += 1
                continue
            kept.append(h)
        print(
            f"--skip-existing: keeping {len(kept)} hospitals, "
            f"skipping {skipped_ok} done + {skipped_failed} previously-failed"
        )
        hospitals = kept
    if args.limit:
        hospitals = hospitals[: args.limit]

    if args.dry_run:
        print(f"DRY RUN: would process {len(hospitals)} hospitals")
        return

    use_local = args.local or os.environ.get("HPT_SCALE_LOCAL", "").lower() in ("1", "true", "yes")
    t0 = time.time()
    if use_local:
        if args.local_threads <= 1:
            results = [worker_fn(h) for h in hospitals]
        else:
            from concurrent.futures import ThreadPoolExecutor

            results = []
            with ThreadPoolExecutor(max_workers=args.local_threads) as ex:
                for r in ex.map(worker_fn, hospitals):
                    results.append(r)
                    if not r.get("error"):
                        print(
                            f"  [{len(results)}/{len(hospitals)}] {r.get('hospital_id')}: "
                            f"{r.get('rows', 0)} rows"
                        )
                    else:
                        print(
                            f"  [{len(results)}/{len(hospitals)}] {r.get('hospital_id')} ERROR: "
                            f"{str(r.get('error'))[:120]}"
                        )
        mode = "LOCAL_OK"
    else:
        from burla import remote_parallel_map

        if args.wipe_shared:
            wipe_subdir = obs_subdir
            def _wipe_shared(_: int) -> str:
                import shutil
                from pathlib import Path as P

                obs = P(f"/workspace/shared/hpt/{wipe_subdir}")
                if obs.is_dir():
                    shutil.rmtree(obs)
                return "wiped"

            list(
                remote_parallel_map(
                    _wipe_shared,
                    [0],
                    func_cpu=1,
                    func_ram=2,
                    grow=True,
                    max_parallelism=1,
                    spinner=True,
                )
            )

        results = remote_parallel_map(
            worker_fn,
            hospitals,
            func_cpu=args.func_cpu,
            func_ram=args.func_ram,
            max_parallelism=args.max_parallelism,
            grow=True,
            spinner=True,
        )
        mode = "REMOTE_OK"

    elapsed = time.time() - t0
    successes = [r for r in results if not r.get("error")]
    failures = [r for r in results if r.get("error")]
    obs = sum(r.get("rows") or 0 for r in successes)

    summary = {
        "mode": mode,
        "pass": "full_chargemaster" if args.full_chargemaster else "curated_targets",
        "elapsed_seconds": round(elapsed, 2),
        "hospitals_submitted": len(hospitals),
        "hospitals_succeeded": len(successes),
        "hospitals_failed": len(failures),
        "observation_rows_reported": obs,
    }
    SAMPLES.mkdir(parents=True, exist_ok=True)
    summary_name = (
        "hpt_scale_chargemaster_summary.json"
        if args.full_chargemaster
        else "hpt_scale_summary.json"
    )
    (SAMPLES / summary_name).write_text(
        json.dumps({**summary, "failures_sample": failures[:20]}, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))
    print(mode)
    if failures:
        print("sample failures:", failures[:3])


if __name__ == "__main__":
    main()
