"""
derby_audit.py
--------------
Permutation test on Burla: shuffles which horse "won" in each historical
backtest year, then re-runs the same N_COMBOS Dirichlet weight search the
real pipeline runs. Repeating that N_PERMS times gives a distribution of
"best score achievable under random labels" -- the null. If the published
score is well above the null distribution, the search picked up real signal;
if not, the published number is largely search noise.

Inputs:  data/historical_results.csv via derby_sensitivity.build_backtest_fields
Outputs: data/audit_results.json (published score + null distribution)
"""

import os
import sys
import json
import time
import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Tunable: number of label permutations to run on Burla. 2000 is what the
# v1 site claimed; we keep parity for the audit so v1 vs v2 numbers compare
# directly. Each permutation runs N_COMBOS weight evaluations against the
# shuffled labels.
N_PERMS = 2000
N_COMBOS = 5000


def run_one_permutation(args_tuple):
    """One permutation: shuffle is_winner per year, then evaluate N_COMBOS
    weight vectors and return the BEST total score for this null draw.
    """
    import numpy as np

    seed, n_combos, factors, fields_pickled = args_tuple
    rng = np.random.default_rng(seed)

    shuffled = {}
    for year, horses in fields_pickled.items():
        n = len(horses)
        if n == 0:
            shuffled[year] = horses
            continue
        new_winner_idx = int(rng.integers(0, n))
        shuffled[year] = [
            {**h, "is_winner": int(i == new_winner_idx)} for i, h in enumerate(horses)
        ]

    # Sample N_COMBOS Dirichlet weight vectors using a permutation-specific seed.
    weight_seed = seed * 31 + 7
    rng2 = np.random.default_rng(weight_seed)
    raw = rng2.dirichlet(np.ones(len(factors)), size=n_combos)

    best = 0
    for w in raw:
        total = 0
        for year, horses in shuffled.items():
            scores = []
            for h in horses:
                s = sum(w[i] * h[f] for i, f in enumerate(factors))
                scores.append((h["name"], s))
            scores.sort(key=lambda x: -x[1])
            actual = next((h["name"] for h in horses if h["is_winner"]), None)
            if actual is None:
                continue
            rank = next((i for i, (n_, _) in enumerate(scores) if n_ == actual), 99)
            pts = [10, 5, 2, 1, 0][min(rank, 4)]
            total += pts
        if total > best:
            best = total
    return {"seed": seed, "best_score": int(best)}


def main():
    sens_path = os.path.join(DATA_DIR, "model_results.json")
    if not os.path.exists(sens_path):
        print("Missing model_results.json -- run derby_sensitivity.py first.")
        return

    with open(sens_path) as f:
        sens_data = json.load(f)
    published_score = int(sens_data.get("sensitivity", {}).get("best_score", 0))

    sys.path.insert(0, os.path.dirname(__file__))
    from derby_sensitivity import build_backtest_fields, FACTORS  # noqa: E402

    hist_path = os.path.join(DATA_DIR, "historical_results.csv")
    hist_df = pd.read_csv(hist_path)
    fields = build_backtest_fields(hist_df, top_k=5)
    print(f"Loaded {len(fields)} years of backtest fields. Published score = {published_score}.")

    # Convert int year keys to strings for JSON-stability through Burla payload.
    fields_for_dispatch = {int(y): h for y, h in fields.items()}

    args_list = [
        (perm_idx, N_COMBOS, FACTORS, fields_for_dispatch)
        for perm_idx in range(N_PERMS)
    ]

    t0 = time.time()
    try:
        from burla import remote_parallel_map

        print(f"Dispatching {N_PERMS} permutations × {N_COMBOS} weight combos to Burla...")
        results = remote_parallel_map(run_one_permutation, args_list, grow=True)
        print(f"Burla returned {len(results)} permutation results in {time.time() - t0:.1f}s")
    except Exception as exc:
        print(f"Burla unavailable ({exc}), falling back to local threads...")
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as ex:
            results = list(ex.map(run_one_permutation, args_list))
        print(f"Local fallback completed {len(results)} permutations in {time.time() - t0:.1f}s")

    null_scores = sorted([r["best_score"] for r in results])
    null_arr = np.array(null_scores)
    median_null = float(np.median(null_arr))
    p_null_geq_published = float((null_arr >= published_score).mean())

    # Per-year random-pick baseline: top-K=5 (+ winner) per year, picking
    # uniformly at random gives ~1/6 chance of nailing the winner each year.
    # Over 16 years the expected score is roughly:
    n_years = len(fields)
    expected_random_pts = sum(
        (1.0 / max(len(h), 1)) * 10 + (1.0 / max(len(h) - 1, 1)) * 5
        for h in fields.values()
    ) if n_years else 0
    edge_over_random = published_score - expected_random_pts

    audit = {
        "n_permutations": N_PERMS,
        "n_combos_per_perm": N_COMBOS,
        "n_years": n_years,
        "max_possible_score": n_years * 10,
        "published_score": published_score,
        "null_median": median_null,
        "null_min": int(null_arr.min()),
        "null_max": int(null_arr.max()),
        "null_p25": float(np.percentile(null_arr, 25)),
        "null_p75": float(np.percentile(null_arr, 75)),
        "p_null_ge_published": p_null_geq_published,
        "expected_random_pick_score": round(expected_random_pts, 2),
        "edge_over_random": round(edge_over_random, 2),
        "elapsed_seconds": round(time.time() - t0, 1),
    }

    out_path = os.path.join(DATA_DIR, "audit_results.json")
    if os.path.exists(out_path):
        with open(out_path) as fp_existing:
            existing = json.load(fp_existing)
        existing.update(audit)
        audit_to_write = existing
    else:
        audit_to_write = audit
    with open(out_path, "w") as fp:
        json.dump(audit_to_write, fp, indent=2)

    print()
    print(f"Published score:        {published_score} / {n_years*10}")
    print(f"Null median:            {median_null:.1f}")
    print(f"Null IQR:               {audit['null_p25']:.1f} – {audit['null_p75']:.1f}")
    print(f"P(null ≥ published):    {p_null_geq_published*100:.1f}%")
    print(f"Random-pick baseline:   {expected_random_pts:.1f}")
    print(f"Edge over random:       +{edge_over_random:.1f}")
    print(f"Saved -> {out_path}")


if __name__ == "__main__":
    main()
