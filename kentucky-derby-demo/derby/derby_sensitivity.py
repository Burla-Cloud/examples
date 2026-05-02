"""
derby_sensitivity.py
--------------------
Tests 5,000 weight combinations for the 10-factor scoring model via Burla.
Each combination is back-tested against the 2022-2025 Derby holdout years.
The best-performing weight set replaces the manually-chosen defaults.
Saves results into data/model_results.json.
"""

import os
import sys
import json
import numpy as np
import pandas as pd
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

FACTORS = [
    "beyer_norm", "run_style_score", "trainer_score_norm",
    "jockey_score_norm", "dosage_score", "pedigree_dist",
    "post_wp_norm", "post_itm_norm", "win_rate_norm", "stamina_test",
]

# Historical Derby field snapshots (2022-2025) encoded for back-testing.
# Format: list of dicts with factor scores + is_winner flag.
# Scores are on 0-10 scale matching the feature engineering in derby_features.py.
BACKTEST_FIELDS = {
    2022: [
        dict(name="Rich Strike",    beyer_norm=2.7, run_style_score=5.5, trainer_score_norm=3.0,
             jockey_score_norm=2.0, dosage_score=7.0, pedigree_dist=6.0,
             post_wp_norm=4.0, post_itm_norm=4.0, win_rate_norm=6.0, stamina_test=1, is_winner=1),
        dict(name="Epicenter",      beyer_norm=8.0, run_style_score=8.0, trainer_score_norm=7.0,
             jockey_score_norm=7.0, dosage_score=7.0, pedigree_dist=7.0,
             post_wp_norm=7.0, post_itm_norm=6.0, win_rate_norm=8.0, stamina_test=1, is_winner=0),
        dict(name="Smile Happy",    beyer_norm=6.0, run_style_score=7.0, trainer_score_norm=8.0,
             jockey_score_norm=4.0, dosage_score=7.5, pedigree_dist=7.5,
             post_wp_norm=3.0, post_itm_norm=4.5, win_rate_norm=7.0, stamina_test=1, is_winner=0),
        dict(name="Zandon",         beyer_norm=7.5, run_style_score=8.0, trainer_score_norm=5.0,
             jockey_score_norm=8.0, dosage_score=6.5, pedigree_dist=7.0,
             post_wp_norm=8.0, post_itm_norm=7.0, win_rate_norm=7.5, stamina_test=1, is_winner=0),
        dict(name="Mo Donegal",     beyer_norm=6.5, run_style_score=8.0, trainer_score_norm=7.5,
             jockey_score_norm=8.5, dosage_score=7.0, pedigree_dist=7.0,
             post_wp_norm=5.0, post_itm_norm=5.5, win_rate_norm=7.0, stamina_test=1, is_winner=0),
    ],
    2023: [
        dict(name="Mage",           beyer_norm=7.0, run_style_score=7.0, trainer_score_norm=5.5,
             jockey_score_norm=8.5, dosage_score=7.5, pedigree_dist=8.5,
             post_wp_norm=9.0, post_itm_norm=7.0, win_rate_norm=3.0, stamina_test=1, is_winner=1),
        dict(name="Two Phil's",     beyer_norm=6.5, run_style_score=8.0, trainer_score_norm=3.0,
             jockey_score_norm=7.0, dosage_score=8.0, pedigree_dist=6.0,
             post_wp_norm=8.0, post_itm_norm=8.0, win_rate_norm=6.0, stamina_test=1, is_winner=0),
        dict(name="Angel of Empire",beyer_norm=7.0, run_style_score=8.0, trainer_score_norm=8.0,
             jockey_score_norm=8.5, dosage_score=7.5, pedigree_dist=8.0,
             post_wp_norm=7.0, post_itm_norm=7.0, win_rate_norm=7.0, stamina_test=1, is_winner=0),
        dict(name="Forte",          beyer_norm=9.0, run_style_score=8.0, trainer_score_norm=7.5,
             jockey_score_norm=7.0, dosage_score=7.0, pedigree_dist=7.5,
             post_wp_norm=6.0, post_itm_norm=6.0, win_rate_norm=8.5, stamina_test=1, is_winner=0),
        dict(name="Disarm",         beyer_norm=6.0, run_style_score=8.0, trainer_score_norm=7.5,
             jockey_score_norm=8.5, dosage_score=7.0, pedigree_dist=7.0,
             post_wp_norm=5.0, post_itm_norm=5.5, win_rate_norm=6.0, stamina_test=1, is_winner=0),
    ],
    2024: [
        dict(name="Mystik Dan",     beyer_norm=6.5, run_style_score=8.0, trainer_score_norm=4.5,
             jockey_score_norm=7.0, dosage_score=7.5, pedigree_dist=7.0,
             post_wp_norm=6.0, post_itm_norm=6.5, win_rate_norm=6.0, stamina_test=1, is_winner=1),
        dict(name="Sierra Leone",   beyer_norm=8.0, run_style_score=8.0, trainer_score_norm=6.0,
             jockey_score_norm=8.5, dosage_score=7.0, pedigree_dist=7.5,
             post_wp_norm=7.0, post_itm_norm=7.0, win_rate_norm=8.0, stamina_test=1, is_winner=0),
        dict(name="Forever Young",  beyer_norm=7.0, run_style_score=8.0, trainer_score_norm=3.0,
             jockey_score_norm=3.5, dosage_score=7.0, pedigree_dist=6.5,
             post_wp_norm=9.0, post_itm_norm=7.5, win_rate_norm=7.0, stamina_test=1, is_winner=0),
        dict(name="Catching Freedom",beyer_norm=7.5,run_style_score=8.0, trainer_score_norm=8.0,
             jockey_score_norm=5.0, dosage_score=7.0, pedigree_dist=7.5,
             post_wp_norm=4.0, post_itm_norm=5.0, win_rate_norm=7.0, stamina_test=1, is_winner=0),
        dict(name="Fierceness",     beyer_norm=9.5, run_style_score=8.0, trainer_score_norm=7.5,
             jockey_score_norm=8.0, dosage_score=7.0, pedigree_dist=8.0,
             post_wp_norm=6.0, post_itm_norm=6.5, win_rate_norm=8.5, stamina_test=1, is_winner=0),
    ],
    2025: [
        dict(name="Sovereignty",    beyer_norm=7.7, run_style_score=8.0, trainer_score_norm=7.0,
             jockey_score_norm=8.0, dosage_score=8.5, pedigree_dist=8.0,
             post_wp_norm=8.0, post_itm_norm=7.0, win_rate_norm=7.0, stamina_test=1, is_winner=1),
        dict(name="Journalism",     beyer_norm=8.5, run_style_score=8.0, trainer_score_norm=9.5,
             jockey_score_norm=8.5, dosage_score=7.5, pedigree_dist=8.5,
             post_wp_norm=5.0, post_itm_norm=5.5, win_rate_norm=8.5, stamina_test=1, is_winner=0),
        dict(name="Sandman",        beyer_norm=7.5, run_style_score=7.0, trainer_score_norm=8.0,
             jockey_score_norm=8.5, dosage_score=7.5, pedigree_dist=8.0,
             post_wp_norm=2.5, post_itm_norm=4.0, win_rate_norm=7.5, stamina_test=1, is_winner=0),
        dict(name="Flying Mohican", beyer_norm=7.0, run_style_score=7.0, trainer_score_norm=5.0,
             jockey_score_norm=7.5, dosage_score=7.0, pedigree_dist=7.5,
             post_wp_norm=7.0, post_itm_norm=6.5, win_rate_norm=6.5, stamina_test=1, is_winner=0),
    ],
}


def score_field(horses: list[dict], weights: np.ndarray) -> str:
    """Return the name of the highest-scoring horse given these weights."""
    scores = []
    for h in horses:
        s = sum(weights[i] * h[f] for i, f in enumerate(FACTORS))
        scores.append((h["name"], s))
    scores.sort(key=lambda x: -x[1])
    return scores[0][0]


def backtest_weights(weights_list, factors, backtest_fields) -> dict:
    """
    Evaluate one weight combination across all back-test years.
    Returns the total score (winner=10pts, 2nd=5pts, 3rd=2pts).
    Burla unpacks tuples as *args so signature must match the tuple structure.
    """
    import numpy as np

    weights = np.array(weights_list)

    total_score = 0
    details = {}
    for year, horses in backtest_fields.items():
        scores = []
        for h in horses:
            s = sum(weights[i] * h[f] for i, f in enumerate(factors))
            scores.append((h["name"], s))
        scores.sort(key=lambda x: -x[1])
        predicted_winner = scores[0][0]
        actual_winner    = next(h["name"] for h in horses if h["is_winner"])
        rank_of_actual   = next(i for i, (n, _) in enumerate(scores) if n == actual_winner)
        pts = [10, 5, 2, 1, 0][min(rank_of_actual, 4)]
        total_score += pts
        details[str(year)] = {
            "predicted": predicted_winner,
            "actual": actual_winner,
            "rank_of_actual": rank_of_actual,
            "pts": pts,
        }

    return {"weights": weights_list, "total_score": total_score, "details": details}


def sample_weight_combinations(n: int = 5000, seed: int = 42) -> list[list[float]]:
    """Sample n weight vectors from a Dirichlet distribution (sum to 1)."""
    rng = np.random.default_rng(seed)
    # Dirichlet with alpha=1 gives uniform over the simplex
    raw = rng.dirichlet(np.ones(len(FACTORS)), size=n)
    return raw.tolist()


def run_sensitivity_burla(combos: list, factors: list, backtest_fields: dict) -> list[dict]:
    """Dispatch all weight evaluations to Burla (or local threads as fallback)."""
    args_list = [(combo, factors, backtest_fields) for combo in combos]

    try:
        from burla import remote_parallel_map
        print(f"  Dispatching {len(args_list)} weight evaluations to Burla...")
        results = remote_parallel_map(backtest_weights, args_list, grow=True)
        print(f"  Burla returned {len(results)} results")
        return results
    except Exception as exc:
        print(f"  Burla unavailable ({exc}), using local ThreadPoolExecutor...")
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as ex:
            futures = [ex.submit(backtest_weights, *args) for args in args_list]
            results = [f.result() for f in futures]
        return results


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    out_path = os.path.join(DATA_DIR, "model_results.json")

    n_combos = 5000
    print(f"Sampling {n_combos} weight combinations via Dirichlet distribution...")
    combos = sample_weight_combinations(n_combos)

    print("Running sensitivity analysis (back-test on 2022-2025)...")
    results = run_sensitivity_burla(combos, FACTORS, BACKTEST_FIELDS)

    results.sort(key=lambda r: -r["total_score"])
    best   = results[0]
    top10  = results[:10]

    print(f"\nBest weight combination (score={best['total_score']}/40 pts):")
    for i, (f, w) in enumerate(zip(FACTORS, best["weights"])):
        print(f"  {f:<28} {w:.4f}  ({w*100:.1f}%)")
    print("\nYear-by-year back-test detail:")
    for year, d in best["details"].items():
        print(f"  {year}: predicted={d['predicted']:<20} actual={d['actual']:<20} "
              f"rank={d['rank_of_actual']+1}  pts={d['pts']}")

    score_distribution = {}
    for r in results:
        s = str(r["total_score"])
        score_distribution[s] = score_distribution.get(s, 0) + 1

    print(f"\nScore distribution across {len(results)} combos: {score_distribution}")

    # Load existing results and merge
    existing = {}
    if os.path.exists(out_path):
        with open(out_path) as f:
            existing = json.load(f)

    existing["sensitivity"] = {
        "n_combos_tested": len(results),
        "best_score": best["total_score"],
        "best_weights": {f: w for f, w in zip(FACTORS, best["weights"])},
        "best_details": best["details"],
        "top10_scores": [r["total_score"] for r in top10],
        "score_distribution": score_distribution,
    }

    with open(out_path, "w") as f:
        json.dump(existing, f, indent=2)
    print(f"\nSaved sensitivity results -> {out_path}")


if __name__ == "__main__":
    main()
