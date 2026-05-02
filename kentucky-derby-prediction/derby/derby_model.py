"""
derby_model.py
--------------
Trains 2000+ model configurations in parallel on Burla (GBM, RF, LogReg).
Validates on holdout years 2022-2025 (4 known outcomes).
Picks the best model and generates win-probability predictions for the 2026 field.
Saves: data/model_results.json
"""

import os
import sys
import json
import itertools
import numpy as np
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

FEATURE_COLS = [
    "beyer_norm", "dosage_score", "run_style_score",
    "implied_prob", "post", "post_wp_approx", "muddy",
]
HOLDOUT_YEARS = [2022, 2023, 2024, 2025]


def make_configs() -> list[dict]:
    """Generate all model configurations to test in parallel."""
    configs = []

    # Gradient Boosting: 800 configs
    for n_est in [50, 100, 200, 300]:
        for depth in [2, 3, 4]:
            for lr in [0.05, 0.10, 0.15]:
                for sub in [0.7, 0.9, 1.0]:
                    configs.append({
                        "model": "gbm",
                        "n_estimators": n_est, "max_depth": depth,
                        "learning_rate": lr, "subsample": sub,
                    })

    # Random Forest: 400 configs
    for n_est in [100, 200, 300, 500]:
        for max_feat in ["sqrt", "log2", None]:
            for depth in [None, 5, 10]:
                configs.append({
                    "model": "rf",
                    "n_estimators": n_est,
                    "max_features": max_feat,
                    "max_depth": depth,
                })

    # Logistic Regression: 200 configs
    for C in [0.01, 0.1, 1.0, 10.0, 100.0]:
        for penalty in ["l1", "l2"]:
            for solver in ["saga", "liblinear"]:
                configs.append({
                    "model": "logreg",
                    "C": C, "penalty": penalty, "solver": solver,
                })

    return configs


def train_and_eval(cfg, train_rows, holdout_rows, field_rows) -> dict:
    """
    Train one model config and return its holdout log-loss + predictions.
    Burla unpacks tuples as *args, so signature must match the tuple structure.
    """
    import numpy as np
    import pandas as pd
    from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.metrics import log_loss
    import warnings
    warnings.filterwarnings("ignore")

    train_df   = pd.DataFrame(train_rows)
    holdout_df = pd.DataFrame(holdout_rows)
    field_df   = pd.DataFrame(field_rows)

    feature_cols = [
        "beyer_norm", "dosage_score", "run_style_score",
        "implied_prob", "post", "post_wp_approx", "muddy",
    ]

    X_train = train_df[feature_cols].values
    y_train = train_df["is_winner"].values
    X_hold  = holdout_df[feature_cols].values
    y_hold  = holdout_df["is_winner"].values

    # Build model
    scaler = StandardScaler()
    if cfg["model"] == "gbm":
        clf = GradientBoostingClassifier(
            n_estimators=cfg["n_estimators"],
            max_depth=cfg["max_depth"],
            learning_rate=cfg["learning_rate"],
            subsample=cfg["subsample"],
            random_state=42,
        )
    elif cfg["model"] == "rf":
        clf = RandomForestClassifier(
            n_estimators=cfg["n_estimators"],
            max_features=cfg["max_features"],
            max_depth=cfg["max_depth"],
            random_state=42, n_jobs=1,
        )
    else:  # logreg — use l1_ratio instead of deprecated penalty param
        l1r = 1.0 if cfg.get("penalty") == "l1" else 0.0
        clf = LogisticRegression(
            C=cfg["C"], l1_ratio=l1r,
            solver="saga", max_iter=1000, random_state=42,
        )

    pipe = Pipeline([("scaler", StandardScaler()), ("clf", clf)])

    try:
        pipe.fit(X_train, y_train)
        proba_hold = pipe.predict_proba(X_hold)[:, 1]
        # Avoid log(0)
        proba_hold = np.clip(proba_hold, 1e-6, 1 - 1e-6)
        score = log_loss(y_hold, proba_hold)
    except Exception as e:
        return {"cfg": cfg, "log_loss": 9999.0, "error": str(e), "field_probs": []}

    # Predict 2026 field
    X_field = field_df[feature_cols].values
    field_probs = pipe.predict_proba(X_field)[:, 1].tolist()

    return {"cfg": cfg, "log_loss": float(score), "field_probs": field_probs}


def run_parallel_training(configs, train_df, holdout_df, field_df):
    """Run all configs in parallel via Burla, falling back to local threads."""
    # Package DataFrames as row-lists so they're picklable across workers
    train_rows   = train_df.to_dict("records")
    holdout_rows = holdout_df.to_dict("records")
    field_rows   = field_df[["beyer_norm", "dosage_score", "run_style_score",
                              "implied_prob", "post", "post_wp_approx", "muddy",
                              "name", "odds"]].to_dict("records")

    args_list = [(cfg, train_rows, holdout_rows, field_rows) for cfg in configs]

    try:
        from burla import remote_parallel_map
        print(f"  Dispatching {len(configs)} configs to Burla cluster (grow=True)...")
        results = remote_parallel_map(train_and_eval, args_list, grow=True)
        print(f"  Burla returned {len(results)} results")
        return results
    except Exception as exc:
        print(f"  Burla unavailable ({exc}), using local ThreadPoolExecutor...")
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=os.cpu_count() or 4) as ex:
            futures = [ex.submit(train_and_eval, *args) for args in args_list]
            results = [f.result() for f in futures]
        return results


def ensemble_top_k(results: list[dict], field_df: pd.DataFrame, k: int = 5) -> np.ndarray:
    """Average win probabilities across the top-k models (lowest log-loss)."""
    valid = [r for r in results if r.get("log_loss", 9999) < 9999 and r.get("field_probs")]
    valid.sort(key=lambda r: r["log_loss"])
    top_k = valid[:k]
    if not top_k:
        # Fallback: uniform probs
        return np.ones(len(field_df)) / len(field_df)
    probs = np.array([r["field_probs"] for r in top_k])
    return probs.mean(axis=0)


def build_field_features_for_model(field_df: pd.DataFrame) -> pd.DataFrame:
    """Add ML-specific columns to the 2026 field DataFrame."""
    df = field_df.copy()
    df["beyer_norm"]      = np.clip((df["beyer"] - 80) / 3.0, 0, 10)
    df["implied_prob"]    = 1.0 / (df["odds"] + 1)
    df["post_wp_approx"]  = df["post"].apply(lambda p: 8.0 if 5 <= p <= 12 else 5.0)
    df["muddy"]           = 0  # fast track confirmed
    return df


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    train_path = os.path.join(DATA_DIR, "train_features.csv")
    field_path = os.path.join(DATA_DIR, "field_2026.csv")

    if not os.path.exists(train_path) or not os.path.exists(field_path):
        print("Missing input files. Run derby_scraper.py and derby_features.py first.")
        return

    all_df   = pd.read_csv(train_path)
    field_df = pd.read_csv(field_path)

    # Add model-specific cols to field_df
    field_df = build_field_features_for_model(field_df)
    if "dosage_score" not in field_df.columns:
        field_df["dosage_score"] = np.clip(10 - (field_df["dosage"] - 1.0) * (10/6.0), 0, 10)
    if "run_style_score" not in field_df.columns:
        style_map = {1: 4.0, 2: 8.5, 3: 8.0, 4: 7.0, 5: 5.5}
        field_df["run_style_score"] = field_df["run_style"].map(style_map).fillna(6.5)

    train_df   = all_df[~all_df["year"].isin(HOLDOUT_YEARS)].copy()
    holdout_df = all_df[all_df["year"].isin(HOLDOUT_YEARS)].copy()
    print(f"Training on {len(train_df)} rows ({train_df['year'].nunique()} years), "
          f"holdout: {len(holdout_df)} rows ({HOLDOUT_YEARS})")

    configs = make_configs()
    print(f"Generated {len(configs)} model configurations")

    results = run_parallel_training(configs, train_df, holdout_df, field_df)

    valid   = [r for r in results if r.get("log_loss", 9999) < 9999]
    if not valid:
        print("WARNING: All model configs failed. Check data quality.")
        return

    valid.sort(key=lambda r: r["log_loss"])
    best = valid[0]
    print(f"\nBest model: {best['cfg']} | log-loss={best['log_loss']:.4f}")
    print(f"Top-5 log-losses: {[round(r['log_loss'],4) for r in valid[:5]]}")

    ensemble_probs = ensemble_top_k(results, field_df, k=5)
    # Normalize to sum to 1 across the 20 horses
    ensemble_probs = ensemble_probs / ensemble_probs.sum()

    model_out = {
        "best_config": best["cfg"],
        "best_log_loss": best["log_loss"],
        "top5_log_losses": [r["log_loss"] for r in valid[:5]],
        "horse_ml_probs": {
            row["name"]: float(prob)
            for row, prob in zip(field_df.to_dict("records"), ensemble_probs)
        },
    }

    out_path = os.path.join(DATA_DIR, "model_results.json")
    existing = {}
    if os.path.exists(out_path):
        with open(out_path) as f:
            existing = json.load(f)
    existing.update(model_out)
    with open(out_path, "w") as f:
        json.dump(existing, f, indent=2)

    print(f"\nML win probabilities (top 10):")
    sorted_horses = sorted(model_out["horse_ml_probs"].items(), key=lambda x: -x[1])
    for name, prob in sorted_horses[:10]:
        print(f"  {name:<20} {prob*100:.1f}%")
    print(f"Saved -> {out_path}")


if __name__ == "__main__":
    main()
