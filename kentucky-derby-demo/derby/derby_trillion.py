"""
derby_trillion.py
-----------------
Run 1,000,000,000,000 (1 trillion) Kentucky Derby race simulations on Burla.

Strategy
--------
- 50,000 Burla workers x 20,000,000 sims each = 1,000,000,000,000 total.
- Each worker processes 20M sims in chunks of 100,000 (200 chunks/worker).
- Fully vectorized NumPy: Gumbel-max trick replaces the per-sim Python loop
  used in the 1M version.
- Single `remote_parallel_map` call with all 50,000 inputs, the idiomatic
  Burla pattern (per the burla-agent-starter-kit). Streamed via
  `generator=True` so we aggregate counts as workers finish, persisting a
  partial snapshot every PERSIST_EVERY workers. If the run is killed, the
  partial snapshot is the real data so far.
- NO local fallback: if Burla cannot run the work, fail loudly so the
  operator can fix the cluster.

The Gumbel-max trick
--------------------
To sample k items without replacement from categorical(softmax(logits)):
  keys = logits + Gumbel(0, 1) noise
  order = argsort(-keys)[:k]

Inputs
------
Same log_probs as the 1M run: log(softmax((final_score - mean) / 5.0)).
"""

import sys
import os
import json
import time
import math
import traceback
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Horse data (canvas scores = final_score from compute_final_scores) ───────
HORSES = [
    {"post": 16, "name": "Further Ado",     "odds": "6.5-1",  "beyer": 106,
     "dosage": 2.08,  "style": "Press", "trainerDW": 1, "jockeyDW": 3,
     "score": 50.7,   "impliedPct": 13.3},
    {"post": 11, "name": "Chief Wallabee",  "odds": "8-1",    "beyer":  93,
     "dosage": 1.92,  "style": "Press", "trainerDW": 1, "jockeyDW": 1,
     "score": 49.3,   "impliedPct": 11.1},
    {"post":  8, "name": "The Puma",        "odds": "10-1",   "beyer":  99,
     "dosage": 2.47,  "style": "Close", "trainerDW": 1, "jockeyDW": 1,
     "score": 42.8,   "impliedPct":  9.1},
    {"post": 13, "name": "Emerging Market", "odds": "15-1",   "beyer":  98,
     "dosage": 2.20,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 1,
     "score": 40.1,   "impliedPct":  6.2},
    {"post":  5, "name": "Commandment",     "odds": "6.5-1",  "beyer": 104,
     "dosage": 3.44,  "style": "Stalk", "trainerDW": 1, "jockeyDW": 1,
     "score": 38.0,   "impliedPct": 13.3},
    {"post":  7, "name": "So Happy",        "odds": "6-1",    "beyer":  98,
     "dosage": 7.00,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 2,
     "score": 36.8,   "impliedPct": 14.3},
    {"post": 15, "name": "Six Speed",       "odds": "50-1",   "beyer":  91,
     "dosage": 5.00,  "style": "Pace",  "trainerDW": 0, "jockeyDW": 1,
     "score": 35.6,   "impliedPct":  2.0},
    {"post": 10, "name": "Incredibolt",     "odds": "20-1",   "beyer":  91,
     "dosage": 3.00,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 0,
     "score": 30.7,   "impliedPct":  4.8},
    {"post": 20, "name": "Robusta",         "odds": "50-1",   "beyer":  93,
     "dosage": 3.44,  "style": "Pace",  "trainerDW": 2, "jockeyDW": 0,
     "score": 30.1,   "impliedPct":  2.0},
    {"post":  2, "name": "Albus",           "odds": "30-1",   "beyer":  95,
     "dosage": 2.60,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 0,
     "score": 28.2,   "impliedPct":  3.2},
    {"post":  3, "name": "Intrepido",       "odds": "50-1",   "beyer":  94,
     "dosage": 3.40,  "style": "Press", "trainerDW": 0, "jockeyDW": 0,
     "score": 27.5,   "impliedPct":  2.0},
    {"post":  4, "name": "Litmus Test",     "odds": "30-1",   "beyer":  96,
     "dosage": 3.36,  "style": "Stalk", "trainerDW": 6, "jockeyDW": 0,
     "score": 26.9,   "impliedPct":  3.2},
    {"post":  9, "name": "Wonder Dean",     "odds": "30-1",   "beyer":  95,
     "dosage": 2.11,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 0,
     "score": 26.6,   "impliedPct":  3.2},
    {"post":  1, "name": "Renegade",        "odds": "4-1",    "beyer":  97,
     "dosage": 3.00,  "style": "Close", "trainerDW": 2, "jockeyDW": 0,
     "score": 26.3,   "impliedPct": 18.2},
    {"post": 14, "name": "Pavlovian",       "odds": "30-1",   "beyer":  93,
     "dosage": 1.29,  "style": "Pace",  "trainerDW": 2, "jockeyDW": 0,
     "score": 23.4,   "impliedPct":  3.2},
    {"post": 12, "name": "Potente",         "odds": "20-1",   "beyer":  96,
     "dosage": 3.44,  "style": "Pace",  "trainerDW": 6, "jockeyDW": 0,
     "score": 23.2,   "impliedPct":  4.8},
    {"post": 18, "name": "Great White",     "odds": "50-1",   "beyer":  94,
     "dosage": 3.00,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 0,
     "score": 22.2,   "impliedPct":  2.0},
    {"post":  6, "name": "Danon Bourbon",   "odds": "20-1",   "beyer":  94,
     "dosage": 1.86,  "style": "Stalk", "trainerDW": 0, "jockeyDW": 0,
     "score": 20.4,   "impliedPct":  4.8},
    {"post": 19, "name": "Ocelli",          "odds": "50-1",   "beyer":  81,
     "dosage": 1.73,  "style": "Deep",  "trainerDW": 0, "jockeyDW": 0,
     "score": 16.2,   "impliedPct":  2.0},
    {"post": 17, "name": "Golden Tempo",    "odds": "30-1",   "beyer":  90,
     "dosage": 3.00,  "style": "Deep",  "trainerDW": 0, "jockeyDW": 0,
     "score":  9.9,   "impliedPct":  3.2},
]

NOISE_SIGMA     = 1.8

# ── Scale settings ────────────────────────────────────────────────────────
N_WORKERS       = 50_000        # 50,000 workers × 20M sims = 1T total
SIMS_PER_WORKER = 20_000_000
CHUNK_SIZE      = 100_000
TOTAL_SIMS      = N_WORKERS * SIMS_PER_WORKER  # 1,000,000,000,000

# Burla dispatch settings (idiomatic per burla-agent-starter-kit Recipe #2:
# pass all inputs in one call). Generator mode lets us aggregate counts as
# workers complete and persist progress incrementally.
MAX_PARALLELISM = 2_081         # full GCP CPUS_PER_VM_FAMILY quota
FUNC_CPU        = 1             # 1 vCPU per worker → up to 2,081 concurrent
FUNC_RAM        = 2             # 2GB is plenty for 100K x 20-horse arrays
GROW_CLUSTER    = False         # cluster is pre-provisioned at 65 × 32vCPU
                                 # = 2,080 vCPU which is *exactly* the GCP
                                 # CPUS_PER_VM_FAMILY quota of 2,081.
                                 # grow=True asks for MORE, blowing the
                                 # quota. Don't grow; queue on existing.
PERSIST_EVERY   = 500           # write a partial snapshot every N workers


def _compute_log_probs(horses):
    """Reproduce the exact log_probs used in derby_montecarlo.py."""
    scores = np.array([h["score"] for h in horses], dtype=np.float64)
    exp_s = np.exp((scores - scores.mean()) / 5.0)
    win_probs = exp_s / exp_s.sum()
    return np.log(win_probs + 1e-9).tolist()


def simulate_race_batch(log_probs_list: list, sims_per_worker: int,
                        chunk_size: int, seed: int) -> dict:
    """
    Run sims_per_worker races on one Burla worker. Fully vectorized via the
    Gumbel-max trick.
    """
    import subprocess, sys as _sys
    try:
        subprocess.check_call([_sys.executable, "-m", "pip", "install", "numpy", "-q"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

    import numpy as np

    rng = np.random.default_rng(seed)
    n_horses = len(log_probs_list)
    log_probs = np.array(log_probs_list, dtype=np.float64)
    counts = np.zeros((n_horses, 4), dtype=np.int64)

    n_full_chunks, remainder = divmod(sims_per_worker, chunk_size)

    def _process_chunk(size: int) -> None:
        noise = rng.standard_normal((size, n_horses)) * NOISE_SIGMA
        noisy = log_probs + noise

        row_max = noisy.max(axis=1, keepdims=True)
        log_sum_exp = np.log(np.exp(noisy - row_max).sum(axis=1, keepdims=True)) + row_max
        log_p = noisy - log_sum_exp

        gumbel_noise = rng.gumbel(0.0, 1.0, (size, n_horses))
        keys = log_p + gumbel_noise

        part = np.argpartition(-keys, 4, axis=1)[:, :4]
        top_keys = -keys[np.arange(size)[:, None], part]
        rank_order = np.argsort(top_keys, axis=1)
        order = part[np.arange(size)[:, None], rank_order]

        for pos in range(4):
            np.add.at(counts[:, pos], order[:, pos], 1)

    for _ in range(n_full_chunks):
        _process_chunk(chunk_size)
    if remainder:
        _process_chunk(remainder)

    return {"counts": counts.tolist(), "n_sims": sims_per_worker}


def kelly_fraction(win_prob: float, odds_str: str) -> float:
    try:
        num, denom = odds_str.split("-")
        b = float(num) / float(denom)
    except Exception:
        return 0.0
    p, q = win_prob, 1.0 - win_prob
    k = (b * p - q) / b if b > 0 else 0.0
    return round(max(0.0, min(k, 0.25)), 3)


def _build_snapshot(total_counts, total_sims, elapsed, n_workers_done):
    win_pct   = (total_counts[:, 0] / max(total_sims, 1) * 100)
    place_pct = ((total_counts[:, 0] + total_counts[:, 1]) / max(total_sims, 1) * 100)
    show_pct  = (total_counts[:, :3].sum(axis=1) / max(total_sims, 1) * 100)

    horses = []
    for i, h in enumerate(HORSES):
        wp = round(float(win_pct[i]), 4)
        pp = round(float(place_pct[i]), 4)
        sp = round(float(show_pct[i]), 4)
        kf = kelly_fraction(wp / 100, h["odds"])
        implied = h["impliedPct"]
        if wp > implied * 1.15:
            val = "BET"
        elif wp < implied * 0.85:
            val = "FADE"
        else:
            val = "FAIR"
        horses.append({**h, "winPct": wp, "placePct": pp, "showPct": sp,
                       "value": val, "kelly": kf})

    return {
        "total_sims":       int(total_sims),
        "elapsed_s":        round(elapsed, 2),
        "backend":          "Burla",
        "n_workers":        N_WORKERS,
        "n_workers_done":   n_workers_done,
        "noise_sigma":      NOISE_SIGMA,
        "horses":           horses,
    }


def main():
    log_probs = _compute_log_probs(HORSES)
    n_horses  = len(HORSES)

    print(f"1 Trillion Simulation Kentucky Derby Model")
    print(f"==========================================")
    print(f"Workers       : {N_WORKERS:,}")
    print(f"Sims/worker   : {SIMS_PER_WORKER:,}")
    print(f"Chunk size    : {CHUNK_SIZE:,}")
    print(f"Total sims    : {TOTAL_SIMS:,}")
    print(f"Max parallel  : {MAX_PARALLELISM}")
    print(f"func_cpu      : {FUNC_CPU}, func_ram: {FUNC_RAM}G")
    print(f"Persist every : {PERSIST_EVERY:,} workers")
    print()

    args_list = [
        (log_probs, SIMS_PER_WORKER, CHUNK_SIZE, seed)
        for seed in range(N_WORKERS)
    ]

    out_path = os.path.join(DATA_DIR, "trillion_results.json")

    total_counts = np.zeros((n_horses, 4), dtype=np.int64)
    total_sims   = 0
    n_done       = 0
    t_start      = time.time()

    print(f"Dispatching {N_WORKERS:,} workers in a single Burla call "
          f"(idiomatic pattern). Streaming results...\n")

    from burla import remote_parallel_map
    stream = remote_parallel_map(
        simulate_race_batch, args_list,
        func_cpu=FUNC_CPU, func_ram=FUNC_RAM,
        max_parallelism=MAX_PARALLELISM,
        grow=GROW_CLUSTER,
        generator=True,
        spinner=True,
    )

    last_log = t_start
    for r in stream:
        arr = np.array(r["counts"], dtype=np.int64)
        if arr.shape == total_counts.shape:
            total_counts += arr
            total_sims   += r["n_sims"]
            n_done       += 1

        if n_done % PERSIST_EVERY == 0 or n_done == N_WORKERS:
            elapsed = time.time() - t_start
            snap = _build_snapshot(total_counts, total_sims, elapsed, n_done)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(snap, f, indent=2)
            pct = total_sims / TOTAL_SIMS * 100
            rate = total_sims / max(elapsed, 1)
            eta_s = (TOTAL_SIMS - total_sims) / max(rate, 1)
            print(f"  [{elapsed/60:6.1f}min] {n_done:>6,}/{N_WORKERS:,} workers, "
                  f"{total_sims:>15,} sims ({pct:6.2f}%), "
                  f"rate={rate/1e9:.2f}B/s, ETA {eta_s/60:.1f}min")
            last_log = time.time()

    elapsed = time.time() - t_start
    snap = _build_snapshot(total_counts, total_sims, elapsed, n_done)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snap, f, indent=2)

    print(f"\nSaved -> {out_path}")
    print(f"\n{'Horse':<22} {'Win%':>7} {'Place%':>8} {'Show%':>7} {'Value':>6}  Kelly")
    print("-" * 62)
    for h in snap["horses"]:
        print(f"{h['name']:<22} {h['winPct']:>7.3f}% {h['placePct']:>7.3f}% "
              f"{h['showPct']:>7.3f}%  {h['value']:>5}  {h['kelly']:.3f}")

    print(f"\nDone: {total_sims:,} simulations in {elapsed:.1f}s via Burla")
    return snap


if __name__ == "__main__":
    main()
