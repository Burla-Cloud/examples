# Kentucky Derby 2026 Prediction — and an Honest Audit

> **One trillion Monte Carlo simulations + a 2,000-permutation null test, both on Burla, both in minutes.** A worked example of using `remote_parallel_map` for a real prediction task — and then using the same cluster to *audit* the prediction and call out where the methodology is weak.

**Live site:** [jackburla.github.io/BurlaKentuckyDerby](https://jackburla.github.io/BurlaKentuckyDerby/)

## What this example shows

This is a complete prediction-and-audit pipeline for the 2026 Kentucky Derby, with two Burla operations doing the heavy lifting:

1. **1,000,000,000,000 Monte Carlo race simulations** (`derby/derby_trillion.py`) — 50,000 workers × 20,000,000 sims each, dispatched as a single `remote_parallel_map` call. Wall time on a 65-node × 32-vCPU cluster: **18.3 minutes**.

2. **2,000 permutation tests on the model's "best of 5,000 weight combinations" backtest** (`derby/derby_audit.py`) — each permutation shuffles the historical winner labels and re-runs the same 5,000-weight Dirichlet search to ask: *what does the best score look like under random labels?* If the model is real, shuffled-label runs should score lower. Wall time: **13.8 seconds**.

The audit found that the headline `22/40` backtest score is hit or exceeded by **87.4% of permuted-label runs** — the search procedure has so much capacity that high scores are the default. That finding is published in the live site's "Methodology audit" section.

## File layout

```
kentucky-derby-prediction/
├── README.md                            # you are here
├── requirements.txt
├── derby/
│   ├── derby_trillion.py                # 1 trillion Monte Carlo sims on Burla
│   ├── derby_audit.py                   # 2K-permutation null test on Burla
│   ├── derby_scraper.py                 # historical data (note: see audit)
│   ├── derby_features.py                # 2026 field features
│   ├── derby_model.py                   # 164-config ML grid (note: see audit)
│   ├── derby_sensitivity.py             # 5,000-Dirichlet weight search
│   ├── derby_montecarlo.py              # original 1M MC (predecessor of trillion)
│   ├── update_website.py                # patches docs/index.html with results
│   └── data/
│       ├── trillion_results.json        # output of derby_trillion.py
│       ├── audit_results.json           # output of derby_audit.py
│       ├── model_results.json           # ML + sensitivity outputs
│       ├── field_2026.csv               # generated 2026 field features
│       └── historical_results.csv       # 2000-2025 top-4 finishers
└── docs/
    └── index.html                       # the live demo website (single file)
```

## The 1T Monte Carlo: idiomatic Burla

This is the canonical "split a big stochastic job into many independent chunks" pattern, scaled up.

```python
from burla import remote_parallel_map

N_WORKERS       = 50_000
SIMS_PER_WORKER = 20_000_000          # 50K × 20M = 1,000,000,000,000 sims
CHUNK_SIZE      = 100_000

def simulate_race_batch(log_probs_list, sims_per_worker, chunk_size, seed):
    """Run sims_per_worker races on one Burla worker.

    Fully vectorized via the Gumbel-max trick: to sample k items
    without replacement from categorical(softmax(logits)),
        keys  = logits + Gumbel(0, 1)
        order = argsort(-keys)[:k]
    No per-sim Python loop.
    """
    import numpy as np
    rng = np.random.default_rng(seed)
    log_probs = np.array(log_probs_list, dtype=np.float64)
    n_horses  = len(log_probs)
    counts    = np.zeros((n_horses, 4), dtype=np.int64)

    for _ in range(sims_per_worker // chunk_size):
        noise        = rng.standard_normal((chunk_size, n_horses)) * 1.8
        noisy        = log_probs + noise
        log_p        = noisy - np.log(np.exp(noisy - noisy.max(axis=1, keepdims=True))
                                       .sum(axis=1, keepdims=True)) - noisy.max(axis=1, keepdims=True)
        gumbel       = rng.gumbel(0.0, 1.0, (chunk_size, n_horses))
        keys         = log_p + gumbel
        part         = np.argpartition(-keys, 4, axis=1)[:, :4]
        rank         = np.argsort(-keys[np.arange(chunk_size)[:, None], part], axis=1)
        order        = part[np.arange(chunk_size)[:, None], rank]
        for pos in range(4):
            np.add.at(counts[:, pos], order[:, pos], 1)

    return {"counts": counts.tolist(), "n_sims": sims_per_worker}


args_list = [(log_probs, SIMS_PER_WORKER, CHUNK_SIZE, seed)
             for seed in range(N_WORKERS)]

results = list(remote_parallel_map(
    simulate_race_batch, args_list,
    func_cpu=1, func_ram=2,
    max_parallelism=2_081,            # full CPUS_PER_VM_FAMILY quota
    grow=False,                        # cluster pre-provisioned at quota
    generator=True,                    # stream + aggregate as workers finish
))
```

Key things this demo shows:

- **Single dispatch beats batched dispatch.** An earlier version of this script split the 50,000 workers into 50 batches of 1,000 and paid the function-upload + queue-warmup cost 50 times, plus hit `AllNodesBusy` between batches. Switching to one `remote_parallel_map(work, all_50_000_inputs)` call (per the [burla-agent-starter-kit](https://github.com/Burla-Cloud/burla-agent-starter-kit) Recipe #2 pattern) collapsed 167 minutes of expected wall time to 18.
- **Pre-provisioning at the quota beats `grow=True`.** With the cluster sized to the GCP `CPUS_PER_VM_FAMILY` quota, `grow=True` would push past it and trigger `QUOTA_EXCEEDED`. Setting `grow=False` lets Burla queue work onto the existing capacity.
- **Vectorization matters more than worker count.** The Gumbel-max trick replaces the per-sim Python loop with one matrix op per 100K-row chunk. Without it, 1T sims would be infeasible at any worker count.

## The audit: 2,000 permutations in 14 seconds

The same cluster is just as good at *invalidating* a model as building one. The audit script (`derby/derby_audit.py`) takes the published "best of 5,000 weight combinations" result (22/40) and asks the obvious question:

> If you shuffle who actually won and re-run the same search, what does the best score look like under random labels?

```python
from burla import remote_parallel_map

N_PERMUTATIONS = 2_000
N_DIRICHLET    = 5_000

args_list = [(seed, N_DIRICHLET, fields_serializable)
             for seed in range(N_PERMUTATIONS)]

results = list(remote_parallel_map(
    perm_null_worker, args_list,
    func_cpu=1, func_ram=2,
    max_parallelism=2_081,
    grow=False,
))

null_scores = np.array([r["best_score"] for r in results])
p_value = (null_scores >= 22).mean()
```

Real output from this run:

```
Always pick the favorite       :  6/40
Always pick the highest Beyer  : 10/40
Random pick (closed-form)      : 15.3/40
Published model                : 22/40
PERMUTATION NULL median        : 30/40
P(null >= 22)                  : 87.4%
Permutation null max           : 40/40
```

The model's headline 22/40 is *below* the median null score of 30/40. The "best of 5,000 random Dirichlet samples" search has so much capacity that it fits any 4-race outcome to within a few points — including completely random labels. That is now disclosed in red on the live site.

This is what running 2,081 parallel CPUs for 14 seconds can do for you when you point them at *checking your own work* instead of running another simulation.

## Run it yourself

```bash
pip install -r requirements.txt
burla login

# 1 trillion Monte Carlo sims (~18 min on a 2,080-vCPU cluster)
python derby/derby_trillion.py

# 2,000 permutations (~14 seconds)
python derby/derby_audit.py
```

The trillion run requires a cluster pre-provisioned at the GCP `CPUS_PER_VM_FAMILY` quota (recommended: `n4-standard-32 × 65 = 2,080 vCPU`, 30-minute idle timeout). The audit will run on whatever capacity is available.

## What this example is NOT

The pipeline contains real methodological problems that more compute does not fix. They are documented in detail in the live site's "Methodology audit" section and called out by the audit script:

- Beyer figures for non-winners in the historical training data are **synthesized** from finish position (`derby_scraper.py:194` — `beyer = winner_beyer - 3 × (finish - 1)`).
- The `derby_scraper.py` "Burla scrape" never wrote scraped data anywhere — the saved DataFrame is hard-coded `FALLBACK_DATA`.
- The 5,000-combo sensitivity backtest runs against hand-typed feature scores in `derby_sensitivity.py BACKTEST_FIELDS`, not features computed from the same pipeline.
- The ML training set has `implied_prob = 1/(odds+1)` as a feature — the classifier mostly relearns the market.
- The ML output is essentially uniform — Robusta (50-1) gets 10.6%, Renegade (4-1 morning-line favorite) gets 3.6%.

The example is included as-is *with* its broken parts because the audit script and the audit section on the live site explicitly reference these flaws. The point of this example is showing what running 2,081 parallel CPUs at honest self-criticism looks like, not pretending the model is sound.

## License

MIT. Built on [Burla](https://docs.burla.dev). Live at [jackburla.github.io/BurlaKentuckyDerby](https://jackburla.github.io/BurlaKentuckyDerby/). For entertainment purposes only — not financial advice.
