import math
import numpy as np
from burla import remote_parallel_map

TOTAL = 1_000_000_000
N_CHUNKS = 2_000
PER_CHUNK = TOTAL // N_CHUNKS

params = {"S0": 100.0, "K": 95.0, "T": 1.0, "r": 0.01, "sigma": 0.3}
tasks = [(i, PER_CHUNK, params) for i in range(N_CHUNKS)]


def run_chunk(chunk_id: int, n: int, p: dict) -> dict:

    rng = np.random.default_rng(seed=42 + chunk_id)

    Z = rng.standard_normal(n)
    ST = p["S0"] * np.exp((p["r"] - 0.5 * p["sigma"] ** 2) * p["T"] + p["sigma"] * np.sqrt(p["T"]) * Z)
    payoff = np.maximum(ST - p["K"], 0.0) * np.exp(-p["r"] * p["T"])

    return {
        "chunk_id": chunk_id,
        "n": n,
        "sum": float(payoff.sum()),
        "sum_sq": float((payoff ** 2).sum()),
    }


# 2,000 chunks -> Burla grows the cluster on demand and runs them in parallel
results = remote_parallel_map(run_chunk, tasks, func_cpu=1, func_ram=2, grow=True)

total_n = sum(r["n"] for r in results)
total_sum = sum(r["sum"] for r in results)
total_sum_sq = sum(r["sum_sq"] for r in results)

mean = total_sum / total_n
var = (total_sum_sq / total_n) - mean ** 2
se = math.sqrt(var / total_n)

print(f"price estimate: {mean:.6f}  stderr: {se:.6f}  (n = {total_n:,})")
