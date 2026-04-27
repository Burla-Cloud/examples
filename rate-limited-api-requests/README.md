# Run Millions of API Requests in Parallel with Rate Limiting in Python

Fire 2,000,000 HTTP requests against a rate-limited API using 1,000 workers at the same time, with per-worker local rate limits and 429 retry.

## The Problem

You need to hit an API 2 million times: enrich a user list, backfill records, call an LLM, scrape a price feed. The API allows 1,000 requests per second total.

`asyncio` on one machine tops out at a few thousand concurrent connections and dies if your laptop flakes. A thread pool is worse. Running 1,000 VMs yourself means a queue, a deployer, and a retry layer. A naive parallel `for` loop blows past the rate limit on the first second and gets you banned.

You want exactly N workers running at once, each doing a small share of the work, each respecting the global rate limit.

## The Solution (Burla)

`remote_parallel_map` runs your function on 1,000 workers at the same time. Cap concurrency with `max_parallelism=1000`. Chunk your 2M requests into 2,000 tasks of 1,000 requests each. Each worker handles its local rate (1 req/sec) so the global rate is ~1,000 req/sec. On 429, back off in-function.

No queue service, no worker processes, no orchestration.

## Example

```python
import time
import httpx
from burla import remote_parallel_map

with open("user_ids.txt") as f:
    user_ids = [line.strip() for line in f if line.strip()]

CHUNK = 1000
chunks = [user_ids[i : i + CHUNK] for i in range(0, len(user_ids), CHUNK)]
print(f"{len(user_ids)} requests across {len(chunks)} tasks")


def enrich_chunk(ids: list[str]) -> list[dict]:
    import time
    import httpx

    out = []
    with httpx.Client(timeout=30.0, headers={"Authorization": "Bearer $API_KEY"}) as client:
        for uid in ids:
            for attempt in range(5):
                r = client.get(f"https://api.example.com/v1/users/{uid}")
                if r.status_code == 429:
                    wait = float(r.headers.get("Retry-After", 2 ** attempt))
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                out.append({"user_id": uid, **r.json()})
                break
            time.sleep(1.0)  # 1 req/sec per worker, x 1000 workers = ~1000 req/sec global
    return out


# Burla grows the cluster on demand and caps live workers at 1,000 => global ~1,000 req/sec
results = remote_parallel_map(
    enrich_chunk,
    chunks,
    func_cpu=1,
    func_ram=2,
    max_parallelism=1000,
    generator=True,
    grow=True,
)

import json
with open("enriched.jsonl", "w") as f:
    for chunk_result in results:
        for row in chunk_result:
            f.write(json.dumps(row) + "\n")
```

## Why This Is Better

**vs Ray** - Ray needs a cluster and actor pool. You still have to write the rate-limit logic and retry loop. You still have to manage the head node.

**vs Dask** - Dask is built for DataFrames, not HTTP fan-out. You end up writing custom `delayed` tasks and a scheduler that doesn't really help with 429 backoff.

**vs AWS Batch / Lambda fan-out** - Batch cold starts in minutes per job. Lambda has 15-minute timeouts and per-account concurrency limits you have to raise with a support ticket. Burla starts tasks in seconds and gives you `max_parallelism` as a first-class knob.

## How It Works

You hand Burla a function that takes one chunk of IDs and a list of 2,000 chunks. It holds `max_parallelism=1000` workers live at once. Each worker runs `enrich_chunk` back-to-back on its assigned chunks. `generator=True` streams results as they finish so you can write them to disk incrementally.

## When To Use This

- Enriching 100k+ records via a third-party API.
- Backfilling millions of webhook events you have to re-fetch.
- Bulk LLM calls where you need per-provider concurrency.
- Web API scraping when the provider publishes a rate limit.

