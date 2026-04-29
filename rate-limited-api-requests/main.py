import time
import httpx
from burla import remote_parallel_map

with open("user_ids.txt") as f:
    user_ids = [line.strip() for line in f if line.strip()]

CHUNK = 1000
chunks = [user_ids[i : i + CHUNK] for i in range(0, len(user_ids), CHUNK)]
print(f"{len(user_ids)} requests across {len(chunks)} tasks")


def enrich_chunk(ids: list[str]) -> list[dict]:

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


# 2,000 tasks, capped to 1,000 workers running in parallel => global ~1,000 req/sec
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
