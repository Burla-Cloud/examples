# Burla GPU vector-embedding demo

Embed a slice of English Wikipedia (`wikimedia/wikipedia` `20231101.en`) on Burla A100 workers with `BAAI/bge-large-en-v1.5`, then run a semantic-search query over the results.

Validated end-to-end at 50,000 articles across 50 shards on 8 A100s. Total wall time on warm cluster: ~10 min including boot; Stage 2 embedding itself is ~3-4 min.

## What you get

- `Dockerfile` — `pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime` + `sentence-transformers` + `datasets` + `BAAI/bge-large-en-v1.5` weights baked in (~7 GB). Pushed to Docker Hub as `jakezuliani/burla-embedder:latest`.
- `demo.py` — the runnable pipeline.
- `demo.ipynb` — same flow as a notebook for step-by-step walkthroughs.

## Prereqs

- `burla login` done.
- GCP A100 (a2-highgpu-1g) quota in `us-central1` (or whichever region your cluster is in).
- `docker login` done (only if you want to rebuild the image).
- **Python 3.11 on the client.** Burla enforces exact `major.minor` match between client and worker. The image ships Python 3.11.9, so the client must be 3.11.x. See "Gotchas".

## Run

```bash
# 500 articles, 2 shards, CPU nodes only (validation)
DEMO_TOTAL_ARTICLES=500 DEMO_ARTICLES_PER_SHARD=250 DEMO_STAGE=download \
    /path/to/python3.11 demo.py

# Full default: 50,000 articles, 50 shards, up to 8 A100s
/path/to/python3.11 demo.py
```

Expected output at the end:

```
Top 5 results for: 'Who invented the telephone?'

  1. [0.8161] Alexander Graham Bell
  2. [0.7474] Thomas A. Watson
  3. [0.6338] André-Marie Ampère
  4. [0.6276] Alessandro Volta
  5. [0.5990] Sidney Howe Short
```

## Environment variables

| Var | Default | Meaning |
|-----|---------|---------|
| `DEMO_STAGE` | `all` | One of `all`, `download`, `embed`, `search`. |
| `DEMO_TOTAL_ARTICLES` | `50000` | Total Wikipedia articles to embed. |
| `DEMO_ARTICLES_PER_SHARD` | `1000` | Articles per shard (so `N_SHARDS = TOTAL/PER_SHARD`). |
| `DEMO_MAX_GPU_PARALLELISM` | `8` | Cap concurrent A100s Burla is allowed to grow to. |
| `DEMO_MAX_CPU_PARALLELISM` | `N_SHARDS` | Cap concurrent CPU workers for the download stage. |
| `DEMO_QUERY` | `Who invented the telephone?` | Search query. |
| `DEMO_TOP_K` | `5` | Number of results to show. |
| `DEMO_SHARD_LIMIT` | (unset) | If set, use only the first N existing text shards (useful for stage-level testing). |
| `GOOGLE_CLOUD_PROJECT` | (auto-detected) | GCP project. Defaults to `google.auth.default()` (your `gcloud config` project). Used for GCS bucket name and Firestore lookups. |

## Pipeline

The demo is 4 stages, all orchestrated by `main()`:

1. **Stage 1 — download (CPU).** Each worker downloads one Wikipedia parquet file from HuggingFace's CDN, takes the first `ARTICLES_PER_SHARD` rows, writes `shard-{i}.jsonl` to `/workspace/shared/vector_embeddings_demo/texts/`.
2. **Stage 2 — embed (GPU A100).** Each worker reads its JSONL shard, uses `torch` + `sentence_transformers`, loads `bge-large` into a module-level `cache` on first call, batch-embeds (`batch_size=64`, `normalize_embeddings=True`), writes `emb-{i}.npy` + `ids-{i}.json` to `/workspace/shared/vector_embeddings_demo/embeddings/`.
3. **Stage 3 — query embed (GPU A100).** One `remote_parallel_map` call on one A100 to embed the query string (keeps the client free of torch/CUDA).
4. **Stage 4 — local search (client).** Downloads all `.npy` + `.json` shards from the GCS bucket (not the worker-side `/workspace/shared` path — the client doesn't mount that), concatenates into one matrix, computes cosine similarity against the query embedding, prints top-K titles with dedup.

Each stage can be run independently via `DEMO_STAGE=download|embed|search`. `embed` and `search` rebuild their inputs from whatever's already in the GCS bucket.

## Gotchas worth knowing

**1. Python-version match between client and image is exact.**
Burla rejects job assignment with HTTP 409 "No compatible containers" when the client's `major.minor` doesn't match any worker's. The image ships Python 3.11.9. Run the client with Python 3.11.x. A mismatch looks like:

```
Node burla-node-xxxxxxxx refused job assignment, removed from job.
```

**2. Use the GPU image for CUDA libraries.**
The image already includes CUDA-ready `torch` and `sentence_transformers`; using the client environment's CPU wheels would break the GPU path.

**3. Grown nodes get a 60-second inactivity timeout.**
`GROW_INACTIVITY_SHUTDOWN_TIME_SEC = 60` in `main_service`. If the real job doesn't reach the node within 60s of it becoming READY and idle, the node shuts itself down. In practice, stages 1→2→3 happen back-to-back so the timer never fires mid-run, but if you step through stages manually, each new stage may have to re-boot nodes.

**4. `/workspace/shared` is only visible to workers.**
The client can't `Path(...).read_text()` on `/workspace/shared/...`. Stage 4's similarity search reaches the shards via the GCS API (`{PROJECT_ID}-burla-shared-workspace` bucket).

**5. With `N_SHARDS > 41` (the number of wiki parquet files), the current `download_shard` cycles back and re-reads the first rows of earlier files.**
That's why the search output uses title-level dedup. If you want strictly unique articles, keep `N_SHARDS ≤ 41` or modify `download_shard` to take a different per-shard offset within each parquet file.

## Rebuilding the image

Only needed if you change the Dockerfile:

```bash
docker buildx build \
    --platform=linux/amd64 \
    --push \
    -t jakezuliani/burla-embedder:latest \
    -f Dockerfile .
```

The `--platform=linux/amd64` is required because GCP VMs are x86_64 Linux and the client machine may be arm64 (Apple Silicon).

## Cost ballpark

With the defaults (50k articles, 8 A100s, us-central1):

- First run from a cold cluster: ~10 min. Dominated by ~5 min A100 boot + image pull.
- Re-run with nodes warm: ~3-4 min for stage 2 itself.
- 8 × `a2-highgpu-1g` on-demand ≈ $3.67/hr each in us-central1 ≈ **~$5 per full-scale run**.

Scaling knobs:

- Bigger corpus: raise `DEMO_TOTAL_ARTICLES`. `ARTICLES_PER_SHARD` can grow too; an A100 embeds a 1000-article shard in ~15-25s with `batch_size=64`.
- Faster: raise `DEMO_MAX_GPU_PARALLELISM` (needs A100 quota headroom).
- Cheaper sanity check: `DEMO_TOTAL_ARTICLES=500 DEMO_ARTICLES_PER_SHARD=250 DEMO_MAX_GPU_PARALLELISM=1` on one A100.
