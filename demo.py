"""Embed a slice of English Wikipedia on Burla A100 workers, then run a
semantic-search query over the results.

Stages:
    1. Stage 1 (CPU, parallel): each worker downloads one Wikipedia parquet
       shard from the HuggingFace CDN, takes the first ARTICLES_PER_SHARD rows,
       and writes them as JSONL to /workspace/shared.
    2. Stage 2 (GPU A100, parallel): each worker reads its JSONL, embeds all
       titles+texts with BAAI/bge-large-en-v1.5, writes embeddings as .npy and
       ids/titles as .json to /workspace/shared.
    3. Stage 3 (GPU A100, 1 call): embed the query string.
    4. Stage 4 (client): load all .npy shards, cosine-similarity top-K, print.

Run with Python 3.11 to match the image. Burla enforces exact major.minor
Python match between client and worker; the image is based on
pytorch/pytorch:2.4.0-cuda12.1-cudnn9-runtime which ships Python 3.11.

Top-level imports are intentionally minimal (no torch, no datasets, no numpy)
to stop Burla's package-sync from reinstalling client-side CPU wheels on top
of the CUDA wheels already in the image.
"""

import json
import os
from pathlib import Path

from burla import remote_parallel_map


IMAGE = "jakezuliani/burla-embedder:latest"
MODEL_NAME = "BAAI/bge-large-en-v1.5"
SHARED_ROOT = "/workspace/shared/vector_embeddings_demo"

N_PARQUET_FILES = 41  # wikimedia/wikipedia 20231101.en has 41 parquet shards
PARQUET_URL_TEMPLATE = (
    "https://huggingface.co/datasets/wikimedia/wikipedia/resolve/main/"
    "20231101.en/train-{shard_idx:05d}-of-00041.parquet"
)

# Tune these via env. Defaults target ~5 min on a single A100.
TOTAL_ARTICLES = int(os.environ.get("DEMO_TOTAL_ARTICLES", 50_000))
ARTICLES_PER_SHARD = int(os.environ.get("DEMO_ARTICLES_PER_SHARD", 1_000))
N_SHARDS = TOTAL_ARTICLES // ARTICLES_PER_SHARD
# Cap concurrent workers so the cluster doesn't provision one A100 per shard.
MAX_GPU_PARALLELISM = int(os.environ.get("DEMO_MAX_GPU_PARALLELISM", 8))
MAX_CPU_PARALLELISM = int(os.environ.get("DEMO_MAX_CPU_PARALLELISM", N_SHARDS))

# DEMO_STAGE: "all" | "download" | "embed" | "search"
DEMO_STAGE = os.environ.get("DEMO_STAGE", "all")

QUERY = os.environ.get("DEMO_QUERY", "Who invented the telephone?")
TOP_K = int(os.environ.get("DEMO_TOP_K", 5))

def _resolve_project_id():
    env = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if env:
        return env
    import google.auth

    _, project = google.auth.default()
    if project:
        return project
    msg = "Could not resolve a GCP project; set GOOGLE_CLOUD_PROJECT or run `gcloud config set project <id>`."
    raise RuntimeError(msg)


PROJECT_ID = _resolve_project_id()


def download_shard(shard_idx, articles_per_shard, shared_root):
    import io
    import json
    import urllib.request
    from pathlib import Path
    import pyarrow.parquet as pq

    parquet_url = PARQUET_URL_TEMPLATE.format(shard_idx=shard_idx % N_PARQUET_FILES)
    req = urllib.request.Request(parquet_url, headers={"User-Agent": "burla-demo/1.0"})
    with urllib.request.urlopen(req) as response:
        parquet_bytes = response.read()
    table = pq.read_table(io.BytesIO(parquet_bytes))
    n = min(articles_per_shard, len(table))
    table = table.slice(0, n)

    out_path = Path(shared_root) / "texts" / f"shard-{shard_idx:05d}.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        for row in table.to_pylist():
            record = {
                "id": row["id"],
                "title": row["title"],
                "text": (row.get("text") or "")[:2000],
            }
            f.write(json.dumps(record) + "\n")
    print(f"shard {shard_idx}: wrote {n} articles to {out_path}")
    return str(out_path)


# Module-level dict survives across calls on the same worker process,
# letting us load the model exactly once per A100.
cache = {}


def embed_shard(shard_path, model_name, shared_root):
    import json
    from pathlib import Path
    import numpy as np
    import torch
    from sentence_transformers import SentenceTransformer

    if "model" not in cache:
        print(f"loading {model_name} (cuda={torch.cuda.is_available()}) ...")
        cache["model"] = SentenceTransformer(model_name, device="cuda")
    model = cache["model"]

    ids, titles, texts = [], [], []
    for line in Path(shard_path).read_text().splitlines():
        row = json.loads(line)
        ids.append(row["id"])
        titles.append(row["title"])
        combined = f"{row['title']}\n\n{row['text']}"
        texts.append(combined)

    vecs = model.encode(
        texts,
        batch_size=64,
        normalize_embeddings=True,
        show_progress_bar=False,
        convert_to_numpy=True,
    ).astype("float32")

    shard_name = Path(shard_path).stem.replace("shard-", "")
    emb_dir = Path(shared_root) / "embeddings"
    emb_dir.mkdir(parents=True, exist_ok=True)
    emb_path = emb_dir / f"emb-{shard_name}.npy"
    ids_path = emb_dir / f"ids-{shard_name}.json"
    np.save(emb_path, vecs)
    ids_path.write_text(json.dumps({"ids": ids, "titles": titles}))
    print(f"shard {shard_name}: wrote {len(ids)} vectors to {emb_path.name}")
    return {"emb_path": str(emb_path), "ids_path": str(ids_path), "n": len(ids)}


def embed_query(query, model_name):
    import torch
    from sentence_transformers import SentenceTransformer

    if "model" not in cache:
        print(f"loading {model_name} (cuda={torch.cuda.is_available()}) ...")
        cache["model"] = SentenceTransformer(model_name, device="cuda")
    model = cache["model"]
    vec = model.encode(
        [query],
        batch_size=1,
        normalize_embeddings=True,
        show_progress_bar=False,
        convert_to_numpy=True,
    )[0].astype("float32")
    return vec.tolist()


def run_stage_download():
    print(f"--- Stage 1: Downloading {N_SHARDS} shards on CPU workers ---")
    download_inputs = [(i, ARTICLES_PER_SHARD, SHARED_ROOT) for i in range(N_SHARDS)]
    text_paths = remote_parallel_map(
        download_shard,
        download_inputs,
        image=IMAGE,
        grow=True,
        func_cpu=2,
        func_ram=8,
        max_parallelism=min(MAX_CPU_PARALLELISM, N_SHARDS),
    )
    print(f"Stage 1 done: {len(text_paths)} JSONL shards written\n")
    return text_paths


def run_stage_embed(text_paths):
    max_par = min(MAX_GPU_PARALLELISM, len(text_paths))
    msg = f"--- Stage 2: Embedding {len(text_paths)} shards on A100 GPU workers "
    msg += f"(up to {max_par} concurrent) ---"
    print(msg)
    embed_inputs = [(p, MODEL_NAME, SHARED_ROOT) for p in text_paths]
    embed_results = remote_parallel_map(
        embed_shard,
        embed_inputs,
        image=IMAGE,
        grow=True,
        func_gpu="A100",
        max_parallelism=max_par,
    )
    total_vecs = sum(r["n"] for r in embed_results)
    print(f"Stage 2 done: {total_vecs} vectors across {len(embed_results)} shards\n")
    return embed_results


def run_stage_search(embed_results):
    print(f"--- Stage 3: Embedding query on A100 ---")
    query_vec_list = remote_parallel_map(
        embed_query,
        [(QUERY, MODEL_NAME)],
        image=IMAGE,
        grow=True,
        func_gpu="A100",
        max_parallelism=1,
    )
    print(f"Stage 3 done\n")

    print(f"--- Stage 4: Local similarity search ---")
    import io
    import numpy as np
    from google.cloud import storage

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(f"{PROJECT_ID}-burla-shared-workspace")

    def _download(worker_path):
        # worker_path looks like "/workspace/shared/vector_embeddings_demo/embeddings/emb-00000.npy"
        blob_name = worker_path.replace("/workspace/shared/", "", 1)
        return bucket.blob(blob_name).download_as_bytes()

    query_vec = np.asarray(query_vec_list[0], dtype="float32")
    matrices, titles_flat = [], []
    for result in sorted(embed_results, key=lambda r: r["emb_path"]):
        matrices.append(np.load(io.BytesIO(_download(result["emb_path"]))))
        titles_flat.extend(json.loads(_download(result["ids_path"]))["titles"])
    matrix = np.concatenate(matrices, axis=0)

    scores = matrix @ query_vec
    ranked = np.argsort(-scores)
    seen_titles = set()
    print(f"\nTop {TOP_K} results for: {QUERY!r}\n")
    printed = 0
    for idx in ranked:
        title = titles_flat[idx]
        if title in seen_titles:
            continue
        seen_titles.add(title)
        printed += 1
        print(f"  {printed}. [{scores[idx]:.4f}] {title}")
        if printed >= TOP_K:
            break


def _find_existing_text_paths():
    """List text shards already written to /workspace/shared (lives on GCS).

    The client doesn't mount /workspace/shared, so we list via the GCS API
    and translate blob names back to the in-worker paths.
    """
    from google.cloud import storage

    bucket_name = f"{PROJECT_ID}-burla-shared-workspace"
    prefix = "vector_embeddings_demo/texts/"
    client = storage.Client(project=PROJECT_ID)
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))
    paths = sorted(
        f"/workspace/shared/{b.name}"
        for b in blobs
        if b.name.endswith(".jsonl")
    )
    limit = os.environ.get("DEMO_SHARD_LIMIT")
    if limit:
        paths = paths[: int(limit)]
    return paths


def _find_existing_embed_results():
    """List embedding shards already written to /workspace/shared via the GCS API."""
    from google.cloud import storage

    bucket_name = f"{PROJECT_ID}-burla-shared-workspace"
    prefix = "vector_embeddings_demo/embeddings/"
    client = storage.Client(project=PROJECT_ID)
    blobs = {b.name: b for b in client.list_blobs(bucket_name, prefix=prefix)}
    out = []
    for name in sorted(blobs):
        if not name.startswith(f"{prefix}emb-") or not name.endswith(".npy"):
            continue
        shard_name = Path(name).stem.replace("emb-", "")
        ids_name = f"{prefix}ids-{shard_name}.json"
        if ids_name not in blobs:
            continue
        out.append(
            {
                "emb_path": f"/workspace/shared/{name}",
                "ids_path": f"/workspace/shared/{ids_name}",
                "n": 0,
            }
        )
    return out


def main():
    print(f"=== Burla Wikipedia embedding demo ===")
    print(f"Image: {IMAGE}")
    print(f"Model: {MODEL_NAME}")
    print(f"Stage: {DEMO_STAGE}")
    print(f"Total articles: {TOTAL_ARTICLES} across {N_SHARDS} shards")
    print()

    text_paths = None
    embed_results = None

    if DEMO_STAGE in ("all", "download"):
        text_paths = run_stage_download()
    if DEMO_STAGE in ("all", "embed"):
        if text_paths is None:
            text_paths = _find_existing_text_paths()
        embed_results = run_stage_embed(text_paths)
    if DEMO_STAGE in ("all", "search"):
        if embed_results is None:
            embed_results = _find_existing_embed_results()
        run_stage_search(embed_results)


if __name__ == "__main__":
    main()
