# Run Batch ML Inference Across 1,000+ CPUs and GPUs in Python

Run a HuggingFace model (or any PyTorch / ONNX / sklearn model) on 10 million rows by fanning out to 1,000 workers at the same time.

## The Problem

You have 10M rows of text and a sentiment (or embedding, or classifier) model. On one machine, it takes days. Using a single GPU, still hours. SageMaker Batch Transform needs a Docker image, an endpoint config, a manifest file, and IAM. Ray Serve and KServe are overkill for a one-shot batch job.

You want to run `model.predict(batch)` on 1,000 workers in parallel, each pulling its own slice.

## The Solution (Burla)

Chunk the 10M rows into 1,000 batches of 10,000. Hand Burla the batches and the function. Each worker loads the model once, runs inference on its batch, returns predictions. Results stream back as they finish.

No endpoint, no manifest, no Dockerfile. The model loads from the HuggingFace cache or a path you specify. Set `func_cpu=4` and `func_ram=16` for CPU inference, or request GPU workers.

## Example

```python
import pyarrow.dataset as ds
from burla import remote_parallel_map

dataset = ds.dataset("s3://my-bucket/reviews/", format="parquet")
texts = dataset.to_table(columns=["review_id", "text"]).to_pandas()

BATCH = 10_000
batches = [
    texts.iloc[i : i + BATCH].to_dict("records")
    for i in range(0, len(texts), BATCH)
]
print(f"{len(texts):,} rows, {len(batches)} batches")


def predict_batch(rows: list[dict]) -> list[dict]:
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch

    if not hasattr(predict_batch, "_model"):
        model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        predict_batch._tok = AutoTokenizer.from_pretrained(model_name)
        predict_batch._model = AutoModelForSequenceClassification.from_pretrained(model_name).eval()

    tok, model = predict_batch._tok, predict_batch._model
    labels = ["negative", "neutral", "positive"]

    texts = [r["text"] for r in rows]
    enc = tok(texts, padding=True, truncation=True, max_length=256, return_tensors="pt")
    with torch.no_grad():
        logits = model(**enc).logits
        probs = torch.softmax(logits, dim=-1).numpy()

    return [
        {"review_id": r["review_id"], "label": labels[p.argmax()], "score": float(p.max())}
        for r, p in zip(rows, probs)
    ]


# 1,000 batches -> 1,000 workers loading the model and running inference in parallel
results = remote_parallel_map(
    predict_batch, batches, func_cpu=4, func_ram=16, generator=True
)

import json
with open("predictions.jsonl", "w") as f:
    for batch_out in results:
        for row in batch_out:
            f.write(json.dumps(row) + "\n")
```

## Why This Is Better

**vs Ray Serve / KServe** — both are for serving, not batch. You don't need an endpoint, autoscaling policy, or health checks for a one-shot 10M-row job.

**vs SageMaker Batch Transform** — you write a Dockerfile, push to ECR, build a manifest in S3, configure an endpoint config. Then you wait for capacity. With Burla, you call a function.

**vs AWS Batch + a Docker image with PyTorch** — you own the image, the IAM role, the queue, the compute env, and the retries. Burla workers already have PyTorch. Cold start is seconds.

**vs Ray Data** — Ray Data can do this, but you still need a Ray cluster running and the Ray serialization layer. Burla ships the function and returns a list.

## How It Works

You batch your input list. Burla ships your function and batches to 1,000 pre-warmed workers. Each worker caches the model on the first call (the `hasattr` trick above) and reuses it for subsequent batches assigned to that worker. Results come back as a list (or a generator with `generator=True`).

## When To Use This

- Large one-shot batch inference: classification, embedding, scoring, summarization.
- Nightly jobs over millions of rows where latency doesn't matter but throughput does.
- Running open-weight LLMs (Llama, Mistral, Qwen) over a fixed corpus.
- Computing embeddings for a vector DB backfill.

## When NOT To Use This

- Real-time single-request inference — use a serving framework.
- Tiny datasets (<10k rows) — your laptop GPU is faster once you factor in startup.
- Models that need distributed training or tensor parallelism across workers — Burla workers don't coordinate with each other.
