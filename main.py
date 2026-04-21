import pyarrow.dataset as ds
import torch  # noqa: F401 -- top-level import so Burla installs torch on workers
import transformers  # noqa: F401 -- top-level import so Burla installs transformers on workers
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
