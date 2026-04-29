"""arXiv Fossils of Science — what ideas in science went extinct?
================================================================

Runs a data-first discovery pipeline over ~2.7M arXiv papers (the full public
snapshot, 1986-present) and asks three questions whose answers we do NOT know
in advance:

  1. Which research topics have quietly gone extinct? (peaked, then collapsed)
  2. Which topics are being born right now? (appeared in the last ~24 months)
  3. Which single paper is the loneliest idea in science? (no neighbors)

Pipeline shape:
  * Stage 0 (one big-box worker): download the weekly arXiv metadata snapshot
    from the Hugging Face mirror `jackkuo/arXiv-metadata-oai-snapshot` and
    shard it into ~270 parquet files of 10K papers each under
    /workspace/shared/arxiv-fossils/raw/.
  * Map (many workers): each worker reads one raw shard, embeds every
    title+abstract with sentence-transformers/all-MiniLM-L6-v2, and writes a
    vector shard to /workspace/shared/arxiv-fossils/vec/.
  * Reduce (one big-box worker): load every vector shard, cluster with
    MiniBatchKMeans (k=500), compute per-cluster temporal patterns, rank
    clusters by extinction / emergence, find the loneliest paper by 5th-NN
    cosine distance (via FAISS), write three static HTML reports into
    /workspace/shared/arxiv-fossils/out/.

Run on Burla (cluster runs Python 3.12 / burla 1.4.5 per
~/.burla/joeyper23/user_config.json):

    /Users/josephperry/.burla/joeyper23/.venv/bin/python arxiv_fossils.py

Env vars:
    ARXIV_MAX_PAPERS   cap the corpus (default: full dataset, ~2.7M)
    REDUCE_ONLY=1      skip stage 0 + map, reduce over existing vec shards
    SKIP_STAGE=1       skip stage 0, assume raw shards already on GCS
    LOCAL=1            run the pipeline in-process on a 50K-row sample
"""

from __future__ import annotations

import os

from huggingface_hub import hf_hub_download
os.environ.setdefault("DISABLE_BURLA_TELEMETRY", "True")
# ONNX Runtime / OpenMP will otherwise see the *host* CPU count on Burla's
# cgroup-limited workers and spawn ~60 threads onto 1 real CPU, thrashing
# context-switch overhead. Pin to 1 thread and let Burla give us parallelism
# via more workers, not more threads-per-worker.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("ONNXRUNTIME_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Fastembed avoids the 2+ GB torch install per worker and ships a pre-exported
# ONNX model for all-MiniLM-L6-v2.
import huggingface_hub  # noqa: F401
import fastembed  # noqa: F401
import sklearn  # noqa: F401
import faiss  # noqa: F401

SHARED_ROOT = Path(os.environ.get("SHARED_DIR", "/workspace/shared"))
RAW_DIR = SHARED_ROOT / "arxiv-fossils" / "raw"
VEC_DIR = SHARED_ROOT / "arxiv-fossils" / "vec"
OUT_DIR = SHARED_ROOT / "arxiv-fossils" / "out"

HF_REPO = "jackkuo/arXiv-metadata-oai-snapshot"
HF_FILENAME = "arxiv-metadata-oai-snapshot.json"
SHARD_SIZE = 10_000

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBED_DIM = 384
EMBED_BATCH = 256

N_CLUSTERS = 400
MIN_CLUSTER_PAPERS = 50
PEAK_REQUIRE_YEARS = 3
KMEANS_FIT_SAMPLE = 300_000  # fit kmeans on a sample for speed, predict on full set


def _ensure_dirs() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    VEC_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)


_MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
)}


def _extract_created(rec: dict) -> str:
    """Return YYYY-MM-DD of earliest known submission for this paper.

    Prefers versions[0].created (original v1 date), falls back to update_date.
    Handles RFC-2822 style dates like "Sat, 30 Mar 2019 08:00:00 GMT".
    """
    versions = rec.get("versions") or []
    if isinstance(versions, list) and versions:
        v0 = versions[0]
        if isinstance(v0, dict):
            raw = v0.get("created", "") or ""
            parts = raw.split()
            if len(parts) >= 4 and parts[2] in _MONTHS:
                try:
                    day = int(parts[1])
                    month = _MONTHS[parts[2]]
                    year = int(parts[3])
                    return f"{year:04d}-{month:02d}-{day:02d}"
                except ValueError:
                    pass
    upd = rec.get("update_date") or ""
    return upd[:10] if upd else ""


def stage_raw(_ignored) -> List[str]:
    """Stage 0: download the HF metadata snapshot and shard it to parquet.

    Runs on ONE big-box worker. Idempotent — skips download/shard if results
    already exist on /workspace/shared.
    """
    _ensure_dirs()
    existing = sorted(str(p) for p in RAW_DIR.glob("shard_*.parquet"))
    if existing and sum(Path(p).stat().st_size for p in existing) > 50_000_000:
        print(f"stage_raw: {len(existing)} shards already on shared FS, skipping")
        return existing


    t0 = time.time()
    print(f"stage_raw: downloading {HF_REPO}/{HF_FILENAME} ...")
    local_path = hf_hub_download(
        repo_id=HF_REPO,
        filename=HF_FILENAME,
        repo_type="dataset",
        local_dir=str(SHARED_ROOT / "arxiv-fossils" / "download"),
    )
    size_mb = Path(local_path).stat().st_size / 1e6
    print(f"stage_raw: downloaded {size_mb:.1f} MB in {time.time()-t0:.1f}s → {local_path}")

    cap = int(os.environ.get("ARXIV_MAX_PAPERS", "0") or "0") or None

    buffer: List[dict] = []
    shard_idx = 0
    shard_paths: List[str] = []
    total = 0

    def flush(records, idx):
        if not records:
            return None
        out = RAW_DIR / f"shard_{idx:05d}.parquet"
        tbl = pa.table({
            "id": [r.get("id", "") for r in records],
            "title": [" ".join((r.get("title") or "").split()) for r in records],
            "abstract": [" ".join((r.get("abstract") or "").split()) for r in records],
            "categories": [r.get("categories", "") or "" for r in records],
            "created": [_extract_created(r) for r in records],
        })
        pq.write_table(tbl, str(out))
        return str(out)

    t1 = time.time()
    with open(local_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not rec.get("id") or not rec.get("abstract") or not rec.get("title"):
                continue
            buffer.append(rec)
            total += 1
            if len(buffer) >= SHARD_SIZE:
                path = flush(buffer, shard_idx)
                if path:
                    shard_paths.append(path)
                buffer = []
                shard_idx += 1
            if cap and total >= cap:
                break
    if buffer:
        path = flush(buffer, shard_idx)
        if path:
            shard_paths.append(path)

    print(
        f"stage_raw: wrote {len(shard_paths)} shards covering {total:,} papers "
        f"in {time.time()-t1:.1f}s"
    )
    return shard_paths


_MODEL = None


def _get_model():
    global _MODEL
    if _MODEL is None:
        os.environ["OMP_NUM_THREADS"] = "1"
        os.environ["MKL_NUM_THREADS"] = "1"
        os.environ["ONNXRUNTIME_NUM_THREADS"] = "1"
        from fastembed import TextEmbedding

        # threads=1 is critical: without it, onnxruntime reads /proc/cpuinfo
        # and launches ~60 intra-op threads onto the worker's single-CPU
        # cgroup, tanking throughput from ~200/s down to ~12/s.
        _MODEL = TextEmbedding(model_name=EMBED_MODEL, threads=1)
    return _MODEL


def _l2_normalize(vecs: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms < 1e-12, 1.0, norms)
    return vecs / norms


def embed_shard(raw_path: str) -> str:
    """Map task: embed one raw shard → vector shard."""
    _ensure_dirs()
    shard_name = Path(raw_path).stem
    out_path = VEC_DIR / f"{shard_name}.parquet"
    if out_path.exists() and out_path.stat().st_size > 1024:
        return str(out_path)

    tbl = pq.read_table(raw_path)
    if tbl.num_rows == 0:
        pq.write_table(tbl.append_column(
            "vector", pa.array([], type=pa.list_(pa.float32(), EMBED_DIM))
        ), str(out_path))
        return str(out_path)

    ids = tbl.column("id").to_pylist()
    titles = tbl.column("title").to_pylist()
    abstracts = tbl.column("abstract").to_pylist()
    categories = tbl.column("categories").to_pylist()
    created = tbl.column("created").to_pylist()
    texts = [f"{t}\n{a}" for t, a in zip(titles, abstracts)]

    t0 = time.time()
    model = _get_model()
    vecs_iter = model.embed(texts, batch_size=EMBED_BATCH)
    vecs = np.asarray(list(vecs_iter), dtype="float32")
    vecs = _l2_normalize(vecs)
    elapsed = time.time() - t0

    out_tbl = pa.table({
        "id": ids,
        "title": titles,
        "abstract": abstracts,
        "categories": categories,
        "created": created,
        "vector": pa.array(vecs.tolist(), type=pa.list_(pa.float32(), EMBED_DIM)),
    })
    pq.write_table(out_tbl, str(out_path))

    print(
        f"{shard_name}: embedded {len(texts):,} in {elapsed:.1f}s "
        f"({len(texts)/max(elapsed,1e-3):.1f}/s)"
    )
    return str(out_path)


def _load_one_shard(path: str) -> Tuple[pd.DataFrame | None, np.ndarray | None]:
    try:
        tbl = pq.read_table(path)
    except Exception as exc:
        print(f"  WARN: skipping {path} — {exc}", flush=True)
        return None, None
    if tbl.num_rows == 0:
        return None, None
    df = tbl.drop(["vector"]).to_pandas()
    # Reading nested list-of-float32 as a numpy array: PyArrow returns a
    # FixedSizeListArray. `to_numpy` with zero_copy_only=False materializes it
    # as a 2-D float32 array fast, avoiding the slow per-row to_pylist path.
    vec_col = tbl.column("vector")
    try:
        flat = vec_col.combine_chunks().values.to_numpy(zero_copy_only=False)
        v = np.asarray(flat, dtype="float32").reshape(-1, EMBED_DIM)
    except Exception:
        v = np.asarray(vec_col.to_pylist(), dtype="float32")
    if v.ndim != 2 or v.shape[1] != EMBED_DIM:
        return None, None
    return df, v


def _load_all_shards(paths: List[str], max_workers: int = 16) -> Tuple[pd.DataFrame, np.ndarray]:
    from concurrent.futures import ThreadPoolExecutor

    frames: List[pd.DataFrame] = []
    vecs_chunks: List[np.ndarray] = []
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for df, v in ex.map(_load_one_shard, paths):
            done += 1
            if df is None or v is None:
                continue
            frames.append(df)
            vecs_chunks.append(v)
            if done % 50 == 0 or done == len(paths):
                rows = sum(f.shape[0] for f in frames)
                print(
                    f"  loaded {done}/{len(paths)} shards, {rows:,} rows so far "
                    f"({time.time()-t0:.1f}s)",
                    flush=True,
                )
    if not frames:
        return pd.DataFrame(), np.zeros((0, EMBED_DIM), dtype="float32")
    meta = pd.concat(frames, ignore_index=True)
    vecs = np.concatenate(vecs_chunks, axis=0)
    print(f"  concatenated {len(meta):,} rows, vec shape {vecs.shape}", flush=True)
    keep_mask = ~meta["id"].duplicated(keep="first").values
    meta = meta.loc[keep_mask].reset_index(drop=True)
    vecs = vecs[keep_mask]
    print(f"  after dedupe: {len(meta):,} unique papers", flush=True)
    return meta, vecs


def _parse_ym(created: str) -> Tuple[int, int] | None:
    if not created or len(created) < 7:
        return None
    try:
        y = int(created[0:4])
        m = int(created[5:7])
    except ValueError:
        return None
    if y < 1990 or y > 2100 or not (1 <= m <= 12):
        return None
    return y, m


def _cluster_vectors(vecs: np.ndarray, k: int) -> np.ndarray:
    """Fit MiniBatchKMeans on a random sample, then assign every vector in
    big batches. Fitting on 300K rather than 2.4M cuts wall time by ~8x with
    negligible quality loss for topic discovery at k=400.
    """
    from sklearn.cluster import MiniBatchKMeans

    n = vecs.shape[0]
    fit_n = min(KMEANS_FIT_SAMPLE, n)
    if fit_n < n:
        rng = np.random.RandomState(42)
        fit_idx = rng.choice(n, size=fit_n, replace=False)
        fit_vecs = vecs[fit_idx]
    else:
        fit_vecs = vecs

    print(f"  kmeans.fit on {fit_vecs.shape[0]:,} sample vectors ...", flush=True)
    t0 = time.time()
    km = MiniBatchKMeans(
        n_clusters=k,
        random_state=42,
        batch_size=16384,
        max_iter=80,
        n_init=1,
        reassignment_ratio=0.01,
        init="k-means++",
    )
    km.fit(fit_vecs)
    print(f"  kmeans.fit done in {time.time()-t0:.1f}s", flush=True)

    t1 = time.time()
    labels = np.empty(n, dtype=np.int32)
    chunk = 100_000
    for i in range(0, n, chunk):
        labels[i:i + chunk] = km.predict(vecs[i:i + chunk])
    print(f"  predicted {n:,} labels in {time.time()-t1:.1f}s", flush=True)
    return labels


def _cluster_summary(label: int, members: pd.DataFrame) -> dict:
    cats_series = members["categories"].fillna("")
    first_cats = cats_series.str.split().explode()
    top_cats = first_cats.value_counts().head(5).to_dict()
    if len(members) >= 3:
        sample_idx = [0, len(members) // 2, len(members) - 1]
    else:
        sample_idx = list(range(len(members)))
    samples = members.iloc[sample_idx][["id", "title", "created"]].to_dict("records")
    return {
        "cluster_id": int(label),
        "n_papers": int(len(members)),
        "top_categories": {str(k): int(v) for k, v in top_cats.items()},
        "samples": [{"id": str(s["id"]), "title": str(s["title"]), "created": str(s["created"])} for s in samples],
    }


def _label_extinct_and_emergent(
    meta: pd.DataFrame, labels: np.ndarray, now_year: int, now_month: int,
) -> Tuple[List[dict], List[dict]]:
    """Rank all clusters, then take the top-10 most extinct and most emergent.

    Rather than applying hard cutoffs (which tend to return empty lists when
    the research landscape is growing overall, as arXiv is), we always surface
    the 10 clusters with the steepest decline and the 10 with the sharpest
    recent burst. Constraints are kept mild: cluster needs enough history
    (>= MIN_CLUSTER_PAPERS) and at least PEAK_REQUIRE_YEARS distinct years.
    """
    meta = meta.copy()
    meta["cluster"] = labels
    ym = meta["created"].apply(_parse_ym)
    meta = meta.loc[ym.notna()].copy()
    meta["year"] = ym.apply(lambda t: t[0] if t else None).astype("Int64")
    meta["ym"] = ym.apply(lambda t: t[0] * 12 + (t[1] - 1) if t else None).astype("Int64")

    now_idx = now_year * 12 + (now_month - 1)
    extinct_candidates: List[dict] = []
    emergent_candidates: List[dict] = []

    for cid, members in meta.groupby("cluster"):
        if cid < 0 or len(members) < MIN_CLUSTER_PAPERS:
            continue
        years = members["year"].dropna().astype(int)
        if years.empty:
            continue
        year_counts = years.value_counts().sort_index()
        if year_counts.size < PEAK_REQUIRE_YEARS:
            continue

        peak_year = int(year_counts.idxmax())
        peak_val = int(year_counts.max())
        first_year = int(year_counts.index.min())
        last_year = int(year_counts.index.max())
        last_5_years = [y for y in year_counts.index if y >= now_year - 4]
        recent_5yr_sum = int(year_counts.loc[last_5_years].sum()) if last_5_years else 0

        # Decline score: small means cluster has collapsed relative to its peak.
        # Only consider peaks 5+ years old so we're not just flagging a 2024 spike.
        if (now_year - peak_year) >= 5 and peak_val >= 10:
            decline_ratio = recent_5yr_sum / max(peak_val * 5, 1)
            entry = _cluster_summary(int(cid), members)
            entry.update({
                "peak_year": peak_year,
                "peak_volume": peak_val,
                "recent_5yr_volume": recent_5yr_sum,
                "decline_ratio": round(decline_ratio, 3),
                "first_year": first_year,
                "last_year": last_year,
                "years_since_peak": int(now_year - peak_year),
            })
            extinct_candidates.append(entry)

        # Burst score: fraction of cluster papers submitted in last 24 months.
        # A cluster that has 80% of its lifetime papers in the last 2 years is
        # clearly a newborn topic — even if scattered older work seeded the
        # centroid.
        recent_24m = int((members["ym"] >= now_idx - 24).sum())
        burst = recent_24m / max(len(members), 1)
        if len(members) >= MIN_CLUSTER_PAPERS and recent_24m >= 30:
            entry = _cluster_summary(int(cid), members)
            entry.update({
                "papers_last_24m": recent_24m,
                "burst_fraction": round(burst, 3),
                "first_year": first_year,
                "last_year": last_year,
            })
            emergent_candidates.append(entry)

    extinct_candidates.sort(key=lambda d: d["decline_ratio"])
    emergent_candidates.sort(key=lambda d: -d["burst_fraction"])
    return extinct_candidates[:10], emergent_candidates[:10]


def _find_loneliest_paper(meta: pd.DataFrame, vecs: np.ndarray) -> dict | None:
    """Find the paper whose 5th-nearest neighbor is furthest away.

    Vectors are already L2-normalized, so inner product == cosine similarity.
    We use an IVF index (not brute-force IndexFlatIP) because n² search on
    2.4M vectors would take > 1 hr; IVF with nprobe=32 is ~30s for the same
    answer quality at this scale.
    """
    if vecs.shape[0] < 1000:
        return None

    norm_vecs = np.ascontiguousarray(vecs, dtype="float32")
    n = norm_vecs.shape[0]

    try:
        import faiss  # type: ignore

        t0 = time.time()
        # HNSW is graph-based and for cosine-similarity top-k it's typically
        # 10-20x faster than IVF on CPU at comparable recall on < 10M vectors.
        index = faiss.IndexHNSWFlat(EMBED_DIM, 32, faiss.METRIC_INNER_PRODUCT)
        index.hnsw.efConstruction = 80
        index.hnsw.efSearch = 64
        index.add(norm_vecs)
        print(f"  faiss HNSW index built in {time.time()-t0:.1f}s (n={n:,})", flush=True)

        t1 = time.time()
        D_chunks: List[np.ndarray] = []
        chunk = 50_000
        for i in range(0, n, chunk):
            D, _I = index.search(norm_vecs[i:i + chunk], 6)
            D_chunks.append(D)
            if (i // chunk) % 10 == 0:
                print(f"    searched {i + chunk}/{n} ({time.time()-t1:.1f}s)", flush=True)
        D = np.concatenate(D_chunks, axis=0)
        print(f"  faiss search done in {time.time()-t1:.1f}s", flush=True)

        fifth_sim = D[:, 5]
        worst = int(np.argmin(fifth_sim))
        row = meta.iloc[worst]
        return {
            "id": str(row["id"]),
            "title": str(row["title"]),
            "abstract": str(row["abstract"])[:800],
            "created": str(row["created"]),
            "categories": str(row["categories"]),
            "nearest_5th_similarity": float(fifth_sim[worst]),
        }
    except ImportError:
        from sklearn.neighbors import NearestNeighbors

        sample_idx = np.random.RandomState(7).choice(
            n, size=min(n, 200_000), replace=False
        )
        sample_vecs = vecs[sample_idx]
        nn = NearestNeighbors(n_neighbors=6, metric="cosine", n_jobs=-1)
        nn.fit(sample_vecs)
        dists, _ = nn.kneighbors(sample_vecs)
        kth = dists[:, 5]
        worst_local = int(np.argmax(kth))
        row = meta.iloc[int(sample_idx[worst_local])]
        return {
            "id": str(row["id"]),
            "title": str(row["title"]),
            "abstract": str(row["abstract"])[:800],
            "created": str(row["created"]),
            "categories": str(row["categories"]),
            "kth_distance": float(kth[worst_local]),
        }


_CSS = """
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 860px; margin: 40px auto; padding: 0 20px; line-height: 1.55; color: #0f172a; }
  h1 { font-size: 32px; margin-bottom: 4px; }
  h2 { font-size: 20px; margin-top: 32px; margin-bottom: 6px; }
  .sub { color: #64748b; margin-top: 0; }
  .card { border: 1px solid #e2e8f0; border-radius: 12px; padding: 18px 22px; margin: 16px 0;
          box-shadow: 0 1px 2px rgba(15,23,42,0.04); }
  .card .label { font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
  .card h3 { margin: 4px 0 8px 0; font-size: 18px; }
  .stats { color: #475569; font-size: 13px; }
  .sample { font-size: 13px; color: #334155; margin: 6px 0 0 16px; }
  .sample code { background: #f1f5f9; padding: 1px 4px; border-radius: 3px; font-size: 12px; }
  .footer { color: #94a3b8; font-size: 12px; margin-top: 40px; }
  a { color: #2563eb; text-decoration: none; }
  a:hover { text-decoration: underline; }
</style>
"""


def _render_extinct_html(extinct: List[dict], total_papers: int, generated_at: str) -> str:
    blocks = ["<h1>The Fossils of Science</h1>"]
    blocks.append(
        f"<p class=sub>Ten research topics that peaked, then quietly collapsed. Discovered by "
        f"clustering {total_papers:,} arXiv abstracts into {N_CLUSTERS} topics and ranking each "
        f"cluster by how steeply its recent 5-year volume has fallen below its all-time peak.</p>"
    )
    if not extinct:
        blocks.append("<p><em>No cluster met the extinction criteria.</em></p>")
    for rank, c in enumerate(extinct, 1):
        cats = ", ".join(list(c["top_categories"].keys())[:5])
        sample_li = "".join(
            f"<li class=sample><code><a href='https://arxiv.org/abs/{s['id']}' target=_blank>{s['id']}</a></code> "
            f"— {s['title']} <span style='color:#94a3b8'>({s['created'][:7]})</span></li>"
            for s in c["samples"]
        )
        blocks.append(f"""
        <div class=card>
          <div class=label>#{rank} &middot; peaked {c['peak_year']} &middot; now {round(c['decline_ratio']*100, 1)}% of peak rate</div>
          <h3>{cats or '(uncategorized)'}</h3>
          <div class=stats>{c['n_papers']:,} papers from {c['first_year']}-{c['last_year']} &middot; peak-year volume {c['peak_volume']}, last 5 years {c['recent_5yr_volume']}</div>
          <ul>{sample_li}</ul>
        </div>""")
    blocks.append(f"<div class=footer>Generated {generated_at}. Source: arXiv metadata snapshot via Burla.</div>")
    return f"<!doctype html><meta charset=utf-8><title>Fossils of Science</title>{_CSS}{''.join(blocks)}"


def _render_emergent_html(emergent: List[dict], total_papers: int, generated_at: str) -> str:
    blocks = ["<h1>Newborn Sciences</h1>"]
    blocks.append(
        f"<p class=sub>Ten research topics whose papers cluster most heavily in the last 24 months "
        f"relative to the topic's full history on arXiv. Found by clustering {total_papers:,} abstracts "
        f"into {N_CLUSTERS} topics and ranking each cluster by how much of its mass is concentrated "
        f"in the recent window.</p>"
    )
    if not emergent:
        blocks.append("<p><em>No cluster met the emergence criteria.</em></p>")
    for rank, c in enumerate(emergent, 1):
        cats = ", ".join(list(c["top_categories"].keys())[:5])
        sample_li = "".join(
            f"<li class=sample><code><a href='https://arxiv.org/abs/{s['id']}' target=_blank>{s['id']}</a></code> "
            f"— {s['title']} <span style='color:#94a3b8'>({s['created'][:7]})</span></li>"
            for s in c["samples"]
        )
        blocks.append(f"""
        <div class=card>
          <div class=label>#{rank} &middot; {round(c['burst_fraction']*100, 1)}% of papers in last 24 months &middot; first seen {c['first_year']}</div>
          <h3>{cats or '(uncategorized)'}</h3>
          <div class=stats>{c['n_papers']:,} papers total ({c['first_year']}-{c['last_year']}) &middot; {c['papers_last_24m']:,} in last 24 months</div>
          <ul>{sample_li}</ul>
        </div>""")
    blocks.append(f"<div class=footer>Generated {generated_at}. Source: arXiv metadata snapshot via Burla.</div>")
    return f"<!doctype html><meta charset=utf-8><title>Newborn Sciences</title>{_CSS}{''.join(blocks)}"


def _render_loneliest_html(paper: dict | None, total_papers: int, generated_at: str) -> str:
    if paper is None:
        body = "<p>No candidate found — corpus too small.</p>"
    else:
        sim = paper.get("nearest_5th_similarity")
        if sim is None:
            sim_txt = f"5th-NN cosine distance: {paper.get('kth_distance', 0):.3f}"
        else:
            sim_txt = f"5th-NN cosine similarity: {sim:.3f}"
        body = f"""
        <div class=card>
          <div class=label>arXiv id <a href='https://arxiv.org/abs/{paper['id']}' target=_blank><code>{paper['id']}</code></a> &middot; posted {paper['created']}</div>
          <h3>{paper['title']}</h3>
          <p class=stats>Categories: {paper['categories']} &middot; {sim_txt}</p>
          <p>{paper['abstract']}</p>
        </div>"""
    return f"""<!doctype html><meta charset=utf-8><title>The Loneliest Paper in Science</title>{_CSS}
<h1>The Loneliest Paper in Science</h1>
<p class=sub>Across {total_papers:,} arXiv abstracts, this one sits furthest from any neighborhood of
related work. Its 5th-nearest neighbor in 384-dimensional embedding space is more dissimilar than any
other paper's.</p>
{body}
<div class=footer>Generated {generated_at}. Source: arXiv metadata snapshot via Burla.</div>"""


def reduce_fossils(vec_paths: List[str]) -> str:
    """Reduce: load shards, cluster, analyze, write HTML artifacts."""
    _ensure_dirs()
    t0 = time.time()
    if not vec_paths:
        vec_paths = sorted(str(p) for p in VEC_DIR.glob("*.parquet"))
        print(f"reduce: globbed {len(vec_paths)} existing vec shards", flush=True)

    meta, vecs = _load_all_shards(vec_paths)
    if meta.empty:
        print("reduce: no records loaded", flush=True)
        return str(OUT_DIR)
    print(
        f"reduce: loaded {len(meta):,} unique papers; vector matrix {vecs.shape} "
        f"({time.time()-t0:.1f}s)",
        flush=True,
    )

    t1 = time.time()
    labels = _cluster_vectors(vecs, k=N_CLUSTERS)
    print(f"reduce: clustered in {time.time()-t1:.1f}s", flush=True)

    now = datetime.now(timezone.utc)
    extinct, emergent = _label_extinct_and_emergent(meta, labels, now.year, now.month)
    print(f"reduce: {len(extinct)} extinct, {len(emergent)} emergent clusters", flush=True)

    loneliest = _find_loneliest_paper(meta, vecs)
    print(f"reduce: loneliest = {(loneliest or {}).get('id')}", flush=True)

    generated_at = now.isoformat(timespec="seconds")
    (OUT_DIR / "extinct.html").write_text(
        _render_extinct_html(extinct, len(meta), generated_at), encoding="utf-8"
    )
    (OUT_DIR / "emergent.html").write_text(
        _render_emergent_html(emergent, len(meta), generated_at), encoding="utf-8"
    )
    (OUT_DIR / "loneliest.html").write_text(
        _render_loneliest_html(loneliest, len(meta), generated_at), encoding="utf-8"
    )
    (OUT_DIR / "summary.json").write_text(json.dumps({
        "total_papers": int(len(meta)),
        "n_shards": len(vec_paths),
        "n_clusters": N_CLUSTERS,
        "extinct_count": len(extinct),
        "emergent_count": len(emergent),
        "loneliest_id": (loneliest or {}).get("id"),
        "reduce_elapsed_s": round(time.time() - t0, 2),
        "generated_at_utc": generated_at,
    }, indent=2))
    print(f"reduce done in {time.time()-t0:.1f}s. artifacts → {OUT_DIR}")
    return str(OUT_DIR)


def main() -> int:
    from burla import remote_parallel_map  # type: ignore

    # NB: _ensure_dirs() is intentionally NOT called on the driver — /workspace
    # only exists on the Burla workers. Worker-side functions call it themselves.

    reduce_only = os.environ.get("REDUCE_ONLY", "").strip() not in ("", "0", "false", "False")

    if reduce_only:
        print("REDUCE_ONLY=1: skipping stage_raw + embed_shard, reducing over existing vec shards")
        vec_paths: List[str] = []
    else:
        # stage_raw is idempotent and fast on subsequent runs (~1s to check + return
        # existing shard list). Always run it on a worker so we can collect the raw
        # shard paths that live on /workspace/shared (invisible to the driver).
        print("stage 0: sharding / verifying raw arXiv metadata on a worker ...")
        [raw_paths] = list(remote_parallel_map(
            stage_raw, [None],
            func_cpu=8, func_ram=32,
        ))
        print(f"stage 0 done. raw shards: {len(raw_paths)}")

        print(f"map: embedding {len(raw_paths)} raw shards across workers ...")
        vec_paths = list(remote_parallel_map(
            embed_shard, raw_paths,
            func_cpu=1, func_ram=4,
        ))
        print(f"map done. vec shards returned: {len(vec_paths)}")

    [results_dir] = list(remote_parallel_map(
        reduce_fossils, [vec_paths],
        func_cpu=16, func_ram=64,
    ))
    print(f"reduce done. results: {results_dir}")
    return 0


def main_local() -> int:
    """Local dev path: run pipeline in-process with ARXIV_MAX_PAPERS cap."""
    _ensure_dirs()
    raw_paths = stage_raw(None)
    vec_paths = [embed_shard(p) for p in raw_paths]
    reduce_fossils(vec_paths)
    return 0


if __name__ == "__main__":
    if os.environ.get("LOCAL", "").strip() not in ("", "0", "false", "False"):
        raise SystemExit(main_local())
    raise SystemExit(main())
