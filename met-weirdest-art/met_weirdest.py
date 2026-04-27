"""Met Museum Open Access — the weirdest art in human history (and the hidden copies).
====================================================================================

Runs a data-first visual archaeology pipeline over the Met Museum's public-domain
artwork collection (~214K objects with full metadata + direct CDN images) and
asks two questions whose answers we do NOT know in advance:

  1. Which artworks are the most visually isolated on Earth? (no aesthetic
     neighbors anywhere across 5000 years of human-made objects)
  2. Which pairs of artworks are secret near-duplicates — visually almost
     identical but separated by centuries, cultures, or departments — that
     the museum has never flagged as related?

Pipeline shape:
  * Stage 0 (one big-box worker): download two community CSVs —
    `BetterMetObjects.csv` (enhanced Met open-access with every field the
    `/objects/{id}` API returns, 485K rows) and `met-openaccess-images.csv`
    (object_id → CRDImages `urlpath` mapping, 214K unique artworks). Inner-
    join them to get 213K artworks with both full metadata AND a direct URL
    on the `images.metmuseum.org/CRDImages/` CDN. Swap `/original/` → `/web-large/`
    in the URL path so we pull ~100 KB thumbnails instead of full-res 1.5 MB
    originals. The CDN has NO rate limiting (unlike the IIIF endpoint).
    Write `objects.parquet` to /workspace/shared/met-weirdest/.
  * Map (fan-out across many workers): each worker reads its slice of
    objects.parquet by object_id and pulls images concurrently via a 16-thread
    HTTP pool — no pacing needed because CRDImages isn't rate-limited. Every
    JPEG is thumbnailed and embedded with the CLIP ViT-B-32 vision ONNX model
    via fastembed. Writes a single parquet shard `(id, vector, title, date,
    culture, ...)` to /workspace/shared/met-weirdest/vec/.
  * Reduce (one big-box worker): load every vector shard, L2-normalize, build
    a FAISS IVF cosine index, and for every artwork compute its kth-nearest
    neighbor distance (for outliers) plus its top nearest neighbor (for twin
    candidates, filtered to pairs from different centuries/cultures/departments).
    Writes three static HTML reports into /workspace/shared/met-weirdest/out/.

Run on Burla (cluster runs Python 3.12 / burla 1.4.5):

    /Users/josephperry/.burla/joeyper23/.venv/bin/python met_weirdest.py

Env vars:
    MET_MAX_OBJECTS    cap the corpus (default: full joined set, ~213K)
    MET_BATCH_SIZE     ids per map task (default: 500)
    MET_MAX_WORKERS    max concurrent workers during the image-fetch map (default: 8)
    MET_HTTP_THREADS   per-worker HTTP thread pool (default: 16)
    REDUCE_ONLY=1      skip stage 0 + map, reduce over existing vec shards
    SKIP_DISCOVERY=1   skip stage 0, assume objects.parquet already exists
    LOCAL=1            run pipeline in-process on a 400-object sample
"""

from __future__ import annotations

import os

os.environ.setdefault("DISABLE_BURLA_TELEMETRY", "True")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("ONNXRUNTIME_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")

import io
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Top-level so Burla's dep-detector auto-installs these on every worker.
import fastembed  # noqa: F401
import PIL  # noqa: F401
import requests  # noqa: F401
import sklearn  # noqa: F401
import faiss  # noqa: F401

SHARED_ROOT = Path(os.environ.get("SHARED_DIR", "/workspace/shared"))
ROOT = SHARED_ROOT / "met-weirdest"
OBJECTS_PATH = ROOT / "objects.parquet"
VEC_DIR = ROOT / "vec"
OUT_DIR = ROOT / "out"

# Community-maintained enhancement of the official Met openaccess CSV: every
# field from `/objects/{id}` denormalized into a static row. Git-LFS backed at
# ~290 MB / 485K rows (of which ~336K have an `primary_image` IIIF URL).
MET_META_URL = (
    "https://media.githubusercontent.com/media/graslowsnail/"
    "metmuseum-api-dump-enhanced/main/BetterMetObjects.csv"
)
# Community-mirrored CRDImages path mapping: object_id → `urlpath` such that
# the full image is at `{CRD_IMAGE_BASE}/{urlpath}`. Covers ~214K of the Met's
# ~336K open-access-with-image set, but crucially these URLs are served by the
# Met's CDN which has NO per-IP rate limiting (vs the IIIF endpoint's 80 rps).
MET_CRD_URL = (
    "https://raw.githubusercontent.com/gregsadetsky/"
    "met-openaccess-images/master/met-openaccess-images.csv"
)
CRD_IMAGE_BASE = "https://images.metmuseum.org/CRDImages/"

CLIP_MODEL = "Qdrant/clip-ViT-B-32-vision"
CLIP_DIM = 512
CLIP_BATCH = 16

BATCH_SIZE = int(os.environ.get("MET_BATCH_SIZE", "500") or "500")
HTTP_TIMEOUT = 25
# Per-worker HTTP thread pool. CRDImages is served by the Met's CDN with no
# observable per-IP rate limit, so we can crank this up to saturate worker
# bandwidth. 16 threads @ ~100 KB/image comfortably fit in the shared-IP pool.
HTTP_THREADS = int(os.environ.get("MET_HTTP_THREADS", "16") or "16")

USER_AGENT = "BurlaMetDemo/1.0 (+https://burla.dev)"

TOP_OUTLIERS = 24
TOP_TWINS = 30
KTH_FOR_ISOLATION = 10
TWIN_SIM_THRESHOLD = 0.90
# Pairs above this similarity are almost always either (a) the same physical
# object photographed twice, (b) sequential plates of a single book/album, or
# (c) plates the Met has catalogued as separate objects but that share a
# single photographic master. None of those are "hidden twins" — they're
# duplicates, and we reject them from the twin report.
TWIN_SIM_MAX = 0.98
TWIN_MIN_CENTURY_GAP = 1

# Metadata columns we keep from the enhanced CSV. Everything else is dropped to
# keep objects.parquet small (~20 MB for the joined 213K-row set).
KEEP_COLS = [
    "object_id",
    "title",
    "artist",
    "date",
    "medium",
    "department",
    "culture",
    "object_begin_date",
    "object_end_date",
    "credit_line",
    "classification",
    "artist_nationality",
]


def _ensure_dirs() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    VEC_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Stage 0 — discovery
# ---------------------------------------------------------------------------


def _download(url: str, timeout: int = 300) -> bytes:
    import requests
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    buf = io.BytesIO()
    dl = 0
    for chunk in resp.iter_content(chunk_size=2 * 1024 * 1024):
        if not chunk:
            continue
        buf.write(chunk)
        dl += len(chunk)
    return buf.getvalue()


def discover_objects(params: dict | None) -> List[List[int]]:
    """Stage 0: pull the two community CSVs (enhanced metadata +
    CRDImages URL paths), inner-join them on `object_id`, write
    ``objects.parquet`` with the metadata plus a `crd_urlpath` column, and
    return batched IDs ready for fan-out.

    The resulting parquet has ~213K rows — every artwork that has both a
    denormalized metadata row AND a rate-limit-free CDN image URL.

    Env-vars don't propagate from driver → worker, so caller passes
    ``{'cap': int, 'batch_size': int}``.
    """
    params = params or {}
    cap = int(params.get("cap", 0) or 0)
    batch_size = int(params.get("batch_size", BATCH_SIZE) or BATCH_SIZE)

    _ensure_dirs()
    t0 = time.time()

    if OBJECTS_PATH.exists() and OBJECTS_PATH.stat().st_size > 1_000_000:
        df = pd.read_parquet(OBJECTS_PATH)
        print(
            f"discover: reusing {OBJECTS_PATH} ({len(df):,} rows, "
            f"{OBJECTS_PATH.stat().st_size/1e6:.1f} MB)",
            flush=True,
        )
    else:
        print("discover: downloading BetterMetObjects.csv ...", flush=True)
        meta_bytes = _download(MET_META_URL)
        print(
            f"discover: got {len(meta_bytes)/1e6:.1f} MB metadata CSV in "
            f"{time.time()-t0:.1f}s",
            flush=True,
        )
        meta_df = pd.read_csv(io.BytesIO(meta_bytes), low_memory=False)
        keep = [c for c in KEEP_COLS if c in meta_df.columns]
        meta_df = meta_df[keep].copy()
        meta_df["object_id"] = pd.to_numeric(meta_df["object_id"], errors="coerce")
        meta_df = meta_df.dropna(subset=["object_id"])
        meta_df["object_id"] = meta_df["object_id"].astype("int64")
        meta_df = meta_df.drop_duplicates(subset=["object_id"]).reset_index(drop=True)

        print("discover: downloading met-openaccess-images.csv ...", flush=True)
        t1 = time.time()
        crd_bytes = _download(MET_CRD_URL)
        crd_df = pd.read_csv(io.BytesIO(crd_bytes), on_bad_lines="skip")
        crd_df = crd_df.rename(columns={"id": "object_id", "urlpath": "crd_urlpath"})
        crd_df["object_id"] = pd.to_numeric(crd_df["object_id"], errors="coerce")
        crd_df = crd_df.dropna(subset=["object_id", "crd_urlpath"])
        crd_df["object_id"] = crd_df["object_id"].astype("int64")
        crd_df = crd_df.drop_duplicates(subset=["object_id"], keep="first").reset_index(drop=True)
        print(
            f"discover: got {len(crd_df):,} CRDImages URL paths in {time.time()-t1:.1f}s",
            flush=True,
        )

        # Swap /original/ → /web-large/ to pull ~100 KB thumbnails instead of
        # 1-3 MB originals. The Met's CDN supports both.
        crd_df["crd_urlpath"] = crd_df["crd_urlpath"].str.replace(
            "/original/", "/web-large/", regex=False
        )

        df = meta_df.merge(crd_df, on="object_id", how="inner")
        for c in ("object_begin_date", "object_end_date"):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
        df.to_parquet(OBJECTS_PATH, index=False)
        print(
            f"discover: joined → {len(df):,} artworks w/ full metadata + CRD URL "
            f"at {OBJECTS_PATH} ({OBJECTS_PATH.stat().st_size/1e6:.1f} MB)",
            flush=True,
        )

    ids = df["object_id"].tolist()
    random.Random(42).shuffle(ids)
    if cap and cap < len(ids):
        ids = ids[:cap]
        print(f"discover: capped to {cap:,}", flush=True)

    batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
    print(
        f"discover: {len(ids):,} ids → {len(batches)} batches of ~{batch_size}",
        flush=True,
    )
    return batches


# ---------------------------------------------------------------------------
# Map — fetch + embed
# ---------------------------------------------------------------------------


_CLIP_MODEL = None


def _get_clip():
    global _CLIP_MODEL
    if _CLIP_MODEL is None:
        os.environ["OMP_NUM_THREADS"] = "1"
        os.environ["MKL_NUM_THREADS"] = "1"
        os.environ["ONNXRUNTIME_NUM_THREADS"] = "1"
        from fastembed import ImageEmbedding

        _CLIP_MODEL = ImageEmbedding(model_name=CLIP_MODEL, threads=1)
    return _CLIP_MODEL


def _batch_shard_name(batch: List[int]) -> str:
    return f"shard_{batch[0]:09d}_{len(batch):04d}"


def _fetch_image_bytes(session, url: str) -> bytes | None:
    # CRDImages CDN: 404 means that particular image isn't on the CDN (~10%
    # miss rate); bail fast and let the caller move on.
    #
    # BUT: at high sustained throughput the Met's CDN/WAF will start returning
    # 403 Forbidden and 429 Too Many Requests for healthy URLs too — our
    # baseline run on Burla (8 workers × 16 threads) saw ~20K images succeed
    # then 403-storm for the next ~175K. Exponential backoff with jitter makes
    # us survive the transient blocks; max ~8s wasted per worker-thread per bad
    # URL is acceptable.
    import random as _rand
    for attempt in range(5):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                ct = r.headers.get("content-type", "")
                if "image" not in ct:
                    return None
                data = r.content
                if len(data) < 1_000 or len(data) > 16_000_000:
                    return None
                return data
            if r.status_code in (403, 429, 503, 504):
                # exponential: 0.5, 1.2, 2.4, 5.0, 10.0 + up to 0.5s jitter
                time.sleep(0.5 * (2.4 ** attempt) + _rand.uniform(0, 0.5))
                continue
            return None  # 404 / other permanent — give up
        except Exception:
            time.sleep(0.3 + _rand.uniform(0, 0.3))
    return None


def fetch_and_embed(batch: List[int]) -> str:
    """Map task: take ~BATCH_SIZE Met object IDs → write a CLIP-embedded vector shard.

    Loads objects.parquet on the worker (it's ~20 MB), picks the rows for this
    batch, and fetches the CRDImages URL for each with a big ThreadPoolExecutor.
    No request pacing: the CDN is not rate-limited.

    Idempotent: if the output shard already exists with rows, returns immediately.
    """
    import requests
    from PIL import Image
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _ensure_dirs()
    shard_name = _batch_shard_name(batch)
    out_path = VEC_DIR / f"{shard_name}.parquet"
    if out_path.exists():
        try:
            existing_meta = pq.read_metadata(str(out_path))
            if existing_meta.num_rows > 0:
                return str(out_path)
        except Exception:
            pass

    objs = pd.read_parquet(OBJECTS_PATH)
    objs = objs.set_index("object_id", drop=False)
    # reindex tolerates missing ids (returns NaN rows we later skip).
    rows = objs.reindex(batch).dropna(subset=["crd_urlpath"]).reset_index(drop=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "image/*"})

    t0 = time.time()
    print(
        f"{shard_name}: fetching {len(rows)} images via CDN with {HTTP_THREADS} threads",
        flush=True,
    )

    def _fetch(oid_url):
        oid, url = oid_url
        return oid, _fetch_image_bytes(session, url)

    results: dict[int, bytes | None] = {}
    work = [
        (int(oid), CRD_IMAGE_BASE + str(path))
        for oid, path in zip(rows["object_id"].tolist(), rows["crd_urlpath"].tolist())
        if isinstance(path, str) and path
    ]

    with ThreadPoolExecutor(max_workers=HTTP_THREADS) as ex:
        futures = [ex.submit(_fetch, w) for w in work]
        n_done = 0
        n_ok = 0
        for fut in as_completed(futures):
            oid, data = fut.result()
            results[oid] = data
            n_done += 1
            if data is not None:
                n_ok += 1
            if n_done % 100 == 0 or n_done == len(futures):
                dt = time.time() - t0
                print(
                    f"{shard_name}: {n_done}/{len(futures)} done, "
                    f"ok={n_ok} ({n_ok / max(1, n_done):.1%}), "
                    f"{n_done/max(dt, 0.001):.1f} rps, {dt:.1f}s",
                    flush=True,
                )

    t_fetch = time.time() - t0

    images: List[Image.Image] = []
    rec_ids: List[int] = []
    img_urls: List[str] = []
    titles: List[str] = []
    artists: List[str] = []
    dates: List[str] = []
    begin_year: List[int] = []
    cultures: List[str] = []
    depts: List[str] = []
    classifications: List[str] = []
    mediums: List[str] = []
    credit: List[str] = []

    for row in rows.itertuples(index=False):
        oid = int(row.object_id)
        data = results.get(oid)
        if data is None:
            continue
        try:
            img = Image.open(io.BytesIO(data)).convert("RGB")
            img.thumbnail((384, 384))
        except Exception:
            continue
        images.append(img)
        rec_ids.append(oid)
        img_urls.append(CRD_IMAGE_BASE + str(getattr(row, "crd_urlpath", "") or ""))
        titles.append(str(getattr(row, "title", "") or ""))
        artists.append(str(getattr(row, "artist", "") or ""))
        dates.append(str(getattr(row, "date", "") or ""))
        try:
            begin_year.append(int(getattr(row, "object_begin_date", 0) or 0))
        except Exception:
            begin_year.append(0)
        cultures.append(str(getattr(row, "culture", "") or ""))
        depts.append(str(getattr(row, "department", "") or ""))
        classifications.append(str(getattr(row, "classification", "") or ""))
        mediums.append(str(getattr(row, "medium", "") or ""))
        credit.append(str(getattr(row, "credit_line", "") or ""))

    if not images:
        print(
            f"{shard_name}: 0 images after {t_fetch:.1f}s — not writing shard "
            f"(will retry on rerun)",
            flush=True,
        )
        return ""

    t1 = time.time()
    model = _get_clip()
    vecs_iter = model.embed(images, batch_size=CLIP_BATCH)
    vecs = np.asarray(list(vecs_iter), dtype="float32")

    # L2 normalize so inner-product ≡ cosine similarity during FAISS reduce.
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms < 1e-12, 1.0, norms)
    vecs = vecs / norms

    t_embed = time.time() - t1

    out_tbl = pa.table({
        "object_id": rec_ids,
        "vector": pa.array(vecs.tolist(), type=pa.list_(pa.float32(), CLIP_DIM)),
        "image_url": img_urls,
        "title": titles,
        "artist": artists,
        "object_date": dates,
        "begin_year": begin_year,
        "culture": cultures,
        "department": depts,
        "classification": classifications,
        "medium": mediums,
        "credit_line": credit,
    })
    pq.write_table(out_tbl, str(out_path))

    print(
        f"{shard_name}: embedded {len(images)}/{len(rows)} "
        f"(fetch={t_fetch:.1f}s, embed={t_embed:.1f}s)",
        flush=True,
    )
    return str(out_path)


# ---------------------------------------------------------------------------
# Reduce — outliers + twins
# ---------------------------------------------------------------------------


def _load_one_vec_shard(path: str):
    try:
        tbl = pq.read_table(path)
    except Exception as exc:
        print(f"  WARN: skipping {path} — {exc}", flush=True)
        return None, None
    if tbl.num_rows == 0:
        return None, None
    df = tbl.drop(["vector"]).to_pandas()
    vec_col = tbl.column("vector")
    try:
        flat = vec_col.combine_chunks().values.to_numpy(zero_copy_only=False)
        v = np.asarray(flat, dtype="float32").reshape(-1, CLIP_DIM)
    except Exception:
        v = np.asarray(vec_col.to_pylist(), dtype="float32")
    if v.ndim != 2 or v.shape[1] != CLIP_DIM:
        return None, None
    return df, v


def _load_all_vec_shards(paths: List[str], max_workers: int = 16):
    from concurrent.futures import ThreadPoolExecutor

    frames: List[pd.DataFrame] = []
    vecs_chunks: List[np.ndarray] = []
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for df, v in ex.map(_load_one_vec_shard, paths):
            done += 1
            if df is None or v is None:
                continue
            frames.append(df)
            vecs_chunks.append(v)
            if done % 100 == 0 or done == len(paths):
                rows = sum(f.shape[0] for f in frames)
                print(
                    f"  loaded {done}/{len(paths)} vec shards, {rows:,} rows "
                    f"({time.time()-t0:.1f}s)",
                    flush=True,
                )
    if not frames:
        return pd.DataFrame(), np.zeros((0, CLIP_DIM), dtype="float32")
    meta = pd.concat(frames, ignore_index=True)
    vecs = np.concatenate(vecs_chunks, axis=0)
    keep = ~meta["object_id"].duplicated(keep="first").values
    meta = meta.loc[keep].reset_index(drop=True)
    vecs = vecs[keep]
    print(f"  after dedupe: {len(meta):,} artworks", flush=True)
    return meta, vecs


def _century(year: int) -> int | None:
    if year is None or year == 0 or year < -3000 or year > 2100:
        return None
    return (int(year) // 100 + (1 if year > 0 else 0))


def _build_index(vecs: np.ndarray):
    import faiss

    n = vecs.shape[0]
    contig = np.ascontiguousarray(vecs, dtype="float32")
    # IVF's training overhead isn't worth it below ~5K points, and faiss will
    # bail if n < nlist anyway. Flat inner-product over a few thousand items is
    # already ~instant.
    if n < 5_000:
        index = faiss.IndexFlatIP(CLIP_DIM)
        index.add(contig)
        return index, contig
    nlist = max(32, int(np.sqrt(n)))
    quantizer = faiss.IndexFlatIP(CLIP_DIM)
    index = faiss.IndexIVFFlat(quantizer, CLIP_DIM, nlist, faiss.METRIC_INNER_PRODUCT)
    train_n = min(n, 200_000)
    train_idx = np.random.RandomState(31).choice(n, size=train_n, replace=False)
    index.train(contig[train_idx])
    index.add(contig)
    index.nprobe = 32
    return index, contig


def _search_knn(index, contig: np.ndarray, k: int, chunk: int = 20_000):
    D_all: List[np.ndarray] = []
    I_all: List[np.ndarray] = []
    n = contig.shape[0]
    t0 = time.time()
    for i in range(0, n, chunk):
        D, I = index.search(contig[i:i + chunk], k)
        D_all.append(D)
        I_all.append(I)
        if (i // chunk) % 5 == 0:
            print(
                f"    knn progress: {min(i+chunk, n):,}/{n:,} "
                f"({time.time()-t0:.1f}s)",
                flush=True,
            )
    return np.concatenate(D_all, axis=0), np.concatenate(I_all, axis=0)


def _pick_outliers(meta: pd.DataFrame, D: np.ndarray, I: np.ndarray, kth: int, top_n: int) -> List[dict]:
    # D is inner-product; higher is more similar. We want artworks whose
    # kth-NN is LEAST similar → most isolated.
    col = min(kth, D.shape[1] - 1)
    sims = D[:, col]
    order = np.argsort(sims)
    picked: List[dict] = []
    for idx in order:
        if len(picked) >= top_n:
            break
        row = meta.iloc[int(idx)]
        # Variety key: department + classification + culture-prefix. Same-series
        # baseball cards / tobacco ephemera share identical classifications, so
        # this prevents a single oddball series from monopolizing the top 20.
        key = (
            str(row.get("department", "")),
            str(row.get("classification", ""))[:30],
            str(row.get("culture", ""))[:20],
        )
        if sum(1 for p in picked if (p["_key"] == key)) >= 2:
            continue
        # Also cap at 3 per department overall for department variety.
        dept = str(row.get("department", ""))
        if sum(1 for p in picked if p["_key"][0] == dept) >= 4:
            continue
        picked.append({
            "_key": key,
            "object_id": int(row["object_id"]),
            "title": str(row.get("title", "")),
            "artist": str(row.get("artist", "")),
            "object_date": str(row.get("object_date", "")),
            "culture": str(row.get("culture", "")),
            "department": str(row.get("department", "")),
            "classification": str(row.get("classification", "")),
            "medium": str(row.get("medium", "")),
            "image_url": str(row.get("image_url", "")),
            "kth_similarity": float(sims[int(idx)]),
            "nearest_5": [
                int(meta.iloc[int(j)]["object_id"])
                for j in I[int(idx), 1:6] if int(j) >= 0 and int(j) != int(idx)
            ],
        })
    for p in picked:
        p.pop("_key", None)
    return picked


def _pick_twins(meta: pd.DataFrame, D: np.ndarray, I: np.ndarray, top_n: int) -> List[dict]:
    """Find pairs of highly-similar artworks that the Met has never flagged:
    we demand different century AND different department or different culture."""
    n = D.shape[0]
    begin = meta["begin_year"].fillna(0).astype("int64").values
    dept = meta["department"].fillna("").values
    culture = meta["culture"].fillna("").values
    classification = meta["classification"].fillna("").values

    artist_arr = meta["artist"].fillna("").values if "artist" in meta.columns else None
    title_arr = meta["title"].fillna("").values if "title" in meta.columns else None

    pairs: List[Tuple[int, int, float]] = []
    seen_pair: set = set()
    for i in range(n):
        for k in range(1, min(D.shape[1], 8)):
            j = int(I[i, k])
            if j < 0 or j == i:
                continue
            sim = float(D[i, k])
            if sim < TWIN_SIM_THRESHOLD:
                break  # sorted desc
            # Cull exact-dupe photographs (sim ~= 1.0) which are almost never
            # a genuine discovery — they're plate duplicates / catalog twins.
            if sim > TWIN_SIM_MAX:
                continue
            ci = _century(int(begin[i]))
            cj = _century(int(begin[j]))
            if ci is None or cj is None:
                continue
            century_gap = abs(ci - cj)
            same_dept = dept[i] == dept[j] and dept[i] != ""
            same_culture = culture[i] == culture[j] and culture[i] != ""
            # Same artist is almost always visually similar — not surprising.
            if artist_arr is not None and artist_arr[i] == artist_arr[j] and artist_arr[i] != "":
                continue
            # Interesting pair = distant in time OR cross-departmental / cross-culture.
            if century_gap < TWIN_MIN_CENTURY_GAP and same_dept and same_culture:
                continue
            if century_gap < 1 and same_culture:
                continue
            # Near-identical titles are usually catalog dupes.
            if title_arr is not None:
                ta, tb = str(title_arr[i])[:40].lower(), str(title_arr[j])[:40].lower()
                if ta and ta == tb:
                    continue
            key = (min(i, j), max(i, j))
            if key in seen_pair:
                continue
            seen_pair.add(key)
            pairs.append((i, j, sim))
            break
    # Sort by: (century_gap desc, similarity desc) — time-travelling twins are
    # the viral artifact, not the most similar pair.
    pairs.sort(key=lambda t: (
        -abs((_century(int(begin[t[0]])) or 0) - (_century(int(begin[t[1]])) or 0)),
        -t[2],
    ))
    out: List[dict] = []
    for i, j, sim in pairs[:top_n]:
        a = meta.iloc[i]
        b = meta.iloc[j]
        out.append({
            "similarity": round(sim, 4),
            "century_gap": abs((_century(int(begin[i])) or 0) - (_century(int(begin[j])) or 0)),
            "a": {
                "object_id": int(a["object_id"]),
                "title": str(a.get("title", "")),
                "artist": str(a.get("artist", "")),
                "object_date": str(a.get("object_date", "")),
                "culture": str(a.get("culture", "")),
                "department": str(a.get("department", "")),
                "image_url": str(a.get("image_url", "")),
            },
            "b": {
                "object_id": int(b["object_id"]),
                "title": str(b.get("title", "")),
                "artist": str(b.get("artist", "")),
                "object_date": str(b.get("object_date", "")),
                "culture": str(b.get("culture", "")),
                "department": str(b.get("department", "")),
                "image_url": str(b.get("image_url", "")),
            },
        })
    return out


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


_CSS = """
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         max-width: 1180px; margin: 40px auto; padding: 0 20px; line-height: 1.55; color: #0f172a; }
  h1 { font-size: 34px; margin-bottom: 4px; }
  h2 { font-size: 20px; margin-top: 32px; margin-bottom: 6px; }
  .sub { color: #64748b; margin-top: 0; font-size: 15px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 18px; margin: 18px 0; }
  .card { border: 1px solid #e2e8f0; border-radius: 12px; overflow: hidden; background: #fff;
          box-shadow: 0 1px 2px rgba(15,23,42,0.06); display: flex; flex-direction: column; }
  .card img { width: 100%; height: 260px; object-fit: cover; display: block; background: #f1f5f9; }
  .card .body { padding: 12px 14px; font-size: 13px; flex: 1; }
  .card .label { font-size: 11px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
  .card h3 { font-size: 14px; margin: 4px 0 6px 0; }
  .card .meta { color: #475569; font-size: 12px; margin: 2px 0; }
  .twin { display: grid; grid-template-columns: 1fr 1fr; gap: 0; border: 1px solid #e2e8f0;
          border-radius: 12px; overflow: hidden; margin: 14px 0; background: #fff;
          box-shadow: 0 1px 2px rgba(15,23,42,0.06); }
  .twin .side { padding: 10px 14px; }
  .twin img { width: 100%; height: 260px; object-fit: cover; background: #f1f5f9; display: block; }
  .twin .hdr { display: flex; align-items: center; justify-content: space-between; padding: 10px 14px;
               background: #f8fafc; border-bottom: 1px solid #e2e8f0; font-size: 12px; color: #334155; }
  .chip { background: #e0f2fe; color: #075985; border-radius: 999px; padding: 2px 10px; font-size: 11px; }
  .footer { color: #94a3b8; font-size: 12px; margin-top: 40px; }
  a { color: #2563eb; text-decoration: none; } a:hover { text-decoration: underline; }
</style>
"""


def _obj_link(oid: int) -> str:
    return f"https://www.metmuseum.org/art/collection/search/{oid}"


def _c(s) -> str:
    """Coerce NaNs and 'nan' strings to empty so HTML renders cleanly."""
    if s is None:
        return ""
    s = str(s)
    if s == "nan" or s == "NaN":
        return ""
    return s


def _render_outliers_html(rows: List[dict], total: int, generated_at: str) -> str:
    cards = []
    for rank, r in enumerate(rows, 1):
        url = _c(r.get("image_url"))
        title = _c(r.get("title")) or "(untitled)"
        culture = _c(r.get("culture"))
        date = _c(r.get("object_date"))
        dept = _c(r.get("department"))
        cls = _c(r.get("classification"))
        cards.append(
            f"""<div class=card>
  <a href='{_obj_link(r['object_id'])}' target=_blank><img loading=lazy src='{url}' alt=''/></a>
  <div class=body>
    <div class=label>#{rank} · isolation {1 - r['kth_similarity']:.2f}</div>
    <h3>{title[:140]}</h3>
    <div class=meta>{date}  · {culture or '—'}</div>
    <div class=meta>{dept}  · {cls}</div>
  </div>
</div>"""
        )
    grid = "\n".join(cards)
    return f"""<!doctype html><meta charset=utf-8><title>The Weirdest Art at the Met</title>{_CSS}
<h1>The Weirdest Art at the Met</h1>
<p class=sub>The {len(rows)} most visually isolated artworks in the Met's {total:,}-piece public
domain collection. Each was CLIP-embedded, then ranked by how dissimilar its {KTH_FOR_ISOLATION}th
visual neighbor is across five thousand years of human-made objects. No human curated this list.</p>
<div class=grid>{grid}</div>
<div class=footer>Generated {generated_at}. Source: Met Museum Open Access via Burla.</div>"""


def _render_twins_html(twins: List[dict], total: int, generated_at: str) -> str:
    blocks = []
    for rank, t in enumerate(twins, 1):
        a, b = t["a"], t["b"]
        a_img = _c(a.get("image_url")); b_img = _c(b.get("image_url"))
        a_title = _c(a.get("title")) or "(untitled)"
        b_title = _c(b.get("title")) or "(untitled)"
        a_date = _c(a.get("object_date")); b_date = _c(b.get("object_date"))
        a_culture = _c(a.get("culture")); b_culture = _c(b.get("culture"))
        a_dept = _c(a.get("department")); b_dept = _c(b.get("department"))
        a_artist = _c(a.get("artist")); b_artist = _c(b.get("artist"))
        blocks.append(
            f"""<div class=twin>
  <div class=hdr>
    <div><b>#{rank}</b>  · cosine similarity <b>{t['similarity']:.3f}</b></div>
    <div><span class=chip>{t['century_gap']} centuries apart</span></div>
  </div>
  <div style='display:grid;grid-template-columns:1fr 1fr;gap:0;'>
    <a href='{_obj_link(a['object_id'])}' target=_blank><img loading=lazy src='{a_img}' alt=''/></a>
    <a href='{_obj_link(b['object_id'])}' target=_blank><img loading=lazy src='{b_img}' alt=''/></a>
  </div>
  <div style='display:grid;grid-template-columns:1fr 1fr;gap:0;'>
    <div class=side>
      <div class=label>{a_dept}  · {a_culture or '—'}</div>
      <h3>{a_title[:120]}</h3>
      <div class=meta>{a_date}  · {a_artist or 'Unknown'}</div>
    </div>
    <div class=side>
      <div class=label>{b_dept}  · {b_culture or '—'}</div>
      <h3>{b_title[:120]}</h3>
      <div class=meta>{b_date}  · {b_artist or 'Unknown'}</div>
    </div>
  </div>
</div>"""
        )
    body = "\n".join(blocks)
    return f"""<!doctype html><meta charset=utf-8><title>The Met's Hidden Twins</title>{_CSS}
<h1>The Met's Hidden Twins</h1>
<p class=sub>The {len(twins)} most visually similar artwork pairs in the Met's
{total:,}-piece public domain collection that were NOT flagged as related by
the museum — different centuries, different departments, different cultures,
but nearly identical to CLIP.</p>
{body}
<div class=footer>Generated {generated_at}. Source: Met Museum Open Access via Burla.</div>"""


def reduce_met(vec_paths: List[str]) -> str:
    _ensure_dirs()
    t0 = time.time()
    vec_paths = [p for p in (vec_paths or []) if p]
    if not vec_paths:
        vec_paths = sorted(str(p) for p in VEC_DIR.glob("*.parquet"))
        print(f"reduce: globbed {len(vec_paths)} vec shards", flush=True)

    meta, vecs = _load_all_vec_shards(vec_paths)
    if meta.empty:
        print("reduce: no records loaded", flush=True)
        return str(OUT_DIR)

    print(f"reduce: loaded {len(meta):,} artworks in {time.time()-t0:.1f}s", flush=True)

    t1 = time.time()
    index, contig = _build_index(vecs)
    print(f"reduce: faiss index built in {time.time()-t1:.1f}s", flush=True)

    t2 = time.time()
    k = max(KTH_FOR_ISOLATION + 1, 8)
    D, I = _search_knn(index, contig, k=k)
    print(f"reduce: knn ({k}-NN) done in {time.time()-t2:.1f}s", flush=True)

    outliers = _pick_outliers(meta, D, I, kth=KTH_FOR_ISOLATION, top_n=TOP_OUTLIERS)
    twins = _pick_twins(meta, D, I, top_n=TOP_TWINS)
    print(
        f"reduce: {len(outliers)} outliers, {len(twins)} twins",
        flush=True,
    )

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (OUT_DIR / "weirdest.html").write_text(
        _render_outliers_html(outliers, len(meta), generated_at), encoding="utf-8"
    )
    (OUT_DIR / "twins.html").write_text(
        _render_twins_html(twins, len(meta), generated_at), encoding="utf-8"
    )
    (OUT_DIR / "summary.json").write_text(json.dumps({
        "total_artworks": int(len(meta)),
        "n_shards": len(vec_paths),
        "outlier_count": len(outliers),
        "twin_count": len(twins),
        "reduce_elapsed_s": round(time.time() - t0, 2),
        "generated_at_utc": generated_at,
        "top_outliers_preview": [
            {"object_id": o["object_id"], "title": o["title"], "kth_similarity": o["kth_similarity"]}
            for o in outliers[:5]
        ],
        "top_twins_preview": [
            {
                "similarity": t["similarity"],
                "century_gap": t["century_gap"],
                "a_id": t["a"]["object_id"],
                "b_id": t["b"]["object_id"],
            }
            for t in twins[:5]
        ],
    }, indent=2))

    print(f"reduce done in {time.time()-t0:.1f}s. artifacts → {OUT_DIR}", flush=True)
    return str(OUT_DIR)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main() -> int:
    from burla import remote_parallel_map  # type: ignore

    reduce_only = os.environ.get("REDUCE_ONLY", "").strip() not in ("", "0", "false", "False")
    skip_discovery = os.environ.get("SKIP_DISCOVERY", "").strip() not in ("", "0", "false", "False")

    cap = int(os.environ.get("MET_MAX_OBJECTS", "0") or "0")
    max_workers = int(os.environ.get("MET_MAX_WORKERS", "8") or "8")

    if reduce_only:
        print("REDUCE_ONLY=1: skipping discovery + fetch_and_embed, reducing over vec shards")
        vec_paths: List[str] = []
    else:
        if skip_discovery:
            print("SKIP_DISCOVERY=1: using existing objects.parquet")
            # Re-batch from existing parquet, locally, without downloading.
            df = pd.read_parquet(OBJECTS_PATH) if OBJECTS_PATH.exists() else pd.DataFrame()
            ids = df["object_id"].astype("int64").tolist() if not df.empty else []
            random.Random(42).shuffle(ids)
            if cap and cap < len(ids):
                ids = ids[:cap]
            batches = [ids[i:i + BATCH_SIZE] for i in range(0, len(ids), BATCH_SIZE)]
            print(f"skip-discover: {len(ids):,} ids → {len(batches)} batches")
        else:
            print("stage 0: discovering Met open access objects on a worker ...")
            params = {"cap": cap, "batch_size": BATCH_SIZE}
            [batches] = list(remote_parallel_map(
                discover_objects, [params],
                func_cpu=8, func_ram=32,
            ))
            total_ids = sum(len(b) for b in batches)
            print(f"stage 0 done. batches: {len(batches)} ({total_ids:,} object ids)")

        # CRDImages CDN isn't rate-limited per-IP (unlike the IIIF endpoint),
        # so we can scale workers + threads until bandwidth caps us.
        print(
            f"map: fetching + CLIP-embedding {len(batches)} batches "
            f"(max_parallelism={max_workers}, {HTTP_THREADS} http threads/worker) "
            f"across workers ..."
        )
        vec_paths_raw = list(remote_parallel_map(
            fetch_and_embed, batches,
            func_cpu=1, func_ram=4,
            max_parallelism=max_workers,
        ))
        vec_paths = [p for p in vec_paths_raw if p]
        print(f"map done. vec shards returned: {len(vec_paths)} (of {len(vec_paths_raw)} tasks)")

    [results_dir] = list(remote_parallel_map(
        reduce_met, [vec_paths],
        func_cpu=16, func_ram=64,
    ))
    print(f"reduce done. results: {results_dir}")
    return 0


def main_local() -> int:
    _ensure_dirs()
    cap = int(os.environ.get("MET_MAX_OBJECTS", "400") or "400")
    batches = discover_objects({"cap": cap, "batch_size": 50})
    print(f"LOCAL: fetching {sum(len(b) for b in batches)} objects across {len(batches)} batches")
    vec_paths = [fetch_and_embed(b) for b in batches]
    reduce_met(vec_paths)
    return 0


if __name__ == "__main__":
    if os.environ.get("LOCAL", "").strip() not in ("", "0", "false", "False"):
        raise SystemExit(main_local())
    raise SystemExit(main())
