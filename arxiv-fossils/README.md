# Fossils of the arXiv — a Burla demo

**Live site: <https://burla-cloud.github.io/arxiv-fossils/>**

Cluster every one of the **2,710,783 arXiv abstracts** ever posted, project
them into 384-dimensional sentence-embedding space, and let three simple
questions fall out of the geometry:

1. **Which research topics peaked and then quietly collapsed?** (the extinct)
2. **Which ones appeared essentially overnight in the last 24 months?** (the emergent)
3. **Which single paper sits furthest from every other paper ever written?** (the loneliest)

No prior narrative, no keyword list. The clusters are drawn by MiniBatchKMeans
on top of MiniLM-L6-v2 embeddings; the extinct/emergent/lonely labels are
just rankings over cluster-year time series and nearest-neighbor distance.

## Headline

> **The loneliest paper in science is `2203.12842` — "Financial statements
> of companies in Norway"**. Its 5th nearest neighbor across 2.71 M papers
> has cosine similarity **0.138**. Nothing else on arXiv is remotely about
> the same thing.

|  |  |
|---|---:|
| Abstracts embedded | **2,710,783** |
| Embedding model | `sentence-transformers/all-MiniLM-L6-v2` (384-d, ONNX via fastembed) |
| Shards on `/workspace/shared` | 272 |
| Clusters | 400 (MiniBatchKMeans) |
| Serial equivalent compute | **~75–90 min** (single laptop thread) |
| **Burla wall-clock (discover + map + reduce)** | **~25 min** |
| Reduce stage only | **142.4 s** |
| Peak parallel workers | 16 |

## Top-10 extinct topics (peaked → then collapsed)

Each row is a cluster. "% of peak" is the last-5-year paper rate as a
fraction of the peak-year rate; the categories are the 5 most-common arXiv
primary-category tags across the cluster.

| # | Peak year | % of peak | Top arXiv categories (flavor) |
|:---:|:---:|:---:|:---|
| 1 | 2000 | **16.7 %** | hep-th, gr-qc, hep-ph, astro-ph.CO — *Randall-Sundrum braneworld cosmology* |
| 2 | 1997 | 18.0 % | hep-ph, hep-th, hep-ex — *weak-scale SUSY model building* |
| 3 | 2020 | 19.7 % | q-bio.PE, physics.soc-ph, stat.AP — *pandemic-era SIR epidemic modelling* |
| 4 | 2005 | 30.3 % | cond-mat.mes-hall, physics.app-ph — *single-walled carbon nanotube assembly* |
| 5 | 2009 | 34.2 % | quant-ph, hep-th, gr-qc — *Casimir-force measurement* |
| 6 | 2015 | 35.1 % | hep-ph, hep-ex — *Higgs / 2HDM post-discovery phenomenology* |
| 7 | 2011 | 37.0 % | cond-mat.mes-hall — *graphene on SiC / substrate engineering* |
| 8 | 2012 | 37.1 % | hep-ex, hep-ph — *heavy-quark production at LHC run-1 energies* |
| 9 | 2002 | 37.9 % | hep-ph, nucl-th — *pion electromagnetic form factors* |
| 10 | 2002 | 38.4 % | hep-th, hep-ph, gr-qc — *pp-wave / AdS-CFT correspondence* |

Full report with sample-paper links: `arxiv_fossils_out/extinct.html`.

## Top-3 emergent topics (last 24 months concentration)

| # | % of all papers in last 24 months | Flavor |
|:---:|:---:|:---|
| 1 | **55.3 %** | cs.CL, cs.AI — *LLM capabilities, evaluations, and alignment* |
| 2 | 51.9 % | cs.CV, cs.LG — *generative / diffusion models for imagery + video* |
| 3 | 44.3 % | cs.CL, cs.AI, cs.LO — *LLM reasoning, tool-use, math/code benchmarks* |

Full report: `arxiv_fossils_out/emergent.html`.

## Why arXiv

arXiv is the only open, full-corpus, >30-year record of what scientists were
actually working on. The metadata snapshot
(`arxiv-metadata-oai-snapshot.json`, ~3.8 GB) is public on Kaggle/Hugging
Face; every single abstract is available without API rate limits, which makes
it the right shape for a remote_parallel_map sweep.

**Limitations the demo does not hide:**

1. The "extinct" topics that peaked in the 1990s–2000s are often still
   actively published, just at lower rates relative to the explosion of
   ML/CV papers; our "% of peak" metric is honest but not adjusted for the
   growth of the arXiv itself.
2. MiniLM-L6-v2 is trained on general web text. It clusters "LLM evaluation"
   and "LLM safety" into the same cluster even though they are subfields.
   Bigger models (`all-mpnet-base-v2`, `bge-large-en`) would give finer
   clusters but 3–5× the compute budget.
3. The single loneliest paper is sensitive to K (we use the 5th-NN). Using
   20-NN tightens the ranking but the top-1 result is very stable — the
   Norway financial-statements paper is the one actual outlier in the whole
   corpus. It's legitimately the canonical outlier.

## Data source

- Full arXiv metadata snapshot: `https://huggingface.co/datasets/Cornell-University/arxiv`
  (one JSONL, ~3.8 GB, refreshed monthly)
- Fields used: `id`, `title`, `abstract`, `categories`, `update_date`

## How it works

Three stages, one pipeline:

- **Stage 0 — `discover_papers`** (1 worker, 16 CPU, 64 GB RAM). Downloads
  the arXiv snapshot, streams the JSONL, filters papers with a non-empty
  abstract, and reshards into 10,000-paper parquet shards under
  `/workspace/shared/arxiv-fossils/raw/`.
- **Map — `embed_shard`** (16 workers × 1 CPU × 4 GB). Each worker reads one
  raw shard, embeds every abstract with `fastembed`'s ONNX export of
  MiniLM-L6-v2, L2-normalizes, and writes the matching vector shard to
  `/workspace/shared/arxiv-fossils/vec/`. `OMP_NUM_THREADS=1` is pinned so
  onnxruntime doesn't over-commit on the cgroup-limited workers.
- **Reduce — `reduce_arxiv`** (1 worker, 16 CPU, 64 GB). Loads every vector
  shard, concatenates, builds an IVF FAISS cosine index, runs
  MiniBatchKMeans(k=400), computes per-cluster year histograms, ranks
  extinct/emergent clusters, and finds the single loneliest paper by k-NN
  distance. Renders three HTML reports.

## How to run

```bash
# Full Burla run (~25 min wall-clock)
python arxiv_fossils.py

# Reduce-only (skip discover + map, reuse existing vec shards on Burla shared)
REDUCE_ONLY=1 python arxiv_fossils.py

# Cap corpus for a fast dry-run (embed 100K papers instead of 2.7M)
ARXIV_MAX_PAPERS=100000 python arxiv_fossils.py
```

Artifacts land in `/workspace/shared/arxiv-fossils/out/` on the Burla
cluster. Copy them down with `fetch_artifacts.py` (TODO) or directly via
the Burla dashboard.

## Artifacts (in `arxiv_fossils_out/`)

| File | Contents |
|---|---|
| `extinct.html` | Top-10 clusters that peaked and collapsed |
| `emergent.html` | Top-10 clusters concentrated in the last 24 months |
| `loneliest.html` | The single paper furthest from anything else |
| `summary.json` | Paper count, shard count, cluster count, timings |

## Files

```
arxiv_fossils.py         discover + embed + reduce in one script
arxiv_fossils_out/       artifacts from the latest run
requirements.txt         burla + fastembed + faiss-cpu + pandas + ...
```

---

*Source: arXiv metadata snapshot · Embeddings: MiniLM-L6-v2 via fastembed · Index: FAISS IVF · Clustering: MiniBatchKMeans · Orchestration: Burla `remote_parallel_map`.*
