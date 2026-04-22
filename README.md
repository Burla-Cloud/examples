# 1M READMEs — a Burla demo

**Live site: <https://burla-cloud.github.io/github-repo-summarizer/>**

We streamed **1,200,000 real GitHub READMEs** — pulled from
`bigquery-public-data.github_repos`, one per repo, 1.3 GB of compressed
Parquet — through a Burla cluster of **500+ parallel CPUs**, ran
deterministic summary heuristics on every one of them, and built a
searchable explorer that tells you what the internet is actually building.

**Zero LLM calls.** Titles, TLDRs, categories, install methods, keyword
rankings, and TF-IDF distinctive words all come from regex + word counts.

## The headline

> **One in three public repos is web.** 425,681 of the 1.2M (35.5%) are
> some flavor of node/npm/react. **Python owns ML** (36.5% of top ML
> repos). **Go owns DevOps** (26.5%). **Objective-C still dominates Mobile**
> in this dataset (36.5% — the BigQuery snapshot is 2016-era). **Security
> README culture is allergic to install instructions** — 72.5% of top
> security repos have no install block at all. And: *twenty-plus* of the
> top-30 longest READMEs in the corpus are different forks of **the same
> 31 KB MEAN stack template**.

|  |  |
|---|---:|
| Repos processed | **1,200,000** |
| Source rows scanned | **2.3 B files, 281 M contents** |
| Languages represented | **30+** |
| Heuristic categories | **14** |
| Pipeline shards (map) | **600** |
| Reduce buckets | **16** |
| Peak concurrent Burla workers | **500+** |
| Map throughput | **~25,000 repos / sec** |
| Burla wall-clock (map + reduce) | **47.9 s + 23.4 s** |
| LLMs used | **zero** |

## What's in this repo

- **`index.html` + `css/` + `js/`** — the GitHub-parody explorer. Hero
  with live stats, category landscape (14 bars), category grid, 9 findings
  cards (bar charts, language dominance, install-by-category, repo lists,
  TF-IDF distinctive words), and a client-side search index over ~6k
  sampled repos.
- **`FINDINGS.md`** — the full writeup of the nine findings.
- **`data/`** — the frontend artifacts:
  - `index.json` — headline stats + top categories / languages / installs
  - `categories.json` — per-category metadata
  - `findings.json` — the nine ranked findings in structured form
  - `search.json` — sampled search index (6,000 repos)
  - `categories/*.json` — one file per category with its top repos
- **`prepare.py`** — pulls 1.2M READMEs from BigQuery via
  `bigquery-public-data.github_repos`, streams Arrow batches directly to
  a zstd-compressed Parquet file on disk (no in-memory materialization).
- **`probe.py`** — BigQuery + README access smoke test.
- **`scale.py`** — uploads the parquet to the Burla shared filesystem via
  a scatter-gather pattern (N parallel workers write chunks → 1 finalizer
  concatenates + decompresses), then fans out `summarize_shard(600)`.
- **`pipeline.py`** — the worker. Streams its stripe of `readmes.parquet`
  with `pq.iter_batches()` (never loads the 1.3 GB file), extracts
  title/tldr/install/category/badges/code-fence-count/token list for each
  README, emits a per-shard JSON to `/workspace/shared/grs/shards/`.
- **`reduce.py`** — 16 parallel reducers merge ~40 shard files each into
  bucket summaries, then a final local merge produces
  `samples/grs_reduced.json`.
- **`analysis.py`** — local, LLM-free: TF-IDF over 14 category documents,
  dedupe, 9 findings generation, search-index emission.

## The 9 findings (shortlist)

1. **The map of open-source GitHub** — 14 categories, web dominates at 35.5%
2. **Which languages own which categories** — Python owns ML + data; Go
   owns DevOps; JavaScript is top-2 in every single category
3. **How the thing installs by category** — ML is pip, web is npm, devops
   is docker, mobile is brew. 74.6% of all repos give no install block.
4. **The most lovingly documented repos on GitHub** — ranked by `badges × 3
   + code fences`. Top: `mschile/terra-core` (186 badges, 1 code fence).
5. **The loneliest READMEs on GitHub** — 297-byte placeholders, "*Actual
   Name TBD*" projects, ShareX forks with no description.
6. **The words that define each category** — TF-IDF over 14 category
   documents. ML: tensorflow/neural/keras/recurrent. Crypto:
   ethereum/wallet/truffle/geth. OS: kernel/linux/boot/libvirt.
7. **The longest READMEs we found** — mostly `awesome-*` lists and the
   same MEAN stack template cloned into 24 different repos.
8. **The winner-takes-all install ecosystems** — web (npm, 62.8%),
   security (no install specified, 72.5%), mobile (no install, 62.8%).
9. **The awesome-list epidemic** — **20+ forks** of `awesome-datascience`
   alone. Curating other people's code has become its own genre.

See **[`FINDINGS.md`](./FINDINGS.md)** for the numbers.

## Reproduce

```bash
# One-time setup
curl -fsSL https://raw.githubusercontent.com/Burla-Cloud/burla-agent-starter-kit/main/install.sh | sh
pip install -r requirements.txt

# Auth BigQuery
gcloud auth application-default login

# Pull 1.2M READMEs into samples/readmes.parquet (~30 s, 1.3 GB)
python prepare.py --out samples/readmes.parquet

# Upload parquet to Burla shared fs + fan out 600 shards (500+ workers)
python scale.py

# Reduce across 16 buckets (~23 s on Burla)
python reduce.py

# Local TF-IDF + findings + search index (~30 s)
python analysis.py

# Serve
python -m http.server 8767
# open http://localhost:8767
```

## Why `bigquery-public-data.github_repos`

- It's the only public, indexed, queryable dataset of GitHub README
  content at scale — ~2.3B file rows and 281M file-contents rows, stored
  in BigQuery and re-hostable under the Google Cloud public-data program.
- No scraping. No API rate limits. One BigQuery job — `JOIN files +
  contents + languages`, filter to `path LIKE '%README%'`, take the
  biggest README per repo — produces a 1.3 GB parquet in 30 seconds that
  contains 1.2M repos ready for parallel processing.
- The snapshot is 2016-era (BigQuery licensing), which is part of the
  finding: it shows what *becoming a mainstream language ecosystem looks
  like on the upswing* — Objective-C is still #1 for mobile, Swift is at
  29%, Go has already taken DevOps. Running the same pipeline on a
  fresher mirror would re-shuffle a few leaderboards but the *shape* of
  the landscape wouldn't change much.

## Caveats

- **Categorization is keyword-based, not learned.** Deliberate: we wanted
  a deterministic, reproducible, LLM-free pipeline. "Web" is a catch-all
  whenever `npm` / `node` / `react` appears. A repo mentioning kubernetes
  in passing might land in DevOps even if it's actually a library. The
  scale is the point, not per-repo precision.
- **Many "READMEs" are boilerplate.** MEAN stack template clones, Cobra
  library forks, `awesome-*` list copies. We keep the duplicates in
  because the duplication *is the finding* — open source runs on copy-
  paste.
- **No LLMs.** The only "model" in this pipeline is TF-IDF. Every title,
  TLDR, install method, category, and ranked list was produced by regex
  and word count.

Part of the Burla demo collection. Source: [`Burla-Cloud/burla-agent-starter-kit`](https://github.com/Burla-Cloud/burla-agent-starter-kit).
