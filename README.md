# Amazon Review Distiller — a Burla demo

**Live site: <https://burla-cloud.github.io/amazon-review-distiller/>**

We streamed **571 million Amazon reviews** — the entire public
`McAuley-Lab/Amazon-Reviews-2023` corpus on HuggingFace, **275 GB of raw
JSONL** — through a Burla cluster of **500+ parallel CPUs**, scored every
single review on profanity / screaming / punctuation / length / rage-vs-star
mismatch, and built the **Wall of Fucked Up**: a rank-ordered shrine to the
most unhinged things humans have ever typed into the shopping-cart equivalent
of a grief journal.

No LLM sanitized anything. Every review is a real, verbatim string from
a real Amazon purchase.

## The headline

> The filthiest category on Amazon is **Video Games** — 6.54% of all
> video-game reviews contain at least one strong profanity. The loudest
> single review is **1,169 words of ALL CAPS** from a self-described "100%
> disabled decorated Vietnam veteran and Mozart scholar," who starts with an
> apology for the caps (macular degeneration) and then uses them for the rest
> of the paragraph. The longest single run of exclamation marks we found is
> **10,594 "!"s** in a two-word review of a baby product: *"love these"*.

|  |  |
|---|---:|
| Reviews parsed | **571,544,386** |
| Raw data streamed | **275 GB** (HTTP Range reads, no local download) |
| Categories | **34** |
| Reviews tagged profane | **20,187,204** (3.53% globally) |
| Source | `McAuley-Lab/Amazon-Reviews-2023` (HuggingFace) |
| Byte-range chunks dispatched | **545** |
| Peak concurrent Burla workers | **500+** |
| Map wall-clock | **3.21 min** |
| Reduce wall-clock | **9.2 s** |
| LLMs used | **zero** |

## What's in this repo

- **`index.html` + `css/` + `js/`** — the Amazon-parody site. Wall of Fucked
  Up hero, category grid (click any to see the top-100 unhinged reviews for
  that category), nine findings cards, search bar that hits all 34 categories
  client-side, and an **Unhinged Mode** toggle that hides star ratings and
  just shows the rage.
- **`FINDINGS.md`** — the full writeup of the nine findings.
- **`data/`** — frontend artifacts: `overall.json` (aggregate stats),
  `wall.json` (the ranked Wall), `categories.json` (per-category metadata),
  `findings.json` (the 9 findings), and `categories/*.json` (per-category
  top-reviews).
- **`probe.py`** — verify you can stream one HuggingFace JSONL shard.
- **`pipeline.py`** — the worker. Takes a `(file_path, start_byte, end_byte,
  chunk_id)` tuple, opens a range request against the HF CDN, parses review
  JSONL, scores every row, emits per-chunk JSON with top-scoring reviews per
  bucket (profanity, screaming, exclamation, short-brutal, long-rant, 5-star
  with rage, etc.).
- **`scale.py`** — fans out 545 byte-range chunks to Burla with
  `remote_parallel_map`, `max_parallelism=500+`.
- **`reduce.py`** — 34 parallel reducers (one per category) plus a global
  reducer that aggregates every chunk's top-K lists into a single
  `ard_reduced.json`.
- **`analysis.py`** — local: deduplicate, filter proper-noun spam, rescore
  for profanity variety, produce the final `wall.json` and `findings.json`.

## The 9 findings (shortlist)

1. **The filthiest categories ranked.** Video Games 6.54%, Movies & TV 5.93%,
   CDs & Vinyl 5.66%, Kindle Store 5.41%. Gift Cards last at 1.19%.
2. **The loudest reviewers on Amazon.** A 1,169-word all-caps rant about a
   Mozart CD is #1.
3. **Punctuation bombs.** *"love these" !!!!!!!!!!!!!!!!!!!!!!!!!…* × 10,594.
4. **Reviews too brutal for two sentences.** The Wall of Fucked Up hero
   section. The haikus of Amazon despair.
5. **Rant hall of fame.** A 2,000-word monologue about an oil-covered motor
   unit that cascades into a critique of Amazon returns policy.
6. **Five-star reviews that scream.** People who loved the product but still
   typed in ALL CAPS.
7. **One-star reviews with a smile emoji in the title.** Passive-aggressive
   gold.
8. **Profanity diversity.** Reviews using ≥5 unique strong curse words.
9. **Repeat offenders.** Reviewer IDs with the highest per-review profanity
   density across the entire corpus.

See **[`FINDINGS.md`](./FINDINGS.md)** for the full numbers.

## Reproduce

```bash
# One-time setup
curl -fsSL https://raw.githubusercontent.com/Burla-Cloud/burla-agent-starter-kit/main/install.sh | sh
pip install -r requirements.txt

# Verify HF streaming works (1k reviews, ~30 s)
python probe.py

# Run the Burla pipeline (545 byte chunks × 500+ workers, ~3 min)
python scale.py

# Reduce (34 categories + global, ~10 s)
python reduce.py

# Local analysis + dedupe + rescore (~30 s)
python analysis.py

# Serve
python -m http.server 8766
# open http://localhost:8766
```

## Why McAuley-Lab/Amazon-Reviews-2023

- It's the largest public Amazon review dump — **571 million reviews**
  across 34 categories — released for academic use by the McAuley lab at
  UCSD. Newer than the 2018 dump and far larger.
- Served as one `.jsonl.gz` per category from the HuggingFace CDN, which
  supports HTTP Range requests. **We never download a file** — every
  worker streams its own byte range and processes on the fly.
- Every row has `title`, `text`, `rating`, `helpful_vote`, `timestamp`,
  `asin`, `parent_asin`, `user_id`. No customer PII beyond the anonymous
  `user_id`.

## Caveats / content warning

- **The profanity is real.** We do not censor or rewrite. Every string on
  the Wall of Fucked Up is copy-paste from a real Amazon purchase. Open the
  site with headphones / at home / behind your employer's HR firewall.
- **Profanity detection is rule-based**, not model-based. We used a
  curated word list of strong English profanity + a proper-noun filter
  to avoid false positives on brand names. Reviews in languages other
  than English are scored by length/caps/exclamation only.
- **We dedupe and re-score.** Amazon reviews have a non-trivial volume of
  "crap crap crap crap crap" spam from the same few users; we filter
  these out of the Wall so the genuinely creative rage isn't buried.
- **No LLM touches the text.** Every ranking, bucket, and finding in this
  repo is produced by regex, tokenization, and arithmetic. The *site* has
  an LLM-free pipeline end to end.

Part of the Burla demo collection. Source: [`Burla-Cloud/burla-agent-starter-kit`](https://github.com/Burla-Cloud/burla-agent-starter-kit).
