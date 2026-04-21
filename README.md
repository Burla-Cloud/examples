# The Met's Hidden Twins — a Burla demo

**Live site: <https://jmp1062.github.io/met-weirdest-art/>**

We fetched **every Met Museum CC0 artwork with a published image** (≈192 K
pieces from the 470 K-item Open Access collection), ran CLIP ViT-B/32 over all
of them, and asked the pixels one question: *which artworks, separated by
thousands of years and half the planet, look like each other anyway?*

No tags, no curator metadata. The neighborhoods fall out of the pixels.

## The headline

> The closest "hidden twin" in Met history is a pair of objects
> **49 centuries apart**, with a cosine similarity of **0.932**: a
> 19th-century Birmingham silverware case (object
> [186597](https://www.metmuseum.org/art/collection/search/186597)) and a
> Bronze Age Cypriot dagger blade (object
> [244170](https://www.metmuseum.org/art/collection/search/244170)) that
> CLIP — a model trained on random internet images — correctly sees as
> visually nearly identical despite no one in the Met ever filing them
> together.

|  |  |
|---|---:|
| Artworks embedded | **191,922** |
| Embedding model | `Qdrant/clip-ViT-B-32-vision` (512-d, ONNX via fastembed) |
| Images fetched from CRDImages CDN | 191,922 (~420 GB at source) |
| Shards on `/workspace/shared` | 428 |
| Cross-century "hidden twins" surfaced | **30** |
| Tightest match (cosine) | **0.959** |
| Biggest time gap | **49 centuries** |
| Serial equivalent compute | **~4–12 h** (single IP gets rate-limited by the CDN first) |
| **Burla wall-clock (discover + fetch + embed + reduce)** | **~45–60 min** |
| Reduce stage only | **49.7 s** |
| Peak parallel workers | 4–8 (intentionally capped to be polite to the CDN) |

## What's in this repo

- **`index.html`** — the shareable single-page gallery. All 30 pairs rendered
  inline with images and metadata, hero diptych, full methodology, honest
  caveats. This is what we link.
- **`met_weirdest.py`** — one script: discover + fetch + embed + reduce.
- **`met_weirdest_out/`** — raw artifacts from the latest run (parquet shards,
  summary.json, and two HTML subpages: `twins.html` is the standalone gallery
  and `weirdest.html` is a bonus ranking of CLIP outliers).

## Top-5 hidden twins

Each row is one pair. Image URLs point straight at the Met's public
CRDImages CDN; the Met collection pages are linked off the object IDs.

| # | Cosine | Gap | Object A (`/search/ID`) | Object B (`/search/ID`) |
|:---:|---:|:---:|:---|:---|
| 1 | **0.932** | **49 centuries** | 186597 — Knife and fork case, British 19c | 244170 — Cypriot dagger blade, ca. 3000 BCE |
| 14 | **0.956** | 45 centuries | 244387 — Cypriot bronze pin, ca. 2500 BCE | 7106 — Tiffany Studios soldering bar, 1900 |
| 21 | **0.949** | 43 centuries | 49139 — Chinese Neolithic Hu vase, ca. 2400 BCE | 4575 — American pitcher, 1814 |
| 6 | 0.933 | 47 centuries | 552016 — Egyptian jar, ca. 2500 BCE | 487245 — Danish Kähler vase, 1922 |
| 30 | **0.959** | 41 centuries | 501743 — British Natural Horn, 1790 | 324455 — Hattian sword/dagger, ca. 2300 BCE |

The full gallery of 30 pairs (with images inline) lives at
`index.html`. A standalone plain version is at `met_weirdest_out/twins.html`.

## Why these pairs exist

CLIP is a general-purpose vision model trained on random internet images.
It doesn't know that pair #10 is a sacred Banshan funerary jar and a
19th-century Philadelphia pitcher — it sees two round things with handles
photographed from the same angle. Two-thirds of the top 30 are essentially
"round vessel meets round vessel."

Also relevant: the Met photographs every artifact against a neutral gray
ground with soft overhead lighting. That uniform staging is part of what
the model is keying on. This isn't a bug; it's an honest feature of the
dataset, and it's part of the reason the demo works as cleanly as it does.

The point of the demo isn't that these artifacts are stylistically related —
they're not. The point is that the visual nearness exists *despite* total
historical unrelatedness, and that a cluster built purely from pixels,
with no metadata, can surface it.

## Why the Met Open Access

- It's the largest CC0, ethically-unambiguous image corpus in art history —
  ~470 K artworks, all cleared for public use by the museum.
- Every single artwork has a deterministic CDN image URL at
  `https://images.metmuseum.org/CRDImages/{dept}/web-large/{filename}`, so
  we don't need the Met API (which rate-limits) — just a parquet of the
  CSV-derived filenames, committed once, re-fetched in parallel.
- The department diversity (arms & armor, European paintings, Egyptian,
  Asian, photography, costume, prints, musical instruments, ...) means the
  twins question actually has interesting answers.

**Limitations the demo does not hide:**

1. Of the ~470 K Open Access artworks, only ~260 K actually have a
   CRDImages `web-large` rendition available at any given time (the rest
   are in-progress digitization or non-image works). We embed ~192 K after
   CDN 404s and size-threshold filtering (min 1 KB, max 16 MB).
2. The Met CDN's WAF will 403-storm a single IP after ~20 K sustained image
   pulls. We work around this with exponential backoff *and* by keeping
   concurrency at 4–8 workers with 8–16 HTTP threads each — a single
   laptop's IP would be blocked within the first hour.
3. CLIP ViT-B/32 is *very good* at "looks like" but *not good* at "is
   stylistically from the same school." The top twins are correctly
   "visually identical"; they are not "art-historically related." That's
   the interesting part.

## Data source

- Met Open Access CSV: `https://github.com/metmuseum/openaccess` (470 K rows,
  updated weekly). Field: `Object ID`, `Title`, `Department`,
  `Object Begin Date`, etc.
- Community mirror with CDN filename: `met-openaccess-images.csv` (from the
  `met-community-data` repo) — this gives us the exact `CRDImages` path
  per `Object ID`, which is what skips the rate-limited Met API.
- Images: `https://images.metmuseum.org/CRDImages/{dept}/web-large/*` —
  public CDN, no authentication.

## How it works

Three stages, one script:

- **Stage 0 — `discover_objects`** (1 worker, 8 CPU, 32 GB RAM). Downloads
  the two CSVs (master Open Access + CDN-filename mirror), joins on
  `Object ID`, filters to artworks with a published web-large image, and
  writes `objects.parquet` — one row per artwork to embed.
- **Map — `fetch_and_embed`** (4–8 workers × 1 CPU × 4 GB,
  `HTTP_THREADS=8–16` inside each). Each worker takes a 500-id batch, fans
  out concurrent HTTP GETs to the CDN with exponential backoff on
  `403/429/503/504`, runs fastembed's ONNX CLIP vision model on every
  successfully-fetched image, L2-normalizes, and writes a 512-d vector
  shard.
- **Reduce — `reduce_met`** (1 worker, 16 CPU, 64 GB). Loads every vector
  shard, builds a FAISS IVF cosine index over all 192 K vectors, and does a
  targeted cross-century similarity search for the twins.

## How to run

```bash
# Full Burla run (~45-60 min wall-clock; most of that is CDN-polite fetching)
python met_weirdest.py

# Skip the fetch+embed (reuse existing vector shards on Burla shared disk)
REDUCE_ONLY=1 python met_weirdest.py

# Cap the corpus for a fast dry-run (embed 5K images instead of 192K)
MET_MAX_OBJECTS=5000 python met_weirdest.py
```

## Files

```
index.html                 single-page shareable gallery (all 30 pairs inline)
met_weirdest.py            discover + fetch + embed + reduce in one script
met_weirdest_out/          artifacts from the latest run (parquet, JSON, HTML)
requirements.txt           burla + fastembed + Pillow + faiss-cpu + ...
```

---

*Source: Met Museum Open Access (CC0) + CRDImages CDN · Embeddings: CLIP ViT-B/32 via fastembed · Index: FAISS IVF · Orchestration: Burla `remote_parallel_map`.*
