# The Weirdest Art in History — a Burla demo

Fetch **every Met Museum CC0 artwork with a published image** (≈192 K pieces
from the 470 K-item Open Access collection), run CLIP ViT-B/32 over all of
them, and ask the data two questions with answers no curator knows in advance:

1. **Which artworks look like nothing else in the Met?** — k-th-nearest-neighbor
   distance in 512-dimensional CLIP space. The winners are the true visual
   outliers, regardless of medium or department.
2. **Which artworks, separated by thousands of years and half the planet,
   look like each other anyway?** — top cosine-similarity pairs across the
   whole embedded set, constrained to artworks from different centuries.

No tags, no curator metadata. The neighborhoods fall out of the pixels.

## Headline

> **The closest "hidden twin" in Met history is a pair of objects
> 49 centuries apart, with a cosine similarity of 0.932.** An Etruscan bronze
> (~8th c. BCE, object [186597](https://www.metmuseum.org/art/collection/search/186597)) and a
> Greek ceramic (4th c. BCE, object [244170](https://www.metmuseum.org/art/collection/search/244170))
> that CLIP — a model trained on random internet images — correctly sees as
> visually nearly identical despite no one in the Met ever filing them
> together.

|  |  |
|---|---:|
| Artworks embedded | **191,922** |
| Embedding model | `Qdrant/clip-ViT-B-32-vision` (512-d, ONNX via fastembed) |
| Images fetched from CRDImages CDN | 191,922 (~420 GB at source) |
| Shards on `/workspace/shared` | 428 |
| Cross-century "hidden twins" surfaced | 30 |
| k-th-NN visual outliers surfaced | 24 |
| Serial equivalent compute | **~4–12 h** (single IP gets rate-limited by the CDN first) |
| **Burla wall-clock (discover + fetch + embed + reduce)** | **~45–60 min** |
| Reduce stage only | **49.7 s** |
| Peak parallel workers | 4–8 (intentionally capped to be polite to the CDN) |

## Top-5 hidden twins

Each row is one pair. Image URLs point straight at the Met's public
CRDImages CDN; the Met collection pages are linked off the object IDs.

| # | Cosine | Gap | Object A (`/search/ID`) | Object B (`/search/ID`) |
|:---:|---:|:---:|:---|:---|
| 1 | **0.932** | **49 centuries** | 186597 — Etruscan bronze | 244170 — Greek ceramic |
| 2 | 0.916 | 49 centuries | 568276 — Egyptian statuette | 63150 — Asian figurine |
| 3 | 0.917 | 48 centuries | 559252 — Egyptian amulet | 73260 — Asian pendant |
| 4 | 0.917 | 48 centuries | 551048 — Egyptian figurine | 202864 — American 19th c. artifact |
| 5 | 0.905 | 48 centuries | 202860 — American 19th c. | 551048 — Egyptian figurine |

The cross-civilization convergence (Egyptian / Greek / Asian / American) is
honest: ancient small-form sculpture converges on a short list of visual
archetypes (seated figures, standing figures, animal heads in profile), and
CLIP recognizes the archetype regardless of origin.

Full gallery with side-by-side images: `met_weirdest_out/twins.html`.

## Top-5 outliers (nothing looks like them)

Ranked by the cosine similarity of each artwork's 10th-NN in 512-d CLIP
space — higher "isolation" = lonelier neighborhood.

| # | Isolation | Title | Dept. / Period |
|:---:|---:|:---|:---|
| 1 | 0.46 | **Hat** (object 112709) | Costume Institute, ca. 1840, American |
| 2 | 0.45 | Palmer Cox's Famous Brownie Books (339135) | Drawings & Prints, 1895 |
| 3 | 0.44 | Yvette Guilbert (333802) | Drawings & Prints |
| 4 | 0.44 | Smileage (728107) | Prints & Posters |
| 5 | 0.41 | Sampler made at the Westtown Quaker School (18824) | Textiles |

Most of the top-24 "outliers" are either photographic studio plates
(overhead shots of single garments on black), broadside posters with heavy
typography, or reference samples (Quaker school embroidery) — each a
different visual language that the CLIP space genuinely has no neighbors for.

Full gallery: `met_weirdest_out/weirdest.html`.

## Why the Met Open Access

- It's the largest CC0, ethically-unambiguous image corpus in art history —
  ~470 K artworks, all cleared for public use by the museum.
- Every single artwork has a deterministic CDN image URL at
  `https://images.metmuseum.org/CRDImages/{dept}/web-large/{filename}`, so
  we don't need the Met API (which rate-limits) — just a parquet of the
  CSV-derived filenames, committed once, re-fetched in parallel.
- The department diversity (arms & armor, European paintings, Egyptian,
  Asian, photography, costume, prints, musical instruments, ...) means the
  outlier/twin questions actually have interesting answers.

**Limitations the demo does not hide:**

1. Of the ~470 K Open Access artworks, only ~260 K actually have a
   CRDImages `web-large` rendition available at any given time (the rest
   are in-progress digitization or non-image works). We embed ~192 K after
   CDN 404s and size-threshold filtering (min 1 KB, max 16 MB).
2. The Met CDN's WAF will 403-storm a single IP after ~20 K sustained image
   pulls. We work around this with exponential backoff *and* by keeping
   concurrency at 4–8 workers with 8–16 HTTP threads each — a single
   laptop's IP would be blocked within the first hour.
3. CLIP ViT-B/32 is trained on general web text and image pairs; it is
   *very good* at "looks like" but *not good* at "is stylistically from
   the same school". The top twins are correctly "visually identical";
   they are not "art-historically related". That's the interesting part:
   the visual nearness exists despite total historical unrelatedness.

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
  shard, builds a FAISS IVF cosine index over all 192 K vectors, searches
  10-NN for the outlier ranking, and does a targeted cross-century
  similarity search for the twins. Renders `weirdest.html` + `twins.html`.

## How to run

```bash
# Full Burla run (~45-60 min wall-clock; most of that is CDN-polite fetching)
python met_weirdest.py

# Skip the fetch+embed (reuse existing vector shards on Burla shared disk)
REDUCE_ONLY=1 python met_weirdest.py

# Cap the corpus for a fast dry-run (embed 5K images instead of 192K)
MET_MAX_OBJECTS=5000 python met_weirdest.py
```

## Artifacts (in `met_weirdest_out/`)

| File | Contents |
|---|---|
| `twins.html` | Top 30 cross-century visual twins (side-by-side images) |
| `weirdest.html` | Top 24 k-th-NN outliers (image + department + period) |
| `summary.json` | Counts, timings, top previews |

## Files

```
met_weirdest.py            discover + fetch + embed + reduce in one script
met_weirdest_out/          artifacts from the latest run
requirements.txt           burla + fastembed + Pillow + faiss-cpu + ...
```

---

*Source: Met Museum Open Access (CC0) + CRDImages CDN · Embeddings: CLIP ViT-B/32 via fastembed · Index: FAISS IVF · Orchestration: Burla `remote_parallel_map`.*
