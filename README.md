# World Photo Index — a Burla demo

**Live site: <https://burla-cloud.github.io/world-photo-index/>**

We ran **9,487,758 geotagged public Flickr photos** through a Burla cluster
of **967 parallel workers**, reverse-geocoded every one of them, tokenized
every tag / title / description, and asked the map one question:
**what does each country actually photograph?**

No captions from a model, no curator metadata. The answers come from what
9.49M real humans typed as tags.

## The headline

> Singapore photographs the **beach** (92% of its geotagged photos match
> water + coast vocabulary). South Korea is basically **Seoul** (69% of the
> country's photos come from one city). Cambodia is basically **Angkor Wat**
> (59%). Kazakhstan owns the word **expedition** worldwide (84% of
> "expedition"-tagged photos are Kazakhstani mountaineering).

|  |  |
|---|---:|
| Public photos processed | **9,487,758** |
| Countries represented | **246** |
| Cities represented | **53,198** |
| Admin-1 regions with a "signature tag" | **2,975** |
| Source | `dalle-mini/YFCC100M_OpenAI_subset` on HuggingFace |
| Reverse-geocoded locally on each worker | `reverse_geocoder` |
| Peak concurrent Burla workers | **967** |
| Total wall-clock (extract + tokenize + reduce + analyze) | **~8 minutes** |
| LLMs used | **zero** |

Every finding in the UI is a real lat/lon from a real Flickr photo whose
user-provided tags put it on the map.

## What's in this repo

- **`index.html` + `css/` + `js/`** — the shareable site. Interactive D3
  choropleth of 246 countries, a drawer for every country showing its top
  tags / top cities / per-capita rank / theme vocabulary, and nine
  one-click findings cards.
- **`FINDINGS.md`** — the full writeup of the nine findings, with citations.
- **`data/`** — the output artifacts consumed by the frontend
  (`world.json`, `findings.json`, `countries/*.json`, 220 country detail
  files).
- **`probe.py`** — quick HF + reverse-geocoder verification script.
- **`pipeline.py`** — one Burla worker: downloads one HF shard, reverse-
  geocodes all lat/lon, writes a JSONL row per photo to
  `/workspace/shared/wpi/shards/`.
- **`aggregate.py`** — reads each worker's shard, tokenizes
  tags/titles/descriptions, writes per-shard aggregate JSONs.
- **`reduce.py`** — 64 parallel reducers merge 4,094 shard aggregates into
  one `wpi_reduced.json` (and `wpi_reduced_v2.json` with region-level
  rollups).
- **`analysis.py`** — local: TF-IDF + theme-vocabulary scoring, writes every
  JSON the frontend needs, including the nine findings.
- **`smoke.py` / `warmup.py` / `scale.py`** — helpers for cluster warm-up
  and mass dispatch.

## Top-9 findings (shortlist)

1. **What every country photographs** — United States: *art*. UK: *music*.
   France: *concert*. Italy: *architecture*. Egypt: *temple*. Japan:
   *shrine*. Mexico: *ruins*. Norway: *beer*. Thailand: *Buddhism*.
2. **Things only one country photographs** — Panama owns "rodents / agouti"
   (100% — Smithsonian Barro Colorado camera traps). Belgium owns "Kmeron"
   (95% — one concert photographer is Belgium's Flickr identity).
3. **Cities whose entire camera roll is one thing** — Onagawa, Japan is 31%
   "earthquake" (2011 tsunami). Shiyan, China is 31% "LED" (the factory
   monoculture made visible). Citrus Park, FL is 48% "Big Cat Rescue."
4. **What Earth photographs most** — nature, art, music, concert,
   architecture, museum, festival, car, food, flowers. In that order.
5. **Regional signatures** — we computed a distinctive phrase for 2,975
   admin-1 regions (states, provinces, prefectures).
6. **Theme signatures by country** — Singapore is 92% beach. Iceland is
   26% nature (glaciers). Czech Republic is 29% architecture.
7. **Photos per capita** — Vatican City: 14.85M per million residents.
   Iceland: 45,530. UK: 22,432. Small tourist destinations dominate.
8. **One-city countries** — 100% of Singapore's photos come from
   Singapore city. 69% of South Korea's come from Seoul. 59% of Cambodia's
   come from Siem Reap (Angkor Wat).
9. **Small countries owning global vocabulary** — Kazakhstan owns
   "expedition" (84%). Belgium owns "world war" (83%, WWI battlefields).
   Portugal owns "biodiversity" (83%).

Full writeup with numbers: **[`FINDINGS.md`](./FINDINGS.md)**.

## Reproduce

```bash
# One-time setup (installs burla, authenticates the cluster)
curl -fsSL https://raw.githubusercontent.com/Burla-Cloud/burla-agent-starter-kit/main/install.sh | sh

# From this folder:
pip install -r requirements.txt

# Warm the cluster
python warmup.py

# Run the Burla pipeline (967 workers, ~8 min)
python scale.py

# Reduce + analyze locally
python reduce.py
python analysis.py

# Serve
python -m http.server 8765
# open http://localhost:8765
```

## Why YFCC100M

- The `dalle-mini/YFCC100M_OpenAI_subset` is a CC-licensed, 15M-row subset
  of the Yahoo Flickr Creative Commons 100M corpus, curated for the
  original DALL-E paper. 63% of its rows have usable lat/lon — that's
  9.49M globally-distributed geotagged photos, each with user-provided
  tags, title, and description.
- No API calls. No rate limits. No LLM. The only "model" in this pipeline
  is TF-IDF and a theme-keyword classifier.

## Caveats

- **This is a 2014-era Flickr snapshot.** It reflects what *Flickr's
  CC-licensed photographer base* photographed — richer in Western Europe,
  camera-owning demographics, concert / art / architecture subjects. A
  2024 Instagram or Google Photos dump would re-shuffle the leaderboards
  (and would not be CC-licensed).
- **Per-capita rankings are sensitive to small populations.** Vatican City
  tops the list with ~11.9k photos against a population of ~800. We show
  the number because it's honest; the zoom-out is the finding.
- **Tokenization is intentional.** We lowercase, strip punctuation,
  tokenize on non-word boundaries. We do not stem. "Shrine" and "shrines"
  are different tokens. This makes the signatures category-sensitive
  rather than concept-sensitive by design.

Part of the Burla demo collection. Source: [`Burla-Cloud/burla-agent-starter-kit`](https://github.com/Burla-Cloud/burla-agent-starter-kit).
