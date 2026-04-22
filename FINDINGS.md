# World Photo Index — Findings

**9,487,758** public geotagged photographs. **246** countries and territories. **53,198** cities. Processed in under 8 minutes of wall-clock time on a **967-worker** Burla cluster.

## The pipeline

| stage | what | time | peak workers |
|---|---|---|---|
| extract | download HF shards, geocode all lat/lon | 118 s | 967 |
| tokenize | extract phrases + tokens per photo | 107 s | 640 |
| reduce | 4,094 agg files → single wpi_reduced_v2.json | 230 s | 64 |
| analysis | compute TF-IDF + 9 findings, write per-country detail | 3 s | local |

## The nine findings

### 1. The most photographed _thing_ in every country

After filtering out cities, regions, and country-name aliases, the #1 non-place tag tells us what's actually in the frame:

| country | thing | photos |
|---|---|---|
| 🇺🇸 United States | art | 99,607 |
| 🇬🇧 United Kingdom | music | 46,004 |
| 🇨🇦 Canada | nature | 9,250 |
| 🇫🇷 France | concert | 5,215 |
| 🇩🇪 Germany | art | 5,716 |
| 🇮🇹 Italy | architecture | 3,899 |
| 🇪🇬 Egypt | temple | 3,541 |
| 🇮🇳 India | nature | 2,912 |
| 🇯🇵 Japan | shrine | 3,102 |
| 🇲🇽 Mexico | ruins (mayan) | 2,562 |
| 🇳🇴 Norway | beer | 1,217 |
| 🇹🇭 Thailand | buddhism / buddha | 1,114 |
| 🇿🇦 South Africa | world cup / fifa | 4,017 |

**The zoom-out:** Western Europe photographs culture (art, music, architecture). North Africa & SE Asia photographs religion. Oceania photographs landscapes.

### 2. Things only one country photographs

Phrases that appear ≥1,500 times worldwide but ≥85% of them come from a single country:

- 🇵🇦 **Panama — rodents rabbits / dasyprocta punctata / central american agouti** (100%) — Smithsonian's Barro Colorado Island (BCI) camera-trap research program.
- 🇧🇪 **Belgium — kmeron / vincent philbert** (95%) — one concert photographer's catalog is so large he _is_ Belgium's Flickr identity.
- 🇺🇸 **United States — baseball, brooklyn, atlantic yards, barclays center arena** — one photographer's documentation of the Atlantic Yards construction project is visible at the national level.

### 3. Cities whose entire camera roll is one thing

Cities with 1,500+ tagged photos where a single phrase accounts for ≥10% of everything tagged there:

- 🇯🇵 **Onagawa Cho, Miyagi — earthquake** (31%) — the 2011 tsunami.
- 🇰🇪 **Nanyuki, Laikipia — mpala** (28%) — Mpala Research Centre, Princeton's Kenyan field station.
- 🇨🇳 **Shiyan, Guangdong — LED** (31%) — the LED-factory monoculture made visible.
- 🇬🇧 **Appley Bridge / Knutsford — diving** (~28%) — local diving schools.
- 🇺🇸 **Citrus Park, Florida — big cat rescue** (48%) — _the_ Big Cat Rescue sanctuary.
- 🇩🇪 **Rust, Baden-Wuerttemberg — europa park** (48%) — largest theme park in Germany.

### 4. What Earth photographs most

The raw top tag (place names filtered out) across all 9.49M photos:

`nature` · `art` · `music` · `concert` · `architecture` · `museum` · `festival` · `car` · `food` · `flowers`

### 5. Regional signatures (2,975 admin-1 regions)

For every state / province / prefecture with 2,000+ tagged photos, the distinctive phrase. Examples:

- 🇺🇸 Michigan — **baseball**
- 🇺🇸 Alaska — **aurora / wildlife**
- 🇯🇵 Tokyo — **shrine / setagaya-ku**
- 🇬🇧 England — **music / dorset / music festival**
- 🇨🇦 British Columbia — **vancouver / skiing**

### 6. Every country's photographic obsession, by theme

Seven themed vocabularies (food, beach, religion, transport, architecture, nature, wildlife). Measured as the share of each country's total photos that match the theme:

- 🇸🇬 **Singapore — 92% beach** (highest in the world)
- 🇲🇾 Malaysia — 70% beach
- 🇨🇿 Czech Republic — 29% architecture
- 🇷🇴 Romania — 34% nature
- 🇮🇸 Iceland — 26% nature (glaciers, waterfalls, ice)
- 🇵🇭 Philippines — 29% beach

### 7. Photographed-per-capita ranking

Public geotagged photos per million residents:

| country | photos / M |
|---|---|
| 🇻🇦 Vatican City | 14,850,000 |
| 🇮🇸 Iceland | 45,530 |
| 🇬🇧 United Kingdom | 22,432 |
| 🇵🇦 Panama | 18,909 |
| 🇮🇪 Ireland | 17,053 |
| 🇳🇿 New Zealand | 16,864 |

**The zoom-out:** small-population tourist destinations dominate. Vatican City is a rounding error statistically (population 800, photos 11,880) — but the cultural read is real.

### 8. Countries whose whole country is one city

What share of a nation's geotagged photos come from its single top city?

| country | top city | share |
|---|---|---|
| 🇸🇬 Singapore | Singapore | 100% |
| 🇻🇦 Vatican City | Vatican City | 100% |
| 🇰🇷 South Korea | Seoul | 69% |
| 🇰🇭 Cambodia | Siem Reap | 59% (Angkor Wat is a monoculture) |
| 🇵🇦 Panama | Nueva Providencia (BCI) | 60% |
| 🇲🇾 Malaysia | Kampung Pasir Gudang Baru | 51% |
| 🇧🇪 Belgium | Brussels | 44% |
| 🇫🇮 Finland | Helsinki | 32% |
| 🇲🇦 Morocco | Marrakesh | 32% |
| 🇩🇰 Denmark | Copenhagen | 29% |

Low numbers = distributed. High numbers = single-landmark tourism.

### 9. Small countries that punch above their weight on one concept

Excluding the Big-4 photo producers (US, UK, Canada, Australia), these are global-vocabulary phrases where smaller nations own 15–84% of the frame:

- 🇰🇿 **Kazakhstan — expedition** (84%) — mountaineering capital.
- 🇦🇹 **Austria — aesthetic / presentation / precious** (82-84%)
- 🇧🇪 **Belgium — world war** (83%) — WWI battlefields.
- 🇵🇹 **Portugal — biodiversity** (83%)
- 🇸🇬 **Singapore — cnidaria** (81%) — Wild Singapore jellyfish photography.
- 🇮🇪 **Ireland — jogging** (80%)
- 🇯🇴 **Jordan — ancient history / aerial archaeology** (77-79%)

## Data & code

- Data: `dalle-mini/YFCC100M_OpenAI_subset` on HuggingFace (4,094 shards, 15M rows, 63% geotagged)
- Pipeline: `pipeline.py` (one shard per Burla worker, reverse-geocodes via `reverse_geocoder`, writes one JSONL row per photo to `/workspace/shared/wpi/shards/`)
- Aggregate: `aggregate.py` (tokenizes per shard, writes one agg JSON per shard)
- Reduce: `reduce.py` (64 parallel reducers, merged locally to `wpi_reduced_v2.json`)
- Analysis: `analysis.py` (TF-IDF + theme scoring + findings)
- Frontend: `frontend/index.html` with D3 + Natural Earth topojson choropleth

Code lives at [`agents/world-photo-index/`](.) in the burla-agent-starter-kit repo.
