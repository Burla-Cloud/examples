# Amazon Review Distiller — Findings

What happens when you stream 275 GB of raw Amazon reviews across 34 categories through a Burla cluster with 500+ workers in parallel and rank every single review on profanity, screaming, punctuation, length, five-star-vs-rage mismatch, and sheer one-line brutality?

This file. These are the findings.

## Scale

| Metric | Value |
| --- | --- |
| Source | `McAuley-Lab/Amazon-Reviews-2023` (HuggingFace, raw JSONL) |
| Total reviews parsed | **571,544,386** |
| Total bytes streamed | **275 GB** |
| Categories | 34 |
| Reviews flagged profane | **20,187,204** (3.53% global rate) |
| Pipeline wall time | 3.21 min map + 9.2 s reduce |
| Peak concurrent CPUs | **500+** (single Burla run) |
| Chunks dispatched | 545 byte-range chunks |

Nothing was downloaded. Every worker used HTTP Range requests to stream its own slice directly from the HuggingFace CDN.

## The nine findings

### F1 — The filthiest categories, ranked

Video Games blows every other category out of the water. 6.54% of all video game reviews contain at least one strong profanity hit. Luxury consumption (Gift Cards, Handmade) is almost profanity-free.

| # | Category | Profanity rate |
| - | --- | --- |
| 1 | Video Games | **6.54%** |
| 2 | Movies & TV | 5.93% |
| 3 | CDs & Vinyl | 5.66% |
| 4 | Subscription Boxes | 5.41% |
| 5 | Kindle Store | 5.41% |
| … | … | … |
| 33 | Gift Cards | 1.19% |
| 34 | Handmade Products | 1.08% |

Video games, movies, music and books sit on top — cultural products, not consumer goods. People bring feelings to culture; they bring utility to a hex wrench set.

### F2 — The loudest reviewers on Amazon

Ranked by all-caps ratio × sqrt(length). This surfaces reviews that don't just shout once — they sustain a full paragraph of shouting. The top of the list is dominated by a 1,169-word ALL-CAPS CD liner note from a self-described "100% disabled decorated Vietnam veteran and Mozart scholar." He apologizes in the first line for using caps due to macular degeneration, then uses them for 1,169 more words.

### F3 — Punctuation bombs

Reviews ranked by the longest single run of consecutive exclamation marks. The winner is a two-word review of a baby product:

> **"love these"** … followed by **10,594** exclamation marks.

10,594. That is not a typo. One human pressed one key that many times to compliment a baby product.

### F4 — Reviews too brutal for two sentences

Under 35 words, full of profanity, scored for severity and variety. The Wall of Fucked Up hero section is mostly drawn from this bucket. These are the haikus of Amazon despair — people who couldn't even be bothered to type a second sentence but needed to deploy three different curse words first.

### F5 — Rant hall of fame

Score = length × profanity × caps × exclamation. The top of the list is a 2,000-word monologue about an oil-covered motor unit that cascades into a treatise on Amazon returns policy, FootSmart, false invoices, and the state of e-commerce. The purest artisanal Karen energy Amazon has ever stored in its database.

### F6 — Five stars, completely unhinged

5★ reviews that are also full of strong profanity — the "this product fucking slaps" genre. Our favorite is a 243-word rant about a reality-TV series that gave it five stars, called the cast "a bitch," "a moron," "a jackass," and described the reviewer's own life as "sucks" and wanting "to be a rh" — all while recommending it highly. Amazon's own recommendation engine counts this as a positive signal.

### F7 — Five stars, zero words

40 reviewers gave a product five stars and wrote exactly zero or one word. This is the bleakest genre of human text: the highest possible rating, paired with complete silence. One reviewer of a cherry cough drop wrote the single word **"Taste."** and gave it five stars. What more needed to be said?

### F8 — Which categories get the most 1-star rage

Ranked by share of 1-star ratings.

| # | Category | % 1-star |
| - | --- | --- |
| 1 | Subscription Boxes | **15.89%** |
| 2 | All Beauty | 14.55% |
| 3 | Software | 14.26% |
| 4 | Health & Personal Care | 14.08% |
| 5 | Patio, Lawn & Garden | 13.84% |

Subscription boxes are the single angriest category on Amazon — almost 1 in 6 reviews is a 1-star. Turns out charging people monthly for a curated surprise generates a lot of regret.

### F9 — Who types the longest reviews?

| # | Category | Avg characters |
| - | --- | --- |
| 1 | CDs & Vinyl | **428** |
| 2 | Books | 423 |
| 3 | Kindle Store | 367 |
| 4 | Digital Music | 340 |
| 5 | Video Games | 308 |

Book readers and music collectors write novels back at each other. Gift card buyers type nothing. Culture → words, utility → silence.

## The Wall of Fucked Up

The 120 most unhinged reviews overall, pooled from the top profane-strong, rant, and short-brutal candidates, deduplicated, and reranked by a variety-weighted severity score that penalizes single-word-repeat spam (no more "crap crap crap crap" dominating the top).

The most represented categories on the Wall:

| Rank | Category | Wall entries |
| - | --- | - |
| 1 | Unknown (unlabeled) | 16 |
| 2 | Movies & TV | 10 |
| 3 | CDs & Vinyl | 9 |
| 3 | Automotive | 9 |
| 5 | Electronics | 8 |
| 5 | Home & Kitchen | 8 |

Movies, music, and cars — the things people feel strongly about — dominate, as you'd expect.

## Reproducing this

```bash
cd agents/amazon-review-distiller
/burla                    # authenticate once
burla run scale.py        # ~3 min at 500+ workers
burla run reduce.py       # ~10 s
python analysis.py        # local, builds frontend/data/*
cd frontend && python -m http.server 8765
```

## Caveats

- **No LLM sanitization.** All text is verbatim from Amazon.
- **Rule-based scoring.** Word lists for strong/medium/mild profanity, caps ratio, exclamation runs. No sentiment model. That's the point — we wanted raw, reproducible, human-auditable signal.
- **Proper-noun filtering.** The Wall rescorer filters capitalized words, so "Dick Tracy" and similar titles don't inflate the profanity count.
- **Spam filter.** Reviews where a single token is > 45% of the text, or a 15–60 char substring repeats 4+ times, are dropped from Wall candidates.
- **2023 snapshot.** This is the McAuley Lab release; it does not include reviews posted after mid-2023.

## Source

Open source at `agents/amazon-review-distiller/`. Pipeline: `pipeline.py`, `scale.py`, `reduce.py`. Analysis: `analysis.py`. UI: `frontend/`.
