# Amazon Review Distiller. a Burla demo

**Live site: <https://burla-cloud.github.io/amazon-review-distiller/>**

We streamed the entire public `McAuley-Lab/Amazon-Reviews-2023` corpus on
HuggingFace, **571 million Amazon reviews, 275 GB of raw JSONL**, through a
Burla cluster of **500+ parallel CPUs**, scored every review on profanity /
caps / rants / censored-slur hits, and built two walls:

- **Wall of Rants** (default). the 120 most unhinged profane reviews,
  re-ranked for variety.
- **Wall of Fucked Up** (Unhinged Mode). the worst-of-worst pass,
  including asterisk-censored strong profanity and categorized slurs.

No LLM sanitized anything. Every review is a real, verbatim string from a
real Amazon purchase.

## The headline

> The filthiest category on Amazon is **Video Games**. 6.54% of all
> video-game reviews contain at least one strong profanity. The loudest
> single review is **1,169 words of ALL CAPS** from a self-described "100%
> disabled decorated Vietnam veteran and Mozart scholar," who opens with an
> apology for the caps (macular degeneration) and then uses them for the
> rest of the paragraph. The longest single run of exclamation marks we
> found is **10,594 "!"s** in a two-word review of a baby product:
> *"love these"*.

|  |  |
|---|---:|
| Reviews parsed | **571,544,386** |
| Raw data streamed | **275 GB** (HTTP Range reads, no local download) |
| Categories | **34** |
| Reviews tagged profane | **20,187,204** (3.53% globally) |
| Source | `McAuley-Lab/Amazon-Reviews-2023` (HuggingFace) |
| Byte-range chunks dispatched | **545** |
| Peak concurrent Burla workers | **500+** |
| LLMs used | **zero** |

## Repo layout

```
lexicon.py     word lists + censored-variant regexes + context classifier
pipeline.py    Burla map/reduce worker + CLI dispatch
analysis.py    rescore, merge hard+worst corpora, emit data/*.json
probe.py       stream 4 MB of one category to sanity-check HF access

index.html     the Amazon-parody site
css/style.css  dark/light themes (flips in Unhinged Mode)
js/app.js      pure vanilla JS. loads data/*.json, renders, searches
data/*.json    frontend artifacts: wall, unhinged, search pools,
               categories, findings
```

## Reproduce

```bash
curl -fsSL https://raw.githubusercontent.com/Burla-Cloud/burla-agent-starter-kit/main/install.sh | sh
pip install -r requirements.txt

python pipeline.py probe         # streaming sanity check
python pipeline.py map-main      # main pass across the cluster
python pipeline.py map-worst     # worst-of-worst pass
python pipeline.py reduce-main   # merge main shards -> samples/ard_reduced.json
python pipeline.py reduce-worst  # merge worst shards -> samples/ard_worst.json
python analysis.py               # rescore + merge + write data/*.json

python -m http.server 8766       # browse http://localhost:8766
```

## The findings (shortlist)

See **[`FINDINGS.md`](./FINDINGS.md)** for the full writeup.

1. The filthiest categories ranked. Video Games 6.54%, Movies & TV 5.93%,
   CDs & Vinyl 5.66%, Kindle Store 5.41%. Gift Cards last at 1.19%.
2. The loudest reviewers on Amazon. A 1,169-word all-caps rant about a
   Mozart CD wins.
3. Punctuation bombs. *"love these"* × 10,594 exclamation marks.
4. Reviews too brutal for two sentences. The Wall of Rants hero section.
5. Rant hall of fame. A 2,000-word oil-covered motor-unit monologue that
   cascades into a critique of Amazon's return policy.
6. Five stars, zero words. The bleakest genre of human text.
7. Profanity diversity. Reviews using 5+ unique strong curse words.

## Why McAuley-Lab/Amazon-Reviews-2023

Largest public Amazon review dump. **571 million reviews** across 34
categories, released for academic use by the McAuley lab at UCSD. Served
as one `.jsonl.gz` per category from the HuggingFace CDN, which supports
HTTP Range requests. **No file is ever fully downloaded.** every worker
streams its own byte range.

## Caveats / content warning

- **The profanity is real.** We do not censor or rewrite anywhere on the
  Wall. Slurs in Unhinged Mode are rendered with a category badge and the
  middle characters blanked in the UI, but the underlying JSON ships the
  same raw strings the reviewer typed.
- **Profanity detection is rule-based**, not model-based. Reviews in
  languages other than English are scored by length / caps / exclamation
  only.
- **No LLM touches the text.** Every ranking, bucket, and finding in this
  repo is produced by regex, tokenization, and arithmetic.

Part of the Burla demo collection.
Source: [`Burla-Cloud/burla-agent-starter-kit`](https://github.com/Burla-Cloud/burla-agent-starter-kit).
