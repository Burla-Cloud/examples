# Airbnb x Burla -- viral summary

(All numbers below are computed from data/outputs/*.json. Regenerated every run.)

## Headline numbers

- 1,097,241 Airbnb listings worldwide (Inside Airbnb, latest snapshot per city)
- 1,406,718 photo URLs scraped from public listing pages
- 1,243,339 images CLIP-scored on Burla CPU
- 48,122 images run through YOLOv8 on Burla A100s
- 50,686,612 reviews heuristic-scored, top 100 sent through Claude

## What we found

### TVs in places no one should mount a TV

Top-50 listings where YOLO confirmed a TV in the upper half of the photo
and CLIP rated the image high on "TV mounted above a fireplace."

### Messiest photos a host actually posted

Top-50 listings, ranked by CLIP score against "a messy cluttered room
with stuff everywhere."

### Mirror selfies

Top-24 listings where the host got caught reflected in their own
mirror photo (CLIP score against "a photographer reflected in a mirror").

### Plant-maximalist Airbnbs

Top-30 listings combining CLIP "room full of houseplants" with YOLO
potted plant counts.

### Cleaning fees > nightly rate

Top-100 listings where the cleaning fee exceeds the nightly price. The
worst offenders charge 0.0x the nightly rate as a cleaning fee.

### The funniest reviews

Top-100 reviews surfaced by 3-tier funnel (heuristic -> embedding cluster
-> Claude humor score).

## What held up under bootstrap

- brightness_quartile
- tv_too_high

## What did not survive

- cleaning_fee_ratio_bucket
- messiness_quartile
- plant_count_bucket

## Replication

Repo: airbnb-burla
Runtime: 10.9 hours wall time, peak 1000 Burla workers.
