"""Transform ard_reduced.json into UI-ready artifacts + findings.

Outputs in frontend/data/:
  index.json            — summary stats
  wall.json             — Wall of Fucked Up (top global unhinged reviews)
  findings.json         — all other rollups
  categories.json       — per-category summary rows
  categories/{cat}.json — per-category deep dive
"""
from __future__ import annotations

import html
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

from pipeline import STRONG_PROFANE, MEDIUM_PROFANE, MILD_PROFANE

WORD_RX = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")


def _spammy(text: str) -> bool:
    """Reject reviews that are just one word/phrase repeated, or near-empty."""
    if not text:
        return True
    tokens = WORD_RX.findall(text.lower())
    if len(tokens) < 3:
        return False
    counts = Counter(tokens)
    most_common_word, most_common_count = counts.most_common(1)[0]
    if len(tokens) >= 20 and most_common_count / len(tokens) > 0.45:
        return True
    if most_common_count / len(tokens) > 0.70:
        return True
    # Phrase-level: any 15+ char substring that recurs 4+ times is spam.
    s = text[:600]
    for seed_len in (15, 20, 30, 40):
        for start in range(0, min(len(s) - seed_len, 200), 10):
            seed = s[start:start + seed_len]
            if seed.strip() and s.count(seed) >= 4:
                return True
    return False


def _rescore_profanity(text: str) -> Dict[str, Any]:
    """Re-score a review with context awareness.

    Returns:
      unique_strong: number of DISTINCT strong-profanity words (lowercase).
      unique_medium / unique_mild: same for weaker buckets.
      total_profane: sum of hits (lowercased only, filters proper-noun matches).
      severity: weighted sum — strong × 3 + medium × 2 + mild × 1.
      variety_score: severity × (1 + 0.5 * unique_strong). Rewards varied rants.
    """
    if not text:
        return {"severity": 0, "variety_score": 0, "unique_strong": 0}
    lo = text.lower()
    # Only count the lowercased instances to filter "Dick Tracy" proper nouns.
    # If the review is entirely uppercase, we lowercase it once and count all.
    all_caps = text.upper() == text
    strong_hits = Counter()
    medium_hits = Counter()
    mild_hits = Counter()
    # Scan as whole words
    for m in re.finditer(r"[A-Za-z]+(?:'[A-Za-z]+)?", text):
        raw = m.group(0)
        low = raw.lower()
        # Proper-noun filter: if it's the only capitalized word in its immediate
        # context and not all-caps, treat as a proper noun and skip.
        is_capitalized = raw[0].isupper() and not raw.isupper()
        if is_capitalized and not all_caps and low in STRONG_PROFANE and low in {
            "dick", "dicks", "ass", "cock", "cocks", "bastard", "bastards", "pussy",
        }:
            # e.g., "Dick Tracy", "Grim Bastards", "John Cock". Skip.
            continue
        if low in STRONG_PROFANE:
            strong_hits[low] += 1
        elif low in MEDIUM_PROFANE:
            medium_hits[low] += 1
        elif low in MILD_PROFANE:
            mild_hits[low] += 1

    total_strong = sum(strong_hits.values())
    total_medium = sum(medium_hits.values())
    total_mild = sum(mild_hits.values())
    unique_strong = len(strong_hits)
    severity = total_strong * 3 + total_medium * 2 + total_mild * 1
    variety_score = severity * (1 + 0.5 * unique_strong)
    return {
        "severity": severity,
        "variety_score": variety_score,
        "unique_strong": unique_strong,
        "total_strong": total_strong,
        "total_medium": total_medium,
        "total_mild": total_mild,
        "strong_words": dict(strong_hits),
    }


HERE = Path(__file__).parent
IN_PATH = HERE / "samples" / "ard_reduced.json"
OUT_DIR = HERE / "frontend" / "data"
OUT_CATS = OUT_DIR / "categories"

CAT_DISPLAY = {
    "All_Beauty": ("All Beauty", "💄"),
    "Amazon_Fashion": ("Amazon Fashion", "👗"),
    "Appliances": ("Appliances", "🍳"),
    "Arts_Crafts_and_Sewing": ("Arts, Crafts & Sewing", "🧵"),
    "Automotive": ("Automotive", "🚗"),
    "Baby_Products": ("Baby Products", "👶"),
    "Beauty_and_Personal_Care": ("Beauty & Personal Care", "💋"),
    "Books": ("Books", "📚"),
    "CDs_and_Vinyl": ("CDs & Vinyl", "💿"),
    "Cell_Phones_and_Accessories": ("Cell Phones & Accessories", "📱"),
    "Clothing_Shoes_and_Jewelry": ("Clothing, Shoes & Jewelry", "👕"),
    "Digital_Music": ("Digital Music", "🎵"),
    "Electronics": ("Electronics", "📺"),
    "Gift_Cards": ("Gift Cards", "🎁"),
    "Grocery_and_Gourmet_Food": ("Grocery & Gourmet Food", "🛒"),
    "Handmade_Products": ("Handmade Products", "🪡"),
    "Health_and_Household": ("Health & Household", "💊"),
    "Health_and_Personal_Care": ("Health & Personal Care", "🩹"),
    "Home_and_Kitchen": ("Home & Kitchen", "🍽️"),
    "Industrial_and_Scientific": ("Industrial & Scientific", "🔬"),
    "Kindle_Store": ("Kindle Store", "📖"),
    "Magazine_Subscriptions": ("Magazine Subscriptions", "📰"),
    "Movies_and_TV": ("Movies & TV", "🎬"),
    "Musical_Instruments": ("Musical Instruments", "🎸"),
    "Office_Products": ("Office Products", "📎"),
    "Patio_Lawn_and_Garden": ("Patio, Lawn & Garden", "🌱"),
    "Pet_Supplies": ("Pet Supplies", "🐾"),
    "Software": ("Software", "💾"),
    "Sports_and_Outdoors": ("Sports & Outdoors", "⚽"),
    "Subscription_Boxes": ("Subscription Boxes", "📦"),
    "Tools_and_Home_Improvement": ("Tools & Home Improvement", "🔧"),
    "Toys_and_Games": ("Toys & Games", "🧸"),
    "Unknown": ("Unknown", "❓"),
    "Video_Games": ("Video Games", "🎮"),
}


def display(cat: str) -> Dict[str, str]:
    name, emoji = CAT_DISPLAY.get(cat, (cat.replace("_", " "), "📦"))
    return {"cat": cat, "name": name, "emoji": emoji}


def clean_review(r: Dict[str, Any]) -> Dict[str, Any]:
    """Defensive copy; keep raw text but truncate to something UI-safe."""
    return {
        "text": (r.get("text") or "").strip(),
        "title": (r.get("title") or "").strip(),
        "rating": r.get("rating"),
        "asin": r.get("asin"),
        "helpful_vote": r.get("helpful_vote"),
        "verified": r.get("verified"),
        "category": r.get("category") or "",
        "score": r.get("score") or {},
        "ts": r.get("ts"),
    }


def flatten_top(d: Dict[str, Any], signal: str, filter_spam: bool = True) -> List[Dict[str, Any]]:
    rows = []
    seen_bodies = set()
    for cat, cat_data in d["categories"].items():
        for item in (cat_data.get("top", {}).get(signal, []) or []):
            rev = clean_review(item.get("review") or {})
            text = rev.get("text") or ""
            if filter_spam and _spammy(text):
                continue
            body_fp = re.sub(r"\s+", " ", text.lower())[:150]
            if body_fp in seen_bodies:
                continue
            seen_bodies.add(body_fp)
            rev["_score"] = item.get("score")
            rev["_category"] = cat
            rows.append(rev)
    rows.sort(key=lambda r: -r.get("_score", 0))
    return rows


def flatten_wall(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Rescore top profane candidates using context-aware scoring.
    Pulls from 'profane_strong' + 'rant' + 'short_brutal' across every category,
    dedupes, reranks by variety-weighted severity.
    """
    seen_bodies = set()
    rows = []
    for sig in ("profane_strong", "rant", "short_brutal"):
        for cat, cat_data in d["categories"].items():
            for item in (cat_data.get("top", {}).get(sig, []) or []):
                r = item.get("review") or {}
                text = (r.get("text") or "").strip()
                if not text:
                    continue
                if _spammy(text):
                    continue
                body_fp = re.sub(r"\s+", " ", text.lower())[:150]
                if body_fp in seen_bodies:
                    continue
                seen_bodies.add(body_fp)
                rescored = _rescore_profanity(text + " " + (r.get("title") or ""))
                if rescored["unique_strong"] < 2 and rescored["severity"] < 6:
                    continue
                rev = clean_review(r)
                rev["_category"] = cat
                rev["_score"] = rescored
                rev["_sort"] = rescored["variety_score"]
                rows.append(rev)
    rows.sort(key=lambda r: -r["_sort"])
    return rows


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_CATS.mkdir(parents=True, exist_ok=True)

    d = json.loads(IN_PATH.read_text())
    total_parsed = d["total_parsed"]
    total_profane = d["total_profane"]

    cats = d["categories"]

    # Category summary (used on homepage)
    cat_rows = []
    for cat, cd in cats.items():
        meta = display(cat)
        cat_rows.append({
            **meta,
            "n_parsed": cd["n_parsed"],
            "n_profane": cd["n_profane"],
            "profanity_rate": cd["profanity_rate"],
            "mean_length": cd["mean_length"],
            "rating_counts": cd["rating_counts"],
            "pct_1_star": round(cd["rating_counts"].get("1", 0) / max(cd["n_parsed"], 1), 4),
            "pct_5_star": round(cd["rating_counts"].get("5", 0) / max(cd["n_parsed"], 1), 4),
        })
    cat_rows.sort(key=lambda r: -r["profanity_rate"])

    (OUT_DIR / "categories.json").write_text(json.dumps(cat_rows))

    # --- WALL OF FUCKED UP ---------------------------------------------
    # Re-scored with context awareness + variety weighting.
    wall = flatten_wall(d)[:120]
    (OUT_DIR / "wall.json").write_text(json.dumps({
        "title": "The Wall of Fucked Up",
        "blurb": f"The {len(wall)} most unhinged reviews from "
                 f"{total_parsed:,} Amazon reviews across {len(cat_rows)} categories. "
                 "Reranked by profanity diversity, intensity, and rant length. "
                 "No sanitization — raw Amazon, as written.",
        "rows": wall,
    }))

    # --- FINDINGS -------------------------------------------------------
    findings = []

    # F1 Category profanity leaderboard
    findings.append({
        "id": "category_profanity",
        "title": "The filthiest Amazon categories, ranked",
        "blurb": "What share of each category's reviews contain at least one "
                 "profanity hit? Ranked by rate. Video games pulled ahead by a mile.",
        "rows": [
            {**r, "profanity_pct": round(r["profanity_rate"] * 100, 3)}
            for r in cat_rows[:34]
        ],
    })

    # F2 Screaming hall of fame
    screaming = flatten_top(d, "screaming")[:60]
    findings.append({
        "id": "screaming",
        "title": "The loudest reviewers on Amazon",
        "blurb": "Reviews ranked by all-caps word ratio × sqrt(length). "
                 "Longer screaming wins over shorter screaming. THE VOLUME IS REAL.",
        "rows": screaming,
    })

    # F3 Exclamation storms
    exclaim = flatten_top(d, "exclamation")[:60]
    findings.append({
        "id": "exclamation",
        "title": "Punctuation bombs",
        "blurb": "Reviews with the most consecutive exclamation marks. Not "
                 "the most enraged — just the most emotional.",
        "rows": exclaim,
    })

    # F4 Short-brutal — 4-30 words, real profanity density. Mine from
    # short_brutal + profane_strong since both catch terse reviews.
    short_all = (flatten_top(d, "short_brutal") + flatten_top(d, "profane_strong"))
    short = []
    seen_short = set()
    for r in short_all:
        text = r.get("text") or ""
        n_words = len(WORD_RX.findall(text))
        if n_words < 4 or n_words > 35:
            continue
        body_fp = re.sub(r"\s+", " ", text.lower())[:80]
        if body_fp in seen_short:
            continue
        seen_short.add(body_fp)
        rescored = _rescore_profanity(text + " " + (r.get("title") or ""))
        if rescored["severity"] >= 5:
            r["_score"] = rescored
            short.append(r)
        if len(short) >= 60:
            break
    findings.append({
        "id": "short_brutal",
        "title": "Reviews too brutal for two sentences",
        "blurb": "Under 35 words, full of profanity. Pure concentrated rage in a haiku.",
        "rows": short,
    })

    # F5 Rant champions
    rant = flatten_top(d, "rant")[:60]
    findings.append({
        "id": "rant",
        "title": "Rant hall of fame",
        "blurb": "Score = length + profanity + ALL-CAPS + exclamation marks. The "
                 "purest artisanal Karen energy Amazon has seen.",
        "rows": rant,
    })

    # F6 Five-star obscene — rescore to require real profanity intensity,
    # not just "damn/hell" in a Christian theology book.
    fso_all = flatten_top(d, "five_star_obscene", filter_spam=True)
    fso = []
    for r in fso_all:
        if (r.get("rating") or 0) < 5:
            continue
        rescored = _rescore_profanity((r.get("text") or "") + " " + (r.get("title") or ""))
        if rescored["total_strong"] >= 1 and rescored["unique_strong"] >= 1 and rescored["severity"] >= 4:
            r["_score"] = rescored
            fso.append(r)
        if len(fso) >= 50:
            break
    findings.append({
        "id": "five_star_obscene",
        "title": "Five stars but still completely unhinged",
        "blurb": "5★ reviews that are also full of profanity. The 'this "
                 "product fucking slaps' genre, which is a strictly positive review.",
        "rows": fso,
    })

    # F7 Five-star one-word
    fow = flatten_top(d, "five_star_one_word")[:40]
    findings.append({
        "id": "five_star_one_word",
        "title": "Five stars, zero words",
        "blurb": "Reviews that gave a product five stars and then wrote "
                 "nothing. The bleakest genre of human text. (Some actually "
                 "have zero characters. Others are one- or two-word wonders.)",
        "rows": fow,
    })

    # F8 Rating distribution per category
    rating_dist_rows = []
    for r in cat_rows:
        total = r["n_parsed"] or 1
        rc = r["rating_counts"]
        rating_dist_rows.append({
            **display(r["cat"]),
            "n_parsed": r["n_parsed"],
            "pct_1": round(100 * rc.get("1", 0) / total, 2),
            "pct_2": round(100 * rc.get("2", 0) / total, 2),
            "pct_3": round(100 * rc.get("3", 0) / total, 2),
            "pct_4": round(100 * rc.get("4", 0) / total, 2),
            "pct_5": round(100 * rc.get("5", 0) / total, 2),
        })
    rating_dist_rows.sort(key=lambda r: -r["pct_1"])
    findings.append({
        "id": "rating_distribution",
        "title": "Which categories get the most 1-star rage reviews",
        "blurb": "Share of 1-star to 5-star ratings, per category. The wider "
                 "the left tail, the angrier the customer base.",
        "rows": rating_dist_rows,
    })

    # F9 Words per review
    words_rows = sorted(
        [
            {**display(r["cat"]), "n_parsed": r["n_parsed"], "mean_length": r["mean_length"]}
            for r in cat_rows
        ],
        key=lambda r: -r["mean_length"],
    )
    findings.append({
        "id": "mean_length",
        "title": "Who writes the longest reviews?",
        "blurb": "Mean review length (characters). Book readers are typing novels "
                 "back at each other. Gift card buyers are typing nothing.",
        "rows": words_rows,
    })

    (OUT_DIR / "findings.json").write_text(json.dumps(findings))

    # --- Per-category detail pages -------------------------------------
    for cat, cd in cats.items():
        meta = display(cat)
        top = cd.get("top") or {}

        def pick(sig, k):
            return [
                {**clean_review(it["review"]), "_score": it["score"]}
                for it in (top.get(sig, []) or [])[:k]
            ]

        page = {
            **meta,
            "n_parsed": cd["n_parsed"],
            "n_profane": cd["n_profane"],
            "profanity_rate": cd["profanity_rate"],
            "mean_length": cd["mean_length"],
            "rating_counts": cd["rating_counts"],
            "top_profane": pick("profane_strong", 30),
            "top_rant": pick("rant", 15),
            "top_screaming": pick("screaming", 15),
            "top_exclaim": pick("exclamation", 15),
            "top_short_brutal": pick("short_brutal", 15),
            "top_five_star_obscene": pick("five_star_obscene", 15),
            "top_five_star_one_word": pick("five_star_one_word", 15),
        }
        (OUT_CATS / f"{cat}.json").write_text(json.dumps(page))

    # --- Index summary -------------------------------------------------
    (OUT_DIR / "index.json").write_text(json.dumps({
        "total_parsed": total_parsed,
        "total_profane": total_profane,
        "profanity_rate_global": round(total_profane / max(total_parsed, 1), 4),
        "n_categories": len(cats),
        "rating_counts": d["total_rating_counts"],
    }))

    print(f"wrote {OUT_DIR}")
    print(f"  total_parsed:  {total_parsed:,}")
    print(f"  total_profane: {total_profane:,}")
    print(f"  categories:    {len(cats)}")
    print(f"  findings:      {len(findings)}")
    print(f"  wall rows:     {len(wall)}")


if __name__ == "__main__":
    main()
