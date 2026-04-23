"""Rescore the 500-item vulgar corpus to surface pissed-off consumers, not
Dick Tracy DVD plot summaries.

Reads samples/ard_vulgar.json, applies a quality filter, writes:
  samples/ard_vulgar_ranked.json  — top 200 for the site
  samples/ard_vulgar_wall.json    — top 40 for a hero 'Wall of Vulgar' section
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List


# Reviews in these categories often describe FICTIONAL uses of hard words
# (romance novels, detective stories, cartoons). We don't exclude them, but
# we do require stronger signal that the reviewer is actually complaining.
FICTION_CATS = {
    "Books", "Kindle_Store", "Movies_and_TV", "Digital_Music", "CDs_and_Vinyl",
    "Unknown",  # the 2023 dataset's miscellaneous bucket is mostly book-like
}

# Physical-product categories where profanity = pissed-off customer.
# Massive boost so these out-rank book/movie plot summaries.
PHYSICAL_CATS = {
    "Home_and_Kitchen", "Grocery_and_Gourmet_Food", "Health_and_Personal_Care",
    "Health_and_Household", "Electronics", "Tools_and_Home_Improvement",
    "Automotive", "Cell_Phones_and_Accessories", "Computers",
    "Clothing_Shoes_and_Jewelry", "Beauty_and_Personal_Care",
    "Sports_and_Outdoors", "Toys_and_Games", "Pet_Supplies",
    "Appliances", "Musical_Instruments", "Office_Products",
    "Industrial_and_Scientific", "Baby_Products", "Patio_Lawn_and_Garden",
    "Arts_Crafts_and_Sewing", "Video_Games", "Software",
    "Amazon_Fashion", "All_Beauty", "Subscription_Boxes",
    "Gift_Cards", "Handmade_Products", "Magazine_Subscriptions",
}

# Phrases that scream "I am describing the plot of something", not "this
# product made me angry".
PLOT_PHRASES = [
    r"\bplot\b", r"\bcharacters?\b", r"\bauthor\b", r"\bnarrat", r"\bstoryline\b",
    r"\bprotagonist\b", r"\bheroine\b", r"\bvillain\b", r"\bchapters?\b",
    r"\bsequel\b", r"\btrilogy\b", r"\bnovel\b", r"\bseries\b",
    r"\bfilm\b", r"\bmovie\b", r"\bepisode\b", r"\bseasons?\b",
    r"\bscene\b", r"\bdialog(ue)?\b",
    r"\bdirector\b", r"\bactor\b", r"\bactress\b", r"\bcast\b",
]
PLOT_RX = re.compile("|".join(PLOT_PHRASES), re.I)

# Proper-noun traps — if any of these appear, the hit is almost certainly
# a name/title, not profanity.
PROPER_NOUN_TRAPS = [
    r"\bdick tracy\b", r"\bdick van dyke\b", r"\bphilip k\.? dick\b",
    r"\bmoby[ -]?dick\b", r"\bdick clark\b", r"\bdick butkus\b",
    r"\bdick cheney\b",
    r"\b3rd rock\b", r"\bthird rock\b",
    r"\bbastards? inc\b", r"\binglourious bastards?\b",
    r"\bdirty bastards?\b",     # Recurring romance/MC series
    r"\bgrim bastards?\b",
    r"\bcunt(s|y)? of monte cristo\b",  # absurd but safe
]
PROPER_NOUN_RX = re.compile("|".join(PROPER_NOUN_TRAPS), re.I)

# Consumer-complaint signal — phrases real pissed-off humans write.
COMPLAINT_PHRASES = [
    r"\bwaste of (money|time|my money)\b",
    r"\bpiece of (shit|crap|garbage|junk)\b",
    r"\bdon'?t (buy|bother|waste)\b",
    r"\bdo not (buy|bother|waste)\b",
    r"\bnever (buy|again|bought)\b",
    r"\brip[ -]?off\b",
    r"\bscam(med)?\b",
    r"\brefund\b", r"\breturn(ed|ing)?\b", r"\bmoney back\b",
    r"\bbroken\b", r"\bbroke\b", r"\bdoesn'?t work\b", r"\bdid not work\b",
    r"\bstopped working\b", r"\bfell apart\b", r"\bpiece of junk\b",
    r"\bjunk\b", r"\bgarbage\b", r"\btrash(ed|y)?\b",
    r"\bi'?m pissed\b", r"\bpissed off\b", r"\bso pissed\b",
    r"\bfurious\b", r"\blivid\b", r"\bi swear to (god|christ)\b",
    r"\bthis thing\b", r"\bthis product\b",
    r"\bcheap(ly)? made\b", r"\bflimsy\b", r"\bpoor quality\b",
    r"\bwors(t|e) than\b", r"\bworst (purchase|product|thing)\b",
]
COMPLAINT_RX = re.compile("|".join(COMPLAINT_PHRASES), re.I)

# If the review mentions these, it is almost certainly a consumer product rant
# (not literary criticism). Give it a bigger kick.
HARD_COMPLAINT = re.compile(
    r"\bworst (purchase|product|thing|crap)\b|"
    r"\bpiece of (shit|crap|garbage)\b|"
    r"\bwaste of (money|\$)|"
    r"\bdo(n'?t| not) (buy|bother)\b|"
    r"\brip[ -]?off\b",
    re.I,
)


def _is_fictional_review(text: str, cat: str) -> bool:
    """Heuristic: the reviewer is describing fictional content, not complaining."""
    if cat in FICTION_CATS:
        if PLOT_RX.search(text):
            return True
    return False


def _has_proper_noun_trap(text: str) -> bool:
    return bool(PROPER_NOUN_RX.search(text))


def _rescore(row: Dict[str, Any]) -> float:
    text = ((row.get("title") or "") + "  " + (row.get("text") or "")).strip()
    if not text:
        return 0.0
    cat = row.get("category") or ""
    sc = row.get("score") or {}
    unique_roots = int(sc.get("unique_roots") or 0)
    total_hits = int(sc.get("total_hits") or 0)
    roots = sc.get("roots") or {}
    rating = float(row.get("rating") or 0)

    # Base: variety matters way more than volume.
    base = unique_roots ** 1.8 + min(total_hits, 20) * 0.3

    # Proper-noun trap: slam it to zero so Dick Tracy / 3rd Rock / Dirty
    # Bastards fall out entirely.
    if _has_proper_noun_trap(text):
        return 0.0

    # If every hit is from a single root and that root matches a known
    # benign name, penalise hard.
    if unique_roots == 1 and roots:
        (only_root, only_count), = roots.items()
        if only_root in {"dick", "bastard"} and only_count >= 8:
            # Probably someone writing about a Dick movie or Dirty Bastards book.
            base *= 0.2

    # Fiction-category plot summary penalty — much stronger now.
    if _is_fictional_review(text, cat):
        base *= 0.18
    elif cat in FICTION_CATS:
        base *= 0.55   # fiction cats without plot words still de-prioritised

    # Physical-product boost — angry customer rants about REAL things.
    if cat in PHYSICAL_CATS:
        base += 6.0
        base *= 1.5

    # Positive signal — real complaints.
    complaint_hits = len(COMPLAINT_RX.findall(text))
    base += complaint_hits * 1.6
    if HARD_COMPLAINT.search(text):
        base += 6.0

    # Low-rating boost — 1-star reviewers are funnier.
    if rating and rating <= 2:
        base += 4.0
    elif rating and rating >= 4:
        base *= 0.6

    # Caps/exclamation mania — unhinged energy.
    base += float(sc.get("caps_ratio") or 0) * 6.0
    base += min(int(sc.get("exclam_count") or 0), 20) * 0.08

    # Length sanity — too short isn't funny, too long isn't readable.
    n = int(sc.get("word_count") or 0)
    if n < 12:
        base *= 0.5
    elif n > 500:
        base *= 0.75

    return round(base, 3)


def main() -> None:
    src = Path(__file__).parent / "samples" / "ard_vulgar.json"
    d = json.load(open(src))
    rows: List[Dict[str, Any]] = d["global_top"]

    ranked = []
    for r in rows:
        s = _rescore(r)
        if s <= 0:
            continue
        r = {**r, "_rescore": s}
        ranked.append(r)
    ranked.sort(key=lambda r: -r["_rescore"])

    top = ranked[:200]
    wall = ranked[:40]

    out_ranked = Path(__file__).parent / "samples" / "ard_vulgar_ranked.json"
    out_ranked.write_text(json.dumps({
        "total_reviews_parsed": d["total_reviews_parsed"],
        "total_hard_hits": d["total_hard_hits"],
        "hits_per_million": d["hits_per_million"],
        "kept": len(top),
        "rows": top,
    }, indent=2))

    out_wall = Path(__file__).parent / "samples" / "ard_vulgar_wall.json"
    out_wall.write_text(json.dumps({
        "blurb": (
            f"Unsanitized rants from {d['total_reviews_parsed']:,} Amazon reviews, "
            f"ranked by how much four-letter fury the reviewer let slip."
        ),
        "rows": wall,
    }, indent=2))

    print(f"input:   {len(rows)} rows")
    print(f"kept:    {len(top)}  (wrote ard_vulgar_ranked.json)")
    print(f"wall:    {len(wall)}  (wrote ard_vulgar_wall.json)")
    print()
    print("=== TOP 15 AFTER RESCORE ===")
    for i, r in enumerate(top[:15], 1):
        title = (r.get("title") or "").strip()[:72]
        text = (r.get("text") or "").strip().replace("\n", " ")[:160]
        rating = r.get("rating") or "?"
        cat = r.get("category")
        roots = (r.get("score") or {}).get("roots") or {}
        print(f'{i:2}. [{cat}] {rating}★  score={r["_rescore"]:.1f}  roots={dict(sorted(roots.items(), key=lambda kv: -kv[1]))}')
        print(f"    title: {title}")
        print(f"    text:  {text}")
        print()


if __name__ == "__main__":
    main()
