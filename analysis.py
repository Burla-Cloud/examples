"""Turn Burla pass outputs into the UI artifacts in `data/`.

Three stages run end-to-end:

  1. `rescore_main`   applies context-aware scoring to the main corpus:
                      dedupes, drops spam, filters proper-noun collisions,
                      and builds the default Wall of Rants + findings.

  2. `rescore_worst`  applies the "worst-of-worst" filters to the slur /
                      censored-profanity corpus: name-trap regexes,
                      ambiguous-root gating, erotica/hip-hop penalties,
                      context multipliers. Produces the ranked input for
                      Unhinged Mode.

  3. `merge_unhinged` normalises the two corpora onto a shared [0, 1] scale,
                      dedupes by (asin, title-slice, text-slice), and writes
                      the Unhinged wall + search pool. Slur-category hits get
                      a tier bonus so RS_HARD lands above raw hard profanity.

Inputs: `samples/ard_reduced.json` (main) and `samples/ard_worst.json` (worst).
Outputs: `data/*.json` consumed by the static frontend. Run: `python analysis.py`.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lexicon import STRONG_PROFANE, MEDIUM_PROFANE, MILD_PROFANE, WORD_RX

HERE = Path(__file__).parent
SAMPLES = HERE / "samples"
OUT_DIR = HERE / "data"
OUT_CATS = OUT_DIR / "categories"
REDUCED_MAIN = SAMPLES / "ard_reduced.json"
REDUCED_WORST = SAMPLES / "ard_worst.json"


# ---------------------------------------------------------------------------
# Category display + bucketing.
# ---------------------------------------------------------------------------
CAT_DISPLAY: Dict[str, Tuple[str, str]] = {
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

FICTION_CATS = {
    "Books", "Kindle_Store", "Movies_and_TV", "Digital_Music", "CDs_and_Vinyl",
    "Unknown",
}
PHYSICAL_CATS = {
    "Home_and_Kitchen", "Grocery_and_Gourmet_Food", "Health_and_Personal_Care",
    "Health_and_Household", "Electronics", "Tools_and_Home_Improvement",
    "Automotive", "Cell_Phones_and_Accessories", "Computers",
    "Clothing_Shoes_and_Jewelry", "Beauty_and_Personal_Care",
    "Sports_and_Outdoors", "Toys_and_Games", "Pet_Supplies", "Appliances",
    "Musical_Instruments", "Office_Products", "Industrial_and_Scientific",
    "Baby_Products", "Patio_Lawn_and_Garden", "Arts_Crafts_and_Sewing",
    "Video_Games", "Software", "Amazon_Fashion", "All_Beauty",
    "Subscription_Boxes", "Gift_Cards", "Handmade_Products",
    "Magazine_Subscriptions",
}


def display(cat: str) -> Dict[str, str]:
    name, emoji = CAT_DISPLAY.get(cat, (cat.replace("_", " "), "📦"))
    return {"cat": cat, "name": name, "emoji": emoji}


# ---------------------------------------------------------------------------
# Shared filters.
# ---------------------------------------------------------------------------
def _spammy(text: str) -> bool:
    """Reject near-empty text and hyper-repetitive one-word spam."""
    if not text:
        return True
    tokens = WORD_RX.findall(text.lower())
    if len(tokens) < 3:
        return False
    counts = Counter(tokens)
    _, top_count = counts.most_common(1)[0]
    if len(tokens) >= 20 and top_count / len(tokens) > 0.45:
        return True
    if top_count / len(tokens) > 0.70:
        return True
    # Any 15-40 char substring repeated 4+ times = spam.
    s = text[:600]
    for seed_len in (15, 20, 30, 40):
        for start in range(0, min(len(s) - seed_len, 200), 10):
            seed = s[start:start + seed_len]
            if seed.strip() and s.count(seed) >= 4:
                return True
    return False


def _body_fp(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").lower())[:150]


PLOT_RX = re.compile(
    r"\b(plot|characters?|author|narrat|storyline|protagonist|heroine|villain|"
    r"chapters?|sequel|trilogy|novel|series|film|movie|episode|seasons?|"
    r"scene|dialog(ue)?|director|actor|actress|cast)\b",
    re.I,
)
COMPLAINT_RX = re.compile(
    r"\bwaste of (money|time|\$)\b|"
    r"\bpiece of (shit|crap|garbage|junk|sh\*t)\b|"
    r"\bdo(n'?t| not) (buy|bother|waste|purchase)\b|"
    r"\brefund\b|\breturn(ed|ing)?\b|\bmoney back\b|"
    r"\bbroken\b|\bbroke\b|\bdoesn'?t work\b|\bdid not work\b|"
    r"\bstopped working\b|\bfell apart\b|"
    r"\bpissed off\b|\bfurious\b|\blivid\b|"
    r"\brip[ \-]?off\b|\bscam(med)?\b|"
    r"\bworst (purchase|product|thing|seller|experience)\b|"
    r"\bnever (again|buying|order)\b|"
    r"\bcheap(ly)? made\b|\bflimsy\b|\bpoor quality\b",
    re.I,
)
HARD_COMPLAINT_RX = re.compile(
    r"\bworst (purchase|product|thing|crap)\b|"
    r"\bpiece of (shit|crap|garbage|sh\*t)\b|"
    r"\bwaste of (money|\$)|"
    r"\bdo(n'?t| not) (buy|bother)\b|"
    r"\brip[ \-]?off\b",
    re.I,
)


def _clean(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "text": (r.get("text") or "").strip(),
        "title": (r.get("title") or "").strip(),
        "rating": r.get("rating"),
        "asin": r.get("asin"),
        "helpful_vote": r.get("helpful_vote"),
        "verified": r.get("verified"),
        "category": r.get("category") or r.get("_category") or "",
        "score": r.get("score") or {},
        "ts": r.get("ts"),
    }


# ---------------------------------------------------------------------------
# Main-corpus rescoring. context-aware profanity scoring for the Wall of
# Rants + the findings page.
# ---------------------------------------------------------------------------
PROPER_NOUN_TRAP_MAIN = re.compile(
    r"\bdick (tracy|van dyke|clark|butkus|cheney|cavett)\b|"
    r"\bmoby[ -]?dick\b|\bphilip k\.? dick\b|\bvan dyke\b|"
    r"\b(inglourious|dirty|grim) bastards?\b|\brichard (nixon|pryor|burton|gere)\b",
    re.I,
)


def _rescore_profanity(text: str) -> Dict[str, Any]:
    """Variety-weighted severity. counts lowercase-only to skip proper nouns."""
    if not text:
        return {"severity": 0, "variety_score": 0, "unique_strong": 0}
    all_caps = text.upper() == text
    strong = Counter(); medium = Counter(); mild = Counter()
    proper_noun_skip = {"dick", "dicks", "cock", "cocks", "bastard", "bastards", "pussy"}

    for m in WORD_RX.finditer(text):
        raw = m.group(0)
        low = raw.lower()
        if (raw[0].isupper() and not raw.isupper() and not all_caps
                and low in proper_noun_skip):
            continue
        if low in STRONG_PROFANE:
            strong[low] += 1
        elif low in MEDIUM_PROFANE:
            medium[low] += 1
        elif low in MILD_PROFANE:
            mild[low] += 1

    ts, tm, tl = sum(strong.values()), sum(medium.values()), sum(mild.values())
    severity = ts * 3 + tm * 2 + tl
    return {
        "severity": severity,
        "variety_score": severity * (1 + 0.5 * len(strong)),
        "unique_strong": len(strong),
        "total_strong": ts, "total_medium": tm, "total_mild": tl,
        "strong_words": dict(strong),
    }


def _rows_from_signal(d: Dict[str, Any], signal: str,
                      filter_spam: bool = True) -> List[Dict[str, Any]]:
    """Flatten every category's top-K for `signal` into a deduped flat list."""
    rows = []
    seen = set()
    for cat, cat_data in d["categories"].items():
        for item in (cat_data.get("top", {}).get(signal, []) or []):
            rev = _clean(item.get("review") or {})
            text = rev.get("text") or ""
            if filter_spam and _spammy(text):
                continue
            fp = _body_fp(text)
            if fp in seen:
                continue
            seen.add(fp)
            rev["_score"] = item.get("score")
            rev["_category"] = cat
            rows.append(rev)
    rows.sort(key=lambda r: -(r.get("_score") or 0))
    return rows


def _build_wall_of_rants(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Merge profane_strong + rant + short_brutal, rescored for variety."""
    seen = set()
    rows = []
    for sig in ("profane_strong", "rant", "short_brutal"):
        for cat, cat_data in d["categories"].items():
            for item in (cat_data.get("top", {}).get(sig, []) or []):
                r = item.get("review") or {}
                text = (r.get("text") or "").strip()
                if not text or _spammy(text):
                    continue
                fp = _body_fp(text)
                if fp in seen:
                    continue
                seen.add(fp)
                rescored = _rescore_profanity(text + " " + (r.get("title") or ""))
                if rescored["unique_strong"] < 2 and rescored["severity"] < 6:
                    continue
                rev = _clean(r)
                rev["_category"] = cat
                rev["_score"] = rescored
                rev["_sort"] = rescored["variety_score"]
                rows.append(rev)
    rows.sort(key=lambda r: -r["_sort"])
    return rows


# ---------------------------------------------------------------------------
# Worst-of-worst rescoring. slur / censored-profanity filter pipeline.
# ---------------------------------------------------------------------------
AMBIGUOUS_ROOTS = {
    "cracker", "crackers", "coon", "coons", "spic", "spics",
    "slant", "slants", "slanteye", "chink", "chinks", "gook", "gooks",
    "dyke", "dykes", "gringo", "gringos", "beaner", "beaners",
    "redskin", "redskins",
    "queer", "queers", "homo", "homos", "pansy", "pansies", "sissy", "sissies",
    "lame", "lamer", "moron", "idiot", "imbecile", "dumbo", "cripple", "crippled",
    "mick", "micks", "gypsy", "gypsies", "oriental", "orientals",
    "hoe", "hoes", "tramp", "tramps", "tranny", "trannies", "shemale", "shemales",
}

CATEGORY_ROOT_BLOCKLIST: Dict[str, set] = {
    "Grocery_and_Gourmet_Food": {"cracker", "crackers", "coon", "coons", "tramp",
                                  "tramps", "beaner", "beaners", "redskin",
                                  "redskins", "hoe", "hoes"},
    "Pet_Supplies": {"cracker", "crackers", "coon", "coons"},
    "Patio_Lawn_and_Garden": {"coon", "coons", "hoe", "hoes", "beaner", "beaners"},
    "Sports_and_Outdoors": {"beaner", "beaners", "cracker", "crackers"},
    "Automotive": {"tranny", "trannies"},
    "Tools_and_Home_Improvement": {"dyke", "dykes"},
    "Beauty_and_Personal_Care": {"slant", "slants"},
    "All_Beauty": {"slant", "slants"},
    "Clothing_Shoes_and_Jewelry": {"slant", "slants"},
    "Industrial_and_Scientific": {"spic", "spics"},
    "Handmade_Products": {"hoe", "hoes"},
}

HIGH_CONFIDENCE_ROOTS = {
    "nigger", "nigger*", "niggers", "nigga", "niggas", "niggah", "niggahs", "niggaz",
    "kike", "kike*", "kikes", "wetback", "wetbacks", "jigaboo", "jigaboos",
    "porchmonkey", "raghead", "ragheads", "sandnigger", "pickaninny",
    "chink*", "gook*", "spic*",
    "faggot", "faggots", "fag*", "fagged", "tranny*",
    "retard", "retards", "retarded", "retard*", "retardo", "mongoloid", "mongoloids",
}

HARD_VULG_ROOTS = {
    "fuck", "fucker", "fuckers", "fucking", "fuckin", "fucked",
    "shit", "shits", "shitty", "shithead", "shithole", "bullshit",
    "bitch", "bitches", "bitchy", "bitching", "cunt", "cunts", "cunty",
    "asshole", "assholes", "cock", "cocks", "cocksucker",
    "dick", "dicks", "dickhead", "pussy", "pussies",
    "whore", "whores", "slut", "sluts", "slutty",
    "bastard", "bastards", "motherfucker", "motherfuckers", "motherfucking",
    "prick", "pricks",
    "fuck*", "shit*", "bitch*", "cunt*", "dick*", "cock*", "asshole*",
    "pussy*", "whore*", "slut*", "bastard*", "prick*", "motherfucker*",
}

EXPLICIT_HATE_CTX = re.compile(
    r"\b(black|white|asian|latino|latina|hispanic|mexican|jewish|jew|"
    r"muslim|arab|indian|native|racist|racism|bigot(ry|ed)?|prejudic|"
    r"supremacis|white\s+trash|redneck|hillbilly|n[\-\s]?word|minorit|"
    r"ethnic|slur|offensive|derogator|stereotyp|gay|lesbian|homosexual|"
    r"transgender|lgbt|pride)\b",
    re.I,
)

# Proper-noun / product-name regex traps. Keyed by (one of the) trap roots.
NAME_TRAPS: Dict[str, re.Pattern] = {
    "slant": re.compile(
        r"\bslants?\s+(?:razor|pocket|shelf|bar|board|cut|edge|top|tile|wall|roof|fence)\b|"
        r"\b(?:futur|merkur|parker|razor)\s+slants?\b|\bslant\-?top\b|\bslant\-?front\b", re.I),
    "chink": re.compile(
        r"\bchinks?\s+(?:in|of)\s+(?:the\s+)?(?:armor|armour|light|metal|sound|glass|coin)\b|"
        r"\bmetal(?:lic)?\s+chinks?\b", re.I),
    "gook": re.compile(
        r"\bgooks?\s+(?:buildup|build\-up|residue|gunk|grease|oil|sludge|dirt)\b|"
        r"\b(?:engine|motor|greasy|slimy|sticky|thick|dried|caked|chemical)\s+gooks?\b|"
        r"\bgook\s+on\b|\b(green|brown|black)\s+gook\b", re.I),
    "dyke": re.compile(
        r"\bdykes?\s+(?:plier|pliers|cutter|cutters|tool|tools|wrench|handle|jaw|jaws|spring|head|blade)\b|"
        r"\b(?:klein|channel\s*lock|side\-?cut|flush\-?cut|linesman)\s+dykes?\b|"
        r"\bnose\s+pliers?\b|\bwire\s+cutters?\b|\bside[ \-]?cutters?\b|\bvan\s+dyke\b", re.I),
    "gringo": re.compile(
        r"\bgringos?\s+(?:bandito|hot\s*sauce|sauce|salsa|chile|chili|food|cuisine|taco|restaurant)\b|"
        r"\b(?:hot\s*sauce|salsa|chile|chili|mexican|food|brand|label)\s+gringos?\b|\bel\s+gringo\b", re.I),
    "dick": re.compile(
        r"\bdick (tracy|van dyke|clark|butkus|cheney|cavett)\b|\bmoby[ \-]?dick\b|"
        r"\bphilip k\.? dick\b|\brichard (nixon|pryor|burton|gere)\b", re.I),
    "mick": re.compile(r"\bmick (jagger|mars)\b|\bmickey (mouse|mantle)\b", re.I),
    "homo": re.compile(r"\bhomo[ \-]?sapiens?\b|\bhomoge(neous|nous|niz)", re.I),
    "pansy": re.compile(r"\bpansy\s+(flower|plant|seed|garden|bed)\b", re.I),
    "gypsy": re.compile(r"\bgypsy\s+(king|rose|jazz|music|caravan|moth)\b", re.I),
    "coon": re.compile(
        r"\bmaine coons?\b|\braccoons?\b|\bcoon\s*hound\b|"
        r"\b(?:cat|kitten|feline|pet|dog|puppy|rabies|flea|tick|leash|litter|paw|claw)\b", re.I),
    "spic": re.compile(r"\bspic[\s\-]+(?:and|n|&|'n')[\s\-]+span|\bspic[\s\-]?n[\s\-]?span\b", re.I),
    "cracker": re.compile(
        r"\bcheese.*cracker|\bbutter.*cracker|\bsalt(ine)?.*cracker|"
        r"\bgraham.*cracker|\branch.*cracker|\boyster.*cracker|\banimal.*cracker|"
        r"\bwheat.*cracker|\brice.*cracker|\bnut[ \-]?cracker|\bcracker\s+jack\b|"
        r"\bfirecracker\b|\bcracker barrel|\bnabisco|\bkeebler|"
        r"\b(box|tin|pack) of crackers|\bflavor(ed|s)?\s+crackers?|"
        r"\bcrackers?\s+(taste|flavor|are|were|have|had|come|package|crispy|"
        r"crunchy|stale|thick|thin|cheesy|salty|sweet|spicy|plain|vegan|gluten|"
        r"organic|broke|shipping|delicious|great)", re.I),
    "tranny": re.compile(
        r"\btranny\s+(?:fluid|cooler|oil|mount|filter|pan|rebuild|swap|gear)\b|"
        r"\b(auto|automatic|manual|transmission|gearbox)\s+tranny\b", re.I),
    "hoe": re.compile(r"\bgarden(?:ing)?\s+hoe\b|\bhoe\s+(blade|handle|tool|stick)\b", re.I),
    "lame": re.compile(r"\blame (duck|horse|excuse|joke)\b", re.I),
    "sissy": re.compile(
        r"\bsissy\s+(maid|training|slut|boy|crossdress|feminization)\b|"
        r"[A-Z][a-z]+\s+Sissy\b|\bSissy\s+[A-Z]|\bnamed\s+sissy\b|"
        r"\b(my|his|her|big|little)\s+sissy\b", re.I),
    "shemale": re.compile(r"\bshemale\b", re.I),
    "oriental": re.compile(r"\boriental (rug|carpet|style|market|restaurant|food|cuisine)\b", re.I),
    "idiot": re.compile(r"\b(village|local|town)\s+idiot\b|\bidiot savant\b", re.I),
    "moron": re.compile(r"\boxy?\s?moron|\bmoronic\b", re.I),
    "queer": re.compile(
        r"\bqueer\s+(eye|theory|studies|nation|as\s+folk)\b|\bthe\s+queer\b|"
        r"\bburroughs\b", re.I),
    "hebe": re.compile(r"\bhebe\s+(?:carlton|jones|smith|goddess|was|is|had|came|went)\b", re.I),
}

EROTICA_MARKERS = re.compile(
    r"\berotic|\bsexy\b|\bsensual\b|\bromance novel\b|\bporn(o|y|ography)?\b|"
    r"\bbdsm\b|\bfetish\b|\bdominat(ion|rix|ed|ing)\b|\bsubmissive\b|"
    r"\bsissy\s+(maid|training|slut|boy)\b|\bshemale\b|\bcock\s+worship\b",
    re.I,
)
HIPHOP_MARKERS = re.compile(
    r"\b(hip[ \-]?hop|rap(per|ping)?|emcee|m\.?c\.?|verse|bars|beats?|"
    r"tupac|scarface|wu[ \-]?tang|biggie|eminem|kanye|dr\.?\s*dre|snoop|"
    r"kendrick|nas\b|ice[ \-]?cube|gangsta|album|track|feat\.|featuring)\b",
    re.I,
)
MULTI_CENSOR = re.compile(r"\b\w\*{3,}\s+\w\*{3,}", re.I)
SINGLE_CAP_CENSOR = re.compile(r"\b[A-Z]\*{4,}\b")

WORST_CAT_WEIGHT = {
    "RS_HARD": 55.0, "RS": 25.0, "HOM": 20.0, "ABL": 8.0,
    "SEX": 5.0, "XEN": 10.0, "VULG": 1.0,
}
WORST_CTX_MULT = {"deploy": 1.5, "quote_crit": 0.3, "reclaim": 0.15, "ambiguous": 0.85}


def _filter_worst_cats(text: str, cats: Dict[str, Dict[str, int]],
                       category: str) -> Dict[str, Dict[str, int]]:
    """Run the full trap/ambiguity/censor-stripping filter chain."""
    if not cats:
        return cats

    # 1. drop hits whose root has a name-trap match in the text
    cleaned: Dict[str, Dict[str, int]] = {}
    for cat, words in cats.items():
        kept = {}
        for w, n in words.items():
            root = w.lower().rstrip("*")
            trap = NAME_TRAPS.get(root)
            if trap and trap.search(text):
                continue
            kept[w] = n
        if kept:
            cleaned[cat] = kept
    cats = cleaned

    # 2. if the text has self-censored proper nouns ("N****** P****"), drop
    #    all heavy-asterisk variants (they're almost always company names).
    multi = bool(MULTI_CENSOR.search(text))
    single_caps = SINGLE_CAP_CENSOR.findall(text)
    if multi or len(single_caps) >= 2:
        cats = {c: {w: n for w, n in d.items() if not w.endswith("*")}
                for c, d in cats.items()}
        cats = {c: d for c, d in cats.items() if d}

    # 3. per-category blocklist (engine tranny, maine coon, saltine crackers...)
    block = CATEGORY_ROOT_BLOCKLIST.get(category, set())
    if block:
        cats = {c: {w: n for w, n in d.items() if w.lower() not in block}
                for c, d in cats.items()}
        cats = {c: d for c, d in cats.items() if d}

    # 4. ambiguous roots only count when supported by a high-confidence slur OR
    #    explicit identity/race/hate keyword in the text.
    has_high_conf = any(w.lower() in HIGH_CONFIDENCE_ROOTS
                        for words in cats.values() for w in words)
    has_explicit = bool(EXPLICIT_HATE_CTX.search(text))
    if not (has_high_conf or has_explicit):
        cats = {c: {w: n for w, n in d.items()
                    if w.lower() not in AMBIGUOUS_ROOTS and w.lower().rstrip("*") not in AMBIGUOUS_ROOTS}
                for c, d in cats.items()}
        cats = {c: d for c, d in cats.items() if d}

    return cats


def _rescore_worst(row: Dict[str, Any]) -> float:
    text = ((row.get("title") or "") + "  " + (row.get("text") or "")).strip()
    if not text:
        return 0.0
    sc = row.get("score") or {}
    cats = sc.get("categories") or {}
    ctx = sc.get("context") or "ambiguous"
    category = row.get("category") or ""
    rating = float(row.get("rating") or 0)
    word_count = int(sc.get("word_count") or 0)

    cats = _filter_worst_cats(text, cats, category)
    if not cats:
        return 0.0

    # Require a real hard-VULG word OR any slur-category hit.
    has_hard_vulg = any(w.lower() in HARD_VULG_ROOTS for w in cats.get("VULG", {}).keys())
    has_slur = any(cat != "VULG" for cat in cats.keys())
    if not (has_hard_vulg or has_slur):
        return 0.0

    severity = 0.0
    for cat, d in cats.items():
        severity += WORST_CAT_WEIGHT.get(cat, 1.0) * (sum(d.values()) + 0.5 * (len(d) - 1))
    severity *= WORST_CTX_MULT[ctx]

    if category in FICTION_CATS:
        severity *= 0.6 if ctx == "deploy" else 0.25
        if list(cats.keys()) == ["ABL"]:
            severity *= 0.2
    elif category in PHYSICAL_CATS:
        severity = severity * 1.4 + 4.0

    if EROTICA_MARKERS.search(text):
        severity *= 0.30
    if HIPHOP_MARKERS.search(text) and ("RS_HARD" in cats or "RS" in cats):
        severity *= 0.35

    severity += len(COMPLAINT_RX.findall(text)) * 1.4
    if HARD_COMPLAINT_RX.search(text):
        severity += 5.0

    if rating == 1: severity = severity * 1.15 + 8.0
    elif rating == 2: severity += 3.0
    elif rating == 4: severity *= 0.55
    elif rating == 5: severity *= 0.35

    severity += float(sc.get("caps_ratio") or 0) * 10.0
    severity += min(int(sc.get("exclam_count") or 0), 20) * 0.15

    if word_count and word_count < 10:
        severity *= 0.5
    elif word_count > 600:
        severity *= 0.8
    return round(severity, 3)


# ---------------------------------------------------------------------------
# Merge main (hard-profanity) + worst (slurs+censored) -> Unhinged corpus.
# ---------------------------------------------------------------------------
BADGE_LABEL = {
    "RS_HARD": "RACIAL_SLUR", "RS": "RACIAL_SLUR", "HOM": "HOMOPHOBIC_SLUR",
    "ABL": "ABLEIST_SLUR", "SEX": "GENDERED_SLUR", "XEN": "XENOPHOBIC_SLUR",
    "VULG": "PROFANITY",
}
BADGE_ORDER = ["RS_HARD", "RS", "HOM", "XEN", "ABL", "SEX", "VULG"]


def _badge_class(cats: Dict[str, Dict[str, int]]) -> str:
    for k in BADGE_ORDER:
        if k in cats:
            return BADGE_LABEL[k]
    return ""


def _dedup_key(r: Dict[str, Any]) -> str:
    return (f"{r.get('asin','')}|{(r.get('title') or '')[:40].lower()}|"
            f"{(r.get('text') or '')[:60].lower()}")


def _normalize(rows: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    if not rows:
        return rows
    scores = [float(r.get("_rescore") or 0) for r in rows]
    lo, hi = min(scores), max(scores) or 1.0
    rng = (hi - lo) or 1.0
    for r in rows:
        r["_source"] = source
        r["_norm"] = (float(r.get("_rescore") or 0) - lo) / rng
    return rows


# ---------------------------------------------------------------------------
# Top-level orchestration. writes every file the frontend needs.
# ---------------------------------------------------------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_CATS.mkdir(parents=True, exist_ok=True)

    d = json.loads(REDUCED_MAIN.read_text())
    total_parsed = d["total_parsed"]
    total_profane = d["total_profane"]
    cats = d["categories"]

    # ---- Category summary + rating table -----------------------------------
    cat_rows = []
    for cat, cd in cats.items():
        meta = display(cat)
        cat_rows.append({
            **meta,
            "n_parsed": cd["n_parsed"], "n_profane": cd["n_profane"],
            "profanity_rate": cd["profanity_rate"],
            "mean_length": cd["mean_length"],
            "rating_counts": cd["rating_counts"],
            "pct_1_star": round(cd["rating_counts"].get("1", 0) / max(cd["n_parsed"], 1), 4),
            "pct_5_star": round(cd["rating_counts"].get("5", 0) / max(cd["n_parsed"], 1), 4),
        })
    cat_rows.sort(key=lambda r: -r["profanity_rate"])
    (OUT_DIR / "categories.json").write_text(json.dumps(cat_rows))

    # ---- Wall of Rants (default mode) --------------------------------------
    wall_rows = _build_wall_of_rants(d)[:120]
    (OUT_DIR / "wall.json").write_text(json.dumps({
        "title": "The Wall of Rants",
        "blurb": (f"The {len(wall_rows)} most unhinged reviews from "
                  f"{total_parsed:,} Amazon reviews across {len(cat_rows)} categories. "
                  "Reranked by profanity diversity, intensity, and rant length. "
                  "No sanitization. Raw Amazon, as written."),
        "rows": wall_rows,
    }))

    # ---- Findings ---------------------------------------------------------
    findings: List[Dict[str, Any]] = []
    findings.append({
        "id": "category_profanity",
        "title": "The filthiest Amazon categories, ranked",
        "blurb": "Share of each category's reviews containing at least one profanity hit. Video games leads by a mile.",
        "rows": [{**r, "profanity_pct": round(r["profanity_rate"] * 100, 3)}
                 for r in cat_rows[:34]],
    })
    findings.append({
        "id": "screaming",
        "title": "The loudest reviewers on Amazon",
        "blurb": "All-caps word ratio multiplied by sqrt(length). Longer screaming beats shorter screaming.",
        "rows": _rows_from_signal(d, "screaming")[:60],
    })
    findings.append({
        "id": "exclamation",
        "title": "Punctuation bombs",
        "blurb": "Reviews with the most consecutive exclamation marks. More emotional than enraged.",
        "rows": _rows_from_signal(d, "exclamation")[:60],
    })

    short_all = _rows_from_signal(d, "short_brutal") + _rows_from_signal(d, "profane_strong")
    short: List[Dict[str, Any]] = []
    seen_short: set = set()
    for r in short_all:
        text = r.get("text") or ""
        n_words = len(WORD_RX.findall(text))
        if n_words < 4 or n_words > 35:
            continue
        fp = re.sub(r"\s+", " ", text.lower())[:80]
        if fp in seen_short:
            continue
        seen_short.add(fp)
        rescored = _rescore_profanity(text + " " + (r.get("title") or ""))
        if rescored["severity"] >= 5:
            r["_score"] = rescored
            short.append(r)
        if len(short) >= 60:
            break
    findings.append({
        "id": "short_brutal",
        "title": "Reviews too brutal for two sentences",
        "blurb": "Under 35 words, full of profanity. Concentrated rage in a haiku.",
        "rows": short,
    })

    findings.append({
        "id": "rant",
        "title": "Rant hall of fame",
        "blurb": "Score = length + profanity + ALL-CAPS + exclamation marks. Pure artisanal Karen energy.",
        "rows": _rows_from_signal(d, "rant")[:60],
    })

    fso: List[Dict[str, Any]] = []
    for r in _rows_from_signal(d, "five_star_obscene"):
        if (r.get("rating") or 0) < 5:
            continue
        rescored = _rescore_profanity((r.get("text") or "") + " " + (r.get("title") or ""))
        if rescored["total_strong"] >= 1 and rescored["severity"] >= 4:
            r["_score"] = rescored
            fso.append(r)
        if len(fso) >= 50:
            break
    findings.append({
        "id": "five_star_obscene",
        "title": "Five stars but still completely unhinged",
        "blurb": "5-star reviews also stuffed with profanity. The 'this product fucking slaps' genre.",
        "rows": fso,
    })

    findings.append({
        "id": "five_star_one_word",
        "title": "Five stars, zero words",
        "blurb": "Five stars with one word or less of text. The bleakest genre of human writing.",
        "rows": _rows_from_signal(d, "five_star_one_word")[:40],
    })

    rating_rows = []
    for r in cat_rows:
        total = r["n_parsed"] or 1
        rc = r["rating_counts"]
        rating_rows.append({
            **display(r["cat"]),
            "n_parsed": r["n_parsed"],
            "pct_1": round(100 * rc.get("1", 0) / total, 2),
            "pct_2": round(100 * rc.get("2", 0) / total, 2),
            "pct_3": round(100 * rc.get("3", 0) / total, 2),
            "pct_4": round(100 * rc.get("4", 0) / total, 2),
            "pct_5": round(100 * rc.get("5", 0) / total, 2),
        })
    rating_rows.sort(key=lambda r: -r["pct_1"])
    findings.append({
        "id": "rating_distribution",
        "title": "Which categories get the most 1-star rage reviews",
        "blurb": "Share of 1-star to 5-star ratings per category. Wider left tail = angrier customers.",
        "rows": rating_rows,
    })

    words_rows = sorted(
        [{**display(r["cat"]), "n_parsed": r["n_parsed"], "mean_length": r["mean_length"]}
         for r in cat_rows],
        key=lambda r: -r["mean_length"],
    )
    findings.append({
        "id": "mean_length",
        "title": "Who writes the longest reviews?",
        "blurb": "Mean review length (characters). Book readers write novels. Gift card buyers write nothing.",
        "rows": words_rows,
    })
    (OUT_DIR / "findings.json").write_text(json.dumps(findings))

    # ---- Per-category detail pages ----------------------------------------
    for cat, cd in cats.items():
        meta = display(cat)
        top = cd.get("top") or {}

        def pick(sig: str, k: int) -> List[Dict[str, Any]]:
            return [{**_clean(it["review"]), "_score": it["score"]}
                    for it in (top.get(sig, []) or [])[:k]]

        (OUT_CATS / f"{cat}.json").write_text(json.dumps({
            **meta,
            "n_parsed": cd["n_parsed"], "n_profane": cd["n_profane"],
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
        }))

    (OUT_DIR / "index.json").write_text(json.dumps({
        "total_parsed": total_parsed, "total_profane": total_profane,
        "profanity_rate_global": round(total_profane / max(total_parsed, 1), 4),
        "n_categories": len(cats),
        "rating_counts": d["total_rating_counts"],
    }))

    # ---- Main search pool. top short + wall rows, capped for payload -----
    search_rows: List[Dict[str, Any]] = []
    seen_search: set = set()
    for r in wall_rows[:120] + short[:60] + _rows_from_signal(d, "rant")[:60]:
        fp = _body_fp(r.get("text") or "")
        if fp in seen_search or not fp:
            continue
        seen_search.add(fp)
        search_rows.append(r)
    (OUT_DIR / "search.json").write_text(json.dumps({"rows": search_rows}))

    # ---- Worst-of-worst rescoring + Unhinged corpus ----------------------
    if not REDUCED_WORST.exists():
        print(f"warn: {REDUCED_WORST} not found. skipping Unhinged Mode.")
        print(f"wrote {OUT_DIR}  (main only)")
        return

    dw = json.loads(REDUCED_WORST.read_text())
    worst_ranked: List[Dict[str, Any]] = []
    for r in dw.get("global_top") or []:
        s = _rescore_worst(r)
        if s <= 0:
            continue
        worst_ranked.append({**r, "_rescore": s})
    worst_ranked.sort(key=lambda r: -r["_rescore"])

    # Treat wall_rows (main corpus) as the "hard profanity" source; we
    # already ranked them with _rescore_profanity above.
    hard: List[Dict[str, Any]] = []
    for r in wall_rows:
        hard.append({**r, "_rescore": (r.get("_sort") or r.get("_score") or {}).get("variety_score")
                     if isinstance(r.get("_score"), dict) else (r.get("_sort") or 0)})
    # Some hard rows will miss _rescore. use _sort fallback.
    for r in hard:
        if r.get("_rescore") is None:
            r["_rescore"] = r.get("_sort") or 0

    _normalize(hard, "hard_profanity")
    _normalize(worst_ranked, "worst_of_worse")

    merged: Dict[str, Dict[str, Any]] = {}
    for r in hard + worst_ranked:
        key = _dedup_key(r)
        existing = merged.get(key)
        if existing is None or r["_norm"] > existing["_norm"]:
            merged[key] = r

    def _rank(r: Dict[str, Any]) -> float:
        base = r.get("_norm", 0) * 70
        slur_cats = (r.get("score") or {}).get("categories") or {}
        bump = 0.0
        if "RS_HARD" in slur_cats: bump += 15.0
        if "RS" in slur_cats:      bump += 6.0
        if "HOM" in slur_cats:     bump += 4.0
        return base + bump

    unhinged_rows = sorted(merged.values(), key=_rank, reverse=True)
    for r in unhinged_rows:
        cats_map = (r.get("score") or {}).get("categories") or {}
        r["_badge"] = _badge_class(cats_map)
        r["_slur_categories"] = sorted(cats_map.keys())
        r.setdefault("_category", r.get("category"))

    (OUT_DIR / "unhinged.json").write_text(json.dumps({
        "blurb": ("The most unhinged of 571,544,386 Amazon reviews. every f-bomb, slur, "
                  "censored rant, and full-caps meltdown the three-pass pipeline "
                  "could surface. Flip Unhinged Mode off to return to the Wall of Rants."),
        "rows": unhinged_rows[:120],
    }))
    (OUT_DIR / "unhinged_search.json").write_text(json.dumps({
        "rows": unhinged_rows[:400],
    }))

    print(f"wrote {OUT_DIR}")
    print(f"  total_parsed:  {total_parsed:,}")
    print(f"  total_profane: {total_profane:,}")
    print(f"  wall rows:     {len(wall_rows)}")
    print(f"  unhinged rows: {len(unhinged_rows[:120])}  (pool {len(unhinged_rows)})")
    print(f"  findings:      {len(findings)}")


if __name__ == "__main__":
    main()
