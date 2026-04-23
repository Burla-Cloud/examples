"""Rescore the worst-of-worst corpus so the TRULY unhinged surfaces first.

Filters out the avalanche of false positives we get from:
  * Censored proper-noun company names (e.g. `N****** P****` = Nations Photo)
  * Product names containing slur-adjacent strings (Spic and Span, Maine Coon,
    Dick Van Dyke, Dick Tracy, Moby Dick, cheese crackers, etc.)
  * Rap/hip-hop album reviews quoting lyrics
  * Erotica plot descriptions listing fetish content
  * Ambiguous terms (queer, homo, sissy, cracker, lame, idiot) used
    non-pejoratively

Boosts:
  * Deploy-context slurs in physical-product categories (genuine rant)
  * Low-rating reviews
  * Caps-lock, exclamation-heavy energy
  * Hard-R slur category when context = deploy

Outputs:
  samples/ard_worst_ranked.json . top 400 for search / expanded corpus
  samples/ard_worst_wall.json   . top 50 for the Unhinged Mode wall
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List


FICTION_CATS = {
    "Books", "Kindle_Store", "Movies_and_TV", "Digital_Music", "CDs_and_Vinyl",
    "Unknown",
}

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

# -------------------------------------------------------------------------
# Words that are SLURS but have MAJOR benign meanings. These must ALWAYS
# co-occur with either (a) a deploy context, (b) another high-confidence
# slur category, OR (c) an explicit hate/race/identity keyword before they
# count. Otherwise we zero them out. too many food crackers / Maine Coon
# / Spic-and-Span / Van Dyke / Mick Jagger false positives.
# -------------------------------------------------------------------------
AMBIGUOUS_ROOTS = {
    # RS
    "cracker", "crackers", "coon", "coons", "spic", "spics",
    "slant", "slants", "slanteye",      # razor / pocket style
    "chink", "chinks",                  # "chink in the armor"
    "gook", "gooks",                    # engine gunk / slang for mess
    "dyke", "dykes",                    # side-cutter pliers
    "gringo", "gringos",                # food brands, humorous self-use
    "beaner", "beaners",                # climbing carabiner slang
    "redskin", "redskins",              # peanut variety
    # HOM
    "queer", "queers", "homo", "homos", "pansy", "pansies",
    "sissy", "sissies",
    # ABL
    "lame", "lamer", "moron", "idiot", "imbecile", "dumbo", "cripple", "crippled",
    # XEN
    "mick", "micks", "gypsy", "gypsies", "oriental", "orientals",
    # SEX / internet
    "hoe", "hoes", "tramp", "tramps",
    # HOM auto-transmission (uncensored forms. "tranny fluid")
    "tranny", "trannies",
    # also the SEX-category erotica defaults
    "shemale", "shemales",
}

# Roots that are too noisy to count as slur hits in specific categories -
# always drop them for those categories regardless of other signals.
CATEGORY_ROOT_BLOCKLIST = {
    "Grocery_and_Gourmet_Food": {
        "cracker", "crackers", "coon", "coons", "tramp", "tramps",
        "beaner", "beaners", "redskin", "redskins", "hoe", "hoes",
    },
    "Pet_Supplies": {"cracker", "crackers", "coon", "coons"},
    "Patio_Lawn_and_Garden": {
        "coon", "coons", "hoe", "hoes", "beaner", "beaners",
    },
    "Sports_and_Outdoors": {
        "beaner", "beaners",  # carabiner
        "cracker", "crackers",  # food/snack
    },
    "Automotive": {
        "tranny", "trannies",
    },
    "Tools_and_Home_Improvement": {
        "dyke", "dykes",
    },
    "Beauty_and_Personal_Care": {"slant", "slants"},
    "All_Beauty": {"slant", "slants"},
    "Clothing_Shoes_and_Jewelry": {"slant", "slants"},
    "Industrial_and_Scientific": {"spic", "spics"},
    "Handmade_Products": {"hoe", "hoes"},
}

# Words that are HIGH confidence. almost always used as slurs.
HIGH_CONFIDENCE_ROOTS = {
    # RS_HARD
    "nigger", "nigger*", "niggers", "nigga", "niggas", "niggah", "niggahs", "niggaz",
    # RS
    "kike", "kike*", "kikes",
    "wetback", "wetbacks", "jigaboo", "jigaboos", "porchmonkey", "raghead",
    "ragheads", "sandnigger", "pickaninny",
    "chink*",  # censored `ch*nk` = slur only; uncensored `chink` = ambiguous
    "gook*",
    "spic*",
    # HOM
    "faggot", "faggots", "fag*", "fagged", "tranny*",
    # ABL
    "retard", "retards", "retarded", "retard*", "retardo", "mongoloid", "mongoloids",
}

# Explicit race / identity keywords. presence in the text is enough to let
# an AMBIGUOUS_ROOTS hit count.
EXPLICIT_HATE_CONTEXT = re.compile(
    r"\b(black|white|asian|latino|latina|hispanic|mexican|jewish|jew|"
    r"muslim|arab|indian|native|racist|racism|bigot(ry|ed)?|prejudic|"
    r"supremacis|white\s+trash|redneck|hillbilly|n[\-\s]?word|"
    r"minorit|ethnic|slur|offensive|derogator|stereotyp|"
    r"gay|lesbian|homosexual|transgender|lgbt|pride)\b",
    re.I,
)


# -------------------------------------------------------------------------
# Proper-noun / product-name traps. If matched anywhere in the review,
# that root's hits are dropped entirely.
# -------------------------------------------------------------------------
NAME_TRAPS_BY_ROOT: Dict[str, str] = {
    # RS root: "slant". razor head style, pocket style, shelf style
    "slant": (
        r"\bslant\s+(?:razor|razors|pocket|pockets|shelf|shelves|bar|bars|"
        r"board|boards|cut|cuts|edge|edges|top|tops|tile|wall|roof|fence)\b|"
        r"\b(?:futur|merkur|parker|razor)\s+slants?\b|"
        r"\bslant\-?top\b|\bslant\-?front\b"
    ),
    "slants": (
        r"\bslants?\s+(?:razor|pocket|shelf|bar|board|cut|edge|top|tile|wall|roof|fence)\b|"
        r"\b(?:futur|merkur|parker|razor)\s+slants?\b"
    ),
    # RS root: "chink". "chink in the armor", sound of metal
    "chink": (
        r"\bchink\s+(?:in|of)\s+(?:the\s+)?(?:armor|armour)\b|"
        r"\bchink\s+of\s+(?:metal|glass|coin|light|sound)\b|"
        r"\bmetal(?:lic)?\s+chink\b"
    ),
    "chinks": (
        r"\bchinks?\s+in\s+(?:the\s+)?(?:armor|armour)\b|"
        r"\bchinks?\s+of\s+(?:light|metal|sound|glass)\b|"
        r"\bmetallic\s+chinks?\b"
    ),
    # RS root: "gook". automotive/mechanical gunk
    "gook": (
        r"\bgook\s+(?:buildup|build\-up|residue|gunk|grease|oil|sludge|dirt)\b|"
        r"\b(?:engine|motor|greasy|slimy|sticky|thick|dried|caked|chemical)\s+gook\b|"
        r"\bgook\s+on\b|\bgreen\s+gook\b|\bbrown\s+gook\b|\bblack\s+gook\b"
    ),
    "gooks": (
        r"\b(?:engine|motor|greasy|sticky|thick)\s+gooks?\b"
    ),
    # HOM root: "dyke" / "dykes". Klein side-cutter pliers
    "dyke": (
        r"\bdyke\s+(?:plier|pliers|cutter|cutters|tool|tools|wrench)\b|"
        r"\b(?:klein|channel\s*lock|channellock|side\-?cut|flush\-?cut|linesman)\s+dykes?\b|"
        r"\bdykes?\s+(?:handle|jaw|jaws|spring|head|blade)\b|"
        r"\bnose\s+pliers?\b|\bwire\s+cutters?\b|\bside[ \-]?cutters?\b"
    ),
    "dykes": (
        r"\bdykes?\s+(?:plier|pliers|cutter|cutters|tool|tools|handle|jaw|jaws|spring|head|blade)\b|"
        r"\b(?:klein|channel\s*lock|channellock|side\-?cut|flush\-?cut|linesman)\s+dykes?\b|"
        r"\bnose\s+pliers?\b|\bwire\s+cutters?\b|\bside[ \-]?cutters?\b"
    ),
    # XEN root: "gringo". food brand (Gringo Bandito, Hot Sauce Gringo, etc.)
    "gringo": (
        r"\bgringo\s+(?:bandito|hot\s*sauce|sauce|salsa|chile|chili|food|cuisine|taco|restaurant)\b|"
        r"\b(?:hot\s*sauce|salsa|chile|chili|mexican|food|brand|label)\s+gringo\b|"
        r"\bel\s+gringo\b"
    ),
    "gringos": (
        r"\bgringos?\s+(?:bandito|hot\s*sauce|sauce|salsa|chile|chili|food|cuisine|taco)\b|"
        r"\b(?:hot\s*sauce|salsa|chile|chili|mexican|food|brand|label)\s+gringos?\b"
    ),
    "dick": (
        r"\bdick van dyke\b|\bvan dyke\b|\bdick tracy\b|"
        r"\bmoby[ \-]?dick\b|\bdick clark\b|\bdick butkus\b|"
        r"\bdick cheney\b|\bphilip k\.? dick\b|\bdick cavett\b|"
        r"\brichard (nixon|pryor|burton|gere)\b"
    ),
    "dyke": r"\bvan dyke\b|\bdick van dyke\b",
    "mick": r"\bmick jagger\b|\bmick mars\b|\bmickey (mouse|mantle)\b",
    "homo": r"\bhomo[ \-]?sapiens?\b|\bhomoge(neous|nous|nize|nized|nises|nize|nously)\b",
    "pansy": r"\bpansy\s+(flower|plant|seed|garden|bed)\b",
    "gypsy": r"\bgypsy\s+(king|rose|jazz|music|caravan|moth)\b",
    "coon": (
        r"\bmaine coon|\bmaine coons\b|\braccoon|\bcoon\s*hound\b|"
        r"\b(?:cat|kitten|kitty|feline|pet|dog|puppy|animal|rabies|fur|"
        r"flea|tick|collar|leash|litter|paw|claw|collar|spray)\b"
    ),
    "spic": r"\bspic[\s\-]+(?:and|n|&|'n')[\s\-]+span|\bspic[\s\-]?n[\s\-]?span\b",
    "cracker": (
        r"\bcheese.*cracker|\bbutter.*cracker|\bsalt(ine)?.*cracker|"
        r"\bgraham.*cracker|\branch.*cracker|\boyster.*cracker|"
        r"\banimal.*cracker|\bwheat.*cracker|\bmatzo.*cracker|"
        r"\brice.*cracker|\bmultigrain|\bgluten\-?free|\bnut[ \-]?cracker|"
        r"\bcracker\s+jack\b|\bfirecracker\b|\bcracker barrel|"
        r"\bnabisco|\bkeebler|\bpremium crackers|\bchex|\bbox of crackers|"
        r"\btin of crackers|\bpack of crackers|\bflavor(ed|s)?\s+crackers?|"
        r"\bcrackers?\s+(taste|flavor|are|were|have|had|come|package|crispy|"
        r"crunchy|stale|thick|thin|cheesy|salty|sweet|spicy|plain|vegan|"
        r"gluten|organic|broke|shipping|delicious|great|box|tin|pack)"
    ),
    "tranny": (
        r"\btranny\s+(?:fluid|cooler|oil|mount|filter|pan|rebuild|swap|gear)\b|"
        r"\b(auto|automatic|manual|transmission|gearbox)\s+tranny\b"
    ),
    "hoe": r"\bgarden(?:ing)? hoe\b|\bhoe\s+(blade|handle|tool|stick)\b",
    "coons": r"\bmaine coons\b|\braccoons\b",
    "crackers": (
        r"\bcheese.*crackers?|\bbutter.*crackers?|\bsalt(ine)?.*crackers?|"
        r"\bgraham.*crackers?|\branch.*crackers?|\boyster.*crackers?|"
        r"\banimal.*crackers?|\bwheat.*crackers?|\bmatzo.*crackers?|"
        r"\brice.*crackers?|\bnabisco|\bkeebler|\bpremium crackers|"
        r"\bbox of crackers|\btin of crackers|\bpack of crackers|"
        r"\bflavor(ed|s)?\s+crackers?|"
        r"\bcrackers?\s+(taste|flavor|are|were|have|had|come|package|crispy|"
        r"crunchy|stale|thick|thin|cheesy|salty|sweet|spicy|plain|vegan|"
        r"gluten|organic|broke|shipping|delicious|great|box|tin|pack)"
    ),
    "lame": r"\blame (duck|horse|excuse|joke)\b",
    "sissy": (
        r"\bsissy\s+(maid|training|slut|boy|crossdress|feminization)\b|"
        # "Sissy" capitalized mid-sentence = character name (heuristic).
        # Only accept uncensored "sissy" as a slur when it appears lowercase
        # AND is not preceded by capital-context markers.
        r"[A-Z][a-z]+\s+Sissy\b|\bSissy\s+[A-Z]|"
        r"\bnamed\s+sissy\b|\bcharacter\s+sissy\b|"
        # literary baby-talk usage. `sissy` meaning `sister`
        r"\b(my|his|her|big|little)\s+sissy\b"
    ),
    "sissies": r"\bsissies\s+(maid|boy|crossdress)\b|\bSissies\b",
    "shemale": r"\bshemale\b",  # all shemale hits in Amazon context = erotica
    "lamer": r"\blame excuse\b",
    "oriental": r"\boriental (rug|carpet|style|market|restaurant|food|cuisine)\b",
    "idiot": r"\b(village|local|town)\s+idiot\b|\bidiot savant\b",
    "moron": r"\boxy?\s?moron|\bmoronic\b",  # "oxymoron" shouldn't count
    # Roots that are almost always character names in book reviews.
    "hebe": r"\bhebe\s+(?:carlton|jones|smith|goddess|was|is|had|came|went)\b|\bgoddess\s+hebe\b",
    "hebes": r"\bhebes\b",  # rare enough to keep all
    # "The Queer" = Burroughs novel. "Queer Eye" = TV show. etc.
    "queer": (
        r"\bqueer\s+(eye|theory|studies|nation|as\s+folk)\b|"
        r"\bthe\s+queer\b|\bburroughs\b|\bwilliam\s+s\.?\s+burroughs\b"
    ),
}

ALL_NAME_TRAPS_RX = {
    root: re.compile(pat, re.I)
    for root, pat in NAME_TRAPS_BY_ROOT.items()
}


# Erotica / fetish detectors. if present AND cats includes HOM or SEX
# without deploy context, penalize hard.
EROTICA_MARKERS = re.compile(
    r"\berotic|\bsexy\b|\bsensual\b|\bromance novel\b|\bporn(o|y|ography)?\b|"
    r"\bbdsm\b|\bfetish\b|\bdominat(ion|rix|ed|ing)\b|\bsubmissive\b|"
    r"\bsissy\s+(maid|training|slut|boy)\b|\bshemale\b|\bcock\s+worship\b|"
    r"\bseductive nights?\b|\bthe token\b",
    re.I,
)


# Hip-hop / music review detector.
HIPHOP_MARKERS = re.compile(
    r"\b(hip[ \-]?hop|rap(per|ping)?|emcee|m\.?c\.?|verse|bars|beats?|"
    r"tupac|scarface|wu[ \-]?tang|biggie|eminem|kanye|dr\.?\s*dre|"
    r"snoop|kendrick|nas\b|ice[ \-]?cube|gangsta|album|track|feat\.|featuring)\b",
    re.I,
)


# Proper-noun self-censor detector. `N****** P****` style. when multiple
# adjacent tokens are heavily asterisk-censored, they're almost always
# self-censored proper nouns, not a slur deployed against a group.
MULTI_CENSOR = re.compile(r"\b\w\*{3,}\s+\w\*{3,}", re.I)
SINGLE_CAP_CENSOR = re.compile(r"\b[A-Z]\*{4,}\b")  # "N******" stand-alone


# Consumer deploy signals that already exist in rescore_vulgar.
COMPLAINT_RX = re.compile(
    r"\bwaste of (money|time|\$)\b|"
    r"\bpiece of (shit|crap|garbage|junk|sh\*t)\b|"
    r"\bdo(n'?t| not) (buy|bother|waste|purchase)\b|"
    r"\brefund\b|\breturn(ed|ing)?\b|\bmoney back\b|"
    r"\bbroken\b|\bbroke\b|\bdoesn'?t work\b|\bdid not work\b|"
    r"\bstopped working\b|\bfell apart\b|"
    r"\bpissed off\b|\bfurious\b|\blivid\b|"
    r"\brip[ \-]?off\b|\bscam(med)?\b|"
    r"\bworst (purchase|product|thing|seller|experience) (ever|of my)\b|"
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


# Category → render-group used by the UI.
CATEGORY_LABEL = {
    "RS_HARD": "RACIAL SLUR",
    "RS":      "RACIAL SLUR",
    "HOM":     "HOMOPHOBIC SLUR",
    "ABL":     "ABLEIST SLUR",
    "SEX":     "GENDERED SLUR",
    "XEN":     "XENOPHOBIC SLUR",
    "VULG":    "PROFANITY",
}


CATEGORY_WEIGHT = {
    "RS_HARD": 55.0,
    "RS":      25.0,
    "HOM":     20.0,
    "ABL":      8.0,
    "SEX":      5.0,
    "XEN":     10.0,
    "VULG":     1.0,
}


# Hard VULG tokens that justify inclusion on their own. "Pissed"/"piss off"
# and similar milder stems do NOT qualify. we need a real f-bomb-grade word.
HARD_VULG_ROOTS = {
    "fuck", "fucker", "fuckers", "fucking", "fuckin", "fucked",
    "shit", "shits", "shitty", "shithead", "shithole", "bullshit",
    "bitch", "bitches", "bitchy", "bitching",
    "cunt", "cunts", "cunty",
    "asshole", "assholes",
    "cock", "cocks", "cocksucker",
    "dick", "dicks", "dickhead",
    "pussy", "pussies",
    "whore", "whores",
    "slut", "sluts", "slutty",
    "bastard", "bastards",
    "motherfucker", "motherfuckers", "motherfucking",
    "prick", "pricks",
    # Censored-variant display keys from hate_lexicon.CENSORED_PATTERNS.
    "fuck*", "shit*", "bitch*", "cunt*", "dick*", "cock*",
    "asshole*", "pussy*", "whore*", "slut*", "bastard*", "prick*",
    "motherfucker*",
}


def _apply_name_traps(text: str, categories: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    """Remove hits whose root has a name-trap match in the text."""
    if not categories:
        return categories
    cleaned: Dict[str, Dict[str, int]] = {}
    for cat, words in categories.items():
        kept: Dict[str, int] = {}
        for w, n in words.items():
            root = w.lower()
            trap = ALL_NAME_TRAPS_RX.get(root) or ALL_NAME_TRAPS_RX.get(root.rstrip("*"))
            if trap and trap.search(text):
                continue
            kept[w] = n
        if kept:
            cleaned[cat] = kept
    return cleaned


def _filter_ambiguous(text: str, categories: Dict[str, Dict[str, int]], ctx: str) -> Dict[str, Dict[str, int]]:
    """Ambiguous roots only count when supported by corroborating signal:
    (a) a high-confidence slur hit in the same review, or
    (b) explicit hate / race / identity context in the text.
    Deploy context alone is NOT enough. "worst product ever" tells us the
    person is angry, not that they're using a slur-as-slur."""
    if not categories:
        return categories
    has_high_conf = any(
        w.lower() in HIGH_CONFIDENCE_ROOTS
        for words in categories.values() for w in words
    )
    has_explicit_ctx = bool(EXPLICIT_HATE_CONTEXT.search(text))
    allow_ambiguous = has_high_conf or has_explicit_ctx

    cleaned: Dict[str, Dict[str, int]] = {}
    for cat, words in categories.items():
        kept: Dict[str, int] = {}
        for w, n in words.items():
            lw = w.lower()
            is_ambig = lw in AMBIGUOUS_ROOTS or lw.rstrip("*") in AMBIGUOUS_ROOTS
            if is_ambig and not allow_ambiguous:
                continue
            kept[w] = n
        if kept:
            cleaned[cat] = kept
    return cleaned


def _strip_company_censor(text: str, categories: Dict[str, Dict[str, int]]) -> Dict[str, Dict[str, int]]:
    """If the text contains adjacent heavily-censored tokens (= self-censored
    company / person name), drop ALL heavy-asterisk hits. `N****** P****`
    style censoring produces spurious matches across VULG + slur categories
    alike. none of them are real slurs against a group."""
    if not categories:
        return categories
    multi = bool(MULTI_CENSOR.search(text))
    single_caps = SINGLE_CAP_CENSOR.findall(text)
    if not multi and len(single_caps) < 2:
        return categories
    cleaned: Dict[str, Dict[str, int]] = {}
    for cat, words in categories.items():
        kept: Dict[str, int] = {}
        for w, n in words.items():
            if w.endswith("*"):
                continue
            kept[w] = n
        if kept:
            cleaned[cat] = kept
    return cleaned


def _rescore(row: Dict[str, Any]) -> float:
    text = ((row.get("title") or "") + "  " + (row.get("text") or "")).strip()
    if not text:
        return 0.0
    sc = row.get("score") or {}
    cats = sc.get("categories") or {}
    ctx = sc.get("context") or "ambiguous"
    category = row.get("category") or ""
    rating = float(row.get("rating") or 0)
    word_count = int(sc.get("word_count") or 0)

    # Pass 1: kill proper-noun traps, self-censored company names,
    # per-category noise roots, and ambiguous roots lacking corroboration.
    cats = _apply_name_traps(text, cats)
    cats = _strip_company_censor(text, cats)
    block = CATEGORY_ROOT_BLOCKLIST.get(category, set())
    if block:
        cleaned: Dict[str, Dict[str, int]] = {}
        for cat, words in cats.items():
            kept = {w: n for w, n in words.items() if w.lower() not in block}
            if kept:
                cleaned[cat] = kept
        cats = cleaned
    cats = _filter_ambiguous(text, cats, ctx)
    if not cats:
        return 0.0

    # Require at least one HARD signal: a real f-bomb-grade VULG token, OR a
    # slur-category hit. "pissed off" alone doesn't cut it for worst-of-worst.
    has_hard_vulg = any(
        w.lower() in HARD_VULG_ROOTS
        for w in cats.get("VULG", {}).keys()
    )
    has_slur = any(cat != "VULG" for cat in cats.keys())
    if not (has_hard_vulg or has_slur):
        return 0.0

    # Pass 2: weighted severity per category.
    severity = 0.0
    for cat, d in cats.items():
        cat_hits = sum(d.values())
        unique = len(d)
        w = CATEGORY_WEIGHT.get(cat, 1.0)
        severity += w * (cat_hits + 0.5 * (unique - 1))

    # Pass 3: context gating. Deploy boosts, quote/reclaim/erotica penalise.
    ctx_mult = {"deploy": 1.5, "quote_crit": 0.3, "reclaim": 0.15, "ambiguous": 0.85}[ctx]
    severity *= ctx_mult

    # Pass 4: category gating.
    if category in FICTION_CATS:
        if ctx != "deploy":
            severity *= 0.25
        else:
            severity *= 0.6
        # Fiction categories swim in "retarded" / "moron" / "idiot" used to
        # describe characters, plots, films. heavily down-weight ABL-only
        # fiction hits.
        if list(cats.keys()) == ["ABL"]:
            severity *= 0.2
    elif category in PHYSICAL_CATS:
        severity += 4.0
        severity *= 1.4

    # Pass 5: erotica / hip-hop penalisation.
    if EROTICA_MARKERS.search(text):
        severity *= 0.30
    if HIPHOP_MARKERS.search(text) and ("RS_HARD" in cats or "RS" in cats):
        severity *= 0.35

    # Pass 6: consumer-complaint boosts.
    complaint_hits = len(COMPLAINT_RX.findall(text))
    severity += complaint_hits * 1.4
    if HARD_COMPLAINT_RX.search(text):
        severity += 5.0

    # Pass 7: rating & energy.
    # 1★ "WORST PURCHASE EVER" rants are the gold. 5★ reviews dropping f-bombs
    # as praise ("this s*** is fire!") aren't the mood we're going for here.
    if rating and rating == 1:
        severity += 8.0
        severity *= 1.15
    elif rating and rating == 2:
        severity += 3.0
    elif rating and rating == 4:
        severity *= 0.55
    elif rating and rating == 5:
        severity *= 0.35

    severity += float(sc.get("caps_ratio") or 0) * 10.0
    severity += min(int(sc.get("exclam_count") or 0), 20) * 0.15

    # Pass 8: length sanity.
    if word_count and word_count < 10:
        severity *= 0.5
    elif word_count > 600:
        severity *= 0.8

    return round(severity, 3)


def main() -> None:
    src = Path(__file__).parent / "samples" / "ard_worst.json"
    d = json.load(open(src))
    rows: List[Dict[str, Any]] = d["global_top"]

    ranked: List[Dict[str, Any]] = []
    for r in rows:
        s = _rescore(r)
        if s <= 0:
            continue
        ranked.append({**r, "_rescore": s})
    ranked.sort(key=lambda r: -r["_rescore"])

    top_search = ranked[:400]
    wall = ranked[:50]

    out_ranked = Path(__file__).parent / "samples" / "ard_worst_ranked.json"
    out_ranked.write_text(json.dumps({
        "total_reviews_parsed": d["total_reviews_parsed"],
        "total_hits": d["total_hits"],
        "hits_per_million": d["hits_per_million"],
        "kept": len(top_search),
        "rows": top_search,
    }, indent=2))

    out_wall = Path(__file__).parent / "samples" / "ard_worst_wall.json"
    out_wall.write_text(json.dumps({
        "blurb": (
            f"The worst of {d['total_reviews_parsed']:,} Amazon reviews: including "
            f"slurs, censored profanity, and unhinged rants the first two passes missed."
        ),
        "rows": wall,
    }, indent=2))

    print(f"ranked: {len(ranked)} / {len(rows)} survived filters")
    print(f"wrote ard_worst_ranked.json ({len(top_search)} rows)")
    print(f"wrote ard_worst_wall.json ({len(wall)} rows)")
    print()
    print("=== TOP 20 AFTER RESCORE ===")
    for i, r in enumerate(ranked[:20], 1):
        sc = r.get("score") or {}
        cats = sc.get("categories") or {}
        text_full = ((r.get("title") or "") + "  " + (r.get("text") or ""))
        ctx = sc.get("context", "?")
        cat = r.get("category", "")
        cats = _apply_name_traps(text_full, cats)
        cats = _strip_company_censor(text_full, cats)
        block = CATEGORY_ROOT_BLOCKLIST.get(cat, set())
        if block:
            cats = {
                c: {w: n for w, n in words.items() if w.lower() not in block}
                for c, words in cats.items()
            }
            cats = {c: d for c, d in cats.items() if d}
        cats = _filter_ambiguous(text_full, cats, ctx)
        title = (r.get("title") or "").strip()[:70]
        text = (r.get("text") or "").strip().replace("\n", " ")[:140]
        rating = r.get("rating") or "?"
        print(f'{i:2}. [{cat}] {rating}★ ctx={ctx} score={r["_rescore"]:.1f}  cats={cats}')
        print(f"    title: {title}")
        print(f"    text:  {text}")
        print()


if __name__ == "__main__":
    main()
