"""Categorized hate / slur lexicon for Amazon Review Distiller Unhinged Mode.

This file exists to support ANALYSIS ONLY of hate speech in a 571M-review
Amazon corpus. Terms are categorized by target type so the pipeline can
produce aggregate statistics and tag context (deploy / quote / reclaim /
ambiguous). Every term is held in lowercase. Public UI ALWAYS renders these
redacted with a category badge — the raw strings never ship verbatim to
burla-cloud.github.io.

Data sources referenced when building these lists:
  - Hurtlex (University of Bologna, MIT-licensed): category taxonomy
  - LDNOOBW (Shutterstock, MIT-licensed): cross-reference completeness
  - HateBase public dictionary: severity tiers

Categories:
  RS  = racial slur (RS_HARD = single worst-tier)
  XEN = xenophobic / ethnic / national slur
  HOM = homophobic / transphobic slur
  ABL = ableist slur
  SEX = sexist / misogynistic slur beyond the baseline profanity set

Regex families below handle the self-censoring that dominates Amazon text
(f***, sh!t, n-word, f@g, etc.). This is the single biggest signal we were
missing in the first pass.
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple


# -------------------------------------------------------------------------
# Category: RS_HARD — the hard-R racial slur tier.
# Render: [RACIAL SLUR] badge, every char after first → `*`.
# -------------------------------------------------------------------------
RS_HARD: List[str] = [
    "nigger", "niggers", "nigga", "niggas",
    "niggah", "niggahs", "niggaz",
]

# -------------------------------------------------------------------------
# Category: RS — other racial slurs (anti-Asian, anti-Black beyond hard-R,
# anti-Indigenous, anti-Hispanic, anti-Arab, anti-Jewish).
# -------------------------------------------------------------------------
RS: List[str] = [
    "chink", "chinks", "gook", "gooks", "jap", "japs",
    "slant", "slants", "slanteye", "slopehead",
    "spic", "spics", "wetback", "wetbacks", "beaner", "beaners",
    "redskin", "redskins", "injun", "injuns",
    "towelhead", "towelheads", "sandnigger", "raghead", "ragheads",
    "kike", "kikes", "yid", "yids", "hebe", "hebes",
    "coon", "coons", "jigaboo", "jigaboos", "porchmonkey",
    "pickaninny", "mulatto", "quadroon", "octoroon",
    "wog", "wogs", "paki", "pakis",
    "gringo", "gringos",
    "cracker", "crackers",   # ambiguous: often product; rescore context-check
    "honky", "honkies",
]

# -------------------------------------------------------------------------
# Category: HOM — homophobic / transphobic slurs.
# -------------------------------------------------------------------------
HOM: List[str] = [
    "fag", "fags", "faggot", "faggots", "faggy", "fagged",
    "dyke", "dykes",
    "tranny", "trannies", "shemale", "shemales",
    "homo", "homos",        # ambiguous: can be technical; rescore
    "queer", "queers",      # ambiguous: reclaimed; rescore
    "poof", "poofs", "poofter", "poofters",
    "sissy", "sissies",
    "pansy", "pansies",     # ambiguous: flower
]

# -------------------------------------------------------------------------
# Category: ABL — ableist slurs.
# -------------------------------------------------------------------------
ABL: List[str] = [
    "retard", "retards", "retarded", "retardo", "retardation",
    "spastic", "spaz", "spazz", "spazzed",
    "mongoloid", "mongoloids",
    "cripple", "crippled",
    "lame", "lamer",        # extremely ambiguous; rescore
    "dumbo",
    "idiot", "moron", "imbecile",  # historical slur tier; rescore hard
]

# -------------------------------------------------------------------------
# Category: SEX — gendered slurs beyond the baseline bitch/whore/slut set.
# -------------------------------------------------------------------------
SEX: List[str] = [
    "skank", "skanks", "skanky",
    "tramp", "tramps",
    "hussy", "hussies",
    "harlot", "harlots",
    "thot", "thots",
    "slag", "slags",
    "hoe", "hoes", "hoes",
    "cuck", "cucks", "cuckold",
    "simp", "simps",        # modern internet insult
    "incel", "incels",
]

# -------------------------------------------------------------------------
# Category: XEN — xenophobic / national origin slurs.
# -------------------------------------------------------------------------
XEN: List[str] = [
    "dago", "dagos",
    "wop", "wops",
    "polack", "polacks",
    "kraut", "krauts",
    "limey", "limeys",
    "mick", "micks",
    "chinaman", "chinamen",
    "oriental", "orientals",  # dated; rescore
    "gypsy", "gypsies",       # ambiguous; rescore
]


CATEGORIES: Dict[str, List[str]] = {
    "RS_HARD": RS_HARD,
    "RS":      RS,
    "HOM":     HOM,
    "ABL":     ABL,
    "SEX":     SEX,
    "XEN":     XEN,
}

# Severity weights per category (higher = worse). Drives scoring.
CATEGORY_WEIGHT: Dict[str, float] = {
    "RS_HARD": 60.0,
    "RS":      35.0,
    "HOM":     30.0,
    "ABL":     12.0,
    "SEX":      6.0,
    "XEN":     15.0,
}

# Flat word → category lookup (exact-match pass).
WORD_TO_CAT: Dict[str, str] = {}
for _cat, _words in CATEGORIES.items():
    for _w in _words:
        WORD_TO_CAT[_w] = _cat


# -------------------------------------------------------------------------
# Censored-variant regex families.
# Amazon reviewers self-censor heavily: f***, sh!t, b*tch, n****r, f@g.
# The original word-list approach missed ALL of these. These regexes catch
# them even when the middle characters are asterisks / symbols / digits.
#
# Each entry: (category, display_key, pattern).
# display_key is the "root" we normalize to (for grouping in scoring).
# -------------------------------------------------------------------------

def _sym_class(n_min: int, n_max: int) -> str:
    """Regex char class matching n_min..n_max symbol-substitution chars."""
    return r"[\*\@\#\$\%\&\!\_\-\.\+\=\/\^\|\~\?\:\;1-9]{" + f"{n_min},{n_max}" + "}"


# Pure-asterisk pattern class (only `*`, the overwhelmingly most common
# Amazon censoring char — think `f***ing`, `sh**`, `n*****`, `b****`).
def _stars(n_min: int, n_max: int) -> str:
    return r"\*{" + f"{n_min},{n_max}" + "}"


# Build for common profane roots that the original tokenizer missed.
# These run IN ADDITION to the exact-word lookup from hunt_vulgar.HARD_ROOTS.
#
# Two families per root where it matters:
#   (a) partial censor: anchor letters preserved (f*ck, sh!t, b*tch)
#   (b) heavy censor: asterisks replace middle + tail (f***, f***ing, sh**)
CENSORED_PATTERNS: List[Tuple[str, str, re.Pattern]] = [
    # Classic profanity — partial-letter censored/leet variants.
    ("VULG", "fuck",    re.compile(r"\bf" + _sym_class(1, 3) + r"k(?:ing|in|ed|er|ers|s)?\b", re.I)),
    # "f*ck" / "f*cking" — one sym replaces the u, ck preserved.
    ("VULG", "fuck",    re.compile(r"\bf" + _sym_class(1, 1) + r"ck(?:ing|in|ed|er|ers|s)?\b", re.I)),
    ("VULG", "shit",    re.compile(r"\bsh" + _sym_class(1, 2) + r"t(?:ty|s|ted|ting|head|hole|bag)?\b", re.I)),
    ("VULG", "bitch",   re.compile(r"\bb" + _sym_class(1, 3) + r"tch(?:es|y|in|ing|ed)?\b", re.I)),
    ("VULG", "cunt",    re.compile(r"\bc" + _sym_class(1, 3) + r"nt(?:s|y)?\b", re.I)),
    ("VULG", "dick",    re.compile(r"\bd" + _sym_class(1, 2) + r"ck(?:head|heads|s|wad)?\b", re.I)),
    ("VULG", "cock",    re.compile(r"\bc" + _sym_class(1, 2) + r"ck(?:sucker|suckers|s)?\b", re.I)),
    ("VULG", "pussy",   re.compile(r"\bp" + _sym_class(1, 3) + r"s(?:s)?y\b", re.I)),
    ("VULG", "asshole", re.compile(r"\ba" + _sym_class(1, 2) + r"s(?:h" + _sym_class(1, 2) + r"le|holes?)\b", re.I)),
    ("VULG", "asshole", re.compile(r"\ba" + _sym_class(1, 3) + r"hole(?:s)?\b", re.I)),   # a**hole
    ("VULG", "whore",   re.compile(r"\bwh" + _sym_class(1, 3) + r"re(?:s)?\b", re.I)),
    ("VULG", "slut",    re.compile(r"\bsl" + _sym_class(1, 2) + r"t(?:s|ty)?\b", re.I)),
    ("VULG", "bastard", re.compile(r"\bb" + _sym_class(1, 3) + r"stard(?:s)?\b", re.I)),
    ("VULG", "piss",    re.compile(r"\bp" + _sym_class(1, 2) + r"ss(?:ed|ing|er|y|off)?\b", re.I)),
    ("VULG", "motherfucker", re.compile(r"\bm(?:other|utha)[ \-]?f" + _sym_class(1, 3) + r"k(?:er|ers|ing|in)?\b", re.I)),

    # Heavy-censor fallback: asterisks replace the middle AND tail
    # (Amazon's overwhelming default). Anchored on first letter + length.
    # Trailing (?!\w) instead of \b because `*` is non-word and \b won't
    # fire at a * → space transition.
    ("VULG", "fuck",     re.compile(r"\bf" + _stars(2, 4) + r"(?:ing|in|ed|er|ers|s)?(?!\w)", re.I)),
    ("VULG", "shit",     re.compile(r"\bsh" + _stars(2, 3) + r"(?:ty|s|ted|head|hole|bag)?(?!\w)", re.I)),
    ("VULG", "bitch",    re.compile(r"\bb" + _stars(3, 5) + r"(?:es|y)?(?!\w)", re.I)),
    ("VULG", "cunt",     re.compile(r"\bc" + _stars(3, 4) + r"(?:s|y)?(?!\w)", re.I)),
    ("VULG", "dick",     re.compile(r"\bd" + _stars(2, 3) + r"(?:s|head)?(?!\w)", re.I)),
    ("VULG", "cock",     re.compile(r"\bc" + _stars(2, 3) + r"(?:s|sucker)?(?!\w)", re.I)),
    ("VULG", "pussy",    re.compile(r"\bp" + _stars(3, 4) + r"y?(?!\w)", re.I)),
    ("VULG", "asshole",  re.compile(r"\ba" + _stars(5, 6) + r"(?!\w)", re.I)),
    ("VULG", "whore",    re.compile(r"\bwh" + _stars(2, 4) + r"(?!\w)", re.I)),
    ("VULG", "slut",     re.compile(r"\bsl" + _stars(2, 3) + r"(?!\w)", re.I)),
    ("VULG", "bastard",  re.compile(r"\bb" + _stars(4, 6) + r"(?!\w)", re.I)),
    ("VULG", "prick",    re.compile(r"\bpr" + _sym_class(1, 2) + r"ck(?:s)?\b", re.I)),
    ("VULG", "prick",    re.compile(r"\bpr" + _stars(2, 3) + r"(?:s)?(?!\w)", re.I)),

    # Slur censored variants — mixed (partial letters preserved).
    # Require >=2 symbol chars AND multi-char suffix to avoid "N.a"/"N-A"
    # (Bible abbreviations, "not applicable", etc.) false positives.
    ("RS_HARD", "nigger", re.compile(r"\bn" + _sym_class(2, 4) + r"(?:er|ers|ah|ahs|az|as)\b", re.I)),
    ("RS",      "chink",  re.compile(r"\bch" + _sym_class(1, 2) + r"nk(?:s|y)?\b", re.I)),
    ("RS",      "spic",   re.compile(r"\bsp" + _sym_class(1, 2) + r"c(?:s)?\b", re.I)),
    ("RS",      "kike",   re.compile(r"\bk" + _sym_class(1, 2) + r"ke(?:s)?\b", re.I)),
    ("RS",      "gook",   re.compile(r"\bg" + _sym_class(1, 2) + r"ok(?:s)?\b", re.I)),
    ("RS",      "coon",   re.compile(r"\bc" + _sym_class(1, 2) + r"on(?:s)?\b", re.I)),
    ("HOM",     "fag",    re.compile(r"\bf" + _sym_class(1, 2) + r"g(?:g?ot|s|gy|got)?\b", re.I)),
    ("HOM",     "dyke",   re.compile(r"\bd" + _sym_class(1, 2) + r"ke(?:s)?\b", re.I)),
    ("HOM",     "tranny", re.compile(r"\btr" + _sym_class(1, 2) + r"nn(?:y|ies)\b", re.I)),
    ("ABL",     "retard", re.compile(r"\br" + _sym_class(1, 3) + r"t" + _sym_class(0, 2) + r"rd(?:ed|s|ation)?\b", re.I)),

    # Heavy-censor slur fallbacks. Require an explicit tail so we don't
    # double-count `f***` as both fuck AND fag, or match every `n*****`
    # (censored company name) as the n-word.
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(3, 6) + r"(?:er|ah|az)(?!\w)", re.I)),
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(3, 6) + r"a(?!\w)", re.I)),   # n***a, n****a (common)
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(3, 5) + r"r(?!\w)", re.I)),   # n***r, n****r
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(5, 6) + r"(?!\w)", re.I)),    # n***** alone
    ("HOM",     "fag",    re.compile(r"\bf" + _stars(2, 5) + r"(?:got|gy)(?!\w)", re.I)),
    ("HOM",     "fag",    re.compile(r"\bf" + _stars(1, 2) + r"gg?(?:ot|y)(?!\w)", re.I)),  # f*ggot
    ("ABL",     "retard", re.compile(r"\br" + _stars(3, 6) + r"(?:d?ed|s|ation)(?!\w)", re.I)),
]


# -------------------------------------------------------------------------
# Context detection — deploy / quote-and-criticize / reclaim / ambiguous.
# Used by the rescorer to boost genuine-deploy hits and down-weight the rest.
# -------------------------------------------------------------------------

QUOTE_CRIT_PHRASES = [
    r"\bshocked\b", r"\bdisgusted\b", r"\bappalled\b", r"\boffended\b",
    r"\bthe n[ \-]?word\b", r"\bthe f[ \-]?word\b", r"\bthe r[ \-]?word\b",
    r"\bshould (not|be banned|be removed|be pulled)\b",
    r"\bracist\b", r"\bracism\b", r"\bhomophobic\b", r"\bhomophobia\b",
    r"\btransphobic\b", r"\bxenophobic\b", r"\bableist\b", r"\bmisogynist",
    r"\btrigger warning\b", r"\bcontent warning\b",
    r"\buses? the word\b", r"\bdrops? the\b",
    r"\bslur\b", r"\bslurs\b",
    r"\bcannot believe\b", r"\bcan'?t believe\b",
    r"\bhow (is|are) this\b", r"\bno place\b",
    r"\b(book|movie|film|show|author|writer) (uses?|contains?|has|drops)\b",
    r"\bwould not recommend\b",
]
QUOTE_CRIT_RX = re.compile("|".join(QUOTE_CRIT_PHRASES), re.I)


DEPLOY_PHRASES = [
    r"\byou (fucking |f\S* )?(piece|idiot|moron|retard|loser)\b",
    r"\bgo (fuck|kill|die) (yourself|urself)\b",
    r"\bi hate (them|those|you|amazon|the)\b",
    r"\bhope (you|they|he|she|it) (dies?|burns?|rots?)\b",
    r"\b(fuck|screw|damn) (you|them|this|amazon|seller)\b",
    r"\bworst (product|purchase|seller|experience|thing) (ever|of my)\b",
    r"\bnever (again|buying|order)\b",
    r"\bpiece of (shit|crap|garbage|junk)\b",
    r"\bdo not (buy|bother|waste|purchase)\b",
    r"\bscam\b", r"\brip[ \-]?off\b",
]
DEPLOY_RX = re.compile("|".join(DEPLOY_PHRASES), re.I)


RECLAIM_MARKERS = [
    r"\bmy (nigg|homie|bro)\b",
    r"\bfellow\b",
    r"\bas a (black|gay|queer|trans)\b",
    r"\blyrics?\b", r"\bsong\b", r"\brap(per|ping)?\b",
]
RECLAIM_RX = re.compile("|".join(RECLAIM_MARKERS), re.I)


def classify_context(text: str) -> str:
    """Return one of: 'deploy', 'quote_crit', 'reclaim', 'ambiguous'."""
    if not text:
        return "ambiguous"
    t = text[:4000]
    has_quote = bool(QUOTE_CRIT_RX.search(t))
    has_deploy = bool(DEPLOY_RX.search(t))
    has_reclaim = bool(RECLAIM_RX.search(t))

    if has_deploy and not has_quote:
        return "deploy"
    if has_quote and not has_deploy:
        return "quote_crit"
    if has_reclaim and not (has_quote or has_deploy):
        return "reclaim"
    if has_deploy and has_quote:
        return "deploy"  # mixed — treat as deploy
    return "ambiguous"


CATEGORY_LABELS: Dict[str, str] = {
    "RS_HARD": "RACIAL SLUR",
    "RS":      "RACIAL SLUR",
    "HOM":     "HOMOPHOBIC SLUR",
    "ABL":     "ABLEIST SLUR",
    "SEX":     "GENDERED SLUR",
    "XEN":     "XENOPHOBIC SLUR",
    "VULG":    "PROFANITY",
}
