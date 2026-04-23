"""All word lists, regex patterns, and context helpers used to score a
review. Kept in one place so the pipeline file stays about flow, not data.

Tiers
-----
STRONG / MEDIUM / MILD      generic English profanity (drives the main Wall).
HARD_ROOTS                  strong roots grouped for "variety" scoring.
CATEGORIES                  racial, homophobic, ableist, gendered, xenophobic
                            slurs. drives Unhinged Mode.
CENSORED_PATTERNS           regexes that catch f***, sh!t, b*tch, n****r, ...
                            (Amazon reviewers self-censor, so these carry
                            most of the hard-language signal.)

Public UI never ships the raw strings. Slurs are always rendered with a
category badge and the middle chars blanked.
"""
from __future__ import annotations

import re
from typing import Dict, List, Set, Tuple


# ---------------------------------------------------------------------------
# Tier 1/2/3 profanity. scored as whole tokens.
# ---------------------------------------------------------------------------
STRONG_PROFANE: Set[str] = {
    "fuck", "fucks", "fucked", "fucking", "fucker", "fuckers", "fuckin",
    "motherfucker", "motherfuckers", "motherfucking",
    "shit", "shits", "shitty", "shittier", "shittiest", "shitshow", "shithole",
    "bitch", "bitches", "bitching", "bitchy",
    "asshole", "assholes", "asshat", "dumbass", "jackass",
    "cunt", "cunts", "bastard", "bastards",
    "dick", "dicks", "dickhead", "dickheads",
    "cock", "cocks", "cocksucker", "pussy", "pussies",
    "whore", "whores", "whorish", "piss", "pissed", "pissing", "pissoff",
    "crap", "craptastic", "crappy",
}
MEDIUM_PROFANE: Set[str] = {
    "damn", "damned", "damnit", "goddamn", "goddamnit",
    "hell", "hellish", "bullshit", "horseshit",
    "wtf", "stfu", "fubar",
    "douche", "douchebag", "douchy",
    "moron", "morons", "moronic", "idiot", "idiots", "idiotic",
    "retard", "retarded", "retards",
    "garbage", "rubbish", "trash",
    "screwed", "screwing",
}
MILD_PROFANE: Set[str] = {
    "suck", "sucked", "sucks", "sucky", "sucking", "sucker", "suckers",
    "stupid", "stupidity", "lame", "lamely",
    "terrible", "horrible", "awful", "horrid",
    "worst", "hate", "hated", "hates", "hating", "hatred",
    "pathetic", "useless", "worthless",
}

WORD_RX = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")
EXCLAM_RX = re.compile(r"!+")


# ---------------------------------------------------------------------------
# Hard-profanity roots, grouped. Used in variety scoring: a review hitting
# three distinct roots (fuck + shit + bitch) scores much higher than one
# that drops 40 f-bombs.
# ---------------------------------------------------------------------------
HARD_ROOTS: Dict[str, List[str]] = {
    "fuck":    ["fuck", "fucks", "fucked", "fucking", "fucker", "fuckers",
                "fuckin", "fuckery", "motherfuck", "motherfucker",
                "motherfuckers", "motherfucking", "muthafucka", "muthafucker"],
    "shit":    ["shit", "shits", "shitty", "shittier", "shittiest", "shitshow",
                "shithole", "bullshit", "horseshit", "batshit",
                "shithead", "shitheads"],
    "bitch":   ["bitch", "bitches", "bitchy", "bitching", "sonofabitch"],
    "cunt":    ["cunt", "cunts", "cunty"],
    "whore":   ["whore", "whores", "whorish", "whoring"],
    "slut":    ["slut", "sluts", "slutty"],
    "asshole": ["asshole", "assholes", "asshat", "dumbass", "jackass"],
    "dick":    ["dick", "dicks", "dickhead", "dickheads"],
    "cock":    ["cock", "cocks", "cocksucker"],
    "pussy":   ["pussy", "pussies"],
    "twat":    ["twat", "twats", "twatty"],
    "douche":  ["douche", "douches", "douchebag", "douchebags"],
    "bastard": ["bastard", "bastards"],
    "prick":   ["prick", "pricks"],
    "piss":    ["piss", "pissed", "pissing", "pissoff"],
}
HARD_WORDS: Dict[str, str] = {
    w: root for root, variants in HARD_ROOTS.items() for w in variants
}


# ---------------------------------------------------------------------------
# Slur lexicon, categorized. Raw strings never ship to the client.
# ---------------------------------------------------------------------------
RS_HARD = ["nigger", "niggers", "nigga", "niggas", "niggah", "niggahs", "niggaz"]
RS = [
    "chink", "chinks", "gook", "gooks", "jap", "japs",
    "slant", "slants", "slanteye", "slopehead",
    "spic", "spics", "wetback", "wetbacks", "beaner", "beaners",
    "redskin", "redskins", "injun", "injuns",
    "towelhead", "towelheads", "sandnigger", "raghead", "ragheads",
    "kike", "kikes", "yid", "yids", "hebe", "hebes",
    "coon", "coons", "jigaboo", "jigaboos", "porchmonkey",
    "pickaninny", "mulatto", "quadroon", "octoroon",
    "wog", "wogs", "paki", "pakis", "gringo", "gringos",
    "cracker", "crackers", "honky", "honkies",
]
HOM = [
    "fag", "fags", "faggot", "faggots", "faggy", "fagged",
    "dyke", "dykes", "tranny", "trannies", "shemale", "shemales",
    "homo", "homos", "queer", "queers",
    "poof", "poofs", "poofter", "poofters",
    "sissy", "sissies", "pansy", "pansies",
]
ABL = [
    "retard", "retards", "retarded", "retardo", "retardation",
    "spastic", "spaz", "spazz", "spazzed",
    "mongoloid", "mongoloids", "cripple", "crippled",
    "lame", "lamer", "dumbo", "idiot", "moron", "imbecile",
]
SEX = [
    "skank", "skanks", "skanky", "tramp", "tramps",
    "hussy", "hussies", "harlot", "harlots",
    "thot", "thots", "slag", "slags",
    "hoe", "hoes", "cuck", "cucks", "cuckold",
    "simp", "simps", "incel", "incels",
]
XEN = [
    "dago", "dagos", "wop", "wops", "polack", "polacks",
    "kraut", "krauts", "limey", "limeys",
    "mick", "micks", "chinaman", "chinamen",
    "oriental", "orientals", "gypsy", "gypsies",
]

CATEGORIES: Dict[str, List[str]] = {
    "RS_HARD": RS_HARD, "RS": RS, "HOM": HOM,
    "ABL": ABL, "SEX": SEX, "XEN": XEN,
}
CATEGORY_WEIGHT: Dict[str, float] = {
    "RS_HARD": 60.0, "RS": 35.0, "HOM": 30.0,
    "ABL": 12.0, "SEX": 6.0, "XEN": 15.0, "VULG": 1.0,
}
CATEGORY_LABELS: Dict[str, str] = {
    "RS_HARD": "RACIAL SLUR", "RS": "RACIAL SLUR",
    "HOM": "HOMOPHOBIC SLUR", "ABL": "ABLEIST SLUR",
    "SEX": "GENDERED SLUR", "XEN": "XENOPHOBIC SLUR",
    "VULG": "PROFANITY",
}
WORD_TO_CAT: Dict[str, str] = {
    w: cat for cat, words in CATEGORIES.items() for w in words
}


# ---------------------------------------------------------------------------
# Censored-variant regexes. The original word-list tokenizer missed every
# `f***`, `sh!t`, `n****r`, etc. These catch them. Each entry is
# (category, display_root, compiled regex).
# ---------------------------------------------------------------------------
def _sym(n_min: int, n_max: int) -> str:
    return r"[\*\@\#\$\%\&\!\_\-\.\+\=\/\^\|\~\?\:\;1-9]{" + f"{n_min},{n_max}" + "}"


def _stars(n_min: int, n_max: int) -> str:
    return r"\*{" + f"{n_min},{n_max}" + "}"


CENSORED_PATTERNS: List[Tuple[str, str, re.Pattern]] = [
    # Profanity. partial-letter censoring (f*ck, sh!t, b*tch).
    ("VULG", "fuck",    re.compile(r"\bf" + _sym(1, 3) + r"k(?:ing|in|ed|er|ers|s)?\b", re.I)),
    ("VULG", "fuck",    re.compile(r"\bf" + _sym(1, 1) + r"ck(?:ing|in|ed|er|ers|s)?\b", re.I)),
    ("VULG", "shit",    re.compile(r"\bsh" + _sym(1, 2) + r"t(?:ty|s|ted|ting|head|hole|bag)?\b", re.I)),
    ("VULG", "bitch",   re.compile(r"\bb" + _sym(1, 3) + r"tch(?:es|y|in|ing|ed)?\b", re.I)),
    ("VULG", "cunt",    re.compile(r"\bc" + _sym(1, 3) + r"nt(?:s|y)?\b", re.I)),
    ("VULG", "dick",    re.compile(r"\bd" + _sym(1, 2) + r"ck(?:head|heads|s|wad)?\b", re.I)),
    ("VULG", "cock",    re.compile(r"\bc" + _sym(1, 2) + r"ck(?:sucker|suckers|s)?\b", re.I)),
    ("VULG", "pussy",   re.compile(r"\bp" + _sym(1, 3) + r"s(?:s)?y\b", re.I)),
    ("VULG", "asshole", re.compile(r"\ba" + _sym(1, 3) + r"hole(?:s)?\b", re.I)),
    ("VULG", "whore",   re.compile(r"\bwh" + _sym(1, 3) + r"re(?:s)?\b", re.I)),
    ("VULG", "slut",    re.compile(r"\bsl" + _sym(1, 2) + r"t(?:s|ty)?\b", re.I)),
    ("VULG", "bastard", re.compile(r"\bb" + _sym(1, 3) + r"stard(?:s)?\b", re.I)),
    ("VULG", "prick",   re.compile(r"\bpr" + _sym(1, 2) + r"ck(?:s)?\b", re.I)),
    ("VULG", "piss",    re.compile(r"\bp" + _sym(1, 2) + r"ss(?:ed|ing|er|y|off)?\b", re.I)),
    ("VULG", "motherfucker", re.compile(r"\bm(?:other|utha)[ \-]?f" + _sym(1, 3) + r"k(?:er|ers|ing|in)?\b", re.I)),

    # Heavy-censor fallbacks. asterisks in middle + tail, e.g. `f****ing`, `b*****`.
    ("VULG", "fuck",     re.compile(r"\bf" + _stars(2, 4) + r"(?:ing|in|ed|er|ers|s)?(?!\w)", re.I)),
    ("VULG", "shit",     re.compile(r"\bsh" + _stars(2, 3) + r"(?:ty|s|ted|head|hole|bag)?(?!\w)", re.I)),
    ("VULG", "bitch",    re.compile(r"\bb" + _stars(3, 5) + r"(?:es|y)?(?!\w)", re.I)),
    ("VULG", "cunt",     re.compile(r"\bc" + _stars(3, 4) + r"(?:s|y)?(?!\w)", re.I)),
    ("VULG", "asshole",  re.compile(r"\ba" + _stars(5, 6) + r"(?!\w)", re.I)),

    # Slur censor variants. Require multi-char suffix to avoid N.A/"not applicable".
    ("RS_HARD", "nigger", re.compile(r"\bn" + _sym(2, 4) + r"(?:er|ers|ah|ahs|az|as)\b", re.I)),
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(3, 6) + r"(?:er|ah|az|a|r)(?!\w)", re.I)),
    ("RS_HARD", "nigger", re.compile(r"\bn" + _stars(5, 6) + r"(?!\w)", re.I)),
    ("RS",      "chink",  re.compile(r"\bch" + _sym(1, 2) + r"nk(?:s|y)?\b", re.I)),
    ("RS",      "spic",   re.compile(r"\bsp" + _sym(1, 2) + r"c(?:s)?\b", re.I)),
    ("RS",      "kike",   re.compile(r"\bk" + _sym(1, 2) + r"ke(?:s)?\b", re.I)),
    ("RS",      "coon",   re.compile(r"\bc" + _sym(1, 2) + r"on(?:s)?\b", re.I)),
    ("HOM",     "fag",    re.compile(r"\bf" + _sym(1, 2) + r"g(?:g?ot|s|gy|got)?\b", re.I)),
    ("HOM",     "fag",    re.compile(r"\bf" + _stars(1, 5) + r"(?:got|gy|ots)(?!\w)", re.I)),
    ("HOM",     "dyke",   re.compile(r"\bd" + _sym(1, 2) + r"ke(?:s)?\b", re.I)),
    ("HOM",     "tranny", re.compile(r"\btr" + _sym(1, 2) + r"nn(?:y|ies)\b", re.I)),
    ("ABL",     "retard", re.compile(r"\br" + _sym(1, 3) + r"t" + _sym(0, 2) + r"rd(?:ed|s|ation)?\b", re.I)),
    ("ABL",     "retard", re.compile(r"\br" + _stars(3, 6) + r"(?:d?ed|s|ation)(?!\w)", re.I)),
]


# ---------------------------------------------------------------------------
# Context classification. deploy vs. quote-and-criticize vs. reclaim.
# Lets the rescorer boost real rants and kill literary criticism.
# ---------------------------------------------------------------------------
_QUOTE_CRIT_RX = re.compile(
    r"\b(shocked|disgusted|appalled|offended|racist|racism|homophobic|"
    r"transphobic|xenophobic|ableist|misogynist|slur|slurs|trigger warning|"
    r"content warning|uses? the word|drops? the)\b|"
    r"\bthe [nfr][\-\s]?word\b|"
    r"\b(should be (banned|removed|pulled)|can'?t believe|no place|"
    r"would not recommend)\b",
    re.I,
)
_DEPLOY_RX = re.compile(
    r"\byou (fucking |f\S* )?(piece|idiot|moron|retard|loser)\b|"
    r"\b(fuck|screw|damn) (you|them|this|amazon|seller)\b|"
    r"\bgo (fuck|kill|die) (yourself|urself)\b|"
    r"\bworst (product|purchase|seller|experience|thing) (ever|of my)\b|"
    r"\bnever (again|buying|order)\b|"
    r"\bpiece of (shit|crap|garbage|junk)\b|"
    r"\bdo not (buy|bother|waste|purchase)\b|"
    r"\bscam\b|\brip[ \-]?off\b",
    re.I,
)
_RECLAIM_RX = re.compile(
    r"\bmy (nigg|homie|bro)\b|\bfellow\b|"
    r"\bas a (black|gay|queer|trans)\b|"
    r"\blyrics?\b|\bsong\b|\brap(per|ping)?\b",
    re.I,
)


def classify_context(text: str) -> str:
    """Return 'deploy', 'quote_crit', 'reclaim', or 'ambiguous'."""
    t = (text or "")[:4000]
    q = bool(_QUOTE_CRIT_RX.search(t))
    d = bool(_DEPLOY_RX.search(t))
    r = bool(_RECLAIM_RX.search(t))
    if d and not q:
        return "deploy"
    if q and not d:
        return "quote_crit"
    if r and not (q or d):
        return "reclaim"
    if d and q:
        return "deploy"
    return "ambiguous"
