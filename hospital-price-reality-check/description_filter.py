"""Description-vs-code match filter.

Real hospital MRFs sometimes publish line items where the CPT/HCPCS code is
correct on paper but the chargemaster line is for a completely different
product. The most common failure mode is hospitals tagging surgical hardware
(plates, screws, suture anchors) under a numeric code that happens to be
adjacent to a vaccine or drug code, or hospitals using a J-code as a generic
"unclassified drug or supply" bucket.

This module rejects hospital observations whose representative description
clearly does not describe the code, so the cheapest/priciest podium and the
spread leaderboard always compare the same procedure across hospitals.

Strategy:
    1. Hard hardware exclusion. If the code's category is not surgical /
       orthopedic / cardiovascular and the line item starts with or
       prominently features surgical hardware tokens (PLATE, SCREW, ROD,
       SUTURE ANCHOR, etc.), reject.

    2. Per-code positive keywords. The description must contain at least one
       expected keyword for the code. Keywords are auto-derived from the
       code's display_name (drug names, procedure tokens) plus a small,
       hand-tuned override dictionary for cases where the official display
       name and the chargemaster shorthand disagree (e.g. "ER visit level 4"
       on our side vs "HC ED Type a Level 4" in real MRFs).

    3. When in doubt (description empty, or no usable keywords for the code),
       keep the observation. Better to leave some noise in than over-filter
       legitimate hospitals.
"""
from __future__ import annotations

import re
from functools import lru_cache

# Words that are too generic to anchor a code-specific match. "vaccine" alone
# matches a flu vaccine line and a COVID vaccine line, which is not what we
# want when filtering CPT 90688 (influenza) against CPT 91318 (COVID).
_GENERIC_TOKENS = frozenset({
    "the", "and", "for", "per", "with", "without", "a", "an", "of", "in",
    "on", "at", "to", "or", "by", "from", "into", "out",
    "vaccine", "drug", "test", "level", "type", "visit", "screening",
    "service", "supplies", "supply", "code", "fee", "rate",
    "procedure", "exam", "examination",
    "inpatient", "outpatient", "surgical", "medical", "clinical",
    "general", "specific", "other", "misc", "misc.", "miscellaneous",
    "dose", "dosage", "mg", "mcg", "ml", "iu", "unit", "units",
    "injection", "inj", "tablet", "capsule", "solution", "oral", "iv", "im",
    "each", "ea", "ct", "ct.",
})

# Surgical hardware that has nothing to do with vaccines, drugs, lab tests,
# imaging studies, or ER level codes. Matching as whole word at the start of
# the description (first 40 characters) is enough to catch the false-positive
# observations without rejecting genuinely surgical line items.
_HARDWARE_TOKENS = (
    "plate", "screw", "rod", "wire", "anchor", "implant", "cannula",
    "catheter", "stent", "valve", "mesh", "graft", "drain",
    "suture", "sutr", "scalpel", "blade", "needle",
    "lead", "tube", "guidewire",
    "cement", "spacer", "prosthesis", "trocar",
    "trochar", "drill", "burr", "rasp", "broach",
    "clip", "clamp", "stapler", "pin", "k-wire",
)

# Categories where hardware tokens ARE legitimate (it's surgery on hardware,
# or device-based imaging). Don't apply hardware exclusion here.
_SURGICAL_CATEGORIES = frozenset({
    "surgical",
    "inpatient_drg",
    "cardiovascular",
    "maternity",
    "gi_endoscopy",
    "cancer_treatment",
})

# Phrases that, when present, force a description to be rejected for the
# given code even if it matches a positive keyword. Mostly used to exclude
# the bilateral / revision / partial variant of a procedure that has its
# own CPT code, so we don't compare a 2-knee bilateral case against
# unilateral knees.
_NEGATIVE_OVERRIDES: dict[str, list[str]] = {
    # CPT 27447 is unilateral total knee. Bilateral knee is billed as two
    # 27447s (or 27447-50) and the chargemaster value for the bilateral
    # row is the doubled price. Revision knee is CPT 27487. Partial knee
    # is CPT 27446. Each of those is a different procedure with its own
    # price and shouldn't sit on the same podium.
    "CPT:27447": ["bilateral", "revision", "partial knee", "unicompart",
                  "unicondyl", "uni-compart", "uni-condyl", "uka"],
    # CPT 27130 is unilateral total hip; mirror the carve-outs.
    "CPT:27130": ["bilateral", "revision", "partial hip", "hemi-arthr",
                  "hemiarthr"],
    # CPT 47562 (lap chole) excludes open and converted-to-open which is
    # 47600/47605/47610.
    "CPT:47562": ["open chole", "converted to open"],
    # CPT 45378 (diagnostic colonoscopy) is the bare exam. Therapeutic
    # variants (45380 biopsy, 45385 polypectomy, 45388 ablation,
    # 45390 EMR) are different codes -- exclude their giveaway phrasing.
    "CPT:45378": ["with biopsy", "with polyp", "with snare",
                  "with submucosal", "with ablation", "with hot",
                  "with cold snare", "with EMR", "with band ligation"],
}

# Hand-tuned positive keywords for codes where the auto-derived list either
# misses common chargemaster shorthand or matches too loosely. Format is
# "<SYS>:<CODE>" -> list of substrings (lowercase). Description matches if
# any substring is present.
_POSITIVE_OVERRIDES: dict[str, list[str]] = {
    # Influenza vaccines: hospitals often write "IIV4" or "IIV3" (Inactivated
    # Influenza Vaccine 4-valent / 3-valent), or brand names.
    "CPT:90686": ["influenza", "flu ", "iiv4", "iiv ", "fluzone", "flublok",
                  "flulaval", "afluria", "fluvirin", "fluarix", "fluad",
                  "flucelvax"],
    "CPT:90688": ["influenza", "flu ", "iiv4", "iiv ", "fluzone", "flublok",
                  "flulaval", "afluria", "fluvirin", "fluarix", "fluad",
                  "flucelvax", "iiv4", "high-dose flu", "hdflu"],
    "CPT:90656": ["influenza", "flu ", "iiv ", "fluzone", "flublok"],
    "CPT:90672": ["influenza", "flumist", "live attenuated"],
    "CPT:90662": ["influenza", "fluzone high-dose", "fluad", "high-dose flu"],
    "CPT:90471": ["administration", "admin", "vaccine admin", "immuniz"],
    "CPT:90473": ["administration", "admin", "intranasal"],

    # Other vaccines
    "CPT:90649": ["hpv", "gardasil", "papillomavirus", "quadrivalent"],
    "CPT:90651": ["hpv", "gardasil", "papillomavirus", "9-valent", "9 valent"],
    "CPT:90715": ["tdap", "boostrix", "adacel", "tetanus"],
    "CPT:90633": ["hepatitis a", "hep a", "havrix", "vaqta"],
    "CPT:90636": ["hep a", "hep b", "twinrix", "hepatitis"],
    "CPT:90644": ["meningococcal", "menveo", "menactra"],
    "CPT:90707": ["mmr", "measles", "mumps", "rubella"],
    "CPT:90716": ["varicella", "chickenpox", "varivax"],
    "CPT:90734": ["meningitis", "meningococcal"],
    "CPT:90736": ["zoster", "shingles", "shingrix", "zostavax"],
    "CPT:91318": ["covid", "sars", "spikevax", "comirnaty", "pfizer", "moderna",
                  "mrna"],
    "CPT:91319": ["covid", "sars", "novavax"],
    "CPT:91320": ["covid", "sars"],
    "CPT:91321": ["covid", "sars", "booster"],

    # Joint replacement codes use multiple chargemaster shorthand spellings
    "CPT:27447": ["knee", "kne ", "tka ", "tka,", "arthrp", "arthroplasty"],
    "CPT:27130": ["hip", "femur", "femoral", "acetab", "thr ", "thr,",
                  "arthrp", "arthroplasty"],

    # Cesarean / vaginal delivery
    "CPT:59510": ["cesarean", "c-sec", "c sect", "csection", "csec"],
    "CPT:59400": ["vaginal", "delivery", "antepartum", "obstetric"],
    "CPT:59409": ["vaginal", "delivery"],
    "CPT:59514": ["cesarean", "c-sec"],

    # ER visit codes: real MRFs use "ED" (Emergency Department), "Emer",
    # "Type A" / "Type B" (CMS facility coding), and Level 1-5 with "Lvl"
    # short forms.
    "CPT:99281": ["emergency", "emer", "type a", "type b",
                  "level 1", "level i", "lvl 1", "lvl i"],
    "CPT:99282": ["emergency", "emer", "type a", "type b",
                  "level 2", "level ii", "lvl 2", "lvl ii"],
    "CPT:99283": ["emergency", "emer", "type a", "type b",
                  "level 3", "level iii", "lvl 3", "lvl iii"],
    "CPT:99284": ["emergency", "emer", "type a", "type b",
                  "level 4", "level iv", "lvl 4", "lvl iv"],
    "CPT:99285": ["emergency", "emer", "type a", "type b",
                  "level 5", "level v", "lvl 5", "lvl v"],

    # Office / hospital visit codes
    "CPT:99213": ["office", "outpatient", "established", "level 3"],
    "CPT:99214": ["office", "outpatient", "established", "level 4"],

    # Imaging
    "CPT:74177": ["ct ", "ct,", "ct-", "abdomen", "pelvis", "abd",
                  "computed tomography"],
    "CPT:74178": ["ct ", "ct,", "abdomen", "pelvis"],
    "CPT:74176": ["ct ", "ct,", "abdomen", "pelvis"],
    "CPT:71250": ["ct ", "ct,", "chest", "thorax"],
    "CPT:71260": ["ct ", "ct,", "chest"],
    "CPT:71270": ["ct ", "ct,", "chest"],
    "CPT:70450": ["ct ", "ct,", "head", "brain"],
    "CPT:70470": ["ct ", "ct,", "head", "brain"],
    "CPT:75571": ["ct ", "ct,", "calcium", "heart", "cardiac"],
    "CPT:70551": ["mri", "mr ", "brain"],
    "CPT:70553": ["mri", "mr ", "brain"],
    "CPT:72148": ["mri", "mr ", "lumbar", "spine"],
    "CPT:73721": ["mri", "mr ", "knee"],
    "CPT:77067": ["mammo", "mammogram", "breast", "screening"],
    "CPT:77065": ["mammo", "mammogram", "breast"],
    "CPT:77066": ["mammo", "mammogram", "breast"],
    "CPT:76700": ["ultrasound", "us ", "abdomen", "abd"],
    "CPT:76536": ["ultrasound", "us ", "thyroid", "neck"],
    "CPT:76700": ["ultrasound", "us "],
    "CPT:74018": ["x-ray", "xray", "abdomen", "kub"],
    "CPT:71046": ["x-ray", "xray", "chest", "cxr"],

    # Cancer screening / biopsy
    "CPT:19101": ["breast", "biopsy", "core"],
    "CPT:19083": ["breast", "biopsy", "stereotactic", "ultrasound"],
    # MRFs commonly write colonoscopies as "Diagnostic exam of large bowel
    # using a flexible instrument" (the literal CMS long descriptor) so we
    # accept the bowel/flexible-instrument phrasing alongside the obvious
    # "colonoscopy" token.
    "CPT:45378": ["colonoscopy", "colon", "bowel", "endoscopic", "flexible instrument"],
    "CPT:45380": ["colonoscopy", "colon", "bowel", "biopsy"],
    "CPT:45385": ["colonoscopy", "colon", "bowel", "polyp"],

    # Cytopathology code: the canonical descriptor is "Cytopathology, fluids,
    # washings or brushings, except cervical or vaginal" so chargemasters
    # frequently start with "Cytopathology". Treat this as a cytology code in
    # general rather than only cervical cytology.
    "CPT:88104": ["cytopath", "cytology", "fluids", "washings", "brushings"],

    # AAA repair: the chargemaster line for the device "Aortic Tube Prosthesis"
    # is the implanted graft used in the procedure - keep those.
    "CPT:34800": ["endovascular", "aorta", "aortic", "aaa", "abdominal aort",
                  "tube prosthesis", "graft"],
    "CPT:34802": ["endovascular", "aorta", "aortic", "aaa", "abdominal aort",
                  "tube prosthesis", "graft"],
    "CPT:34803": ["endovascular", "aorta", "aortic", "aaa", "abdominal aort",
                  "tube prosthesis", "graft"],
    "CPT:34812": ["endovascular", "femoral", "iliac", "aorta", "aaa"],

    # Lab panels
    "CPT:80050": ["panel", "general health", "comprehensive metabolic"],
    "CPT:80053": ["panel", "comprehensive metabolic", "cmp"],
    "CPT:80061": ["lipid", "cholesterol", "ldl", "hdl", "triglyceride"],
    "CPT:80048": ["basic metabolic", "panel", "bmp"],
    "CPT:81001": ["urine", "urinalysis", "ua"],
    "CPT:81003": ["urine", "urinalysis"],
    "CPT:82947": ["glucose"],
    "CPT:83036": ["hemoglobin", "a1c", "hba1c"],
    "CPT:84443": ["tsh", "thyroid"],
    "CPT:85025": ["cbc", "complete blood count", "blood count"],
    "CPT:85027": ["cbc", "blood count"],
    "CPT:87086": ["urine culture", "culture"],
    "CPT:87635": ["sars", "covid", "molecular"],
    "CPT:87880": ["strep", "rapid"],

    # Generic supply / unclassified codes that are intentionally broad. We
    # keep these wide open since the code name itself is "Unclassified" or
    # "Special supplies".
    "HCPCS:J3490": [],   # unclassified drugs - no positive filter
    "HCPCS:A4657": [],   # syringe sterile - no positive filter
    "CPT:99070":   [],   # special supplies - no positive filter

    # Drug brand <-> generic aliases
    "HCPCS:J9000": ["doxorubicin", "adriamycin", "doxoru"],
    "HCPCS:J9035": ["bevacizumab", "avastin"],
    "HCPCS:J9045": ["carboplatin"],
    "HCPCS:J9070": ["cyclophosphamide", "cytoxan"],
    "HCPCS:J9201": ["gemcitabine", "gemzar", "gemcit"],
    "HCPCS:J9250": ["methotrexate", "methotrex"],
    "HCPCS:J9263": ["oxaliplatin", "eloxatin", "oxalip"],
    "HCPCS:J9264": ["paclitaxel", "abraxane", "paclita"],
    "HCPCS:J9265": ["paclitaxel", "taxol", "paclita"],
    "HCPCS:J9270": ["plerixafor", "mozobil"],
    "HCPCS:J9271": ["pembrolizumab", "keytruda", "pembro", "pembrolizum"],
    "HCPCS:J9299": ["nivolumab", "opdivo", "nivolum"],
    "HCPCS:J9305": ["pemetrexed", "alimta", "pemetre"],
    "HCPCS:J9395": ["fulvestrant", "faslodex", "fulves"],
    "HCPCS:J9355": ["trastuzumab", "herceptin", "trastu"],
    "HCPCS:J9357": ["valrubicin", "valstar"],
    "HCPCS:J9999": [],   # unclassified antineoplastic - no positive filter
    "HCPCS:J1626": ["granisetron", "kytril", "sancuso", "granisetr"],
    "HCPCS:J2469": ["palonosetron", "aloxi", "palonose", "palonos",
                    "palonsetron", "palonosetr"],
    "HCPCS:J2405": ["ondansetron", "zofran", "ondanset"],
    "HCPCS:J7060": ["dextrose", "5%"],
    "HCPCS:J7050": ["normal saline", "saline 0.9", "sodium chloride"],
    "HCPCS:J1100": ["dexamethasone", "decadron"],
    "HCPCS:J1200": ["diphenhydramine", "benadryl"],
    "HCPCS:J3490": [],
}


# MRFs sometimes carry a stub description like "NDC Description Not Available"
# or a generic chargemaster bucket like "Noncdm Charge Record Medical
# Supplies". Treat these as missing so we keep the observation rather than
# rejecting it for an empty signal. Anything more specific than the stub gets
# evaluated against the code's positive keywords.
_MISSING_DESCRIPTION_RE = re.compile(
    r"^\s*("
    r"description\s+not\s+available|"
    r"no\s+description|"
    r"ndc\s+description\s+not\s+available|"
    r"procedure\s+description\s+not\s+available|"
    r"noncdm\s+charge\s+record.*|"
    r"none|"
    r"n/?a|"
    r"unspecified|"
    r"not\s+specified"
    r")\s*$",
    re.IGNORECASE,
)


def _strip_unit_suffix(text: str) -> str:
    """Drop "per Xmg" / "per X mcg" / "X.5 mL" tail tokens from display_name
    so we don't keyword-match every drug description on "10mg"."""
    text = re.sub(r"\bper\s+\d+(?:\.\d+)?\s*[a-z]+\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+(?:\.\d+)?\s*(mg|mcg|ug|ml|l|iu|units?|g)\b", " ",
                  text, flags=re.IGNORECASE)
    return text


@lru_cache(maxsize=2048)
def _auto_keywords(code_key: str, display_name: str) -> tuple[str, ...]:
    """Derive positive keywords from a code's display name.

    Lowercase, drop unit/dose tokens and stopwords, keep substrings of length
    at least 4. This catches drug names ("doxorubicin", "palonosetron"),
    procedure tokens ("knee", "hip", "colonoscopy"), and most distinctive
    chargemaster phrasings.
    """
    if code_key in _POSITIVE_OVERRIDES:
        return tuple(s.lower() for s in _POSITIVE_OVERRIDES[code_key])
    if not display_name:
        return ()
    text = _strip_unit_suffix(display_name).lower()
    text = re.sub(r"[\(\)\[\],/\-]", " ", text)
    parts = [p for p in text.split() if p]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if p in _GENERIC_TOKENS:
            continue
        if len(p) < 4:
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return tuple(out)


def _has_hardware_lead(description: str) -> bool:
    """True if the first ~40 characters of the description are dominated by a
    surgical hardware token. We check the lead because chargemaster lines for
    actual implants typically start with the hardware kind ("PLATE 2.7/3.5...",
    "SCREW BONE 4MM 15MM..."). Catching the lead avoids rejecting legitimate
    drug or vaccine lines that happen to contain "needle" or "tube" later in
    the description.
    """
    head = re.sub(r"^[^a-zA-Z]+", "", description[:50].lower())
    if not head:
        return False
    first_word = head.split()[0] if head.split() else ""
    if first_word in _HARDWARE_TOKENS:
        return True
    second = head.split()[1] if len(head.split()) > 1 else ""
    for tok in ("plate ", "screw ", "rod ", "wire ", "anchor ", "catheter ",
                "stent ", "suture ", "sutr ", "drain ", "cannula ",
                "guidewire ", "burr ", "rasp ", "broach "):
        if head.startswith(tok):
            return True
    return False


def description_matches_code(meta: dict, description: str | None) -> bool:
    """True when this description plausibly describes the given code.

    `meta` is one of the dicts from codes.CODES (must have code_system, code,
    display_name, category). `description` is the line item description from
    the hospital's MRF, possibly None.

    We deliberately err on the side of keeping observations: a missing or
    empty description is treated as a match because some MRF parsers do not
    carry the description through.
    """
    if not description:
        return True
    desc_raw = description.strip()
    if not desc_raw:
        return True
    if _MISSING_DESCRIPTION_RE.match(desc_raw):
        return True
    desc = desc_raw.lower()

    code_system = (meta.get("code_system") or "").upper().replace("-", "")
    if code_system == "MSDRG":
        code_system = "MS-DRG"
    code = (meta.get("code") or "").strip()
    code_key = f"{code_system}:{code}"
    category = (meta.get("category") or "").lower()
    display_name = meta.get("display_name") or ""

    # Codes flagged with an explicitly empty positive list are generic
    # buckets (Unclassified drugs, Special supplies, Syringe sterile) where
    # the chargemaster line item can legitimately be a screw, a catheter,
    # or any other consumable. Skip the hardware blocklist for those.
    is_generic_bucket = (
        code_key in _POSITIVE_OVERRIDES and not _POSITIVE_OVERRIDES[code_key]
    )
    if category not in _SURGICAL_CATEGORIES and not is_generic_bucket:
        if _has_hardware_lead(description):
            return False

    # Per-code carve-outs for variants that share a code but bill a
    # different procedure (bilateral, revision, partial, ...). If the
    # description contains any of these phrases, this row is comparing
    # apples to a different fruit -- drop it.
    negatives = _NEGATIVE_OVERRIDES.get(code_key) or []
    for neg in negatives:
        if neg.lower() in desc:
            return False

    keywords = _auto_keywords(code_key, display_name)
    if not keywords:
        return True
    return any(_keyword_in_desc(kw, desc) for kw in keywords)


@lru_cache(maxsize=4096)
def _compile_keyword(kw: str) -> re.Pattern[str]:
    """Compile a keyword to a word-boundary regex.

    Short keywords (4 chars or fewer) use word boundaries on BOTH ends so
    "flu" matches "Flu,Quad" but not "Fluted" (the false positive that hit
    a hip-implant stem on the flu vaccine page). Longer keywords use a
    start-only word boundary so chargemaster prefix-shortenings like
    "GRANISETR.1MGIJ" still match the keyword "granisetr".
    """
    kw = kw.strip()
    pattern = re.escape(kw)
    if len(kw) <= 4:
        return re.compile(rf"\b{pattern}\b", re.IGNORECASE)
    return re.compile(rf"\b{pattern}", re.IGNORECASE)


def _keyword_in_desc(kw: str, desc_lower: str) -> bool:
    """Match keyword as a left-anchored word in the description."""
    kw_clean = kw.strip().lower()
    if not kw_clean:
        return False
    return bool(_compile_keyword(kw_clean).search(desc_lower))
