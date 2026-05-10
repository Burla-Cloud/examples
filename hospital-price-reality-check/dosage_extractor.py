"""Parse a chargemaster line item description and return the total dose
expressed in the HCPCS unit family the code is billed in.

Most CSV/XLSX MRFs do not populate the optional CMS v3 ``drug_information``
block, so the ``drug_unit`` field on each observation is ``None`` and the
regex normalizer in ``reduce.py`` cannot scale a 50mg vial down to a
per-10mg HCPCS unit. This module fills that gap by extracting the dose
straight from text like:

    "DOXORUBICIN 50 MG INJ"                       -> 50 mg
    "INJECTION, DOXORUBICIN HYDROCHLORIDE, 10 MG" -> 10 mg
    "DOXORUBICIN 50 MG/25 ML INTRAVENOUS SOLUTION"-> 50 mg (vial)
    "DOXORUBICIN 2 MG/ML SOLN 100 ML VIAL"        -> 200 mg (concentration x volume)
    "DOXORUBICIN PER 10 MG"                       -> 10 mg
    "PALONOSETRON HCL 0.05 MG/ML SOLN 5 ML VIAL"  -> 0.25 mg = 250 mcg

The extractor returns ``(total_qty, unit)`` in the canonical units
``"mg"``, ``"mcg"``, ``"ml"``, ``"unit"``, or ``"each"``, or ``None`` if
no dose can be extracted.

Combined with ``reduce.py``'s ``_parse_hcpcs_unit`` (which reads the per-X
unit out of the code's display name), we can normalize any priced row to
"per HCPCS unit" the same way ``drug_unit`` does, even when the field is
absent from the MRF.
"""
from __future__ import annotations

import re

__all__ = ["extract_dose_from_description"]

# A unit terminator -- "MG" / "ML" / "MCG" must be followed by either a
# real word boundary, a digit, or a known abbreviation continuation
# (SDV / MDV / SOLN / VIAL / INJ / IV / PWD / IM / SUSP / SOLR ...).
# Many chargemaster strings smash tokens like "25MLSDV" or "100MGINJ"
# without separators and a plain ``\b`` would skip those.
_UNIT_END = r"(?=$|\s|/|\d|,|\.|\-|;|\)|SDV|MDV|VIAL|SOLN|SOLR|SOLT|INJ|IV|IM|IT|PWD|SUSP|CHEMO|SDU)"

# Match "<num1> mg/<num2> ml" -- total dose / vial volume form. Captures
# (total_mg, volume_ml). The number after the slash must be present, which is
# what distinguishes this from the "<num> mg/ml" concentration form below.
# A "left edge" guard for numbers: don't match a number that's part of a
# bigger token like "12345A". We treat letters and digits as
# token-internal; "_", "-", "/", "(", "." (leading-decimal) and whitespace
# are valid separators. We allow "." in front so ".25 MG" is captured.
_NUM_LEFT = r"(?<![A-Za-z0-9])"
_NUM_BODY = r"\d*\.?\d+"

_TOTAL_MG_OVER_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*MG\s*/\s*(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)
_TOTAL_MCG_OVER_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:MCG|UG|UCG)\s*/\s*(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)
_TOTAL_G_OVER_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:GM|G)\s*/\s*(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)

# Match concentration form "<num> mg/ml" with no number before ML, then a
# later "<num> ml" giving the vial volume. Matches across the whole
# description so e.g. "2 MG/ML SOLN 100 ML VIAL" yields concentration=2,
# volume=100, total=200 mg.
_CONC_MG_PER_ML_THEN_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*MG\s*/\s*ML\b[^/]*?(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)
_CONC_MCG_PER_ML_THEN_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:MCG|UG)\s*/\s*ML\b[^/]*?(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)
_CONC_G_PER_ML_THEN_VOL = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:GM|G)\s*/\s*ML\b[^/]*?(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)

# Plain-number-with-unit fallback. Negative lookahead rejects the
# concentration-only form (no volume to multiply by).
#
# The ``(?!\s+ML\b)`` guard is what makes us reject "25 MG mL" (a
# concentration-without-volume pattern that survives ``_tokenize`` --
# it splits "mgmL" into "mg mL"). The earlier ``_CONC_*`` patterns
# match concentration WITH a follow-on volume token; if neither fires,
# we want to refuse to fall back to the plain match instead of
# silently treating concentration as total dose. Without this guard
# the LLM audit caught J9301 obinutuzumab rows like "obinutuzumab
# 25 mgmL Sol" where we returned 25 mg as the dose -- it isn't.
_PLAIN_MG = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*MG(?!\s*/)(?!\s+ML\b)" + _UNIT_END,
    re.IGNORECASE,
)
_PLAIN_MCG = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:MCG|UG)(?!\s*/)(?!\s+ML\b)" + _UNIT_END,
    re.IGNORECASE,
)
_PLAIN_G = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:GM|G)(?!\s*/)(?!\s+ML\b)" + _UNIT_END,
    re.IGNORECASE,
)
_PLAIN_UNITS = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*(?:UNIT|UNITS|IU)\b",
    re.IGNORECASE,
)
_PLAIN_ML = re.compile(
    _NUM_LEFT + r"(" + _NUM_BODY + r")\s*ML" + _UNIT_END,
    re.IGNORECASE,
)

# Non-dose-y noise that should never be confused for a dose:
# "Q3W", "PER VIAL", "FOR INJ" -- these come before/after numbers and we
# only ever match against the explicit unit suffixes above, so they're a
# soft guard rather than a hard reject.

# Catch-all to abandon descriptions that look like internal billing IDs
# instead of clinical descriptions.
_LOOKS_LIKE_ID = re.compile(r"^\s*[A-Z0-9_]{6,}\s*$")


# Insert whitespace at letter/digit transitions so "PER25MCGIJ" becomes
# "PER 25 MCGIJ" before the regex pass. We see this in chargemasters that
# strip whitespace ("DOXORUBICIN50MG", "10MGINJ", ".25MGIJ"). Without it,
# the lookbehind asserting "no letter/digit before the number" never
# fires on these tokens.
_LETTER_DIGIT = re.compile(r"(?<=[A-Za-z])(?=\d)|(?<=\d)(?=[A-Za-z])")

# Insert a space after a recognized unit token if it is followed directly
# by more letters: "25 MCGIJ" -> "25 MCG IJ", "10 MGSDV" -> "10 MG SDV".
# Order matters: longest unit names first so "MCG" is split out before
# the shorter "MG" pattern catches it.
_UNIT_SPLIT = re.compile(
    r"(?i)(?<=[\s\d])(MCG|UCG|UG|MG|ML|GM|G|IU|UNITS|UNIT)(?=[A-Za-z])"
)

# Strip thousands separators inside numeric tokens before regex parsing
# so "1,000 mg" parses as 1000.
_NUM_COMMA = re.compile(r"(?<=\d),(?=\d{3}\b)")


def _tokenize(s: str) -> str:
    s = _LETTER_DIGIT.sub(" ", s)
    s = _UNIT_SPLIT.sub(r"\1 ", s)
    s = _NUM_COMMA.sub("", s)
    return s


def extract_dose_from_description(description: str | None) -> tuple[float, str] | None:
    """Return (qty, unit_in_canonical_form) or None.

    Canonical units: ``"mg"``, ``"mcg"``, ``"ml"``, ``"unit"``, ``"each"``.

    Resolution priority (first match wins):

    1. ``<X> mg/<Y> ml``  -- vial form, total dose is X mg.
    2. ``<X> mcg/<Y> ml`` -- vial form, total dose is X mcg.
    3. ``<X> g/<Y> ml``   -- vial form, total dose is X * 1000 mg.
    4. ``<X> mg/ml ... <Y> ml`` -- concentration x volume, total = X*Y mg.
    5. ``<X> mcg/ml ... <Y> ml`` -- ditto in mcg.
    6. ``<X> g/ml ... <Y> ml`` -- ditto in grams.
    7. Plain ``<X> mg`` / ``<X> mcg`` / ``<X> g`` / ``<X> unit(s)`` /
       ``<X> ml``.

    Any value <= 0 or > 1e6 of its unit is rejected as a typo / parser
    artifact.
    """
    if not description:
        return None
    s = str(description).strip()
    if not s or _LOOKS_LIKE_ID.match(s):
        return None
    s = _tokenize(s)

    # 1-3. total-over-volume forms (vial size)
    m = _TOTAL_MG_OVER_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)), "mg")

    m = _TOTAL_MCG_OVER_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)), "mcg")

    m = _TOTAL_G_OVER_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)) * 1000.0, "mg")

    # 4-6. concentration-times-volume forms
    m = _CONC_MG_PER_ML_THEN_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)) * float(m.group(2)), "mg")

    m = _CONC_MCG_PER_ML_THEN_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)) * float(m.group(2)), "mcg")

    m = _CONC_G_PER_ML_THEN_VOL.search(s)
    if m:
        return _maybe(float(m.group(1)) * float(m.group(2)) * 1000.0, "mg")

    # 7. plain forms. Take the MAXIMUM matched value -- chargemaster
    # descriptions routinely repeat the HCPCS billing unit before the
    # actual vial size, e.g.::
    #
    #     "CARFILZOMIB/1MG 30MG PWIJ"
    #
    # which after tokenization is ``CARFILZOMIB / 1 MG 30 MG PWIJ``.
    # The first MG is the per-1mg HCPCS reminder, the second is the
    # 30 mg vial. ``re.search`` returns the first match (1 mg) and we'd
    # fail to scale the price down. Taking ``max`` over ``findall``
    # picks 30 mg, which is what the LLM audit said the dose actually
    # is. The vial-form and concentration-form patterns above always
    # win first for descriptions that contain a vial volume, so this
    # max-of-plain heuristic only kicks in for "just a number with a
    # unit" descriptions where the largest number is overwhelmingly
    # the right answer.
    matches = _PLAIN_MCG.findall(s)
    if matches:
        return _maybe(max(float(x) for x in matches), "mcg")

    matches = _PLAIN_MG.findall(s)
    if matches:
        return _maybe(max(float(x) for x in matches), "mg")

    matches = _PLAIN_G.findall(s)
    if matches:
        return _maybe(max(float(x) for x in matches) * 1000.0, "mg")

    matches = _PLAIN_UNITS.findall(s)
    if matches:
        return _maybe(max(float(x) for x in matches), "unit")

    matches = _PLAIN_ML.findall(s)
    if matches:
        return _maybe(max(float(x) for x in matches), "ml")

    return None


def _maybe(qty: float, unit: str) -> tuple[float, str] | None:
    if qty <= 0:
        return None
    if qty > 1_000_000:  # absurd, almost certainly a parsing artifact
        return None
    return qty, unit
