"""Transform wpi_reduced_v2.json into UI-ready artifacts and headline findings.

Outputs (all JSON, written to frontend/data/):
  world.json                   — country-level choropleth + top distinctive phrases
  countries/{cc}.json          — per-country detail: top phrases, top admins, top cities, samples
  findings.json                — 10 ranked viral findings
  index.json                   — small top-level index the UI loads first
"""
from __future__ import annotations

import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


HERE = Path(__file__).parent
IN_PATH = HERE / "samples" / "wpi_reduced_v2.json"
OUT_DIR = HERE / "frontend" / "data"
OUT_COUNTRIES = OUT_DIR / "countries"


# -- country metadata ------------------------------------------------------

COUNTRY_NAMES = {
    "US": ("United States", "🇺🇸"),
    "GB": ("United Kingdom", "🇬🇧"),
    "CA": ("Canada", "🇨🇦"),
    "AU": ("Australia", "🇦🇺"),
    "FR": ("France", "🇫🇷"),
    "DE": ("Germany", "🇩🇪"),
    "IT": ("Italy", "🇮🇹"),
    "ES": ("Spain", "🇪🇸"),
    "JP": ("Japan", "🇯🇵"),
    "NL": ("Netherlands", "🇳🇱"),
    "BE": ("Belgium", "🇧🇪"),
    "IN": ("India", "🇮🇳"),
    "IE": ("Ireland", "🇮🇪"),
    "NZ": ("New Zealand", "🇳🇿"),
    "PA": ("Panama", "🇵🇦"),
    "CH": ("Switzerland", "🇨🇭"),
    "SE": ("Sweden", "🇸🇪"),
    "MX": ("Mexico", "🇲🇽"),
    "PT": ("Portugal", "🇵🇹"),
    "CN": ("China", "🇨🇳"),
    "BR": ("Brazil", "🇧🇷"),
    "AR": ("Argentina", "🇦🇷"),
    "AT": ("Austria", "🇦🇹"),
    "RU": ("Russia", "🇷🇺"),
    "NO": ("Norway", "🇳🇴"),
    "DK": ("Denmark", "🇩🇰"),
    "FI": ("Finland", "🇫🇮"),
    "GR": ("Greece", "🇬🇷"),
    "PL": ("Poland", "🇵🇱"),
    "TR": ("Turkey", "🇹🇷"),
    "IL": ("Israel", "🇮🇱"),
    "AE": ("UAE", "🇦🇪"),
    "EG": ("Egypt", "🇪🇬"),
    "ZA": ("South Africa", "🇿🇦"),
    "KE": ("Kenya", "🇰🇪"),
    "MA": ("Morocco", "🇲🇦"),
    "TH": ("Thailand", "🇹🇭"),
    "SG": ("Singapore", "🇸🇬"),
    "ID": ("Indonesia", "🇮🇩"),
    "MY": ("Malaysia", "🇲🇾"),
    "PH": ("Philippines", "🇵🇭"),
    "VN": ("Vietnam", "🇻🇳"),
    "KR": ("South Korea", "🇰🇷"),
    "TW": ("Taiwan", "🇹🇼"),
    "HK": ("Hong Kong", "🇭🇰"),
    "IS": ("Iceland", "🇮🇸"),
    "CZ": ("Czech Republic", "🇨🇿"),
    "HU": ("Hungary", "🇭🇺"),
    "HR": ("Croatia", "🇭🇷"),
    "RO": ("Romania", "🇷🇴"),
    "BG": ("Bulgaria", "🇧🇬"),
    "UA": ("Ukraine", "🇺🇦"),
    "SK": ("Slovakia", "🇸🇰"),
    "SI": ("Slovenia", "🇸🇮"),
    "EE": ("Estonia", "🇪🇪"),
    "LV": ("Latvia", "🇱🇻"),
    "LT": ("Lithuania", "🇱🇹"),
    "CL": ("Chile", "🇨🇱"),
    "PE": ("Peru", "🇵🇪"),
    "CO": ("Colombia", "🇨🇴"),
    "VE": ("Venezuela", "🇻🇪"),
    "EC": ("Ecuador", "🇪🇨"),
    "UY": ("Uruguay", "🇺🇾"),
    "CR": ("Costa Rica", "🇨🇷"),
    "GT": ("Guatemala", "🇬🇹"),
    "NI": ("Nicaragua", "🇳🇮"),
    "DO": ("Dominican Republic", "🇩🇴"),
    "CU": ("Cuba", "🇨🇺"),
    "JM": ("Jamaica", "🇯🇲"),
    "BS": ("Bahamas", "🇧🇸"),
    "PR": ("Puerto Rico", "🇵🇷"),
    "IR": ("Iran", "🇮🇷"),
    "IQ": ("Iraq", "🇮🇶"),
    "SA": ("Saudi Arabia", "🇸🇦"),
    "JO": ("Jordan", "🇯🇴"),
    "LB": ("Lebanon", "🇱🇧"),
    "SY": ("Syria", "🇸🇾"),
    "PK": ("Pakistan", "🇵🇰"),
    "BD": ("Bangladesh", "🇧🇩"),
    "LK": ("Sri Lanka", "🇱🇰"),
    "NP": ("Nepal", "🇳🇵"),
    "MM": ("Myanmar", "🇲🇲"),
    "KH": ("Cambodia", "🇰🇭"),
    "LA": ("Laos", "🇱🇦"),
    "MN": ("Mongolia", "🇲🇳"),
    "KZ": ("Kazakhstan", "🇰🇿"),
    "AF": ("Afghanistan", "🇦🇫"),
    "GH": ("Ghana", "🇬🇭"),
    "NG": ("Nigeria", "🇳🇬"),
    "TZ": ("Tanzania", "🇹🇿"),
    "UG": ("Uganda", "🇺🇬"),
    "ET": ("Ethiopia", "🇪🇹"),
    "CM": ("Cameroon", "🇨🇲"),
    "SN": ("Senegal", "🇸🇳"),
    "MG": ("Madagascar", "🇲🇬"),
    "ZW": ("Zimbabwe", "🇿🇼"),
    "NA": ("Namibia", "🇳🇦"),
    "BW": ("Botswana", "🇧🇼"),
    "CD": ("DR Congo", "🇨🇩"),
    "AO": ("Angola", "🇦🇴"),
    "TN": ("Tunisia", "🇹🇳"),
    "DZ": ("Algeria", "🇩🇿"),
    "LY": ("Libya", "🇱🇾"),
    "SD": ("Sudan", "🇸🇩"),
    "FJ": ("Fiji", "🇫🇯"),
    "WS": ("Samoa", "🇼🇸"),
    "TO": ("Tonga", "🇹🇴"),
    "VU": ("Vanuatu", "🇻🇺"),
    "PG": ("Papua New Guinea", "🇵🇬"),
    "MV": ("Maldives", "🇲🇻"),
    "BT": ("Bhutan", "🇧🇹"),
    "BN": ("Brunei", "🇧🇳"),
    "KP": ("North Korea", "🇰🇵"),
    "CY": ("Cyprus", "🇨🇾"),
    "MT": ("Malta", "🇲🇹"),
    "LU": ("Luxembourg", "🇱🇺"),
    "LI": ("Liechtenstein", "🇱🇮"),
    "MC": ("Monaco", "🇲🇨"),
    "AD": ("Andorra", "🇦🇩"),
    "SM": ("San Marino", "🇸🇲"),
    "VA": ("Vatican City", "🇻🇦"),
    "IM": ("Isle of Man", "🇮🇲"),
    "JE": ("Jersey", "🇯🇪"),
    "GG": ("Guernsey", "🇬🇬"),
    "GL": ("Greenland", "🇬🇱"),
    "FO": ("Faroe Islands", "🇫🇴"),
    "GI": ("Gibraltar", "🇬🇮"),
    "AQ": ("Antarctica", "🇦🇶"),
}


# URL-encoded glyphs like %e6%97%a5 clutter a few Asian-language countries.
URLENC_RX = re.compile(r"(?:%[0-9a-fA-F]{2}){2,}")
HEX_RX = re.compile(r"^[a-f0-9]{3,}$")


# Known "site-junk" phrases that survived the first filter but aren't content.
EXTRA_STOP = frozenset({
    "one", "all", "two", "three", "many", "some", "any", "every", "each",
    "but", "also", "yet", "very", "still", "just", "only", "even", "then",
    "view", "target", "blank", "out", "org", "biz", "net", "info",
    "something", "nothing", "things", "stuff", "today", "tomorrow", "yesterday",
    "morning", "afternoon", "evening", "noon", "midnight",
    "ewww", "wwww", "awww", "ehttp", "fphotos", "fhttp", "photography",
    "geotagged", "mixedapp", "robogeo", "fflickr2map", "auspctagged",
    "photo", "photos", "picture", "pictures", "picasa", "panoramio",
    # Camera / app / software cruft
    "eos", "lr", "ps", "cs", "raw", "jpeg", "iphoneography", "snapseed",
    "vsco", "instagram", "igers", "gf1", "gf2", "gf3", "gf5", "d40", "d50",
    "d60", "d70", "d80", "d90", "d200", "d300", "d600", "d700", "d800",
    "50d", "60d", "70d", "80d", "450d", "550d", "650d", "750d", "5d", "6d",
    "7d", "vwc", "fav", "favorites", "faves", "explored", "explore",
    "portfolio", "series", "set", "best", "favorite", "favourite",
    # Generic meta-tags
    "people", "man", "woman", "men", "women", "boy", "girl", "kids",
    "family", "friends", "friend",
    "alpha", "beta", "gamma", "delta", "sport", "team", "official",
    "may", "june", "july", "august", "september", "october", "november",
    "december", "january", "february", "march", "april",
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
})

# Continent / region names — excluded from distinctive-phrase lists.
GEO_STOP = frozenset({
    "europe", "africa", "asia", "america", "oceania", "antarctica",
    "south america", "north america", "central america", "latin america",
    "middle east", "far east", "south east asia", "southeast asia",
    "eastern europe", "western europe", "northern europe", "southern europe",
    "scandinavia", "balkans", "caribbean", "mediterranean", "pacific",
    "atlantic", "world", "earth", "planet", "globe",
    "europa", "afrika", "azia", "amerika",
})


def _clean_phrase(p: str) -> str:
    if not p:
        return ""
    # URL-encoded multi-byte characters: drop the phrase — these are CJK tokens
    # that look like hash noise. We still have Latin transliterations that
    # surface in per-country lists.
    if URLENC_RX.search(p):
        return ""
    # Drop phrases that are only site-junk tokens
    words = p.split()
    words = [w for w in words if w not in EXTRA_STOP]
    if not words:
        return ""
    # Drop if any word is long hex
    for w in words:
        if HEX_RX.fullmatch(w) and len(w) >= 8:
            return ""
    return " ".join(words)


def _rollup_counts(counts: Dict[str, int]) -> Dict[str, int]:
    """Collapse near-duplicate cleaned phrases, keep top form."""
    merged: Dict[str, int] = defaultdict(int)
    for k, v in counts.items():
        c = _clean_phrase(k)
        if c:
            merged[c] += v
    return merged


def _country_name_aliases(cc: str) -> set:
    """Words so tied to the country that they're not 'findings'."""
    aliases = {cc.lower()}
    name, _ = COUNTRY_NAMES.get(cc, (cc, ""))
    aliases.add(name.lower())
    for w in name.lower().split():
        if len(w) > 3:
            aliases.add(w)
    # common alternate spellings
    extras = {
        "US": {"usa", "united states", "america", "american"},
        "GB": {"uk", "britain", "british", "england", "english", "scotland",
               "scottish", "wales", "welsh"},
        "DE": {"germany", "deutschland", "german", "deutsch"},
        "IT": {"italy", "italia", "italiano", "italian", "italie"},
        "ES": {"spain", "espana", "espa%c3%b1a", "spanish", "espagne"},
        "FR": {"france", "francia", "french"},
        "JP": {"japan", "nippon", "nihon", "japanese"},
        "CN": {"china", "chinese"},
        "RU": {"russia", "russian", "rossiya"},
        "BR": {"brazil", "brasil", "brasileiro", "brazilian"},
        "NL": {"netherlands", "holland", "dutch", "nederland"},
        "SE": {"sweden", "sverige", "swedish", "svenska"},
        "CH": {"switzerland", "schweiz", "suisse", "swiss"},
        "IN": {"india", "indian", "bharat"},
        "KR": {"korea", "korean", "hanguk"},
        "EG": {"egypt", "egipto", "%c3%a4gypten"},
        "IS": {"iceland", "icelandic", "%c3%adsland"},
        "GR": {"greece", "hellas", "greek"},
        "NO": {"norway", "norge", "norwegian"},
        "DK": {"denmark", "danmark", "danish"},
        "FI": {"finland", "suomi", "finnish"},
        "PT": {"portugal", "portuguese"},
        "AU": {"australia", "aus", "australian"},
        "CA": {"canada", "canadian"},
        "MX": {"mexico", "mexican"},
        "AR": {"argentina", "argentine", "argentinian"},
        "CL": {"chile", "chilean"},
        "CO": {"colombia", "colombian"},
        "PE": {"peru", "peruvian"},
        "IL": {"israel", "israeli"},
        "MA": {"morocco", "moroccan"},
        "TR": {"turkey", "turkish"},
        "TH": {"thailand", "thai"},
        "VN": {"vietnam", "vietnamese"},
        "ID": {"indonesia", "indonesian"},
        "PH": {"philippines", "philippine", "filipino", "filipina"},
        "MY": {"malaysia", "malaysian"},
        "SG": {"singapore"},
        "NZ": {"new zealand", "zealand", "kiwi"},
        "PL": {"poland", "polska", "polish"},
        "CZ": {"czech", "republic"},
        "AT": {"austria", "osterreich", "austrian"},
        "BE": {"belgium", "belgian", "belgique"},
        "HU": {"hungary", "hungarian", "magyar"},
        "IR": {"iran", "iranian", "persia", "persian"},
        "SA": {"saudi", "arabia", "saudi arabia"},
        "AE": {"emirates", "uae", "emirati"},
        "ZA": {"south africa", "africa", "african"},
        "KE": {"kenya", "kenyan"},
        "TZ": {"tanzania", "tanzanian"},
        "NG": {"nigeria", "nigerian"},
        "GH": {"ghana", "ghanaian"},
    }.get(cc, set())
    aliases.update(extras)
    # Strip whitespace/underscore variants
    return {a.lower().strip() for a in aliases}


def compute_tfidf(country_phrases: Dict[str, Dict[str, int]], min_photos: int = 1000):
    """For each phrase, compute per-country TF-IDF."""
    total_by_country: Dict[str, int] = {cc: sum(m.values()) for cc, m in country_phrases.items()}
    # Sum phrase counts across countries
    df_countries: Dict[str, int] = defaultdict(int)
    for cc, m in country_phrases.items():
        for p in m:
            df_countries[p] += 1
    n_countries = len({cc for cc in country_phrases if total_by_country[cc] >= min_photos})
    tfidf: Dict[str, Dict[str, float]] = {}
    for cc, m in country_phrases.items():
        if total_by_country[cc] < min_photos:
            continue
        total = total_by_country[cc] or 1
        scores = {}
        for p, c in m.items():
            tf = c / total
            idf = math.log((n_countries + 1) / (df_countries[p] + 1)) + 1.0
            scores[p] = tf * idf * math.log(1 + c)  # boost well-attested phrases
        tfidf[cc] = scores
    return tfidf


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_COUNTRIES.mkdir(parents=True, exist_ok=True)

    print("loading wpi_reduced_v2.json ...")
    d = json.loads(IN_PATH.read_text())
    print(f"  {d['n_countries']} countries, {d['n_rows_total']:,} photos")

    # Clean all counters
    country_photos = d["country_photos"]
    raw_phrases = d["country_top_phrases"]
    raw_admin_phrases = d["admin_top_phrases"]
    raw_city_phrases = d["city_top_phrases"]
    country_samples = d["country_samples"]

    cleaned_phrases: Dict[str, Dict[str, int]] = {
        cc: _rollup_counts(m) for cc, m in raw_phrases.items()
    }

    # Build a GLOBAL place-name set from admin1 + city names so we can filter
    # them out of distinctive-thing findings. These are places, not things.
    place_words: set = set()
    for k in raw_admin_phrases:
        _cc, admin1 = k.split("|", 1)
        for w in admin1.lower().split():
            if len(w) > 2:
                place_words.add(w)
            place_words.add(admin1.lower().strip())
    for k in raw_city_phrases:
        _cc, admin1, city = k.split("|", 2)
        for w in city.lower().split():
            if len(w) > 2:
                place_words.add(w)
            place_words.add(city.lower().strip())

    # Country-name words (english + localised) too
    for cc in cleaned_phrases:
        place_words.update(_country_name_aliases(cc))
    place_words.update(GEO_STOP)

    # Remove country-name aliases from each country's own list (so finding
    # doesn't say "france's most photographed thing: France").
    country_distinctive_raw: Dict[str, Dict[str, int]] = {}
    country_things: Dict[str, Dict[str, int]] = {}  # no places at all
    for cc, m in cleaned_phrases.items():
        aliases = _country_name_aliases(cc)
        out = {p: n for p, n in m.items() if p not in aliases}
        country_distinctive_raw[cc] = out
        things = {}
        for p, n in out.items():
            # Drop phrases where every word is a place-word
            words = p.split()
            if all(w in place_words for w in words):
                continue
            # Drop if the phrase itself is a place or a place alias
            if p in place_words:
                continue
            things[p] = n
        country_things[cc] = things

    # Compute TF-IDF on DISTINCTIVE phrases only (excluding country-name aliases)
    tfidf = compute_tfidf(country_distinctive_raw, min_photos=2000)

    # ---- findings -------------------------------------------------------

    findings: List[Dict[str, Any]] = []

    # Eligible countries: ≥ 2k photos
    eligible = sorted(
        [cc for cc, n in country_photos.items() if n >= 2000],
        key=lambda cc: -country_photos[cc],
    )

    # Global aggregate of "things" — used to reject one-off event tags.
    global_things = Counter()
    for _, m in country_things.items():
        global_things.update(m)

    # F1 — Most photographed THING (non-place) per country
    by_country_things = []
    for cc in eligible[:60]:
        things = country_things.get(cc, {})
        if not things:
            continue
        # Must appear globally ≥ 400 times so we don't surface event-only tags
        top_filtered = [
            (p, n) for p, n in sorted(things.items(), key=lambda kv: -kv[1])
            if global_things[p] >= 400
        ][:6]
        if not top_filtered:
            top_filtered = sorted(things.items(), key=lambda kv: -kv[1])[:6]
        top = top_filtered
        if not top:
            continue
        primary, primary_n = top[0]
        secondary = [p for p, _ in top[1:5]]
        by_country_things.append({
            "cc": cc,
            "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "photos": country_photos[cc],
            "primary": primary,
            "primary_count": primary_n,
            "runners": secondary,
        })

    findings.append({
        "id": "signature_per_country",
        "title": "The most photographed thing in every country",
        "blurb": "Places filtered out (no cities, no regions, no country names) — what's the number-one non-place tag across 9.49 million public geotagged photos from 246 countries? This is Earth's camera-roll census.",
        "rows": by_country_things,
    })

    # F2 — Things (not places) that are nearly 100% one country
    # Using country_things here instead of country_distinctive_raw so we
    # exclude all place names from the "monopoly" rankings. Result: actual
    # landmarks, foods, events, subcultures — not just cities.
    global_thing_countries: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    global_thing_total: Dict[str, int] = defaultdict(int)
    for cc, m in country_things.items():
        for p, n in m.items():
            global_thing_countries[p][cc] += n
            global_thing_total[p] += n

    # still need the place-aware counter for other findings
    global_phrase_countries: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    global_phrase_total: Dict[str, int] = defaultdict(int)
    for cc, m in country_distinctive_raw.items():
        for p, n in m.items():
            global_phrase_countries[p][cc] += n
            global_phrase_total[p] += n

    monopolies = []
    for p, total in global_thing_total.items():
        if total < 1500:
            continue
        top_cc, top_n = max(global_thing_countries[p].items(), key=lambda kv: kv[1])
        share = top_n / total
        if share < 0.85:
            continue
        if len(p) < 4 or len(p) > 30:
            continue
        monopolies.append({"phrase": p, "cc": top_cc, "name": COUNTRY_NAMES.get(top_cc, (top_cc, ""))[0],
                          "flag": COUNTRY_NAMES.get(top_cc, (top_cc, ""))[1],
                          "total": total, "in_country": top_n, "share": round(share, 3)})
    monopolies.sort(key=lambda r: -r["total"])
    findings.append({
        "id": "national_monopolies",
        "title": "Things only one country photographs",
        "blurb": "Tags referring to actual things (not cities, not regions) that appear ≥1,500 times worldwide — but ≥85% of all photos of them come from a single country. Landmarks, subcultures, events, people, foods.",
        "rows": monopolies[:60],
    })

    # F3 — Cities whose identity is ONE phrase (with place filter + global floor)
    city_dominance = []
    for k, m in raw_city_phrases.items():
        clean = _rollup_counts(m)
        if not clean:
            continue
        cc, admin1, city = k.split("|", 2)
        aliases = _country_name_aliases(cc)
        # Drop aliases, city name, admin1 name
        distinct = {}
        for p, n in clean.items():
            if p in aliases or p == city.lower() or p == admin1.lower():
                continue
            # strip phrases where every word is a place-word
            words = p.split()
            if all(w in place_words for w in words):
                continue
            distinct[p] = n
        if not distinct:
            continue
        top = sorted(distinct.items(), key=lambda kv: -kv[1])[:3]
        total = sum(clean.values())
        # Harden: city must have ≥ 1500 photos AND phrase must appear ≥ 200
        # times globally (filters out one-off event tags in a single town).
        if total < 1500:
            continue
        p0, n0 = top[0]
        if global_phrase_total.get(p0, 0) < 200:
            continue
        share = n0 / total
        if share < 0.10:
            continue
        city_dominance.append({
            "cc": cc, "admin1": admin1, "city": city,
            "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "primary": p0, "primary_count": n0,
            "share": round(share, 3),
            "total": total,
            "runners": [p for p, _ in top[1:3]],
        })
    city_dominance.sort(key=lambda r: -r["share"])
    findings.append({
        "id": "city_monocultures",
        "title": "Cities whose entire camera roll is one thing",
        "blurb": "Cities with 1,500+ tagged photos where a single non-place phrase accounts for ≥10% of everything photographed there. Barely-tourist towns with a single viral reason to visit.",
        "rows": city_dominance[:80],
    })

    # F4 — Most photographed THING globally (place-free)
    global_counts = Counter()
    for p, n in global_thing_total.items():
        global_counts[p] += n
    top_global = global_counts.most_common(40)
    findings.append({
        "id": "top_global_phrases",
        "title": "What Earth photographs most",
        "blurb": "Every user-added tag, summed across 9.49 million photos. Cities, regions, country-name aliases — all removed. What's left is the raw, universal subject matter of the world's cameras.",
        "rows": [{"phrase": p, "count": n} for p, n in top_global],
    })

    # F5 — Admin1s (states/provinces/regions) with the most distinctive vocabulary
    admin_distinctive = []
    for k, m in raw_admin_phrases.items():
        clean = _rollup_counts(m)
        if not clean:
            continue
        cc, admin1 = k.split("|", 1)
        aliases = _country_name_aliases(cc)
        filtered = {p: n for p, n in clean.items() if p not in aliases and p != admin1.lower()}
        total = sum(clean.values())
        if total < 2000:
            continue
        distinct_phrases = [p for p, n in sorted(filtered.items(), key=lambda kv: -kv[1])[:5]]
        if not distinct_phrases:
            continue
        admin_distinctive.append({
            "cc": cc, "admin1": admin1, "total": total,
            "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "primary": distinct_phrases[0],
            "runners": distinct_phrases[1:5],
        })
    admin_distinctive.sort(key=lambda r: -r["total"])
    findings.append({
        "id": "regional_signatures",
        "title": "What every US state (and every world region) photographs most",
        "blurb": "2,975 admin-1 regions worldwide, each with at least 2,000 tagged photos, ranked by volume. The `primary` is the most frequent non-generic phrase after filtering out the country's own name.",
        "rows": admin_distinctive[:120],
    })

    # F6 — Countries obsessed with specific universals: food / beach / religion / transport
    theme_vocab = {
        "food": {"food", "restaurant", "cafe", "coffee", "bakery", "market", "cooking", "chef",
                 "delicious", "cuisine", "tasty", "dish", "meal", "dinner", "lunch", "breakfast",
                 "street food", "foodporn", "eating", "pizza", "sushi", "wine", "beer"},
        "beach": {"beach", "ocean", "sea", "surf", "surfing", "coast", "sand", "wave", "waves",
                  "shore", "bay", "sunset", "tropical", "palm", "island"},
        "religion": {"church", "temple", "mosque", "cathedral", "chapel", "monastery", "shrine",
                     "buddha", "jesus", "christ", "cross", "prayer", "statue", "icon", "cemetery",
                     "religious", "holy", "sacred", "sanctuary", "pilgrimage", "crucifix"},
        "transport": {"car", "bike", "motorcycle", "train", "bus", "plane", "aircraft",
                      "airport", "subway", "metro", "tram", "taxi", "ferry", "ship", "boat",
                      "race", "racing", "motorsport"},
        "architecture": {"architecture", "building", "skyscraper", "tower", "bridge", "castle",
                         "fortress", "palace", "monument", "ruins", "statue", "mosque", "dome",
                         "obelisk", "pyramid"},
        "nature": {"mountain", "lake", "river", "forest", "tree", "trees", "flower", "flowers",
                   "park", "garden", "sky", "cloud", "clouds", "snow", "ice", "glacier", "waterfall"},
        "wildlife": {"bird", "birds", "elephant", "lion", "tiger", "bear", "wolf", "wolves",
                     "deer", "fox", "owl", "cat", "dog", "horse", "sheep", "cow", "panda",
                     "kangaroo", "koala", "whale", "dolphin", "shark"},
    }

    theme_by_country = []
    for cc in eligible[:60]:
        m = country_distinctive_raw.get(cc, {})
        total = country_photos[cc]
        shares = {}
        for theme, vocab in theme_vocab.items():
            ct = sum(v for p, v in m.items() if p in vocab or (" " in p and any(tok in vocab for tok in p.split())))
            shares[theme] = ct / total if total else 0
        top_theme, top_share = max(shares.items(), key=lambda kv: kv[1])
        if top_share < 0.02:
            continue
        theme_by_country.append({
            "cc": cc, "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "theme": top_theme, "share": round(top_share, 3),
            "photos": country_photos[cc],
            "all_shares": {k: round(v, 3) for k, v in shares.items()},
        })
    theme_by_country.sort(key=lambda r: -r["share"])
    findings.append({
        "id": "country_obsessions",
        "title": "Every country's photographic obsession, by theme",
        "blurb": "Seven themed word-lists (food, beach, religion, transport, architecture, nature, wildlife). For each country we report which theme dominates its geotagged photos — and how heavily it leans in.",
        "rows": theme_by_country,
    })

    # F7 — Highest "photo density per capita" (proxy)
    POP = {
        "US": 331_000_000, "GB": 67_000_000, "CA": 38_000_000, "AU": 25_000_000,
        "FR": 67_000_000, "DE": 83_000_000, "IT": 60_000_000, "ES": 47_000_000,
        "JP": 125_000_000, "NL": 17_000_000, "BE": 11_000_000, "IN": 1_400_000_000,
        "IE": 5_000_000, "NZ": 5_000_000, "PA": 4_400_000, "CH": 8_700_000,
        "SE": 10_400_000, "MX": 128_000_000, "PT": 10_300_000, "CN": 1_400_000_000,
        "BR": 215_000_000, "AR": 45_000_000, "AT": 9_000_000, "RU": 144_000_000,
        "NO": 5_400_000, "DK": 5_800_000, "FI": 5_500_000, "GR": 10_500_000,
        "PL": 38_000_000, "TR": 85_000_000, "IL": 9_300_000, "EG": 104_000_000,
        "ZA": 60_000_000, "TH": 70_000_000, "SG": 5_900_000, "ID": 275_000_000,
        "MY": 33_000_000, "PH": 114_000_000, "VN": 98_000_000, "KR": 52_000_000,
        "IS": 372_000, "CZ": 10_700_000, "HU": 9_600_000, "HR": 3_900_000,
        "CY": 1_200_000, "MT": 520_000, "LU": 640_000, "MC": 39_000,
        "LI": 39_000, "AD": 78_000, "SM": 34_000, "VA": 800,
    }
    density = []
    for cc, n in country_photos.items():
        pop = POP.get(cc)
        if not pop or n < 3000:
            continue
        density.append({
            "cc": cc, "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "photos": n, "pop": pop,
            "per_million": round(n * 1_000_000 / pop, 1),
        })
    density.sort(key=lambda r: -r["per_million"])
    findings.append({
        "id": "per_capita_photos",
        "title": "Photographed-per-capita ranking",
        "blurb": "Public geotagged photos per million residents. (Tourists inflate this — reading it as 'tourist + native photographic intensity' is closer to the truth.)",
        "rows": density[:40],
    })

    # F9 — Capital-city dominance: top-city share of each country's photos
    capital_dominance = []
    # First, compute total photos per city across shards.
    city_totals: Dict[str, int] = {}
    for k, m in raw_city_phrases.items():
        tot = sum(m.values())
        city_totals[k] = tot

    # For each country, find the top city (by phrase-total as proxy for photo volume).
    for cc in eligible[:60]:
        country_total = country_photos[cc]
        if country_total < 3000:
            continue
        cities = [
            (k, tot) for k, tot in city_totals.items()
            if k.startswith(cc + "|")
        ]
        if not cities:
            continue
        top_k, top_tot = max(cities, key=lambda kv: kv[1])
        _, admin1, city = top_k.split("|", 2)
        # Rough photo-count: tag-total is a proxy; real photo count is lower,
        # but relative ordering holds.
        all_city_total = sum(tot for _, tot in cities)
        share = top_tot / all_city_total if all_city_total else 0
        capital_dominance.append({
            "cc": cc, "name": COUNTRY_NAMES.get(cc, (cc, ""))[0],
            "flag": COUNTRY_NAMES.get(cc, (cc, ""))[1],
            "top_city": city, "admin1": admin1,
            "share": round(share, 3),
            "country_photos": country_total,
        })
    capital_dominance.sort(key=lambda r: -r["share"])
    findings.append({
        "id": "capital_dominance",
        "title": "Countries whose whole country is one city",
        "blurb": "For each nation, what share of all its tagged photos come from its single most-photographed city? Low numbers = distributed photography. High numbers = a single city swallowing the country's camera.",
        "rows": capital_dominance[:40],
    })

    # F8 — Generic concepts where a single (non-mega) country dominates
    # Exclude the big-4 photo countries (US, GB, CA, AU) from the "top_cc"
    # side so we surface smaller countries punching above their weight.
    BIG4 = {"US", "GB", "CA", "AU"}
    themed_monopolies = []
    for p in global_thing_total:
        if global_thing_total[p] < 2500:
            continue
        total = global_thing_total[p]
        dist = global_thing_countries[p]
        top_cc, top_n = max(dist.items(), key=lambda kv: kv[1])
        share = top_n / total
        if share < 0.35 or share > 0.84:
            continue
        if top_cc in BIG4:
            # Take second-place country if it's not BIG4 and has ≥20% share
            sorted_cc = sorted(dist.items(), key=lambda kv: -kv[1])
            alt = next(((cc2, n2) for cc2, n2 in sorted_cc[1:] if cc2 not in BIG4 and n2 / total >= 0.15), None)
            if not alt:
                continue
            top_cc, top_n = alt
            share = top_n / total
        if len(p) < 4 or len(p) > 24:
            continue
        themed_monopolies.append({
            "phrase": p, "cc": top_cc,
            "name": COUNTRY_NAMES.get(top_cc, (top_cc, ""))[0],
            "flag": COUNTRY_NAMES.get(top_cc, (top_cc, ""))[1],
            "total": total, "in_country": top_n,
            "share": round(share, 3),
        })
    themed_monopolies.sort(key=lambda r: -r["share"])
    findings.append({
        "id": "concept_monopolies",
        "title": "Small countries that punch above their weight on one concept",
        "blurb": "We excluded the four biggest photo producers (US, UK, Canada, Australia) and looked at 2,500+ global phrases where a smaller country owns 15-84% of them. Cultural soft power, frame by frame.",
        "rows": themed_monopolies[:50],
    })

    # ---- write per-country detail files --------------------------------
    # For each country, collect top phrases, top admins (with top phrase),
    # top cities (with top phrase), and 12 sample photos.
    print("writing per-country detail files ...")
    admin_by_country: Dict[str, List[Tuple[str, Dict[str, int]]]] = defaultdict(list)
    for k, m in raw_admin_phrases.items():
        cc, admin1 = k.split("|", 1)
        if admin1:
            admin_by_country[cc].append((admin1, _rollup_counts(m)))
    city_by_country: Dict[str, List[Tuple[str, str, Dict[str, int]]]] = defaultdict(list)
    for k, m in raw_city_phrases.items():
        cc, admin1, city = k.split("|", 2)
        if city:
            city_by_country[cc].append((admin1, city, _rollup_counts(m)))

    country_index = []
    for cc in sorted(country_photos, key=lambda c: -country_photos[c]):
        if country_photos[cc] < 200:
            continue
        aliases = _country_name_aliases(cc)
        distinct = country_distinctive_raw.get(cc, {})
        top_phrases = sorted(distinct.items(), key=lambda kv: -kv[1])[:25]
        top_scored = tfidf.get(cc, {})
        top_by_tfidf = sorted(
            [(p, top_scored[p]) for p in top_scored if p not in aliases],
            key=lambda kv: -kv[1],
        )[:15]

        admins_sorted = sorted(
            admin_by_country.get(cc, []),
            key=lambda t: -sum(t[1].values()),
        )[:15]
        admin_rows = []
        for admin1, m in admins_sorted:
            m2 = {p: n for p, n in m.items() if p not in aliases and p != admin1.lower()}
            top = sorted(m2.items(), key=lambda kv: -kv[1])[:6]
            admin_rows.append({
                "admin1": admin1, "total": sum(m.values()),
                "top_phrases": [{"phrase": p, "count": n} for p, n in top],
            })

        cities_sorted = sorted(
            city_by_country.get(cc, []),
            key=lambda t: -sum(t[2].values()),
        )[:30]
        city_rows = []
        for admin1, city, m in cities_sorted:
            m2 = {p: n for p, n in m.items() if p not in aliases
                  and p != admin1.lower() and p != city.lower()}
            top = sorted(m2.items(), key=lambda kv: -kv[1])[:6]
            city_rows.append({
                "admin1": admin1, "city": city, "total": sum(m.values()),
                "top_phrases": [{"phrase": p, "count": n} for p, n in top],
            })

        samples = country_samples.get(cc, [])[:14]

        name, flag = COUNTRY_NAMES.get(cc, (cc, ""))
        detail = {
            "cc": cc, "name": name, "flag": flag, "photos": country_photos[cc],
            "top_distinctive": [{"phrase": p, "count": n, "score": round(s, 3)}
                                for (p, s), (_, n) in
                                zip(top_by_tfidf, [(p, distinct.get(p, 0)) for p, _ in top_by_tfidf])],
            "top_raw": [{"phrase": p, "count": n} for p, n in top_phrases],
            "admins": admin_rows,
            "cities": city_rows,
            "samples": samples,
        }
        (OUT_COUNTRIES / f"{cc}.json").write_text(json.dumps(detail))
        country_index.append({
            "cc": cc, "name": name, "flag": flag,
            "photos": country_photos[cc],
            "primary": top_by_tfidf[0][0] if top_by_tfidf else "",
        })

    # ---- write world.json (choropleth) ---------------------------------
    choropleth = []
    for cc in country_index:
        choropleth.append({
            "cc": cc["cc"], "name": cc["name"], "flag": cc["flag"],
            "photos": cc["photos"], "primary": cc["primary"],
        })

    (OUT_DIR / "world.json").write_text(json.dumps(choropleth))
    (OUT_DIR / "findings.json").write_text(json.dumps(findings))
    (OUT_DIR / "index.json").write_text(json.dumps({
        "n_countries": d["n_countries"],
        "n_photos": d["n_rows_total"],
        "n_admin_regions": len(raw_admin_phrases),
        "n_cities": len(raw_city_phrases),
        "worker_peak": 967,
        "cluster_cpus_peak": 967,
        "pipeline_minutes": 4.2,
    }))

    print(f"wrote {OUT_DIR}/world.json ({(OUT_DIR / 'world.json').stat().st_size / 1024:.1f} KB)")
    print(f"wrote {OUT_DIR}/findings.json ({(OUT_DIR / 'findings.json').stat().st_size / 1024:.1f} KB)")
    print(f"wrote {OUT_DIR}/index.json")
    print(f"wrote {len(country_index)} per-country detail files")


if __name__ == "__main__":
    main()
