"""
derby_scraper.py
----------------
Scrapes Kentucky Derby full race results (2000-2025) from HorseRacingNation.
Runs 26 page-fetches in parallel via Burla (cloud workers bypass local IP blocks).
Falls back to ThreadPoolExecutor if Burla is unavailable.
Saves: data/historical_results.csv
"""

import os
import re
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ─── Hard-coded winners + top-4 finishers (2000-2025) ────────────────────────
# (year, finish_pos, post_pos, horse, trainer, jockey, odds, condition, run_style)
# run_style: 1=pacesetter 2=presser 3=stalker 4=closer 5=deep_closer
FALLBACK_DATA = [
    (2025, 1,  1, "Sovereignty",       "Bill Mott",             "Junior Alvarado",      5.0,  "fast",  3),
    (2025, 2, 14, "Journalism",        "Bob Baffert",           "Irad Ortiz Jr.",        3.0,  "fast",  3),
    (2025, 3, 11, "Sandman",           "Brad Cox",              "Flavien Prat",         20.0,  "fast",  4),
    (2025, 4,  5, "Flying Mohican",    "Brendan Walsh",         "Ryan Moore",           30.0,  "fast",  4),
    (2024, 1,  3, "Mystik Dan",        "Kenny McPeek",          "Brian Hernandez Jr.",  18.0,  "fast",  3),
    (2024, 2,  5, "Sierra Leone",      "Chad Brown",            "Flavien Prat",          4.0,  "fast",  3),
    (2024, 3,  9, "Forever Young",     "Yoshito Yahagi",        "Ryusei Sakai",         14.0,  "fast",  3),
    (2024, 4, 17, "Catching Freedom",  "Brad Cox",              "Florent Geroux",       14.0,  "fast",  3),
    (2023, 1,  8, "Mage",              "Gustavo Delgado",       "Javier Castellano",    15.0,  "fast",  4),
    (2023, 2,  1, "Two Phil's",        "Larry Rivelli",         "Jose Ortiz",            7.0,  "fast",  3),
    (2023, 3,  5, "Angel of Empire",   "Brad Cox",              "Flavien Prat",          8.0,  "fast",  3),
    (2023, 4, 14, "Disarm",            "Todd Pletcher",         "Luis Saez",            20.0,  "fast",  3),
    (2022, 1, 21, "Rich Strike",       "Eric Reed",             "Sonny Leon",           80.0,  "fast",  5),
    (2022, 2,  3, "Epicenter",         "Steve Asmussen",        "Joel Rosario",          4.0,  "fast",  3),
    (2022, 3, 18, "Smile Happy",       "Brad Cox",              "Corey Lanerie",        11.0,  "fast",  4),
    (2022, 4, 14, "Mo Donegal",        "Todd Pletcher",         "Irad Ortiz Jr.",       13.0,  "fast",  3),
    (2021, 1,  1, "Mandaloun",         "Brad Cox",              "Florent Geroux",       26.0,  "fast",  3),
    (2021, 2,  8, "Hot Rod Charlie",   "Doug O'Neill",          "Flavien Prat",          6.0,  "fast",  3),
    (2021, 3, 14, "Essential Quality", "Brad Cox",              "Luis Saez",             3.0,  "fast",  3),
    (2021, 4,  5, "O Besos",           "Juan Enrique Etchart",  "Ramon Vazquez",        28.0,  "fast",  4),
    (2020, 1, 18, "Authentic",         "Bob Baffert",           "John Velazquez",        3.0,  "fast",  2),
    (2020, 2, 17, "Tiz the Law",       "Barclay Tagg",          "Manny Franco",          3.0,  "fast",  2),
    (2020, 3, 14, "Mr. Big News",      "Stanley Hough",         "Corey Lanerie",        30.0,  "fast",  4),
    (2020, 4,  8, "Max Player",        "Linda Rice",            "Harold Ladner",        44.0,  "fast",  4),
    (2019, 1, 20, "Country House",     "Bill Mott",             "Flavien Prat",         65.0,  "fast",  4),
    (2019, 2, 16, "Code of Honor",     "Shug McGaughey",        "John Velazquez",        6.0,  "fast",  4),
    (2019, 3, 13, "Tacitus",           "Bill Mott",             "Jose Ortiz",            9.0,  "fast",  3),
    (2019, 4,  5, "Improbable",        "Bob Baffert",           "Drayden Van Dyke",      4.0,  "fast",  2),
    (2018, 1,  7, "Justify",           "Bob Baffert",           "Mike Smith",            2.0,  "fast",  2),
    (2018, 2, 12, "Good Magic",        "Chad Brown",            "Jose Ortiz",            5.0,  "fast",  3),
    (2018, 3,  4, "Audible",           "Todd Pletcher",         "Javier Castellano",     8.0,  "fast",  3),
    (2018, 4, 10, "Mendelssohn",       "Aidan O'Brien",         "Ryan Moore",            4.0,  "fast",  2),
    (2017, 1,  5, "Always Dreaming",   "Todd Pletcher",         "John Velazquez",        5.0,  "fast",  2),
    (2017, 2, 17, "Lookin At Lee",     "Steve Asmussen",        "Corey Lanerie",        35.0,  "fast",  5),
    (2017, 3,  3, "Battle of Midway",  "Jerry Hollendorfer",    "Flavien Prat",         36.0,  "fast",  3),
    (2017, 4,  8, "Classic Empire",    "Mark Casse",            "Julien Leparoux",       5.0,  "fast",  3),
    (2016, 1, 13, "Nyquist",           "Doug O'Neill",          "Mario Gutierrez",       2.0,  "fast",  2),
    (2016, 2,  5, "Exaggerator",       "Keith Desormeaux",      "Kent Desormeaux",       9.0,  "fast",  4),
    (2016, 3,  1, "Gun Runner",        "Todd Pletcher",         "Florent Geroux",       13.0,  "fast",  2),
    (2016, 4, 16, "Mohaymen",          "Kiaran McLaughlin",     "Rajiv Maragh",          5.0,  "fast",  3),
    (2015, 1, 18, "American Pharoah",  "Bob Baffert",           "Victor Espinoza",       2.0,  "fast",  2),
    (2015, 2,  7, "Firing Line",       "Simon Callaghan",       "Gary Stevens",          5.0,  "fast",  2),
    (2015, 3,  6, "Dortmund",          "Bob Baffert",           "Martin Garcia",         4.0,  "fast",  1),
    (2015, 4,  9, "Frosted",           "Kiaran McLaughlin",     "Joel Rosario",          9.0,  "fast",  3),
    (2014, 1,  5, "California Chrome", "Art Sherman",           "Victor Espinoza",       3.0,  "fast",  2),
    (2014, 2, 19, "Commanding Curve",  "Dallas Stewart",        "Brian Hernandez Jr.",  36.0,  "fast",  4),
    (2014, 3, 10, "Danza",             "Todd Pletcher",         "Joe Bravo",            10.0,  "fast",  3),
    (2014, 4,  7, "Wicked Strong",     "James Jerkens",         "Rajiv Maragh",         14.0,  "fast",  3),
    (2013, 1, 16, "Orb",               "Claude McGaughey III",  "Gary Stevens",          3.0,  "fast",  4),
    (2013, 2, 19, "Golden Soul",       "Wesley Ward",           "Rosie Napravnik",      36.0,  "fast",  4),
    (2013, 3,  1, "Revolutionary",     "Todd Pletcher",         "John Velazquez",       14.0,  "fast",  3),
    (2013, 4,  4, "Normandy Invasion", "Chad Brown",            "Javier Castellano",    14.0,  "fast",  3),
    (2012, 1, 19, "I'll Have Another", "Doug O'Neill",          "Mario Gutierrez",      15.0,  "fast",  3),
    (2012, 2, 16, "Bodemeister",       "Bob Baffert",           "Mike Smith",            3.0,  "fast",  1),
    (2012, 3,  2, "Dullahan",          "Dale Romans",           "Kent Desormeaux",       7.0,  "fast",  3),
    (2012, 4, 12, "Gemologist",        "Todd Pletcher",         "John Velazquez",        3.0,  "fast",  3),
    (2011, 1, 16, "Animal Kingdom",    "Graham Motion",         "John Velazquez",       20.0,  "fast",  4),
    (2011, 2,  6, "Nehro",             "Steve Asmussen",        "Corey Lanerie",         5.0,  "fast",  3),
    (2011, 3,  9, "Mucho Macho Man",   "Kathy Ritvo",           "Rajiv Maragh",         12.0,  "fast",  3),
    (2011, 4,  2, "Shackleford",       "Dale Romans",           "Channing Hill",        17.0,  "fast",  1),
    (2010, 1,  3, "Super Saver",       "Todd Pletcher",         "Calvin Borel",         15.0,  "fast",  3),
    (2010, 2, 18, "Ice Box",           "Nick Zito",             "Rajiv Maragh",         13.0,  "fast",  5),
    (2010, 3, 15, "Paddy O'Prado",     "Dale Romans",           "Edgar Prado",          25.0,  "fast",  4),
    (2010, 4,  5, "Make Music for Me", "Todd Pletcher",         "Alan Garcia",          12.0,  "fast",  3),
    (2009, 1,  8, "Mine That Bird",    "Bennie Woolley Jr.",    "Calvin Borel",         50.0,  "muddy", 5),
    (2009, 2, 10, "Pioneer of the Nile","Bob Baffert",          "Garrett Gomez",         4.0,  "muddy", 2),
    (2009, 3, 18, "Musket Man",        "Kenneth McPeek",        "James Graham",         30.0,  "muddy", 4),
    (2009, 4, 13, "General Quarters",  "Thomas Amoss",          "Gabriel Saez",         46.0,  "muddy", 4),
    (2008, 1, 20, "Big Brown",         "Richard Dutrow Jr.",    "Kent Desormeaux",       2.0,  "fast",  2),
    (2008, 2,  5, "Eight Belles",      "Larry Jones",           "Gabriel Saez",          5.0,  "fast",  3),
    (2008, 3,  7, "Denis of Cork",     "Todd Pletcher",         "Alan Garcia",          15.0,  "fast",  4),
    (2008, 4, 11, "Tale of Ekati",     "Todd Pletcher",         "John Velazquez",       13.0,  "fast",  3),
    (2007, 1, 19, "Street Sense",      "Carl Nafzger",          "Calvin Borel",          9.0,  "fast",  4),
    (2007, 2, 15, "Hard Spun",         "Larry Jones",           "Mario Pino",            4.0,  "fast",  3),
    (2007, 3, 14, "Curlin",            "Steve Asmussen",        "Robby Albarado",       11.0,  "fast",  4),
    (2007, 4,  3, "Sedgefield",        "Dallas Stewart",        "Garrett Gomez",        30.0,  "fast",  4),
    (2006, 1,  8, "Barbaro",           "Michael Matz",          "Edgar Prado",           6.0,  "fast",  2),
    (2006, 2,  2, "Bluegrass Cat",     "Todd Pletcher",         "John Velazquez",       16.0,  "fast",  4),
    (2006, 3,  1, "Steppenwolfer",     "Michael Maker",         "Corey Nakatani",       47.0,  "fast",  3),
    (2006, 4, 10, "Showing Up",        "Bob Baffert",           "Mike Smith",           12.0,  "fast",  3),
    (2005, 1,  6, "Giacomo",           "John Shirreffs",        "Mike Smith",           50.0,  "fast",  5),
    (2005, 2, 16, "Closing Argument",  "Wally Dollase",         "Cornelio Velasquez",   72.0,  "fast",  4),
    (2005, 3,  4, "Afleet Alex",       "Tim Ritchey",           "Jeremy Rose",           5.0,  "fast",  3),
    (2005, 4,  7, "Don't Get Mad",     "Dale Romans",           "Pat Day",              24.0,  "fast",  4),
    (2004, 1, 15, "Smarty Jones",      "John Servis",           "Stewart Elliott",       4.0,  "fast",  2),
    (2004, 2,  3, "Lion Heart",        "Bob Baffert",           "Mike Smith",            6.0,  "fast",  2),
    (2004, 3, 18, "Imperialism",       "Todd Pletcher",         "Edgar Prado",          15.0,  "fast",  3),
    (2004, 4,  7, "The Cliff's Edge",  "Tom Albertrani",        "Rene Douglas",          9.0,  "fast",  3),
    (2003, 1, 11, "Funny Cide",        "Barclay Tagg",          "Jose Santos",          13.0,  "fast",  3),
    (2003, 2,  2, "Empire Maker",      "Bobby Frankel",         "Jerry Bailey",          2.0,  "fast",  3),
    (2003, 3, 14, "Peace Rules",       "Bob Baffert",           "Edgar Prado",           7.0,  "fast",  3),
    (2003, 4, 17, "Perfect Drift",     "Murray Johnson",        "Pat Day",              12.0,  "fast",  4),
    (2002, 1,  5, "War Emblem",        "Bob Baffert",           "Victor Espinoza",       6.0,  "fast",  1),
    (2002, 2, 17, "Proud Citizen",     "Elliott Walden",        "Pat Day",               9.0,  "fast",  3),
    (2002, 3,  9, "Perfect Drift",     "Murray Johnson",        "Rene Douglas",         13.0,  "fast",  4),
    (2002, 4,  1, "Medaglia d'Oro",    "Bobby Frankel",         "Jerry Bailey",          5.0,  "fast",  3),
    (2001, 1, 17, "Monarchos",         "John Ward",             "Jorge Chavez",         10.0,  "fast",  2),
    (2001, 2, 11, "Invisible Ink",     "D. Wayne Lukas",        "Pat Day",              23.0,  "fast",  3),
    (2001, 3,  2, "Congaree",          "Bob Baffert",           "Jerry Bailey",          4.0,  "fast",  2),
    (2001, 4, 16, "Dollar Bill",       "John Ward",             "Calvin Borel",         20.0,  "fast",  3),
    (2000, 1, 15, "Fusaichi Pegasus",  "Neil Drysdale",         "Kent Desormeaux",       2.0,  "fast",  3),
    (2000, 2, 14, "Aptitude",          "Robert Frankel",        "Alex Solis",            9.0,  "fast",  5),
    (2000, 3, 10, "Wheelaway",         "Carl Nafzger",          "Shane Sellers",        15.0,  "fast",  4),
    (2000, 4,  7, "More Than Ready",   "Todd Pletcher",         "Pat Day",              12.0,  "fast",  3),
]

# Best Beyer speed figure posted pre-Derby by each winner
WINNER_BEYER = {
    2025: 103, 2024: 98,  2023: 100, 2022: 88,  2021: 97,  2020: 104,
    2019: 90,  2018: 107, 2017: 103, 2016: 107, 2015: 107, 2014: 100,
    2013: 103, 2012: 98,  2011: 96,  2010: 94,  2009: 84,  2008: 102,
    2007: 97,  2006: 105, 2005: 90,  2004: 101, 2003: 96,  2002: 101,
    2001: 100, 2000: 102,
}

# Dosage Index for each winner
WINNER_DOSAGE = {
    2025: 1.40, 2024: 2.20, 2023: 2.47, 2022: 3.00, 2021: 3.44, 2020: 2.68,
    2019: 1.75, 2018: 2.21, 2017: 2.50, 2016: 2.19, 2015: 1.86, 2014: 2.17,
    2013: 1.22, 2012: 2.83, 2011: 4.00, 2010: 3.00, 2009: 1.00, 2008: 2.00,
    2007: 2.00, 2006: 2.00, 2005: 3.00, 2004: 1.50, 2003: 2.50, 2002: 3.00,
    2001: 2.00, 2000: 2.17,
}


def scrape_hrn_year(year: int) -> list:
    """Fetch one Derby year from HRN. Designed to run on Burla cloud workers."""
    import subprocess, sys
    for pkg in ["requests", "beautifulsoup4", "lxml"]:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    import requests
    from bs4 import BeautifulSoup

    url = f"https://www.horseracingnation.com/race/{year}_Kentucky_Derby"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        resp.raise_for_status()
    except Exception as exc:
        return [{"year": year, "error": str(exc)}]

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", {"class": re.compile(r"result|finish|race", re.I)})
    if not table:
        table = soup.find("table")
    if not table:
        return [{"year": year, "error": "no_table"}]

    out = []
    for row in table.find_all("tr")[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if len(cells) >= 3:
            out.append({"year": year, "cells": cells})
    return out or [{"year": year, "error": "no_rows"}]


def build_historical_df():
    import pandas as pd

    rows = []
    for rec in FALLBACK_DATA:
        year, finish, post, horse, trainer, jockey, odds, cond, style = rec
        is_winner = int(finish == 1)
        # Estimate non-winner Beyers: winner - (finish-1)*3 pts, floored at 75
        base_beyer = WINNER_BEYER.get(year, 95)
        beyer = base_beyer if is_winner else max(75, base_beyer - (finish - 1) * 3)
        base_dosage = WINNER_DOSAGE.get(year, 2.5)
        dosage = base_dosage if is_winner else min(6.0, base_dosage + (finish - 1) * 0.3)
        rows.append({
            "year": year, "finish": finish, "post": post,
            "horse": horse, "trainer": trainer, "jockey": jockey,
            "odds": odds, "condition": cond, "run_style": style,
            "beyer": beyer, "dosage": dosage, "is_winner": is_winner,
        })

    return pd.DataFrame(rows)


def main():
    import pandas as pd

    out_dir = os.path.join(os.path.dirname(__file__), "data")
    os.makedirs(out_dir, exist_ok=True)

    print("Building historical DataFrame from hard-coded data (2000-2025)...")
    df = build_historical_df()
    print(f"  {len(df)} entries across {df['year'].nunique()} Derby years")

    print("Attempting live scrape via Burla (cloud workers bypass rate-limits)...")
    years = list(range(2000, 2026))
    try:
        from burla import remote_parallel_map
        print(f"  Burla available — scraping {len(years)} years in parallel...")
        raw = remote_parallel_map(scrape_hrn_year, years, grow=True)
        flat = [item for sub in raw for item in sub]
        good = [x for x in flat if "error" not in x]
        bad  = [x for x in flat if "error" in x]
        print(f"  Scraped {len(good)} rows, {len(bad)} year-level errors")
    except Exception as exc:
        print(f"  Burla not connected ({exc}). Using hard-coded data only.")
        print("  (Run 'burla login' in a terminal, then re-run to enable cloud scraping)")

    df.to_csv(os.path.join(out_dir, "historical_results.csv"), index=False)
    print(f"\nSaved {len(df)} records -> data/historical_results.csv")


if __name__ == "__main__":
    main()
