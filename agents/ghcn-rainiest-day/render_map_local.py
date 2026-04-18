"""Render burla_results/map.html from burla_results/top_500.csv.

Used when the remote reduce skipped the map (e.g. no folium on the cluster).
"""

from __future__ import annotations

import csv
from pathlib import Path

import folium


def main() -> int:
    here = Path(__file__).resolve().parent
    csv_path = here / "burla_results" / "top_500.csv"
    out_path = here / "burla_results" / "map.html"
    if not csv_path.exists():
        raise SystemExit(f"missing {csv_path}; run fetch_artifacts.py first")

    rows = []
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            try:
                rank = int(r["rank"])
                lat = float(r["lat"])
                lon = float(r["lon"])
                prcp = float(r["prcp_mm"])
            except (ValueError, KeyError):
                continue
            rows.append(
                {
                    "rank": rank,
                    "lat": lat,
                    "lon": lon,
                    "prcp_mm": prcp,
                    "date": r["date"],
                    "name": r["name"],
                    "country": r["country"],
                }
            )

    if not rows:
        raise SystemExit("no rows to plot")

    center = (rows[0]["lat"], rows[0]["lon"])
    m = folium.Map(location=center, zoom_start=3, tiles="OpenStreetMap")
    for r in rows[:25]:
        color = "red" if r["rank"] == 1 else "blue"
        popup = (
            f"#{r['rank']} · {r['prcp_mm']} mm · {r['date']}<br>"
            f"{r['name']}, {r['country']}"
        )
        folium.CircleMarker(
            location=(r["lat"], r["lon"]),
            radius=10 if r["rank"] == 1 else 5,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=folium.Popup(popup, max_width=320),
        ).add_to(m)

    m.save(str(out_path))
    print(f"wrote {out_path} ({out_path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
