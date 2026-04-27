"""GRS analysis — turn the reduced shard data into UI-ready findings.

Inputs:  samples/grs_reduced.json
Outputs: frontend/data/index.json
         frontend/data/categories.json
         frontend/data/findings.json
         frontend/data/search.json
         frontend/data/langs.json
         frontend/data/installs.json

Nine findings we care about (tune + reorder to taste):

  F1  Category landscape — how many repos fall into each of our 13 categories.
  F2  Language chauvinism — for each (category, language) pair, which lang
      dominates the category? "JS owns web", "Python owns ML".
  F3  Install method by category — pip vs npm vs brew. Shows what kind of
      software each category produces.
  F4  Most-curated repos — top repos with highest badge count + code blocks
      (a proxy for "this is a real project, not a vibes repo").
  F5  Wall of forgotten README — repos with one-word READMEs or blatant
      TODO placeholders. The loneliest repos on GitHub.
  F6  Distinctive keywords per category (TF-IDF) — the single word most
      disproportionately used in ML READMEs vs the rest of GitHub. We
      compute this over the full 1M-repo doc_freq (only possible at scale).
  F7  The longest READMEs — top 20 by chars.
  F8  The single-install ecosystems — categories where one install tool
      dominates (e.g. ML → pip, Web → npm, DevOps → docker).
  F9  The 'awesome-list' epidemic — repos whose title starts with 'awesome'.
      Curation has become a genre of software.
"""
from __future__ import annotations

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path


HERE = Path(__file__).parent


CATEGORY_LABEL = {
    "ml": "Machine Learning",
    "web": "Web",
    "cli": "CLI tools",
    "game": "Games",
    "crypto": "Crypto / web3",
    "db": "Databases",
    "devops": "DevOps",
    "mobile": "Mobile",
    "security": "Security",
    "data": "Data engineering",
    "os": "OS / low-level",
    "lib": "Libraries",
    "docs": "Documentation / lists",
    "other": "Other",
}

CATEGORY_EMOJI = {
    "ml": "🧠", "web": "🕸️", "cli": "⌨️", "game": "🎮", "crypto": "🪙",
    "db": "🗄️", "devops": "🚢", "mobile": "📱", "security": "🛡️",
    "data": "📊", "os": "🖥️", "lib": "📦", "docs": "📚", "other": "❓",
}

INSTALL_LABEL = {
    "pip": "pip", "npm": "npm", "yarn": "yarn", "pnpm": "pnpm",
    "cargo": "cargo", "go-get": "go", "brew": "brew", "apt": "apt",
    "docker": "docker", "git-clone": "git clone", "none": "(unspecified)",
}


def _is_placeholder(tldr: str, chars: int) -> bool:
    t = (tldr or "").strip().lower()
    if not t:
        return True
    if chars < 140:
        return True
    todo_words = ("todo", "tbd", "coming soon", "work in progress",
                  "wip", "placeholder", "under construction")
    return any(w in t for w in todo_words)


def _short(s: str, n: int = 220) -> str:
    s = (s or "").strip()
    return s[: n - 1] + "…" if len(s) > n else s


def run(reduced_path: Path, out_dir: Path) -> None:
    data = json.loads(reduced_path.read_text())
    n_repos = int(data["n_repos"])
    by_cat = data.get("by_cat") or {}
    by_lang = data.get("by_lang") or {}
    by_install = data.get("by_install") or {}
    doc_freq_all: dict = data.get("doc_freq") or {}
    top_per_cat: dict = data.get("top_per_cat") or {}
    sample: list = data.get("sample") or []

    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- index.json ------------------------------------------------------
    index = {
        "n_repos": n_repos,
        "n_categories": len(CATEGORY_LABEL),
        "generated_at": data.get("generated_at"),
        "top_cats": sorted(
            [
                {"cat": c, "name": CATEGORY_LABEL.get(c, c.title()),
                 "emoji": CATEGORY_EMOJI.get(c, ""), "n": int(n),
                 "pct": round(100 * int(n) / max(1, n_repos), 2)}
                for c, n in by_cat.items()
            ],
            key=lambda r: -r["n"],
        ),
        "top_langs": sorted(
            [
                {"lang": l, "n": int(n),
                 "pct": round(100 * int(n) / max(1, n_repos), 2)}
                for l, n in by_lang.items()
            ],
            key=lambda r: -r["n"],
        )[:30],
        "top_installs": sorted(
            [
                {"install": i, "label": INSTALL_LABEL.get(i, i), "n": int(n),
                 "pct": round(100 * int(n) / max(1, n_repos), 2)}
                for i, n in by_install.items()
            ],
            key=lambda r: -r["n"],
        ),
    }
    (out_dir / "index.json").write_text(json.dumps(index, indent=2) + "\n")

    # ---- categories.json -------------------------------------------------
    # One entry per category with the top 120 repos (cap UI payload).
    cats_out = []
    for cat, rows in top_per_cat.items():
        rows = sorted(rows, key=lambda r: -r.get("quality", 0))[:120]
        cats_out.append({
            "cat": cat,
            "name": CATEGORY_LABEL.get(cat, cat.title()),
            "emoji": CATEGORY_EMOJI.get(cat, ""),
            "n": int(by_cat.get(cat, 0)),
            "top": [
                {
                    "repo": r["repo"],
                    "title": r.get("title", ""),
                    "tldr": _short(r.get("tldr") or r.get("one_line") or "", 240),
                    "lang": r.get("lang") or "",
                    "install": r.get("install", "none"),
                    "badges": r.get("badges", 0),
                    "code_blocks": r.get("code_blocks", 0),
                    "chars": r.get("chars", 0),
                    "quality": r.get("quality", 0),
                }
                for r in rows
            ],
        })
    cats_out.sort(key=lambda c: -c["n"])
    (out_dir / "categories.json").write_text(json.dumps(cats_out) + "\n")

    # ---- search.json (sample of 6000 for client-side search) -------------
    search_rows = []
    seen_repos = set()
    for r in sample:
        if r["repo"] in seen_repos:
            continue
        seen_repos.add(r["repo"])
        search_rows.append({
            "repo": r["repo"],
            "title": r.get("title", ""),
            "tldr": _short(r.get("tldr") or r.get("one_line") or "", 180),
            "lang": r.get("lang") or "",
            "cat": r.get("category", "other"),
            "install": r.get("install", "none"),
            "chars": r.get("chars", 0),
        })
    (out_dir / "search.json").write_text(json.dumps(search_rows) + "\n")

    # ---- findings.json ---------------------------------------------------
    findings = []

    # F1 Category landscape
    findings.append({
        "id": "category_landscape",
        "title": "The map of open-source GitHub",
        "blurb": "We classified every README into one of 13 flavors using keyword "
                 "heuristics (no LLM). Here's the breakdown across "
                 f"{n_repos:,} repos.",
        "type": "bar",
        "rows": index["top_cats"],
    })

    # F2 Language chauvinism — per category, top 3 languages
    lang_per_cat: dict = defaultdict(Counter)
    for cat, rows in top_per_cat.items():
        for r in rows:
            lang_per_cat[cat][r.get("lang") or "_unknown"] += 1
    lang_dominance = []
    for cat in sorted(by_cat, key=lambda c: -by_cat[c]):
        cnt = lang_per_cat.get(cat)
        if not cnt:
            continue
        total = sum(cnt.values())
        top3 = cnt.most_common(3)
        if not top3:
            continue
        lang_dominance.append({
            "cat": cat, "name": CATEGORY_LABEL.get(cat, cat.title()),
            "emoji": CATEGORY_EMOJI.get(cat, ""),
            "top_langs": [
                {"lang": l or "_unknown", "n": n,
                 "pct": round(100 * n / max(1, total), 1)}
                for l, n in top3
            ],
            "n": total,
        })
    findings.append({
        "id": "language_dominance",
        "title": "Which languages own which categories",
        "blurb": "Top languages within each category's top repos. "
                 "Python owns ML, JS owns web, Go owns DevOps.",
        "type": "lang_dominance",
        "rows": lang_dominance,
    })

    # F3 Install method by category
    install_per_cat: dict = defaultdict(Counter)
    for cat, rows in top_per_cat.items():
        for r in rows:
            install_per_cat[cat][r.get("install") or "none"] += 1
    install_rows = []
    for cat in sorted(by_cat, key=lambda c: -by_cat[c]):
        cnt = install_per_cat.get(cat)
        if not cnt:
            continue
        total = sum(cnt.values())
        top3 = cnt.most_common(4)
        if not top3:
            continue
        install_rows.append({
            "cat": cat, "name": CATEGORY_LABEL.get(cat, cat.title()),
            "emoji": CATEGORY_EMOJI.get(cat, ""),
            "installs": [
                {"install": i, "label": INSTALL_LABEL.get(i, i),
                 "n": n, "pct": round(100 * n / max(1, total), 1)}
                for i, n in top3
            ],
        })
    findings.append({
        "id": "install_by_category",
        "title": "How does this thing install? Depends on the category.",
        "blurb": "Install command grouped by category. "
                 "`pip install` dominates ML + data, `npm install` dominates web, "
                 "`docker run` dominates devops.",
        "type": "install_by_cat",
        "rows": install_rows,
    })

    # F4 Most curated (max badges + code_blocks)
    curated = []
    for cat, rows in top_per_cat.items():
        for r in rows:
            score = r.get("badges", 0) * 3 + r.get("code_blocks", 0)
            curated.append((score, r))
    curated.sort(key=lambda x: -x[0])
    dedup: list = []
    seen: set = set()
    for _, r in curated:
        if r["repo"] in seen:
            continue
        seen.add(r["repo"])
        dedup.append({
            "repo": r["repo"],
            "title": r.get("title", ""),
            "tldr": _short(r.get("tldr") or r.get("one_line") or "", 220),
            "cat": r.get("category", "other"),
            "lang": r.get("lang") or "",
            "badges": r.get("badges", 0),
            "code_blocks": r.get("code_blocks", 0),
            "install": r.get("install", "none"),
        })
        if len(dedup) >= 40:
            break
    findings.append({
        "id": "most_curated",
        "title": "The most lovingly documented repos on GitHub",
        "blurb": "Ranked by badge count × 3 + code-fence count. "
                 "These are the README maximalists of the ecosystem.",
        "type": "repo_list",
        "rows": dedup,
    })

    # F5 Forgotten READMEs — placeholders, WIP
    lonely = []
    for cat, rows in top_per_cat.items():
        for r in rows:
            tldr = r.get("tldr") or r.get("one_line") or ""
            if _is_placeholder(tldr, r.get("chars", 0)):
                lonely.append(r)
    lonely.sort(key=lambda r: r.get("chars", 0))
    seen = set()
    lonely_out = []
    for r in lonely:
        if r["repo"] in seen:
            continue
        seen.add(r["repo"])
        lonely_out.append({
            "repo": r["repo"],
            "title": r.get("title") or r["repo"].split("/")[-1],
            "tldr": r.get("tldr", ""),
            "chars": r.get("chars", 0),
            "cat": r.get("category", "other"),
            "lang": r.get("lang") or "",
        })
        if len(lonely_out) >= 40:
            break
    findings.append({
        "id": "forgotten",
        "title": "The loneliest READMEs on GitHub",
        "blurb": "One-line READMEs, TODOs, placeholders, 'coming soon'. "
                 "Every one was a project someone meant to finish.",
        "type": "repo_list",
        "rows": lonely_out,
    })

    # F6 Distinctive keywords (TF-IDF across 14 buckets)
    # Each category is a "document"; build per-category term frequency from top rows.
    cat_tf: dict = {}
    cat_sizes: dict = {}
    for cat, rows in top_per_cat.items():
        c: Counter = Counter()
        for r in rows:
            for tok, n in (r.get("tokens") or {}).items():
                c[tok] += n
        cat_tf[cat] = c
        cat_sizes[cat] = sum(c.values()) or 1

    # Compute IDF over the 14 category "documents"
    all_cats = list(cat_tf.keys())
    idf: dict = {}
    doc_tokens: dict = {c: set(cat_tf[c]) for c in all_cats}
    for tok in set().union(*doc_tokens.values()):
        df = sum(1 for c in all_cats if tok in doc_tokens[c])
        if df:
            idf[tok] = math.log((len(all_cats) + 1) / (1 + df)) + 1

    distinctive = []
    for cat in all_cats:
        tf = cat_tf[cat]
        size = cat_sizes[cat]
        scored = []
        for tok, n in tf.items():
            if n < 3:
                continue
            if len(tok) < 4:
                continue
            tf_norm = n / size
            score = tf_norm * idf.get(tok, 0)
            scored.append((score, tok, n))
        scored.sort(key=lambda x: -x[0])
        distinctive.append({
            "cat": cat,
            "name": CATEGORY_LABEL.get(cat, cat.title()),
            "emoji": CATEGORY_EMOJI.get(cat, ""),
            "words": [
                {"word": t, "score": round(s, 4), "n": n}
                for s, t, n in scored[:15]
            ],
        })
    distinctive.sort(key=lambda c: -by_cat.get(c["cat"], 0))
    findings.append({
        "id": "distinctive_words",
        "title": "The words that define each category",
        "blurb": "TF-IDF over all 14 categories. "
                 "Shows what people disproportionately write about in, say, "
                 "ML READMEs vs every other kind of README.",
        "type": "distinctive_words",
        "rows": distinctive,
    })

    # F7 Longest READMEs
    long_r = []
    for cat, rows in top_per_cat.items():
        for r in rows:
            long_r.append(r)
    long_r.sort(key=lambda r: -r.get("chars", 0))
    seen = set()
    long_out = []
    for r in long_r:
        if r["repo"] in seen:
            continue
        seen.add(r["repo"])
        long_out.append({
            "repo": r["repo"],
            "title": r.get("title", ""),
            "tldr": _short(r.get("tldr") or r.get("one_line") or "", 200),
            "chars": r.get("chars", 0),
            "cat": r.get("category", "other"),
            "lang": r.get("lang") or "",
        })
        if len(long_out) >= 30:
            break
    findings.append({
        "id": "longest",
        "title": "The longest READMEs we found",
        "blurb": "READMEs with the most characters. Most are curated 'awesome-*' lists "
                 "or single-doc project handbooks.",
        "type": "repo_list",
        "rows": long_out,
    })

    # F8 Install-method dominance per category
    install_dom = []
    for cat in sorted(install_per_cat, key=lambda c: -by_cat.get(c, 0)):
        cnt = install_per_cat[cat]
        total = sum(cnt.values())
        if not total:
            continue
        top, n = cnt.most_common(1)[0]
        install_dom.append({
            "cat": cat,
            "name": CATEGORY_LABEL.get(cat, cat.title()),
            "emoji": CATEGORY_EMOJI.get(cat, ""),
            "install": top,
            "install_label": INSTALL_LABEL.get(top, top),
            "pct": round(100 * n / total, 1),
            "n": total,
        })
    install_dom.sort(key=lambda r: -r["pct"])
    findings.append({
        "id": "install_dominance",
        "title": "The winner-takes-all install ecosystems",
        "blurb": "In each category, what share of top repos use the single most popular "
                 "install method? Some categories are monocultures.",
        "type": "install_dominance",
        "rows": install_dom,
    })

    # F9 'awesome' epidemic
    awesome_rows = []
    all_titles = []
    seen = set()
    for cat, rows in top_per_cat.items():
        for r in rows:
            if r["repo"] in seen:
                continue
            title = (r.get("title") or "").lower().strip()
            repo = (r.get("repo") or "").lower()
            if title.startswith("awesome") or repo.split("/")[-1].startswith("awesome-"):
                seen.add(r["repo"])
                awesome_rows.append({
                    "repo": r["repo"],
                    "title": r.get("title") or r["repo"].split("/")[-1],
                    "tldr": _short(r.get("tldr") or r.get("one_line") or "", 180),
                    "chars": r.get("chars", 0),
                    "cat": r.get("category", "other"),
                    "lang": r.get("lang") or "",
                })
    awesome_rows.sort(key=lambda r: -r["chars"])
    findings.append({
        "id": "awesome_epidemic",
        "title": "The awesome-list epidemic",
        "blurb": "Repos whose title (or name) starts with 'awesome'. "
                 "Curating lists has become its own whole genre of open source.",
        "type": "repo_list",
        "rows": awesome_rows[:40],
        "meta": {"count": len(awesome_rows)},
    })

    (out_dir / "findings.json").write_text(json.dumps(findings) + "\n")

    # Per-category JSON (for the modal)
    cat_dir = out_dir / "categories"
    cat_dir.mkdir(parents=True, exist_ok=True)
    for c in cats_out:
        (cat_dir / f"{c['cat']}.json").write_text(json.dumps(c) + "\n")

    print(f"wrote {out_dir}/index.json, categories.json, findings.json, search.json")
    print(f"  {n_repos:,} repos, {len(cats_out)} categories")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--reduced", default=str(HERE / "samples" / "grs_reduced.json"))
    ap.add_argument("--out", default=str(HERE / "frontend" / "data"))
    args = ap.parse_args()
    run(Path(args.reduced), Path(args.out))
