"""GitHub Repo Summarizer — worker pipeline.

A Burla worker reads a sharded slice of `/workspace/shared/grs/readmes.parquet`
and, for every README in its slice, computes a compact deterministic summary:

  - title                  (first H1 or fallback to repo name)
  - one_line               (first prose paragraph, 200 chars max)
  - lang                   (primary language from BigQuery languages table)
  - install                (pip | npm | yarn | cargo | go | brew | apt | docker | none)
  - category               (ml | web | cli | game | crypto | db | devops |
                            mobile | security | data | os | lib | docs | other)
  - category_scores        dict: every matched category → rule score
  - badges                 number of shields.io / travis / coveralls badges
  - code_blocks            number of ``` code fences
  - chars                  README length in characters
  - tokens                 dict of top 40 (alpha-only, deduped) tokens → local TF
  - stars_ish              -1 if unknown; pulled from languages bytes sum as a
                           weak popularity signal
  - tldr                   1-sentence extract ("this is/this project/does")

All heuristics. No LLM. 100% deterministic. The map layer hits 500+ workers,
the reduce layer merges per-category top lists, and analysis computes TF-IDF
to find distinctive keywords per category — something only possible at scale.

Runs entirely on the Burla shared filesystem. Each worker writes ONE file:
  /workspace/shared/grs/shards/{shard_id:04d}.json
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import Counter
from typing import Any, Dict, List, Tuple


SHARD_OUT = "/workspace/shared/grs/shards"
PARQUET_PATH = "/workspace/shared/grs/readmes.parquet"


# ---- Category heuristics ---------------------------------------------------

# Each category is a dict of weighted keyword patterns. Word-boundary matched,
# case-insensitive. Score = sum of (pattern_weight × occurrences in README).
CATEGORIES: Dict[str, Dict[str, int]] = {
    "ml": {
        "tensorflow": 4, "pytorch": 4, "keras": 3, "scikit-learn": 3, "sklearn": 3,
        "huggingface": 3, "transformers": 3, "neural network": 3, "deep learning": 3,
        "machine learning": 3, "mlflow": 2, "xgboost": 2, "lightgbm": 2,
        "model": 1, "training": 1, "inference": 2, "classifier": 2, "embedding": 2,
        "gradient": 2, "backpropagation": 3, "onnx": 3, "cuda": 2, "llm": 4,
        "gpt": 3, "bert": 3, "llama": 3, "diffusion": 3, "rag": 3,
    },
    "web": {
        "react": 3, "vue": 3, "angular": 3, "svelte": 3, "nextjs": 3, "next.js": 3,
        "express": 2, "fastapi": 2, "flask": 2, "django": 2, "rails": 2,
        "laravel": 2, "spring boot": 2, "http": 1, "rest api": 2, "graphql": 3,
        "webpack": 1, "vite": 2, "bootstrap": 2, "tailwind": 3, "css": 1,
        "html": 1, "jwt": 2, "oauth": 2, "session": 1, "frontend": 2,
        "backend": 1, "fullstack": 3, "nginx": 2,
    },
    "cli": {
        "command line": 3, "cli": 3, "terminal": 2, "shell": 1, "argparse": 2,
        "click": 2, "cobra": 2, "yargs": 2, "npx": 1, "brew install": 2,
        "go install": 2, "commander": 2, "prompt_toolkit": 3, "tty": 2,
        "stdin": 1, "stdout": 1, "subcommand": 3, "--help": 2, "binary": 1,
    },
    "game": {
        "unity": 3, "unreal": 3, "godot": 3, "pygame": 3, "phaser": 3,
        "roguelike": 3, "platformer": 3, "sprite": 2, "tilemap": 3, "gameplay": 2,
        "player": 1, "enemy": 1, "score": 0, "level": 1, "raylib": 3,
        "sdl": 2, "opengl": 2, "webgl": 2, "three.js": 3, "babylon": 3,
        "minecraft": 3, "rpg": 3, "fps": 2, "2d game": 3, "3d game": 3,
    },
    "crypto": {
        "bitcoin": 3, "ethereum": 3, "blockchain": 3, "solidity": 4, "wallet": 2,
        "nft": 3, "defi": 3, "dao": 2, "smart contract": 4, "erc20": 4,
        "erc721": 4, "web3": 3, "uniswap": 3, "metamask": 3, "ledger": 2,
        "coinbase": 2, "mining": 2, "proof of work": 3, "staking": 2,
        "hardhat": 3, "truffle": 2, "ganache": 3,
    },
    "db": {
        "postgres": 3, "postgresql": 3, "mysql": 3, "mariadb": 2, "sqlite": 3,
        "mongodb": 3, "redis": 3, "memcached": 3, "cassandra": 3, "elasticsearch": 3,
        "dynamodb": 3, "cockroach": 3, "clickhouse": 3, "sql": 1, "nosql": 2,
        "orm": 2, "sequelize": 2, "sqlalchemy": 2, "migration": 1, "schema": 1,
        "index": 1, "transaction": 1, "prisma": 3, "mongoose": 3, "knex": 2,
    },
    "devops": {
        "docker": 3, "kubernetes": 4, "k8s": 3, "helm": 3, "terraform": 4,
        "ansible": 3, "chef": 2, "puppet": 2, "jenkins": 3, "github actions": 3,
        "gitlab ci": 3, "travis": 2, "circleci": 3, "prometheus": 3, "grafana": 3,
        "loki": 2, "tempo": 2, "istio": 3, "envoy": 2, "vault": 3,
        "dockerfile": 2, "compose": 2, "helm chart": 3, "aws": 1, "gcp": 1,
        "azure": 1, "cloudformation": 3, "serverless": 3, "lambda": 2,
    },
    "mobile": {
        "ios": 2, "android": 2, "swift": 2, "kotlin": 2, "react native": 4,
        "flutter": 4, "expo": 3, "xcode": 2, "cocoapods": 3, "gradle": 2,
        "ios app": 3, "android app": 3, "objective-c": 2, "xamarin": 3,
    },
    "security": {
        "vulnerability": 2, "exploit": 2, "pentest": 3, "penetration testing": 3,
        "ctf": 3, "reverse engineering": 3, "malware": 2, "cve": 3, "zero-day": 3,
        "xss": 2, "sqli": 2, "sql injection": 3, "owasp": 3, "burp": 2,
        "wireshark": 3, "metasploit": 3, "encryption": 2, "tls": 1, "ssh": 1,
        "password": 1, "bcrypt": 2, "hash": 1, "firewall": 2,
    },
    "data": {
        "pandas": 3, "numpy": 2, "scipy": 2, "jupyter": 3, "notebook": 1,
        "etl": 2, "airflow": 3, "dbt": 3, "spark": 3, "hadoop": 2,
        "kafka": 3, "flink": 3, "beam": 2, "snowflake": 3, "bigquery": 3,
        "redshift": 3, "parquet": 2, "avro": 1, "csv": 1, "data pipeline": 3,
        "etl pipeline": 3, "datalake": 3, "data warehouse": 3, "pyspark": 3,
    },
    "os": {
        "linux": 1, "kernel": 2, "driver": 1, "systemd": 2, "bootloader": 3,
        "filesystem": 2, "uefi": 2, "bios": 2, "arch linux": 3, "nixos": 3,
        "freebsd": 2, "openbsd": 2, "microkernel": 3, "device driver": 3,
    },
    "lib": {
        "library": 1, "package": 1, "dependency": 1, "import": 0, "npm install": 1,
        "pip install": 1, "go get": 2, "cargo add": 2, "api wrapper": 2,
        "sdk": 1, "toolkit": 1, "framework": 1, "binding": 1,
    },
    "docs": {
        "tutorial": 2, "guide": 1, "cheatsheet": 3, "awesome": 2,
        "list of": 2, "curated": 2, "documentation": 1, "reference": 1,
        "handbook": 3, "cookbook": 3, "examples": 1, "getting started": 1,
    },
}


# ---- Install detection -----------------------------------------------------

INSTALL_PATTERNS: List[Tuple[str, re.Pattern]] = [
    ("pip", re.compile(r"\bpip install\b", re.I)),
    ("npm", re.compile(r"\bnpm (install|i)\b", re.I)),
    ("yarn", re.compile(r"\byarn (add|install)\b", re.I)),
    ("pnpm", re.compile(r"\bpnpm (add|install|i)\b", re.I)),
    ("cargo", re.compile(r"\bcargo (add|install|build)\b", re.I)),
    ("go-get", re.compile(r"\bgo (get|install)\b", re.I)),
    ("brew", re.compile(r"\bbrew install\b", re.I)),
    ("apt", re.compile(r"\bapt(?:-get)? install\b", re.I)),
    ("docker", re.compile(r"\bdocker (run|pull|build)\b", re.I)),
    ("git-clone", re.compile(r"\bgit clone\b", re.I)),
]


# ---- Tokenisation ----------------------------------------------------------

STOP = frozenset("""
a about above after again against all am an and any are aren't as at be because been before
being below between both but by can cannot could couldn't did didn't do does doesn't doing
don't down during each few for from further had hadn't has hasn't have haven't having he
he'd he'll he's her here here's hers herself him himself his how how's i i'd i'll i'm i've
if in into is isn't it it's its itself let's me more most mustn't my myself no nor not of
off on once only or other ought our ours ourselves out over own same shan't she she'd she'll
she's should shouldn't so some such than that that's the their theirs them themselves then
there there's these they they'd they'll they're they've this those through to too under
until up very was wasn't we we'd we'll we're we've were weren't what what's when when's
where where's which while who who's whom why why's with won't would wouldn't you you'd
you'll you're you've your yours yourself yourselves
see use using used uses also one two three will just like many much get got make made gets
may might need via get upon etc say said want goes going gone come came day way thing things
item items file files code line lines text data type types value values name names list
lists set sets run running runs ran works work worked working good better best new old
first second third another full empty true false null none this that these those info
information example examples project repo repository library package module function
functions class classes method methods call calls called method methods args arg option
options param params input inputs output outputs result results readme installation usage
install license mit apache gpl version release build tests test testing docs documentation
support contribute contributing contributor contributors contribution contributions fork
stars issues pull request pr history commit branch tag release releases overview note notes
want wants need needs needed number numbers github gitlab bitbucket
""".split())


WORD_RX = re.compile(r"[A-Za-z][A-Za-z\-]{1,30}")
H1_RX = re.compile(r"^\s*#\s+(.+?)\s*$", re.M)
BADGE_RX = re.compile(
    r"(?:shields\.io|travis-ci|coveralls|img\.shields|github\.com/.*/workflows/.*/badge\.svg|circleci\.com|badgen\.net)",
    re.I,
)
CODE_FENCE_RX = re.compile(r"```")
HTML_RX = re.compile(r"<[^>]+>")
LINK_RX = re.compile(r"\[([^\]]+)\]\([^\)]+\)")


def _extract_title(content: str, repo_name: str) -> str:
    m = H1_RX.search(content)
    if m:
        t = m.group(1).strip()
        # Strip leading emoji block at start of H1
        t = re.sub(r"^[^A-Za-z0-9]+", "", t)
        t = LINK_RX.sub(r"\1", t)
        t = HTML_RX.sub("", t)
        if 2 < len(t) < 160:
            return t
    # fall back: last path segment of repo_name
    return repo_name.split("/")[-1] if "/" in repo_name else repo_name


def _extract_one_line(content: str) -> str:
    # Take text AFTER the first H1, find first prose paragraph.
    h1 = H1_RX.search(content)
    start = h1.end() if h1 else 0
    tail = content[start:]
    # Paragraphs separated by blank lines
    paras = re.split(r"\n\s*\n", tail)
    for p in paras:
        p = p.strip()
        if not p:
            continue
        # Skip code blocks (```)
        if p.startswith("```"):
            continue
        # Skip heading lines
        if p.startswith("#"):
            continue
        # Skip badges-only lines: markdown-image starts with ![
        if re.match(r"^\s*!\[.*?\]\(.*?\)\s*$", p, re.S):
            continue
        # Skip TOC (list of links)
        if re.match(r"^\s*[\*\-]\s*\[", p):
            continue
        # Clean markdown
        p = LINK_RX.sub(r"\1", p)
        p = HTML_RX.sub("", p)
        p = re.sub(r"!\[[^\]]*\]\([^\)]+\)", "", p)  # images
        p = re.sub(r"[`*_~]", "", p)  # md emphasis
        p = re.sub(r"\s+", " ", p).strip()
        if len(p) < 12:
            continue
        return p[:280]
    return ""


def _detect_install(content: str) -> str:
    for name, rx in INSTALL_PATTERNS:
        if rx.search(content):
            return name
    return "none"


def _categorise(content_lower: str) -> Tuple[str, Dict[str, int]]:
    scores: Dict[str, int] = {}
    for cat, kws in CATEGORIES.items():
        score = 0
        for kw, w in kws.items():
            # Simple substring count for multi-word; single word uses word boundary
            if " " in kw:
                n = content_lower.count(kw)
            else:
                n = len(re.findall(rf"\b{re.escape(kw)}\b", content_lower))
            if n:
                score += w * min(n, 4)  # cap contribution per keyword to avoid spam
        if score:
            scores[cat] = score
    if not scores:
        return "other", {}
    top = max(scores.items(), key=lambda kv: kv[1])
    return top[0], scores


def _tokens(content: str) -> Dict[str, int]:
    c = Counter()
    # Strip code blocks (inside ``` ... ```) and inline code; these skew toward
    # keywords like "def", "class", "function" etc.
    clean = re.sub(r"```[\s\S]*?```", " ", content)
    clean = re.sub(r"`[^`]+`", " ", clean)
    clean = HTML_RX.sub(" ", clean)
    clean = LINK_RX.sub(r"\1", clean)
    for w in WORD_RX.findall(clean.lower()):
        if len(w) < 3 or len(w) > 24:
            continue
        if w in STOP:
            continue
        if w.isnumeric():
            continue
        c[w] += 1
    return dict(c.most_common(40))


def _count_badges(content: str) -> int:
    return len(BADGE_RX.findall(content))


def _count_code_blocks(content: str) -> int:
    return len(CODE_FENCE_RX.findall(content)) // 2


def _tldr(one_line: str) -> str:
    """Grab the first sentence (or first 140 chars) of the one-liner."""
    if not one_line:
        return ""
    # split on sentence boundary
    parts = re.split(r"(?<=[.!?])\s+", one_line, maxsplit=1)
    return parts[0][:140]


def summarize_row(repo_name: str, lang: str, path: str, size: int, content: str) -> Dict[str, Any]:
    """Summarize a single README — pure function, deterministic."""
    content_lower = content.lower()
    title = _extract_title(content, repo_name)
    one_line = _extract_one_line(content)
    cat, cat_scores = _categorise(content_lower)
    return {
        "repo": repo_name,
        "lang": lang or "",
        "title": title,
        "one_line": one_line,
        "tldr": _tldr(one_line),
        "chars": len(content),
        "install": _detect_install(content),
        "category": cat,
        "cat_scores": cat_scores,
        "badges": _count_badges(content),
        "code_blocks": _count_code_blocks(content),
        "tokens": _tokens(content),
    }


# ---- Burla worker entrypoint ----------------------------------------------

def summarize_shard(shard_idx: int, n_shards: int) -> Dict[str, Any]:
    """Process every nth row of the parquet in STREAMING fashion.

    Uses arrow row-group iteration to keep memory bounded at ~1 row group
    in memory at a time (~20-100 MB), not the full 1.3 GB parquet.
    """
    import pyarrow.parquet as pq

    os.makedirs(SHARD_OUT, exist_ok=True)
    t0 = time.time()

    # Fast path: if this shard's output already exists and looks healthy,
    # skip re-computing. Makes re-runs after failures cheap.
    out_path = os.path.join(SHARD_OUT, f"{shard_idx:04d}.json")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 500:
        return {
            "shard_idx": shard_idx,
            "n_ok": -1,
            "n_err": 0,
            "elapsed_s": 0,
            "skipped": True,
        }

    rows: List[Dict[str, Any]] = []
    by_cat: Dict[str, int] = {}
    by_lang: Dict[str, int] = {}
    by_install: Dict[str, int] = {}
    doc_freq: Counter = Counter()

    n_ok = 0
    n_err = 0

    pf = pq.ParquetFile(PARQUET_PATH)
    global_idx = 0
    for batch in pf.iter_batches(batch_size=4000,
                                 columns=["repo_name", "lang", "path", "size", "content"]):
        batch_n = batch.num_rows
        # Fast bulk conversion only for the columns we need from this batch
        repo_list = batch.column("repo_name").to_pylist()
        lang_list = batch.column("lang").to_pylist()
        path_list = batch.column("path").to_pylist()
        size_list = batch.column("size").to_pylist()
        content_list = batch.column("content").to_pylist()

        for j in range(batch_n):
            g = global_idx + j
            if (g % n_shards) != shard_idx:
                continue
            try:
                s = summarize_row(
                    repo_list[j] or "",
                    lang_list[j] or "",
                    path_list[j] or "",
                    int(size_list[j] or 0),
                    content_list[j] or "",
                )
            except Exception:
                n_err += 1
                continue
            rows.append(s)
            n_ok += 1
            by_cat[s["category"]] = by_cat.get(s["category"], 0) + 1
            by_lang[s["lang"] or "_unknown"] = by_lang.get(s["lang"] or "_unknown", 0) + 1
            by_install[s["install"]] = by_install.get(s["install"], 0) + 1
            for tok in s["tokens"]:
                doc_freq[tok] += 1

        global_idx += batch_n
        # Eagerly drop references so GC reclaims batch memory
        del repo_list, lang_list, path_list, size_list, content_list

    payload = {
        "shard_idx": shard_idx,
        "n_shards": n_shards,
        "n_ok": n_ok,
        "n_err": n_err,
        "elapsed_s": round(time.time() - t0, 2),
        "by_cat": by_cat,
        "by_lang": by_lang,
        "by_install": by_install,
        "doc_freq": dict(doc_freq),
        "rows": rows,
    }
    with open(out_path, "w") as f:
        json.dump(payload, f)

    return {
        "shard_idx": shard_idx,
        "n_ok": n_ok,
        "n_err": n_err,
        "elapsed_s": payload["elapsed_s"],
    }


# ---- Local smoke test ------------------------------------------------------

if __name__ == "__main__":
    # Synthetic test
    sample = """# Awesome ML Framework

A TensorFlow-style deep learning library for Python. Implements neural networks
with autograd, convolution layers, and a scikit-learn-compatible API.

## Install

```bash
pip install awesome-ml
```

## Usage

```python
from awesome_ml import Model
m = Model()
m.fit(X, y)
```
"""
    s = summarize_row("foo/awesome-ml", "Python", "README.md", len(sample), sample)
    print(json.dumps(s, indent=2))
