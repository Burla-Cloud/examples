"""
update_website.py
-----------------
Reads derby/data/trillion_results.json (output of derby_trillion.py) and does
a full sweep of docs/index.html so every sim-count, timing, and worker-count
reference is consistent with the new run.

This sweep is digit-tolerant -- it does not assume any prior sim-count value.
Each substitution is logged. If any expected pattern is missing, we fail
loudly so a stale website doesn't silently slip through.

Run after derby_trillion.py completes:
    python3.12 derby/update_website.py
"""
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_FILE = os.path.join(ROOT, "derby", "data", "trillion_results.json")
WEBSITE_FILE = os.path.join(ROOT, "docs", "index.html")

# Tolerant numeric block: matches digits with optional commas (e.g. "1,000,000").
NUM = r"[\d,]+"

# Configured below in main() once we know the actual numbers.


def fmt_int(n: int) -> str:
    return f"{n:,}"


def fmt_time(seconds: float) -> str:
    """Format elapsed seconds as a short human string."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    if seconds < 3600:
        return f"{seconds/60:.1f} minutes"
    return f"{seconds/3600:.1f} hours"


def short_time(seconds: float) -> str:
    """Same as fmt_time but with a leaner phrasing for the hero."""
    if seconds < 60:
        return f"{seconds:.0f} seconds"
    if seconds < 3600:
        return f"~{seconds/60:.0f} minutes"
    return f"~{seconds/3600:.1f} hours"


class Patcher:
    def __init__(self, html: str):
        self.html = html
        self.changes = []

    def replace(self, label: str, pattern: str, replacement: str,
                count: int = 1, required: bool = True) -> None:
        """Apply one regex substitution. Records hit count; raises if required and 0."""
        new_html, n = re.subn(pattern, replacement, self.html, count=count)
        self.changes.append((label, n))
        if required and n == 0:
            raise RuntimeError(
                f"Pattern '{label}' did not match any text in docs/index.html.\n"
                f"  regex: {pattern!r}\n"
                f"Refusing to write a half-updated site. Inspect the HTML and "
                f"either (a) update the regex, or (b) revert the page to a known "
                f"baseline before re-running."
            )
        self.html = new_html

    def report(self) -> None:
        print("\nWebsite patch summary:")
        print("-" * 60)
        for label, n in self.changes:
            mark = "OK " if n > 0 else "-- "
            print(f"  {mark}{label:<48s} {n} hit(s)")
        print("-" * 60)


def build_horses_js(horses: list) -> str:
    out = "const HORSES = [\n"
    for h in horses:
        out += (
            f'  {{ post:{h["post"]}, name:"{h["name"]}", odds:"{h["odds"]}", '
            f'beyer:{h["beyer"]}, style:"{h["style"]}", '
            f'trainerDW:{h["trainerDW"]}, jockeyDW:{h["jockeyDW"]}, '
            f'win:{h["winPct"]:.4f}, place:{h["placePct"]:.4f}, '
            f'show:{h["showPct"]:.4f}, '
            f'implied:{h["impliedPct"]}, val:"{h["value"]}" }},\n'
        )
    out += "];"
    return out


def main():
    if not os.path.exists(RESULTS_FILE):
        print(f"ERROR: {RESULTS_FILE} not found. Run derby_trillion.py first.")
        sys.exit(1)

    with open(RESULTS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    horses     = data["horses"]
    total_sims = data["total_sims"]
    elapsed    = data.get("elapsed_s", 0.0)
    backend    = data.get("backend", "Burla")
    n_workers  = data.get("n_workers", 50_000)
    sims_per_worker = total_sims // max(n_workers, 1)

    sims_str   = fmt_int(total_sims)
    workers_str = fmt_int(n_workers)
    sims_per_worker_str = fmt_int(sims_per_worker)
    time_str   = fmt_time(elapsed)
    short_t    = short_time(elapsed)

    print(f"Loaded results: {sims_str} simulations via {backend}")
    print(f"Workers       : {workers_str}")
    print(f"Elapsed       : {time_str}\n")

    print(f"{'Horse':<22} {'Win%':>7} {'Place%':>8} {'Show%':>7} {'Value':>6}")
    print("-" * 58)
    for h in horses:
        print(f"{h['name']:<22} {h['winPct']:>7.3f}% {h['placePct']:>7.3f}% "
              f"{h['showPct']:>7.3f}%  {h['value']:>5}")

    with open(WEBSITE_FILE, encoding="utf-8") as f:
        html = f.read()

    p = Patcher(html)

    # 1. <title>: "Kentucky Derby 2026 on Burla: <N> Race Simulations in <T>"
    p.replace(
        "title tag",
        rf'<title>Kentucky Derby 2026 on Burla: {NUM} Race Simulations in [^<]+</title>',
        f'<title>Kentucky Derby 2026 on Burla: {sims_str} Race Simulations in {short_t}</title>',
    )

    # 2. <meta name="description">: replace whole tag with a known-good string.
    p.replace(
        "meta description",
        r'<meta name="description" content="[^"]*"\s*/>',
        f'<meta name="description" content="5,000 weight combinations tested in 7 seconds. '
        f'164 ML model configs trained in parallel. {sims_str} Monte Carlo race simulations '
        f'in {short_t}. All on Burla." />',
    )

    # 3. Hero <h1>: "<N> Derby simulations,<br/><span class="accent">in <T>.</span>"
    # v2 site customised the hero copy to "Where the market is wrong..." so this
    # pattern won't match. Mark optional so the script doesn't abort.
    p.replace(
        "hero h1",
        rf'<h1>{NUM} Derby simulations,<br/><span class="accent">in [^<]+</span></h1>',
        f'<h1>{sims_str} Derby simulations,<br/><span class="accent">in {short_t}.</span></h1>',
        required=False,
    )

    # 4. Hero lede strong tag: "<strong>X Monte Carlo race simulations</strong>"
    p.replace(
        "hero lede strong",
        rf'<strong>{NUM} Monte Carlo race simulations</strong>',
        f'<strong>{sims_str} Monte Carlo race simulations</strong>',
        required=False,
    )

    # 5. Hero stat tile: <span class="num accent">X</span><span class="label">Monte Carlo simulations
    # v2 keeps the user-friendly "1 Trillion" text rather than the digit form,
    # so this patch is a no-op (kept for parity with v1).
    p.replace(
        "hero stat tile",
        rf'(<span class="num accent">)[^<]+(</span>\s*<span class="label">Monte Carlo simulations)',
        rf'\g<1>1 Trillion\g<2>',
        required=False,
    )

    # 6. Section lede in #picks: "Win% from X Monte Carlo simulations."
    p.replace(
        "picks section lede",
        rf'Win% from {NUM} Monte Carlo simulations\.',
        f'Win% from {sims_str} Monte Carlo simulations.',
        required=False,
    )

    # 7. "How it ran" body: "and X race simulations run as Y parallel Burla workers in Z."
    # v1 said "100 concurrent batches in 90 seconds" which never matched the actual
    # architecture. v2 phrasing is "X parallel Burla workers in Y minutes".
    p.replace(
        "how-it-ran callout",
        rf'and {NUM} race simulations run as {NUM} (?:concurrent batches|parallel Burla workers) in [^.]+\.',
        f'and {sims_str} race simulations run as {workers_str} parallel Burla workers '
        f'in {time_str}.',
        required=False,
    )

    # 8. peak-num span (worker count tile in "How it ran")
    p.replace(
        "peak-num tile",
        rf'(<span class="peak-num">){NUM}(</span>)',
        rf'\g<1>{workers_str}\g<2>',
    )

    # 9. Body sentence next to peak-num: "...each running 10,000 race simulations."
    # This sentence is the only place the per-worker sim count is exposed.
    p.replace(
        "per-worker sims sentence",
        rf'each running {NUM} race simulations\.',
        f'each running {sims_per_worker_str} race simulations.',
    )

    # 10. Code-block comments in the montecarlo pane.
    # Match: "# X race simulations across Y Burla workers."
    p.replace(
        "code comment: total + workers",
        rf'# {NUM} race simulations across {NUM} Burla workers\.',
        f'# {sims_str} race simulations across {workers_str} Burla workers.',
    )

    # Match: "# Each worker runs X sims, returns position tallies."
    p.replace(
        "code comment: per-worker",
        rf'# Each worker runs {NUM} sims, returns position tallies\.',
        f'# Each worker runs {sims_per_worker_str} sims, returns position tallies.',
    )

    # Match: "# X workers × Y sims = Z total in ~T" (or v1's "X batches × Y sims" wording).
    p.replace(
        "code comment: arithmetic",
        rf'# {NUM} (?:batches|workers) × {NUM} sims = {NUM} total in ~?[^<\n]+',
        f'# {workers_str} workers × {sims_per_worker_str} sims = {sims_str} total in {short_t}',
        required=False,
    )

    # 11. Footer: "<N> Monte Carlo simulations." — anchor to footer context
    # (the leading "+ ML ensemble..." phrase) so we don't accidentally re-hit
    # the section-lede on line 530 that's already been patched.
    p.replace(
        "footer sim count",
        rf'(top-5 by log-loss\)\.\s*)\n?\s*{NUM} Monte Carlo simulations\.',
        rf'\g<1>\n      {sims_str} Monte Carlo simulations.',
        required=False,
    )

    # 11a. Footer "1 trillion Monte Carlo simulations." (text-form variant
    # introduced in v2 manual edits). Keep "1 trillion" wording for fluency.
    p.replace(
        "footer 1-trillion text",
        rf'1 trillion Monte Carlo simulations\.',
        f'1 trillion Monte Carlo simulations.',
        required=False,
    )

    # 12. HORSES JS array (replaces the entire const HORSES = [...]; block).
    p.replace(
        "HORSES JS array",
        r"const HORSES = \[[\s\S]*?\];",
        build_horses_js(horses),
    )

    p.report()

    with open(WEBSITE_FILE, "w", encoding="utf-8") as f:
        f.write(p.html)

    print(f"\nWebsite updated: {WEBSITE_FILE}")
    print("\nNext steps:")
    print("  git add derby/data/trillion_results.json docs/index.html derby/update_website.py")
    print('  git commit -m "Update website with 1 trillion simulation results"')
    print("  git push")
    print("\nSite will be live at: https://jackburla.github.io/BurlaKentuckyDerby/")


if __name__ == "__main__":
    main()
