import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadAll } from "../api";
import type { CodeEntry, RunMeta, SpreadRow } from "../types";
import { CaveatBanner } from "../components/CaveatBanner";
import { PriceRibbon } from "../components/PriceRibbon";
import { RateBadge } from "../components/RateBadge";
import { categoryLabel, fmtMoney, fmtNum } from "../format";

export function Home() {
  const [data, setData] = useState<{
    codes: CodeEntry[];
    spread: SpreadRow[];
    meta: RunMeta;
  } | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    loadAll()
      .then((d) => setData({ codes: d.codes, spread: d.spread, meta: d.meta }))
      .catch((e) => setErr(String(e)));
  }, []);

  const summary = useMemo(() => {
    if (!data) return null;
    const meta = data.meta;
    const scale = meta.scale_summary || {};
    const obs = (scale.observation_rows_reported as number | undefined) ?? 0;
    const hospitalsWithData =
      meta.hospitals_with_data ??
      meta.observation_files ??
      (scale.hospitals_succeeded as number | undefined) ??
      0;
    const elapsed = (scale.elapsed_seconds as number | undefined) ?? 0;
    const codesWithData = data.codes.filter((c) => (c.stats?.count ?? 0) > 0).length;
    const topRow = data.spread[0] ?? null;
    return {
      hospitalsWithData,
      observations: obs,
      elapsedMin: elapsed ? Math.round(elapsed / 60) : null,
      codesWithData,
      topSpread: topRow?.spread_ratio ?? null,
      topRow,
    };
  }, [data]);

  const featured = useMemo(() => {
    if (!data) return [];
    // Curated mix that shows the breadth of the dataset: a surgery, a chemo
    // drug, an immunotherapy, and a colonoscopy. Falls back to whatever has
    // the most data if a curated code is missing.
    const popular = ["27447", "J9000", "J9271", "45378", "59409", "J3490"];
    const pickByCode = popular
      .map((code) =>
        data.codes.find(
          (c) => c.code === code && (c.stats?.count ?? 0) >= 5 && (c.stats?.median ?? 0) > 0
        )
      )
      .filter((c): c is CodeEntry => Boolean(c))
      .slice(0, 4);
    if (pickByCode.length >= 4) return pickByCode;
    const fallback = data.codes
      .filter((c) => (c.stats?.count ?? 0) >= 5 && (c.stats?.median ?? 0) > 0)
      .sort((a, b) => (b.stats?.count ?? 0) - (a.stats?.count ?? 0))
      .slice(0, 4);
    return [...pickByCode, ...fallback].slice(0, 4);
  }, [data]);

  if (err) return <p className="text-rose">Error loading data: {err}</p>;
  if (!data || !summary) {
    return <HomeSkeleton />;
  }

  const top3 = data.spread.slice(0, 3);

  return (
    <div className="space-y-28 md:space-y-36">
      {/* HERO */}
      <section className="relative -mx-5 md:-mx-8 px-5 md:px-8 pt-4 pb-20 md:pb-28 bg-heroFade overflow-hidden rounded-[28px]">
        <div className="container-7 max-w-5xl px-0 animate-floatIn">
          <div className="flex flex-wrap items-center gap-3 mb-8">
            <span className="inline-flex items-center gap-2 rounded-full border border-line bg-surface/90 px-3.5 py-1.5 text-[11px] font-medium text-ink tracking-eyebrowTight uppercase backdrop-blur">
              <span className="h-1.5 w-1.5 rounded-full bg-mint animate-pulseDot" />
              Live data, refreshed from {fmtNum(summary.hospitalsWithData)} hospitals
            </span>
            <RateBadge plural />
          </div>
          <h1 className="display-1 max-w-4xl text-balance">
            What real hospitals charge,
            <br />
            <span className="italic text-accent">in plain English.</span>
          </h1>
          <p className="body-lead mt-8 max-w-2xl text-pretty">
            Hospitals are required by federal law to publish what they charge
            for every procedure, drug, and lab. We collected those files from
            real US hospitals, pulled out the {data.codes.length}{" "}
            procedures, chemo drugs, and labs people actually need, and put
            them all in one place.
          </p>
          {summary.topRow && summary.topSpread && summary.topSpread > 1 ? (
            <p className="body-lead mt-6 max-w-2xl text-pretty">
              Right now the biggest gap is{" "}
              <Link
                to={`/explore/${encodeURIComponent(
                  summary.topRow.key.split(":")[0]
                )}/${encodeURIComponent(summary.topRow.key.split(":")[1])}`}
                className="font-medium text-ink underline decoration-accent decoration-2 underline-offset-4 hover:text-accent"
              >
                {summary.topRow.display_name.toLowerCase()}
              </Link>
              , where the priciest hospital charges{" "}
              <span className="font-medium text-ink">
                {Math.round(summary.topSpread)} times
              </span>{" "}
              what the cheapest one does.
            </p>
          ) : null}
          <div className="mt-10 flex flex-wrap gap-3">
            <Link to="/explore" className="btn-primary">
              Look up a price
              <svg viewBox="0 0 24 24" className="h-4 w-4" fill="none">
                <path
                  d="M5 12h14m0 0l-5-5m5 5l-5 5"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </Link>
            <Link to="/leaderboard" className="btn-ghost">
              See the biggest gaps
            </Link>
          </div>
        </div>
      </section>

      {/* MARQUEE OF PRICE SPREADS */}
      {featured.length > 0 && (
        <section>
          <div className="container-7 px-0 max-w-5xl mb-10">
            <p className="eyebrow">A taste</p>
            <h2 className="display-2 mt-3 max-w-3xl text-balance">
              The same procedure, very different prices.
            </h2>
            <RateBadge variant="inline" plural className="mt-5" />
          </div>
          <div className="container-7 px-0 grid gap-px overflow-hidden rounded-2xl border border-line bg-line">
            {featured.map((c) => (
              <Link
                key={`${c.code_system}:${c.code}`}
                to={`/explore/${encodeURIComponent(c.code_system)}/${encodeURIComponent(c.code)}`}
                className="group bg-surface px-7 py-8 md:px-10 md:py-9 transition-colors hover:bg-section/50"
              >
                <div className="grid items-center gap-6 md:grid-cols-12">
                  <div className="md:col-span-4">
                    <p className="eyebrow">{categoryLabel(c.category)}</p>
                    <p className="font-display text-2xl font-semibold text-ink mt-2 group-hover:text-accent transition-colors leading-tight">
                      {c.display_name}
                    </p>
                    <p className="mt-1.5 text-xs text-inkSubtle font-mono">
                      {c.code_system} {c.code}
                    </p>
                  </div>
                  <div className="md:col-span-8">
                    <PriceRibbon
                      min={c.stats?.min}
                      p25={c.stats?.p25}
                      median={c.stats?.median}
                      p75={c.stats?.p75}
                      max={c.stats?.max}
                      log={
                        (c.stats?.max ?? 0) /
                          Math.max(c.stats?.min ?? 1, 1) >
                        80
                      }
                    />
                  </div>
                </div>
              </Link>
            ))}
          </div>
          <div className="container-7 px-0 mt-6 flex justify-end">
            <Link to="/explore" className="btn-link">
              Look up your procedure
              <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" fill="none">
                <path
                  d="M5 12h14m0 0l-5-5m5 5l-5 5"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </Link>
          </div>
        </section>
      )}

      {/* STATS — clean editorial row, not boxy cards */}
      <section className="container-7 px-0">
        <div className="grid gap-12 md:grid-cols-4 md:gap-8 border-t border-b border-line py-12">
          <Stat
            label="Hospitals with data"
            value={fmtNum(summary.hospitalsWithData)}
            hint="Real prices we could read"
          />
          <Stat
            label="Procedures and drugs"
            value={`${summary.codesWithData}`}
            hint={`of ${data.codes.length} we tracked`}
          />
          <Stat
            label="Real price points"
            value={fmtNum(summary.observations)}
            hint="Pre-insurance list prices, not estimates"
          />
          <Stat
            label="Latest run"
            value={summary.elapsedMin != null ? `${summary.elapsedMin} min` : "Open"}
            hint={
              summary.elapsedMin != null
                ? "End to end, fetch through analysis"
                : "Source. Fork it. Ship it."
            }
            tone="accent"
          />
        </div>
      </section>

      <CaveatBanner />

      {/* TOP SPREADS — editorial row */}
      <section className="container-7 px-0">
        <div className="mb-10 flex flex-wrap items-end justify-between gap-4 max-w-5xl">
          <div>
            <p className="eyebrow">Where the gaps are widest</p>
            <h2 className="display-2 mt-3 text-balance max-w-2xl">
              Where the same care can cost an order of magnitude more.
            </h2>
          </div>
          <Link to="/leaderboard" className="btn-link hidden md:inline-flex">
            See the full ranking
            <svg viewBox="0 0 24 24" className="h-3.5 w-3.5" fill="none">
              <path
                d="M5 12h14m0 0l-5-5m5 5l-5 5"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </Link>
        </div>
        <div className="grid gap-5 md:grid-cols-3">
          {top3.map((row, i) => (
            <Link
              key={row.key}
              to={`/explore/${encodeURIComponent(
                row.key.split(":")[0]
              )}/${encodeURIComponent(row.key.split(":")[1])}`}
              className="surface-card surface-card-hover p-7 group flex flex-col"
            >
              <div className="flex items-center justify-between">
                <span className="eyebrow">#{i + 1}</span>
                <span className="pill">{categoryLabel(row.category)}</span>
              </div>
              <p className="font-display text-xl font-semibold text-ink leading-snug mt-6 group-hover:text-accent transition-colors">
                {row.display_name}
              </p>
              <div className="mt-7 flex items-baseline gap-2">
                <span className="font-display text-5xl font-medium tracking-[-0.02em] text-ink">
                  {row.spread_ratio ? `${Math.round(row.spread_ratio)}` : "—"}
                </span>
                <span className="font-display text-2xl text-inkSubtle">×</span>
              </div>
              <p className="mt-1 eyebrow">highest vs lowest</p>
              <div className="mt-auto pt-7 grid grid-cols-2 gap-5 border-t border-lineSoft text-sm">
                <div>
                  <p className="eyebrow text-mint">Lowest</p>
                  <p className="font-display text-lg font-semibold text-ink mt-1">
                    {fmtMoney(row.lowest)}
                  </p>
                </div>
                <div>
                  <p className="eyebrow text-rose">Highest</p>
                  <p className="font-display text-lg font-semibold text-ink mt-1">
                    {fmtMoney(row.highest)}
                  </p>
                </div>
              </div>
            </Link>
          ))}
        </div>
      </section>

      {/* HOW IT WORKS */}
      <section className="container-7 px-0">
        <div className="mb-12 max-w-3xl">
          <p className="eyebrow">How it works</p>
          <h2 className="display-2 mt-3 text-balance">
            Three steps from raw federal files to plain English.
          </h2>
        </div>
        <div className="grid gap-px overflow-hidden rounded-2xl border border-line bg-line md:grid-cols-3">
          <Step
            n={1}
            title="We grab the official files"
            body="Every US hospital posts a machine-readable file of standard charges. We fetch them straight from the hospital, no middlemen."
          />
          <Step
            n={2}
            title={`We pull out ${data.codes.length} things people need`}
            body="MRI of the brain. C-section. Chemotherapy. Keytruda. Colonoscopy. Insulin. We translate the medical billing codes into the words you'd actually use."
          />
          <Step
            n={3}
            title="We show you the spread"
            body="For every procedure, you see the lowest, the typical price, and the highest. Plus which hospital is which, by state."
          />
        </div>
      </section>

      {/* CTA */}
      <section className="container-7 px-0">
        <div className="relative overflow-hidden rounded-2xl border border-ink/10 bg-ink text-bg px-7 py-14 md:px-14 md:py-20">
          <div className="absolute -right-24 -top-24 h-80 w-80 rounded-full bg-accent/35 blur-3xl" />
          <div className="absolute -left-12 -bottom-16 h-72 w-72 rounded-full bg-mint/20 blur-3xl" />
          <div className="relative z-10 flex flex-col gap-7 md:flex-row md:items-end md:justify-between">
            <div className="max-w-2xl space-y-5">
              <p className="eyebrow text-bg/60">Want the source</p>
              <h3 className="font-display text-3xl font-medium tracking-[-0.02em] md:text-[44px] leading-[1.05] text-balance">
                Run this on any hospital list you want.
              </h3>
              <p className="text-bg/85 text-lg leading-relaxed max-w-xl">
                The whole pipeline is open source. Point it at a different
                hospital list and rerun it. Each refresh takes minutes.
              </p>
            </div>
            <div className="flex flex-wrap gap-3">
              <Link
                to="/how-we-did-this"
                className="inline-flex items-center gap-2 rounded-full bg-bg px-6 py-3 text-sm font-medium text-ink transition-all hover:bg-section hover:-translate-y-0.5"
              >
                How we did this
              </Link>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}

function Stat({
  label,
  value,
  hint,
  tone = "default",
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "default" | "accent";
}) {
  return (
    <div>
      <p className="eyebrow">{label}</p>
      <p
        className={`stat-num mt-3 ${
          tone === "accent" ? "text-accent" : "text-ink"
        }`}
      >
        {value}
      </p>
      {hint && <p className="mt-2 text-sm text-inkMuted">{hint}</p>}
    </div>
  );
}

function Step({ n, title, body }: { n: number; title: string; body: string }) {
  return (
    <div className="bg-surface px-7 py-10 md:px-9 md:py-12">
      <div className="flex items-center gap-4">
        <span className="font-display text-3xl font-medium text-accent tracking-[-0.02em]">
          {String(n).padStart(2, "0")}
        </span>
        <div className="h-px flex-1 bg-line" />
      </div>
      <h3 className="font-display text-xl font-semibold text-ink mt-6 leading-snug">
        {title}
      </h3>
      <p className="mt-3 text-base text-inkMuted leading-relaxed">{body}</p>
    </div>
  );
}

function HomeSkeleton() {
  return (
    <div className="space-y-20 animate-pulse">
      <div className="rounded-[28px] bg-section h-[420px] md:h-[520px]" />
      <div className="rounded-2xl bg-section h-72" />
      <div className="grid gap-4 md:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="rounded-2xl bg-section h-32" />
        ))}
      </div>
      <div className="grid gap-5 md:grid-cols-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="rounded-2xl bg-section h-72" />
        ))}
      </div>
    </div>
  );
}
