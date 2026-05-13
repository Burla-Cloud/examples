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
    const codesWithData = data.codes.filter((c) => (c.stats?.count ?? 0) > 0).length;
    const topRow = data.spread[0] ?? null;
    return {
      hospitalsWithData,
      observations: obs,
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
            Search real hospital prices
            <br />
            <span className="italic text-accent">across the US.</span>
          </h1>
          <p className="body-lead mt-8 max-w-2xl text-pretty">
            A central, searchable database of what hospitals charge for
            procedures, medicines, and labs before insurance.
          </p>
          <p className="body-lead mt-6 max-w-2xl text-pretty">
            We collected pricing files from{" "}
            <span className="font-medium text-ink">
              {fmtNum(summary.hospitalsWithData)}
            </span>{" "}
            US hospitals, cleaned the messy data, and made it easy to
            compare prices in plain English. Right now we cover{" "}
            <span className="font-medium text-ink">
              {data.codes.length}
            </span>{" "}
            of the procedures, chemo drugs, and labs people actually look up.
          </p>
          {summary.topRow && summary.topSpread && summary.topSpread > 1 ? (
            <p className="body-lead mt-6 max-w-2xl text-pretty">
              We also highlight where prices get ridiculous: procedures,
              drugs, and labs where one hospital charges many times more
              than another for the exact same item. Right now the biggest
              gap is{" "}
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
              Search prices
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
        <div className="grid gap-12 md:grid-cols-3 md:gap-10 border-t border-b border-line py-12">
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
            One searchable database, built from raw federal files.
          </h2>
          <p className="mt-5 max-w-2xl text-base text-inkMuted leading-relaxed">
            The goal is simple: type in a procedure, drug, or lab and get
            real prices from real hospitals back, in plain English.
          </p>
        </div>
        <div className="grid gap-px overflow-hidden rounded-2xl border border-line bg-line md:grid-cols-3">
          <Step
            n={1}
            title="We grab the official files"
            body="Every US hospital posts a machine-readable file of standard charges. We fetch them straight from the hospital, no middlemen."
          />
          <Step
            n={2}
            title="We clean them up"
            body={`Raw chargemasters are messy: thousands of line items per hospital, every layout different, every description abbreviated. We translate the billing codes into plain English and normalize drug doses so a 100 mg vial and a 1 mg vial sit on the same scale.`}
          />
          <Step
            n={3}
            title="We put it all in one searchable place"
            body={`Every hospital, every procedure, drug, and lab in one site. Search by name, filter by state, click into any code to see exactly which hospital published it and what they charge.`}
          />
        </div>
        <div className="mt-10 rounded-2xl border border-line bg-section/40 px-7 py-7 md:px-9 md:py-8 max-w-4xl">
          <p className="eyebrow text-accent">Bonus</p>
          <p className="mt-3 font-display text-xl font-medium text-ink leading-snug text-balance">
            We also analyze the spread.
          </p>
          <p className="mt-3 text-sm text-inkMuted leading-relaxed max-w-3xl">
            Because the data is all in one place, we can show you where the
            same item costs many times more at one hospital than at another.{" "}
            <Link
              to="/leaderboard"
              className="font-medium text-ink underline decoration-accent decoration-2 underline-offset-4 hover:text-accent"
            >
              The biggest gaps
            </Link>{" "}
            is the ranked list of those.
          </p>
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
