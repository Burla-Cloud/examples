import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { loadAll } from "../api";
import type { SpreadRow } from "../types";
import { categoryLabel, fmtMoney } from "../format";
import { PriceRibbon } from "../components/PriceRibbon";
import { RateBadge } from "../components/RateBadge";

export function Leaderboard() {
  const [rows, setRows] = useState<SpreadRow[]>([]);
  const [cat, setCat] = useState("");
  const [sortKey, setSortKey] = useState<
    "spread_ratio" | "dollar_spread" | "median"
  >("spread_ratio");

  useEffect(() => {
    loadAll().then((d) => setRows(d.spread));
  }, []);

  const cats = useMemo(
    () => Array.from(new Set(rows.map((r) => r.category))).sort(),
    [rows]
  );

  const filtered = useMemo(() => {
    const list = rows.filter((r) => !cat || r.category === cat);
    return list.sort((a, b) => {
      const av = (a[sortKey] ?? 0) as number;
      const bv = (b[sortKey] ?? 0) as number;
      return bv - av;
    });
  }, [rows, cat, sortKey]);

  if (rows.length === 0) {
    return (
      <div className="space-y-6 animate-pulse">
        <div className="h-12 w-1/2 rounded-2xl bg-section" />
        <div className="grid gap-5 md:grid-cols-3">
          {Array.from({ length: 3 }).map((_, i) => (
            <div key={i} className="h-56 rounded-2xl bg-section" />
          ))}
        </div>
      </div>
    );
  }

  const top3 = filtered.slice(0, 3);
  const rest = filtered.slice(3);

  const sortLabel: Record<typeof sortKey, string> = {
    spread_ratio: "highest-vs-lowest ratio",
    dollar_spread: "dollar gap",
    median: "typical price",
  };

  return (
    <div className="space-y-14 animate-floatIn">
      <div className="max-w-3xl">
        <p className="eyebrow">Biggest price gaps</p>
        <h1 className="display-2 mt-3 text-balance">
          Same procedure, very different prices.
        </h1>
        <p className="body-lead mt-5 text-pretty">
          For each procedure or drug we ranked the most expensive hospital
          against the cheapest. A 10× means the priciest hospital posts ten
          times what the cheapest does for the same thing.
        </p>
        <RateBadge variant="inline" plural className="mt-5" />
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center justify-between gap-5">
        <div className="flex flex-wrap gap-2">
          <FilterChip active={!cat} onClick={() => setCat("")}>
            All
          </FilterChip>
          {cats.map((c) => (
            <FilterChip
              key={c}
              active={cat === c}
              onClick={() => setCat(cat === c ? "" : c)}
            >
              {categoryLabel(c)}
            </FilterChip>
          ))}
        </div>
        <div className="flex items-center gap-3">
          <label className="eyebrow whitespace-nowrap">Sort by</label>
          <select
            className="select-clean"
            value={sortKey}
            onChange={(e) => setSortKey(e.target.value as typeof sortKey)}
          >
            <option value="spread_ratio">Highest vs lowest</option>
            <option value="dollar_spread">Dollar gap</option>
            <option value="median">Typical list price</option>
          </select>
        </div>
      </div>

      {/* Top 3 podium */}
      {top3.length > 0 && (
        <section className="grid gap-5 md:grid-cols-3">
          {top3.map((row, i) => (
            <Link
              key={row.key}
              to={`/explore/${encodeURIComponent(
                row.key.split(":")[0]
              )}/${encodeURIComponent(row.key.split(":")[1])}`}
              className="surface-card surface-card-hover p-7 group flex flex-col"
            >
              <div className="flex items-center justify-between">
                <span className="font-display text-5xl font-medium text-ink/15 tracking-[-0.02em]">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <span className="pill">{categoryLabel(row.category)}</span>
              </div>
              <p className="font-display text-2xl font-semibold text-ink leading-snug mt-5 group-hover:text-accent transition-colors">
                {row.display_name}
              </p>
              <p className="text-xs text-inkSubtle font-mono mt-1">{row.key}</p>
              <div className="mt-7 flex items-baseline gap-2">
                <span className="font-display text-6xl font-medium tracking-[-0.025em] text-ink">
                  {row.spread_ratio
                    ? row.spread_ratio < 10
                      ? row.spread_ratio.toFixed(1)
                      : Math.round(row.spread_ratio)
                    : "—"}
                </span>
                <span className="font-display text-3xl text-inkSubtle">×</span>
              </div>
              <p className="mt-1 eyebrow">highest vs lowest</p>
              <div className="mt-auto pt-7 border-t border-lineSoft">
                <p className="text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
                  Pre-insurance list price
                </p>
                <div className="mt-2 grid grid-cols-2 gap-x-5 gap-y-4 text-sm">
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
                  <div>
                    <p className="eyebrow">Typical</p>
                    <p className="font-display text-lg font-semibold text-ink mt-1">
                      {fmtMoney(row.median)}
                    </p>
                  </div>
                  <div>
                    <p className="eyebrow">Dollar gap</p>
                    <p className="font-display text-lg font-semibold text-ink mt-1">
                      {fmtMoney(row.dollar_spread)}
                    </p>
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </section>
      )}

      {/* Editorial table for the rest */}
      <section>
        <div className="mb-6 flex items-end justify-between">
          <div>
            <p className="eyebrow">The full ranking</p>
            <h2 className="display-3 mt-2">
              {filtered.length} procedure{filtered.length === 1 ? "" : "s"}, ordered by {sortLabel[sortKey]}
            </h2>
          </div>
        </div>
        <div className="surface-card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="editorial-table">
              <caption className="bg-section/40 px-7 py-3 text-left text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
                All dollar values are pre-insurance list prices (gross charge or cash-pay), straight from each hospital's machine-readable file.
              </caption>
              <thead>
                <tr>
                  <th className="px-7 py-4 w-12 text-right">#</th>
                  <th className="px-7 py-4">Procedure</th>
                  <th className="px-7 py-4 w-[260px]">Spread</th>
                  <th className="px-7 py-4 text-right">Lowest list</th>
                  <th className="px-7 py-4 text-right">Typical list</th>
                  <th className="px-7 py-4 text-right">Highest list</th>
                  <th className="px-7 py-4 text-right">Ratio</th>
                </tr>
              </thead>
              <tbody>
                {rest.map((r, i) => (
                  <tr key={r.key}>
                    <td className="px-7 text-right text-inkSubtle font-mono text-xs">
                      {i + 4}
                    </td>
                    <td className="px-7 max-w-[280px]">
                      <Link
                        className="font-medium text-ink hover:text-accent transition-colors"
                        to={`/explore/${encodeURIComponent(
                          r.key.split(":")[0]
                        )}/${encodeURIComponent(r.key.split(":")[1])}`}
                      >
                        {r.display_name}
                      </Link>
                      <div className="text-xs text-inkSubtle mt-0.5">
                        {categoryLabel(r.category)} · {r.key}
                      </div>
                    </td>
                    <td className="px-7 w-[260px]">
                      <PriceRibbon
                        min={r.lowest}
                        p25={r.lowest}
                        median={r.median}
                        p75={r.highest}
                        max={r.highest}
                        log={(r.spread_ratio ?? 1) > 80}
                      />
                    </td>
                    <td className="px-7 text-right font-medium text-ink">
                      {fmtMoney(r.lowest)}
                    </td>
                    <td className="px-7 text-right font-medium text-ink">
                      {fmtMoney(r.median)}
                    </td>
                    <td className="px-7 text-right font-medium text-ink">
                      {fmtMoney(r.highest)}
                    </td>
                    <td className="px-7 text-right font-display text-lg font-medium text-ink tracking-[-0.01em]">
                      {r.spread_ratio != null
                        ? `${
                            r.spread_ratio < 10
                              ? r.spread_ratio.toFixed(1)
                              : Math.round(r.spread_ratio)
                          }×`
                        : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {rest.length === 0 && (
            <p className="px-6 py-12 text-center text-sm text-inkSubtle">
              No more rows in this category.
            </p>
          )}
        </div>
      </section>
    </div>
  );
}

function FilterChip({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-4 py-2 text-xs font-medium transition-all duration-200 ${
        active
          ? "border-ink bg-ink text-bg"
          : "border-line bg-surface text-inkMuted hover:border-ink hover:text-ink"
      }`}
    >
      {children}
    </button>
  );
}
