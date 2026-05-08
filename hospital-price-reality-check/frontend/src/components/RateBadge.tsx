/**
 * RateBadge makes it impossible to miss what the audience is looking at:
 * the hospital's published pre-insurance list price (gross charge or
 * cash-pay rate). Three variants:
 *   - "pill"    : compact inline pill for hero rows, card grids, table headers.
 *   - "inline"  : single-line callout above a price grid.
 *   - "banner"  : full-width disclosure banner with the long-form explanation.
 *
 * One canonical phrasing is repeated everywhere on purpose so readers stop
 * wondering "is this what I would actually pay?" before they get past the fold.
 */

type Variant = "pill" | "inline" | "banner";

const SHORT = "Pre-insurance list price";
const SHORT_PLURAL = "Pre-insurance list prices";

export function RateBadge({
  variant = "pill",
  className = "",
  plural = false,
}: {
  variant?: Variant;
  className?: string;
  plural?: boolean;
}) {
  const label = plural ? SHORT_PLURAL : SHORT;

  if (variant === "pill") {
    return (
      <span
        className={`inline-flex items-center gap-2 rounded-full border border-ink/15 bg-surface px-3 py-1 text-[11px] font-semibold uppercase tracking-eyebrowTight text-ink ${className}`}
        aria-label={`${label}. Not the price after insurance.`}
      >
        <span className="h-1.5 w-1.5 rounded-full bg-accent" aria-hidden />
        {label}
      </span>
    );
  }

  if (variant === "inline") {
    return (
      <p
        className={`inline-flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] font-semibold uppercase tracking-eyebrowTight text-ink ${className}`}
      >
        <span className="inline-flex items-center gap-2 rounded-full border border-ink/15 bg-surface px-3 py-1">
          <span className="h-1.5 w-1.5 rounded-full bg-accent" aria-hidden />
          {label}
        </span>
        <span className="text-inkMuted normal-case tracking-normal font-medium">
          Not the rate insurance negotiates. Not your final bill.
        </span>
      </p>
    );
  }

  return (
    <div
      className={`surface-edge px-6 py-5 flex gap-4 items-start bg-accent/10 border-accent/30 ${className}`}
    >
      <div className="mt-0.5 grid h-8 w-8 place-items-center rounded-full bg-accent/20 text-accent">
        <svg viewBox="0 0 24 24" fill="none" className="h-4 w-4">
          <path
            d="M12 2v20M5 9h13a3 3 0 010 6H6a3 3 0 000 6h13"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
      <div className="text-sm text-ink leading-relaxed max-w-3xl">
        <p className="font-display text-base font-semibold text-ink">
          You are looking at {SHORT_PLURAL.toLowerCase()}.
        </p>
        <p className="mt-1.5 text-inkMuted">
          Every dollar amount on this page is the hospital's published
          standard charge or cash-pay rate, taken straight from their
          machine-readable file. These are <span className="font-semibold text-ink">not</span>{" "}
          insurance-negotiated rates and <span className="font-semibold text-ink">not</span>{" "}
          what you owe after coverage. Use them to compare hospitals to each other,
          not to predict your bill.
        </p>
      </div>
    </div>
  );
}
