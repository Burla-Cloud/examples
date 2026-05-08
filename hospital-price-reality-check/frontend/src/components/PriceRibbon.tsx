import { fmtMoney } from "../format";

type Props = {
  min: number | null | undefined;
  p25: number | null | undefined;
  median: number | null | undefined;
  p75: number | null | undefined;
  max: number | null | undefined;
  /** Optional log scale for very wide spreads */
  log?: boolean;
};

/**
 * Editorial price-spread ribbon. Shows min, P25-P75 typical band, median, max
 * on a horizontal axis. Very Stripe / FT graphics.
 */
export function PriceRibbon({ min, p25, median, p75, max, log = false }: Props) {
  if (!min || !max || min <= 0 || max <= 0 || max < min) {
    return null;
  }

  const t = (v: number) => {
    if (log) {
      const lo = Math.log10(min);
      const hi = Math.log10(max);
      return ((Math.log10(v) - lo) / (hi - lo)) * 100;
    }
    return ((v - min) / (max - min)) * 100;
  };

  const safe = (v: number | null | undefined): number | null =>
    v != null && v > 0 ? v : null;
  const a = safe(p25);
  const b = safe(p75);
  const m = safe(median);

  const bandLeft = a ? t(a) : null;
  const bandRight = b ? t(b) : null;
  const bandWidth = bandLeft != null && bandRight != null ? Math.max(bandRight - bandLeft, 0.4) : null;

  const medianPos = m ? t(m) : null;

  return (
    <div className="w-full">
      <p className="mb-2 text-[10px] font-semibold uppercase tracking-eyebrowTight text-inkSubtle">
        Pre-insurance list price <span className="text-inkSubtle/70">·</span>{" "}
        <span className="font-medium normal-case tracking-normal text-inkSubtle">
          gross charge or cash-pay, before insurance
        </span>
      </p>
      <div className="relative h-7">
        <div className="absolute inset-x-0 top-1/2 -translate-y-1/2 h-px bg-line" />
        {bandLeft != null && bandWidth != null && (
          <div
            className="absolute top-1/2 -translate-y-1/2 h-2 rounded-full bg-ink/10"
            style={{ left: `${bandLeft}%`, width: `${bandWidth}%` }}
          />
        )}
        <Tick pos={0} variant="rail" />
        {medianPos != null && <Tick pos={medianPos} variant="median" />}
        <Tick pos={100} variant="rail" />
      </div>
      <div className="mt-2 flex items-center justify-between text-xs">
        <div className="flex flex-col">
          <span className="eyebrow">Lowest list price</span>
          <span className="font-display text-base font-semibold text-mint">{fmtMoney(min)}</span>
        </div>
        {medianPos != null && (
          <div className="flex flex-col items-center">
            <span className="eyebrow">Typical list price</span>
            <span className="font-display text-base font-semibold text-ink">{fmtMoney(m)}</span>
          </div>
        )}
        <div className="flex flex-col items-end">
          <span className="eyebrow">Highest list price</span>
          <span className="font-display text-base font-semibold text-rose">{fmtMoney(max)}</span>
        </div>
      </div>
    </div>
  );
}

function Tick({
  pos,
  variant,
}: {
  pos: number;
  variant: "rail" | "median";
}) {
  if (variant === "median") {
    return (
      <div
        className="absolute top-1/2 -translate-y-1/2 h-5 w-0.5 bg-ink rounded-full"
        style={{ left: `${pos}%`, transform: `translate(-50%, -50%)` }}
      />
    );
  }
  return (
    <div
      className="absolute top-1/2 -translate-y-1/2 h-3 w-0.5 bg-line rounded-full"
      style={{ left: `${pos}%`, transform: `translate(-50%, -50%)` }}
    />
  );
}
