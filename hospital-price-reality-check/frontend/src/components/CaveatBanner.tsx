export function CaveatBanner() {
  return (
    <div className="surface-edge px-6 py-5 flex gap-4 items-start">
      <div className="mt-0.5 grid h-8 w-8 place-items-center rounded-full bg-sunSoft text-sun">
        <svg viewBox="0 0 24 24" fill="none" className="h-4 w-4">
          <path
            d="M12 8v5m0 3.5h.01M10.3 3.86l-7.4 12.79A2 2 0 004.6 19h14.8a2 2 0 001.7-2.99L13.7 3.86a2 2 0 00-3.4 0z"
            stroke="currentColor"
            strokeWidth="1.8"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
      <div className="text-sm text-ink leading-relaxed max-w-3xl">
        <span className="font-semibold">What you are looking at.</span>{" "}
        <span className="text-inkMuted">
          Every price on this page is a{" "}
          <span className="font-semibold text-ink">pre-insurance list price</span>,
          either the hospital's gross charge or its cash-pay rate, pulled
          directly from the machine-readable file the hospital is required to
          publish. Insurance-negotiated rates and your final out-of-pocket cost
          are different numbers. Always ask your hospital's billing office for
          a written estimate before a planned procedure.
        </span>
      </div>
    </div>
  );
}
