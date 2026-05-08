import { stateName } from "../format";

export function StateFilter({
  states,
  value,
  onChange,
  label = "Showing",
}: {
  states: string[];
  value: string;
  onChange: (v: string) => void;
  label?: string;
}) {
  return (
    <div className="flex items-center gap-3">
      <label className="eyebrow whitespace-nowrap">{label}</label>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="appearance-none rounded-full border border-line bg-surface pl-5 pr-10 py-2 text-sm font-medium text-ink hover:border-ink focus:border-ink focus:outline-none focus:ring-4 focus:ring-ink/10 transition-all"
        >
          <option value="">All states ({states.length})</option>
          {states.map((s) => (
            <option key={s} value={s}>
              {stateName(s)}
            </option>
          ))}
        </select>
        <svg
          aria-hidden
          viewBox="0 0 24 24"
          className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-inkMuted"
          fill="none"
        >
          <path
            d="M6 9l6 6 6-6"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
    </div>
  );
}
