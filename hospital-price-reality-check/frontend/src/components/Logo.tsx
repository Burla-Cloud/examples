export function Logo({ size = 32 }: { size?: number }) {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      viewBox="0 0 32 32"
      width={size}
      height={size}
      role="img"
      aria-label="Hospital Price Reality Check"
    >
      <rect width="32" height="32" rx="9" fill="#0A0F1A" />
      <path
        d="M5 18 L11 18 L13 13 L15 22 L18 9 L20 18 L27 18"
        stroke="#E5573F"
        strokeWidth="2.4"
        strokeLinecap="round"
        strokeLinejoin="round"
        fill="none"
      />
    </svg>
  );
}
