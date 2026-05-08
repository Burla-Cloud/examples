/**
 * Refined editorial palette: warm off-white, deep ink, restrained accent.
 * Less is more. One serif (Fraunces) for display, Inter for everything else.
 */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: "#FAFAF7",
        surface: "#FFFFFF",
        section: "#F2EFE8",
        sectionDeep: "#E8E4D9",
        ink: "#0A0F1A",
        inkMuted: "#4D5664",
        inkSubtle: "#8A93A1",
        primary: "#0A0F1A",
        primaryDark: "#000000",
        primarySoft: "#EDEFF3",
        accent: "#E5573F",
        accentDeep: "#C84326",
        accentSoft: "#FBE8E2",
        mint: "#1F8F6E",
        mintSoft: "#DFEEDF",
        sun: "#D9A024",
        sunSoft: "#F7EDD4",
        rose: "#C13A3A",
        roseSoft: "#F9E1DE",
        line: "#E5E1D5",
        lineSoft: "#EDEAE0",
        chartGrid: "#EFEBDF",
      },
      fontFamily: {
        sans: ["Inter", "system-ui", "sans-serif"],
        display: ["Fraunces", "Manrope", "Inter", "Georgia", "serif"],
        mono: ["JetBrains Mono", "ui-monospace", "monospace"],
      },
      letterSpacing: {
        eyebrow: "0.18em",
        eyebrowTight: "0.12em",
      },
      boxShadow: {
        card: "0 1px 1.5px rgba(10, 15, 26, 0.04), 0 8px 28px rgba(10, 15, 26, 0.05)",
        cardHover:
          "0 2px 4px rgba(10, 15, 26, 0.06), 0 24px 48px rgba(10, 15, 26, 0.09)",
        ring: "0 0 0 4px rgba(10, 15, 26, 0.10)",
        accentRing: "0 0 0 4px rgba(229, 87, 63, 0.18)",
      },
      backgroundImage: {
        heroFade:
          "radial-gradient(60% 80% at 0% 0%, rgba(229, 87, 63, 0.10), transparent 65%), radial-gradient(50% 70% at 100% 0%, rgba(31, 143, 110, 0.08), transparent 60%), linear-gradient(180deg, #FCFAF4 0%, #FAFAF7 100%)",
        appFade: "linear-gradient(180deg, #FCFAF4 0%, #FAFAF7 320px, #FAFAF7 100%)",
        ribbon:
          "linear-gradient(90deg, #1F8F6E 0%, #1F8F6E var(--low,33%), #D9A024 var(--low,33%), #D9A024 var(--mid,66%), #C13A3A var(--mid,66%), #C13A3A 100%)",
      },
      keyframes: {
        floatIn: {
          "0%": { opacity: "0", transform: "translateY(10px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        slideRight: {
          "0%": { transform: "translateX(-8px)", opacity: "0" },
          "100%": { transform: "translateX(0)", opacity: "1" },
        },
        pulseDot: {
          "0%,100%": { transform: "scale(1)", opacity: "1" },
          "50%": { transform: "scale(1.6)", opacity: "0.5" },
        },
      },
      animation: {
        floatIn: "floatIn 500ms cubic-bezier(0.22, 1, 0.36, 1) both",
        slideRight: "slideRight 600ms cubic-bezier(0.22, 1, 0.36, 1) both",
        pulseDot: "pulseDot 2s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
