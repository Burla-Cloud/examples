import { Link, NavLink } from "react-router-dom";
import { Logo } from "./Logo";

export function Layout({ children }: { children: React.ReactNode }) {
  const navItems = [
    { to: "/explore", label: "Look up a price", end: false },
    { to: "/leaderboard", label: "Biggest gaps", end: false },
    { to: "/hospitals", label: "Hospitals", end: false },
    { to: "/how-we-did-this", label: "How we did this", end: false },
  ];

  return (
    <div className="min-h-screen flex flex-col bg-appFade">
      <header className="sticky top-0 z-30 border-b border-line/60 bg-bg/85 backdrop-blur-md">
        <div className="container-7 flex flex-wrap items-center justify-between gap-6 py-5">
          <Link to="/" className="group flex items-center gap-3">
            <Logo size={30} />
            <div className="leading-none">
              <p className="font-display text-[15px] font-semibold tracking-tight text-ink group-hover:text-accent transition-colors">
                Hospital Price Reality Check
              </p>
            </div>
          </Link>
          <nav className="flex flex-wrap items-center gap-7 text-sm">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.end}
                className={({ isActive }) =>
                  `relative font-medium transition-colors ${
                    isActive ? "text-ink" : "text-inkMuted hover:text-ink"
                  }`
                }
              >
                {({ isActive }) => (
                  <>
                    {item.label}
                    {isActive && (
                      <span className="absolute -bottom-[22px] left-0 right-0 h-px bg-ink" />
                    )}
                  </>
                )}
              </NavLink>
            ))}
          </nav>
        </div>
      </header>

      <main className="flex-1 container-7 py-12 md:py-20">{children}</main>

      <footer className="mt-24 border-t border-line bg-bg">
        <div className="container-7 grid gap-12 py-16 md:grid-cols-12 text-sm text-inkMuted">
          <div className="md:col-span-5">
            <div className="flex items-center gap-3">
              <Logo size={28} />
              <p className="font-display font-semibold text-ink">
                Hospital Price Reality Check
              </p>
            </div>
            <p className="mt-5 max-w-md leading-relaxed">
              Real prices from real hospitals' federal transparency files. Not
              medical advice and not your final bill, but a useful starting point
              before you ask a hospital for an estimate.
            </p>
          </div>
          <div className="md:col-span-3">
            <p className="eyebrow">Look around</p>
            <ul className="mt-5 space-y-3">
              <li>
                <Link to="/explore" className="hover:text-ink transition-colors">
                  Look up a procedure or drug
                </Link>
              </li>
              <li>
                <Link to="/leaderboard" className="hover:text-ink transition-colors">
                  Biggest price gaps
                </Link>
              </li>
              <li>
                <Link to="/hospitals" className="hover:text-ink transition-colors">
                  Hospitals in this run
                </Link>
              </li>
              <li>
                <Link to="/how-we-did-this" className="hover:text-ink transition-colors">
                  How we did this
                </Link>
              </li>
            </ul>
          </div>
          <div className="md:col-span-4">
            <p className="eyebrow">Behind the scenes</p>
            <ul className="mt-5 space-y-3">
              <li>
                Built on the federal Hospital Price Transparency rule,{" "}
                <span className="text-ink font-medium">45 CFR Part 180</span>.
              </li>
              <li>Open source. Always real prices, no synthetic fill.</li>
              <li>
                <Link
                  to="/how-we-did-this"
                  className="text-ink underline-offset-4 hover:underline"
                >
                  How we did this
                </Link>{" "}
                covers the sources, the parsers, and the stack.
              </li>
            </ul>
          </div>
        </div>
        <div className="border-t border-lineSoft py-6 text-center text-xs text-inkSubtle">
          Numbers shown here are what hospitals publish, not what you will pay.
          Insurance coverage, deductibles, and balance bills can change the
          final amount in either direction.
        </div>
      </footer>
    </div>
  );
}
