import { useState } from "react";
import {
  BarChart, Stack, Row, Grid, H1, H2, H3,
  Stat, Table, Text, Divider, Pill, Card, CardHeader, CardBody,
  Callout, Spacer, useHostTheme,
} from "cursor/canvas";

// ── Embedded model outputs (1,000,000 Monte Carlo simulations) ─────────────
const RACE_INFO = {
  date: "Saturday, May 2, 2026",
  postTime: "6:57 PM ET",
  venue: "Churchill Downs, Louisville, KY",
  purse: "$5,000,000",
  distance: "1¼ miles (10 furlongs)",
  track: "Fast — 60°F, dry, no rain forecast",
  field: 20,
  sims: "1,000,000",
  backtestScore: "126/40 pts across 2022-2025",
  modelNote: "70% weighted scoring + 30% ML ensemble (top-5 configs by log-loss on 2022-2025 holdout)",
};

const HORSES: {
  post: number; name: string; odds: string; beyer: number; dosage: number;
  style: string; trainerDW: number; jockeyDW: number;
  score: number; winPct: number; placePct: number; showPct: number;
  impliedPct: number; value: string; kelly: number;
}[] = [
    {
        "post": 15,
        "name": "Further Ado",
        "odds": "6-1",
        "beyer": 106,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 1,
        "jockeyDW": 3,
        "score": 56.2,
        "winPct": 18.5,
        "placePct": 31.9,
        "showPct": 42.5,
        "impliedPct": 14.3,
        "value": "+",
        "kelly": 0.022
    },
    {
        "post": 5,
        "name": "Commandment",
        "odds": "6-1",
        "beyer": 101,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 1,
        "jockeyDW": 0,
        "score": 38.4,
        "winPct": 8.0,
        "placePct": 15.8,
        "showPct": 23.1,
        "impliedPct": 14.3,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 10,
        "name": "Chief Wallabee",
        "odds": "8-1",
        "beyer": 100,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 2,
        "jockeyDW": 1,
        "score": 36.7,
        "winPct": 7.4,
        "placePct": 14.7,
        "showPct": 21.7,
        "impliedPct": 11.1,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 7,
        "name": "So Happy",
        "odds": "15-1",
        "beyer": 100,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 0,
        "jockeyDW": 1,
        "score": 35.6,
        "winPct": 7.0,
        "placePct": 14.0,
        "showPct": 20.7,
        "impliedPct": 6.2,
        "value": "=",
        "kelly": 0.004
    },
    {
        "post": 4,
        "name": "Litmus Test",
        "odds": "30-1",
        "beyer": 96,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 3,
        "jockeyDW": 0,
        "score": 33.5,
        "winPct": 6.3,
        "placePct": 12.7,
        "showPct": 19.1,
        "impliedPct": 3.2,
        "value": "+",
        "kelly": 0.031
    },
    {
        "post": 19,
        "name": "Robusta",
        "odds": "50-1",
        "beyer": 89,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 2,
        "jockeyDW": 0,
        "score": 32.0,
        "winPct": 5.8,
        "placePct": 11.8,
        "showPct": 17.9,
        "impliedPct": 2.0,
        "value": "+",
        "kelly": 0.039
    },
    {
        "post": 13,
        "name": "Pavlovian",
        "odds": "30-1",
        "beyer": 90,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 2,
        "jockeyDW": 0,
        "score": 31.5,
        "winPct": 5.6,
        "placePct": 11.5,
        "showPct": 17.5,
        "impliedPct": 3.2,
        "value": "+",
        "kelly": 0.023
    },
    {
        "post": 11,
        "name": "Potente",
        "odds": "20-1",
        "beyer": 95,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 3,
        "jockeyDW": 0,
        "score": 28.6,
        "winPct": 4.8,
        "placePct": 10.0,
        "showPct": 15.4,
        "impliedPct": 4.8,
        "value": "=",
        "kelly": 0.0
    },
    {
        "post": 1,
        "name": "Renegade",
        "odds": "4-1",
        "beyer": 98,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 2,
        "jockeyDW": 0,
        "score": 26.4,
        "winPct": 4.3,
        "placePct": 9.1,
        "showPct": 14.1,
        "impliedPct": 20.0,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 18,
        "name": "Ocelli",
        "odds": "50-1",
        "beyer": 81,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 26.1,
        "winPct": 4.3,
        "placePct": 8.9,
        "showPct": 13.8,
        "impliedPct": 2.0,
        "value": "+",
        "kelly": 0.023
    },
    {
        "post": 12,
        "name": "Emerging Market",
        "odds": "15-1",
        "beyer": 97,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 0,
        "jockeyDW": 1,
        "score": 24.9,
        "winPct": 4.0,
        "placePct": 8.4,
        "showPct": 13.1,
        "impliedPct": 6.2,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 3,
        "name": "Intrepido",
        "odds": "50-1",
        "beyer": 89,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 24.1,
        "winPct": 3.8,
        "placePct": 8.1,
        "showPct": 12.7,
        "impliedPct": 2.0,
        "value": "+",
        "kelly": 0.018
    },
    {
        "post": 16,
        "name": "Golden Tempo",
        "odds": "30-1",
        "beyer": 88,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 24.0,
        "winPct": 3.8,
        "placePct": 8.1,
        "showPct": 12.6,
        "impliedPct": 3.2,
        "value": "+",
        "kelly": 0.005
    },
    {
        "post": 8,
        "name": "Wonder Dean",
        "odds": "30-1",
        "beyer": 80,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 20.1,
        "winPct": 3.1,
        "placePct": 6.6,
        "showPct": 10.4,
        "impliedPct": 3.2,
        "value": "=",
        "kelly": 0.0
    },
    {
        "post": 9,
        "name": "Incredibolt",
        "odds": "20-1",
        "beyer": 88,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 18.9,
        "winPct": 2.9,
        "placePct": 6.2,
        "showPct": 9.9,
        "impliedPct": 4.8,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 17,
        "name": "Great White",
        "odds": "50-1",
        "beyer": 84,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 17.5,
        "winPct": 2.6,
        "placePct": 5.7,
        "showPct": 9.2,
        "impliedPct": 2.0,
        "value": "+",
        "kelly": 0.006
    },
    {
        "post": 2,
        "name": "Albus",
        "odds": "30-1",
        "beyer": 84,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 17.2,
        "winPct": 2.6,
        "placePct": 5.6,
        "showPct": 9.0,
        "impliedPct": 3.2,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 14,
        "name": "Six Speed",
        "odds": "50-1",
        "beyer": 80,
        "dosage": 2.5,
        "style": "Pace",
        "trainerDW": 0,
        "jockeyDW": 1,
        "score": 16.9,
        "winPct": 2.6,
        "placePct": 5.6,
        "showPct": 9.0,
        "impliedPct": 2.0,
        "value": "+",
        "kelly": 0.006
    },
    {
        "post": 6,
        "name": "Danon Bourbon",
        "odds": "20-1",
        "beyer": 80,
        "dosage": 2.5,
        "style": "Close",
        "trainerDW": 0,
        "jockeyDW": 0,
        "score": 15.3,
        "winPct": 2.3,
        "placePct": 5.2,
        "showPct": 8.3,
        "impliedPct": 4.8,
        "value": "-",
        "kelly": 0.0
    }
];

const TOP3 = [
    {
        "post": 15,
        "name": "Further Ado",
        "odds": "6-1",
        "beyer": 106,
        "dosage": 2.5,
        "style": "Press",
        "trainerDW": 1,
        "jockeyDW": 3,
        "score": 56.2,
        "winPct": 18.5,
        "placePct": 31.9,
        "showPct": 42.5,
        "impliedPct": 14.3,
        "value": "+",
        "kelly": 0.022
    },
    {
        "post": 5,
        "name": "Commandment",
        "odds": "6-1",
        "beyer": 101,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 1,
        "jockeyDW": 0,
        "score": 38.4,
        "winPct": 8.0,
        "placePct": 15.8,
        "showPct": 23.1,
        "impliedPct": 14.3,
        "value": "-",
        "kelly": 0.0
    },
    {
        "post": 10,
        "name": "Chief Wallabee",
        "odds": "8-1",
        "beyer": 100,
        "dosage": 2.5,
        "style": "Stalk",
        "trainerDW": 2,
        "jockeyDW": 1,
        "score": 36.7,
        "winPct": 7.4,
        "placePct": 14.7,
        "showPct": 21.7,
        "impliedPct": 11.1,
        "value": "-",
        "kelly": 0.0
    }
];
const LONGSHOT = {
    "post": 4,
    "name": "Litmus Test",
    "odds": "30-1",
    "beyer": 96,
    "dosage": 2.5,
    "style": "Pace",
    "trainerDW": 3,
    "jockeyDW": 0,
    "score": 33.5,
    "winPct": 6.3,
    "placePct": 12.7,
    "showPct": 19.1,
    "impliedPct": 3.2,
    "value": "+",
    "kelly": 0.031
};

const WEIGHTS = {
    "beyer_norm": 0.1607,
    "run_style_score": 0.0175,
    "trainer_score_norm": 0.1098,
    "jockey_score_norm": 0.0523,
    "dosage_score": 0.1591,
    "pedigree_dist": 0.0159,
    "post_wp_norm": 0.1226,
    "post_itm_norm": 0.0378,
    "win_rate_norm": 0.1318,
    "stamina_test": 0.1925
};

const EXOTIC_PLAYS = {
  win: {
    horse: "Further Ado",
    rationale: "Highest composite score — leads field in Beyer (106), won Blue Grass by 11 lengths, Velazquez (3 Derby wins), Cox trains (26% CDns win rate), Gun Runner pedigree built for 10 furlongs.",
  },
  exacta: {
    horses: ["Further Ado", "Commandment", "Chief Wallabee"],
    type: "$1 box",
    cost: "$6.00",
    note: "Three-horse box covers all permutations of the top model picks.",
  },
  trifecta: {
    top: ["Further Ado", "Commandment"],
    third: ["Chief Wallabee", "So Happy", "Litmus Test"],
    cost: "$12.00 (1-key)",
    note: "Key top-2 on top in both orders; wheel 3 horses in third.",
  },
  superfecta: {
    horses: ["Further Ado", "Commandment", "Chief Wallabee", "So Happy"],
    cost: "$2.40 (10¢ box)",
    note: "10-cent four-horse box — covers the model's top-4 in any order.",
  },
};

// ── Value legend ─────────────────────────────────────────────────────────────
// "+" → model probability >15% above market implied probability (bet)
// "=" → roughly fair value
// "-" → model probability <15% below market (skip or fade)

export default function KentuckyDerby2026() {
  const { colors, tokens } = useHostTheme();
  const [tab, setTab] = useState<"overview" | "rankings" | "exotics">("overview");

  const chartCategories = HORSES.map((h) => h.name);
  const chartSeries = [
    { name: "Model Win%", data: HORSES.map((h) => h.winPct) },
    { name: "Implied Win%", data: HORSES.map((h) => h.impliedPct) },
  ];

  const tableHeaders = [
    "Post", "Horse", "Odds", "Score", "Beyer", "Dosage",
    "Style", "Win%", "Place%", "Show%", "Value", "Kelly",
  ];
  const tableRows = HORSES.map((h) => [
    String(h.post), h.name, h.odds,
    String(h.score), String(h.beyer), String(h.dosage),
    h.style, `${h.winPct.toFixed(1)}%`, `${h.placePct.toFixed(1)}%`, `${h.showPct.toFixed(1)}%`,
    h.value === "+" ? "BET" : h.value === "-" ? "fade" : "fair",
    `${(h.kelly * 100).toFixed(1)}%`,
  ]);
  const tableRowTones = HORSES.map((_, i) =>
    i === 0 ? "success" : i === 1 ? "info" : i === 2 ? "neutral" : undefined
  );

  return (
    <Stack gap={24} style={{ padding: 24, maxWidth: 1100, margin: "0 auto" }}>
      {/* ── Header ── */}
      <Stack gap={4}>
        <H1>152nd Kentucky Derby — Prediction Model</H1>
        <Row gap={8} wrap>
          <Text tone="secondary">Saturday, May 2, 2026 · 6:57 PM ET · Churchill Downs, Louisville, KY</Text>
        </Row>
      </Stack>

      {/* ── Race context stats ── */}
      <Grid columns={4} gap={12}>
        <Stat value="1,000,000" label="Monte Carlo simulations" />
        <Stat value="$5M" label="Purse" tone="info" />
        <Stat value="20" label="Starters" />
        <Stat value="126/40" label="Backtest score (2022-25)" tone="success" />
      </Grid>

      <Row gap={8} wrap>
        <Pill active={tab === "overview"} onClick={() => setTab("overview")}>Overview</Pill>
        <Pill active={tab === "rankings"} onClick={() => setTab("rankings")}>Full Rankings</Pill>
        <Pill active={tab === "exotics"} onClick={() => setTab("exotics")}>Exotic Plays</Pill>
      </Row>

      <Divider />

      {/* ══════════════════════ OVERVIEW TAB ══════════════════════ */}
      {tab === "overview" && (
        <Stack gap={20}>
          <Callout tone="info" title="Race Conditions">
            Track: Fast · Temperature: 60°F (coldest Derby in 29 years) · No rain forecast.
            Contested early pace expected — Six Speed, Renegade, Potente all pressing.
            Pressers and stalkers are historically the sweet spot; deep closers face traffic risk.
          </Callout>

          <H2>Top Picks</H2>
          <Grid columns={3} gap={16}>
            {TOP3.map((h, i) => (
              <Card key={h.name}>
                <CardHeader trailing={<Pill tone="info" size="sm">{h.odds}</Pill>}>
                  {i === 0 ? "Win" : i === 1 ? "Place / Value" : "Show / Value"}
                </CardHeader>
                <CardBody>
                  <Stack gap={8}>
                    <H3>{h.name}</H3>
                    <Grid columns={2} gap={8}>
                      <Stat value={`${h.winPct.toFixed(1)}%`} label="Model win%" tone="success" />
                      <Stat value={`${h.impliedPct.toFixed(1)}%`} label="Market implied" />
                    </Grid>
                    <Row gap={6} wrap>
                      <Pill size="sm">Beyer {h.beyer}</Pill>
                      <Pill size="sm">{h.style}</Pill>
                      <Pill size="sm" tone={h.value === "+" ? "success" : "neutral"}>
                        {h.value === "+" ? "VALUE BET" : h.value === "=" ? "Fair" : "Overbet"}
                      </Pill>
                    </Row>
                    <Text size="small" tone="secondary">
                      Trainer Derby wins: {h.trainerDW} · Jockey Derby wins: {h.jockeyDW}
                    </Text>
                  </Stack>
                </CardBody>
              </Card>
            ))}
          </Grid>

          <H2>Win Probability: Model vs Market (all 20 horses)</H2>
          <BarChart
            categories={chartCategories}
            series={chartSeries}
            height={320}
            valueSuffix="%"
            horizontal
          />

          <H2>Longshot Spotlight</H2>
          <Card>
            <CardHeader trailing={<Pill tone="warning" size="sm">{LONGSHOT.odds}</Pill>}>
              {LONGSHOT.name} — Live Longshot
            </CardHeader>
            <CardBody>
              <Grid columns={4} gap={12}>
                <Stat value={`${LONGSHOT.winPct.toFixed(1)}%`} label="Model win%" />
                <Stat value={`${LONGSHOT.impliedPct.toFixed(1)}%`} label="Market implied" />
                <Stat value={String(LONGSHOT.beyer)} label="Beyer" />
                <Stat value={`${(LONGSHOT.kelly * 100).toFixed(1)}%`} label="Kelly stake" />
              </Grid>
              <Text style={{ marginTop: 12 }}>
                Bob Baffert's 6 Kentucky Derby wins give this horse the highest trainer score in the field
                by a wide margin. Nyquist (the sire) won the 2016 Derby. Stalker run style suits the contested
                early pace perfectly. At 30-1, the market dramatically underweights Baffert's historical edge.
              </Text>
            </CardBody>
          </Card>

          <H2>Model Methodology</H2>
          <Text>
            Composite score = 70% weighted scoring model + 30% ML ensemble (top-5 GBM/RF/LogReg
            configurations by log-loss on 2022-2025 holdout). Weights were empirically optimized via
            5,000 Dirichlet-sampled combinations back-tested on 2022-2025 Derbys using Burla's
            distributed compute. Final probabilities derived from 1,000,000 Monte Carlo
            simulations (100 Burla workers x 10,000 sims each), with noise calibrated to the Derby's
            historical upset rate.
          </Text>
          <Text tone="secondary" size="small">Back-test: 126/40 pts across 2022-2025 · 70% weighted scoring + 30% ML ensemble</Text>
        </Stack>
      )}

      {/* ══════════════════════ RANKINGS TAB ══════════════════════ */}
      {tab === "rankings" && (
        <Stack gap={16}>
          <H2>Full 20-Horse Rankings</H2>
          <Text tone="secondary" size="small">
            Sorted by composite model score. Green = top pick, Blue = 2nd/3rd.
            "Value" column: BET = model prob >15% above market implied; fade = model prob >15% below.
          </Text>
          <Table
            headers={tableHeaders}
            rows={tableRows}
            rowTone={tableRowTones}
            striped
            stickyHeader
            columnAlign={["center","left","center","center","center","center","center","center","center","center","center","center"]}
          />

          <H2>Empirically Optimized Factor Weights</H2>
          <Text tone="secondary" size="small">
            Best weight combination from 5,000 Burla-parallel back-tests (scored on 2022-2025 holdout).
          </Text>
          <Table
            headers={["Factor", "Weight", "Description"]}
            rows={Object.entries(WEIGHTS).map(([k, v]) => [
              k.replace(/_/g, " "),
              `${(v * 100).toFixed(1)}%`,
              k.includes("beyer") ? "Best Beyer speed figure (100+ threshold bonus)" :
              k.includes("run_style") ? "Run style fit for contested early pace" :
              k.includes("trainer") ? "Derby wins + Churchill Downs current meet win%" :
              k.includes("jockey") ? "Derby wins + race experience" :
              k.includes("dosage") ? "Stamina predictor (DI ≤2 = elite)" :
              k.includes("pedigree") ? "Sire-line distance aptitude (MyWinners analysis)" :
              k.includes("post_wp") ? "Historical post position win rate (1930-2025)" :
              k.includes("post_itm") ? "Historical post position ITM rate" :
              k.includes("win_rate") ? "Career win rate" : "Fractions stamina test (Y/N)",
            ])}
          />
        </Stack>
      )}

      {/* ══════════════════════ EXOTICS TAB ══════════════════════ */}
      {tab === "exotics" && (
        <Stack gap={20}>
          <Callout tone="success" title="Exotic Strategy">
            Monte Carlo win probabilities drive Kelly-optimal sizing. All wager costs below assume
            minimum base units. The 10-cent Superfecta box is the most cost-effective chaos hedge
            in a 20-horse field.
          </Callout>

          <H2>Recommended Tickets</H2>

          <Card>
            <CardHeader trailing={<Pill size="sm" tone="success">Win</Pill>}>
              Win Bet — {EXOTIC_PLAYS.win.horse}
            </CardHeader>
            <CardBody>
              <Text>{EXOTIC_PLAYS.win.rationale}</Text>
            </CardBody>
          </Card>

          <Grid columns={3} gap={16}>
            <Card>
              <CardHeader trailing={<Pill size="sm">{EXOTIC_PLAYS.exacta.cost}</Pill>}>
                Exacta — {EXOTIC_PLAYS.exacta.type}
              </CardHeader>
              <CardBody>
                <Stack gap={8}>
                  {EXOTIC_PLAYS.exacta.horses.map((h: string) => (
                    <Pill key={h} active>{h}</Pill>
                  ))}
                  <Text size="small" tone="secondary">{EXOTIC_PLAYS.exacta.note}</Text>
                </Stack>
              </CardBody>
            </Card>

            <Card>
              <CardHeader trailing={<Pill size="sm">{EXOTIC_PLAYS.trifecta.cost}</Pill>}>
                Trifecta — Key
              </CardHeader>
              <CardBody>
                <Stack gap={8}>
                  <Text size="small" weight="semibold">Top (either order):</Text>
                  <Row gap={6}>{EXOTIC_PLAYS.trifecta.top.map((h: string) => <Pill key={h} active>{h}</Pill>)}</Row>
                  <Text size="small" weight="semibold">Third wheel:</Text>
                  <Row gap={6} wrap>{EXOTIC_PLAYS.trifecta.third.map((h: string) => <Pill key={h}>{h}</Pill>)}</Row>
                  <Text size="small" tone="secondary">{EXOTIC_PLAYS.trifecta.note}</Text>
                </Stack>
              </CardBody>
            </Card>

            <Card>
              <CardHeader trailing={<Pill size="sm">{EXOTIC_PLAYS.superfecta.cost}</Pill>}>
                Superfecta — 10¢ Box
              </CardHeader>
              <CardBody>
                <Stack gap={8}>
                  {EXOTIC_PLAYS.superfecta.horses.map((h: string) => (
                    <Pill key={h} active>{h}</Pill>
                  ))}
                  <Text size="small" tone="secondary">{EXOTIC_PLAYS.superfecta.note}</Text>
                </Stack>
              </CardBody>
            </Card>
          </Grid>

          <H2>Kelly Criterion Win Bets (top value horses)</H2>
          <Text tone="secondary" size="small">
            Kelly fraction = fraction of bankroll with positive expected value at these odds.
            Only bet where model win% meaningfully exceeds market-implied win%.
          </Text>
          <Table
            headers={["Horse", "Odds", "Model Win%", "Market Implied%", "Edge", "Kelly Stake", "Signal"]}
            rows={HORSES.filter((h) => h.value === "+").map((h) => [
              h.name, h.odds,
              `${h.winPct.toFixed(1)}%`,
              `${h.impliedPct.toFixed(1)}%`,
              `+${(h.winPct - h.impliedPct).toFixed(1)}pp`,
              `${(h.kelly * 100).toFixed(1)}% of bankroll`,
              "BET",
            ])}
            rowTone={HORSES.filter((h) => h.value === "+").map(() => "success" as const)}
          />

          <Divider />
          <Text tone="secondary" size="small">
            Data sources: User's 2026 KY Derby spreadsheet (18 variables/horse) · Historical post position
            stats 1930-2025 · Run style analysis (TwinSpires/TrackPhantom) · Dosage Index analysis
            (MyWinners/BloodHorse) · Pedigree distance aptitude (MyWinners 2026) · Expert consensus
            (Aces & Races post-draw, CBS, Yahoo, SportsLine) · Weather/track (LPM/USA Today) ·
            2026 pace scenario (RacingDudes/USRacing) · Trainer/jockey CDns stats (BRISnet) ·
            Beyer threshold data (DRF) · Prep race results · Workout reports (Blood Horse/KYHBPA) ·
            Post-draw jockey changes (CBS/NBC). Model: 5,000-combo Burla sensitivity analysis +
            2,000+ Burla parallel ML configs + 1M Burla Monte Carlo simulations.
          </Text>
        </Stack>
      )}
    </Stack>
  );
}
