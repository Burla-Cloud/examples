// Amazon Review Distiller. Data loader + renderer.
// Pure vanilla, no frameworks. All data lives in /data/*.json.
//
// Core flow:
//   Normal mode  -> Wall shows the 120-row "cry for help" corpus (data/wall.json).
//   Unhinged Mode -> Wall swaps to the 120-row merged unhinged corpus
//                   (data/unhinged.json) = hard profanity + censored-slur hits
//                   rescored to push false positives down, real rants up.
//
// All slur tokens in unhinged rows are auto-redacted at render time with a
// category badge. Raw strings never ship verbatim to the page.

const DATA = {
  index: null,
  wall: null,
  unhinged: null,
  findings: null,
  categories: null,
  catPages: {},
  searchPool: null,
  unhingedSearchPool: null,
};

const el = (id) => document.getElementById(id);
const esc = (s) =>
  String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");

// Clean inline HTML from Amazon review text before escaping.
const escReview = (s) => {
  let t = String(s == null ? "" : s);
  t = t.replace(/<br\s*\/?>/gi, "\n");
  t = t.replace(/<\/?(p|div|span|em|strong|i|b|u)[^>]*>/gi, "");
  t = t
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
  return esc(t);
};
const fmt = (n) => (n == null ? "…" : n.toLocaleString());
const fmtShort = (n) => {
  if (n == null) return "…";
  if (n >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
};
const pct = (x, dp = 2) => (x == null ? "…" : (x * 100).toFixed(dp) + "%");

// ---------------------------------------------------------------------
// Slur redaction. The raw lexicon never ships to the browser. Instead
// we render each slur as first-letter + asterisks + category badge.
// These patterns match both uncensored and already-censored variants.
// ---------------------------------------------------------------------

// Minimum character counts per pattern to avoid accidentally redacting
// legitimate short words. Patterns are ordered most-specific-first so
// overlapping matches resolve in favor of the harder category.
const SLUR_PATTERNS = [
  // RS_HARD. The hard-R tier.
  { re: /\bn(?:i|[\*\@\#\$\!\%\_\-\.1]){1,3}gg?(?:er|a|ah|az|ers|as|ahs)\b/gi, label: "RACIAL SLUR" },
  { re: /\bn\*{3,6}(?:er|a|ah|az|r)?\b/gi, label: "RACIAL SLUR" },
  // RS (other racial).
  { re: /\bch(?:i|[\*\@\#\$\!\%1])nk(?:s|y)?\b/gi, label: "RACIAL SLUR" },
  { re: /\bg(?:o|[\*\@\#\$\!\%])ok(?:s)?\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:jap|japs)\b/gi, label: "RACIAL SLUR" },
  { re: /\bsp(?:i|[\*\@\#\$\!])c(?:s)?\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:wetback|wetbacks|beaner|beaners)\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:raghead|ragheads|towelhead|towelheads)\b/gi, label: "RACIAL SLUR" },
  { re: /\bk(?:i|[\*\@\#\$\!])ke(?:s)?\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:kike|kikes|hebe|hebes|yid|yids)\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:jigaboo|jigaboos|porchmonkey|pickaninny|coons|sandnigger)\b/gi, label: "RACIAL SLUR" },
  { re: /\b(?:mulatto|quadroon|octoroon|injun|injuns|redskin|redskins)\b/gi, label: "RACIAL SLUR" },
  { re: /\bgringos?\b/gi, label: "RACIAL SLUR" },
  { re: /\bcracker(?:s)?\b/gi, label: "RACIAL SLUR" },
  // HOM.
  { re: /\bf(?:a|[\*\@\#\$\!])gg(?:ot|y|ots)\b/gi, label: "HOMOPHOBIC SLUR" },
  { re: /\bf\*{2,5}(?:got|gy|ots)\b/gi, label: "HOMOPHOBIC SLUR" },
  { re: /\b(?:fag|fags|faggot|faggots|faggy|fagged)\b/gi, label: "HOMOPHOBIC SLUR" },
  { re: /\b(?:dyke|dykes)\b/gi, label: "HOMOPHOBIC SLUR" },
  { re: /\btr(?:a|[\*\@\#\$\!])nn(?:y|ies)\b/gi, label: "HOMOPHOBIC SLUR" },
  { re: /\b(?:tranny|trannies|shemale|shemales)\b/gi, label: "HOMOPHOBIC SLUR" },
  // ABL. Only the clear slur tokens; idiot/moron/lame are too ambiguous.
  { re: /\br(?:e|[\*\@\#\$\!])t(?:a|[\*\@\#\$\!])rd(?:ed|s|ation)?\b/gi, label: "ABLEIST SLUR" },
  { re: /\br\*{3,6}(?:d?ed|s|ation)?\b/gi, label: "ABLEIST SLUR" },
  { re: /\b(?:retard|retards|retarded|retardo|mongoloid|mongoloids|spastic|spaz|spazz)\b/gi, label: "ABLEIST SLUR" },
  // Profanity. Display as first-letter + asterisks, no badge (common enough
  // that a badge on every f-bomb would be visually exhausting).
  { re: /\bf\*{2,5}(?:ing|in|ed|er|ers|s)?\b/gi, label: "" },
  { re: /\bsh\*{2,4}(?:ty|s|head|hole|bag)?\b/gi, label: "" },
  { re: /\bb\*{3,5}(?:es|y)?\b/gi, label: "" },
  { re: /\bc\*{3,4}(?:s|y)?\b/gi, label: "" },
  { re: /\ba\*{5,6}\b/gi, label: "" },
];

function redactSlursHTML(safeText) {
  // safeText is already HTML-escaped. We inject <span> tags with the
  // redacted form + category badge. Text outside matches passes through
  // unchanged.
  let out = safeText;
  for (const { re, label } of SLUR_PATTERNS) {
    out = out.replace(re, (m) => {
      const first = m[0] || "";
      const redacted = first + "*".repeat(Math.max(m.length - 1, 3));
      if (label) {
        return `<span class="slur" data-cat="${esc(label)}"><span class="slur-txt">${esc(
          redacted,
        )}</span><span class="slur-badge">${esc(label)}</span></span>`;
      }
      return `<span class="censor">${esc(redacted)}</span>`;
    });
  }
  return out;
}

const stars = (n) => {
  n = Number(n || 0);
  const full = "★".repeat(Math.round(n));
  const empty = "☆".repeat(5 - Math.round(n));
  return `${full}${empty}`;
};

// --- loader -----------------------------------------------------------

async function loadJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`failed ${path} ${r.status}`);
  return await r.json();
}

async function init() {
  try {
    const [index, wall, findings, categories] = await Promise.all([
      loadJSON("data/index.json"),
      loadJSON("data/wall.json"),
      loadJSON("data/findings.json"),
      loadJSON("data/categories.json"),
    ]);
    DATA.index = index;
    DATA.wall = wall;
    DATA.findings = findings;
    DATA.categories = categories;
  } catch (e) {
    document.body.insertAdjacentHTML(
      "afterbegin",
      `<div style="padding:40px;background:#fee;color:#900;text-align:center">Data not loaded yet: ${esc(
        e.message,
      )}. Run the analysis first (<code>python analysis.py</code>).</div>`,
    );
    return;
  }
  renderHeader();
  renderHero();
  renderWall();
  renderCategories();
  renderFindings();
  wireSearch();
  wireUnhingedToggle();
  wireModalClose();

  // Load the unhinged corpus + wider search index async.
  loadJSON("data/unhinged.json")
    .then((d) => {
      DATA.unhinged = d;
      // If unhinged toggle is already on from localStorage, re-render.
      if (unhingedOn()) renderWall();
    })
    .catch(() => {
      // Silent fallback: normal wall still works.
    });

  loadJSON("data/search.json")
    .then((rows) => {
      DATA.searchPool = rows;
    })
    .catch(() => {});

  loadJSON("data/unhinged_search.json")
    .then((d) => {
      DATA.unhingedSearchPool = d.rows || d;
    })
    .catch(() => {});
}

// --- header + hero ----------------------------------------------------

function renderHeader() {
  const idx = DATA.index;
  el("hdrTotal").textContent = fmtShort(idx.total_parsed) + " reviews";

  const selector = el("catFilter");
  DATA.categories.forEach((c) => {
    const opt = document.createElement("option");
    opt.value = c.cat;
    opt.textContent = c.name;
    selector.appendChild(opt);
  });
}

function renderHero() {
  const idx = DATA.index;
  el("sRev").textContent = fmt(idx.total_parsed);
  el("sProf").textContent = fmt(idx.total_profane);
  el("sCats").textContent = idx.n_categories;
  el("sRate").textContent = pct(idx.profanity_rate_global, 2);
  el("heroRows").textContent = fmt(idx.total_parsed);
  el("heroCats").textContent = idx.n_categories;
}

// --- review card ------------------------------------------------------

function tagList(r) {
  const tags = [];
  const cat = DATA.categories.find((c) => c.cat === (r._category || r.category));
  if (cat) tags.push(`<span class="cat">${esc(cat.emoji)} ${esc(cat.name)}</span>`);
  return tags.join("");
}

function slurCategoryTags(r) {
  const out = [];
  const cats = r._slur_categories || [];
  const LABEL = {
    RS_HARD: "RACIAL SLUR",
    RS: "RACIAL SLUR",
    HOM: "HOMOPHOBIC",
    ABL: "ABLEIST",
    SEX: "GENDERED",
    XEN: "XENOPHOBIC",
    VULG: "HARD PROFANITY",
  };
  const SEEN = new Set();
  for (const c of cats) {
    const lbl = LABEL[c] || c;
    if (SEEN.has(lbl)) continue;
    SEEN.add(lbl);
    const klass = lbl.toLowerCase().replace(/\s+/g, "-");
    out.push(`<span class="slur-tag slur-tag-${esc(klass)}">${esc(lbl)}</span>`);
  }
  return out.join("");
}

function metaTags(r) {
  const out = [];
  if (r.verified) out.push(`<span class="tag verified">verified</span>`);
  if (r.helpful_vote >= 10) out.push(`<span class="tag helpful">${r.helpful_vote} helpful</span>`);
  const s = r.score || r._score || {};
  if (s.strong_profane > 0) out.push(`<span class="tag danger">${s.strong_profane} profanity</span>`);
  if (s.caps_ratio > 0.5 && s.n_words > 5) out.push(`<span class="tag">${(s.caps_ratio * 100) | 0}% CAPS</span>`);
  if (s.max_exclam_run >= 5) out.push(`<span class="tag">${s.max_exclam_run}!</span>`);
  if (s.n_chars != null) out.push(`<span class="tag" style="background:#555">${s.n_chars} chars</span>`);
  return out.join(" ");
}

function reviewCard(r, rank, { unhinged = false } = {}) {
  const rating = Number(r.rating || 0);
  const starMarkup = Array.from({ length: 5 })
    .map((_, i) => (i < Math.round(rating) ? "★" : '<span class="ghost">★</span>'))
    .join("");
  let title = escReview(r.title || "(no title)");
  let body = escReview(r.text || "(no body)");
  if (unhinged) {
    title = redactSlursHTML(title);
    body = redactSlursHTML(body);
  }
  const slurTags = unhinged ? slurCategoryTags(r) : "";
  return `
    <article class="rev ${unhinged ? "rev-unhinged" : ""}">
      ${rank != null ? `<span class="rank">#${rank}</span>` : ""}
      ${tagList(r)}
      ${slurTags ? `<div class="slur-tag-row">${slurTags}</div>` : ""}
      <div class="stars">${starMarkup}</div>
      <div class="title">${title}</div>
      <div class="body">${body}</div>
      <div class="meta">
        <span class="tags">${metaTags(r)}</span>
        <span class="asin">ASIN ${esc(r.asin || "…")}</span>
      </div>
    </article>
  `;
}

// Clamp tall review bodies and inject a "Show more" toggle. Only cards
// whose rendered body exceeds CLAMP_PX get the fade overlay + button;
// short reviews render unchanged.
function attachMoreHandlers(scope) {
  if (!scope) return;
  const CLAMP_PX = 340;
  requestAnimationFrame(() => {
    scope.querySelectorAll(".rev").forEach((card) => {
      if (card.dataset.moreChecked) return;
      card.dataset.moreChecked = "1";
      const body = card.querySelector(".body");
      if (!body) return;
      if (body.scrollHeight <= CLAMP_PX + 20) return;
      card.classList.add("has-more");
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "show-more";
      btn.textContent = "Show more ▾";
      btn.addEventListener("click", () => {
        const expanded = card.classList.toggle("expanded");
        btn.textContent = expanded ? "Show less ▴" : "Show more ▾";
      });
      body.insertAdjacentElement("afterend", btn);
    });
  });
}

function miniRev(r, rank) {
  const rating = Number(r.rating || 0);
  const starMarkup = Array.from({ length: 5 })
    .map((_, i) => (i < Math.round(rating) ? "★" : '<span class="ghost">★</span>'))
    .join("");
  const cat = DATA.categories.find((c) => c.cat === (r._category || r.category));
  const title = escReview(r.title || "");
  const text = escReview((r.text || "").slice(0, 240));
  return `
    <div class="minirev">
      <span class="rnk" style="font-family:JetBrains Mono;color:#888">#${String(rank).padStart(2, "0")}</span>
      ${title ? `<div style="font-weight:700;margin:2px 0 4px">${title}</div>` : ""}
      <div class="qt">${text}${(r.text || "").length > 240 ? "…" : ""}</div>
      <div class="stars">${starMarkup}</div>
      <div class="ft">
        <span>${cat ? `${cat.emoji} ${esc(cat.name)}` : ""}</span>
        <span>ASIN ${esc(r.asin || "…")}</span>
      </div>
    </div>
  `;
}

// --- unified wall -----------------------------------------------------
//
// Normal mode   -> DATA.wall.rows (mild, "cry for help")
// Unhinged mode -> DATA.unhinged.rows (hard profanity + slur corpus merged)
//
// Toggling re-renders in place. Slur redaction is applied when rendering
// the unhinged source only.

function activeWallData() {
  if (unhingedOn() && DATA.unhinged) return { data: DATA.unhinged, unhinged: true };
  return { data: DATA.wall, unhinged: false };
}

function renderWall() {
  const { data, unhinged } = activeWallData();
  if (!data) return;
  const title = el("wallTitle");
  const blurb = el("wallBlurb");
  if (title) {
    title.textContent = unhinged ? "The Wall of Fucked Up" : "The Wall of Rants";
  }
  blurb.textContent = data.blurb || "";
  const wrap = el("wallList");
  wrap.innerHTML = data.rows
    .map((r, i) => reviewCard(r, i + 1, { unhinged }))
    .join("");
  attachMoreHandlers(wrap);
}

// --- categories grid --------------------------------------------------

function renderCategories() {
  const wrap = el("catGrid");
  const rows = DATA.categories.filter((c) => c.n_parsed > 0);
  const maxRate = Math.max(...rows.map((c) => c.profanity_rate));

  wrap.innerHTML = rows
    .map((c) => {
      const w = (c.profanity_rate / maxRate) * 100;
      return `
        <button class="cat-card" data-cat="${esc(c.cat)}">
          <span class="emoji">${esc(c.emoji)}</span>
          <div class="nm">${esc(c.name)}</div>
          <div class="bar"><span style="width:${w}%"></span></div>
          <div class="lbl">
            <span>${fmtShort(c.n_parsed)} reviews</span>
            <span class="pct">${(c.profanity_rate * 100).toFixed(2)}%</span>
          </div>
        </button>
      `;
    })
    .join("");

  wrap.querySelectorAll(".cat-card").forEach((btn) => {
    btn.onclick = () => openCategoryModal(btn.dataset.cat);
  });
}

async function openCategoryModal(cat) {
  const modal = el("catModal");
  const body = el("catBody");
  body.innerHTML = `<p>Loading ${esc(cat)}…</p>`;
  modal.showModal();
  if (!DATA.catPages[cat]) {
    try {
      DATA.catPages[cat] = await loadJSON(`data/categories/${cat}.json`);
    } catch (e) {
      body.innerHTML = `<p>Failed to load category: ${esc(e.message)}</p>`;
      return;
    }
  }
  renderCategoryModal(cat);
}

function renderCategoryModal(cat) {
  const d = DATA.catPages[cat];
  const body = el("catBody");
  const sections = [
    ["top_profane", "🔪 Top profane"],
    ["top_rant", "📢 Rant hall of fame"],
    ["top_screaming", "CAPS LOCK CHAMPIONS"],
    ["top_exclaim", "!!! Punctuation bombs"],
    ["top_short_brutal", "✂️ Short & brutal"],
    ["top_five_star_obscene", "⭐ Five-star obscene"],
    ["top_five_star_one_word", "🤐 Five-star silent"],
  ];
  const rc = d.rating_counts || {};
  const total = d.n_parsed || 1;
  const unhinged = unhingedOn();
  body.innerHTML = `
    <div class="modal-head">
      <span class="emoji">${esc(d.emoji)}</span>
      <div>
        <h2>${esc(d.name)}</h2>
        <div class="ck">${fmt(d.n_parsed)} reviews · ${pct(d.profanity_rate)} profane · ${d.mean_length} chars mean · ratings
          ${["1", "2", "3", "4", "5"].map((k) => `<b>${k}★</b> ${(100 * (rc[k] || 0) / total).toFixed(1)}%`).join(" · ")}
        </div>
      </div>
    </div>
    ${sections
      .map(([key, label]) => {
        const rows = (d[key] || []).slice(0, 10);
        if (!rows.length) return "";
        return `
          <div class="modal-section">
            <h3>${label}</h3>
            <div class="review-feed modal-feed">
              ${rows.map((r, i) => reviewCard({ ...r, _category: cat }, i + 1, { unhinged })).join("")}
            </div>
          </div>
        `;
      })
      .join("")}
  `;
  attachMoreHandlers(body);
}

function wireModalClose() {
  const modal = el("catModal");
  modal.querySelector(".modal-close").onclick = () => modal.close();
  modal.addEventListener("click", (e) => {
    if (e.target === modal) modal.close();
  });
}

// --- findings ---------------------------------------------------------

const RENDERERS = {
  category_profanity: (r, i) => `
    <div class="frow">
      <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
      <span class="label"><span class="emoji">${esc(r.emoji)}</span>${esc(r.name)}
        <span class="sm">${fmt(r.n_parsed)} reviews · ${fmt(r.n_profane)} profane</span>
      </span>
      <span class="val">${r.profanity_pct.toFixed(2)}%</span>
    </div>`,
  mean_length: (r, i) => `
    <div class="frow">
      <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
      <span class="label"><span class="emoji">${esc(r.emoji)}</span>${esc(r.name)}
        <span class="sm">${fmt(r.n_parsed)} reviews</span>
      </span>
      <span class="val">${r.mean_length} chars</span>
    </div>`,
  rating_distribution: (r, i) => {
    const colors = ["#cc0000", "#ff6600", "#ffa41c", "#b8cf58", "#007600"];
    const segs = [r.pct_1, r.pct_2, r.pct_3, r.pct_4, r.pct_5]
      .map((p, j) => `<span style="width:${p}%;background:${colors[j]}" title="${j + 1}★ ${p}%"></span>`)
      .join("");
    return `
      <div class="bar-row">
        <span class="label"><span class="emoji">${esc(r.emoji)}</span>${esc(r.name)}</span>
        <span class="stacked">${segs}</span>
        <span class="val" style="color:#cc0000">${r.pct_1}%</span>
      </div>`;
  },
};

function renderFindings() {
  const wrap = el("findingsWrap");
  wrap.innerHTML = "";
  for (const f of DATA.findings) {
    const card = document.createElement("div");
    card.className = "finding";
    const header = `<h3>${esc(f.title)}</h3><p class="blurb">${esc(f.blurb)}</p>`;
    const renderer = RENDERERS[f.id];
    let bodyHtml = "";
    if (renderer) {
      bodyHtml = f.rows.slice(0, 12).map((r, i) => renderer(r, i)).join("");
    } else {
      bodyHtml = f.rows.slice(0, 5).map((r, i) => miniRev(r, i + 1)).join("");
    }
    card.innerHTML = header + bodyHtml;
    wrap.appendChild(card);
  }
}

// --- search -----------------------------------------------------------

// Any row sourced from the vulgar / worst corpora carries either a `roots`
// dict (hunt_vulgar) or `_slur_categories` / `_source` (merge_unhinged).
function isUnhingedRow(r) {
  if (!r) return false;
  if (r._source === "hard_profanity" || r._source === "worst_of_worse") return true;
  const s = r.score;
  if (s && typeof s === "object" && s.roots && Object.keys(s.roots).length) return true;
  if (Array.isArray(r._slur_categories) && r._slur_categories.length) return true;
  return false;
}

function unhingedOn() {
  return document.body.classList.contains("unhinged");
}

function searchCorpus() {
  const includeUnhinged = unhingedOn();
  const seen = new Set();
  const out = [];
  const push = (r) => {
    const key = `${r.asin || ""}|${(r.title || "").slice(0, 40)}|${(r.text || "").slice(0, 60)}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(r);
  };
  // Wall always available.
  for (const r of DATA.wall?.rows || []) push(r);
  if (includeUnhinged) {
    for (const r of DATA.unhinged?.rows || []) push(r);
    for (const r of DATA.unhingedSearchPool || []) push(r);
  }
  for (const r of DATA.searchPool || []) {
    if (!includeUnhinged && isUnhingedRow(r)) continue;
    push(r);
  }
  return out;
}

function wireSearch() {
  const q = el("q");
  const sel = el("catFilter");
  const btn = el("searchBtn");
  const wrap = el("wallList");
  const blurb = el("wallBlurb");

  let debounce = null;

  const run = ({ scroll = false } = {}) => {
    const needle = (q.value || "").toLowerCase().trim();
    const catWanted = sel.value;

    if (!needle && !catWanted) {
      renderWall();
      return;
    }

    const unhinged = unhingedOn();
    const pool = searchCorpus();
    const filtered = pool.filter((r) => {
      if (catWanted && (r._category || r.category) !== catWanted) return false;
      if (needle) {
        const haystack = ((r.title || "") + " " + (r.text || "")).toLowerCase();
        if (!haystack.includes(needle)) return false;
      }
      return true;
    });

    const shown = filtered.slice(0, 200);

    if (shown.length) {
      wrap.innerHTML = shown
        .map((r, i) => reviewCard(r, i + 1, { unhinged: unhinged && isUnhingedRow(r) }))
        .join("");
      attachMoreHandlers(wrap);
    } else {
      const normalSugg = ["crap", "worst", "refund", "broken", "garbage", "pissed", "terrible", "damn"];
      const unhingedSugg = ["bitch", "shit", "asshole", "dick", "slut", "fuck", "cunt", "whore"];
      const suggList = unhinged ? unhingedSugg : normalSugg;
      wrap.innerHTML = `
        <div style="grid-column:1/-1;padding:36px 24px;text-align:center;background:#fff;border:1px dashed #ccc;border-radius:8px;color:#555">
          <div style="font-size:22px;font-weight:700;margin-bottom:6px">No matches for "${esc(needle || catWanted)}"</div>
          <div style="margin-bottom:14px">Amazon masks a lot of the classic four-letter words. Try one of these instead:</div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center">
            ${suggList
              .map(
                (w) =>
                  `<button class="suggest" data-suggest="${esc(w)}" style="background:#febd69;border:none;padding:6px 12px;border-radius:16px;font-weight:600;cursor:pointer">${esc(w)}</button>`,
              )
              .join("")}
          </div>
        </div>`;
      wrap.querySelectorAll(".suggest").forEach((b) => {
        b.onclick = () => {
          q.value = b.dataset.suggest;
          run({ scroll: true });
        };
      });
    }

    const catLabel = catWanted ? ` in ${catWanted.replace(/_/g, " ")}` : "";
    const truncNote = filtered.length > shown.length ? ` (showing first ${shown.length})` : "";
    blurb.textContent = `${filtered.length.toLocaleString()} matching review${
      filtered.length === 1 ? "" : "s"
    }${needle ? ` for "${needle}"` : ""}${catLabel}${truncNote}.`;

    if (scroll) {
      const wallSec = document.getElementById("wall");
      if (wallSec) wallSec.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  };

  q.addEventListener("input", () => {
    clearTimeout(debounce);
    debounce = setTimeout(() => run({ scroll: false }), 140);
  });
  q.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      clearTimeout(debounce);
      run({ scroll: true });
    }
  });
  btn.addEventListener("click", () => {
    clearTimeout(debounce);
    run({ scroll: true });
  });
  sel.addEventListener("change", () => run({ scroll: true }));

  window.__rerunSearch = () => {
    if ((q.value || "").trim() || sel.value) {
      run({ scroll: false });
    } else {
      renderWall();
    }
  };
}

// --- unhinged toggle --------------------------------------------------

const PLACEHOLDER_TAME = "Search reviews (try: crap, worst, refund, broken, pissed)";
const PLACEHOLDER_UNHINGED = "Search reviews (try: bitch, shit, fuck, cunt, whore, refund)";

function applyUnhingedState(on) {
  document.body.classList.toggle("unhinged", on);
  const q = el("q");
  if (q) q.placeholder = on ? PLACEHOLDER_UNHINGED : PLACEHOLDER_TAME;
  if (DATA.wall) renderWall();
  if (typeof window.__rerunSearch === "function") window.__rerunSearch();
}

function setUnhinged(on) {
  const t = el("unhingedToggle");
  if (t) t.checked = !!on;
  applyUnhingedState(!!on);
  localStorage.setItem("unhinged", on ? "1" : "0");
}

function wireUnhingedToggle() {
  const t = el("unhingedToggle");
  const stored = localStorage.getItem("unhinged") === "1";
  t.checked = stored;
  applyUnhingedState(stored);
  t.onchange = () => setUnhinged(t.checked);

  document.querySelectorAll("[data-mode]").forEach((a) => {
    a.addEventListener("click", (e) => {
      const mode = a.dataset.mode;
      if (mode === "unhinged") setUnhinged(true);
      else if (mode === "tame") setUnhinged(false);
      const wall = document.getElementById("wall");
      if (wall) {
        e.preventDefault();
        wall.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
  });
}

init();
