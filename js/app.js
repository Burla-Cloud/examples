// Amazon Review Distiller — data loader + renderer.
// Pure vanilla, no frameworks. All data lives in frontend/data/*.json.

const DATA = {
  index: null,
  wall: null,
  vulgar: null,
  findings: null,
  categories: null,
  catPages: {},
  searchPool: null,
};

const el = (id) => document.getElementById(id);
const esc = (s) =>
  String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");

// Clean inline HTML from Amazon review text before escaping. We convert
// <br> variants to newlines, strip a handful of tags people literally type,
// and decode the most common HTML entities (&#34;, &amp;, etc).
const escReview = (s) => {
  let t = String(s == null ? "" : s);
  t = t.replace(/<br\s*\/?>/gi, "\n");
  t = t.replace(/<\/?(p|div|span|em|strong|i|b|u)[^>]*>/gi, "");
  // Decode common numeric + named entities users wrote literally.
  t = t
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(parseInt(n, 10)))
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
  return esc(t);
};
const fmt = (n) => (n == null ? "—" : n.toLocaleString());
const fmtShort = (n) => {
  if (n == null) return "—";
  if (n >= 1e9) return (n / 1e9).toFixed(2) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
};
const pct = (x, dp = 2) => (x == null ? "—" : (x * 100).toFixed(dp) + "%");
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

  // Hard-mode (truly vulgar) is a separate Burla run; load async so the
  // main site paints first. Visibility of the section is CSS-gated by
  // body.unhinged, so we don't toggle inline display here.
  loadJSON("data/vulgar.json")
    .then((d) => {
      DATA.vulgar = d;
      renderVulgar();
    })
    .catch(() => {
      // Fallback silently. If vulgar.json is missing, the locked banner
      // still tells users to enable Unhinged Mode, and the (empty) Hard
      // Mode section will just render its headline without reviews.
    });

  // Load the wider search index in the background so the search bar can
  // hit ~4,000 reviews across every category, not just the 120-item Wall.
  loadJSON("data/search.json")
    .then((rows) => {
      DATA.searchPool = rows;
    })
    .catch(() => {
      // Fallback silently — search will still work against the Wall.
    });
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

function tagList(r, showCat = true) {
  const tags = [];
  const cat = DATA.categories.find((c) => c.cat === (r._category || r.category));
  if (showCat && cat) tags.push(`<span class="cat">${esc(cat.emoji)} ${esc(cat.name)}</span>`);
  return tags.join("");
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

function reviewCard(r, rank) {
  const rating = Number(r.rating || 0);
  const starMarkup = Array.from({ length: 5 })
    .map((_, i) => (i < Math.round(rating) ? "★" : '<span class="ghost">★</span>'))
    .join("");
  const title = escReview(r.title || "(no title)");
  const body = escReview(r.text || "(no body)");
  return `
    <article class="rev">
      ${rank != null ? `<span class="rank">#${rank}</span>` : ""}
      ${tagList(r)}
      <div class="stars">${starMarkup}</div>
      <div class="title">${title}</div>
      <div class="body">${body}</div>
      <button class="more" data-more>Read full review →</button>
      <div class="meta">
        <span class="tags">${metaTags(r)}</span>
        <span class="asin">ASIN ${esc(r.asin || "—")}</span>
      </div>
    </article>
  `;
}

function attachMoreHandlers(scope) {
  scope.querySelectorAll("[data-more]").forEach((btn) => {
    btn.onclick = () => {
      const card = btn.closest(".rev");
      card.classList.toggle("expanded");
      btn.textContent = card.classList.contains("expanded") ? "Collapse" : "Read full review →";
    };
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
        <span>ASIN ${esc(r.asin || "—")}</span>
      </div>
    </div>
  `;
}

// --- wall of fucked up ------------------------------------------------

function renderWall() {
  const w = DATA.wall;
  el("wallBlurb").textContent = w.blurb;
  const wrap = el("wallList");
  wrap.innerHTML = w.rows
    .map((r, i) => reviewCard(r, i + 1))
    .join("");
  attachMoreHandlers(wrap);
}

// --- hard mode (truly vulgar) -----------------------------------------

function renderVulgar() {
  const v = DATA.vulgar;
  const blurbEl = el("vulgarBlurb");
  const wrap = el("vulgarList");
  if (!v || !v.rows || !wrap) return;
  if (blurbEl) blurbEl.textContent = v.blurb || "";
  wrap.innerHTML = v.rows
    .map((r, i) => reviewCard(r, i + 1))
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
            <div class="review-feed" style="grid-template-columns: 1fr 1fr">
              ${rows.map((r, i) => reviewCard({ ...r, _category: cat }, i + 1)).join("")}
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
      // default: review list
      bodyHtml = f.rows.slice(0, 5).map((r, i) => miniRev(r, i + 1)).join("");
    }
    card.innerHTML = header + bodyHtml;
    wrap.appendChild(card);
  }
}

// --- search -----------------------------------------------------------

// A row is a "hard-mode" vulgar entry if it was produced by hunt_vulgar.py.
// Those rows always carry a non-empty `score.roots` dict. Regular wall /
// search.json entries have a different score shape (strong_profane, etc.).
function isVulgarRow(r) {
  const s = r && r.score;
  return !!(s && typeof s === "object" && s.roots && Object.keys(s.roots).length);
}

function unhingedOn() {
  return document.body.classList.contains("unhinged");
}

function searchCorpus() {
  // Merge Wall + (Hard Mode if unhinged) + wider search index, dedupe by
  // (asin|title|text slice). When unhinged is off, vulgar-only rows are
  // filtered out so searches for hard words fall back to the milder pool.
  const includeVulgar = unhingedOn();
  const seen = new Set();
  const out = [];
  const push = (r) => {
    const key = `${r.asin || ""}|${(r.title || "").slice(0, 40)}|${(r.text || "").slice(0, 60)}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push(r);
  };
  for (const r of DATA.wall?.rows || []) push(r);
  if (includeVulgar) {
    for (const r of DATA.vulgar?.rows || []) push(r);
  }
  for (const r of DATA.searchPool || []) {
    if (!includeVulgar && isVulgarRow(r)) continue;
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
  const originalBlurb = (DATA.wall && DATA.wall.blurb) || blurb.textContent;

  let debounce = null;

  const run = ({ scroll = false } = {}) => {
    const needle = (q.value || "").toLowerCase().trim();
    const catWanted = sel.value;

    if (!needle && !catWanted) {
      renderWall();
      blurb.textContent = originalBlurb;
      return;
    }

    const pool = searchCorpus();
    const filtered = pool.filter((r) => {
      if (catWanted && r._category !== catWanted) return false;
      if (needle) {
        const haystack = ((r.title || "") + " " + (r.text || "")).toLowerCase();
        if (!haystack.includes(needle)) return false;
      }
      return true;
    });

    // Cap to keep the DOM snappy.
    const shown = filtered.slice(0, 200);

    if (shown.length) {
      wrap.innerHTML = shown.map((r, i) => reviewCard(r, i + 1)).join("");
      attachMoreHandlers(wrap);
    } else {
      wrap.innerHTML = `
        <div style="grid-column:1/-1;padding:36px 24px;text-align:center;background:#fff;border:1px dashed #ccc;border-radius:8px;color:#555">
          <div style="font-size:22px;font-weight:700;margin-bottom:6px">No matches for "${esc(needle || catWanted)}"</div>
          <div style="margin-bottom:14px">Amazon masks a lot of the classic four-letter words. Try one of these instead:</div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center">
            ${["crap", "worst", "refund", "broken", "garbage", "pissed", "terrible", "damn"]
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

  // Live search while typing, 140ms debounced.
  q.addEventListener("input", () => {
    clearTimeout(debounce);
    debounce = setTimeout(() => run({ scroll: false }), 140);
  });
  // Enter / button / category change: run and jump to results.
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

  // Expose a no-scroll rerun so the Unhinged toggle can refresh the corpus.
  window.__rerunSearch = () => run({ scroll: false });
}

// --- unhinged toggle --------------------------------------------------

const PLACEHOLDER_TAME = "Search reviews (try: crap, worst, refund, broken, pissed)";
const PLACEHOLDER_UNHINGED = "Search reviews (try: bitch, whore, shit, pissed, refund)";

function applyUnhingedState(on) {
  document.body.classList.toggle("unhinged", on);
  const q = el("q");
  if (q) q.placeholder = on ? PLACEHOLDER_UNHINGED : PLACEHOLDER_TAME;
  // Re-run any active search so the corpus change (with/without vulgar) takes
  // effect immediately.
  if (typeof window.__rerunSearch === "function") window.__rerunSearch();
}

function wireUnhingedToggle() {
  const t = el("unhingedToggle");
  const stored = localStorage.getItem("unhinged") === "1";
  t.checked = stored;
  applyUnhingedState(stored);
  t.onchange = () => {
    applyUnhingedState(t.checked);
    localStorage.setItem("unhinged", t.checked ? "1" : "0");
  };
}

init();
