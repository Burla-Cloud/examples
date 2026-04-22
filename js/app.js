// Amazon Review Distiller — data loader + renderer.
// Pure vanilla, no frameworks. All data lives in frontend/data/*.json.

const DATA = {
  index: null,
  wall: null,
  findings: null,
  categories: null,
  catPages: {},
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

function wireSearch() {
  const q = el("q");
  const sel = el("catFilter");
  const btn = el("searchBtn");

  const run = () => {
    const needle = (q.value || "").toLowerCase().trim();
    const catWanted = sel.value;
    if (!needle && !catWanted) {
      renderWall();
      return;
    }

    // Search across wall (already the juiciest 120). For broader scope,
    // we could lazily load all category pages, but this covers 80% of intent.
    const filtered = DATA.wall.rows.filter((r) => {
      if (catWanted && r._category !== catWanted) return false;
      if (needle) {
        const haystack = ((r.title || "") + " " + (r.text || "")).toLowerCase();
        if (!haystack.includes(needle)) return false;
      }
      return true;
    });

    const wrap = el("wallList");
    wrap.innerHTML = filtered.length
      ? filtered.map((r, i) => reviewCard(r, i + 1)).join("")
      : `<p style="padding:24px;color:#666">Nothing matched. Try a filthier search.</p>`;
    attachMoreHandlers(wrap);
    el("wallBlurb").textContent = `${filtered.length} matching reviews on the Wall of Fucked Up${
      catWanted ? " (filtered to " + catWanted.replace(/_/g, " ") + ")" : ""
    }.`;
  };

  btn.onclick = run;
  q.onkeydown = (e) => e.key === "Enter" && run();
  sel.onchange = run;
}

// --- unhinged toggle --------------------------------------------------

function wireUnhingedToggle() {
  const t = el("unhingedToggle");
  const stored = localStorage.getItem("unhinged") === "1";
  t.checked = stored;
  document.body.classList.toggle("unhinged", stored);
  t.onchange = () => {
    document.body.classList.toggle("unhinged", t.checked);
    localStorage.setItem("unhinged", t.checked ? "1" : "0");
  };
}

init();
