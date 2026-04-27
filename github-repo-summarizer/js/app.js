// 1M READMEs — vanilla JS
const esc = (s) => String(s == null ? "" : s)
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
const fmt = (n) => (n == null ? "—" : Number(n).toLocaleString());
const repoUrl = (r) => `https://github.com/${encodeURIComponent(r.split("/")[0])}/${encodeURIComponent(r.split("/")[1] || "")}`;

// Fetch JSON with fallback & basic telemetry
async function fetchJson(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}

(async () => {
  let index, cats, findings, search;
  try {
    [index, cats, findings, search] = await Promise.all([
      fetchJson("data/index.json"),
      fetchJson("data/categories.json"),
      fetchJson("data/findings.json"),
      fetchJson("data/search.json"),
    ]);
  } catch (e) {
    console.error(e);
    document.querySelector(".hero .lede").innerHTML =
      `<b style="color:#cf222e">Data not loaded yet.</b> Run <code>python analysis.py</code> first.`;
    return;
  }

  renderHero(index);
  renderLandscape(index);
  renderCategoryGrid(cats);
  renderFindings(findings);
  wireCategoryModal(cats);
  wireSearch(search);
})();

// ---- Hero stats ----
function renderHero(index) {
  const n = index.n_repos || 0;
  document.getElementById("stat-repos").textContent = fmt(n);
  document.getElementById("stat-cats").textContent = index.n_categories || "14";
  document.getElementById("stat-langs").textContent = fmt(index.top_langs?.length || 0);
  document.getElementById("hero-nrepos").textContent = fmt(n);
}

// ---- Landscape bars ----
function renderLandscape(index) {
  const rows = index.top_cats || [];
  const max = Math.max(1, ...rows.map((r) => r.n));
  const html = rows
    .map((r) => {
      const pct = (100 * r.n) / max;
      return `
      <div class="bar-row">
        <div class="bar-label">
          <span class="bar-emoji">${esc(r.emoji)}</span>
          <span>${esc(r.name)}</span>
        </div>
        <div class="bar-rail"><div class="bar-fill" style="width:${pct}%"></div></div>
        <div class="bar-value">${fmt(r.n)}<span class="bar-pct">${r.pct}%</span></div>
      </div>`;
    })
    .join("");
  document.getElementById("landscape-bars").innerHTML = html;
}

// ---- Category grid ----
function renderCategoryGrid(cats) {
  const html = cats
    .filter((c) => c.cat !== "other" || c.n > 0)
    .map((c) => {
      const pct = c.n ? ((100 * c.n) / cats.reduce((s, x) => s + x.n, 0)).toFixed(1) : 0;
      return `
      <button class="category-card" data-cat="${esc(c.cat)}">
        <div class="cc-emoji">${esc(c.emoji)}</div>
        <div class="cc-name">${esc(c.name)}</div>
        <div class="cc-n">${fmt(c.n)} repos<span class="cc-pct">${pct}%</span></div>
      </button>`;
    })
    .join("");
  document.getElementById("category-grid").innerHTML = html;
}

// ---- Findings ----
function renderFindings(findings) {
  const host = document.getElementById("findings-list");
  const html = findings.map((f) => renderFinding(f)).join("");
  host.innerHTML = html;
}

function renderFinding(f) {
  return `
    <article class="finding" id="f-${esc(f.id)}">
      <h3>${esc(f.title)}</h3>
      <p class="blurb">${esc(f.blurb)}</p>
      ${renderFindingBody(f)}
    </article>
  `;
}

function renderFindingBody(f) {
  switch (f.type) {
    case "bar": return renderBarFinding(f);
    case "lang_dominance": return renderLangDominance(f);
    case "install_by_cat": return renderInstallByCat(f);
    case "repo_list": return renderRepoList(f);
    case "distinctive_words": return renderDistinctiveWords(f);
    case "install_dominance": return renderInstallDominance(f);
    default: return "";
  }
}

function renderBarFinding(f) {
  const max = Math.max(1, ...f.rows.map((r) => r.n));
  const rows = f.rows
    .map((r) => {
      const pct = (100 * r.n) / max;
      return `
      <div class="bar-row">
        <div class="bar-label">
          <span class="bar-emoji">${esc(r.emoji || "")}</span>
          <span>${esc(r.name)}</span>
        </div>
        <div class="bar-rail"><div class="bar-fill" style="width:${pct}%"></div></div>
        <div class="bar-value">${fmt(r.n)}<span class="bar-pct">${r.pct}%</span></div>
      </div>`;
    })
    .join("");
  return `<div class="bars">${rows}</div>`;
}

function renderLangDominance(f) {
  const rows = f.rows
    .map(
      (r) => `
    <div class="lang-row">
      <div class="lang-row-cat">${esc(r.emoji || "")} ${esc(r.name)}</div>
      <div class="lang-row-langs">
        ${r.top_langs.map((l) => `<span class="lang-chip"><b>${esc(l.lang || "?")}</b><span class="pct">${l.pct}%</span></span>`).join("")}
      </div>
    </div>`
    )
    .join("");
  return `<div class="lang-rows">${rows}</div>`;
}

function renderInstallByCat(f) {
  const rows = f.rows
    .map(
      (r) => `
    <div class="lang-row">
      <div class="lang-row-cat">${esc(r.emoji || "")} ${esc(r.name)}</div>
      <div class="lang-row-langs">
        ${r.installs.map((i) => `<span class="lang-chip"><b>${esc(i.label || i.install)}</b><span class="pct">${i.pct}%</span></span>`).join("")}
      </div>
    </div>`
    )
    .join("");
  return `<div class="install-rows">${rows}</div>`;
}

function renderInstallDominance(f) {
  const rows = f.rows
    .map(
      (r) => `
    <div class="install-dom-row">
      <div class="lang-row-cat">${esc(r.emoji || "")} ${esc(r.name)}</div>
      <div class="install-dom-rail"><div class="install-dom-fill" style="width:${r.pct}%"></div></div>
      <div class="install-dom-label"><b>${esc(r.install_label)}</b> ${r.pct}% of ${fmt(r.n)}</div>
    </div>`
    )
    .join("");
  return `<div class="install-dom">${rows}</div>`;
}

function renderDistinctiveWords(f) {
  const rows = f.rows
    .map(
      (r) => `
    <div class="dw-row">
      <div class="dw-row-cat">${esc(r.emoji || "")} ${esc(r.name)}</div>
      <div class="dw-words">
        ${r.words
          .slice(0, 12)
          .map((w) => `<span class="dw-word">${esc(w.word)}<span class="n">${fmt(w.n)}</span></span>`)
          .join("")}
      </div>
    </div>`
    )
    .join("");
  return `<div class="dw-rows">${rows}</div>`;
}

function renderRepoList(f) {
  const cards = f.rows.map((r) => repoCard(r)).join("");
  return `<div class="repo-grid">${cards}</div>`;
}

function repoCard(r) {
  const url = repoUrl(r.repo || "");
  const langChip = r.lang ? `<span class="chip lang">${esc(r.lang)}</span>` : "";
  const catLabel = (r.cat || r.category || "").replace(/_/g, " ");
  return `
    <a class="repo-card" href="${esc(url)}" target="_blank" rel="noopener noreferrer">
      <div class="repo-head">
        <span class="repo-name">${esc(r.repo || r.name || "?")}</span>
      </div>
      <div class="repo-meta">
        ${langChip}
        ${catLabel ? `<span class="chip cat">${esc(catLabel)}</span>` : ""}
        ${r.install && r.install !== "none" ? `<span class="chip install">${esc(r.install)}</span>` : ""}
        ${r.chars ? `<span class="chip">${fmt(r.chars)} chars</span>` : ""}
      </div>
      ${r.tldr ? `<div class="repo-tldr">${esc(r.tldr)}</div>` : ""}
    </a>`;
}

// ---- Category modal ----
function wireCategoryModal(cats) {
  const modal = document.getElementById("cat-modal");
  const title = document.getElementById("cat-modal-title");
  const subtitle = document.getElementById("cat-modal-subtitle");
  const grid = document.getElementById("cat-modal-repos");
  const closeBtn = modal.querySelector(".cat-modal-close");

  const catMap = new Map(cats.map((c) => [c.cat, c]));

  document.getElementById("category-grid").addEventListener("click", (e) => {
    const btn = e.target.closest(".category-card");
    if (!btn) return;
    const cat = btn.dataset.cat;
    const c = catMap.get(cat);
    if (!c) return;
    title.textContent = `${c.emoji || ""} ${c.name}`;
    subtitle.textContent = `${fmt(c.n)} repos in this category. Showing the top ${c.top.length}.`;
    grid.innerHTML = c.top.map((r) => repoCard({ ...r, cat: c.cat })).join("");
    modal.showModal();
  });

  closeBtn.addEventListener("click", () => modal.close());
  modal.addEventListener("click", (e) => {
    const rect = modal.querySelector(".cat-modal-inner").getBoundingClientRect();
    if (e.clientX < rect.left || e.clientX > rect.right || e.clientY < rect.top || e.clientY > rect.bottom) {
      modal.close();
    }
  });
}

// ---- Search ----
function wireSearch(search) {
  const input = document.getElementById("search");
  const countEl = document.getElementById("search-count");
  const resultsSection = document.getElementById("search-results");
  const resultsGrid = document.getElementById("search-grid");

  const doSearch = (q) => {
    q = q.trim().toLowerCase();
    if (!q) {
      resultsSection.hidden = true;
      countEl.textContent = "";
      return;
    }
    const terms = q.split(/\s+/).filter(Boolean);
    const hits = [];
    for (const r of search) {
      const hay = [r.repo, r.title, r.tldr, r.lang, r.cat].filter(Boolean).join(" ").toLowerCase();
      let match = true;
      for (const t of terms) {
        if (!hay.includes(t)) {
          match = false;
          break;
        }
      }
      if (match) hits.push(r);
      if (hits.length >= 60) break;
    }
    countEl.textContent = hits.length ? `${hits.length}${hits.length === 60 ? "+" : ""} matches` : "no matches";
    if (hits.length) {
      resultsSection.hidden = false;
      resultsGrid.innerHTML = hits.map((r) => repoCard(r)).join("");
      resultsSection.scrollIntoView({ behavior: "smooth", block: "start" });
    } else {
      resultsSection.hidden = true;
    }
  };

  let t;
  input.addEventListener("input", (e) => {
    clearTimeout(t);
    t = setTimeout(() => doSearch(e.target.value), 120);
  });
}
