/* World Photo Index — front end */
(async function () {
  "use strict";

  const DATA = {
    index: null,
    world: null,
    findings: null,
    topojson: null,
    isoMap: null,
  };

  const mapEl = document.getElementById("worldmap");
  const panelEl = document.getElementById("countrypanel");
  const panelBody = document.getElementById("countrybody");
  const closeBtn = document.getElementById("closepanel");
  closeBtn.addEventListener("click", () => panelEl.setAttribute("aria-hidden", "true"));
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") panelEl.setAttribute("aria-hidden", "true");
  });

  // --- Load all data in parallel -----------------------------------------
  const TOPO_URL =
    "https://cdn.jsdelivr.net/npm/world-atlas@2/countries-50m.json";

  async function j(url) {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`fetch ${url}: ${r.status}`);
    return await r.json();
  }

  try {
    const [idx, world, findings, topo] = await Promise.all([
      j("data/index.json"),
      j("data/world.json"),
      j("data/findings.json"),
      j(TOPO_URL),
    ]);
    DATA.index = idx;
    DATA.world = world;
    DATA.findings = findings;
    DATA.topojson = topo;
  } catch (err) {
    console.error(err);
    document.body.insertAdjacentHTML(
      "afterbegin",
      `<div style="padding:40px;color:#e05a4e;font-family:monospace">Failed to load data: ${err.message}</div>`
    );
    return;
  }

  // --- ISO alpha-2 lookup (world-atlas uses ISO numeric) -----------------
  // Small inline map: ISO_N3 -> ISO_A2 for ~250 countries.
  const ISO_N_TO_A = {
    4: "AF", 8: "AL", 10: "AQ", 12: "DZ", 16: "AS", 20: "AD", 24: "AO", 28: "AG", 31: "AZ",
    32: "AR", 36: "AU", 40: "AT", 44: "BS", 48: "BH", 50: "BD", 51: "AM", 52: "BB", 56: "BE",
    60: "BM", 64: "BT", 68: "BO", 70: "BA", 72: "BW", 74: "BV", 76: "BR", 84: "BZ", 86: "IO",
    90: "SB", 92: "VG", 96: "BN", 100: "BG", 104: "MM", 108: "BI", 112: "BY", 116: "KH",
    120: "CM", 124: "CA", 132: "CV", 136: "KY", 140: "CF", 144: "LK", 148: "TD", 152: "CL",
    156: "CN", 158: "TW", 162: "CX", 166: "CC", 170: "CO", 174: "KM", 175: "YT", 178: "CG",
    180: "CD", 184: "CK", 188: "CR", 191: "HR", 192: "CU", 196: "CY", 203: "CZ", 204: "BJ",
    208: "DK", 212: "DM", 214: "DO", 218: "EC", 222: "SV", 226: "GQ", 231: "ET", 232: "ER",
    233: "EE", 234: "FO", 238: "FK", 242: "FJ", 246: "FI", 248: "AX", 250: "FR", 254: "GF",
    258: "PF", 260: "TF", 262: "DJ", 266: "GA", 268: "GE", 270: "GM", 275: "PS", 276: "DE",
    288: "GH", 292: "GI", 296: "KI", 300: "GR", 304: "GL", 308: "GD", 312: "GP", 316: "GU",
    320: "GT", 324: "GN", 328: "GY", 332: "HT", 334: "HM", 336: "VA", 340: "HN", 344: "HK",
    348: "HU", 352: "IS", 356: "IN", 360: "ID", 364: "IR", 368: "IQ", 372: "IE", 376: "IL",
    380: "IT", 384: "CI", 388: "JM", 392: "JP", 398: "KZ", 400: "JO", 404: "KE", 408: "KP",
    410: "KR", 414: "KW", 417: "KG", 418: "LA", 422: "LB", 426: "LS", 428: "LV", 430: "LR",
    434: "LY", 438: "LI", 440: "LT", 442: "LU", 446: "MO", 450: "MG", 454: "MW", 458: "MY",
    462: "MV", 466: "ML", 470: "MT", 474: "MQ", 478: "MR", 480: "MU", 484: "MX", 492: "MC",
    496: "MN", 498: "MD", 499: "ME", 500: "MS", 504: "MA", 508: "MZ", 512: "OM", 516: "NA",
    520: "NR", 524: "NP", 528: "NL", 531: "CW", 533: "AW", 534: "SX", 535: "BQ", 540: "NC",
    548: "VU", 554: "NZ", 558: "NI", 562: "NE", 566: "NG", 570: "NU", 574: "NF", 578: "NO",
    580: "MP", 581: "UM", 583: "FM", 584: "MH", 585: "PW", 586: "PK", 591: "PA", 598: "PG",
    600: "PY", 604: "PE", 608: "PH", 612: "PN", 616: "PL", 620: "PT", 624: "GW", 626: "TL",
    630: "PR", 634: "QA", 638: "RE", 642: "RO", 643: "RU", 646: "RW", 652: "BL", 654: "SH",
    659: "KN", 660: "AI", 662: "LC", 663: "MF", 666: "PM", 670: "VC", 674: "SM", 678: "ST",
    682: "SA", 686: "SN", 688: "RS", 690: "SC", 694: "SL", 702: "SG", 703: "SK", 704: "VN",
    705: "SI", 706: "SO", 710: "ZA", 716: "ZW", 724: "ES", 728: "SS", 729: "SD", 732: "EH",
    740: "SR", 744: "SJ", 748: "SZ", 752: "SE", 756: "CH", 760: "SY", 762: "TJ", 764: "TH",
    768: "TG", 772: "TK", 776: "TO", 780: "TT", 784: "AE", 788: "TN", 792: "TR", 795: "TM",
    796: "TC", 798: "TV", 800: "UG", 804: "UA", 807: "MK", 818: "EG", 826: "GB", 831: "GG",
    832: "JE", 833: "IM", 834: "TZ", 840: "US", 850: "VI", 854: "BF", 858: "UY", 860: "UZ",
    862: "VE", 876: "WF", 882: "WS", 887: "YE", 894: "ZM",
  };
  DATA.isoMap = ISO_N_TO_A;

  // --- Populate headliner "big cards" ------------------------------------
  const sigFinding = DATA.findings.find((f) => f.id === "signature_per_country");
  const monFinding = DATA.findings.find((f) => f.id === "national_monopolies");
  const thmFinding = DATA.findings.find((f) => f.id === "country_obsessions");
  const bigcards = document.getElementById("headliners");

  // Pull three "wow" rows curated from the findings.
  const findRow = (rows, matcher) => rows.find(matcher);
  const sigFR = findRow(sigFinding.rows, r => r.cc === "FR");
  const sigJP = findRow(sigFinding.rows, r => r.cc === "JP");
  const thmSG = findRow(thmFinding.rows, r => r.cc === "SG");
  const thmIS = findRow(thmFinding.rows, r => r.cc === "IS");
  const monJO = findRow(monFinding.rows, r => r.phrase === "ancient history" || r.phrase === "aerial archaeology");

  const bc = (label, title, body) => `
    <div class="bigcard">
      <div class="bc-label">${label}</div>
      <div class="bc-title">${title}</div>
      <div class="bc-body">${body}</div>
    </div>`;

  // If any big-card source is missing, gracefully fall back to generic rows.
  const safe = (v, fallback) => v ? v : fallback;
  const safeRow = (arr) => arr && arr[0] ? arr[0] : { name: "?", primary: "?", flag: "", cc: "??" };
  const sig0 = safe(sigFR, safeRow(sigFinding.rows));
  const thm0 = safe(thmSG, safeRow(thmFinding.rows));
  const thm1 = safe(thmIS, thmFinding.rows[1] || safeRow(thmFinding.rows));

  bigcards.innerHTML = [
    bc(
      "HEADLINER 01",
      `In ${sig0.flag}&nbsp;${sig0.name}, cameras keep finding <em>${sig0.primary}</em>.`,
      `${sig0.primary_count.toLocaleString()} photos tagged that way — more than any other non-place phrase in the country.`
    ),
    bc(
      "HEADLINER 02",
      `${thm0.flag}&nbsp;${thm0.name} is <em>${(thm0.share * 100).toFixed(0)}%</em> ${thm0.theme}.`,
      `Of ${thm0.photos.toLocaleString()} public photos from ${thm0.name}, nearly ${(thm0.share * 100).toFixed(0)}% carry our ${thm0.theme} vocabulary — the highest obsession we measured.`
    ),
    bc(
      "HEADLINER 03",
      `${thm1.flag}&nbsp;${thm1.name} photographs <em>${thm1.theme}</em> more than any other theme.`,
      `Out of 16,937 public photos from ${thm1.name}, ${(thm1.share * 100).toFixed(1)}% carry ${thm1.theme} tags — glaciers, waterfalls, ice.`
    ),
  ].join("");

  // --- Render world map --------------------------------------------------
  renderMap();
  renderFindings();

  function renderMap() {
    const svg = d3.select("#worldmap");
    const width = 1600;
    const height = 820;
    svg.attr("viewBox", `0 0 ${width} ${height}`);
    const defs = svg.append("defs");
    const grad = defs.append("radialGradient")
      .attr("id", "oceanGrad")
      .attr("cx", "50%")
      .attr("cy", "40%")
      .attr("r", "70%");
    grad.append("stop").attr("offset", "0%").attr("stop-color", "#0e111a");
    grad.append("stop").attr("offset", "100%").attr("stop-color", "#050710");

    const projection = d3.geoNaturalEarth1()
      .scale(width / 5.5)
      .translate([width / 2, height / 2 + 10]);
    const path = d3.geoPath(projection);

    const topo = DATA.topojson;
    const countries = topojson.feature(topo, topo.objects.countries).features;
    const borders = topojson.mesh(topo, topo.objects.countries, (a, b) => a !== b);

    // Build CC -> data lookup
    const ccData = new Map();
    DATA.world.forEach((row) => ccData.set(row.cc, row));

    const photosArr = DATA.world.map((r) => r.photos).filter((n) => n > 0);
    const colorScale = d3.scaleSequentialLog()
      .domain([d3.min(photosArr), d3.max(photosArr)])
      .interpolator(d3.interpolateRgbBasis([
        "#11151d", "#311a05", "#7a4e1e", "#c8903b", "#f5d87a",
      ]));

    // Sphere background
    svg.append("path")
      .datum({ type: "Sphere" })
      .attr("class", "sphere")
      .attr("d", path);

    svg.append("path")
      .datum(d3.geoGraticule10())
      .attr("class", "graticule")
      .attr("d", path);

    // Tooltip
    const tooltip = d3.select(".map-wrap")
      .append("div")
      .attr("class", "map-tooltip");

    svg.append("g").selectAll("path.country")
      .data(countries)
      .join("path")
      .attr("class", "country")
      .attr("d", path)
      .attr("data-id", (d) => d.id)
      .attr("fill", (d) => {
        const cc = ISO_N_TO_A[+d.id];
        const row = cc ? ccData.get(cc) : null;
        if (!row) return "#15181f";
        return colorScale(row.photos);
      })
      .classed("has-data", (d) => {
        const cc = ISO_N_TO_A[+d.id];
        return !!(cc && ccData.get(cc));
      })
      .on("mouseenter", function (event, d) {
        const cc = ISO_N_TO_A[+d.id];
        const row = cc ? ccData.get(cc) : null;
        if (!row) {
          tooltip.classed("visible", false);
          return;
        }
        const wrap = document.querySelector(".map-wrap").getBoundingClientRect();
        tooltip
          .style("left", (event.clientX - wrap.left) + "px")
          .style("top", (event.clientY - wrap.top) + "px")
          .classed("visible", true)
          .html(
            `<div><span class="tt-flag">${row.flag || ""}</span><span class="tt-country">${row.name}</span></div>
             <div class="tt-thing">most photographed: <b>${row.primary || "?"}</b></div>
             <div class="tt-photos">${row.photos.toLocaleString()} photos</div>`
          );
      })
      .on("mousemove", function (event) {
        const wrap = document.querySelector(".map-wrap").getBoundingClientRect();
        tooltip
          .style("left", (event.clientX - wrap.left) + "px")
          .style("top", (event.clientY - wrap.top) + "px");
      })
      .on("mouseleave", () => tooltip.classed("visible", false))
      .on("click", (event, d) => {
        const cc = ISO_N_TO_A[+d.id];
        if (cc) openCountry(cc);
      });

    svg.append("path")
      .datum(borders)
      .attr("fill", "none")
      .attr("stroke", "#0a0d14")
      .attr("stroke-width", 0.5)
      .attr("d", path)
      .attr("pointer-events", "none");
  }

  // --- Country drawer ----------------------------------------------------
  async function openCountry(cc) {
    panelBody.innerHTML = `<div style="padding:40px;color:#9ea1a8">Loading ${cc}…</div>`;
    panelEl.setAttribute("aria-hidden", "false");
    panelEl.scrollTop = 0;
    try {
      const d = await j(`data/countries/${cc}.json`);
      renderCountry(d);
    } catch (err) {
      panelBody.innerHTML = `<div style="padding:40px;color:#e05a4e">No data for ${cc}. (${err.message})</div>`;
    }
  }
  window._openCountry = openCountry; // debug

  function renderCountry(d) {
    const flag = d.flag || "";
    const topDistinct = (d.top_distinctive || []).slice(0, 10);
    const topRaw = (d.top_raw || []).slice(0, 8);

    const phrasesHtml = topDistinct.map((r, i) => `
      <span class="cp-phrase${i === 0 ? " primary" : ""}">${r.phrase}<span class="count">${r.count.toLocaleString()}</span></span>
    `).join("");

    const rawHtml = topRaw.map((r) => `
      <span class="cp-phrase">${r.phrase}<span class="count">${r.count.toLocaleString()}</span></span>
    `).join("");

    const adminsHtml = (d.admins || []).slice(0, 8).map((a) => {
      const primary = a.top_phrases && a.top_phrases[0];
      return `<li>
        <span class="where">${a.admin1}<small>${a.total.toLocaleString()} photos</small></span>
        <span class="what">${primary ? `most photographed: <em>${primary.phrase}</em>` : "—"}</span>
      </li>`;
    }).join("");

    const citiesHtml = (d.cities || []).slice(0, 10).map((c) => {
      const primary = c.top_phrases && c.top_phrases[0];
      return `<li>
        <span class="where">${c.city}<small>${c.admin1} · ${c.total.toLocaleString()}</small></span>
        <span class="what">${primary ? `most photographed: <em>${primary.phrase}</em>` : "—"}</span>
      </li>`;
    }).join("");

    const galleryHtml = (d.samples || []).slice(0, 12).map((s) => {
      let raw = s.title || (s.top_phrases ? s.top_phrases.slice(0, 3).join(" · ") : "") || "";
      try { raw = decodeURIComponent(raw.replace(/\+/g, " ")); } catch (e) { /* fall through */ }
      const title = raw.slice(0, 60);
      const url = s.downloadurl || "";
      return `<figure>
        <a href="${url}" target="_blank" rel="noopener">
          <img src="${url}" alt="${escapeHtml(title)}" loading="lazy" onerror="this.style.display='none'"/>
        </a>
        <figcaption>${escapeHtml(title)}</figcaption>
      </figure>`;
    }).join("");

    panelBody.innerHTML = `
      <div class="cp-flag">${flag}</div>
      <h2 class="cp-country">${d.name}</h2>
      <span class="cp-iso">${d.cc}</span>
      <div class="cp-stat">
        <span><b>${d.photos.toLocaleString()}</b> public photos</span>
        <span><b>${(d.admins || []).length}</b> regions</span>
        <span><b>${(d.cities || []).length}</b> cities</span>
      </div>

      <div class="cp-h3">MOST PHOTOGRAPHED <em>THING</em> (TF-IDF)</div>
      <div class="cp-phrases">${phrasesHtml || "<em>no data</em>"}</div>

      <div class="cp-h3">MOST USED TAGS (ALL PHRASES)</div>
      <div class="cp-phrases">${rawHtml}</div>

      <div class="cp-h3">TOP REGIONS</div>
      <ul class="cp-list">${adminsHtml || "<em>no data</em>"}</ul>

      <div class="cp-h3">TOP CITIES</div>
      <ul class="cp-list">${citiesHtml}</ul>

      ${galleryHtml ? `
        <div class="cp-h3">SAMPLE PHOTOS</div>
        <div class="cp-gallery">${galleryHtml}</div>
      ` : ""}
    `;
  }

  // --- Findings section --------------------------------------------------
  function renderFindings() {
    const wrap = document.getElementById("findings-list");

    // Row renderers per finding id
    const renderers = {
      signature_per_country: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name} · <b>${r.primary}</b>
            <span class="sm">${r.runners.slice(0, 3).join(", ")}</span>
          </span>
          <span class="val">${r.primary_count.toLocaleString()} photos</span>
        </div>`,
      national_monopolies: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name} · <b>${r.phrase}</b>
            <span class="sm">${(r.share * 100).toFixed(0)}% of all global photos of this</span>
          </span>
          <span class="val">${r.total.toLocaleString()}</span>
        </div>`,
      city_monocultures: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.city}, ${r.admin1} · <b>${r.primary}</b>
            <span class="sm">${(r.share * 100).toFixed(0)}% of ${r.total.toLocaleString()} tags</span>
          </span>
          <span class="val">${r.primary_count.toLocaleString()}</span>
        </div>`,
      top_global_phrases: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><b>${r.phrase}</b></span>
          <span class="val">${r.count.toLocaleString()}</span>
        </div>`,
      regional_signatures: (r, i) => {
        const where = r.admin1 ? r.admin1 : `${r.name} (city-state)`;
        return `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${where} · <b>${r.primary}</b>
            <span class="sm">${r.runners.slice(0, 3).join(", ")}</span>
          </span>
          <span class="val">${r.total.toLocaleString()}</span>
        </div>`;
      },
      country_obsessions: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name} · <b>${r.theme}</b>
            <span class="sm">${(r.share * 100).toFixed(1)}% of ${r.photos.toLocaleString()} photos</span>
          </span>
          <span class="val">${(r.share * 100).toFixed(0)}%</span>
        </div>`,
      per_capita_photos: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name}
            <span class="sm">${r.photos.toLocaleString()} photos / ${(r.pop / 1e6).toFixed(1)}M people</span>
          </span>
          <span class="val">${r.per_million.toLocaleString()} / M</span>
        </div>`,
      concept_monopolies: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name} · <b>${r.phrase}</b>
            <span class="sm">${(r.share * 100).toFixed(0)}% of all photos of this</span>
          </span>
          <span class="val">${r.total.toLocaleString()}</span>
        </div>`,
      capital_dominance: (r, i) => `
        <div class="frow">
          <span class="rnk">${String(i + 1).padStart(2, "0")}</span>
          <span class="label"><span class="flag">${r.flag}</span>${r.name} · <b>${r.top_city}</b>
            <span class="sm">${r.admin1} · ${r.country_photos.toLocaleString()} total photos</span>
          </span>
          <span class="val">${(r.share * 100).toFixed(0)}%</span>
        </div>`,
    };

    DATA.findings.forEach((f) => {
      const renderer = renderers[f.id];
      if (!renderer) return;
      const visible = f.rows.slice(0, 10);
      const title = escapeHtml(f.title).replace(/\bone\b/, "<em>one</em>");
      wrap.insertAdjacentHTML("beforeend", `
        <div class="finding">
          <h3>${title}</h3>
          <div class="blurb">${f.blurb}</div>
          <div class="finding-rows">
            ${visible.map((r, i) => renderer(r, i)).join("")}
          </div>
          <div class="more">+ ${(f.rows.length - visible.length).toLocaleString()} more rows in the dataset</div>
        </div>
      `);
    });
  }

  // --- tiny helpers ------------------------------------------------------
  function escapeHtml(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);
  }
})();
