/*
 * AQI Insight dashboard.
 * Reads the files written by the Spark backend (src/aqi_project.py):
 *   ../outputs/dashboard_data.json  – city and India statistics, model results
 *   ../outputs/daily_aqi_web.json   – every city-day's pollutants, recorded AQI and backend-calculated AQI
 * The Daily AQI page recalculates every day's AQI in the browser with the CPCB formula
 * and checks it against the backend's value.
 * The Live AQI page pulls the last 48 hours of modelled pollutant levels from Open-Meteo (CAMS)
 * and converts them to India's AQI with the same CPCB formula.
 */
(function () {
  "use strict";

  const DATA_URL = "../outputs/dashboard_data.json";
  const DAILY_URL = "../outputs/daily_aqi_web.json";
  const app = document.getElementById("app");
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const BUCKETS = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"];
  const BUCKET_COLORS = ["#1B7F3B", "#7CB342", "#F2C14E", "#EE8A2E", "#D2382F", "#7A2E8E"];
  const LIMITS = { "PM2.5": [60, "µg/m³"], PM10: [100, "µg/m³"], NO2: [80, "µg/m³"], SO2: [80, "µg/m³"], CO: [2, "mg/m³"], O3: [100, "µg/m³"] };
  const KEYS = ["PM25", "PM10", "NO2", "SO2", "CO", "O3", "NH3"];
  const NAMES = ["PM2.5", "PM10", "NO2", "SO2", "CO", "O3", "NH3"];
  const UNITS = ["µg/m³", "µg/m³", "µg/m³", "µg/m³", "mg/m³", "µg/m³", "µg/m³"];
  // Same CPCB breakpoints as src/aqi_project.py; used when daily_aqi_web.json is not loaded yet
  const BP_DEFAULT = { aqi: [0, 50, 100, 200, 300, 400, 500], conc: {
    PM25: [0, 30, 60, 90, 120, 250, 380], PM10: [0, 50, 100, 250, 350, 430, 510], NO2: [0, 40, 80, 180, 280, 400, 520],
    SO2: [0, 40, 80, 380, 800, 1600, 2400], CO: [0, 1, 2, 10, 17, 34, 51], O3: [0, 50, 100, 168, 208, 748, 1288],
    NH3: [0, 200, 400, 800, 1200, 1800, 2400] } };
  const COORDS = {
    Ahmedabad: [23.0225, 72.5714], Aizawl: [23.7271, 92.7176], Amaravati: [16.5131, 80.5165], Amritsar: [31.634, 74.8723],
    Bengaluru: [12.9716, 77.5946], Bhopal: [23.2599, 77.4126], Brajrajnagar: [21.816, 83.9214], Chandigarh: [30.7333, 76.7794],
    Chennai: [13.0827, 80.2707], Coimbatore: [11.0168, 76.9558], Delhi: [28.6139, 77.209], Ernakulam: [9.9816, 76.2999],
    Gurugram: [28.4595, 77.0266], Guwahati: [26.1445, 91.7362], Hyderabad: [17.385, 78.4867], Jaipur: [26.9124, 75.7873],
    Jorapokhar: [23.7, 86.41], Kochi: [9.9312, 76.2673], Kolkata: [22.5726, 88.3639], Lucknow: [26.8467, 80.9462],
    Mumbai: [19.076, 72.8777], Patna: [25.5941, 85.1376], Shillong: [25.5788, 91.8933], Talcher: [20.95, 85.2333],
    Thiruvananthapuram: [8.5241, 76.9366], Visakhapatnam: [17.6868, 83.2185] };
  const LIVE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality";
  const LIVE_TTL = 10 * 60 * 1000;   // refetch at most every 10 minutes

  let DATA = null;      // dashboard_data.json
  let DAILY = null;     // daily_aqi_web.json
  let IDX = null;       // city -> date -> row
  let VERIFY = null;    // backend vs browser check over all days
  let LIVE = null, LIVE_AT = 0, LIVE_PENDING = null;   // city -> live result
  let liveCity = "Mumbai";
  const daily = { mode: "all", date: null, city: "Mumbai", year: null, view: "calc", sel: null,
                  calc: { PM25: "180", PM10: "300", NO2: "60", SO2: "15", CO: "1.5", O3: "40", NH3: "" } };

  // ---------- helpers ----------
  const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const fmt = (n, d = 1) => (n === null || n === undefined ? "–" : Number(n).toFixed(d));
  const intIN = (n) => Number(n).toLocaleString("en-IN");
  function cat(v) {
    if (v === null || v === undefined) return { name: "No AQI", bg: "#E4E7E1", fg: "#4F5D59", soft: "#EEF0EC", ink: "#4F5D59", advice: "" };
    if (v <= 50) return { name: "Good", bg: "#1B7F3B", fg: "#FFFFFF", soft: "#E4F2E7", ink: "#14602C", advice: "Minimal health impact. A good day for outdoor activity." };
    if (v <= 100) return { name: "Satisfactory", bg: "#7CB342", fg: "#142220", soft: "#EEF6E3", ink: "#3F6B12", advice: "Minor breathing discomfort for sensitive people, such as those with asthma." };
    if (v <= 200) return { name: "Moderate", bg: "#F2C14E", fg: "#142220", soft: "#FCF3DC", ink: "#7A5A00", advice: "Breathing discomfort for people with lung disease such as asthma, and discomfort for people with heart disease, children and older adults." };
    if (v <= 300) return { name: "Poor", bg: "#EE8A2E", fg: "#142220", soft: "#FBEBDD", ink: "#8A4300", advice: "Breathing discomfort for most people on prolonged exposure. Limit long outdoor exertion." };
    if (v <= 400) return { name: "Very Poor", bg: "#D2382F", fg: "#FFFFFF", soft: "#FBE7E5", ink: "#8E1F18", advice: "Respiratory illness on prolonged exposure. Avoid outdoor exercise; consider masks and air purifiers." };
    return { name: "Severe", bg: "#7A2E8E", fg: "#FFFFFF", soft: "#F1E4F4", ink: "#5E1F6E", advice: "Affects healthy people and seriously impacts those with existing diseases. Stay indoors where possible." };
  }
  const chip = (v) => { const c = cat(v); return `<span class="chip" style="background:${c.bg};color:${c.fg}">${c.name}</span>`; };
  const fmtDate = (s) => `${Number(s.slice(8, 10))} ${MONTHS[Number(s.slice(5, 7)) - 1]} ${s.slice(0, 4)}`;
  const pad = (n) => String(n).padStart(2, "0");
  const user = () => { try { return JSON.parse(sessionStorage.getItem("aqi_user") || "null"); } catch (e) { return null; } };
  const go = (h) => { location.hash = h; };

  async function loadJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(url);
    return r.json();
  }

  function missingData(file) {
    app.innerHTML = `<div class="error-box" style="margin:32px;max-width:720px">
      <h2 style="margin-bottom:8px">Results not found</h2>
      <p>The dashboard could not load <code>${esc(file)}</code>. Run the Spark backend first, then reload this page:</p>
      <p><code>python src/aqi_project.py</code></p>
      <p class="small">Start the dashboard with <code>python run_dashboard.py</code> from the project folder. Opening index.html directly from the file system does not work, because browsers block reading local files.</p>
    </div>`;
  }

  // ---------- CPCB AQI (same breakpoints as the Spark backend) ----------
  function subIndex(k, c) {
    if (c === null || c === undefined || c === "" || isNaN(c)) return null;
    const B = DAILY ? DAILY.breakpoints : BP_DEFAULT;
    const bp = B.conc[k], A = B.aqi;
    for (let i = 1; i < bp.length; i++) {
      if (c <= bp[i] || i === bp.length - 1) return A[i - 1] + (c - bp[i - 1]) * (A[i] - A[i - 1]) / (bp[i] - bp[i - 1]);
    }
    return null;
  }
  function aqiOf(vals) {
    const si = KEYS.map((k, j) => subIndex(k, vals[j]));
    const ok = si.filter((x) => x !== null);
    if (ok.length < 3 || (si[0] === null && si[1] === null)) return { aqi: null, si, dom: null };
    const aqi = Math.round(Math.max(...ok));
    const d = si.findIndex((x) => x !== null && Math.round(x) === aqi);
    return { aqi, si, dom: d >= 0 ? NAMES[d] : null };
  }

  // ---------- live AQI (Open-Meteo / CAMS -> CPCB formula) ----------
  const istNowHour = () => new Date(Date.now() + 330 * 60000).toISOString().slice(0, 13) + ":00";
  function meanOf(arr, a, b, need) {
    const v = arr.slice(a, b + 1).filter((x) => x !== null && x !== undefined);
    return v.length >= need ? v.reduce((t, x) => t + x, 0) / v.length : null;
  }
  // CPCB averaging: 24-hour mean for PM2.5, PM10, NO2, SO2 (16 of 24 hours needed),
  // highest 8-hour mean in the last 24 hours for CO and O3 (6 of 8 hours needed).
  function windowVals(h, end) {
    if (end < 23) return null;
    const a = end - 23;
    const avg24 = (k) => { const m = meanOf(h[k], a, end, 16); return m === null ? null : Math.round(m * 10) / 10; };
    const max8 = (k, scale, nd) => {
      let best = null;
      for (let s0 = a; s0 + 7 <= end; s0++) { const m = meanOf(h[k], s0, s0 + 7, 6); if (m !== null && (best === null || m > best)) best = m; }
      return best === null ? null : Math.round(best * scale * 10 ** nd) / 10 ** nd;
    };
    return [avg24("pm2_5"), avg24("pm10"), avg24("nitrogen_dioxide"), avg24("sulphur_dioxide"),
            max8("carbon_monoxide", 0.001, 2), max8("ozone", 1, 1), null];   // CO µg/m³ -> mg/m³; NH3 not modelled for India
  }
  function liveFromHourly(h) {
    const now = istNowHour();
    let end = -1;
    for (let i = 0; i < h.time.length; i++) if (h.time[i] <= now && h.pm2_5[i] !== null) end = i;
    const vals = windowVals(h, end);
    if (!vals) return null;
    const trend = [];
    for (let e = Math.max(23, end - 23); e <= end; e++) { const v = windowVals(h, e); trend.push({ t: h.time[e], aqi: v ? aqiOf(v).aqi : null }); }
    return { time: h.time[end], vals, res: aqiOf(vals), trend };
  }
  function fetchLive(force) {
    if (!force && LIVE && Date.now() - LIVE_AT < LIVE_TTL) return Promise.resolve(LIVE);
    if (LIVE_PENDING) return LIVE_PENDING;
    const names = Object.keys(COORDS);
    const q = new URLSearchParams({
      latitude: names.map((n) => COORDS[n][0]).join(","), longitude: names.map((n) => COORDS[n][1]).join(","),
      hourly: "pm2_5,pm10,nitrogen_dioxide,sulphur_dioxide,carbon_monoxide,ozone",
      past_days: "2", forecast_days: "1", timezone: "Asia/Kolkata" });
    LIVE_PENDING = fetch(`${LIVE_URL}?${q}`, { cache: "no-store" })
      .then((r) => { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); })
      .then((j) => {
        const arr = Array.isArray(j) ? j : [j];
        const out = {};
        names.forEach((n, i) => { out[n] = arr[i] && arr[i].hourly ? liveFromHourly(arr[i].hourly) : null; });
        LIVE = out; LIVE_AT = Date.now(); return LIVE;
      })
      .finally(() => { LIVE_PENDING = null; });
    return LIVE_PENDING;
  }
  const liveTime = (t) => `${fmtDate(t.slice(0, 10))}, ${t.slice(11, 16)} IST`;
  const liveNote = `<span class="small">Live values are modelled estimates from the CAMS air quality forecast (via <a href="https://open-meteo.com/" target="_blank" rel="noopener">Open-Meteo</a>), converted to India's AQI with the CPCB formula used in this project. They can differ from CPCB station readings.</span>`;

  // ---------- layout ----------
  function shell(active, inner) {
    const u = user() || { name: "Guest" };
    const initials = u.name.split(/\s+/).map((w) => w[0] || "").join("").slice(0, 2).toUpperCase() || "G";
    const cities = Object.keys(DATA.cities).sort();
    return `
      <header class="topbar">
        <div class="brand">${logo(30)}<span class="brand-name">AQI Insight</span><span class="pill">CPCB data, ${esc(DATA.overview.start)} – ${esc(DATA.overview.end)}</span></div>
        <label class="jump"><span class="hidden">Go to city</span><select id="jumpCity" aria-label="Go to city">
          <option value="">Go to city…</option>${cities.map((c) => `<option ${active === c ? "selected" : ""}>${esc(c)}</option>`).join("")}</select></label>
        <div class="user"><span class="muted">${esc(u.name)}</span><span class="avatar" aria-hidden="true">${esc(initials)}</span>
          <button class="btn btn-small" data-action="logout">Sign out</button></div>
      </header>
      <div class="shell">
        <nav class="side" aria-label="Dashboard">
          <a href="#/overview" class="strong ${active === "overview" ? "active" : ""}">India overview</a>
          <a href="#/live" class="strong ${active === "live" ? "active" : ""}"><span>Live AQI</span><span class="live-tag">LIVE</span></a>
          <a href="#/daily" class="strong ${active === "daily" ? "active" : ""}">Daily AQI calendar</a>
          <span class="group">Cities</span>
          ${cities.map((c) => `<a href="#/city/${encodeURIComponent(c)}" class="city-link ${active === c ? "active" : ""}"><span>${esc(c)}</span><span class="dot" style="background:${cat(DATA.cities[c].avg).bg}"></span></a>`).join("")}
        </nav>
        <main class="main" id="main">${inner}</main>
      </div>`;
  }
  function logo(size) {
    return `<svg width="${size}" height="${size}" viewBox="0 0 36 36" fill="none" aria-hidden="true"><circle cx="18" cy="18" r="16" stroke="#0B6A63" stroke-width="2"/><circle cx="18" cy="18" r="10" stroke="#E0A92A" stroke-width="2"/><circle cx="18" cy="18" r="4" fill="#EE8A2E"/></svg>`;
  }
  function kpi(label, value, sub, extra = "") {
    return `<div class="kpi ${extra}"><span class="muted">${label}</span><span class="value">${value}</span>${sub}</div>`;
  }
  function monthChart(values, title, subtitle) {
    const present = values.filter((v) => v !== null);
    const scale = Math.max(...present) > 300 ? 520 : 300;
    const maxV = Math.max(...present), minV = Math.min(...present);
    return `<section class="card"><div class="card-head"><h2>${title}</h2>
      <span class="muted">${subtitle ? subtitle + " · " : ""}worst month ${MONTHS[values.indexOf(maxV)]} (${maxV}), cleanest ${MONTHS[values.indexOf(minV)]} (${minV})</span></div>
      <div class="vchart" role="img" aria-label="${esc(title)}">${values.map((v) => `<div class="vcol"><small>${v === null ? "–" : v}</small><i style="height:${v === null ? 0 : Math.round(v / scale * 190)}px;background:${cat(v).bg}"></i></div>`).join("")}</div>
      <div class="vlabels">${MONTHS.map((m) => `<span>${m}</span>`).join("")}</div>
      ${legend()}</section>`;
  }
  function legend(extra = "") {
    return `<div class="legend">${BUCKETS.map((b, i) => `<span><i style="background:${BUCKET_COLORS[i]}"></i>${b}</span>`).join("")}${extra}</div>`;
  }
  function bucketBlock(pcts, title, subtitle) {
    const order = [0, 3, 1, 4, 2, 5];
    return `<section class="card"><div class="card-head"><h2>${title}</h2><span class="muted">${subtitle}</span></div>
      <div class="stack">${pcts.map((p, i) => `<div style="width:${p}%;background:${BUCKET_COLORS[i]}"></div>`).join("")}</div>
      <div class="legend-grid">${order.map((i) => `<div><i style="background:${BUCKET_COLORS[i]}"></i><span>${BUCKETS[i]}</span><b>${fmt(pcts[i])}%</b></div>`).join("")}</div>
      ${(pcts[3] + pcts[4] + pcts[5]) > 0 ? `<div class="note warm">${fmt(pcts[3] + pcts[4] + pcts[5])}% of days were Poor or worse.</div>` : ""}</section>`;
  }

  // ---------- views ----------
  function viewLogin() {
    const o = DATA.overview, best = DATA.model.metrics[0];
    app.innerHTML = `
      <div class="login">
        <section class="login-brand">
          <div class="brand">${logo(36)}<span class="brand-name" style="font-size:22px">AQI Insight</span></div>
          <div style="display:flex;flex-direction:column;gap:24px">
            <p class="kicker">Big Data Analysis mini project</p>
            <h1>See how India breathes, city by city.</h1>
            <p class="lead">Five years of CPCB air quality data, processed with Apache Spark, with an AQI prediction model built in Spark MLlib.</p>
            <div class="brand-stats">
              <div><b>${intIN(o.records)}</b><span>daily records</span></div>
              <div><b>${o.cities}</b><span>cities, 2015–2020</span></div>
              <div><b>${fmt(best.R2, 3)}</b><span>R² of AQI model</span></div>
            </div>
          </div>
          <div><span style="font-size:13px;color:#A9B8B3">India National AQI scale</span>
            <div class="scale" style="margin-top:10px">${BUCKET_COLORS.map((c, i) => `<div style="flex:${i < 2 ? 1 : 2};background:${c}"></div>`).join("")}</div>
            <div class="scale-labels">${["Good", "Satisf.", "Moderate", "Poor", "Very Poor", "Severe"].map((b, i) => `<span style="flex:${i < 2 ? 1 : 2}">${b}</span>`).join("")}</div>
          </div>
        </section>
        <section class="login-form-wrap">
          <form class="login-form" id="loginForm" novalidate>
            <div style="display:flex;flex-direction:column;gap:8px"><h2 style="font-size:34px">Sign in</h2>
              <p class="muted" style="margin:0;font-size:16px">Open the air quality dashboard.</p></div>
            <label class="field">Name<input id="name" autocomplete="name" placeholder="Your name"></label>
            <label class="field">Email<input id="email" type="email" autocomplete="email" placeholder="you@college.edu"></label>
            <label class="field">Password<input id="password" type="password" autocomplete="current-password" placeholder="At least 4 characters"></label>
            <p class="form-error hidden" id="loginError" role="alert"></p>
            <button class="btn btn-primary" type="submit">Sign in</button>
            <div class="or">or</div>
            <button class="btn" type="button" data-action="guest">Continue as guest</button>
            <p class="small" style="margin:0">Demo sign-in: details are kept only in this browser tab and are not sent anywhere.</p>
          </form>
        </section>
      </div>`;
    document.getElementById("loginForm").addEventListener("submit", (e) => {
      e.preventDefault();
      const name = document.getElementById("name").value.trim();
      const email = document.getElementById("email").value.trim();
      const pw = document.getElementById("password").value;
      const err = document.getElementById("loginError");
      let msg = "";
      if (!name) msg = "Enter your name.";
      else if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(email)) msg = "Enter a valid email address, like you@college.edu.";
      else if (pw.length < 4) msg = "Password must be at least 4 characters.";
      if (msg) { err.textContent = msg; err.classList.remove("hidden"); return; }
      sessionStorage.setItem("aqi_user", JSON.stringify({ name, email }));
      go("#/overview");
    });
  }

  function viewOverview() {
    const o = DATA.overview, lk = o.lockdown;
    const ranked = Object.entries(DATA.cities).sort((a, b) => b[1].avg - a[1].avg);
    const pct = (a, b) => Math.round((b - a) / a * 100);
    const corr = o.correlation.filter((c) => !["NO", "NOx"].includes(c[0])).slice(0, 5);
    const best = DATA.model.metrics[0];
    const inner = `
      <div class="page-head"><div style="display:flex;flex-direction:column;gap:6px"><h1>India overview</h1>
        <span class="muted" style="font-size:15px">${o.cities} cities, ${esc(o.start)} – ${esc(o.end)}. Pick a city on the left for its own page.</span></div></div>
      <div class="grid-kpi">
        ${kpi("National average AQI", fmt(o.avg_aqi), chip(o.avg_aqi))}
        ${kpi("Average PM2.5", `<span style="color:${o.avg_pm25 > 60 ? "#B42A22" : "inherit"}">${fmt(o.avg_pm25)}</span>`, `<span class="small">µg/m³ · limit 60</span>`)}
        ${kpi("Lockdown change", `<span style="color:#0B6A63">${pct(lk.aqi[0], lk.aqi[1])}%</span>`, `<span class="small">AQI, Mar–May 2020 vs 2019</span>`)}
        ${kpi("Records analysed", intIN(o.records), `<span class="small">${intIN(o.clean_records)} after cleaning</span>`)}
      </div>
      <section class="card"><div class="card-head"><h2>All ${o.cities} cities ranked</h2><span class="muted">Average AQI, most polluted first. Click a city for its page.</span></div>
        <div class="rank-grid">${ranked.map(([n, c], i) => `<a class="rank-row" href="#/city/${encodeURIComponent(n)}"><span style="color:#6B7874">${i + 1}</span>
          <span class="city-cell"><span style="font-weight:500">${esc(n)}</span><small>${esc(c.state)}</small></span>
          <span class="track"><span style="width:${Math.min(c.avg / 400 * 100, 100)}%;background:${cat(c.avg).bg}"></span></span>
          <span style="text-align:right;font-weight:600">${Math.round(c.avg)}</span></a>`).join("")}</div>
        <span class="small">Ahmedabad's very high AQI is driven by unusual CO readings, a likely sensor issue. Cities joined the monitoring network in different years.</span></section>
      <div class="grid-2">
        <section class="card"><div class="card-head"><h2>What drives AQI?</h2><span class="muted">Correlation of each pollutant with AQI</span></div>
          ${corr.map(([n, r]) => `<div class="hbar" style="grid-template-columns:70px minmax(0,1fr) 44px"><span style="font-weight:500">${n}</span><span class="track"><span style="width:${Math.max(r, 0) * 100}%;background:#0B6A63"></span></span><b style="text-align:right">${fmt(r, 2)}</b></div>`).join("")}</section>
        <section class="card"><div class="card-head"><h2>Pollution by region</h2><span class="muted">Average AQI of the cities in each region</span></div>
          ${o.regions.map((g) => `<div class="hbar" style="grid-template-columns:150px minmax(0,1fr) 40px"><span class="city-cell"><span style="font-weight:500">${g.name}</span><small>${g.cities} cities</small></span><span class="track"><span style="width:${Math.min(g.avg / 250 * 100, 100)}%;background:${cat(g.avg).bg}"></span></span><b style="text-align:right">${Math.round(g.avg)}</b></div>`).join("")}
          <span class="small">West is pulled up by Ahmedabad's unusual CO readings.</span></section>
      </div>
      <div class="grid-2">
        ${monthChart(o.monthly, "National AQI by month", "")}
        ${bucketBlock(o.buckets, "How often is the air unhealthy?", `Share of ${intIN(o.clean_records)} city-days in each AQI category`)}
      </div>
      <section class="card"><div class="card-head"><h2>COVID-19 lockdown</h2><span class="muted">${lk.cities} cities with data for both years · 25 March – 31 May, 2019 vs 2020</span></div>
        <div class="grid-3">
          <div class="trio">
            ${[["AQI", lk.aqi], ["PM2.5", lk.pm25], ["NO2", lk.no2]].map(([n, v]) => `<div style="background:#E3F0EE"><span class="small" style="color:#2C3A37">${n}</span><b style="color:#08504B">${pct(v[0], v[1])}%</b><span class="small" style="color:#2C3A37">${fmt(v[0])} → ${fmt(v[1])}</span></div>`).join("")}
          </div>
          <div style="display:flex;flex-direction:column;gap:8px;font-size:14px"><b>Biggest AQI drops</b>
            ${lk.biggest_drops.map(([c, p]) => `<div class="row-split"><a href="#/city/${encodeURIComponent(c)}">${esc(c)}</a><b>${Math.round(p)}%</b></div>`).join("")}</div>
        </div></section>
      <div class="grid-2">
        <section class="card"><div class="card-head"><h2>Model comparison</h2><span class="muted">Spark MLlib, tested on a 20% hold-out set</span></div>
          <div class="table-wrap"><table><thead><tr><th>Model</th><th class="num">RMSE</th><th class="num">MAE</th><th class="num">R²</th></tr></thead><tbody>
          ${DATA.model.metrics.map((m, i) => `<tr class="${i === 0 ? "best" : ""}"><td>${esc(m.Model)}${i === 0 ? ' <span class="chip" style="background:#0B6A63;color:#fff;margin-left:6px">Best</span>' : ""}</td><td class="num">${fmt(m.RMSE, 2)}</td><td class="num">${fmt(m.MAE, 2)}</td><td class="num">${fmt(m.R2, 3)}</td></tr>`).join("")}
          </tbody></table></div>
          <span class="small">Top features: ${DATA.model.importance.slice(0, 3).map(([n, v]) => `${n} (${Math.round(v * 100)}%)`).join(", ")}</span></section>
        <section class="card"><div class="card-head"><h2>Model prediction example</h2><span class="muted">${esc(best.Model)} trained in Spark MLlib</span></div>
          <div class="stat-line">${Object.entries(DATA.model.example.inputs).map(([k, v]) => `<span>${k} <b>${v}</b></span>`).join("")}</div>
          <div class="calc-result" style="background:${cat(DATA.model.example.prediction).soft}"><b style="color:${cat(DATA.model.example.prediction).ink}">${Math.round(DATA.model.example.prediction)}</b>
            <div><b style="font-size:16px;font-family:var(--body);color:${cat(DATA.model.example.prediction).ink}">${cat(DATA.model.example.prediction).name}</b><div class="small">Predicted by the model for the values above</div></div></div>
          <a class="btn" href="#/daily">Calculate AQI for your own values</a></section>
      </div>`;
    app.innerHTML = shell("overview", inner);
  }

  function viewCity(name) {
    const c = DATA.cities[name];
    if (!c) { go("#/overview"); return; }
    const k = cat(c.avg);
    const seasons = ["Winter", "Summer", "Monsoon", "Post-monsoon"];
    const ymax = Math.max(...c.yearly.map((y) => y[1]));
    const lk = c.lockdown;
    const pct = (a, b) => (a === null || b === null ? null : Math.round((b - a) / a * 100));
    const lockRow = (label, v) => `<div class="row-split"><span>${label}</span><span>${v[0] === null || v[1] === null ? "No data" : `<span class="muted">${fmt(v[0])} → </span><b>${fmt(v[1])}</b> (${pct(v[0], v[1])}%)`}</span></div>`;
    const inner = `
      <div class="page-head"><div style="display:flex;flex-direction:column;gap:6px">
        <a class="back" href="#/overview">← India overview</a><h1 style="font-size:38px">${esc(name)}</h1>
        <span class="muted" style="font-size:15px">${esc(c.state)} · data from ${esc(c.first)} to ${esc(c.last)}</span></div>
        <span class="rank-badge">Rank ${c.rank} of ${Object.keys(DATA.cities).length} (1 = most polluted)</span></div>
      <section class="card live-card" id="cityLive" data-city="${esc(name)}"><div class="loading" style="padding:0;border:0">Fetching live AQI for ${esc(name)}…</div></section>
      <div class="grid-kpi">
        ${kpi("Average AQI", fmt(c.avg), chip(c.avg))}
        ${kpi("Average PM2.5", fmt(c.pm25), `<span class="small" style="color:${c.pm25 > 60 ? "#8E1F18" : ""}">µg/m³ · limit 60</span>`)}
        ${kpi("Lockdown change", `<span style="color:#0B6A63">${lk ? fmt(lk.change_pct) + "%" : "n/a"}</span>`, `<span class="small">${lk ? "AQI, Mar–May 2020 vs 2019" : "No data for both lockdown periods"}</span>`)}
        ${kpi("Days of data", intIN(c.days), `<span class="small">daily records after cleaning</span>`)}
      </div>
      <div class="grid-2">
        <div style="display:flex;flex-direction:column;gap:16px">
          ${monthChart(c.monthly, "Average AQI by month", "")}
          <section class="card"><h2>By season</h2><div class="tiles">${c.season.map((v, i) => { const s = cat(v); return `<div class="tile" style="background:${v === null ? "#EEF0EC" : s.bg};color:${v === null ? "#4F5D59" : s.fg}"><span>${seasons[i]}</span><b>${v === null ? "No data" : v}</b></div>`; }).join("")}</div></section>
        </div>
        <section class="card"><div class="card-head"><h2>Pollutants vs safe limits</h2><span class="muted">Average level against India's NAAQS limit (black marker)</span></div>
          ${Object.entries(c.pollutants).map(([p, v]) => { const [lim, unit] = LIMITS[p]; const over = v !== null && v > lim;
            return `<div style="display:flex;flex-direction:column;gap:5px"><div class="row-split"><b style="font-weight:500">${p}</b><span style="color:${over ? "#8E1F18" : "#2C3A37"}">${v === null ? "No data recorded" : `${v} / ${lim} ${unit}`}</span></div>
            <div class="track limit-track"><span style="width:${v === null ? 0 : Math.min(v / lim * 50, 100)}%;background:${over ? "#D2382F" : "#0B6A63"}"></span><i class="limit"></i></div></div>`; }).join("")}
          <span class="small">Red bars are above the limit.</span></section>
      </div>
      <div class="grid-3">
        ${bucketBlock(c.buckets, "Days in each category", `${intIN(c.days)} days with a recorded AQI`)}
        <section class="card"><h2>Average AQI by year</h2>
          <div class="vchart" style="height:140px;border:0">${c.yearly.map(([y, v]) => `<div class="vcol"><small>${v}</small><i style="height:${Math.round(v / ymax * 90)}px;background:${cat(v).bg}"></i><small>${y}</small></div>`).join("")}</div></section>
        <section class="card"><h2>Record days</h2>
          <div class="row-split"><span>Worst day (${esc(c.worst_day)})</span><b style="color:#8E1F18">AQI ${c.worst_aqi}</b></div>
          <div class="row-split"><span>Cleanest day</span><b style="color:#1B7F3B">AQI ${c.best_aqi}</b></div>
          <h2 style="margin-top:8px">Lockdown effect</h2>
          ${lk ? lockRow("AQI", lk.aqi) + lockRow("PM2.5 (µg/m³)", lk.pm25) + lockRow("NO2 (µg/m³)", lk.no2)
               : `<span class="muted">${esc(name)} has no AQI data for both 25 Mar–31 May 2019 and 2020, so the lockdown effect cannot be measured.</span>`}
          <a class="btn btn-small" href="#/daily" data-city="${esc(name)}" data-action="cityDaily" style="align-self:flex-start">See every day in ${esc(name)}</a></section>
      </div>
      <section class="card" style="background:${k.soft}"><h2>Health advice: ${k.name} air on an average day</h2><p style="margin:0">${k.advice}</p>
        <span class="small">Based on CPCB health guidance for the AQI category.</span></section>`;
    app.innerHTML = shell(name, inner);
    fetchLive().then(() => fillCityLive(name)).catch(() => {
      const box = document.getElementById("cityLive");
      if (box && box.dataset.city === name) box.innerHTML = `<span class="muted">Live AQI is unavailable right now. <a href="#/live">Try the Live AQI page</a>.</span>`;
    });
  }
  function fillCityLive(name) {
    const box = document.getElementById("cityLive");
    if (!box || box.dataset.city !== name) return;
    const L = LIVE[name];
    if (!L || L.res.aqi === null) { box.innerHTML = `<span class="muted">Not enough live data for ${esc(name)} right now.</span>`; return; }
    const k = cat(L.res.aqi);
    box.style.background = k.soft;
    box.innerHTML = `<div class="live-row"><div class="calc-result" style="padding:0"><b style="color:${k.ink}">${L.res.aqi}</b>
      <div><b style="font-size:16px;color:${k.ink}">Live now: ${k.name}</b><div class="small" style="color:#2C3A37">Dominant ${L.res.dom || "–"} · ${liveTime(L.time)}</div></div></div>
      <div class="stat-line">${NAMES.slice(0, 6).map((n, j) => `<span>${n} <b>${L.vals[j] ?? "–"}</b></span>`).join("")}</div>
      <a class="btn btn-small" href="#/live" data-action="liveCity" data-city="${esc(name)}">Live details</a></div>`;
  }

  // ---------- live AQI page ----------
  async function viewLive() {
    app.innerHTML = shell("live", `<div class="loading">Fetching live air quality for ${Object.keys(COORDS).length} cities…</div>`);
    try { await fetchLive(); renderLive(); }
    catch (e) { liveError(); }
  }
  function liveError() {
    const main = document.getElementById("main");
    if (main) main.innerHTML = `<div class="error-box"><h2 style="margin-bottom:8px">Live data unavailable</h2>
      <p>Could not reach the live air quality service. Check your internet connection and try again.</p>
      <button class="btn" data-action="liveRefresh">Try again</button></div>`;
  }
  function renderLive() {
    const main = document.getElementById("main");
    if (!main || !LIVE) return;
    const names = Object.keys(COORDS).sort();
    if (!LIVE[liveCity]) liveCity = names.find((n) => LIVE[n]) || names[0];
    const L = LIVE[liveCity];
    const ranked = names.filter((n) => LIVE[n] && LIVE[n].res.aqi !== null).sort((a, b) => LIVE[b].res.aqi - LIVE[a].res.aqi);
    const updated = new Date(LIVE_AT).toLocaleTimeString("en-IN", { hour: "2-digit", minute: "2-digit" });
    let detail;
    if (!L || L.res.aqi === null) {
      detail = `<section class="card"><h2>${esc(liveCity)}</h2><span class="muted">Not enough live data to calculate an AQI right now.</span></section>`;
    } else {
      const k = cat(L.res.aqi), tr = L.trend.filter((x) => x.aqi !== null);
      const tmax = tr.length ? Math.max(...tr.map((x) => x.aqi)) : 1, scale = tmax > 300 ? 520 : 300;
      detail = `<div class="grid-2">
        <section class="card" style="background:${k.soft}"><div class="card-head"><h2>${esc(liveCity)} right now</h2><span class="muted">${liveTime(L.time)}</span></div>
          <div class="calc-result" style="padding:0"><b style="color:${k.ink};font-size:64px">${L.res.aqi}</b>
            <div><b style="font-size:18px;color:${k.ink}">${k.name}</b><div class="small" style="color:#2C3A37">Dominant pollutant: ${L.res.dom || "–"}</div></div></div>
          <p style="margin:0">${k.advice}</p></section>
        <section class="card"><div class="card-head"><h2>How this AQI was calculated</h2><span class="muted">24-hour averages (CO and O3: highest 8-hour average)</span></div>
          <div class="si-row small"><span>Pollutant</span><span>Level</span><span>Sub-index</span><span style="text-align:right">Value</span></div>
          ${KEYS.slice(0, 6).map((key, j) => { const c = L.vals[j], s = L.res.si[j], dom = L.res.dom === NAMES[j];
            return `<div class="si-row"><span style="font-weight:${dom ? 600 : 400}">${NAMES[j]}</span><span class="muted">${c === null ? "no data" : c + " " + UNITS[j]}</span>
              <span class="track"><span style="width:${s === null ? 0 : Math.min(s / 500 * 100, 100)}%;background:${s === null ? "#EEF0EC" : dom ? "#142220" : cat(s).bg}"></span></span><span style="text-align:right;font-weight:${dom ? 600 : 400}">${s === null ? "–" : Math.round(s)}</span></div>`; }).join("")}
          <button class="btn btn-small" data-action="liveToCalc" style="align-self:flex-start">Open in AQI calculator</button></section></div>
        <section class="card"><div class="card-head"><h2>Last 24 hours in ${esc(liveCity)}</h2><span class="muted">AQI at each hour, from the 24 hours before it</span></div>
          <div class="vchart" style="height:180px" role="img" aria-label="Hourly AQI, last 24 hours">${L.trend.map((x) => `<div class="vcol" title="${x.t.slice(11, 16)} · AQI ${x.aqi ?? "n/a"}"><i style="height:${x.aqi === null ? 0 : Math.round(x.aqi / scale * 150)}px;background:${cat(x.aqi).bg}"></i></div>`).join("")}</div>
          <div class="vlabels">${L.trend.map((x, i) => `<span>${i % 3 === 0 ? x.t.slice(11, 13) : ""}</span>`).join("")}</div>
          ${legend()}</section>`;
    }
    main.innerHTML = `
      <div class="page-head"><div style="display:flex;flex-direction:column;gap:6px"><h1>Live AQI</h1>
        <span class="muted" style="font-size:15px">Current air quality for all ${names.length} cities · fetched ${updated}</span></div>
        <div class="toolbar">
          <label class="field" style="font-size:13px">City<select id="liveCitySelect" style="min-width:220px;height:44px">${names.map((n) => `<option ${n === liveCity ? "selected" : ""}>${esc(n)}</option>`).join("")}</select></label>
          <button class="btn" data-action="liveRefresh" style="min-height:44px">Refresh</button></div></div>
      ${detail}
      <section class="card"><div class="card-head"><h2>All cities right now</h2><span class="muted">Most polluted first · click a city for its live details</span></div>
        <div class="rank-grid">${ranked.map((n, i) => { const a = LIVE[n].res.aqi;
          return `<button class="rank-row live-rank ${n === liveCity ? "on" : ""}" data-action="liveCity" data-city="${esc(n)}"><span style="color:#6B7874">${i + 1}</span>
          <span class="city-cell"><span style="font-weight:500">${esc(n)}</span><small>${esc((DATA.cities[n] || {}).state || "")}</small></span>
          <span class="track"><span style="width:${Math.min(a / 400 * 100, 100)}%;background:${cat(a).bg}"></span></span>
          <span style="text-align:right;font-weight:600">${a}</span></button>`; }).join("")}</div>
        ${liveNote}</section>`;
  }

  // ---------- daily AQI ----------
  async function viewDaily() {
    app.innerHTML = shell("daily", `<div class="loading">Loading ${intIN(DATA.overview.records)} days of data…</div>`);
    if (!DAILY) {
      try { DAILY = await loadJSON(DAILY_URL); }
      catch (e) { missingData("outputs/daily_aqi_web.json"); return; }
      IDX = {}; const perDate = {};
      let total = 0, match = 0, calc = 0, filled = 0, both = 0, same = 0;
      for (const [c, rows] of Object.entries(DAILY.cities)) {
        IDX[c] = {};
        for (const r of rows) {
          IDX[c][r[0]] = r; total++;
          const f = aqiOf(r.slice(1, 8)).aqi;
          if (f === r[9]) match++;
          if (r[9] !== null) { calc++; perDate[r[0]] = (perDate[r[0]] || 0) + 1; }
          if (r[8] === null && r[9] !== null) filled++;
          if (r[8] !== null && r[9] !== null) { both++; if (cat(r[8]).name === cat(r[9]).name) same++; }
        }
      }
      VERIFY = { total, match, calc, filled, both, same };
      const most = Math.max(...Object.values(perDate));
      daily.date = Object.keys(perDate).filter((d) => perDate[d] === most).sort().pop();
    }
    renderDaily();
  }

  function renderDaily() {
    const v = VERIFY;
    const head = `
      <div style="display:flex;flex-direction:column;gap:8px;max-width:58rem"><h1>Every day's AQI for every city, calculated twice</h1>
        <p class="muted" style="font-size:16px;margin:0">The AQI for every day in all ${Object.keys(DAILY.cities).length} cities is calculated from its pollutant levels with the CPCB method: once by the Spark backend, and again here in your browser with the same formula.</p></div>
      <div class="grid-kpi">
        ${kpi("Backend vs browser", `${(v.match / v.total * 100).toFixed(1)}%`, `<span class="small">${intIN(v.match)} of ${intIN(v.total)} days give the identical AQI</span>`, "dark")}
        ${kpi("Days with a calculated AQI", intIN(v.calc), `<span class="small">the rest lack 3 pollutants incl. PM</span>`)}
        ${kpi("Gaps filled", `<span style="color:#0B6A63">${intIN(v.filled)}</span>`, `<span class="small">days with no recorded AQI, now calculated</span>`)}
        ${kpi("Same category as recorded AQI", `${(v.same / v.both * 100).toFixed(1)}%`, `<span class="small">of ${intIN(v.both)} days with both values</span>`)}
      </div>
      <div class="toolbar">
        <div class="seg" role="group" aria-label="View">
          <button data-action="mode" data-mode="all" aria-pressed="${daily.mode === "all"}">All cities</button>
          <button data-action="mode" data-mode="city" aria-pressed="${daily.mode === "city"}">One city, full calendar</button></div>
        <label class="field" style="font-size:13px">City<select id="citySelect" style="min-width:220px;height:44px">${Object.keys(DAILY.cities).sort().map((c) => `<option ${c === daily.city ? "selected" : ""}>${esc(c)}</option>`).join("")}</select></label></div>`;
    const body = daily.mode === "all" ? dailyAll() : dailyCity();
    document.getElementById("main").innerHTML = head + body + breakdownAndCalc() + formulaCard();
  }

  function dailyAll() {
    const date = daily.date, cities = Object.keys(DAILY.cities);
    const list = cities.map((c) => { const r = IDX[c][date] || null; return { c, r, f: r ? aqiOf(r.slice(1, 8)) : { aqi: null, dom: null }, back: r ? r[9] : null }; });
    const score = (x) => (x.back !== null ? x.back : x.r ? -1 : -2);
    list.sort((a, b) => score(b) - score(a) || a.c.localeCompare(b.c));
    const rep = list.filter((x) => x.back !== null);
    const avg = rep.length ? Math.round(rep.reduce((t, x) => t + x.back, 0) / rep.length) : null;
    const withRows = list.filter((x) => x.r);
    const matches = withRows.filter((x) => x.f.aqi === x.back).length;
    let rank = 0;
    const yy = Number(date.slice(0, 4)), mm = Number(date.slice(5, 7));
    const dim = new Date(Date.UTC(yy, mm, 0)).getUTCDate();
    const heat = cities.map((c) => {
      let cells = "";
      for (let d = 1; d <= 31; d++) {
        const key = `${yy}-${pad(mm)}-${pad(d)}`;
        if (d > dim) { cells += `<span></span>`; continue; }
        const r = IDX[c][key];
        if (!r) { cells += `<button class="cell ${key === date ? "sel" : ""}" style="background:#F4F5F1" disabled aria-label="${esc(c)}, ${fmtDate(key)}: no readings"></button>`; continue; }
        const k = cat(r[9]);
        cells += `<button class="cell ${key === date ? "sel" : ""}" style="background:${k.bg}" data-action="pickCell" data-city="${esc(c)}" data-date="${key}" title="${esc(c)}, ${fmtDate(key)}: AQI ${r[9] === null ? "n/a" : r[9]}" aria-label="${esc(c)}, ${fmtDate(key)}: AQI ${r[9] === null ? "not available" : r[9] + ", " + k.name}"></button>`;
      }
      return `<div class="cal-row" style="--label:150px"><span style="font-size:13px;color:#142220">${esc(c)}</span>${cells}</div>`;
    }).join("");
    return `
      <section class="card">
        <div class="page-head"><div class="card-head"><h2 style="font-size:24px">India on ${fmtDate(date)}</h2>
          <span class="muted">${rep.length} of ${cities.length} cities have an AQI this day · backend = browser for ${matches} of ${withRows.length} cities</span></div>
          <div class="toolbar"><button class="btn btn-small" style="min-height:44px;min-width:44px" data-action="shift" data-n="-1" aria-label="Previous day">‹</button>
            <label class="field" style="font-size:13px">Date<input type="date" id="dateInput" value="${date}" min="2015-01-01" max="2020-07-01" style="height:44px"></label>
            <button class="btn btn-small" style="min-height:44px;min-width:44px" data-action="shift" data-n="1" aria-label="Next day">›</button></div></div>
        <div class="grid-kpi" style="grid-template-columns:repeat(auto-fit,minmax(170px,1fr))">
          <div class="note" style="background:${cat(avg).soft}"><span class="small" style="color:#2C3A37">Average across cities</span><div style="font-family:var(--display);font-size:28px;font-weight:600;color:${cat(avg).ink}">${avg === null ? "–" : avg}</div></div>
          <div class="note" style="background:#FBE7E5"><span class="small" style="color:#2C3A37">Most polluted</span><div style="font-family:var(--display);font-size:19px;font-weight:600;color:#8E1F18">${rep.length ? esc(rep[0].c) + " · " + rep[0].back : "–"}</div></div>
          <div class="note" style="background:#E4F2E7"><span class="small" style="color:#2C3A37">Cleanest</span><div style="font-family:var(--display);font-size:19px;font-weight:600;color:#14602C">${rep.length ? esc(rep[rep.length - 1].c) + " · " + rep[rep.length - 1].back : "–"}</div></div>
          <div class="note" style="background:#FBEBDD"><span class="small" style="color:#2C3A37">Cities Poor or worse</span><div style="font-family:var(--display);font-size:28px;font-weight:600;color:#8A4300">${rep.filter((x) => x.back > 200).length}</div></div>
        </div>
        <div class="table-wrap"><table style="min-width:860px"><thead><tr><th>#</th><th>City</th><th>Category</th><th class="num">Backend</th><th class="num">Browser</th><th class="num">Recorded</th><th>Dominant</th><th>Check</th><th><span class="hidden">Details</span></th></tr></thead><tbody>
          ${list.map((x) => { const ok = x.r && x.f.aqi === x.back; if (x.back !== null) rank++;
            return `<tr class="${daily.city === x.c && daily.sel === date ? "picked" : ""}"><td style="color:#6B7874">${x.back !== null ? rank : "–"}</td>
              <td><span class="city-cell"><span style="font-weight:500">${esc(x.c)}</span><small>${esc((DATA.cities[x.c] || {}).state || "")}</small></span></td>
              <td>${x.r ? chip(x.back) : '<span class="chip" style="background:#EEF0EC;color:#4F5D59">No readings</span>'}</td>
              <td class="num"><b>${x.back ?? "–"}</b></td><td class="num"><b>${x.f.aqi ?? "–"}</b></td><td class="num muted">${x.r && x.r[8] !== null ? x.r[8] : "–"}</td>
              <td>${x.f.dom || "–"}</td><td style="color:${ok ? "#0B6A63" : "#8E1F18"};font-weight:500">${x.r ? (ok ? "✓ match" : "✗ differs") : "–"}</td>
              <td>${x.r ? `<button class="btn btn-small" data-action="pickDay" data-city="${esc(x.c)}" data-date="${date}">Breakdown</button>` : ""}</td></tr>`; }).join("")}
        </tbody></table></div>
      </section>
      <section class="card"><div class="card-head"><h2>All cities, ${MONTHS[mm - 1]} ${yy}</h2><span class="muted">Calculated AQI for every city and day of the month · click a square to jump to that day</span></div>
        <div class="table-wrap"><div class="cal" style="min-width:980px">
          <div class="cal-row head" style="--label:150px"><span></span>${Array.from({ length: 31 }, (_, i) => `<span>${i + 1}</span>`).join("")}</div>${heat}</div></div>
        ${legend('<span><i style="background:#E4E7E1"></i>No AQI</span><span><i style="background:#F4F5F1;border:1px solid #DDE1DA"></i>No readings</span>')}</section>`;
  }

  function dailyCity() {
    const rows = DAILY.cities[daily.city];
    const counts = {};
    rows.forEach((r) => { if (r[9] !== null) { const y = Number(r[0].slice(0, 4)); counts[y] = (counts[y] || 0) + 1; } });
    const years = Object.keys(counts).map(Number).sort();
    const fullest = years.reduce((a, y) => (counts[y] >= counts[a] ? y : a), years[0]);
    const year = daily.year && years.includes(daily.year) ? daily.year : fullest;
    const byDate = {}; rows.forEach((r) => { if (Number(r[0].slice(0, 4)) === year) byDate[r[0]] = r; });
    const yr = Object.values(byDate), withA = yr.filter((r) => r[9] !== null);
    const worst = withA.reduce((a, r) => (!a || r[9] > a[9] ? r : a), null), best = withA.reduce((a, r) => (!a || r[9] < a[9] ? r : a), null);
    const yMatch = yr.filter((r) => aqiOf(r.slice(1, 8)).aqi === r[9]).length;
    const cal = MONTHS.map((m, mi) => {
      const dim = new Date(Date.UTC(year, mi + 1, 0)).getUTCDate();
      let cells = "";
      for (let d = 1; d <= 31; d++) {
        const key = `${year}-${pad(mi + 1)}-${pad(d)}`, r = byDate[key];
        if (d > dim) { cells += "<span></span>"; continue; }
        if (!r) { cells += `<button class="cell" style="background:#F4F5F1" disabled aria-label="${fmtDate(key)}: no data"></button>`; continue; }
        const val = daily.view === "calc" ? r[9] : r[8], k = cat(val);
        cells += `<button class="cell ${key === daily.sel ? "sel" : ""}" style="background:${k.bg}" data-action="pickDay" data-city="${esc(daily.city)}" data-date="${key}" title="${fmtDate(key)}: AQI ${val ?? "n/a"}" aria-label="${fmtDate(key)}: AQI ${val === null ? "not available" : val + ", " + k.name}"></button>`;
      }
      return `<div class="cal-row"><span>${m}</span>${cells}</div>`;
    }).join("");
    return `<section class="card">
      <div class="toolbar">
        <div class="field" style="font-size:13px">Year<div class="years" role="group" aria-label="Year">${years.map((y) => `<button data-action="year" data-year="${y}" aria-pressed="${y === year}">${y}</button>`).join("")}</div></div>
        <div class="field" style="font-size:13px;margin-left:auto">Colour days by<div class="seg"><button data-action="view" data-view="calc" aria-pressed="${daily.view === "calc"}">Calculated AQI</button><button data-action="view" data-view="rec" aria-pressed="${daily.view === "rec"}">Recorded AQI</button></div></div>
      </div>
      <div class="stat-line"><span><b>${withA.length}</b> days with AQI</span><span>Average <b>${withA.length ? Math.round(withA.reduce((t, r) => t + r[9], 0) / withA.length) : "–"}</b></span>
        <span>Worst <b>${worst ? worst[9] + " (" + fmtDate(worst[0]) + ")" : "–"}</b></span><span>Cleanest <b>${best ? best[9] + " (" + fmtDate(best[0]) + ")" : "–"}</b></span>
        <span>Backend = browser on <b>${yMatch} of ${yr.length} days</b></span></div>
      <div class="table-wrap"><div class="cal"><div class="cal-row head"><span></span>${Array.from({ length: 31 }, (_, i) => `<span>${i + 1}</span>`).join("")}</div>${cal}</div></div>
      ${legend('<span><i style="background:#E4E7E1"></i>No AQI (not enough data)</span><span class="small">Click a day to see how its AQI was calculated.</span>')}</section>`;
  }

  function breakdownAndCalc() {
    let day;
    const r = daily.sel && IDX[daily.city] ? IDX[daily.city][daily.sel] : null;
    if (!r) {
      day = `<div class="card-head"><h2>Pick a day</h2><span class="muted">Click a Breakdown button or any coloured square</span></div>
        <div class="note" style="background:#EEF0EC;color:#2C3A37">The backend and browser values will be compared here.</div>`;
    } else {
      const res = aqiOf(r.slice(1, 8)), bc = cat(r[9]), fc = cat(res.aqi), same = res.aqi === r[9];
      day = `<div class="card-head"><h2>${esc(daily.city)}, ${fmtDate(daily.sel)}</h2><span class="muted">${res.aqi === null ? "Not enough pollutants measured to calculate an AQI" : `Dominant pollutant: ${res.dom} · ${fc.name}`}</span></div>
        <div class="trio"><div style="background:${bc.bg};color:${bc.fg}"><span class="small" style="color:inherit">Spark backend</span><b>${r[9] ?? "n/a"}</b></div>
          <div style="background:${fc.bg};color:${fc.fg}"><span class="small" style="color:inherit">Browser (frontend)</span><b>${res.aqi ?? "n/a"}</b></div>
          <div style="background:#EEF0EC"><span class="small">Recorded by CPCB</span><b>${r[8] ?? "n/a"}</b></div></div>
        <div class="note" style="background:${same ? "#E3F0EE" : "#FBE7E5"};color:${same ? "#08504B" : "#8E1F18"};font-weight:500">${same ? "Match: the browser reproduced the backend value exactly." : "Mismatch between backend and browser."}</div>
        <div class="si-row small"><span>Pollutant</span><span>Level</span><span>Sub-index</span><span style="text-align:right">Value</span></div>
        ${KEYS.map((k, j) => { const c = r[j + 1], s = res.si[j], dom = res.dom === NAMES[j];
          return `<div class="si-row"><span style="font-weight:${dom ? 600 : 400}">${NAMES[j]}</span><span class="muted">${c === null ? "not measured" : c + " " + UNITS[j]}</span>
            <span class="track"><span style="width:${s === null ? 0 : Math.min(s / 500 * 100, 100)}%;background:${s === null ? "#EEF0EC" : dom ? "#142220" : cat(s).bg}"></span></span><span style="text-align:right;font-weight:${dom ? 600 : 400}">${s === null ? "–" : Math.round(s)}</span></div>`; }).join("")}
        <span class="small">${r[8] === null && r[9] !== null ? "CPCB recorded no AQI for this day; the calculation fills the gap." : "AQI = the highest sub-index (bold)."}</span>`;
    }
    const vals = KEYS.map((k) => (daily.calc[k] === "" ? null : Number(daily.calc[k])));
    const cr = aqiOf(vals), cc = cat(cr.aqi);
    return `<div class="grid-2">
      <section class="card" id="breakdown">${day}</section>
      <section class="card"><div class="page-head"><div class="card-head"><h2>AQI calculator</h2><span class="muted">Type pollutant levels; AQI updates live with the CPCB formula</span></div>
          <button class="btn btn-small" data-action="useDay" ${r ? "" : "disabled"}>Use selected day</button></div>
        <div class="calc-fields">${KEYS.map((k, j) => `<label class="field" style="font-size:13px">${NAMES[j]} (${UNITS[j]})<input type="number" min="0" step="any" data-calc="${k}" value="${esc(daily.calc[k])}" placeholder="–"></label>`).join("")}</div>
        <div class="calc-result" id="calcResult" style="background:${cc.soft}">${calcResultHTML(cr, cc)}</div></section></div>`;
  }
  function calcResultHTML(cr, cc) {
    return `<b style="color:${cc.ink}">${cr.aqi ?? "–"}</b><div><b style="font-size:16px;font-family:var(--body);color:${cc.ink}">${cr.aqi === null ? "Not enough data" : cc.name}</b>
      <div class="small" style="color:#2C3A37">${cr.aqi === null ? "Enter at least 3 pollutants, including PM2.5 or PM10." : "Dominant pollutant: " + cr.dom}</div></div>`;
  }
  function formulaCard() {
    const conc = DAILY.breakpoints.conc;
    const bands = ["Good 0–50", "Satisfactory 51–100", "Moderate 101–200", "Poor 201–300", "Very Poor 301–400", "Severe 401–500"];
    const range = (k, i) => (i === 5 ? `${conc[k][5]}+` : `${conc[k][i]}–${conc[k][i + 1]}`);
    return `<section class="card"><h2>How the AQI is calculated</h2>
      <p style="margin:0;max-width:64rem">Each pollutant's level is turned into a sub-index by straight-line interpolation inside its CPCB band: sub-index = I<sub>lo</sub> + (C − C<sub>lo</sub>) × (I<sub>hi</sub> − I<sub>lo</sub>) ÷ (C<sub>hi</sub> − C<sub>lo</sub>). The day's AQI is the highest sub-index, and it is only given when at least 3 pollutants are measured, one of them PM2.5 or PM10. CPCB's recorded AQI uses hourly data, while this dataset has daily averages, so calculated and recorded values differ on some days.</p>
      <div class="table-wrap"><table style="min-width:720px"><thead><tr><th>AQI band</th>${NAMES.map((n, j) => `<th>${n}${j === 4 ? " (mg/m³)" : ""}</th>`).join("")}</tr></thead><tbody>
        ${bands.map((b, i) => `<tr><td><i class="dot" style="display:inline-block;border-radius:2px;background:${BUCKET_COLORS[i]};margin-right:6px"></i>${b}</td>${KEYS.map((k) => `<td>${range(k, i)}</td>`).join("")}</tr>`).join("")}
      </tbody></table></div><span class="small">Units are µg/m³ except CO. Breakpoints are read from the backend output, so the browser uses exactly the same table as Spark.</span></section>`;
  }

  // ---------- events ----------
  document.addEventListener("click", (e) => {
    const t = e.target.closest("[data-action]");
    if (!t) return;
    const a = t.dataset.action;
    if (a === "logout") { sessionStorage.removeItem("aqi_user"); go("#/login"); }
    else if (a === "guest") { sessionStorage.setItem("aqi_user", JSON.stringify({ name: "Guest" })); go("#/overview"); }
    else if (a === "cityDaily") { daily.mode = "city"; daily.city = t.dataset.city; daily.year = null; daily.sel = null; }
    else if (a === "mode") { daily.mode = t.dataset.mode; renderDaily(); }
    else if (a === "shift") { const n = Number(t.dataset.n); const d = new Date(Date.parse(daily.date + "T00:00:00Z") + n * 864e5).toISOString().slice(0, 10); if (d >= "2015-01-01" && d <= "2020-07-01") { daily.date = d; renderDaily(); } }
    else if (a === "pickCell") { daily.date = t.dataset.date; daily.city = t.dataset.city; daily.sel = t.dataset.date; daily.year = Number(t.dataset.date.slice(0, 4)); renderDaily(); document.getElementById("breakdown").scrollIntoView({ behavior: "smooth", block: "start" }); }
    else if (a === "pickDay") { daily.city = t.dataset.city; daily.sel = t.dataset.date; daily.year = Number(t.dataset.date.slice(0, 4)); renderDaily(); document.getElementById("breakdown").scrollIntoView({ behavior: "smooth", block: "start" }); }
    else if (a === "year") { daily.year = Number(t.dataset.year); daily.sel = null; renderDaily(); }
    else if (a === "view") { daily.view = t.dataset.view; renderDaily(); }
    else if (a === "liveRefresh") { const m = document.getElementById("main"); if (m) m.innerHTML = `<div class="loading">Refreshing live data…</div>`; fetchLive(true).then(renderLive).catch(liveError); }
    else if (a === "liveCity") { liveCity = t.dataset.city; if (location.hash === "#/live") { renderLive(); window.scrollTo({ top: 0, behavior: "smooth" }); } }
    else if (a === "liveToCalc") { const L = LIVE && LIVE[liveCity]; if (L) { KEYS.forEach((k, j) => { daily.calc[k] = L.vals[j] === null ? "" : String(L.vals[j]); }); go("#/daily"); } }
    else if (a === "useDay") { const r = IDX[daily.city][daily.sel]; if (r) { KEYS.forEach((k, j) => { daily.calc[k] = r[j + 1] === null ? "" : String(r[j + 1]); }); renderDaily(); } }
  });
  document.addEventListener("change", (e) => {
    if (e.target.id === "citySelect") { daily.city = e.target.value; daily.mode = "city"; daily.year = null; daily.sel = null; renderDaily(); }
    if (e.target.id === "jumpCity" && e.target.value) go("#/city/" + encodeURIComponent(e.target.value));
    if (e.target.id === "liveCitySelect") { liveCity = e.target.value; renderLive(); }
    if (e.target.id === "dateInput" && e.target.value >= "2015-01-01" && e.target.value <= "2020-07-01") { daily.date = e.target.value; renderDaily(); }
  });
  document.addEventListener("input", (e) => {
    const k = e.target.dataset && e.target.dataset.calc;
    if (!k) return;
    daily.calc[k] = e.target.value;
    const cr = aqiOf(KEYS.map((x) => (daily.calc[x] === "" ? null : Number(daily.calc[x])))), cc = cat(cr.aqi);
    const box = document.getElementById("calcResult");
    box.style.background = cc.soft; box.innerHTML = calcResultHTML(cr, cc);
  });

  // ---------- router ----------
  function route() {
    const h = location.hash || "#/login";
    if (!user() && h !== "#/login") { go("#/login"); return; }
    if (h === "#/login") { if (user()) { go("#/overview"); return; } viewLogin(); }
    else if (h === "#/overview") viewOverview();
    else if (h === "#/daily") viewDaily();
    else if (h === "#/live") viewLive();
    else if (h.startsWith("#/city/")) viewCity(decodeURIComponent(h.slice(7)));
    else go("#/overview");
    window.scrollTo(0, 0);
  }

  loadJSON(DATA_URL)
    .then((d) => { DATA = d; window.addEventListener("hashchange", route); route(); })
    .catch(() => missingData("outputs/dashboard_data.json"));
})();
