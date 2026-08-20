(function () {
  "use strict";

  const state = {
    admin: false,
    mode: "business",
    data: null,
    loading: false,
    requestSerial: 0,
    controller: null,
    drawerController: null,
    lastFocus: null,
    pendingInitialMode: "business",
    filters: defaultFilters(),
  };

  const root = document.getElementById("sales-coaching-dashboard");
  const businessPanel = document.getElementById("business-overview-content");
  const businessTab = document.getElementById("business-overview-tab");
  const coachingTab = document.getElementById("sales-coaching-tab");
  if (!root || !businessPanel || !businessTab || !coachingTab) return;

  function dateKey(value) {
    const year = value.getFullYear();
    const month = String(value.getMonth() + 1).padStart(2, "0");
    const day = String(value.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  function defaultFilters() {
    const end = new Date();
    const start = new Date(end);
    start.setDate(start.getDate() - 27);
    return {
      period: "4",
      start: dateKey(start),
      end: dateKey(end),
      seller: "",
      channel: "all",
      lifecycle: "all",
      segment: "all",
      comparison: "previous",
    };
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function number(value, digits = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed)
      ? parsed.toLocaleString("sv-SE", { maximumFractionDigits: digits })
      : "—";
  }

  function percent(value) {
    return value === null || value === undefined
      ? "—"
      : `${number(Number(value) * 100, 1)} %`;
  }

  function statusLabel(status) {
    return {
      sufficient: "Tillräckligt underlag",
      small_sample: "Litet underlag",
      not_computable: "Kan inte beräknas",
      limited_data_quality: "Begränsad datakvalitet",
    }[status] || "Underlag saknas";
  }

  function rateEvidence(rate) {
    if (!rate || rate.denominator === undefined) return "";
    return `${number(rate.numerator)} av ${number(rate.denominator)}`;
  }

  function orderValues(values) {
    return Object.entries(values || {})
      .map(([currency, total]) => `${number(total, 2)} ${escapeHtml(currency)}`)
      .join(", ") || "—";
  }

  function comparisonText(metric) {
    const comparisons = metric?.comparisons || {};
    const previous = comparisons.previous_period;
    const previousValue = previous && typeof previous === "object" ? previous.value : previous?.value ?? previous;
    const parts = [];
    if (previousValue !== null && previousValue !== undefined) {
      parts.push(`Föregående: ${metric.denominator === undefined ? number(previousValue) : percent(previousValue)}`);
    }
    if (comparisons.team_median !== null && comparisons.team_median !== undefined) {
      parts.push(`Teammedian: ${metric.denominator === undefined ? number(comparisons.team_median) : percent(comparisons.team_median)}`);
    }
    return parts.join(" · ");
  }

  function filterMarkup() {
    return `
      <form class="sc-filter-bar" id="sc-filter-form" aria-label="Filter för säljcoachning">
        <div class="sc-field">
          <label for="sc-period">Period</label>
          <select id="sc-period" name="period">
            <option value="1">1 vecka</option><option value="4">4 veckor</option>
            <option value="8">8 veckor</option><option value="12">12 veckor</option>
            <option value="custom">Eget intervall</option>
          </select>
        </div>
        <div class="sc-field">
          <label for="sc-seller">Säljare</label>
          <select id="sc-seller" name="seller"><option value="">Alla säljare</option></select>
        </div>
        <div class="sc-field">
          <label for="sc-channel">Kanal</label>
          <select id="sc-channel" name="channel">
            <option value="all">Alla</option><option value="visit">Besök</option>
            <option value="phone">Telefon</option><option value="email">Manuellt mejl</option>
          </select>
        </div>
        <div class="sc-field">
          <label for="sc-lifecycle">Lifecycle</label>
          <select id="sc-lifecycle" name="lifecycle">
            <option value="all">Alla</option><option value="prospect">Prospekt</option>
            <option value="first_order">Första order</option><option value="established">Etablerad</option>
            <option value="reactivation">Reaktivering</option>
          </select>
        </div>
        <div class="sc-field">
          <label for="sc-segment">Kundsegment</label>
          <select id="sc-segment" name="segment">
            <option value="all">Alla</option><option value="A">A</option><option value="B">B</option>
            <option value="C">C</option><option value="missing">Saknas</option>
          </select>
        </div>
        <div class="sc-field">
          <label for="sc-comparison">Jämförelse</label>
          <select id="sc-comparison" name="comparison"><option value="previous">Föregående lika period</option></select>
        </div>
        <div class="sc-custom-dates" id="sc-custom-dates" hidden>
          <div class="sc-field"><label for="sc-start">Från</label><input id="sc-start" name="start" type="date" /></div>
          <div class="sc-field"><label for="sc-end">Till</label><input id="sc-end" name="end" type="date" /></div>
        </div>
      </form>
      <div id="sc-dashboard-content" aria-live="polite"></div>
    `;
  }

  function hydrateFiltersFromUrl() {
    const params = new URLSearchParams(window.location.search);
    if (params.get("sales_coaching") !== "1") return;
    state.mode = "coaching";
    for (const key of ["start", "end", "seller", "channel", "lifecycle", "segment"]) {
      if (params.has(key)) state.filters[key] = params.get(key);
    }
    state.filters.period = params.get("period") || "custom";
  }

  function setControlValues() {
    for (const key of ["period", "seller", "channel", "lifecycle", "segment", "comparison", "start", "end"]) {
      const control = document.getElementById(`sc-${key}`);
      if (control) control.value = state.filters[key];
    }
    document.getElementById("sc-custom-dates").hidden = state.filters.period !== "custom";
  }

  function updateUrl() {
    const url = new URL(window.location.href);
    const keys = ["sales_coaching", "period", "start", "end", "seller", "channel", "lifecycle", "segment"];
    keys.forEach(key => url.searchParams.delete(key));
    if (state.mode === "coaching") {
      url.searchParams.set("sales_coaching", "1");
      for (const key of ["period", "start", "end", "seller", "channel", "lifecycle", "segment"]) {
        if (state.filters[key]) url.searchParams.set(key, state.filters[key]);
      }
    }
    window.history.replaceState({}, "", url);
  }

  function setPeriod(weeks) {
    if (weeks === "custom") return;
    const end = new Date();
    const start = new Date(end);
    start.setDate(start.getDate() - Number(weeks) * 7 + 1);
    state.filters.end = dateKey(end);
    state.filters.start = dateKey(start);
  }

  function readFilters() {
    const period = document.getElementById("sc-period").value;
    state.filters.period = period;
    if (period === "custom") {
      state.filters.start = document.getElementById("sc-start").value;
      state.filters.end = document.getElementById("sc-end").value;
    } else {
      setPeriod(period);
    }
    for (const key of ["seller", "channel", "lifecycle", "segment", "comparison"]) {
      state.filters[key] = document.getElementById(`sc-${key}`).value;
    }
  }

  function queryString(extra = {}) {
    const params = new URLSearchParams();
    for (const key of ["start", "end", "seller", "channel", "lifecycle", "segment"]) {
      const value = extra[key] ?? state.filters[key];
      if (value && !(key === "seller" && value === "")) params.set(key, value);
    }
    for (const [key, value] of Object.entries(extra)) {
      if (!["start", "end", "seller", "channel", "lifecycle", "segment"].includes(key) && value !== "") params.set(key, value);
    }
    return params.toString();
  }

  function setMode(mode, { update = true } = {}) {
    if (mode === "coaching" && !state.admin) mode = "business";
    state.mode = mode;
    const coaching = mode === "coaching";
    businessPanel.hidden = coaching;
    root.hidden = !coaching;
    businessTab.setAttribute("aria-selected", String(!coaching));
    coachingTab.setAttribute("aria-selected", String(coaching));
    if (update) updateUrl();
    if (coaching && !state.data && !state.loading) loadSummary();
  }

  function setAdmin(isAdmin) {
    state.admin = Boolean(isAdmin);
    if (!state.admin) setMode("business", { update: false });
    else if (state.pendingInitialMode === "coaching" || state.mode === "coaching") {
      state.pendingInitialMode = "business";
      setMode("coaching");
      window.dispatchEvent(new CustomEvent("sales-coaching:open-view"));
    }
  }

  function bindControls() {
    businessTab.addEventListener("click", () => setMode("business"));
    coachingTab.addEventListener("click", () => setMode("coaching"));
    document.getElementById("sc-filter-form").addEventListener("change", event => {
      if (event.target.id === "sc-period") {
        document.getElementById("sc-custom-dates").hidden = event.target.value !== "custom";
      }
      readFilters();
      setControlValues();
      updateUrl();
      loadSummary();
    });
    root.addEventListener("click", handleDashboardClick);
    root.addEventListener("keydown", event => {
      if ((event.key === "Enter" || event.key === " ") && event.target.closest("[data-drilldown]")) {
        event.preventDefault();
        event.target.dispatchEvent(new MouseEvent("click", { bubbles: true }));
      }
    });
    document.addEventListener("keydown", event => {
      if (event.key === "Escape") closeDrawer();
    });
  }

  function renderLoading() {
    const target = document.getElementById("sc-dashboard-content");
    if (state.data) {
      target.setAttribute("aria-busy", "true");
      return;
    }
    target.innerHTML = `<div class="sc-section sc-skeleton" role="status" aria-label="Laddar säljcoachning"></div><div class="sc-section sc-skeleton"></div>`;
  }

  async function loadSummary() {
    if (!state.admin) return;
    const serial = ++state.requestSerial;
    state.controller?.abort();
    state.controller = new AbortController();
    state.loading = true;
    renderLoading();
    try {
      const response = await fetch(`/sales-coaching-insights?${queryString()}`, { signal: state.controller.signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (serial !== state.requestSerial) return;
      state.data = data;
      renderDashboard(data);
    } catch (error) {
      if (error.name === "AbortError" || serial !== state.requestSerial) return;
      renderError();
    } finally {
      if (serial === state.requestSerial) state.loading = false;
    }
  }

  function renderError() {
    const target = document.getElementById("sc-dashboard-content");
    if (state.data) {
      target.removeAttribute("aria-busy");
      target.insertAdjacentHTML("afterbegin", `<div class="sc-error" role="alert">Kunde inte uppdatera. Senast lyckade data visas. <button type="button" data-sc-action="retry">Försök igen</button></div>`);
      return;
    }
    target.innerHTML = `<div class="sc-section sc-error" role="alert">Säljcoachningen kunde inte laddas.<br><button type="button" data-sc-action="retry">Försök igen</button></div>`;
  }

  function renderSellerOptions(options) {
    const select = document.getElementById("sc-seller");
    const selected = state.filters.seller;
    select.innerHTML = `<option value="">Alla säljare</option>${(options || []).map(value => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}`;
    select.value = selected;
  }

  function qualityMarkup(quality) {
    const coverage = quality.priority_percentile_coverage || quality.priority_snapshot_coverage;
    return `
      <button type="button" class="sc-quality-banner" data-status="${escapeHtml(quality.status)}" data-drilldown="data_quality">
        <span class="sc-quality-title">${statusLabel(quality.status)}</span>
        <span class="sc-quality-summary">Säker identitet ${percent(quality.secure_customer_identity?.value)} · Standardiserat ${percent(quality.standardized_activity?.value)} · Snapshot ${percent(quality.priority_snapshot_coverage?.value)} · Historisk percentil ${percent(coverage?.value)} · Väntar på 10 dagar ${number(quality.waiting_outcome_count)} · Exkluderade legacy-rader ${number(quality.excluded_legacy_rows)}</span>
        <span class="sc-quality-arrow" aria-hidden="true">›</span>
      </button>`;
  }

  function kpiMarkup(key, metric) {
    const isRate = metric.denominator !== undefined;
    const value = isRate ? percent(metric.value) : number(metric.value);
    let secondary = "";
    if (key === "human_activities") {
      secondary = `Unika kunder ${number(metric.unique_customers)} · Besök ${number(metric.channel_mix?.visit)} · Telefon ${number(metric.channel_mix?.phone)} · Manuellt mejl ${number(metric.channel_mix?.email)}`;
    }
    if (key === "positive_dialogue" && metric.positive_to_order_10d) {
      secondary = `Positiv → order inom 10 dagar: ${percent(metric.positive_to_order_10d.value)} (${rateEvidence(metric.positive_to_order_10d)})`;
    }
    if (key === "order_10d") {
      secondary = `Attribuerade order ${number(metric.attributed_orders)} · Unika orderkunder ${number(metric.unique_order_customers)} · ${number(metric.dfp, 2)} DFP · ${orderValues(metric.order_value_by_currency)}`;
    }
    return `
      <button type="button" class="sc-kpi-card" data-drilldown="${escapeHtml(metric.drilldown_metric)}" aria-label="${escapeHtml(metric.label)}: ${value}">
        <span class="sc-kpi-header"><span class="sc-kpi-label">${escapeHtml(metric.label)}</span><span class="sc-kpi-info" title="${escapeHtml(metric.definition)}" aria-label="Definition: ${escapeHtml(metric.definition)}">i</span></span>
        <span class="sc-kpi-value">${value}</span>
        ${isRate ? `<span class="sc-kpi-evidence">${rateEvidence(metric)}</span>` : ""}
        <span class="sc-status">${statusLabel(metric.status)}</span>
        <span class="sc-kpi-comparison">${escapeHtml(comparisonText(metric))}</span>
        ${secondary ? `<span class="sc-kpi-secondary">${secondary}</span>` : ""}
      </button>`;
  }

  function kpisMarkup(kpis) {
    const order = ["human_activities", "reach", "positive_dialogue", "order_10d", "priority_focus", "bom_ratio"];
    return `<section class="sc-section" aria-labelledby="sc-kpi-title"><div class="sc-section-heading"><div><h2 id="sc-kpi-title">Coachningsöversikt</h2><p>Rates bedöms neutralt när underlaget är mindre än tio.</p></div></div><div class="sc-kpi-grid">${order.map(key => kpiMarkup(key, kpis[key])).join("")}</div></section>`;
  }

  function matrixMarkup(matrix) {
    const medianX = Number(matrix.medians?.priority_focus ?? 0.5) * 100;
    const medianY = Number(matrix.medians?.order_10d ?? 0.5) * 100;
    const bubbles = (matrix.sellers || []).map(item => {
      const x = Math.max(3, Math.min(97, Number(item.priority_focus.value) * 100));
      const y = Math.max(3, Math.min(97, Number(item.order_10d.value) * 100));
      const size = Math.max(38, Math.min(78, 30 + Math.sqrt(Number(item.human_activities || 0)) * 5));
      const title = `${item.seller}: prioritetsfokus ${percent(item.priority_focus.value)}, order ${percent(item.order_10d.value)}, ${item.human_activities} aktiviteter, percentiltäckning ${percent(item.priority_percentile_coverage.value)}`;
      return `<button type="button" class="sc-bubble" style="left:${x}%;bottom:${y}%;width:${size}px;height:${size}px" data-seller="${escapeHtml(item.seller)}" title="${escapeHtml(title)}" aria-label="${escapeHtml(title)}">${escapeHtml(String(item.seller).slice(0, 2).toUpperCase())}</button>`;
    }).join("");
    const insufficient = (matrix.insufficient_sample || []).map(item => `${escapeHtml(item.seller)} (${item.reasons.map(reason => reason === "order_sample_below_10" ? "färre än 10 mogna kontakter" : "percentiltäckning under 70 %").join(", ")})`).join(" · ");
    return `<section class="sc-section" aria-labelledby="sc-matrix-title"><div class="sc-section-heading"><div><h2 id="sc-matrix-title">Teamets coachningsmatris</h2><p>Prioritetsfokus mot mogen kontaktkonvertering. Bubbelstorlek visar mänskliga aktiviteter.</p></div></div><div class="sc-matrix-wrap"><div class="sc-matrix" role="img" aria-label="Coachningsmatris med teammedianer"><span class="sc-matrix-median-x" style="left:${medianX}%"></span><span class="sc-matrix-median-y" style="bottom:${medianY}%"></span>${bubbles}</div><div class="sc-axis-label">Prioritetsfokus →</div></div>${insufficient ? `<div class="sc-insufficient"><strong>Litet underlag:</strong> ${insufficient}</div>` : ""}</section>`;
  }

  function funnelMarkup(funnel) {
    return `<div><div class="sc-section-heading"><div><h2>Säljtratt</h2><p>Endast Besök och Telefon. Manuella mejl analyseras under kanaler.</p></div></div><div class="sc-funnel">${(funnel.steps || []).map(step => `<button type="button" class="sc-funnel-step" data-drilldown="${escapeHtml(step.drilldown_metric)}"><span class="sc-funnel-count">${number(step.count)}</span><span class="sc-funnel-label">${escapeHtml(step.label)}</span><span class="sc-funnel-rate">${step.rate ? `${percent(step.rate.value)} · ${rateEvidence(step.rate)} · ${statusLabel(step.rate.status)}` : "Startkohort"}</span></button>`).join("")}</div></div>`;
  }

  function trendMarkup(rows) {
    if (!rows?.length) return `<div><div class="sc-section-heading"><h2>Veckotrend</h2></div><div class="sc-empty">Ingen veckodata.</div></div>`;
    const width = 620, height = 250, pad = 32;
    const series = [
      ["human_activities", "Aktiviteter", "#942a52", "human_activities"],
      ["reached", "Nådda", "#2b6f8c", "reach"],
      ["positive", "Positiva", "#19704b", "positive_sync"],
      ["mature_converted_contacts", "Konverterade", "#b7791f", "order_10d_sync"],
    ];
    const max = Math.max(1, ...rows.flatMap(row => series.map(([key]) => Number(row[key] || 0))));
    const x = index => pad + (rows.length === 1 ? (width - pad * 2) / 2 : index * (width - pad * 2) / (rows.length - 1));
    const y = value => height - pad - Number(value || 0) / max * (height - pad * 2);
    const lines = series.map(([key, label, color]) => {
      const points = rows.map((row, index) => `${x(index)},${y(row[key])}`).join(" ");
      const metric = series.find(item => item[0] === key)[3];
      const circles = rows.map((row, index) => `<circle class="sc-trend-point" cx="${x(index)}" cy="${y(row[key])}" r="5" fill="${color}" tabindex="0" role="button" data-drilldown="${metric}" data-start="${escapeHtml(row.period?.start || "")}" data-end="${escapeHtml(row.period?.end || "")}"><title>${escapeHtml(row.week)} ${label}: ${number(row[key])}${row.incomplete ? " (pågående vecka)" : ""}</title></circle>`).join("");
      return `<polyline class="sc-trend-line" stroke="${color}" points="${points}"></polyline>${circles}`;
    }).join("");
    const grid = [0, .25, .5, .75, 1].map(part => `<line class="sc-trend-grid" x1="${pad}" x2="${width - pad}" y1="${y(max * part)}" y2="${y(max * part)}"></line>`).join("");
    const labels = rows.map((row, index) => `<text x="${x(index)}" y="${height - 8}" text-anchor="middle" font-size="11" fill="#746772">${escapeHtml(row.week.replace(/^\d{4}-/, ""))}${row.incomplete ? "*" : ""}</text>`).join("");
    return `<div><div class="sc-section-heading"><div><h2>Veckotrend</h2><p>* Pågående vecka är ofullständig.</p></div></div><div class="sc-trend-wrap"><svg class="sc-trend" viewBox="0 0 ${width} ${height}" role="img" aria-label="Veckotrend för aktiviteter, nådda, positiva och konverterade kontakter">${grid}${lines}${labels}</svg></div><div class="sc-trend-legend">${series.map(([, label, color]) => `<span><i class="sc-legend-dot" style="background:${color}"></i>${label}</span>`).join("")}</div></div>`;
  }

  function visitMarkup(data) {
    const cards = [
      ["Bom-ratio", percent(data.bom_ratio.value), rateEvidence(data.bom_ratio), "bom_ratio"],
      ["Träffgrad för besök", percent(data.reach.value), rateEvidence(data.reach), "reach", "visit"],
      ["Återkommande bommar", number(data.repeat_boms.customers), `${number(data.repeat_boms.visits)} besök`, "repeat_boms"],
      ["Högprioriterade bommar", number(data.high_priority_boms), `${number(data.high_priority_score_fallback)} score-fallback`, "high_priority_boms"],
      ["Planerade besök", percent(data.planned.value), rateEvidence(data.planned), "bom_ratio"],
      ["Oplanerade besök", percent(data.unplanned.value), rateEvidence(data.unplanned), "bom_ratio"],
    ];
    const patterns = [...(data.weekday_patterns || []), ...(data.time_band_patterns || [])];
    return `<section class="sc-section" aria-labelledby="sc-visit-title"><div class="sc-section-heading"><div><h2 id="sc-visit-title">Besökseffektivitet</h2><p>Bom-ratio visas som procent och x av y. Mönster kräver minst tio besök.</p></div></div><div class="sc-card-grid">${cards.map(([label, value, evidence, metric, channel]) => `<button type="button" class="sc-mini-card" data-drilldown="${metric}"${channel ? ` data-channel="${channel}"` : ""} style="text-align:left;color:inherit;font:inherit;cursor:pointer"><div class="sc-mini-label">${label}</div><div class="sc-mini-value">${value}</div><div class="sc-mini-evidence">${evidence}</div></button>`).join("")}</div>${patterns.length ? `<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Mönster</th><th>Bom-ratio</th><th>Underlag</th></tr></thead><tbody>${patterns.map(item => `<tr><td>${escapeHtml(item.label)}</td><td>${percent(item.bom_ratio.value)}</td><td>${rateEvidence(item.bom_ratio)}</td></tr>`).join("")}</tbody></table></div>` : `<div class="sc-insufficient">Inga veckodags- eller tidsgrupper har minst tio besök.</div>`}</section>`;
  }

  function channelMarkup(channels) {
    const labels = { visit: "Besök", phone: "Telefon", email: "Manuellt mejl" };
    return `<section class="sc-section" aria-labelledby="sc-channel-title"><div class="sc-section-heading"><div><h2 id="sc-channel-title">Kanalernas effektivitet</h2><p>Automatiserade CRM-mejl ingår inte.</p></div></div><div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Kanal</th><th>Aktiviteter</th><th>Nådda</th><th>Positiv</th><th>Order 10 dagar</th><th>Utfall</th></tr></thead><tbody>${Object.entries(channels || {}).map(([key, item]) => `<tr><td><button type="button" data-channel="${key}" data-drilldown="human_activities">${labels[key]}</button></td><td>${number(item.human_activities)}</td><td>${key === "email" ? "Ej tillämpligt" : `${percent(item.reach.value)} (${rateEvidence(item.reach)})`}</td><td>${percent(item.positive_dialogue.value)} (${rateEvidence(item.positive_dialogue)})</td><td>${percent(item.order_10d.value)} (${rateEvidence(item.order_10d)})</td><td>${number(item.attributed_orders)} order · ${number(item.dfp, 2)} DFP · ${orderValues(item.order_value_by_currency)}${item.median_days_to_order === null ? " · Median kräver 5 order" : ` · ${number(item.median_days_to_order, 1)} dagar median`}</td></tr>`).join("")}</tbody></table></div></section>`;
  }

  function priorityMarkup(data) {
    const gapRows = (data.priority_gap?.customers || []).slice(0, 12);
    return `<section class="sc-section" aria-labelledby="sc-priority-title"><div class="sc-section-heading"><div><h2 id="sc-priority-title">Prioritering och kundallokering</h2><p>Historiskt prioritetsfokus hålls isär från aktuella portföljmått.</p></div></div><div class="sc-card-grid"><div class="sc-mini-card"><div class="sc-mini-label">Historiskt prioritetsfokus</div><div class="sc-mini-value">${percent(data.priority_focus.value)}</div><div class="sc-mini-evidence">${rateEvidence(data.priority_focus)}</div></div><div class="sc-mini-card"><div class="sc-mini-label">Historisk percentiltäckning</div><div class="sc-mini-value">${percent(data.priority_percentile_coverage.value)}</div><div class="sc-mini-evidence">${rateEvidence(data.priority_percentile_coverage)}</div></div><div class="sc-mini-card"><div class="sc-mini-label">Strategisk täckning, aktuell portfölj</div><div class="sc-mini-value">${percent(data.strategic_coverage.value)}</div><div class="sc-mini-evidence">${rateEvidence(data.strategic_coverage)}</div></div><div class="sc-mini-card"><div class="sc-mini-label">Prioritetsgap, aktuell portfölj</div><div class="sc-mini-value">${number(data.priority_gap.count)}</div><div class="sc-mini-evidence">Högprioriterade utan kontakt</div></div></div>${gapRows.length ? `<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Kund</th><th>Säljare</th><th>Segment</th><th>Prioritet</th><th>Percentil</th></tr></thead><tbody>${gapRows.map(item => `<tr><td>${item.customer_id ? `<button type="button" data-customer-id="${escapeHtml(item.customer_id)}">${escapeHtml(item.customer)}</button>` : escapeHtml(item.customer)}</td><td>${escapeHtml(item.sales_user_name)}</td><td>${escapeHtml(item.segment)}</td><td>${number(item.priority_score, 1)}</td><td>${number(item.priority_percentile, 1)}</td></tr>`).join("")}</tbody></table></div>` : `<div class="sc-empty">Inget aktuellt prioritetsgap i valt filter.</div>`}</section>`;
  }

  function followupMarkup(data) {
    const cards = [
      ["Positiv kontakt med nästa steg/order", percent(data.positive_next_step_coverage.value), rateEvidence(data.positive_next_step_coverage), "followup_gap"],
      ["Positiva utan nästa steg", number(data.positive_without_next_step), "minst tre dagar gamla", "followup_gap"],
      ["Planerade genomförda i tid", percent(data.planned_completed_in_time.value), rateEvidence(data.planned_completed_in_time), "human_activities"],
      ["Försenade planerade", number(data.overdue_planned), "fortfarande öppna", "human_activities"],
      ["Skipped", number(data.skipped), `${number(data.cancelled_excluded)} cancelled exkluderade`, "human_activities"],
      ["Positiv utan order/uppföljning 10 dagar", number(data.positive_without_order_or_follow_up_10d), "mogen kontaktkohort", "followup_gap"],
    ];
    return `<section class="sc-section" aria-labelledby="sc-followup-title"><div class="sc-section-heading"><div><h2 id="sc-followup-title">Uppföljningsdisciplin</h2><p>Cancelled räknas inte som misslyckat genomförande.</p></div></div><div class="sc-card-grid">${cards.map(([label, value, evidence, metric]) => `<button type="button" class="sc-mini-card" data-drilldown="${metric}" style="text-align:left;color:inherit;font:inherit;cursor:pointer"><div class="sc-mini-label">${label}</div><div class="sc-mini-value">${value}</div><div class="sc-mini-evidence">${evidence}</div></button>`).join("")}</div></section>`;
  }

  function coachingMarkup(cards) {
    return `<section class="sc-section" aria-labelledby="sc-coaching-title"><div class="sc-section-heading"><div><h2 id="sc-coaching-title">Coachningskort</h2><p>Deterministiska regler, högst fyra kort.</p></div></div>${cards?.length ? `<div class="sc-coaching-grid">${cards.map(card => `<article class="sc-coaching-card" data-severity="${escapeHtml(card.severity)}"><span class="sc-coaching-code">${escapeHtml(card.code)}</span><h3>${escapeHtml(card.title)}</h3><p>${escapeHtml(card.diagnosis)}</p><p><strong>Bevis:</strong> ${card.evidence?.denominator !== undefined ? `${rateEvidence(card.evidence)} · ${percent(card.evidence.value)}` : number(card.evidence?.value)}</p><p><strong>Coachningsfråga:</strong> ${escapeHtml(card.recommendation)}</p><button type="button" data-drilldown="${escapeHtml(card.drilldown_metric)}" data-drilldown-filters="${escapeHtml(JSON.stringify(card.drilldown_filters || {}))}">Visa underlag</button></article>`).join("")}</div>` : `<div class="sc-empty">Inget coachningskort aktiveras med tillräckligt underlag i valt filter.</div>`}</section>`;
  }

  function renderDashboard(data) {
    renderSellerOptions(data.options?.sellers);
    const target = document.getElementById("sc-dashboard-content");
    target.removeAttribute("aria-busy");
    target.innerHTML = [
      qualityMarkup(data.data_quality || {}),
      kpisMarkup(data.kpis || {}),
      state.filters.seller ? "" : matrixMarkup(data.coaching_matrix || { sellers: [], insufficient_sample: [], medians: {} }),
      `<section class="sc-section"><div class="sc-two-column">${funnelMarkup(data.funnel || {})}${trendMarkup(data.weekly_trend || [])}</div></section>`,
      visitMarkup(data.visit_efficiency || {}),
      channelMarkup(data.channel_effectiveness || {}),
      priorityMarkup(data.priority_allocation || {}),
      followupMarkup(data.follow_up_discipline || {}),
      coachingMarkup(data.coaching_cards || []),
    ].join("");
  }

  function handleDashboardClick(event) {
    const retry = event.target.closest('[data-sc-action="retry"]');
    if (retry) return void loadSummary();
    const customer = event.target.closest("[data-customer-id]");
    if (customer) {
      window.dispatchEvent(new CustomEvent("sales-coaching:open-customer", { detail: { customerId: customer.dataset.customerId } }));
      return;
    }
    const seller = event.target.closest("[data-seller]");
    if (seller) {
      state.filters.seller = seller.dataset.seller;
      setControlValues();
      updateUrl();
      loadSummary();
      return;
    }
    const drilldown = event.target.closest("[data-drilldown]");
    if (!drilldown) return;
    let extra = {};
    if (drilldown.dataset.channel) extra.channel = drilldown.dataset.channel;
    if (drilldown.dataset.start) extra.start = drilldown.dataset.start;
    if (drilldown.dataset.end) extra.end = drilldown.dataset.end;
    if (drilldown.dataset.drilldownFilters) {
      try { extra = { ...extra, ...JSON.parse(drilldown.dataset.drilldownFilters) }; } catch (_) {}
    }
    openDrilldown(drilldown.dataset.drilldown, extra, drilldown);
  }

  function drawerMarkup(metric) {
    return `<div class="sc-drawer-backdrop" id="sc-drawer-backdrop"><aside class="sc-drawer" role="dialog" aria-modal="true" aria-labelledby="sc-drawer-title"><div class="sc-drawer-header"><h2 id="sc-drawer-title">Underlag: ${escapeHtml(metric)}</h2><button type="button" class="sc-drawer-close" data-sc-drawer-close aria-label="Stäng underlag">×</button></div><div id="sc-drawer-content" aria-live="polite"><div class="sc-skeleton"></div></div></aside></div>`;
  }

  async function openDrilldown(metric, extra = {}, trigger = null) {
    if (!metric) return;
    closeDrawer();
    state.lastFocus = trigger || document.activeElement;
    document.body.insertAdjacentHTML("beforeend", drawerMarkup(metric));
    const backdrop = document.getElementById("sc-drawer-backdrop");
    backdrop.addEventListener("click", event => {
      if (event.target === backdrop || event.target.closest("[data-sc-drawer-close]")) closeDrawer();
    });
    backdrop.querySelector(".sc-drawer-close").focus();
    state.drawerController = new AbortController();
    try {
      const response = await fetch(`/sales-coaching-insights/drilldown?${queryString({ ...extra, metric, limit: 200 })}`, { signal: state.drawerController.signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      renderDrilldown(await response.json());
    } catch (error) {
      if (error.name !== "AbortError" && document.getElementById("sc-drawer-content")) {
        document.getElementById("sc-drawer-content").innerHTML = `<div class="sc-error" role="alert">Underlaget kunde inte laddas.</div>`;
      }
    }
  }

  function renderDrilldown(data) {
    const target = document.getElementById("sc-drawer-content");
    if (!target) return;
    const rows = data.rows || [];
    target.innerHTML = `<div class="sc-drawer-meta">Visar ${number(rows.length)} av ${number(data.total_count)}, maximalt ${number(data.limit)} rader.</div>${rows.length ? `<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Datum</th><th>Säljare</th><th>Kund</th><th>Kanal/resultat</th><th>Prioritet</th><th>Orderutfall</th></tr></thead><tbody>${rows.map(row => `<tr><td>${escapeHtml(row.date_time || "")}</td><td>${escapeHtml(row.sales_user_name || "")}</td><td>${row.customer_id ? `<button type="button" data-customer-id="${escapeHtml(row.customer_id)}">${escapeHtml(row.customer || "")}</button>` : escapeHtml(row.customer || "")}</td><td>${escapeHtml(row.channel || "")} · ${escapeHtml(row.result_class || "")}</td><td>${row.priority_percentile_at_contact === null ? "Saknas" : `${number(row.priority_percentile_at_contact, 1)} pct`} · ${escapeHtml(row.snapshot_quality || "missing")}</td><td>${row.order_reference ? `${escapeHtml(row.order_reference)} · ${number(row.days_to_order)} dagar · ${number(row.dfp, 2)} DFP` : "—"}</td></tr>`).join("")}</tbody></table></div>` : `<div class="sc-empty">Inga rader matchar underlaget.</div>`}`;
    target.addEventListener("click", event => {
      const customer = event.target.closest("[data-customer-id]");
      if (!customer) return;
      closeDrawer();
      window.dispatchEvent(new CustomEvent("sales-coaching:open-customer", { detail: { customerId: customer.dataset.customerId } }));
    });
  }

  function closeDrawer() {
    state.drawerController?.abort();
    state.drawerController = null;
    document.getElementById("sc-drawer-backdrop")?.remove();
    if (state.lastFocus?.focus) state.lastFocus.focus();
    state.lastFocus = null;
  }

  root.innerHTML = filterMarkup();
  hydrateFiltersFromUrl();
  state.pendingInitialMode = state.mode;
  setControlValues();
  bindControls();
  setAdmin(document.body.classList.contains("user-admin"));
  setMode(state.mode, { update: false });

  window.salesCoachingDashboard = Object.freeze({
    setAdmin,
    open: () => setMode("coaching"),
    close: () => setMode("business"),
    reload: loadSummary,
  });
})();
