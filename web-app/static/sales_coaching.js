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
    teamTrendView: "order",
    diagnosticTab: "visits",
    pendingInitialMode: "business",
    filters: defaultFilters(),
  };
  const MATRIX_TICKS = [0, 25, 50, 75, 100];
  const TEAM_TREND_COLORS = ["#942a52", "#176b87", "#2f7a4f", "#a66012", "#6657a4", "#a23b32", "#39738f", "#6d6a25"];
  const TEAM_TREND_DASHES = ["", "10 5", "3 4", "13 4 3 4", "7 3", "2 3 9 3", "15 5", "5 3 2 3"];
  const STANDARD_PERIODS = ["1", "2", "4", "8", "12"];
  const ADVANCED_TABS = ["visits", "followup", "channels"];
  const VISIBLE_METRIC_DEFINITION_KEYS = Object.freeze([
    "human_activities", "unique_customers", "visits", "reached_visits",
    "bom_visits", "phone", "manual_email", "reach", "visit_reach",
    "positive_dialogue", "positive_to_order_10d", "order_10d",
    "bom_ratio", "planned_bom_ratio", "unplanned_bom_ratio", "repeat_boms",
    "high_priority_boms", "positive_next_step_coverage",
    "positive_without_next_step", "positive_without_order_or_follow_up_10d",
    "planned_completed_in_time", "overdue_planned", "skipped_planned",
    "cancelled_excluded", "priority_focus", "priority_percentile_coverage",
    "historical_priority_at_contact", "attributed_orders",
    "median_days_to_order", "days_to_order", "attributed_order_dfp",
    "attributed_order_value", "waiting_outcome", "secure_customer_identity",
    "order_attribution_identity_coverage", "standardized_activity",
    "flagged_activity_rows", "quality_issue_count", "exact_snapshots",
    "late_snapshots", "operationally_suppressed",
  ]);
  const DRILLDOWN_DEFINITION_KEYS = Object.freeze({
    attempts: "reach",
    human_activities: "human_activities",
    reach: "reach",
    positive_dialogue: "positive_dialogue",
    positive_to_order_10d: "positive_to_order_10d",
    order_10d: "order_10d",
    repeat_boms: "repeat_boms",
    high_priority_boms: "high_priority_boms",
    planned_boms: "planned_bom_ratio",
    unplanned_boms: "unplanned_bom_ratio",
    followup_success: "positive_next_step_coverage",
    followup_gap: "positive_without_next_step",
    followup_gap_10d: "positive_without_order_or_follow_up_10d",
    planned_on_time: "planned_completed_in_time",
    planned_overdue: "overdue_planned",
    planned_skipped: "skipped_planned",
    priority_focus: "priority_focus",
    waiting_outcome: "waiting_outcome",
    data_quality: "flagged_activity_rows",
  });

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

  function metricDefinition(key, fallback = {}) {
    return state.data?.metric_definitions?.[key] || fallback || {};
  }

  function metricDefinitionText(definition = {}) {
    const copy = definition.definition || "";
    if (definition.metric_type !== "rate" || !definition.numerator_label || !definition.denominator_label) {
      return copy;
    }
    const assessmentMinimum = Number(definition.assessment_minimum);
    const assessmentRule = Number.isFinite(assessmentMinimum) && assessmentMinimum > 0
      ? ` Måttet bedöms först när nämnaren är minst ${number(assessmentMinimum)}.`
      : "";
    return `${copy} Täljaren är ${definition.numerator_label} och nämnaren är ${definition.denominator_label}.${assessmentRule}`;
  }

  function metricLabel(key, fallback) {
    return metricDefinition(key)?.label || fallback;
  }

  function definitionParts(key, context, fallback = {}, buttonClass = "sc-metric-info") {
    const definition = metricDefinition(key, fallback);
    const definitionText = metricDefinitionText(definition);
    if (!definitionText) return { button: "", explanation: "" };
    const safeContext = String(context || key).replace(/[^a-z0-9_-]+/gi, "-");
    const explanationId = `sc-metric-explanation-${safeContext}-${key}`;
    const label = definition.label || fallback.label || key;
    return {
      button: `<button type="button" class="${buttonClass}" data-sc-action="metric-info" aria-expanded="false" aria-controls="${explanationId}" title="${escapeHtml(definitionText)}" aria-label="Förklaring för ${escapeHtml(label)}">i</button>`,
      explanation: `<span class="sc-metric-explanation" id="${explanationId}" hidden>${escapeHtml(definitionText)}</span>`,
    };
  }

  function miniMetricCard({ context, label, value, evidence = "", definitionKey = "", secondaryDefinitionKey = "", drilldownMetric = "", channel = "" }) {
    const info = definitionParts(definitionKey, context);
    const content = `<span class="sc-mini-label">${escapeHtml(label)}</span><span class="sc-mini-value">${value}</span>${evidence ? `<span class="sc-mini-evidence">${evidence}</span>` : ""}`;
    const main = drilldownMetric
      ? `<button type="button" class="sc-mini-main" data-drilldown="${escapeHtml(drilldownMetric)}"${definitionKey ? ` data-metric-definition="${escapeHtml(definitionKey)}"` : ""}${channel ? ` data-channel="${escapeHtml(channel)}"` : ""}>${content}</button>`
      : `<div class="sc-mini-main">${content}</div>`;
    return `<article class="sc-mini-card"${definitionKey ? ` data-metric-definition="${escapeHtml(definitionKey)}"` : ""}${secondaryDefinitionKey ? ` data-secondary-metric-definition="${escapeHtml(secondaryDefinitionKey)}"` : ""}>${main}${info.button}${info.explanation}</article>`;
  }

  function metricHeader(label, key, context) {
    const info = definitionParts(key, context, {}, "sc-metric-info sc-table-info");
    return `<span class="sc-table-metric-label">${escapeHtml(label)}${info.button}${info.explanation}</span>`;
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
      limited_coverage: "Otillräcklig historisk täckning",
      building: "Data byggs upp",
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
    const countMetric = metric?.metric_type === "count";
    const previousSuppressed = comparisons.previous_period_suppressed_reason === "pending_10d_outcomes";
    const previous = comparisons.previous_period;
    const previousValue = previous && typeof previous === "object" ? previous.value : previous?.value ?? previous;
    const formatValue = value => countMetric ? `${number(value, 1)} aktiviteter` : percent(value);
    const formatDelta = value => countMetric
      ? `${value >= 0 ? "+" : ""}${number(value, 1)} aktiviteter`
      : `${value >= 0 ? "+" : ""}${number(value * 100, 1)} pp`;
    const parts = [];
    if (comparisons.peer_median !== null && comparisons.peer_median !== undefined) {
      const delta = comparisons.delta_peer;
      parts.push(`Median övriga säljare ${formatValue(comparisons.peer_median)}${delta === null || delta === undefined ? "" : ` · ${formatDelta(delta)}`}`);
    }
    if (!previousSuppressed && previousValue !== null && previousValue !== undefined) {
      const delta = comparisons.delta_previous;
      parts.push(`Föregående period ${formatValue(previousValue)}${delta === null || delta === undefined ? "" : ` · ${formatDelta(delta)}`}`);
    } else if (!previousSuppressed && comparisons.previous_period_status && comparisons.previous_period_status !== "sufficient") {
      parts.push(`Föregående period: ${statusLabel(comparisons.previous_period_status)}`);
    }
    return parts.join(" · ");
  }

  function definitionEntry(key) {
    const definition = metricDefinition(key);
    return definition?.definition ? definition : null;
  }

  function metricName(key, label, className = "") {
    const classes = className ? ` class="${escapeHtml(className)}"` : "";
    return `<span${classes} data-metric-definition="${escapeHtml(key)}">${escapeHtml(label)}</span>`;
  }

  function periodRange(weeks, end = new Date()) {
    const rangeEnd = new Date(end);
    const start = new Date(rangeEnd);
    start.setDate(start.getDate() - Number(weeks) * 7 + 1);
    return { start: dateKey(start), end: dateKey(rangeEnd) };
  }

  function rangeMatchesPeriod(start, end, weeks) {
    if (!start || !end || !STANDARD_PERIODS.includes(weeks)) return false;
    const expected = periodRange(weeks);
    return start === expected.start && end === expected.end;
  }

  function filterMarkup() {
    return `
      <form class="sc-filter-bar" id="sc-filter-form" aria-label="Filter för säljcoachning">
        <div class="sc-filter-primary">
          <div class="sc-field">
            <label for="sc-period">Period</label>
            <select id="sc-period" name="period">
              <option value="1">1 vecka</option><option value="2">2 veckor</option>
              <option value="4">4 veckor</option><option value="8">8 veckor</option>
              <option value="12">12 veckor</option><option value="custom">Anpassad period</option>
            </select>
          </div>
          <div class="sc-field">
            <label for="sc-seller">Säljare</label>
            <select id="sc-seller" name="seller"><option value="">Teamet</option></select>
          </div>
          <button type="button" class="sc-more-filters-toggle" id="sc-more-filters-toggle" aria-expanded="false" aria-controls="sc-more-filters-panel">
            <span id="sc-more-filters-label">Fler filter</span><span class="sc-disclosure-icon" aria-hidden="true"></span>
          </button>
        </div>
        <div class="sc-custom-dates" id="sc-custom-dates" hidden>
          <div class="sc-field"><label for="sc-start">Från</label><input id="sc-start" name="start" type="date" /></div>
          <div class="sc-field"><label for="sc-end">Till</label><input id="sc-end" name="end" type="date" /></div>
        </div>
        <div class="sc-more-filters-panel" id="sc-more-filters-panel" hidden>
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
    const requestedPeriod = params.get("period");
    if (requestedPeriod === "custom" || (!requestedPeriod && (params.has("start") || params.has("end")))) {
      state.filters.period = "custom";
    } else if (STANDARD_PERIODS.includes(requestedPeriod)) {
      const hasExplicitRange = params.has("start") || params.has("end");
      state.filters.period = hasExplicitRange && !rangeMatchesPeriod(state.filters.start, state.filters.end, requestedPeriod)
        ? "custom"
        : requestedPeriod;
      if (!hasExplicitRange) setPeriod(requestedPeriod);
    }
  }

  function updateMoreFiltersControl() {
    const activeCount = ["channel", "lifecycle", "segment"]
      .filter(key => state.filters[key] && state.filters[key] !== "all").length;
    const toggle = document.getElementById("sc-more-filters-toggle");
    const label = document.getElementById("sc-more-filters-label");
    if (!toggle || !label) return;
    label.textContent = activeCount ? `Fler filter · ${activeCount} ${activeCount === 1 ? "aktivt" : "aktiva"}` : "Fler filter";
    toggle.classList.toggle("is-active", activeCount > 0);
  }

  function setMoreFiltersOpen(open) {
    const toggle = document.getElementById("sc-more-filters-toggle");
    const panel = document.getElementById("sc-more-filters-panel");
    if (!toggle || !panel) return;
    toggle.setAttribute("aria-expanded", String(Boolean(open)));
    panel.hidden = !open;
  }

  function setControlValues() {
    for (const key of ["period", "seller", "channel", "lifecycle", "segment", "start", "end"]) {
      const control = document.getElementById(`sc-${key}`);
      if (control) control.value = state.filters[key];
    }
    document.getElementById("sc-custom-dates").hidden = state.filters.period !== "custom";
    updateMoreFiltersControl();
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
    const range = periodRange(weeks);
    state.filters.end = range.end;
    state.filters.start = range.start;
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
    for (const key of ["seller", "channel", "lifecycle", "segment"]) {
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
    document.getElementById("sc-more-filters-toggle").addEventListener("click", event => {
      setMoreFiltersOpen(event.currentTarget.getAttribute("aria-expanded") !== "true");
    });
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
      const teamTrendTab = event.target.closest("[data-team-trend-view]");
      if (teamTrendTab && ["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
        event.preventDefault();
        const keys = ["order", "positive"];
        const current = keys.indexOf(teamTrendTab.dataset.teamTrendView);
        const next = event.key === "Home" ? 0 : event.key === "End" ? keys.length - 1 : (current + (event.key === "ArrowRight" ? 1 : -1) + keys.length) % keys.length;
        state.teamTrendView = keys[next];
        if (state.data) renderDashboard(state.data);
        document.getElementById(`sc-team-trend-tab-${keys[next]}`)?.focus();
        return;
      }
      const diagnosticTab = event.target.closest("[data-diagnostic-tab]");
      if (diagnosticTab && ["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
        event.preventDefault();
        const current = ADVANCED_TABS.indexOf(diagnosticTab.dataset.diagnosticTab);
        const next = event.key === "Home" ? 0 : event.key === "End" ? ADVANCED_TABS.length - 1 : (current + (event.key === "ArrowRight" ? 1 : -1) + ADVANCED_TABS.length) % ADVANCED_TABS.length;
        state.diagnosticTab = ADVANCED_TABS[next];
        if (state.data) renderDashboard(state.data);
        const advanced = document.getElementById("sc-advanced-analysis");
        if (advanced) advanced.open = true;
        document.getElementById(`sc-diagnostic-tab-${ADVANCED_TABS[next]}`)?.focus();
        return;
      }
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
    select.innerHTML = `<option value="">Teamet</option>${(options || []).map(value => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join("")}`;
    select.value = selected;
  }

  function metricDefinitionsMarkup() {
    const rows = VISIBLE_METRIC_DEFINITION_KEYS.map(key => {
      const definition = definitionEntry(key);
      if (!definition?.label || !definition?.definition) return "";
      return `<div class="sc-definition" data-metric-definition="${escapeHtml(key)}"><dt>${escapeHtml(definition.label)}</dt><dd>${escapeHtml(metricDefinitionText(definition))}</dd></div>`;
    }).join("");
    return `<div class="sc-glossary"><h3>Ordlista över mått</h3><dl>${rows}</dl></div>`;
  }

  function qualityMetric(key, value, evidence = "") {
    const definition = definitionEntry(key);
    return `<li data-metric-definition="${escapeHtml(key)}"><span>${escapeHtml(definition?.label || "")}</span><strong>${value}</strong>${evidence ? `<small>${evidence}</small>` : ""}</li>`;
  }

  function dataQualityDetailsMarkup(quality) {
    const core = quality.core_analytics || {};
    const history = quality.historical_priority || {};
    const comparable = history.comparable_percentile_rate || history.priority_percentile_coverage || quality.priority_percentile_coverage || {};
    const exact = history.exact_snapshot_rate || history.snapshot_coverage || {};
    return `<section class="sc-section sc-details-section" aria-labelledby="sc-details-title"><details id="sc-quality-details"><summary id="sc-details-title"><span>Datakvalitet och definitioner</span><span class="sc-disclosure-icon" aria-hidden="true"></span></summary><div class="sc-details-content"><div class="sc-details-grid"><div><h3>Datakvalitet och täckning</h3><ul class="sc-quality-metrics">${qualityMetric("secure_customer_identity", percent(core.secure_customer_identity?.value), rateEvidence(core.secure_customer_identity))}${qualityMetric("order_attribution_identity_coverage", percent(core.order_attribution_identity_coverage?.value), rateEvidence(core.order_attribution_identity_coverage))}${qualityMetric("standardized_activity", percent(core.standardized_activity?.value), rateEvidence(core.standardized_activity))}${qualityMetric("flagged_activity_rows", number(quality.core_flagged_activity_rows))}${qualityMetric("quality_issue_count", number(quality.quality_issue_count))}${qualityMetric("waiting_outcome", number(quality.waiting_outcome_count))}</ul><button type="button" data-drilldown="data_quality" data-metric-definition="flagged_activity_rows">Visa kvalitetsunderlag</button></div><div><h3>Historisk analysmognad</h3><ul class="sc-quality-metrics">${qualityMetric("priority_percentile_coverage", percent(comparable.value), rateEvidence(comparable))}${qualityMetric("exact_snapshots", number(history.exact_snapshot_count), rateEvidence(exact))}${qualityMetric("late_snapshots", number(history.late_snapshot_count))}${qualityMetric("operationally_suppressed", number(history.operationally_suppressed_count))}</ul><p>Kontaktmått använder de kundvärden som sparades vid kontakten. Planeringsmått använder kundens aktuella värden. Operativa undantag är inte i sig datakvalitetsfel.</p></div></div>${metricDefinitionsMarkup()}</div></details></section>`;
  }

  function kpiMarkup(key, metric) {
    const isRate = metric.denominator !== undefined;
    const value = isRate ? percent(metric.value) : number(metric.value);
    const definition = metricDefinition(key, metric);
    const info = definitionParts(key, `kpi-${key}`, metric, "sc-kpi-info sc-metric-info");
    const selectedChannelUnavailable = state.filters.channel !== "all"
      && metric.status === "not_computable"
      && Array.isArray(definition.channels)
      && !definition.channels.includes(state.filters.channel);
    const evidence = isRate
      ? selectedChannelUnavailable
        ? `<span class="sc-kpi-evidence">${escapeHtml(definition.not_computable_text || statusLabel(metric.status))}</span>`
        : `<span class="sc-kpi-evidence"><span>${rateEvidence(metric)}</span><span class="sc-kpi-denominator">${escapeHtml(definition.denominator_label || "kontakter")}</span></span>`
      : "";
    const status = metric.status === "small_sample"
      ? '<span class="sc-status">Inte tillräckligt underlag</span>'
      : "";
    let secondary = "";
    if (key === "human_activities") {
      secondary = `${metricName("unique_customers", "Unika kunder")} ${number(metric.unique_customers)} · ${metricName("visits", "Besök")} ${number(metric.channel_mix?.visit)} · ${metricName("phone", "Telefon")} ${number(metric.channel_mix?.phone)} · ${metricName("manual_email", "Manuellt mejl")} ${number(metric.channel_mix?.email)}`;
    }
    if (["positive_to_order_10d", "order_10d"].includes(key) && !selectedChannelUnavailable) {
      secondary = Number(metric.waiting_outcome_count) > 0
        ? `Preliminärt · ${number(metric.waiting_outcome_count)} väntar på 10-dagarsutfall`
        : "";
    }
    return `
      <article class="sc-kpi-card" data-kpi-key="${escapeHtml(key)}" data-metric-definition="${escapeHtml(key)}">
        <button type="button" class="sc-kpi-main" data-drilldown="${escapeHtml(metric.drilldown_metric)}" data-metric-definition="${escapeHtml(key)}" aria-label="${escapeHtml(metric.label)}: ${value}">
          <span class="sc-kpi-header"><span class="sc-kpi-label">${escapeHtml(metric.label)}</span></span>
          <span class="sc-kpi-value">${value}</span>
          ${evidence}
          ${status}
          <span class="sc-kpi-comparison">${escapeHtml(comparisonText(metric))}</span>
          ${secondary ? `<span class="sc-kpi-secondary">${secondary}</span>` : ""}
        </button>
        ${info.button}
        ${info.explanation}
      </article>`;
  }

  function kpisMarkup(kpis) {
    const order = ["human_activities", "reach", "positive_dialogue", "positive_to_order_10d", "order_10d"];
    return `<section class="sc-section" aria-labelledby="sc-kpi-title"><div class="sc-section-heading"><div><h2 id="sc-kpi-title">Coachningsöversikt</h2><p>Procentsatser bedöms först när underlaget är minst 10.</p></div></div><div class="sc-kpi-grid">${order.map(key => kpiMarkup(key, kpis[key])).join("")}</div></section>`;
  }

  function sellerSelected(seller) {
    return state.filters.seller && state.filters.seller === seller;
  }

  function teamComparisonMarkup(team) {
    const sellers = team.sellers || [];
    const activityMax = Math.max(1, ...sellers.flatMap(item => [item.visit_breakdown?.analysable || 0, item.channel_mix?.phone || 0, item.channel_mix?.email || 0]));
    const activityGroups = sellers.map(item => {
      const visits = Number(item.visit_breakdown?.analysable || 0);
      const reachedVisits = Number(item.visit_breakdown?.reached || 0);
      const boms = Number(item.visit_breakdown?.boms || 0);
      const title = `${item.seller}: totalt ${number(item.human_activities_total)}, Besök ${number(visits)} · varav ${number(boms)} bom, nådda besök ${number(reachedVisits)}, Telefon ${number(item.channel_mix?.phone)}, manuellt mejl ${number(item.channel_mix?.email)}`;
      return `<button type="button" class="sc-team-group${sellerSelected(item.seller) ? " is-selected" : ""}" data-seller="${escapeHtml(item.seller)}" aria-label="${escapeHtml(title)}" title="${escapeHtml(title)}"><span class="sc-team-total">Totalt ${number(item.human_activities_total)}</span><span class="sc-team-bars"><i class="sc-team-bar is-visit-stack" style="height:${visits / activityMax * 100}%"><b>${number(visits)}</b><span class="sc-team-bar-segment is-visit-reached" style="flex:${reachedVisits}" aria-hidden="true"></span><span class="sc-team-bar-segment is-visit-bom" style="flex:${boms}" aria-hidden="true"></span></i><i class="sc-team-bar is-phone" style="height:${Number(item.channel_mix?.phone || 0) / activityMax * 100}%"><b>${number(item.channel_mix?.phone)}</b></i><i class="sc-team-bar is-email" style="height:${Number(item.channel_mix?.email || 0) / activityMax * 100}%"><b>${number(item.channel_mix?.email)}</b></i></span><span class="sc-team-seller">${escapeHtml(item.seller)}</span></button>`;
    }).join("");
    const rateCell = (metric, pending = 0) => `<span class="sc-rate-value">${percent(metric?.value)}</span><small>${rateEvidence(metric)} · ${statusLabel(metric?.status)}</small>${Number(pending) > 0 ? `<small class="sc-rate-pending">Preliminärt · ${number(pending)} väntar på 10-dagarsutfall</small>` : ""}${comparisonText(metric) ? `<small>${escapeHtml(comparisonText(metric))}</small>` : ""}`;
    const rows = sellers.map(item => `<tr${sellerSelected(item.seller) ? ' class="is-selected"' : ""}><th><button type="button" data-seller="${escapeHtml(item.seller)}">${escapeHtml(item.seller)}</button></th><td>${number(item.human_activities_total)}</td><td>${rateCell(item.reach)}</td><td>${rateCell(item.positive_dialogue)}</td><td>${rateCell(item.positive_to_order_10d, item.waiting_positive_dialogues_count)}</td><td>${rateCell(item.order_10d, item.waiting_outcome_count)}</td><td>${rateCell(item.positive_next_step_coverage)}</td><td>${rateCell(item.bom_ratio)}</td></tr>`).join("");
    return `<section class="sc-section" aria-labelledby="sc-team-title"><div class="sc-section-heading"><div><h2 id="sc-team-title">Teamjämförelse</h2><p>10-dagarsmåtten använder samma definition som i Coachningsöversikten. Kontakter vars 10-dagarsfönster fortfarande är öppet ingår i nämnaren, därför markeras måttet som preliminärt när utfall fortfarande väntar. Jämförelsen gäller alla kanaler under vald period, lifecycle och segment; kanal- och säljarfilter påverkar inte teamblocken.</p></div></div><article class="sc-team-chart"><h3>Mänskliga aktiviteter</h3><p>Besök är stackade: <span class="sc-legend-key is-visit">nådda besök</span> + <span class="sc-legend-key is-bom">bom</span>. <span class="sc-legend-key is-phone">Telefon</span> och manuellt mejl visas separat.</p><div class="sc-team-plot">${activityGroups}</div></article><div class="sc-table-wrap"><table class="sc-table sc-comparison-table"><thead><tr><th>Säljare</th><th>Aktiviteter</th><th>${metricHeader("Träffgrad", "reach", "team-reach")}</th><th>${metricHeader("Positiv dialog", "positive_dialogue", "team-positive")}</th><th>${metricHeader(metricLabel("positive_to_order_10d", "Positiv dialog → order inom 10 dagar"), "positive_to_order_10d", "team-positive-order")}</th><th>${metricHeader(metricLabel("order_10d", "Kontakt – order inom 10 dagar"), "order_10d", "team-order")}</th><th>${metricHeader("Nästa-steg-täckning", "positive_next_step_coverage", "team-next-step")}</th><th>${metricHeader("Bom-ratio", "bom_ratio", "team-bom")}</th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
  }

  function teamTrendWeekLabel(week, previousWeek = "") {
    const match = String(week || "").match(/^(\d{4})-W(\d{2})$/);
    if (!match) return week || "";
    const previousYear = String(previousWeek || "").slice(0, 4);
    return !previousWeek || previousYear !== match[1]
      ? `${match[1]} v.${Number(match[2])}`
      : `v.${Number(match[2])}`;
  }

  function teamTrendStyle(seller) {
    const identity = String(seller || "").normalize("NFKC").trim().toLocaleLowerCase("sv-SE");
    let hash = 2166136261;
    for (const character of identity) {
      hash ^= character.codePointAt(0);
      hash = Math.imul(hash, 16777619);
    }
    const unsignedHash = hash >>> 0;
    const styleIndex = unsignedHash % TEAM_TREND_COLORS.length;
    return {
      color: TEAM_TREND_COLORS[styleIndex],
      dash: TEAM_TREND_DASHES[styleIndex],
      marker: styleIndex % 4,
      key: unsignedHash.toString(36),
    };
  }

  function teamTrendMarker(marker, x, y) {
    if (marker === 1) return `<rect class="sc-team-order-point-shape" x="${x - 5}" y="${y - 5}" width="10" height="10" rx="1"></rect>`;
    if (marker === 2) return `<path class="sc-team-order-point-shape" d="M ${x} ${y - 7} L ${x + 7} ${y} L ${x} ${y + 7} L ${x - 7} ${y} Z"></path>`;
    if (marker === 3) return `<path class="sc-team-order-point-shape" d="M ${x} ${y - 7} L ${x + 7} ${y + 6} L ${x - 7} ${y + 6} Z"></path>`;
    return `<circle class="sc-team-order-point-shape" cx="${x}" cy="${y}" r="5.5"></circle>`;
  }

  function teamTrendPanelMarkup(trends, view, config, activeView) {
    const trend = trends?.metrics?.[config.metricKey] || {};
    const series = trend?.series || [];
    const slots = trends?.week_axis?.length
      ? trends.week_axis
      : (series[0]?.points || []).map(point => ({ week: point.week, period: point.period }));
    const width = 1120, height = 360;
    const pad = { left: 62, right: 24, top: 24, bottom: 58 };
    const plotWidth = width - pad.left - pad.right;
    const plotHeight = height - pad.top - pad.bottom;
    const x = index => pad.left + (slots.length <= 1 ? plotWidth / 2 : index * plotWidth / (slots.length - 1));
    const y = value => pad.top + (1 - Number(value)) * plotHeight;
    const selectedSeller = String(trends?.selected_seller || "");
    const hasSelection = Boolean(selectedSeller);
    const isSelected = seller => hasSelection && String(seller).localeCompare(selectedSeller, "sv-SE", { sensitivity: "base" }) === 0;
    const ticks = [0, 0.25, 0.5, 0.75, 1];
    const grid = ticks.map(value => `<g aria-hidden="true"><line class="sc-team-order-grid" x1="${pad.left}" x2="${width - pad.right}" y1="${y(value)}" y2="${y(value)}"></line><text class="sc-team-order-y-label" x="${pad.left - 10}" y="${y(value) + 4}" text-anchor="end">${value * 100} %</text></g>`).join("");
    const xLabels = slots.map((slot, index) => `<text class="sc-team-order-x-label" x="${x(index)}" y="${height - 18}" text-anchor="middle">${escapeHtml(teamTrendWeekLabel(slot.week, slots[index - 1]?.week || ""))}</text>`).join("");

    const seriesMarkup = series.map(item => {
      const style = teamTrendStyle(item.seller);
      const selected = isSelected(item.seller);
      const selectionClass = selected ? " is-selected" : hasSelection ? " is-unselected" : "";
      const pointByWeek = new Map((item.points || []).map(point => [point.week, point]));
      const segments = [];
      let segment = [];
      slots.forEach((slot, index) => {
        const point = pointByWeek.get(slot.week);
        if (point?.value !== null && Number.isFinite(Number(point.value))) {
          segment.push(`${x(index)},${y(point.value)}`);
        } else {
          if (segment.length > 1) segments.push(segment);
          segment = [];
        }
      });
      if (segment.length > 1) segments.push(segment);
      const lines = segments.map(points => `<polyline class="sc-team-order-line${selectionClass}" style="stroke:${style.color}${style.dash ? `;stroke-dasharray:${style.dash}` : ""}" data-seller="${escapeHtml(item.seller)}" data-series-style="${style.key}" points="${points.join(" ")}"></polyline>`).join("");
      const points = slots.map((slot, index) => {
        const point = pointByWeek.get(slot.week);
        if (!point || point.value === null || !Number.isFinite(Number(point.value))) return "";
        const small = point.status === "small_sample";
        const evidence = `${number(point.numerator)} av ${number(point.denominator)} ${config.evidenceLabel}`;
        const period = `${point.period?.start || "—"}–${point.period?.end || "—"}`;
        const title = `${item.seller} · ${teamTrendWeekLabel(point.week)} · ${period}: ${percent(point.value)} · ${evidence} · ${small ? "Litet underlag" : "Tillräckligt underlag"}`;
        return `<g class="sc-team-order-point${small ? " is-small-sample" : ""}${selectionClass}" style="--series-color:${style.color}" tabindex="0" role="button" data-drilldown="${config.metricKey}" data-trend-view="${view}" data-seller="${escapeHtml(item.seller)}" data-series-style="${style.key}" data-week="${escapeHtml(point.week)}" data-numerator="${number(point.numerator)}" data-denominator="${number(point.denominator)}" data-channel="all" data-start="${escapeHtml(point.period?.start || "")}" data-end="${escapeHtml(point.period?.end || "")}" aria-label="${escapeHtml(title)}"><title>${escapeHtml(title)}</title><circle class="sc-team-order-point-hit" cx="${x(index)}" cy="${y(point.value)}" r="22"></circle>${teamTrendMarker(style.marker, x(index), y(point.value))}</g>`;
      }).join("");
      return `${lines}${points}`;
    }).join("");

    const legend = series.map(item => {
      const style = teamTrendStyle(item.seller);
      const selected = isSelected(item.seller);
      return `<button type="button" class="sc-team-order-legend-item${selected ? " is-selected" : hasSelection ? " is-unselected" : ""}" style="--series-color:${style.color}" data-trend-view="${view}" data-seller="${escapeHtml(item.seller)}" data-series-style="${style.key}" aria-label="Visa ${escapeHtml(item.seller)} som vald säljare"><svg viewBox="0 0 42 18" aria-hidden="true"><line x1="2" x2="40" y1="9" y2="9" style="stroke:${style.color}${style.dash ? `;stroke-dasharray:${style.dash}` : ""}"></line>${teamTrendMarker(style.marker, 21, 9)}</svg><span>${escapeHtml(item.seller)}</span></button>`;
    }).join("");

    return `<div class="sc-team-trend-panel" id="sc-team-trend-panel-${view}" role="tabpanel" aria-labelledby="sc-team-trend-tab-${view}"${activeView === view ? "" : " hidden"}><h3>${escapeHtml(config.title)}</h3><div class="sc-team-order-trend-wrap" tabindex="0" aria-label="Horisontellt rullningsbar trendgraf"><svg class="sc-team-order-trend" viewBox="0 0 ${width} ${height}" role="group" aria-label="Veckovis ${escapeHtml(config.title)} för aktiva säljare, fast skala 0 till 100 procent">${grid}${seriesMarkup}${xLabels}</svg></div><div class="sc-team-order-legend" aria-label="Säljarserier">${legend || `<span class="sc-empty">Inga aktiva säljare.</span>`}</div></div>`;
  }

  function teamTrendsMarkup(trends) {
    const view = state.teamTrendView === "positive" ? "positive" : "order";
    const tabs = [
      ["order", "Kontakt → order", {
        metricKey: "order_10d",
        title: "Kontakt – order inom 10 dagar",
        evidenceLabel: "kontakter har följts av order inom 10 dagar",
      }],
      ["positive", "Positiv dialog → order", {
        metricKey: "positive_to_order_10d",
        title: "Positiv dialog → order inom 10 dagar",
        evidenceLabel: "positiva dialoger har följts av order inom 10 dagar",
      }],
    ];
    const tabList = tabs.map(([key, label]) => `<button type="button" role="tab" id="sc-team-trend-tab-${key}" data-team-trend-view="${key}" aria-selected="${view === key}" aria-controls="sc-team-trend-panel-${key}" tabindex="${view === key ? "0" : "-1"}">${label}</button>`).join("");
    const panels = tabs.map(([key, _label, config]) => teamTrendPanelMarkup(trends, key, config, view)).join("");
    return `<section class="sc-section sc-team-10d-trend-section" aria-labelledby="sc-team-trend-title"><div class="sc-section-heading"><div><h2 id="sc-team-trend-title">10-dagarskonvertering – trend</h2><p>Varje punkt avser en kontaktvecka. Endast veckor där hela 10-dagarsfönstret har passerat visas, så veckopunkterna förändras inte enbart för att fler dagar passerar. Diagrammen använder samma KPI-definitioner som Coachningsöversikten.</p></div></div><p class="sc-team-order-filter-note">Period-, säljar- och kanalfilter begränsar inte grafen. Vald säljare markeras; lifecycle och segment följer filtren.</p><div class="sc-team-trend-tabs" role="tablist" aria-label="Välj 10-dagarstrend">${tabList}</div>${panels}</section>`;
  }

  function matrixReasonLabel(reason) {
    return ({
      order_denominator_zero: "inga berättigade kontakter för ordermåttet",
      priority_denominator_zero: "ingen sparad historisk percentil",
      order_sample_below_10: "färre än 10 berättigade kontakter",
      priority_sample_below_10: "färre än 10 kontakter med historisk percentil",
      priority_percentile_coverage_below_70: "percentiltäckning under 70 %",
    }[reason] || reason);
  }

  function priorityMatrixPanelMarkup(matrix) {
    const xKey = matrix.axes?.x?.key;
    const yKey = matrix.axes?.y?.key;
    const xLabel = matrix.axes?.x?.label || xKey || "X";
    const yLabel = matrix.axes?.y?.label || yKey || "Y";
    const xMedian = matrix.medians?.[xKey];
    const yMedian = matrix.medians?.[yKey];
    const insufficient = (matrix.insufficient_sample || []).map(item => `${escapeHtml(item.seller)} (${(item.reasons || []).map(matrixReasonLabel).join(", ")})`).join(" · ");
    if (!matrix.available) {
      return `<div class="sc-matrix-panel"><div class="sc-priority-build-up"><strong>Historisk prioriteringsdata byggs upp.</strong><br>${rateEvidence(matrix.build_up?.coverage)} nya kontakter har jämförbar historisk prioritet.<br>Matrisen aktiveras vid minst ${percent(matrix.build_up?.minimum_coverage)} täckning och två jämförbara säljare.</div></div>`;
    }
    const medianLines = `${xMedian === null || xMedian === undefined ? "" : `<span class="sc-matrix-median-x" style="left:${Number(xMedian) * 100}%" aria-hidden="true"></span>`}${yMedian === null || yMedian === undefined ? "" : `<span class="sc-matrix-median-y" style="bottom:${Number(yMedian) * 100}%" aria-hidden="true"></span>`}`;
    const gridLines = MATRIX_TICKS.map(tick => `<span class="sc-matrix-gridline is-vertical" style="left:${tick}%" aria-hidden="true"><span class="sc-matrix-x-tick">${tick} %</span></span><span class="sc-matrix-gridline is-horizontal" style="bottom:${tick}%" aria-hidden="true"><span class="sc-matrix-y-tick">${tick} %</span></span>`).join("");
    const occupied = new Map();
    const bubbles = [...(matrix.sellers || [])].sort((a, b) => String(a.seller).localeCompare(String(b.seller), "sv")).map(item => {
      const xRate = item[xKey];
      const yRate = item[yKey];
      const x = Math.max(0, Math.min(100, Number(xRate.value) * 100));
      const y = Math.max(0, Math.min(100, Number(yRate.value) * 100));
      const coordinate = `${x.toFixed(2)}:${y.toFixed(2)}`;
      const overlap = occupied.get(coordinate) || 0;
      occupied.set(coordinate, overlap + 1);
      const collisionOffsets = [[0, 0], [-8, -8], [8, -8], [-8, 8], [8, 8], [0, -12], [0, 12], [-12, 0], [12, 0]];
      const collisionOffset = collisionOffsets[overlap % collisionOffsets.length];
      const collisionRing = Math.floor(overlap / collisionOffsets.length);
      const offsetX = collisionOffset[0] * (collisionRing + 1);
      const offsetY = collisionOffset[1] * (collisionRing + 1);
      const coverage = `, historisk prioritetstäckning ${percent(item.priority_percentile_coverage?.value)}`;
      const pending = Number(xRate.waiting_outcome_count) > 0
        ? `, preliminärt: ${number(xRate.waiting_outcome_count)} väntar på 10-dagarsutfall`
        : "";
      const title = `${item.seller}: ${xLabel} ${percent(xRate.value)} (${rateEvidence(xRate)}, ${statusLabel(xRate.status)}${pending}), ${yLabel} ${percent(yRate.value)} (${rateEvidence(yRate)}, ${statusLabel(yRate.status)}), ${item.human_activities} mänskliga aktiviteter${coverage}`;
      return `<button type="button" class="sc-bubble${item.sample_status === "small_sample" ? " is-small-sample" : ""}${sellerSelected(item.seller) ? " is-selected" : ""}" style="left:${x}%;bottom:${y}%;--offset-x:${offsetX}px;--offset-y:${offsetY}px" data-seller="${escapeHtml(item.seller)}" title="${escapeHtml(title)}" aria-label="${escapeHtml(title)}">${escapeHtml(String(item.seller).slice(0, 2).toUpperCase())}</button>`;
    }).join("");
    const medianNotice = xMedian === null || xMedian === undefined || yMedian === null || yMedian === undefined
      ? `<div class="sc-insufficient">Otillräckligt jämförbart underlag för ${xMedian === null || xMedian === undefined ? xLabel : yLabel}-median. Ingen fiktiv medianlinje visas.</div>`
      : "";
    return `<div class="sc-matrix-panel"><div class="sc-matrix-wrap"><div class="sc-matrix-layout"><div class="sc-matrix-y-axis-label">${escapeHtml(yLabel)}</div><div class="sc-matrix" role="img" aria-label="${escapeHtml(xLabel)} mot ${escapeHtml(yLabel)}, skala 0 till 100 procent. Punktstorleken är fast; aktivitetsvolym finns i tooltip."><div class="sc-matrix-inner">${gridLines}${medianLines}${bubbles}</div></div><span aria-hidden="true"></span><div class="sc-matrix-x-axis-label">${escapeHtml(xLabel)}</div></div></div>${medianNotice}${insufficient ? `<div class="sc-insufficient"><strong>Ej jämförbart underlag:</strong> ${insufficient}</div>` : ""}</div>`;
  }

  function priorityMatrixMarkup(matrix) {
    const safeMatrix = matrix || { sellers: [], medians: {}, insufficient_sample: [] };
    return `<section class="sc-section" aria-labelledby="sc-matrix-title"><div class="sc-section-heading"><div><h2 id="sc-matrix-title">Teamets prioriteringsmatris</h2><p>Matrisen visar sambandet mellan historiskt prioritetsfokus och Kontakt – order inom 10 dagar för vald period. Ordermåttet använder samma preliminära definition som Coachningsöversikten; kontakter som väntar på 10-dagarsutfall ingår i nämnaren. Medianlinjer kräver tillräckligt underlag och minst 70 % jämförbar historisk prioritetstäckning.</p></div></div>${priorityMatrixPanelMarkup(safeMatrix)}</section>`;
  }

  function visitMarkup(data) {
    const highPriority = data.high_priority_boms_metric || { value: data.high_priority_boms, status: "sufficient" };
    const cards = [
      ["Bom-ratio", percent(data.bom_ratio.value), rateEvidence(data.bom_ratio), "bom_ratio", "bom_ratio"],
      ["Träffgrad för besök", percent(data.reach.value), rateEvidence(data.reach), "visit_reach", "reach", "visit"],
      ["Återkommande bommar", number(data.repeat_boms.customers), `${number(data.repeat_boms.visits)} besök`, "repeat_boms", "repeat_boms"],
      ["Högprioriterade bommar", highPriority.value === null ? "—" : number(highPriority.value), highPriority.value === null ? "Kan inte beräknas · jämförbar historisk prioritet saknas" : `${rateEvidence(highPriority.coverage)} med jämförbar historisk prioritet`, "high_priority_boms", highPriority.value === null ? "" : "high_priority_boms"],
      ["Bom-ratio – planerade besök", percent(data.planned.value), rateEvidence(data.planned), "planned_bom_ratio", "planned_boms"],
      ["Bom-ratio – oplanerade besök", percent(data.unplanned.value), rateEvidence(data.unplanned), "unplanned_bom_ratio", "unplanned_boms"],
    ];
    const patterns = [...(data.weekday_patterns || []), ...(data.time_band_patterns || [])];
    const cardMarkup = cards.map(([label, value, evidence, definitionKey, drilldownMetric, channel], index) => miniMetricCard({
      context: `visit-${index}`, label, value, evidence, definitionKey,
      drilldownMetric, channel,
    })).join("");
    return `<section class="sc-section" aria-labelledby="sc-visit-title"><div class="sc-section-heading"><div><h2 id="sc-visit-title">Besökseffektivitet</h2><p>Bom-ratio visas som procent och x av y. Mönster kräver minst tio besök.</p></div></div><div class="sc-card-grid">${cardMarkup}</div>${patterns.length ? `<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Mönster</th><th>${metricHeader("Bom-ratio", "bom_ratio", "visit-pattern-bom")}</th><th>Underlag</th></tr></thead><tbody>${patterns.map(item => `<tr><td>${escapeHtml(item.label)}</td><td data-metric-definition="bom_ratio">${percent(item.bom_ratio.value)}</td><td>${rateEvidence(item.bom_ratio)}</td></tr>`).join("")}</tbody></table></div>` : `<div class="sc-insufficient">Inga veckodags- eller tidsgrupper har minst tio besök.</div>`}</section>`;
  }

  function channelMarkup(channels) {
    const labels = { visit: "Besök", phone: "Telefon", email: "Manuellt mejl" };
    const definitionKeys = { visit: "visits", phone: "phone", email: "manual_email" };
    const displayRate = (rate, pending = 0) => `${percent(rate.value)} (${rateEvidence(rate)})${rate.status === "small_sample" ? " · Litet underlag" : ""}${Number(pending) > 0 ? `<br><span class="sc-rate-pending">Preliminärt · ${number(pending)} väntar på 10-dagarsutfall</span>` : ""}`;
    const channelRate = (channel, key, rate, pending = 0) => {
      const definition = metricDefinition(key, rate);
      return Array.isArray(definition.channels) && !definition.channels.includes(channel)
        ? "Ej tillämpligt"
        : displayRate(rate, pending);
    };
    const medianInfo = definitionParts("median_days_to_order", "channel-median-days", {}, "sc-metric-info sc-table-info");
    return `<section class="sc-section" aria-labelledby="sc-channel-title"><div class="sc-section-heading"><div><h2 id="sc-channel-title">Kanalernas effektivitet</h2><p>10-dagarsmåtten använder samma live-definition som Coachningsöversikten och markeras som preliminära när utfall fortfarande väntar. Automatiserade CRM-mejl ingår inte. Små underlag märks uttryckligen.</p></div></div><div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Kanal</th><th>${metricHeader("Aktiviteter", "human_activities", "channel-activities")}</th><th>${metricHeader("Träffgrad", "reach", "channel-reach")}</th><th>${metricHeader("Positiv dialog", "positive_dialogue", "channel-positive")}</th><th>${metricHeader("Kontakt → order inom 10 dagar", "order_10d", "channel-order")}</th><th>${metricHeader("Positiv dialog → order inom 10 dagar", "positive_to_order_10d", "channel-positive-order")}</th><th>${metricHeader("Attribuerat utfall", "attributed_orders", "channel-attributed")}${medianInfo.button}${medianInfo.explanation}</th></tr></thead><tbody>${Object.entries(channels || {}).map(([key, item]) => `<tr data-channel-row="${key}"><td><button type="button" data-channel="${key}" data-drilldown="human_activities" data-metric-definition="${definitionKeys[key]}">${labels[key]}</button></td><td data-metric-definition="human_activities">${number(item.human_activities)}</td><td data-channel-metric="reach" data-metric-definition="reach">${channelRate(key, "reach", item.reach)}</td><td data-channel-metric="positive_dialogue" data-metric-definition="positive_dialogue">${channelRate(key, "positive_dialogue", item.positive_dialogue)}</td><td data-channel-metric="order_10d" data-metric-definition="order_10d">${channelRate(key, "order_10d", item.order_10d, item.waiting_outcome_count)}</td><td data-channel-metric="positive_to_order_10d" data-metric-definition="positive_to_order_10d">${channelRate(key, "positive_to_order_10d", item.positive_to_order_10d, item.waiting_positive_dialogues_count)}</td><td><span data-metric-definition="attributed_orders">${number(item.attributed_orders)} order</span> · <span data-metric-definition="attributed_order_dfp">${number(item.dfp, 2)} DFP</span> · <span data-metric-definition="attributed_order_value">${orderValues(item.order_value_by_currency)}</span>${item.median_days_to_order === null ? " · Median kräver 5 order" : ` · <span data-metric-definition="median_days_to_order">${number(item.median_days_to_order, 1)} dagar median</span>`}</td></tr>`).join("")}</tbody></table></div></section>`;
  }

  function followupMarkup(data) {
    const cards = [
      ["Positiv kontakt med nästa steg/order", percent(data.positive_next_step_coverage.value), rateEvidence(data.positive_next_step_coverage), "positive_next_step_coverage", "followup_success"],
      ["Positiva utan nästa steg", number(data.positive_without_next_step), "minst tre dagar gamla", "positive_without_next_step", "followup_gap"],
      ["Planerade genomförda i tid", percent(data.planned_completed_in_time.value), rateEvidence(data.planned_completed_in_time), "planned_completed_in_time", "planned_on_time"],
      ["Försenade planerade", number(data.overdue_planned), "fortfarande öppna", "overdue_planned", "planned_overdue"],
      ["Överhoppade planerade", number(data.skipped), `${number(data.cancelled_excluded)} avbrutna exkluderade`, "skipped_planned", "planned_skipped", "cancelled_excluded"],
      ["Positiv utan order/uppföljning 10 dagar", number(data.positive_without_order_or_follow_up_10d), "10-dagarsfönstret har passerat", "positive_without_order_or_follow_up_10d", "followup_gap_10d"],
    ];
    return `<section class="sc-section" aria-labelledby="sc-followup-title"><div class="sc-section-heading"><div><h2 id="sc-followup-title">Uppföljningsdisciplin</h2><p>Avbrutna aktiviteter räknas inte som misslyckat genomförande.</p></div></div><div class="sc-card-grid">${cards.map(([label, value, evidence, definitionKey, drilldownMetric, secondaryDefinitionKey], index) => miniMetricCard({ context: `followup-${index}`, label, value, evidence, definitionKey, secondaryDefinitionKey, drilldownMetric })).join("")}</div></section>`;
  }

  function coachingMarkup(cards) {
    const pendingEvidence = card => (
      ["order_10d", "positive_to_order_10d"].includes(card.metric_key)
      && Number(card.evidence?.waiting_outcome_count) > 0
        ? `<br><span class="sc-rate-pending">Preliminärt · ${number(card.evidence.waiting_outcome_count)} väntar på 10-dagarsutfall</span>`
        : ""
    );
    const evidence = card => {
      const item = card.evidence || {};
      if (item.metric_type === "count") {
        const primary = `${number(item.value)} ${escapeHtml(item.unit || "st")}`;
        const secondary = item.secondary_evidence;
        return secondary?.value === null || secondary?.value === undefined
          ? primary
          : `${primary} · ${number(secondary.value)} ${escapeHtml(secondary.unit || "st")}`;
      }
      return item.denominator !== undefined
        ? `${rateEvidence(item)} · ${percent(item.value)}`
        : number(item.value);
    };
    const comparison = card => card.benchmark?.label || card.comparison?.label || comparisonText({ comparisons: card.comparison || card.benchmark || {} });
    const cardMarkup = (cards || []).map((card, index) => {
      const info = definitionParts(card.metric_key, `coaching-${index}`);
      return `<article class="sc-coaching-card" data-severity="${escapeHtml(card.polarity || card.severity)}" data-metric-definition="${escapeHtml(card.metric_key)}"><span class="sc-coaching-label">${(card.polarity || card.severity) === "strength" ? "STYRKA" : "FOKUSOMRÅDE"}</span>${info.button}${info.explanation}<h3>${escapeHtml(card.title)}</h3><p><strong>Observation:</strong> ${escapeHtml(card.observation || card.diagnosis || "")}</p><p><strong>Bevis:</strong> ${evidence(card)}${pendingEvidence(card)}</p>${comparison(card) ? `<p><strong>Jämförelse:</strong> ${escapeHtml(comparison(card))}</p>` : ""}<p><strong>Nästa steg:</strong> ${escapeHtml(card.next_action || card.recommendation || "")}</p>${card.target ? `<p><strong>Mål:</strong> ${escapeHtml(card.target)}</p>` : ""}<button type="button" data-drilldown="${escapeHtml(card.drilldown_metric)}" data-drilldown-filters="${escapeHtml(JSON.stringify(card.drilldown_filters || {}))}" data-metric-definition="${escapeHtml(card.metric_key)}">Visa underlag</button></article>`;
    }).join("");
    return `<section class="sc-section" aria-labelledby="sc-coaching-title"><div class="sc-section-heading"><div><h2 id="sc-coaching-title">Coachningskort</h2><p>Prioriterade observationer med tydligt underlag.</p></div></div>${cardMarkup ? `<div class="sc-coaching-grid">${cardMarkup}</div>` : `<div class="sc-empty">Inget coachningskort aktiveras med tillräckligt underlag i valt filter.</div>`}</section>`;
  }

  function diagnosticsMarkup(data) {
    const labels = { visits: "Besök", followup: "Uppföljning", channels: "Kanaler" };
    const tabs = ADVANCED_TABS.map(key => [key, labels[key]]);
    const panels = {
      visits: () => visitMarkup(data.visit_efficiency || {}),
      followup: () => followupMarkup(data.follow_up_discipline || {}),
      channels: () => channelMarkup(data.channel_effectiveness || {}),
    };
    const active = panels[state.diagnosticTab] ? state.diagnosticTab : "visits";
    return `<section class="sc-section sc-diagnostics" aria-labelledby="sc-diagnostics-title"><details id="sc-advanced-analysis"><summary id="sc-diagnostics-title"><span>Fördjupad analys</span><span class="sc-disclosure-icon" aria-hidden="true"></span></summary><div class="sc-details-content"><p class="sc-details-intro">Välj ett område för att fördjupa analysen.</p><div class="sc-diagnostic-tabs" role="tablist" aria-label="Fördjupad analys">${tabs.map(([key, label]) => `<button type="button" role="tab" id="sc-diagnostic-tab-${key}" data-diagnostic-tab="${key}" aria-selected="${active === key}" aria-controls="sc-diagnostic-panel-${key}" tabindex="${active === key ? "0" : "-1"}">${label}</button>`).join("")}</div><div class="sc-diagnostic-panel" id="sc-diagnostic-panel-${active}" role="tabpanel" aria-labelledby="sc-diagnostic-tab-${active}" tabindex="0">${panels[active]()}</div></div></details></section>`;
  }

  function renderDashboard(data) {
    renderSellerOptions(data.options?.sellers);
    const target = document.getElementById("sc-dashboard-content");
    target.removeAttribute("aria-busy");
    target.innerHTML = [
      kpisMarkup(data.kpis || {}),
      coachingMarkup(data.coaching_cards || []),
      teamComparisonMarkup(data.team_comparison || { sellers: [] }),
      teamTrendsMarkup(data.team_10d_trends || { metrics: {} }),
      priorityMatrixMarkup(data.coaching_matrix || data.coaching_matrices?.priority),
      diagnosticsMarkup(data),
      dataQualityDetailsMarkup(data.data_quality || {}),
    ].join("");
  }

  function handleDashboardClick(event) {
    const metricInfo = event.target.closest('[data-sc-action="metric-info"]');
    if (metricInfo) {
      const explanation = document.getElementById(metricInfo.getAttribute("aria-controls"));
      if (!explanation) return;
      const expanded = metricInfo.getAttribute("aria-expanded") === "true";
      metricInfo.setAttribute("aria-expanded", String(!expanded));
      explanation.hidden = expanded;
      return;
    }
    const retry = event.target.closest('[data-sc-action="retry"]');
    if (retry) return void loadSummary();
    const teamTrendTab = event.target.closest("[data-team-trend-view]");
    if (teamTrendTab) {
      state.teamTrendView = teamTrendTab.dataset.teamTrendView;
      if (state.data) renderDashboard(state.data);
      document.getElementById(`sc-team-trend-tab-${state.teamTrendView}`)?.focus();
      return;
    }
    const diagnosticTab = event.target.closest("[data-diagnostic-tab]");
    if (diagnosticTab) {
      state.diagnosticTab = diagnosticTab.dataset.diagnosticTab;
      if (state.data) renderDashboard(state.data);
      const advanced = document.getElementById("sc-advanced-analysis");
      if (advanced) advanced.open = true;
      document.getElementById(`sc-diagnostic-tab-${state.diagnosticTab}`)?.focus();
      return;
    }
    const customer = event.target.closest("[data-customer-id]");
    if (customer) {
      window.dispatchEvent(new CustomEvent("sales-coaching:open-customer", { detail: { customerId: customer.dataset.customerId } }));
      return;
    }
    const drilldown = event.target.closest("[data-drilldown]");
    if (drilldown) {
      let extra = {};
      if (drilldown.dataset.seller) extra.seller = drilldown.dataset.seller;
      if (drilldown.dataset.channel) extra.channel = drilldown.dataset.channel;
      if (drilldown.dataset.start) extra.start = drilldown.dataset.start;
      if (drilldown.dataset.end) extra.end = drilldown.dataset.end;
      if (drilldown.dataset.drilldownFilters) {
        try { extra = { ...extra, ...JSON.parse(drilldown.dataset.drilldownFilters) }; } catch (_) {}
      }
      openDrilldown(drilldown.dataset.drilldown, extra, drilldown);
      return;
    }
    const seller = event.target.closest("[data-seller]");
    if (!seller) return;
    state.filters.seller = seller.dataset.seller;
    setControlValues();
    updateUrl();
    loadSummary();
  }

  function drawerMarkup(metric, definitionKey) {
    const entry = definitionEntry(definitionKey) || {};
    const definitionText = metricDefinitionText(entry);
    return `<div class="sc-drawer-backdrop" id="sc-drawer-backdrop"><aside class="sc-drawer" role="dialog" aria-modal="true" aria-labelledby="sc-drawer-title"><div class="sc-drawer-header"><div class="sc-drawer-heading" data-metric-definition="${escapeHtml(definitionKey)}"><h2 id="sc-drawer-title">Underlag: ${escapeHtml(entry.label || "Valt mått")}</h2>${definitionText ? `<p>${escapeHtml(definitionText)}</p>` : ""}</div><button type="button" class="sc-drawer-close" data-sc-drawer-close aria-label="Stäng underlag">×</button></div><div id="sc-drawer-content" aria-live="polite"><div class="sc-skeleton"></div></div></aside></div>`;
  }

  async function openDrilldown(metric, extra = {}, trigger = null) {
    if (!metric) return;
    closeDrawer();
    state.lastFocus = trigger || document.activeElement;
    const definitionKey = trigger?.dataset.metricDefinition || DRILLDOWN_DEFINITION_KEYS[metric] || metric;
    document.body.insertAdjacentHTML("beforeend", drawerMarkup(metric, definitionKey));
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
    const cohortLabels = {
      numerator: "Konverterad",
      resolved_without_order: "Avgjord utan order",
      pending: "Väntar på utfall",
      denominator_only: "Endast nämnare",
      missed_outcome: "Missat utfall",
    };
    const qualityLabels = { exact: "Fullständigt sparat", approximate: "Uppskattat", missing: "Saknas" };
    const rowMarkup = rows.map(row => `<tr><td>${escapeHtml(cohortLabels[row.cohort_role] || row.cohort_role || "Underlag")}</td><td>${escapeHtml(row.date_time || "")}</td><td>${escapeHtml(row.sales_user_name || "")}</td><td>${row.customer_id ? `<button type="button" data-customer-id="${escapeHtml(row.customer_id)}">${escapeHtml(row.customer || "")}</button>` : escapeHtml(row.customer || "")}</td><td>${escapeHtml(row.channel || "")} · ${escapeHtml(row.result_class || "")}</td><td data-metric-definition="historical_priority_at_contact">${row.priority_percentile_at_contact === null ? "Saknas" : `${number(row.priority_percentile_at_contact, 1)} percentil`} · ${escapeHtml(qualityLabels[row.snapshot_quality] || "Saknas")}</td><td data-metric-definition="attributed_orders">${row.order_reference ? `${escapeHtml(row.order_reference)} · <span data-metric-definition="days_to_order">${number(row.days_to_order)} dagar</span> · <span data-metric-definition="attributed_order_dfp">${number(row.dfp, 2)} DFP</span>` : "–"}</td></tr>`).join("");
    target.innerHTML = `<div class="sc-drawer-meta">Visar ${number(rows.length)} av ${number(data.total_count)}, maximalt ${number(data.limit)} rader.</div>${rows.length ? `<div class="sc-table-wrap"><table class="sc-table"><thead><tr><th>Kohortroll</th><th>Datum</th><th>Säljare</th><th>Kund</th><th>Kanal/resultat</th><th>${metricName("historical_priority_at_contact", "Historisk prioritet vid kontakt")}</th><th>${metricName("attributed_orders", "Orderutfall")}</th></tr></thead><tbody>${rowMarkup}</tbody></table></div>` : `<div class="sc-empty">Inga rader matchar underlaget.</div>`}`;
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
