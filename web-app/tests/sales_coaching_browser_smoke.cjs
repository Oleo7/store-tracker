const { chromium } = require("playwright");

const mode = process.argv[2] || "desktop";
const viewport = mode === "mobile"
  ? { width: 390, height: 844 }
  : { width: 1440, height: 1000 };

(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (error) {
    if (!String(error).includes("Executable doesn't exist")) throw error;
    browser = await chromium.launch({ headless: true, channel: "chrome" });
  }
  try {
    const page = await browser.newPage({ viewport });
    await page.goto("http://127.0.0.1:5065/", { waitUntil: "domcontentloaded" });
    const login = await page.evaluate(async () => {
      const response = await fetch("/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_name: "admin", password: "secret" }),
      });
      return { ok: response.ok, body: await response.text() };
    });
    if (!login.ok) throw new Error(`${mode}: harness login failed: ${login.body}`);
    const browserErrors = [];
    let summaryRequestCount = 0;
    page.on("console", message => {
      if (message.type() === "error") browserErrors.push(message.text());
    });
    page.on("pageerror", error => browserErrors.push(String(error)));
    page.on("request", request => {
      const url = new URL(request.url());
      if (url.pathname.endsWith("/sales-coaching-insights")) summaryRequestCount += 1;
    });
    await page.goto("http://127.0.0.1:5065/?sales_coaching=1&period=4&seller=olle", {
      waitUntil: "networkidle",
    });
    await page.locator("#sales-coaching-dashboard:not([hidden])").waitFor();
    await page.locator(".sc-kpi-card").first().waitFor();
    if (await page.locator(".sc-filter-primary .sc-field:visible").count() !== 2) {
      throw new Error(`${mode}: Period and Säljare are not the only initially visible filter fields`);
    }
    if (await page.locator("#sc-more-filters-panel:visible").count()) {
      throw new Error(`${mode}: additional filters are open on first load`);
    }
    if (await page.locator("#sc-custom-dates:visible").count()) {
      throw new Error(`${mode}: custom date fields are visible for a standard period`);
    }
    if (await page.locator('#sc-period option[value="2"]').count() !== 1) {
      throw new Error(`${mode}: the two-week period option is missing`);
    }
    const moreFilters = page.locator("#sc-more-filters-toggle");
    if (await moreFilters.getAttribute("aria-expanded") !== "false") {
      throw new Error(`${mode}: additional-filter control has the wrong initial state`);
    }
    await moreFilters.press("Enter");
    if (await moreFilters.getAttribute("aria-expanded") !== "true" || !(await page.locator("#sc-more-filters-panel").isVisible())) {
      throw new Error(`${mode}: additional filters did not open from the keyboard`);
    }
    for (const filter of ["#sc-channel", "#sc-lifecycle", "#sc-segment"]) {
      if (!(await page.locator(filter).isVisible())) throw new Error(`${mode}: hidden filter ${filter} did not become visible`);
    }
    await moreFilters.press("Space");
    if (await page.locator("#sc-more-filters-panel").isVisible()) {
      throw new Error(`${mode}: additional filters did not close from the keyboard`);
    }
    if (await page.locator(".sc-quality-status").count()) {
      throw new Error(`${mode}: removed top data-quality row still renders`);
    }
    const coachingIntro = await page.locator("#sc-kpi-title + p").innerText();
    if (coachingIntro.trim() !== "Procentsatser bedöms först när underlaget är minst 10.") {
      throw new Error(`${mode}: coaching overview uses the wrong sample-size copy: ${coachingIntro}`);
    }
    const kpiCount = await page.locator(".sc-kpi-card").count();
    if (kpiCount !== 5) throw new Error(`${mode}: expected 5 KPI cards, got ${kpiCount}`);
    const expectedDenominators = [
      "analyserbara besök/telefonsamtal",
      "nådda besök/telefonsamtal",
      "positiva dialoger har följts av order",
      "kontakter har följts av order",
    ];
    const denominatorText = await page.locator(".sc-kpi-denominator").allInnerTexts();
    for (const expected of expectedDenominators) {
      if (!denominatorText.includes(expected)) {
        throw new Error(`${mode}: missing KPI denominator explanation: ${expected}`);
      }
    }
    const infoButtons = page.locator('.sc-kpi-info[data-sc-action="metric-info"]');
    if (await infoButtons.count() !== 5) {
      throw new Error(`${mode}: expected five KPI information controls`);
    }
    for (let index = 0; index < 5; index += 1) {
      const info = infoButtons.nth(index);
      await info.focus();
      await info.press("Enter");
      const explanationId = await info.getAttribute("aria-controls");
      const explanation = page.locator(`#${explanationId}`);
      if (!(await explanation.isVisible()) || !(await explanation.innerText()).trim()) {
        throw new Error(`${mode}: KPI explanation ${index + 1} did not open`);
      }
      if (await info.getAttribute("aria-expanded") !== "true") {
        throw new Error(`${mode}: KPI explanation ${index + 1} has wrong expanded state`);
      }
      if (await page.locator("#sc-drawer-backdrop").count()) {
        throw new Error(`${mode}: KPI information opened drilldown`);
      }
      await info.press("Space");
      if (await explanation.isVisible()) {
        throw new Error(`${mode}: KPI explanation ${index + 1} did not close`);
      }
    }
    const activityLabel = await page.locator('.sc-kpi-card[data-kpi-key="human_activities"] .sc-kpi-label').innerText();
    if (activityLabel.trim() !== "Aktiviteter") {
      throw new Error(`${mode}: activity KPI has the wrong label: ${activityLabel}`);
    }
    const contactOrderLabel = await page.locator('.sc-kpi-card[data-kpi-key="order_10d"] .sc-kpi-label').innerText();
    if (contactOrderLabel.trim() !== "Kontakt – order inom 10 dagar") {
      throw new Error(`${mode}: contact-order KPI has the wrong label: ${contactOrderLabel}`);
    }
    const contactOrderCard = page.locator('.sc-kpi-card[data-kpi-key="order_10d"]');
    const contactOrderText = await contactOrderCard.innerText();
    if (!contactOrderText.includes("20 %") || !contactOrderText.includes("7 av 35") || !contactOrderText.includes("kontakter har följts av order")) {
      throw new Error(`${mode}: provisional contact KPI does not use the full eligible cohort: ${contactOrderText}`);
    }
    if (!contactOrderText.includes("Preliminärt · 22 väntar på 10-dagarsutfall")) {
      throw new Error(`${mode}: contact pending copy/count is wrong: ${contactOrderText}`);
    }
    if (contactOrderText.includes("Föregående period") || !contactOrderText.includes("Median övriga säljare")) {
      throw new Error(`${mode}: pending contact KPI must suppress previous period but keep peer median: ${contactOrderText}`);
    }
    if (contactOrderText.includes("40 %") || contactOrderText.includes("Jämförbart")) {
      throw new Error(`${mode}: removed comparable contact outcome is still visible: ${contactOrderText}`);
    }
    const positiveOrderCard = page.locator('.sc-kpi-card[data-kpi-key="positive_to_order_10d"]');
    const positiveOrderText = await positiveOrderCard.innerText();
    if (!positiveOrderText.includes("25 %") || !positiveOrderText.includes("7 av 28") || !positiveOrderText.includes("positiva dialoger har följts av order")) {
      throw new Error(`${mode}: provisional positive KPI does not use the full eligible cohort: ${positiveOrderText}`);
    }
    if (!positiveOrderText.includes("Preliminärt · 15 väntar på 10-dagarsutfall")) {
      throw new Error(`${mode}: positive pending copy/count is wrong: ${positiveOrderText}`);
    }
    if (positiveOrderText.includes("Föregående period")) {
      throw new Error(`${mode}: pending positive KPI still shows previous period: ${positiveOrderText}`);
    }
    if (!positiveOrderText.includes("Median övriga säljare")) {
      throw new Error(`${mode}: pending positive KPI lost its peer median: ${positiveOrderText}`);
    }
    if (positiveOrderText.includes("40 %") || positiveOrderText.includes("Jämförbart")) {
      throw new Error(`${mode}: removed comparable positive outcome is still visible: ${positiveOrderText}`);
    }
    await contactOrderCard.locator(".sc-kpi-main").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const orderDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!orderDrawerText.includes("Visar 35 av 35") || !orderDrawerText.includes("SMOKE-ORDER-1") || !orderDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: eligible contact drilldown does not match KPI denominator/outcomes: ${orderDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    await positiveOrderCard.locator(".sc-kpi-main").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const positiveDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!positiveDrawerText.includes("Visar 28 av 28") || !positiveDrawerText.includes("SMOKE-ORDER-1") || !positiveDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: eligible positive drilldown does not match KPI denominator/outcomes: ${positiveDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    if (await page.locator(".sc-kpi-comparable").count()) {
      throw new Error(`${mode}: removed comparable KPI row still renders`);
    }
    const closingStrength = page.locator(".sc-coaching-card", {
      hasText: "Stark positiv-till-order-konvertering",
    });
    if (await closingStrength.count() !== 1) {
      throw new Error(`${mode}: deterministic live 10-day strength card is missing`);
    }
    const closingStrengthText = await closingStrength.innerText();
    if (!closingStrengthText.includes("7 av 28 · 25 %") || !closingStrengthText.includes("Preliminärt · 15 väntar på 10-dagarsutfall")) {
      throw new Error(`${mode}: live strength card evidence/pending copy is wrong: ${closingStrengthText}`);
    }
    await closingStrength.locator('[data-drilldown="positive_to_order_10d"]').click();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const closingDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!closingDrawerText.includes("Visar 28 av 28") || (closingDrawerText.match(/Konverterad/g) || []).length !== 7) {
      throw new Error(`${mode}: strength card drilldown does not reconcile 7/28: ${closingDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    const statusLabels = await page.locator(".sc-kpi-card .sc-status").allInnerTexts();
    if (statusLabels.some(label => label.trim() !== "Inte tillräckligt underlag")) {
      throw new Error(`${mode}: a sufficient KPI status badge is still visible`);
    }
    const positiveCard = page.locator('.sc-kpi-card[data-kpi-key="positive_dialogue"]');
    const positiveEvidence = await positiveCard.locator(".sc-kpi-evidence").innerText();
    if (!positiveEvidence.includes("nådda besök/telefonsamtal")) {
      throw new Error(`${mode}: positive dialogue uses the wrong denominator copy`);
    }
    const benchmark = await positiveCard.locator(".sc-kpi-comparison").innerText();
    if (!benchmark.includes("Median övriga säljare")) {
      throw new Error(`${mode}: self-excluding benchmark label is missing`);
    }
    await positiveCard.locator(".sc-kpi-main").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("[data-sc-drawer-close]").click();

    const teamHeaders = await page.locator(".sc-comparison-table thead th").allInnerTexts();
    const normalizedTeamHeaders = teamHeaders.map(text => text.toLocaleLowerCase("sv-SE"));
    const positiveOrderIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("positiv dialog → order inom 10 dagar"));
    const contactOrderIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("kontakt – order inom 10 dagar"));
    const nextStepIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("nästa-steg-täckning"));
    if (!(positiveOrderIndex + 1 === contactOrderIndex && contactOrderIndex + 1 === nextStepIndex)) {
      throw new Error(`${mode}: contact-order metric is in the wrong team-comparison position: ${JSON.stringify(teamHeaders)}`);
    }
    const olleTeamRow = page.locator(".sc-comparison-table tbody tr", { hasText: "Olle" });
    const olleTeamText = await olleTeamRow.innerText();
    const positiveOrderTeamText = await olleTeamRow.locator("td").nth(3).innerText();
    const contactOrderTeamText = await olleTeamRow.locator("td").nth(4).innerText();
    for (const expected of [
      "20 %", "7 av 35", "Preliminärt · 22 väntar på 10-dagarsutfall",
      "25 %", "7 av 28", "Preliminärt · 15 väntar på 10-dagarsutfall",
    ]) {
      if (!olleTeamText.includes(expected)) {
        throw new Error(`${mode}: team comparison does not use the live 10-day KPI (${expected}): ${olleTeamText}`);
      }
    }
    if (positiveOrderTeamText.includes("40 %") || contactOrderTeamText.includes("40 %") || olleTeamText.includes("fullständigt utfall")) {
      throw new Error(`${mode}: comparable outcome leaked into team comparison: ${olleTeamText}`);
    }

    const sectionHeadings = await page.locator("#sc-dashboard-content > .sc-section > .sc-section-heading h2").allInnerTexts();
    const teamComparisonPosition = sectionHeadings.indexOf("Teamjämförelse");
    const trendPosition = sectionHeadings.indexOf("10-dagarskonvertering – trend");
    const matricesPosition = sectionHeadings.indexOf("Teamets prioriteringsmatris");
    if (!(teamComparisonPosition >= 0 && teamComparisonPosition + 1 === trendPosition && trendPosition + 1 === matricesPosition)) {
      throw new Error(`${mode}: long-term trend is in the wrong section order: ${JSON.stringify(sectionHeadings)}`);
    }
    const trendSection = page.locator(".sc-team-10d-trend-section");
    await trendSection.scrollIntoViewIfNeeded();
    const trendCopy = await trendSection.innerText();
    for (const expected of [
      "Varje punkt avser en kontaktvecka",
      "Endast veckor där hela 10-dagarsfönstret har passerat visas",
      "Diagrammen använder samma KPI-definitioner som Coachningsöversikten",
      "Period-, säljar- och kanalfilter begränsar inte grafen",
    ]) {
      if (!trendCopy.includes(expected)) throw new Error(`${mode}: missing trend explanation: ${expected}`);
    }
    if (await page.locator(".sc-matrix-tabs").count()) {
      throw new Error(`${mode}: removed sales/priority matrix tabs still render`);
    }
    const orderPanel = trendSection.locator("#sc-team-trend-panel-order");
    const positivePanel = trendSection.locator("#sc-team-trend-panel-positive");
    const assertTrendPanelState = async expectedView => {
      const panelState = await trendSection.evaluate(section => {
        const panels = [...section.querySelectorAll('[role="tabpanel"]')];
        const tabs = [...section.querySelectorAll('[role="tab"]')];
        return {
          panels: panels.map(panel => ({
            id: panel.id,
            labelledBy: panel.getAttribute("aria-labelledby"),
            hidden: panel.hidden,
          })),
          tabs: tabs.map(tab => ({
            id: tab.id,
            controls: tab.getAttribute("aria-controls"),
            controlsExists: Boolean(document.getElementById(tab.getAttribute("aria-controls"))),
            selected: tab.getAttribute("aria-selected"),
            tabIndex: tab.getAttribute("tabindex"),
          })),
        };
      });
      if (panelState.panels.length !== 2) {
        throw new Error(`${mode}: both permanent trend tabpanels are not in the DOM: ${JSON.stringify(panelState)}`);
      }
      for (const view of ["order", "positive"]) {
        const panel = panelState.panels.find(item => item.id === `sc-team-trend-panel-${view}`);
        const tab = panelState.tabs.find(item => item.id === `sc-team-trend-tab-${view}`);
        if (!panel || !tab || !tab.controlsExists || tab.controls !== panel.id || panel.labelledBy !== tab.id) {
          throw new Error(`${mode}: broken trend tab/panel ARIA relationship for ${view}: ${JSON.stringify(panelState)}`);
        }
        const active = view === expectedView;
        if (panel.hidden === active || tab.selected !== String(active) || tab.tabIndex !== (active ? "0" : "-1")) {
          throw new Error(`${mode}: wrong hidden/selected/tabindex state for ${view}: ${JSON.stringify(panelState)}`);
        }
      }
      if (panelState.panels.filter(panel => !panel.hidden).length !== 1) {
        throw new Error(`${mode}: expected exactly one visible trend panel: ${JSON.stringify(panelState)}`);
      }
    };
    await assertTrendPanelState("order");
    const weekSlotCount = await orderPanel.locator(".sc-team-order-x-label").count();
    if (weekSlotCount !== 16) throw new Error(`${mode}: expected 16 trend week slots, got ${weekSlotCount}`);
    const yLabels = await orderPanel.locator(".sc-team-order-y-label").allTextContents();
    if (JSON.stringify(yLabels) !== JSON.stringify(["0 %", "25 %", "50 %", "75 %", "100 %"])) {
      throw new Error(`${mode}: trend scale is not fixed at 0/25/50/75/100: ${JSON.stringify(yLabels)}`);
    }
    if (await orderPanel.locator(".sc-team-order-legend-item").count() !== 3) {
      throw new Error(`${mode}: expected one trend series for each of three active sellers`);
    }
    if (await orderPanel.locator(".sc-team-order-line").count() < 3) {
      throw new Error(`${mode}: expected at least three rendered seller trend lines`);
    }
    const orderTab = trendSection.locator('[data-team-trend-view="order"]');
    const positiveTab = trendSection.locator('[data-team-trend-view="positive"]');
    if (await orderTab.getAttribute("aria-selected") !== "true") {
      throw new Error(`${mode}: contact-to-order is not the default trend tab`);
    }
    const olleTrendPoint = orderPanel.locator('.sc-team-order-point[data-seller="olle"][data-numerator="4"][data-denominator="10"]').first();
    const sofiaTrendPoint = orderPanel.locator('.sc-team-order-point[data-seller="sofia"][data-numerator="2"][data-denominator="10"]').first();
    const viewerTrendPoint = orderPanel.locator('.sc-team-order-point[data-seller="viewer"][data-numerator="4"][data-denominator="8"]').first();
    for (const [seller, point] of [["olle", olleTrendPoint], ["sofia", sofiaTrendPoint], ["viewer", viewerTrendPoint]]) {
      if (await point.count() !== 1) {
        const renderedPoints = await orderPanel.locator(".sc-team-order-point").evaluateAll(points => points.map(point => ({
          seller: point.dataset.seller,
          numerator: point.dataset.numerator,
          denominator: point.dataset.denominator,
          week: point.dataset.week,
        })));
        throw new Error(`${mode}: missing deterministic ${seller} trend point: ${JSON.stringify(renderedPoints)}`);
      }
    }
    const seriesStyles = {};
    for (const [seller, point] of [["olle", olleTrendPoint], ["sofia", sofiaTrendPoint], ["viewer", viewerTrendPoint]]) {
      const pointStyle = await point.getAttribute("data-series-style");
      const legendStyle = await orderPanel.locator(`.sc-team-order-legend-item[data-seller="${seller}"]`).getAttribute("data-series-style");
      const lineStyles = await orderPanel.locator(`.sc-team-order-line[data-seller="${seller}"]`).evaluateAll(lines => lines.map(line => line.dataset.seriesStyle));
      if (!pointStyle || legendStyle !== pointStyle || lineStyles.some(style => style !== pointStyle)) {
        throw new Error(`${mode}: ${seller} does not keep one identity-derived style across points, lines, and legend`);
      }
      seriesStyles[seller] = pointStyle;
    }
    if (!(await olleTrendPoint.getAttribute("class")).includes("is-selected")) {
      throw new Error(`${mode}: selected seller is not highlighted in the trend`);
    }
    if (!(await viewerTrendPoint.getAttribute("class")).includes("is-small-sample")) {
      throw new Error(`${mode}: small-sample trend point is not hollow/muted`);
    }
    const pointStart = await viewerTrendPoint.getAttribute("data-start");
    const pointEnd = await viewerTrendPoint.getAttribute("data-end");
    await viewerTrendPoint.hover();
    const viewerTooltip = await viewerTrendPoint.locator("title").textContent();
    const viewerAriaLabel = await viewerTrendPoint.getAttribute("aria-label");
    if (!viewerTooltip.includes("viewer") || !viewerTooltip.includes(`${pointStart}–${pointEnd}`) || !viewerTooltip.includes("50 %") || !viewerTooltip.includes("4 av 8 kontakter har följts av order inom 10 dagar") || !viewerTooltip.includes("Litet underlag") || !viewerTooltip.match(/\d{4} v\.\d+/)) {
      throw new Error(`${mode}: trend point tooltip lacks seller/week/date/value/evidence: ${viewerTooltip}`);
    }
    if (viewerAriaLabel !== viewerTooltip) {
      throw new Error(`${mode}: trend point aria-label does not match the complete tooltip`);
    }
    const pointResponse = page.waitForResponse(response => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/sales-coaching-insights/drilldown")
        && url.searchParams.get("metric") === "order_10d"
        && url.searchParams.get("seller") === "viewer"
        && url.searchParams.get("channel") === "all"
        && url.searchParams.get("start") === pointStart
        && url.searchParams.get("end") === pointEnd;
    });
    await viewerTrendPoint.focus();
    await viewerTrendPoint.press("Enter");
    await pointResponse;
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const pointDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!pointDrawerText.includes("Visar 8 av 8") || (pointDrawerText.match(/Konverterad/g) || []).length !== 4 || pointDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: trend point drilldown does not reconcile numerator/denominator: ${pointDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();

    const summaryRequestsBeforeTrendToggle = summaryRequestCount;
    await positiveTab.click();
    await assertTrendPanelState("positive");
    await orderTab.click();
    await assertTrendPanelState("order");
    await orderTab.focus();
    await orderTab.press("ArrowRight");
    await assertTrendPanelState("positive");
    if (await positiveTab.getAttribute("aria-selected") !== "true") {
      throw new Error(`${mode}: positive-dialogue trend was not activated by ArrowRight`);
    }
    const focusedTrendView = await page.evaluate(
      () => document.activeElement?.dataset?.teamTrendView || "",
    );
    if (focusedTrendView !== "positive") {
      throw new Error(`${mode}: activated positive trend tab did not receive focus`);
    }
    if (summaryRequestCount !== summaryRequestsBeforeTrendToggle) {
      throw new Error(`${mode}: trend toggle triggered a new summary request`);
    }
    const positiveOllePoint = positivePanel.locator('.sc-team-order-point[data-trend-view="positive"][data-seller="olle"][data-numerator="3"][data-denominator="6"]').first();
    const positiveSofiaPoint = positivePanel.locator('.sc-team-order-point[data-trend-view="positive"][data-seller="sofia"][data-numerator="2"][data-denominator="6"]').first();
    const positiveViewerPoint = positivePanel.locator('.sc-team-order-point[data-trend-view="positive"][data-seller="viewer"][data-numerator="3"][data-denominator="6"]').first();
    for (const [seller, point] of [["olle", positiveOllePoint], ["sofia", positiveSofiaPoint], ["viewer", positiveViewerPoint]]) {
      if (await point.count() !== 1) throw new Error(`${mode}: missing deterministic ${seller} positive trend point`);
      if (await point.getAttribute("data-series-style") !== seriesStyles[seller]) {
        throw new Error(`${mode}: ${seller} style differs between the two trend views`);
      }
    }
    const positiveStart = await positiveOllePoint.getAttribute("data-start");
    const positiveEnd = await positiveOllePoint.getAttribute("data-end");
    const positiveTooltip = await positiveOllePoint.locator("title").textContent();
    if (!positiveTooltip.includes(`${positiveStart}–${positiveEnd}`) || !positiveTooltip.includes("50 %") || !positiveTooltip.includes("3 av 6 positiva dialoger har följts av order inom 10 dagar")) {
      throw new Error(`${mode}: positive trend tooltip is incomplete: ${positiveTooltip}`);
    }
    const positivePointResponse = page.waitForResponse(response => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/sales-coaching-insights/drilldown")
        && url.searchParams.get("metric") === "positive_to_order_10d"
        && url.searchParams.get("seller") === "olle"
        && url.searchParams.get("channel") === "all"
        && url.searchParams.get("start") === positiveStart
        && url.searchParams.get("end") === positiveEnd;
    });
    await positiveOllePoint.focus();
    await positiveOllePoint.press("Enter");
    await positivePointResponse;
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const positivePointDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!positivePointDrawerText.includes("Visar 6 av 6") || (positivePointDrawerText.match(/Konverterad/g) || []).length !== 3 || positivePointDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: positive trend drilldown does not reconcile: ${positivePointDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    await positiveTab.press("Home");
    await assertTrendPanelState("order");
    if (await trendSection.locator('[data-team-trend-view="order"]').getAttribute("aria-selected") !== "true") {
      throw new Error(`${mode}: Home did not activate the first trend tab`);
    }
    await trendSection.locator('[data-team-trend-view="order"]').press("End");
    await assertTrendPanelState("positive");
    if (await trendSection.locator('[data-team-trend-view="positive"]').getAttribute("aria-selected") !== "true") {
      throw new Error(`${mode}: End did not activate the last trend tab`);
    }
    await trendSection.locator('[data-team-trend-view="positive"]').press("ArrowLeft");
    await assertTrendPanelState("order");
    await trendSection.locator('[data-team-trend-view="order"]').press("ArrowRight");
    await assertTrendPanelState("positive");
    if (summaryRequestCount !== summaryRequestsBeforeTrendToggle) {
      throw new Error(`${mode}: keyboard trend navigation triggered a summary request`);
    }

    const advancedAnalysis = page.locator("#sc-advanced-analysis");
    if (await advancedAnalysis.getAttribute("open") !== null) {
      throw new Error(`${mode}: advanced analysis is open on first render or after a drilldown`);
    }
    await advancedAnalysis.locator(":scope > summary").click();
    const diagnosticTabs = await page.locator(".sc-diagnostic-tabs [role=tab]").allInnerTexts();
    if (JSON.stringify(diagnosticTabs) !== JSON.stringify(["Besök", "Uppföljning", "Kanaler"])) {
      throw new Error(`${mode}: advanced-analysis tabs are wrong: ${JSON.stringify(diagnosticTabs)}`);
    }
    if (await page.locator('[data-diagnostic-tab="visits"]').getAttribute("aria-selected") !== "true") {
      throw new Error(`${mode}: visits is not the initial diagnostic tab`);
    }
    const assertKeyboardTab = async (key, label) => {
      const tab = page.locator(`[data-diagnostic-tab="${key}"]`);
      if (await tab.getAttribute("aria-selected") !== "true") {
        throw new Error(`${mode}: ${label} was not activated by keyboard navigation`);
      }
      const focusedKey = await page.evaluate(() => document.activeElement?.dataset?.diagnosticTab || "");
      if (focusedKey !== key) {
        throw new Error(`${mode}: ${label} was activated without receiving focus`);
      }
    };
    await page.locator('[data-diagnostic-tab="visits"]').focus();
    await page.locator('[data-diagnostic-tab="visits"]').press("ArrowRight");
    await assertKeyboardTab("followup", "Uppföljning after ArrowRight from Besök");
    await page.locator('[data-diagnostic-tab="followup"]').press("Home");
    await assertKeyboardTab("visits", "Besök after Home");
    await page.locator('[data-diagnostic-tab="visits"]').press("ArrowLeft");
    await assertKeyboardTab("channels", "Kanaler after ArrowLeft from Besök");
    await page.locator('[data-diagnostic-tab="channels"]').press("Home");
    await assertKeyboardTab("visits", "Besök after Home from Kanaler");
    await page.locator('[data-diagnostic-tab="visits"]').press("End");
    await assertKeyboardTab("channels", "Kanaler after End");

    if (await page.locator("#sc-quality-details").getAttribute("open") !== null) {
      throw new Error(`${mode}: data quality and definitions is open on first render`);
    }
    await page.locator("#sc-quality-details > summary").click();
    const glossaryLabels = await page.locator(".sc-glossary dt").allInnerTexts();
    if (glossaryLabels.length < 40) {
      throw new Error(`${mode}: metric glossary is unexpectedly incomplete`);
    }
    const definitionAudit = await page.evaluate(() => {
      const used = [...document.querySelectorAll("[data-metric-definition], [data-secondary-metric-definition]")]
        .flatMap(element => [
          element.getAttribute("data-metric-definition"),
          element.getAttribute("data-secondary-metric-definition"),
        ])
        .filter(Boolean);
      const defined = new Set(
        [...document.querySelectorAll(".sc-definition[data-metric-definition]")]
          .filter(element => element.querySelector("dt")?.textContent.trim() && element.querySelector("dd")?.textContent.trim())
          .map(element => element.getAttribute("data-metric-definition")),
      );
      return { used: [...new Set(used)], missing: [...new Set(used)].filter(key => !defined.has(key)) };
    });
    if (definitionAudit.missing.length) {
      throw new Error(`${mode}: missing metric definitions: ${definitionAudit.missing.join(", ")}`);
    }
    for (const key of ["reach", "positive_dialogue", "order_10d", "bom_ratio", "positive_next_step_coverage", "priority_focus"]) {
      const definition = await page.locator(`.sc-definition[data-metric-definition="${key}"] dd`).innerText();
      if (!definition.includes("Täljaren är") || !definition.includes("nämnaren är")) {
        throw new Error(`${mode}: ${key} does not explain its numerator and denominator`);
      }
    }
    if (mode === "mobile") {
      const matrixScroller = page.locator(".sc-matrix-wrap").first();
      if (await matrixScroller.count()) {
        await matrixScroller.scrollIntoViewIfNeeded();
        if (!(await matrixScroller.isVisible())) throw new Error("mobile: matrix scroller missing");
      } else if (!(await page.locator(".sc-priority-build-up").isVisible())) {
        throw new Error("mobile: neither the priority matrix nor its documented build-up state renders");
      }
    }

    await moreFilters.click();
    if (!(await page.locator("#sc-more-filters-panel").isVisible())) {
      throw new Error(`${mode}: additional filters did not reopen before changing channel`);
    }
    await Promise.all([
      page.waitForResponse(response => response.url().includes("/sales-coaching-insights?") && response.url().includes("channel=email")),
      page.locator("#sc-channel").selectOption("email"),
    ]);
    await moreFilters.click();
    if (await page.locator("#sc-more-filters-panel").isVisible()) {
      throw new Error(`${mode}: additional filters did not close after changing channel`);
    }
    await page.locator('.sc-kpi-card[data-kpi-key="positive_dialogue"] .sc-kpi-evidence').waitFor();
    const emailPositive = await page.locator('.sc-kpi-card[data-kpi-key="positive_dialogue"]').innerText();
    if (!emailPositive.includes("Positiv dialog mäts endast för Besök och Telefon.")) {
      throw new Error(`${mode}: email filter fabricated a positive-dialogue rate`);
    }
    const emailPositiveOrder = await page.locator('.sc-kpi-card[data-kpi-key="positive_to_order_10d"]').innerText();
    if (!emailPositiveOrder.includes("Positiv → order mäts endast för Besök och Telefon.")) {
      throw new Error(`${mode}: email filter fabricated a positive-to-order rate`);
    }
    if (await page.locator('.sc-kpi-card[data-kpi-key="order_10d"] .sc-kpi-secondary').count()) {
      throw new Error(`${mode}: zero pending outcomes still render a pending line`);
    }
    if (await page.locator(".sc-coaching-card .sc-rate-pending").count()) {
      throw new Error(`${mode}: zero pending outcomes still render on a coaching card`);
    }
    if (!(await moreFilters.innerText()).includes("1 aktiva") || !(await moreFilters.evaluate(element => element.classList.contains("is-active")))) {
      throw new Error(`${mode}: active hidden filters are not indicated while the filter block is closed`);
    }
    const filteredTeamOlle = await page.locator(".sc-comparison-table tbody tr", { hasText: "Olle" }).innerText();
    if (!filteredTeamOlle.includes("7 av 35") || !filteredTeamOlle.includes("7 av 28")) {
      throw new Error(`${mode}: channel filter incorrectly changed all-channel team comparison: ${filteredTeamOlle}`);
    }
    await page.locator("#sc-advanced-analysis > summary").click();
    await page.locator('[data-diagnostic-tab="channels"]').click();
    const emailRow = page.locator('[data-channel-row="email"]');
    await emailRow.waitFor();
    for (const metric of ["positive_dialogue", "positive_to_order_10d"]) {
      const cell = await emailRow.locator(`[data-channel-metric="${metric}"]`).innerText();
      if (cell.trim() !== "Ej tillämpligt") {
        throw new Error(`${mode}: email row fabricated ${metric}: ${cell}`);
      }
    }

    const bodyText = await page.locator("body").innerText();
    for (const removed of [
      "Jämförbart 10-dagarsutfall",
      "Positiv dialog → order inom 10 dagar – fullständigt utfall",
      "Kontakt – order inom 10 dagar – fullständigt utfall",
    ]) {
      if (bodyText.includes(removed)) {
        throw new Error(`${mode}: removed comparable UI copy is visible: ${removed}`);
      }
    }
    const matrixSection = page.locator(".sc-section", { hasText: "Teamets prioriteringsmatris" }).first();
    if (!(await matrixSection.isVisible()) || !(await matrixSection.innerText()).includes("Kontakt – order inom 10 dagar")) {
      throw new Error(`${mode}: priority matrix is missing or uses the wrong x-axis`);
    }
    if (bodyText.includes("Nästa bästa kunder")) {
      throw new Error(`${mode}: operational customer list leaked into sales coaching`);
    }
    if (bodyText.includes("Peer median")) {
      throw new Error(`${mode}: old peer benchmark label leaked into sales coaching`);
    }
    if (bodyText.toLocaleLowerCase("sv-SE").includes("synkrona")) {
      throw new Error(`${mode}: synchronous wording leaked into sales coaching`);
    }
    for (const internal of ["sync_reached", "sync_positive", "mature_positive", "attribution_eligible", "historical_snapshot", "v2_contacts"]) {
      if (bodyText.includes(internal)) {
        throw new Error(`${mode}: internal analysis name leaked into user copy: ${internal}`);
      }
    }
    const pageOverflows = await page.evaluate(
      () => document.documentElement.scrollWidth > document.documentElement.clientWidth + 1,
    );
    if (pageOverflows) throw new Error(`${mode}: page-level horizontal overflow`);
    if (mode === "mobile") {
      const trendTabHeights = await page.locator(".sc-team-trend-tabs [role=tab]").evaluateAll(
        tabs => tabs.map(tab => tab.getBoundingClientRect().height),
      );
      if (trendTabHeights.length !== 2 || trendTabHeights.some(height => height < 44)) {
        throw new Error(`mobile: trend tabs are not usable touch targets: ${JSON.stringify(trendTabHeights)}`);
      }
      const activeTrendPanel = trendSection.locator(".sc-team-trend-panel:not([hidden])");
      const trendScroller = activeTrendPanel.locator(".sc-team-order-trend-wrap");
      await trendScroller.scrollIntoViewIfNeeded();
      const trendDimensions = await trendScroller.evaluate(element => ({
        scrollWidth: element.scrollWidth,
        clientWidth: element.clientWidth,
        graphWidth: element.querySelector("svg")?.getBoundingClientRect().width || 0,
      }));
      if (!(trendDimensions.scrollWidth > trendDimensions.clientWidth)) {
        throw new Error(`mobile: trend graph is not horizontally scrollable: ${JSON.stringify(trendDimensions)}`);
      }
      const viewerLegend = activeTrendPanel.locator('.sc-team-order-legend-item[data-seller="viewer"]');
      const legendHeight = await viewerLegend.evaluate(element => element.getBoundingClientRect().height);
      if (legendHeight < 44) throw new Error(`mobile: trend legend touch target is only ${legendHeight}px high`);
      await Promise.all([
        page.waitForResponse(response => response.url().includes("/sales-coaching-insights?") && response.url().includes("seller=viewer")),
        viewerLegend.click(),
      ]);
      const selectedViewerLegend = trendSection.locator('.sc-team-trend-panel:not([hidden]) .sc-team-order-legend-item[data-seller="viewer"].is-selected');
      await selectedViewerLegend.waitFor();
      if (!(await selectedViewerLegend.isVisible())) throw new Error("mobile: trend legend did not select viewer");
      if (await selectedViewerLegend.getAttribute("data-series-style") !== seriesStyles.viewer) {
        throw new Error("mobile: viewer series style changed after seller selection and rerender");
      }
    }
    if (browserErrors.length) {
      throw new Error(`${mode}: browser errors: ${JSON.stringify(browserErrors)}`);
    }
    await page.goto("http://127.0.0.1:5065/?sales_coaching=1&period=4&start=2026-07-01&end=2026-07-28", {
      waitUntil: "networkidle",
    });
    if (await page.locator("#sc-period").inputValue() !== "custom" || !(await page.locator("#sc-custom-dates").isVisible())) {
      throw new Error(`${mode}: custom direct-link period was not restored`);
    }
    console.log(`${mode} sales-coaching smoke passed`);
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
