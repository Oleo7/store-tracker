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
    page.on("console", message => {
      if (message.type() === "error") browserErrors.push(message.text());
    });
    page.on("pageerror", error => browserErrors.push(String(error)));
    await page.goto("http://127.0.0.1:5065/?sales_coaching=1&period=4&seller=olle", {
      waitUntil: "networkidle",
    });
    await page.locator("#sales-coaching-dashboard:not([hidden])").waitFor();
    await page.locator(".sc-kpi-card").first().waitFor();
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
    if (!contactOrderText.includes("Preliminärt · 22 väntar på slutligt 10-dagarsutfall")) {
      throw new Error(`${mode}: contact pending copy/count is wrong: ${contactOrderText}`);
    }
    if (!contactOrderText.includes("Jämförbart 10-dagarsutfall: 40 %") || !contactOrderText.includes("4 av 10 med fullständigt 10-dagarsutfall")) {
      throw new Error(`${mode}: comparable contact outcome is missing or wrong: ${contactOrderText}`);
    }
    const positiveOrderCard = page.locator('.sc-kpi-card[data-kpi-key="positive_to_order_10d"]');
    const positiveOrderText = await positiveOrderCard.innerText();
    if (!positiveOrderText.includes("25 %") || !positiveOrderText.includes("7 av 28") || !positiveOrderText.includes("positiva dialoger har följts av order")) {
      throw new Error(`${mode}: provisional positive KPI does not use the full eligible cohort: ${positiveOrderText}`);
    }
    if (!positiveOrderText.includes("Preliminärt · 15 väntar på slutligt 10-dagarsutfall")) {
      throw new Error(`${mode}: positive pending copy/count is wrong: ${positiveOrderText}`);
    }
    if (!positiveOrderText.includes("Jämförbart 10-dagarsutfall: 40 %") || !positiveOrderText.includes("4 av 10 med fullständigt 10-dagarsutfall")) {
      throw new Error(`${mode}: comparable positive outcome is missing or wrong: ${positiveOrderText}`);
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
    await contactOrderCard.locator(".sc-kpi-comparable").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const comparableOrderDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!comparableOrderDrawerText.includes("Visar 10 av 10") || comparableOrderDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: comparable contact drilldown is not mature-only: ${comparableOrderDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    await positiveOrderCard.locator(".sc-kpi-comparable").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const comparablePositiveDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!comparablePositiveDrawerText.includes("Visar 10 av 10") || comparablePositiveDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: comparable positive drilldown is not mature-only: ${comparablePositiveDrawerText}`);
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
    const positiveOrderIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("positiv → order 10 dagar"));
    const contactOrderIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("kontakt – order inom 10 dagar"));
    const nextStepIndex = normalizedTeamHeaders.findIndex(text => text.startsWith("nästa-steg-täckning"));
    if (!(positiveOrderIndex + 1 === contactOrderIndex && contactOrderIndex + 1 === nextStepIndex)) {
      throw new Error(`${mode}: contact-order metric is in the wrong team-comparison position: ${JSON.stringify(teamHeaders)}`);
    }
    const olleTeamRow = page.locator(".sc-comparison-table tbody tr", { hasText: "Olle" });
    const olleTeamText = await olleTeamRow.innerText();
    if ((olleTeamText.match(/40 %/g) || []).length < 2 || olleTeamText.includes("20 %") || olleTeamText.includes("25 %")) {
      throw new Error(`${mode}: team comparison does not use comparable 10-day outcomes: ${olleTeamText}`);
    }

    const sectionHeadings = await page.locator("#sc-dashboard-content > .sc-section > .sc-section-heading h2").allInnerTexts();
    const teamComparisonPosition = sectionHeadings.indexOf("Teamjämförelse");
    const trendPosition = sectionHeadings.indexOf("Kontakt – order inom 10 dagar – trend");
    const matricesPosition = sectionHeadings.indexOf("Teamets coachningsmatriser");
    if (!(teamComparisonPosition >= 0 && teamComparisonPosition + 1 === trendPosition && trendPosition + 1 === matricesPosition)) {
      throw new Error(`${mode}: long-term trend is in the wrong section order: ${JSON.stringify(sectionHeadings)}`);
    }
    const trendSection = page.locator(".sc-team-order-trend-section");
    await trendSection.scrollIntoViewIfNeeded();
    const trendCopy = await trendSection.innerText();
    for (const expected of [
      "Fullständiga 10-dagarsutfall per kontaktvecka för de senaste 16 mogna veckorna",
      "Varje punkt avser endast den aktuella kontaktveckan",
      "Period-, säljar- och kanalfilter begränsar inte grafen",
    ]) {
      if (!trendCopy.includes(expected)) throw new Error(`${mode}: missing trend explanation: ${expected}`);
    }
    const weekSlotCount = await trendSection.locator(".sc-team-order-x-label").count();
    if (weekSlotCount !== 16) throw new Error(`${mode}: expected 16 trend week slots, got ${weekSlotCount}`);
    const yLabels = await trendSection.locator(".sc-team-order-y-label").allTextContents();
    if (JSON.stringify(yLabels) !== JSON.stringify(["0 %", "25 %", "50 %", "75 %", "100 %"])) {
      throw new Error(`${mode}: trend scale is not fixed at 0/25/50/75/100: ${JSON.stringify(yLabels)}`);
    }
    if (await trendSection.locator(".sc-team-order-legend-item").count() !== 3) {
      throw new Error(`${mode}: expected one trend series for each of three active sellers`);
    }
    if (await trendSection.locator(".sc-team-order-line").count() < 3) {
      throw new Error(`${mode}: expected at least three rendered seller trend lines`);
    }
    const olleTrendPoint = trendSection.locator('.sc-team-order-point[data-seller="olle"][data-numerator="5"][data-denominator="10"]');
    const sofiaTrendPoint = trendSection.locator('.sc-team-order-point[data-seller="sofia"][data-numerator="2"][data-denominator="10"]');
    const viewerTrendPoint = trendSection.locator('.sc-team-order-point[data-seller="viewer"][data-numerator="4"][data-denominator="8"]');
    for (const [seller, point] of [["olle", olleTrendPoint], ["sofia", sofiaTrendPoint], ["viewer", viewerTrendPoint]]) {
      if (await point.count() !== 1) throw new Error(`${mode}: missing deterministic ${seller} trend point`);
    }
    if (!(await olleTrendPoint.getAttribute("class")).includes("is-selected")) {
      throw new Error(`${mode}: selected seller is not highlighted in the trend`);
    }
    if (!(await viewerTrendPoint.getAttribute("class")).includes("is-small-sample")) {
      throw new Error(`${mode}: small-sample trend point is not hollow/muted`);
    }
    await viewerTrendPoint.hover();
    const viewerTooltip = await viewerTrendPoint.locator("title").textContent();
    if (!viewerTooltip.includes("viewer") || !viewerTooltip.includes("50 %") || !viewerTooltip.includes("4 av 8") || !viewerTooltip.includes("Litet underlag") || !viewerTooltip.match(/\d{4} v\.\d+/)) {
      throw new Error(`${mode}: trend point tooltip lacks seller/week/value/evidence: ${viewerTooltip}`);
    }
    const pointStart = await viewerTrendPoint.getAttribute("data-start");
    const pointEnd = await viewerTrendPoint.getAttribute("data-end");
    const pointResponse = page.waitForResponse(response => {
      const url = new URL(response.url());
      return url.pathname.endsWith("/sales-coaching-insights/drilldown")
        && url.searchParams.get("metric") === "order_10d_comparable"
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

    const diagnosticTabs = await page.locator(".sc-diagnostic-tabs [role=tab]").allInnerTexts();
    if (diagnosticTabs[0] !== "Besök" || diagnosticTabs[1] !== "Konvertering") {
      throw new Error(`${mode}: diagnostic tabs are in the wrong order`);
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
    await assertKeyboardTab("conversion", "Konvertering after ArrowRight from Besök");
    await page.locator('[data-diagnostic-tab="conversion"]').press("Home");
    await assertKeyboardTab("visits", "Besök after Home");
    await page.locator('[data-diagnostic-tab="visits"]').press("ArrowLeft");
    await assertKeyboardTab("priority", "Prioritering after ArrowLeft from Besök");
    await page.locator('[data-diagnostic-tab="priority"]').press("Home");
    await assertKeyboardTab("visits", "Besök after Home from Prioritering");
    await page.locator('[data-diagnostic-tab="visits"]').press("End");
    await assertKeyboardTab("priority", "Prioritering after End");

    await page.locator("#sc-quality-details > summary").click();
    const glossaryLabels = await page.locator(".sc-glossary dt").allInnerTexts();
    const sortedLabels = [...glossaryLabels].sort(new Intl.Collator("sv-SE", { sensitivity: "base" }).compare);
    if (JSON.stringify(glossaryLabels) !== JSON.stringify(sortedLabels)) {
      throw new Error(`${mode}: metric glossary is not sorted in Swedish order`);
    }
    if (glossaryLabels.length < 12) {
      throw new Error(`${mode}: metric glossary is unexpectedly incomplete`);
    }

    await Promise.all([
      page.waitForResponse(response => response.url().includes("/sales-coaching-insights?") && response.url().includes("channel=email")),
      page.locator("#sc-channel").selectOption("email"),
    ]);
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
    await page.locator('[data-diagnostic-tab="channels"]').click();
    const emailRow = page.locator('[data-channel-row="email"]');
    await emailRow.waitFor();
    for (const metric of ["positive_dialogue", "positive_to_order_10d_comparable"]) {
      const cell = await emailRow.locator(`[data-channel-metric="${metric}"]`).innerText();
      if (cell.trim() !== "Ej tillämpligt") {
        throw new Error(`${mode}: email row fabricated ${metric}: ${cell}`);
      }
    }

    const bodyText = await page.locator("body").innerText();
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
      const trendScroller = page.locator(".sc-team-order-trend-wrap");
      await trendScroller.scrollIntoViewIfNeeded();
      const trendDimensions = await trendScroller.evaluate(element => ({
        scrollWidth: element.scrollWidth,
        clientWidth: element.clientWidth,
        graphWidth: element.querySelector("svg")?.getBoundingClientRect().width || 0,
      }));
      if (!(trendDimensions.scrollWidth > trendDimensions.clientWidth)) {
        throw new Error(`mobile: trend graph is not horizontally scrollable: ${JSON.stringify(trendDimensions)}`);
      }
      const viewerLegend = page.locator('.sc-team-order-legend-item[data-seller="viewer"]');
      const legendHeight = await viewerLegend.evaluate(element => element.getBoundingClientRect().height);
      if (legendHeight < 44) throw new Error(`mobile: trend legend touch target is only ${legendHeight}px high`);
      await Promise.all([
        page.waitForResponse(response => response.url().includes("/sales-coaching-insights?") && response.url().includes("seller=viewer")),
        viewerLegend.click(),
      ]);
      const selectedViewerLegend = page.locator('.sc-team-order-legend-item[data-seller="viewer"].is-selected');
      await selectedViewerLegend.waitFor();
      if (!(await selectedViewerLegend.isVisible())) throw new Error("mobile: trend legend did not select viewer");
      const matrixScroller = page.locator(".sc-matrix-wrap").first();
      await matrixScroller.scrollIntoViewIfNeeded();
      if (!(await matrixScroller.isVisible())) throw new Error("mobile: matrix scroller missing");
    }
    if (browserErrors.length) {
      throw new Error(`${mode}: browser errors: ${JSON.stringify(browserErrors)}`);
    }
    console.log(`${mode} sales-coaching smoke passed`);
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
