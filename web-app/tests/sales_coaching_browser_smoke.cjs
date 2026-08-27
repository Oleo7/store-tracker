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
    const page = await browser.newPage({ viewportSize: viewport });
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
    if (!contactOrderText.includes("Preliminärt · 28 väntar på slutligt 10-dagarsutfall")) {
      throw new Error(`${mode}: contact pending copy/count is wrong: ${contactOrderText}`);
    }
    const positiveOrderCard = page.locator('.sc-kpi-card[data-kpi-key="positive_to_order_10d"]');
    const positiveOrderText = await positiveOrderCard.innerText();
    if (!positiveOrderText.includes("25 %") || !positiveOrderText.includes("7 av 28") || !positiveOrderText.includes("positiva dialoger har följts av order")) {
      throw new Error(`${mode}: provisional positive KPI does not use the full eligible cohort: ${positiveOrderText}`);
    }
    if (!positiveOrderText.includes("Preliminärt · 21 väntar på slutligt 10-dagarsutfall")) {
      throw new Error(`${mode}: positive pending copy/count is wrong: ${positiveOrderText}`);
    }
    await contactOrderCard.locator(".sc-kpi-main").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const orderDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!orderDrawerText.includes("Visar 35 av 35") || !orderDrawerText.includes("EARLY-ORDER-1") || !orderDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: eligible contact drilldown does not match KPI denominator/outcomes: ${orderDrawerText}`);
    }
    await page.locator("[data-sc-drawer-close]").click();
    await positiveOrderCard.locator(".sc-kpi-main").click();
    await page.locator("#sc-drawer-backdrop").waitFor();
    await page.locator("#sc-drawer-content .sc-drawer-meta").waitFor();
    const positiveDrawerText = await page.locator("#sc-drawer-content").innerText();
    if (!positiveDrawerText.includes("Visar 28 av 28") || !positiveDrawerText.includes("EARLY-ORDER-1") || !positiveDrawerText.includes("Väntar på utfall")) {
      throw new Error(`${mode}: eligible positive drilldown does not match KPI denominator/outcomes: ${positiveDrawerText}`);
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
    for (const metric of ["positive_dialogue", "positive_to_order_10d"]) {
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
