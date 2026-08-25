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
      "mogna positiva dialoger",
      "mogna nådda kontakter",
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
    console.log(`${mode} sales-coaching smoke passed`);
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
