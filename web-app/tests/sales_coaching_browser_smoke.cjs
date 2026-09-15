const fs = require("fs");
const path = require("path");
const { chromium } = require("playwright");

const mode = process.argv[2] || "desktop";
const viewport = mode === "mobile"
  ? { width: 390, height: 844 }
  : { width: 1440, height: 1000 };
const outputDir = path.resolve("outputs", "sales-coaching-linked-result");

function equal(actual, expected, message) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${mode}: ${message}\nExpected: ${JSON.stringify(expected)}\nActual: ${JSON.stringify(actual)}`);
  }
}

async function assertTabs(section, group, expectedLabels) {
  const tabs = section.locator('[role="tab"]');
  equal(await tabs.allInnerTexts(), expectedLabels, `${group} tab order changed`);
  if (await section.locator('[role="tabpanel"]:not([hidden])').count() !== 1) {
    throw new Error(`${mode}: ${group} must expose exactly one panel`);
  }
  const requestCountBefore = global.summaryRequestCount;
  for (let index = 0; index < expectedLabels.length; index += 1) {
    const tab = tabs.nth(index);
    await tab.click();
    const controlledId = await tab.getAttribute("aria-controls");
    const panel = section.locator(`#${controlledId}`);
    if (await tab.getAttribute("aria-selected") !== "true" || !(await panel.isVisible())) {
      throw new Error(`${mode}: ${group} tab ${expectedLabels[index]} did not activate its panel`);
    }
    if (await section.locator('[role="tabpanel"]:not([hidden])').count() !== 1) {
      throw new Error(`${mode}: ${group} exposed multiple panels after a tab click`);
    }
  }
  if (global.summaryRequestCount !== requestCountBefore) {
    throw new Error(`${mode}: ${group} tab switches made a new summary request`);
  }
}

(async () => {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (error) {
    if (!String(error).includes("Executable doesn't exist")) throw error;
    browser = await chromium.launch({ headless: true, channel: "chrome" });
  }

  try {
    fs.mkdirSync(outputDir, { recursive: true });
    const page = await browser.newPage({ viewport });
    const browserErrors = [];
    global.summaryRequestCount = 0;
    page.on("console", message => {
      if (message.type() === "error") browserErrors.push(message.text());
    });
    page.on("pageerror", error => browserErrors.push(String(error)));
    page.on("request", request => {
      if (new URL(request.url()).pathname.endsWith("/sales-coaching-insights")) {
        global.summaryRequestCount += 1;
      }
    });

    // Authenticate the shared browser context before the first page requests /session.
    const login = await page.request.post("http://127.0.0.1:5065/login", {
      data: { user_name: "admin", password: "secret" },
    });
    if (!login.ok()) throw new Error(`${mode}: harness login failed: ${await login.text()}`);


    page.on("response", response => {
      if (response.status() === 401) browserErrors.push(`Unexpected 401 after login: ${response.url()}`);
    });
    await page.goto("http://127.0.0.1:5065/?sales_coaching=1&period=4&seller=olle", {
      waitUntil: "networkidle",
    });
    await page.locator("#sales-coaching-dashboard:not([hidden])").waitFor();
    await page.locator(".sc-comparison-table").waitFor();

    const headings = await page.locator("#sc-dashboard-content > .sc-section > .sc-section-heading h2").allInnerTexts();
    equal(headings.slice(0, 5), [
      "Teamjämförelse",
      "Coachningskort",
      "Försäljning-trend",
      "Mänskliga aktiviteter – trend",
      "Historiskt prioritetsfokus",
    ], "main section order changed");

    const teamSection = page.locator('[aria-labelledby="sc-team-title"]');
    const teamHeader = teamSection.locator(":scope > .sc-section-heading");
    if ((await teamHeader.locator("p").count()) !== 0) {
      throw new Error(`${mode}: the removed explanatory paragraph still appears below Teamjämförelse`);
    }
    const visibleTeamHeaders = await page.locator(".sc-comparison-table thead th").evaluateAll(headers => headers.map(header => {
      const copy = header.cloneNode(true);
      copy.querySelectorAll("button, [hidden]").forEach(element => element.remove());
      return copy.textContent.trim();
    }));
    equal(visibleTeamHeaders.slice(0, 4), [
      "Säljare",
      "Aktiviteter",
      "Säljkopplat resultat",
      "Kontakter med orderutfall inom 10 dagar",
    ], "team result columns changed");
    const resultInfo = page.locator('.sc-comparison-table [aria-label="Förklaring för Säljkopplat resultat"]');
    const expectedDefinition = "Modellerat täckningsbidrag från order kopplade till säljarens kontakt inom 10 dagar samt säljarens egna order som saknar sådan kontaktmatchning. Samma order räknas aldrig två gånger.";
    if (await resultInfo.getAttribute("title") !== expectedDefinition) {
      throw new Error(`${mode}: sales-result definition is not exact`);
    }
    const olleRow = page.locator('.sc-comparison-table tbody tr', { has: page.locator('button[data-seller="olle"]') });
    const olleResultText = await olleRow.locator("td").nth(1).innerText();
    if (!olleResultText.includes("kr") || !olleResultText.includes("Via kontakt:") || !olleResultText.includes("Egna order utan kontakt:")) {
      throw new Error(`${mode}: result cell does not expose total and components: ${olleResultText}`);
    }

    const salesSection = page.locator(".sc-team-10d-trend-section");
    const salesCopy = await salesSection.innerText();
    for (const expected of [
      "kontaktveckan för kontaktattribuerade order",
      "orderveckan för egna order utan kontaktmatchning",
    ]) {
      if (!salesCopy.includes(expected)) throw new Error(`${mode}: sales trend misses: ${expected}`);
    }
    equal(await salesSection.locator('[role="tab"][aria-selected="true"]').innerText(), "DFP per vecka", "default sales tab");
    for (const [view, target] of [["dfp", 400], ["result", 15000], ["linked-dfp", 120]]) {
      equal(await salesSection.locator(`#sc-team-trend-panel-${view} .sc-trend-target`).getAttribute("data-target"), String(target), "reference level");
    }
    await assertTabs(salesSection, "sales trend", [
      "DFP per vecka",
      "Säljkopplat TB",
      "Säljkopplade DFP per säljare/vecka",
      "Kontakter med orderutfall inom 10 dagar",
      "Kontakt → order",
      "Positiv dialog → order",
    ]);
    await salesSection.locator("#sc-team-trend-tab-result").click();
    const resultLabels = await salesSection.locator("#sc-team-trend-panel-result .sc-team-order-point").evaluateAll(points => points.map(point => point.getAttribute("aria-label")));
    if (!resultLabels.some(label => label.includes("Säljkopplat TB") && label.includes("Via kontakt") && label.includes("Egna order utan kontakt") && !label.includes("Säljkopplat TB 0 kr"))) {
      throw new Error(`${mode}: result graph does not show a non-zero total with both component labels`);
    }

    const activitySection = page.locator(".sc-human-activity-trend-section");
    if (!(await activitySection.innerText()).includes("helt avslutad ISO-vecka")) {
      throw new Error(`${mode}: activity trend maturity explanation is missing`);
    }
    await assertTabs(activitySection, "activity trend", ["Alla", "Nådda besök", "Bom", "Telefon"]);
    await activitySection.locator("#sc-activity-trend-tab-all").click();
    const activityLabels = await activitySection.locator("#sc-activity-trend-panel-all .sc-team-order-point").evaluateAll(points => points.map(point => point.getAttribute("aria-label")));
    if (!activityLabels.some(label => label.includes("mänskliga aktiviteter"))) {
      throw new Error(`${mode}: activity trend point labels are missing`);
    }

    if (mode === "mobile") {
      const layout = await page.evaluate(() => ({
        bodyOverflow: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        tableScrollable: document.querySelector(".sc-table-wrap").scrollWidth > document.querySelector(".sc-table-wrap").clientWidth,
        salesScrollable: document.querySelector("#sc-team-trend-panel-result .sc-team-order-trend-wrap").scrollWidth > document.querySelector("#sc-team-trend-panel-result .sc-team-order-trend-wrap").clientWidth,
        minTabHeight: Math.min(...[...document.querySelectorAll(".sc-team-trend-tabs [role=tab]")].map(tab => tab.getBoundingClientRect().height)),
      }));
      if (layout.bodyOverflow > 1 || !layout.tableScrollable || !layout.salesScrollable || layout.minTabHeight < 44) {
        throw new Error(`${mode}: responsive contract failed: ${JSON.stringify(layout)}`);
      }
    }

    await teamSection.scrollIntoViewIfNeeded();
    await teamSection.screenshot({ path: path.join(outputDir, `team-comparison-${mode}.png`) });
    await salesSection.scrollIntoViewIfNeeded();
    await salesSection.screenshot({ path: path.join(outputDir, `sales-trend-${mode}.png`) });
    await activitySection.scrollIntoViewIfNeeded();
    await activitySection.screenshot({ path: path.join(outputDir, `activity-trend-${mode}.png`) });

    if (browserErrors.length) {
      throw new Error(`${mode}: browser errors: ${browserErrors.join(" | ")}`);
    }
    console.log(`${mode}: sales coaching browser smoke passed; screenshots in ${outputDir}`);
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exit(1);
});
