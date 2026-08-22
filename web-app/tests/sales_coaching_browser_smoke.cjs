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
    await page.goto("http://127.0.0.1:5065/?sales_coaching=1&period=4", {
      waitUntil: "networkidle",
    });
    await page.locator("#sales-coaching-dashboard:not([hidden])").waitFor();
    await page.locator(".sc-kpi-card").first().waitFor();
    const kpiCount = await page.locator(".sc-kpi-card").count();
    if (kpiCount !== 4) throw new Error(`${mode}: expected 4 KPI cards, got ${kpiCount}`);
    const bodyText = await page.locator("body").innerText();
    if (bodyText.includes("Nästa bästa kunder")) {
      throw new Error(`${mode}: operational customer list leaked into sales coaching`);
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
