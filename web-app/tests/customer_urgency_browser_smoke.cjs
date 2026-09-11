// Run against planning_browser_harness.py (BROWSER_HARNESS_PORT=5072).
// Requires Playwright; override the server with COMMERCIAL_HARNESS_URL.
const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const base = process.env.COMMERCIAL_HARNESS_URL || "http://127.0.0.1:5072";

(async () => {
  let browser;
  try { browser = await chromium.launch({ headless: true }); }
  catch { browser = await chromium.launch({ headless: true, channel: "chrome" }); }
  try {
    for (const [mode, viewport] of Object.entries({ desktop: { width: 1440, height: 1000 }, mobile: { width: 390, height: 844 } })) {
      const page = await browser.newPage({ viewport });
      const errors = [];
      page.on("pageerror", error => errors.push(String(error)));
      const login = await page.request.post(`${base}/login`, { data: { user_name: "olle", password: "secret" } });
      assert.equal(login.status(), 200);
      await page.goto(`${base}/__harness__/role/olle`, { waitUntil: "networkidle" });
      await page.locator(".customer-card .next-action").first().waitFor();
      const results = await page.evaluate(() => {
        const cases = [
          ["urgent", "red", "rgb(255, 243, 244)"],
          ["warning", "yellow", "rgb(255, 248, 214)"],
          ["positive", "yellow", "rgb(255, 248, 214)"],
          ["opportunity", "yellow", "rgb(255, 248, 214)"],
          ["neutral", "yellow", "rgb(255, 248, 214)"],
          ["low", "green", "rgb(239, 250, 244)"],
          [undefined, "yellow", "rgb(255, 248, 214)"],
          ["unknown", "yellow", "rgb(255, 248, 214)"],
        ];
        const customer = customers[0];
        const insight = getCustomerInsight(customer);
        const original = structuredClone(insight);
        const results = [];
        for (const [tone, color, background] of cases) {
          for (const [level, score, priorityClass] of [["Hög prio", 90, "high"], ["Medel prio", 50, "medium"], ["Låg prio", 10, "low"], [null, null, "neutral"]]) {
            Object.assign(insight, { priority_score: score, priority_level: level, recommended_channel: "avvakta", next_action: { tone, label: "Bevaka planerad uppföljning", reason: "Planerad uppföljning om 25 dagar" } });
            const before = JSON.stringify(insight);
            renderList();
            const card = document.querySelector(`.customer-card[data-row="${customer.row}"]`);
            const action = card.querySelector(".next-action");
            results.push({ tone, color, background, level, score, priorityClass,
              cardClass: card.className, border: getComputedStyle(card).borderColor,
              actionClass: action.className, actualBackground: getComputedStyle(action).backgroundColor,
              actionBorder: getComputedStyle(action).borderColor, text: action.textContent,
              summary: card.querySelector(".priority-summary").textContent,
              unchanged: before === JSON.stringify(insight),
              fits: card.scrollWidth <= card.clientWidth && action.scrollWidth <= action.clientWidth,
            });
          }
        }
        Object.assign(insight, original);
        renderList();
        return results;
      });
      for (const r of results) {
        assert.match(r.cardClass, new RegExp(`priority-${r.priorityClass}`));
        assert.equal(r.border, "rgb(200, 205, 215)");
        assert.match(r.actionClass, new RegExp(`next-action-${r.color}`));
        assert.equal(r.actualBackground, r.background);
        assert.equal(r.actionBorder, results[0].actionBorder);
        assert.match(r.text, /Nästa: Bevaka planerad uppföljning · Avvakta/);
        assert.match(r.text, /Status: Planerad uppföljning om 25 dagar/);
        assert.doesNotMatch(r.text, /Varför nu:/);
        assert.equal(r.summary, r.score === null ? "— poäng · Prioritet saknas" : `${r.score} poäng · ${r.level}`);
        assert.equal(r.unchanged, true);
        assert.equal(r.fits, true);
      }
      assert.deepEqual(errors, []);
      console.log(`${mode}: ${results.length} tone/priority combinations PASS; neutral borders, backgrounds, Status, unchanged scores and no card overflow`);
      await page.close();
    }
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
