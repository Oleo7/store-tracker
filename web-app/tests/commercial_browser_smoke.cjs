const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const mode = process.argv[2] || "desktop";
const base = process.env.COMMERCIAL_HARNESS_URL || "http://127.0.0.1:5072";

(async () => {
  let browser;
  try { browser = await chromium.launch({ headless: true }); }
  catch { browser = await chromium.launch({ headless: true, channel: "chrome" }); }
  try {
    const page = await browser.newPage({ viewport: mode === "mobile"
      ? { width: 390, height: 844 } : { width: 1440, height: 1000 } });
    const errors = [];
    page.on("pageerror", error => errors.push(String(error)));
    const login = await page.request.post(`${base}/login`, { data: { user_name: "olle", password: "secret" } });
    assert.equal(login.status(), 200);
    await page.goto(`${base}/__harness__/role/olle`, { waitUntil: "networkidle" });
    await page.locator("#planning-mode-btn").click();
    await page.locator("#planning-recommendation-plan").waitFor();
    const card = await page.locator(".planning-recommendation-card").innerText();
    assert.match(card, /Återaktivera kund efter första ordern/);
    assert.match(card, /Senaste order: 120 DFP/);
    assert.match(card, /Alex/);
    assert.match(card, /Kunden vill diskutera sortimentet/);
    assert.equal(await page.locator("#planning-recommendation-contact").isEnabled(), true);
    await page.locator("#planning-recommendation-plan").click();
    assert.equal(await page.locator('input[name="planning-editor-type"]:checked').inputValue(), "phone");
    await page.locator('.planning-type-choice:has(input[value="visit"])').click();
    await page.locator("#planning-editor-appointment").check();
    await page.locator("#planning-editor-picking-help").check();
    const tomorrow = new Date(Date.now() + 86400000).toISOString().slice(0, 10);
    await page.locator("#planning-editor-date").fill(tomorrow);
    await page.locator("#planning-editor-time").fill("13:00");
    const saved = page.waitForResponse(response => response.url().includes("/planning/suggestions/")
      && response.url().endsWith("/plan") && response.request().method() === "POST");
    await page.locator("#planning-editor-save").click();
    const response = await saved;
    assert.equal(response.status(), 201);
    const payload = await response.json();
    assert.equal(payload.activity.contact_type, "visit");
    assert.equal(payload.activity.appointment_confirmed, true);
    assert.equal(payload.activity.picking_help, true);
    assert.equal(payload.activity.recommended_contact_type, "phone");
    assert.deepEqual(errors, []);
    console.log(`${mode}: v2.2 reactivation, contact context, channel override and confirmed picking-help visit PASS`);
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
