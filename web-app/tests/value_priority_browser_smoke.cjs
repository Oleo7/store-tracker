// Real Playwright smoke against the isolated, never-production CRM harness.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const base = process.env.VALUE_PRIORITY_HARNESS_URL || 'http://127.0.0.1:5065';

async function assertVisibleRanking(page, mode) {
  const detail = await page.evaluate(() => {
    const c = getBaseFilteredCustomers();
    const expected = c.slice(0, Math.min(20, c.length)).map(row => String(row.customer_id));
    const cards = [...document.querySelectorAll('#customer-list .customer-card')];
    const actual = cards.map(card => {
      const row = customers.find(c => Number(c.row) === Number(card.dataset.row));
      return row ? String(row.customer_id) : '';
    });
    const scoresVisible = cards.every(card => {
      const scoreText = card.querySelector('.score-pair')?.textContent || '';
      return scoreText.includes('Kontaktprioritet') &&
        scoreText.includes('Affärsvärde') && !scoreText.includes('—');
    });
    const byId = insights?.__by_customer_id || {};
    return {expected, actual, scoresVisible, n: Object.keys(byId).length,
      total: c.length, mode: listSortMode};
  });
  assert.equal(detail.mode,mode, 'real sort control mode');
  assert.ok(detail.n > 0, 'API must deliver ID-specific scoring');
  assert.ok(detail.total > 0, 'fixture must supply stores');
  assert.deepEqual(detail.actual, detail.expected, 'visible cards follow score sort');
  assert.ok(detail.scoresVisible, 'every visible store displays both scores');
  return detail;
}

(async () => {
  const browser = await chromium.launch({headless:true,
    ...(process.env.CHROMIUM_EXECUTABLE ? {executablePath:process.env.CHROMIUM_EXECUTABLE} : {})});
  try {
    for(const width of [390, 1280]) {
      const page = await browser.newPage({viewport:{width,height:900}});
      const errors = [];
      page.on('pageerror',e=>errors.push(String(e)));
      await page.goto(base + '/__test__/login',{waitUntil:'domcontentloaded'});
      await page.waitForFunction(() => typeof insightsLoaded !== 'undefined' &&
        insightsLoaded && insights?.__by_customer_id &&
        Object.keys(insights.__by_customer_id).length > 0);
      await assertVisibleRanking(page,'contact');
      await page.locator('#list-sort-mode').selectOption('value');
      await assertVisibleRanking(page,'value');
      await page.locator('#list-sort-mode').selectOption('contact');
      await assertVisibleRanking(page,'contact');
      // The old planning score and calendar remain untouched by switching.
      const noModelError = await page.evaluate(() => {
        const first = getBaseFilteredCustomers()[0];
        const insight = getCustomerInsight(first);
        return insight?.priority_score !== undefined &&
          insight?.business_value_raw !== undefined &&
          insight?.contact_priority_raw !== undefined;
      });
      assert.ok(noModelError, 'legacy and new read models both present');
      assert.deepEqual(errors,[], 'no browser JS errors');
      console.log(width + 'px: ID-backed two-score display and both sorting modes PASS');
      await page.close();
    }
  } finally {
    await browser.close();
  }
})().catch(error=>{console.error(error);process.exitCode=1});
