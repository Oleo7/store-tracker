// Run against the isolated sales_coaching_browser_harness.py, never production.
const { chromium } = require('playwright');
const { execFileSync } = require('node:child_process');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const base = process.env.GUIDANCE_HARNESS_URL || 'http://127.0.0.1:5065';
const fixtures = JSON.parse(execFileSync(process.env.GUIDANCE_PYTHON || 'python', ['-c', `
import sys,json
sys.path.insert(0,sys.argv[1])
from test_guidance_integrity import customer,order,contact,email,activity,scored,TODAY
cases = {
 'onboarding':dict(store=customer('B'),orders=[order('one',8)]),
 'second-purchase':dict(orders=[order('one',11)]),
 'repeat':dict(orders=[order('one',20),order('two',40),order('three',60)]),
 'stockfiller':dict(orders=[order('one',30)],mail=email()),
 'product-sheet':dict(orders=[order('one',30)],mail=email('product_sheet')),
 'reactivation-click':dict(orders=[order('one',200),order('two',220)],mail=email()),
 'positive-dialogue':dict(orders=[order('one',10),order('two',30)],contacts=[contact(3,'Positiv')]),
 'a-prospect':dict(store=customer('A'),mail=email(),contacts=[contact(10,'Positiv')]),
 'real-followup':dict(contacts=[contact(10,'Positiv',TODAY.isoformat())]),
 'past-and-future':dict(orders=[order('older',30,11),order('latest',10,13),order('next',-4,15),order('later',-11,99)]),
 'future-only':dict(orders=[order('future',-4,15)]),
 'partial-volume':dict(orders=[order('partial',10,8),order('partial',10,None)]),
 'planned':dict(store=customer('A'),planned=[activity(1)]),
 'overdue':dict(store=customer('A'),planned=[activity(-1)]),
 'idle':dict(),
}
print(json.dumps([dict(name=k,item=scored(**v)) for k,v in cases.items()]))
`, __dirname], {encoding:'utf8'}));

(async () => {
  const browser = await chromium.launch({headless:true,
    ...(process.env.CHROMIUM_EXECUTABLE ? {executablePath:process.env.CHROMIUM_EXECUTABLE} : {})});
  try {
    for (const width of [320,390,1440]) {
      const page = await browser.newPage({viewport:{width,height:1000}});
      const errors=[];
      page.on('pageerror', e=>errors.push(String(e)));
      await page.goto(`${base}/__test__/login`, {waitUntil:'domcontentloaded'});
      await page.waitForFunction(()=>typeof customers !== 'undefined' && customers.length && insightsLoaded);
      for (const {name,item} of fixtures) {
        const result=await page.evaluate(({item})=>{
          const c=customers[0];
          const ins=getCustomerInsight(c);
          Object.assign(ins,item);
          customers=[c];
          Object.values(filterState).forEach(set=>set.clear());
          searchQuery=''; emailClickNoOrderActive=false;
          renderList();
          const card=document.querySelector('.customer-card');
          const guidance=item.customer_guidance;
          let planningReason=null;
          if (guidance.status_key==='act_now') {
            planningRecommendationLoading=false;
            planningRecommendationPendingCount=1;
            planningRecommendation={queue_item_type:'suggestion',customer:c.customer,
              customer_id:c.customer_id,customer_guidance:guidance,action_label:guidance.action_label,
              reason_text:guidance.reason_text,status:'pending',contact_context:item};
            renderPlanningRecommendation();
            planningReason=document.querySelector('.planning-recommendation-reason')?.textContent;
          }
          return {
            reason:card.querySelector('.card-reason').textContent,
            action:card.querySelector('.next-action-label').textContent,
            focus:card.querySelector('.guidance-badge').textContent,
            primaryDelivery:card.querySelector('.card-date-primary').textContent,
            allDates:card.querySelector('.card-dates').textContent,
            fits:card.scrollWidth<=card.clientWidth && card.querySelector('.next-action').scrollWidth<=card.querySelector('.next-action').clientWidth,
            planningReason,
          };
        },{item});
        const g=item.customer_guidance;
        assert.equal(result.reason,g.reason_text,`${width}/${name}: reason`);
        assert.ok(result.action.startsWith(g.action_label),`${width}/${name}: action`);
        assert.equal(result.focus,g.focus_label,`${width}/${name}: focus`);
        assert.equal(result.fits,true,`${width}/${name}: overflow`);
        if (g.status_key==='act_now') assert.equal(result.planningReason,g.reason_text,`${width}/${name}: planning`);
        if (name==='past-and-future') {
          assert.match(result.primaryDelivery,/8 sep.*13 DFP/);
          assert.match(result.allDates,/22 sep.*15 DFP/);
          assert.doesNotMatch(result.allDates,/99 DFP/);
        }
        if (name==='future-only') {
          assert.doesNotMatch(result.primaryDelivery,/DFP|F\u00f6rsta leverans/);
          assert.match(result.allDates,/15 DFP/);
        }
        if (name==='partial-volume') assert.doesNotMatch(result.primaryDelivery,/DFP/);
        if (name==='second-purchase') {
          // Click the real modal checkbox; label and filter key must stay decoupled.
          await page.locator('#chip-focus').click();
          await page.locator('#filter-modal-body input[value="second_purchase"]').check();
          await page.locator('#filter-modal-close').click();
          await page.locator('#filter-modal').waitFor({state:'hidden'});
          assert.equal(await page.locator('.customer-card').count(),1);
          await page.locator('#chip-focus').click();
          assert.equal(await page.locator('input[value="second_purchase"]').isChecked(),true);
          await page.locator('#filter-modal-close').click();
          await page.locator('#filter-modal').waitFor({state:'hidden'});
        }
        if (process.env.GUIDANCE_SCREENSHOT_DIR && name==='past-and-future') {
          fs.mkdirSync(process.env.GUIDANCE_SCREENSHOT_DIR,{recursive:true});
          await page.locator('.customer-card').screenshot({path:path.join(process.env.GUIDANCE_SCREENSHOT_DIR,`delivery-${width}.png`)});
        }
      }
      assert.deepEqual(errors,[],`${width}: JavaScript errors`);
      console.log(`${width}px: ${fixtures.length} guidance/delivery scenarios, focus checkbox, planning parity, no overflow PASS`);
      await page.close();
    }
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
