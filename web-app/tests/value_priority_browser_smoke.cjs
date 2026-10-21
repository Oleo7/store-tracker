// Full browser regression of the customer activity loop against synthetic CRM.
const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');
const base=process.env.VALUE_PRIORITY_HARNESS_URL||'http://127.0.0.1:5065';
assert.ok(['127.0.0.1','localhost'].includes(new URL(base).hostname));
async function ready(page){await page.waitForFunction(()=>typeof insightsLoaded!=='undefined'&&insightsLoaded&&Object.keys(insights.__by_customer_id||{}).length>0);}
async function planning(page){await page.evaluate(()=>openPlanningView());await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading&&planningAttention.loaded);}
async function card(page,name){await page.evaluate(()=>{closePlanningModal();showView('list');});await page.locator('[data-work-filter="all"]').click();await page.locator('#search-input').fill(name);await page.waitForFunction(name=>searchQuery===name,name);return page.locator('#customer-list .customer-card').filter({has:page.locator('.customer-name',{hasText:name})});}
async function snap(page){const response=await page.request.get(base+'/__test__/priority-workflow-state');assert.equal(response.status(),200);return response.json();}
(async()=>{const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_EXECUTABLE?{executablePath:process.env.CHROMIUM_EXECUTABLE}:{})});try{
for(const width of [390,1280]){
 const context=await browser.newContext({viewport:{width,height:900},timezoneId:'America/Los_Angeles'});
 const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(String(e)));
 await page.goto(base+'/__test__/login');await ready(page);
 const response=await page.request.post(base+'/__test__/priority-workflow?pagination=1');assert.equal(response.status(),200);const fixture=await response.json();
 await page.reload();await ready(page);
 let target=await card(page,'QA Bokad');assert.equal(await target.locator('.score-pair-item').count(),1);
 assert.equal(await target.locator('[data-customer-workflow]').count(),0);
 assert.equal(await page.locator('[data-customer-decision]').count(),0);
 await planning(page);
 assert.equal(await page.locator('[data-attention-action="review"]').count(),0);
 // Sixty-two legacy follow-ups belong to one customer row, grouped before pagination.
 const promise=page.locator('#planning-backlog .planning-attention-item').filter({has:page.locator('.planning-backlog-customer',{hasText:'QA Löfte'})});
 assert.equal(await promise.count(),1);assert.match(await promise.innerText(),/äldre missade/);
 await promise.locator('[data-attention-action="log"]').click();
 await page.locator('#view-form.active').waitFor();
 for(const id of ['f-obligation-field','f-obligation-outcome','f-permission-fields','f-followup-agreement'])assert.equal(await page.locator('#'+id).count(),0);
 await page.locator('#f-channel').selectOption({label:'Telefon'});await page.locator('#f-result').selectOption({label:'Ej anträffbar'});
 await page.locator('#f-comment').fill('Syntetiskt verkligt kontaktförsök');
 // Browser and backend agree on Stockholm time, even with another browser timezone.
 await page.locator('#form-save-btn').click();
 await page.waitForFunction(()=>!document.getElementById('view-form').classList.contains('active')&&!planningAttentionLoading);
 await page.waitForFunction(()=>!planningAttention.items.some(i=>i.customer==='QA Löfte'));
 const evidence=await snap(page);const logged=evidence.contacts.filter(c=>c.customer==='QA Löfte'&&c.comment==='Syntetiskt verkligt kontaktförsök');assert.equal(logged.length,1);
 // An old valid service follow-up uses the same skip action without creating a contact.
 const old=page.locator('#planning-backlog .planning-attention-item').filter({has:page.locator('.planning-backlog-customer',{hasText:'QA Granska'})});
 await old.locator('[data-attention-action="skip"]').click();await page.waitForFunction(()=>!planningAttention.items.some(i=>i.customer==='QA Granska'));
 const skipped=await snap(page);assert.equal(skipped.contacts.length,evidence.contacts.length);
 // A real booking keeps appointment and picking fields, no separate agreement.
 target=await card(page,'QA Stort A');await target.locator('.planning-card-plan-btn').click();
 await page.locator('#planning-editor-form').waitFor();assert.equal(await page.locator('#planning-editor-agreement').count(),0);
 await page.locator('#planning-editor-date').fill(await page.evaluate(()=>planningAddDays(planningTodayKey(),1)));
 await page.locator('#planning-editor-time').fill('14:00');await page.locator('#planning-editor-note').fill('Syntetisk demo');
 await page.locator('#planning-editor-appointment').check();await page.locator('#planning-editor-picking-help').check();
 await page.locator('#planning-editor-save').click();await page.waitForFunction(()=>!document.getElementById('planning-modal-backdrop').classList.contains('open'));
 await page.waitForFunction(()=>getCustomerInsight(customers.find(c=>c.customer==='QA Stort A')).list_guidance.status_key==='planned');
 const planned=(await snap(page)).activities.find(a=>a.customer==='QA Stort A');assert.equal(planned.appointment_confirmed,'Y');assert.equal(planned.picking_help,'Y');
 // Logging actual historical contact is available during an automatic order wait.
 target=await card(page,'QA Orderpaus');await target.click();await page.locator('#view-detail.active').waitFor();
 assert.equal(await page.locator('#update-contact-btn').isDisabled(),false);await page.locator('#update-contact-btn').click();await page.locator('#view-form.active').waitFor();
 assert.equal(await page.locator('#f-followup-enabled').isChecked(),false);
 await page.locator('#form-cancel-btn').click();
 assert.deepEqual(errors,[]);
 const overflow=await page.evaluate(()=>Array.from(document.querySelectorAll('.view.active .customer-card,.view.active .planning-attention-item')).filter(e=>e.scrollWidth>e.clientWidth+1).map(e=>e.className));assert.deepEqual(overflow,[]);
 if(process.env.VALUE_PRIORITY_SCREENSHOT_DIR){fs.mkdirSync(process.env.VALUE_PRIORITY_SCREENSHOT_DIR,{recursive:true});await page.screenshot({path:path.join(process.env.VALUE_PRIORITY_SCREENSHOT_DIR,`activity-loop-${width}.png`),fullPage:true});}
 console.log(`${width}px: one score, no case menus, grouped legacy work, actual unreachable attempt, skip without contact, booking fields and logging during wait PASS`);
 await context.close();
}
}finally{await browser.close();}})().catch(e=>{console.error(e);process.exit(1);});
