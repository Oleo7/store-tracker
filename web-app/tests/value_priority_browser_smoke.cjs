// Full browser regression of the customer activity loop against synthetic CRM.
const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');
const base=process.env.VALUE_PRIORITY_HARNESS_URL||'http://127.0.0.1:5065';
assert.ok(['127.0.0.1','localhost'].includes(new URL(base).hostname));
async function ready(page){await page.waitForFunction(()=>typeof insightsLoaded!=='undefined'&&insightsLoaded&&Object.keys(insights.__by_customer_id||{}).length>0);}
async function planning(page){await page.evaluate(()=>openPlanningView());await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading&&planningAttention.loaded);}
async function status(page,value){
 await page.locator('#chip-status').click();
 assert.equal(await page.locator('#filter-modal-body input[type=radio]').count(),4);
 await page.locator(`#filter-modal-body input[value="${value}"]`).check();
 assert.equal(await page.locator('#filter-modal-body input:checked').count(),1);
 await page.locator('#filter-modal-close').click();
}
async function card(page,name){await page.evaluate(()=>{closePlanningModal();showView('list');});await status(page,'all');await page.locator('#search-input').fill(name);await page.waitForFunction(name=>searchQuery===name,name);return page.locator('#customer-list .customer-card').filter({has:page.locator('.customer-name',{hasText:name})});}
async function snap(page){const response=await page.request.get(base+'/__test__/priority-workflow-state');assert.equal(response.status(),200);return response.json();}
async function sellerFrontend(page,width){
 // Browser-only model fixtures exercise the actual controls without CRM writes.
 await page.waitForFunction(()=>insightsForegroundRequests===0&&insightsBackgroundRequests===0);
 await page.evaluate(()=>{
  const rows=Array.from({length:24},(_,i)=>({customer_id:`frontend-${i}`,row:600+i,
   customer:i===0?'A låg':i===1?'Z hög':`Butik ${i}`,city:'Göteborg',region_google:i<2?'Väst':'Öst',customer_segment:'A'}));
  rows.push(...['Bokad','Avvakta','Okänd'].map((customer,i)=>({customer,customer_id:`extra-${i}`,row:700+i,city:'Borås',region_google:'Öst',customer_segment:'B'})));
  customers=rows;
  insights={__by_customer_id:Object.fromEntries(rows.slice(0,-1).map((c,i)=>[c.customer_id,{
   value_model_version:'v3.5',work_priority_score:i<2?75:50-i,work_priority_precise:i===0?75.1:i===1?75.4:50-i,
   business_value_score:i===1?87:i===0?0:null,business_value_raw:100-i,
   business_expected_dfp_per_delivery:i===1?12.6:i===0?12.4:null,
   list_guidance:{focus_key:'repeat_purchase',focus_label:'Återköp',status_key:i===24?'planned':i===25?'wait':i%2?'overdue_followup':'act_now',
    status_label:i===24?'Planerad':i===25?'Avvakta':i%2?'Att hantera':'Agera nu',action_label:'Kundarbete',has_planned_contact:true},
  }]))};
  insightsLoaded=true;insightsLoadFailed=false;listWorkFilter='all';searchQuery='';
  Object.values(filterState).forEach(s=>s.clear());
  Object.keys(_chipIds).forEach(updateChip);renderList();
 });
 assert.equal(await page.locator('.chip-row .chip').first().getAttribute('id'),'chip-status');
 for(const id of ['list-sort-mode','work-filter-row'])assert.equal(await page.locator('#'+id).count(),0);
 assert.equal(await page.locator('.priority-why,.list-guidance-help').count(),0);
 const cards=page.locator('#customer-list .customer-card');
 assert.equal(await cards.count(),20);
 assert.match(await cards.first().innerText(),/Z hög[\s\S]*Prioritet\s+75[\s\S]*Affärsvärde: 87[\s\S]*Uppskattad leveransstorlek: 13 DFP/);
 assert.match(await cards.nth(1).innerText(),/A låg[\s\S]*Affärsvärde: 0[\s\S]*12 DFP/);
 assert.match(await cards.nth(2).locator('.customer-value-summary').innerText(),/Affärsvärde: —[\s\S]*Uppskattad leveransstorlek: —/);
 assert.equal(await cards.first().locator('.next-action + .customer-value-summary').count(),1);
 await status(page,'act');
 assert.equal(await page.locator('#chip-status').innerText(),'Status · Att agera');
 assert.match(await page.locator('#subtitle').innerText(),/24 av 27/);
 assert.equal(await cards.count(),20);
 await page.locator('#load-more-btn').click();assert.equal(await cards.count(),24);
 assert.equal(await cards.filter({has:page.locator('.next-action-red')}).count(),24);
 await page.locator('#chip-region').click();await page.locator('#filter-modal-body input[value="Väst"]').check();
 await page.locator('#filter-modal-close').click();assert.equal(await cards.count(),2);
 assert.deepEqual(await cards.locator('.customer-name').allTextContents(),['Z hög','A låg']);
 await page.locator('#search-input').fill('A låg');await page.waitForFunction(()=>searchQuery==='A låg');
 assert.equal(await cards.count(),1);
 await page.locator('#search-input').fill('');await page.waitForFunction(()=>searchQuery==='');
 await page.locator('#clear-filters-btn').click();assert.match(await page.locator('#subtitle').innerText(),/27 av 27/);
 assert.equal(await page.locator('#chip-status').innerText(),'Status');
 await status(page,'planned');assert.deepEqual(await cards.locator('.customer-name').allTextContents(),['Bokad']);
 await status(page,'wait');assert.deepEqual(await cards.locator('.customer-name').allTextContents(),['Avvakta']);
 assert.match(await page.locator('#customer-list [role=status]').innerText(),/1 butik saknar aktuell status/);
 await page.evaluate(()=>{insightsLoaded=false;renderList();});assert.equal(await cards.count(),0);
 assert.match(await page.locator('#customer-list [role=status]').innerText(),/Laddar kundinsikter/);
 assert.doesNotMatch(await page.locator('#customer-list').innerText(),/saknar aktuell status|Inga butiker matchar/);
 await page.evaluate(()=>{insightsLoaded=true;insightsLoadFailed=true;renderList();});assert.equal(await cards.count(),0);
 assert.match(await page.locator('#customer-list [role=status]').innerText(),/kunde inte laddas/);
 assert.doesNotMatch(await page.locator('#customer-list').innerText(),/saknar aktuell status|Inga butiker matchar/);
 await status(page,'all');assert.equal(await cards.count(),20);
 await page.evaluate(()=>{insightsLoadFailed=false;renderList();renderCustomerDecisionDetail(customers[1]);});
 assert.match(await page.locator('#d-customer-decision .customer-value-summary').innerText(),/Affärsvärde: 87[\s\S]*13 DFP/);
 const overflow=await page.evaluate(()=>Array.from(document.querySelectorAll('#view-list .toolbar,#customer-list .customer-card')).filter(e=>e.scrollWidth>e.clientWidth+1).map(e=>e.className));
 assert.deepEqual(overflow,[]);
 if(process.env.VALUE_PRIORITY_SCREENSHOT_DIR){
  await page.locator('#filter-modal').waitFor({state:'hidden'});
  await page.evaluate(()=>window.scrollTo(0,0));
  fs.mkdirSync(process.env.VALUE_PRIORITY_SCREENSHOT_DIR,{recursive:true});
  await page.screenshot({path:path.join(process.env.VALUE_PRIORITY_SCREENSHOT_DIR,`seller-frontend-${width}.png`)});
 }
 await page.reload();await ready(page);
 await planning(page);assert.equal(await page.locator('#planning-attention-search').count(),0);
 assert.ok(await page.locator('#planning-backlog .planning-attention-item').count()>0);
 console.log(`${width}px: status radio, red overdue classification, filter/search/reset, precise rank, pagination, rounded/missing values, detail and attention without search PASS`);
}
(async()=>{const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_EXECUTABLE?{executablePath:process.env.CHROMIUM_EXECUTABLE}:{})});try{
for(const width of [390,1280]){
 const context=await browser.newContext({viewport:{width,height:900},timezoneId:'America/Los_Angeles'});
 const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(String(e)));
 await page.goto(base+'/__test__/login');await ready(page);
 const response=await page.request.post(base+'/__test__/priority-workflow?pagination=1');assert.equal(response.status(),200);const fixture=await response.json();
 await page.reload();await ready(page);
 await sellerFrontend(page,width);
 if(process.env.SELLER_FRONTEND_ONLY==='1'){assert.deepEqual(errors,[]);await context.close();continue;}
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
