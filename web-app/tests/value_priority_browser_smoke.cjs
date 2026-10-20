// Actual browser + actual Flask model/persistence, using only isolated synthetic CRM rows.
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const base = process.env.VALUE_PRIORITY_HARNESS_URL || 'http://127.0.0.1:5065';
const shots = process.env.VALUE_PRIORITY_SCREENSHOT_DIR;

async function ready(page) {
  await page.waitForFunction(() => typeof insightsLoaded !== 'undefined' && insightsLoaded && Object.keys(insights.__by_customer_id || {}).length > 0);
}
async function row(page, name) {
  return page.evaluate(name => {const c=customers.find(c=>c.customer===name);return {customer:c,insight:getCustomerInsight(c)};}, name);
}
async function list(page, query='QA ') {
  await page.evaluate(() => {closePlanningModal();showView('list');});
  await page.locator('[data-work-filter="all"]').click();
  await page.locator('#search-input').fill(query);
  await page.waitForFunction(query => searchQuery === query, query);
}
function card(page, name) {return page.locator('#customer-list .customer-card').filter({has:page.locator('.customer-name',{hasText:name})});}
async function searchCard(page,name) {await list(page,name);return card(page,name);}
async function workflow(page,name) {
  const target=await searchCard(page,name);
  await target.locator('[data-customer-workflow]').filter({hasText:/^Hantera$/}).click();
}
async function waitDecision(page,name,predicate) {
  await page.waitForFunction(({name,source}) => {
    const c=customers.find(c=>c.customer===name); const i=getCustomerInsight(c);
    return (new Function('i',source))(i);
  },{name,source:predicate});
}
async function planning(page) {
  await page.evaluate(() => openPlanningView());
  await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading&&planningAttention.loaded);
}
function attention(page,name) {return page.locator('#planning-backlog .planning-attention-item').filter({has:page.locator('.planning-backlog-customer',{hasText:name})});}
async function shot(page,name) {
  if(!shots)return;
  fs.mkdirSync(shots,{recursive:true});
  await page.waitForFunction(()=>!document.getElementById('toast').classList.contains('show'));
  const modalOpen=await page.evaluate(()=>Boolean(document.querySelector('#planning-modal-backdrop.open,#email-modal-backdrop.open')));
  await page.screenshot({path:path.join(shots,name+'.png'),fullPage:!modalOpen});
}
async function permission(page,name,status) {
  await workflow(page,name);
  await page.locator('#customer-permission-open').click();
  await page.locator('#customer-permission-source').fill('Syntetiskt kundbesked i browsertest');
  await page.locator('#customer-permission-reason').fill(status==='allowed'?'Kunden har uttryckligen tillåtit ny kontakt.':'Kunden har uttryckligen bett oss att inte kontakta igen.');
  await page.locator('#customer-permission-save').click();
  await page.waitForFunction(()=>!document.getElementById('planning-modal-backdrop').classList.contains('open'));
  await waitDecision(page,name,`return i.contact_permission?.status===${JSON.stringify(status)}`);
}
async function assertNoOverflow(page) {
  const overflow=await page.evaluate(()=>Array.from(document.querySelectorAll('.view.active .customer-card,.view.active .planning-backlog-card,.view.active .planning-attention-item,#planning-modal-backdrop.open .planning-dialog-form,#email-modal-backdrop.open .email-modal-body,#email-modal-backdrop.open .email-modal-footer')).filter(e=>e.scrollWidth>e.clientWidth+1).map(e=>e.className));
  assert.deepEqual(overflow,[],'responsive cards/dialogs fit viewport');
}
(async()=>{
 const browser=await chromium.launch({headless:true,...(process.env.CHROMIUM_EXECUTABLE?{executablePath:process.env.CHROMIUM_EXECUTABLE}:{})});
 try {
  for(const width of [390,1280]) {
   const page=await browser.newPage({viewport:{width,height:900},timezoneId:'America/Los_Angeles'});
   const errors=[];page.on('pageerror',e=>errors.push(String(e)));
   await page.goto(base+'/__test__/login',{waitUntil:'domcontentloaded'});await ready(page);
   const fixtureResponse=await page.request.post(base+'/__test__/priority-workflow');
   assert.equal(fixtureResponse.status(),200);const fixture=await fixtureResponse.json();
   await page.reload();await ready(page);await list(page);
   const booked=await row(page,'QA Bokad');
   assert.ok(booked.insight.work_priority_raw>booked.insight.contact_priority_raw);
   assert.equal(booked.insight.contact_priority_raw,0);
   assert.equal(booked.insight.list_guidance.can_start_unscheduled_contact,false);
   assert.equal(await page.locator('#customer-list .customer-card').first().locator('.customer-name').textContent(),'QA Bokad','high-value booked customer remains first among QA stores');
   assert.equal(await card(page,'QA Bokad').locator('.score-pair-item').count(),1,'one primary score');
   assert.match(await card(page,'QA Bokad').locator('.score-pair').textContent(),/Prioritet/);
   assert.match(await card(page,'QA Bokad').locator('.card-dates').textContent(),/Planerad kontakt.*10:00/s);
   assert.match(await card(page,'QA Bokad').locator('.card-dates').textContent(),/240\s*DFP/,'latest delivery date is paired with its own delivered volume');
   await page.locator('[data-work-filter="act"]').click();
   assert.equal(await card(page,'QA Bokad').count(),0);
   await page.locator('[data-work-filter="planned"]').click();assert.equal(await card(page,'QA Bokad').count(),1);
   assert.equal(await page.locator('[data-work-filter="review"]').count(),0,'no separate review filter');
   assert.equal(await card(page,'QA Bokad').locator('.customer-review-link').count(),1,'separate promise stays visible under a green booking');
   await page.locator('[data-work-filter="all"]').click();
   await page.locator('#list-sort-mode').selectOption('value');
   assert.match(await page.locator('.customer-card .score-pair').first().textContent(),/Affärsvärde/);
   assert.equal(await page.locator('.customer-card').first().locator('.score-pair-item').count(),1);
   await page.locator('#list-sort-mode').selectOption('contact');
   await assertNoOverflow(page);await shot(page,`priority-list-${width}`);

   // Ordinary workflow decisions persist without hiding the customer from search/value.
   await workflow(page,'QA Stort A');await page.locator('[data-customer-decision="snooze"]').click();
   await waitDecision(page,'QA Stort A','return i.customer_workflow?.status==="snoozed" && i.work_priority_raw===0');
   await page.reload();await ready(page);await workflow(page,'QA Stort A');
   await page.locator('[data-customer-decision="restore"]').click();
   await waitDecision(page,'QA Stort A','return i.work_priority_raw>0');
   await workflow(page,'QA Stort A');
   assert.equal(await page.locator('[data-customer-decision="dismiss"]').count(),0,'A prospects retain the v2.2 snooze-only protection');
   await workflow(page,'QA Första order');await page.locator('[data-customer-decision="dismiss"]').click();
   await waitDecision(page,'QA Första order','return i.customer_workflow?.status==="dismissed"');
   await searchCard(page,'QA Första order');assert.equal(await card(page,'QA Första order').count(),1);
   await workflow(page,'QA Första order');await page.locator('[data-customer-decision="restore"]').click();
   await waitDecision(page,'QA Första order','return i.customer_workflow?.status==="active"');

   // A booking changes status and CTA, while keeping commercially relevant work visible.
   const target=await searchCard(page,'QA Stort A');await target.locator('.planning-card-plan-btn').click();
   await page.locator('#planning-editor-form').waitFor();
   assert.equal(await page.locator('input[name="planning-editor-type"]:checked').inputValue(),'visit');
   const tomorrow=await page.evaluate(()=>planningAddDays(planningTodayKey(),1));
   await page.locator('#planning-editor-date').fill(tomorrow);await page.locator('#planning-editor-time').fill('14:00');
   await page.locator('#planning-editor-note').fill('Syntetiskt planerat butiksbesök');
   await page.locator('#planning-editor-appointment').check();await page.locator('#planning-editor-picking-help').check();
   await page.locator('#planning-editor-save').click();
   await page.waitForFunction(()=>!document.getElementById('planning-modal-backdrop').classList.contains('open'));
   await waitDecision(page,'QA Stort A','return i.list_guidance?.status_key==="planned" && i.work_priority_raw>0 && i.contact_priority_raw===0');
   await searchCard(page,'QA Stort A');assert.equal(await card(page,'QA Stort A').locator('.next-action-green').count(),1);
   await card(page,'QA Stort A').locator('.planning-card-plan-btn').click();
   await page.locator('[data-planning-detail-action="edit"]').click();
   assert.equal(await page.locator('#planning-editor-note').inputValue(),'Syntetiskt planerat butiksbesök');
   assert.equal(await page.locator('#planning-editor-appointment').isChecked(),true);
   assert.equal(await page.locator('#planning-editor-picking-help').isChecked(),true);
   await page.locator('#planning-editor-cancel').click();

   // Attention contains only past obligations even while viewing a different week.
   await planning(page);
   assert.equal(await attention(page,'QA Dagens aktivitet').count(),0);
   assert.equal(await attention(page,'QA Bokad').count(),1,'separate older promise survives an unrelated future booking');
   assert.equal(await attention(page,'QA Bokad').locator('[data-attention-action="log"]').count(),0);
   assert.equal(await page.locator('[data-obligation-id="calendar:qa-plan-2"]').count(),0,'future booking itself is not in attention');
   assert.equal(await attention(page,'QA Kalender igår').count(),1);
   assert.equal(await attention(page,'QA Löfte').count(),1);
   assert.equal(await attention(page,'QA Granska').count(),1);
   assert.equal(await attention(page,'QA Granska').locator('[data-attention-action="log"]').count(),0);
   const idsBefore=await page.evaluate(()=>planningAttention.items.map(x=>x.obligation_id));
   await page.locator('#planning-prev-week-btn').click();await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   assert.deepEqual(await page.evaluate(()=>planningAttention.items.map(x=>x.obligation_id)),idsBefore,'attention ignores viewed week');
   await assertNoOverflow(page);await shot(page,`attention-${width}`);

   // A booked email opens with immutable activity/obligation scope. The transport is intercepted: no mail is sent.
   const mailCustomer=(await row(page,'QA Dagens aktivitet')).customer;
   await page.route('**/email-proposal-draft',route=>route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({ok:true,draft:{
     draft_id:'qa-booked-email',customer:mailCustomer,email_type:'new_customer',email_type_label:'Ny kund',
     send_mode:'test',test_recipient:'fixture@example.invalid',subject:'Syntetiskt bokat mejl',intro_text:'Bokad uppföljning',closing_text:'Tack',
     warnings:[],notices:[],links:{},signature:{name:'Olle'},order_rows:[],product_catalog:[],
     recipients:[{email:'fixture@example.invalid',valid:true,selected:true,greeting_name:'Butiken'}],
   }})}));
   const mailPosts=[];
   let mailResponseMode='blocked';
   await page.route('**/email-proposal/send',route=>{
     mailPosts.push(JSON.parse(route.request().postData()));
     return route.fulfill({status:mailResponseMode==='blocked'?409:200,contentType:'application/json',body:JSON.stringify(mailResponseMode==='blocked'
       ? {ok:false,error:'email_contact_blocked',message:'En ny order kräver att kontakten avvaktas.'}
       : {ok:true,is_test:false,duplicate:mailResponseMode==='repaired',planning_completion_saved:mailResponseMode==='repaired'})});
   });
   await searchCard(page,'QA Dagens aktivitet');await card(page,'QA Dagens aktivitet').locator('.planning-card-plan-btn').click();
   // Opening from a list can load a different calendar week asynchronously.
   // Wait for that activity's detail, not merely for the click event to dispatch.
   await page.locator('[data-planning-detail-action="email"]').waitFor();
   const emailActivity=await page.evaluate(()=>({...getPlanningActivity('qa-plan-5')}));
   assert.equal(emailActivity.planned_activity_id,'qa-plan-5','current booked email is loaded before scope assertions');
   await page.locator('[data-planning-detail-action="email"]').click();
   await page.locator('#email-send').waitFor();
   await page.locator('#email-send').click();
   await page.waitForFunction(()=>!reminderSending);
   assert.equal(mailPosts.length,1,'hard block cannot ask to send anyway');
   assert.equal(mailPosts[0].planned_activity_id,emailActivity.planned_activity_id);
   assert.equal(mailPosts[0].expected_planned_activity_revision,emailActivity.revision);
   assert.equal(mailPosts[0].obligation_id,emailActivity.obligation_id);
   assert.equal(mailPosts[0].expected_obligation_revision,emailActivity.obligation_revision);
   assert.match(await page.locator('#email-send-error').textContent(),/ny order.*ladda om kundens aktuella åtgärd/s);
   assert.equal(await page.locator('#email-send').isDisabled(),true);
   await assertNoOverflow(page);await shot(page,`planned-email-${width}`);
   await page.locator('#email-modal-close').click();
   await searchCard(page,'QA Dagens aktivitet');await card(page,'QA Dagens aktivitet').locator('.planning-card-plan-btn').click();
   await page.locator('[data-planning-detail-action="email"]').click();await page.locator('#email-send').waitFor();
   mailResponseMode='partial';await page.locator('#email-send').click();
   await page.waitForFunction(()=>Boolean(reminderRetryPayload)&&!reminderSending);
   assert.equal(await page.locator('#email-subject').isDisabled(),true);
   assert.match(await page.locator('#email-send-error').textContent(),/Mejlet är skickat.*kalenderstatus igen/s);
   assert.equal(await page.locator('#email-send').textContent(),'Försök spara kalenderstatus igen');
   await assertNoOverflow(page);await shot(page,`planned-email-retry-${width}`);
   mailResponseMode='repaired';await page.locator('#email-send').click();
   await page.waitForFunction(()=>!reminderDraft&&!reminderSending&&!document.getElementById('email-modal-backdrop').classList.contains('open'));
   assert.equal(mailPosts.length,3);
   assert.deepEqual(mailPosts[1],mailPosts[2],'calendar-only email repair reuses the exact already-sent payload');
   await page.unroute('**/email-proposal-draft');await page.unroute('**/email-proposal/send');
   await planning(page);

   // Skipping and restoring retains the original calendar activity and makes it reviewable again.
   await attention(page,'QA Kalender igår').locator('[data-attention-action="skip"]').click();
   await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   await page.locator('#planning-date-jump').fill(await page.evaluate(()=>planningAddDays(planningTodayKey(),-1)));
   await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   await page.locator('[data-planning-open-id="qa-plan-4"]').click();
   await page.locator('[data-planning-detail-action="restore"]').click();
   await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading&&!document.getElementById('planning-modal-backdrop').classList.contains('open'));
   assert.equal(await attention(page,'QA Kalender igår').count(),1);

   // Existing missed activity preserves appointment/picking flags and its note when rescheduled.
   await attention(page,'QA Kalender igår').locator('[data-attention-action="reschedule"]').click();
   assert.equal(await page.locator('input[name="planning-editor-type"]:checked').inputValue(),'visit');
   assert.equal(await page.locator('#planning-editor-appointment').isChecked(),true);
   assert.equal(await page.locator('#planning-editor-picking-help').isChecked(),true);
   assert.equal(await page.locator('#planning-editor-date').inputValue(),fixture.today,'past due date is not proposed again');
   assert.equal(await page.locator('#planning-editor-note').inputValue(),'Syntetisk planering Kalender igår');
   await page.locator('#planning-editor-date').fill(tomorrow);await page.locator('#planning-editor-time').fill('15:00');
   await page.locator('#planning-editor-save').click();
   await page.waitForFunction(()=>!document.getElementById('planning-modal-backdrop').classList.contains('open'));
   await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   assert.equal(await attention(page,'QA Kalender igår').count(),0);

   // Lost response after a committed contact retries the same request and keeps the promise open.
   const promiseRevision=await page.evaluate(()=>planningAttention.items.find(i=>i.customer==='QA Löfte').revision);
   await attention(page,'QA Löfte').locator('[data-attention-action="log"]').click();
   assert.equal(await page.locator('#f-channel').inputValue(),'Telefon');
   assert.equal(await page.locator('#f-obligation-outcome').inputValue(),'open');
   await page.locator('#f-result').selectOption({label:'Ej anträffbar'});
   assert.equal(await page.locator('#f-obligation-outcome').isDisabled(),true);
   await page.locator('#f-comment').fill('Syntetiskt missat kontaktförsök med kvarvarande löfte');
   const posted=[];let loseResponse=true;
   await page.route('**/customers/*/contacts',async route=>{
     if(route.request().method()!=='POST')return route.continue();
     posted.push(JSON.parse(route.request().postData()));
     if(loseResponse){loseResponse=false;await route.fetch();await route.abort('failed');}else await route.continue();
   });
   await page.locator('#form-save-btn').click();
   await page.waitForFunction(()=>Boolean(contactRetryPayload));
   await page.locator('#form-save-btn').click();
   await page.waitForFunction(()=>document.getElementById('view-planning').classList.contains('active')&&!planningLoading&&!planningAttentionLoading);
   await page.unroute('**/customers/*/contacts');
   assert.equal(posted.length,2);assert.deepEqual(posted[0],posted[1],'same immutable retry payload');
   assert.equal(posted[0].expected_obligation_revision,promiseRevision,'retain exact obligation CAS token');
   assert.equal(posted[0].obligation_outcome,'open');assert.equal(posted[0].obligation_id,'followup:qa-contact-3');
   const state=await page.request.get(base+'/__test__/priority-workflow-state').then(r=>r.json());
   assert.equal(state.contacts.filter(c=>c.comment==='Syntetiskt missat kontaktförsök med kvarvarande löfte').length,1);
   assert.equal(state.contacts.find(c=>c.contact_id==='qa-contact-3').follow_up_date,await page.evaluate(()=>planningAddDays(planningTodayKey(),-1)));
   assert.equal(await attention(page,'QA Löfte').count(),1,'missed attempt leaves the explicit obligation in attention');

   // Paused obligations remain discoverable and can be restored from the customer.
   let releaseInsights;
   const insightsGate=new Promise(resolve=>{releaseInsights=resolve;});
   await page.route('**/customer-insights',async route=>{await insightsGate;await route.continue();});
   await attention(page,'QA Granska').locator('[data-attention-action="review"]').click();
   await page.locator('#attention-reason').fill('Syntetisk intern utredning av gammalt löfte');
   await page.locator('[data-obligation-action="snooze"]').click();
   await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading&&!document.getElementById('planning-modal-backdrop').classList.contains('open'));
   assert.equal(await attention(page,'QA Granska').count(),0);
   await workflow(page,'QA Granska');await page.locator('[data-customer-obligation]').first().click();
   assert.match(await page.locator('#planning-modal-content').textContent(),/Pausat till/);
   releaseInsights();await page.unrouteAll({behavior:'wait'});
   await page.locator('[data-obligation-action="restore"]').click();
   await page.waitForFunction(()=>!document.getElementById('planning-modal-backdrop').classList.contains('open'));
   await planning(page);assert.equal(await attention(page,'QA Granska').count(),1);

   // Explicit permission events stop outbound CTAs, including booked customers, until explicitly released.
   await permission(page,'QA Bokad','do_not_contact');
   await page.reload();await ready(page);await searchCard(page,'QA Bokad');
   await page.locator('[data-work-filter="planned"]').click();assert.equal(await card(page,'QA Bokad').count(),1,'blocked booking stays discoverable');
   const blocked=await row(page,'QA Bokad');assert.equal(blocked.insight.work_priority_raw,0);
   await card(page,'QA Bokad').locator('.customer-name').click();
   await page.locator('#view-detail.active').waitFor();
   assert.equal(await page.locator('#d-call-btn').isDisabled(),true);
   assert.equal(await page.locator('#d-mail-btn').isDisabled(),true);
   await page.locator('#d-plan-btn').click();
   await page.locator('#planning-modal-backdrop.open').waitFor();
   assert.equal(await page.locator('[data-planning-detail-action="log"]').count(),0);
   assert.equal(await page.locator('[data-planning-detail-action="cancel"]').count(),1);
   await page.locator('#planning-modal-close').click();
   await permission(page,'QA Bokad','allowed');
   await waitDecision(page,'QA Bokad','return i.work_priority_raw>0 && i.contact_priority_raw===0');

   // Permission can also be recorded with a real logged contact result and its documented source.
   await searchCard(page,'QA Kontaktbesked');await card(page,'QA Kontaktbesked').locator('.customer-name').click();
   await page.locator('#view-detail.active').waitFor();await page.locator('#update-contact-btn').click();
   await page.locator('#f-channel').selectOption({label:'Telefon'});await page.locator('#f-result').selectOption({label:'Återkom ej'});
   await page.locator('#f-comment').fill('Syntetiskt explicit kontaktbesked');
   await page.locator('#f-permission-source').fill('Telefon med butiken i test');
   await page.locator('#f-permission-reason').fill('Butiken bad uttryckligen om att inte bli kontaktad.');
   await page.locator('#form-save-btn').click();
   await waitDecision(page,'QA Kontaktbesked','return i.contact_permission?.status==="do_not_contact"');

   // More than one page, several obligations for one customer, and owner switch.
   await page.request.post(base+'/__test__/priority-workflow?pagination=1');
   await page.reload();await ready(page);await planning(page);
   const firstPage=await page.evaluate(()=>({n:planningAttention.items.length,total:planningAttention.total}));
   assert.equal(firstPage.n,50);assert.ok(firstPage.total>50);
   await page.locator('#planning-attention-load-more').click();await page.waitForFunction(()=>!planningAttentionLoading&&planningAttention.items.length===planningAttention.total);
   assert.equal(await page.evaluate(()=>new Set(planningAttention.items.map(i=>i.obligation_id)).size),firstPage.total);
   await page.locator('#planning-owner-select').selectOption('sofia');await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   assert.equal(await page.locator('#planning-backlog .planning-attention-item').count(),0);
   await page.locator('#planning-owner-select').selectOption('olle');await page.waitForFunction(()=>!planningLoading&&!planningAttentionLoading);
   await page.route('**/planning/attention?*',route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'attention_store_unavailable',message:'Syntetiskt läsfel'})}));
   await page.evaluate(()=>loadPlanningAttention());
   assert.match(await page.locator('#planning-backlog').textContent(),/Kunde inte uppdatera Att hantera/);
   assert.doesNotMatch(await page.locator('#planning-backlog').textContent(),/Inga missade aktiviteter eller förfallna uppföljningar/);
   await page.unroute('**/planning/attention?*');
   await list(page);
   await page.locator('[data-work-filter="act"]').click();
   await page.route('**/customer-insights',route=>route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({error:'value_priority_disabled',message:'Modellen är avstängd'})}));
   await page.evaluate(()=>loadInsights());
   assert.match(await page.locator('.list-insights-error').textContent(),/tillfälligt avstängd/);
   assert.equal(await page.locator('#customer-list .customer-card').count(),9,'model-disabled response retains searchable master customers');
   assert.equal(await page.locator('#customer-list .planning-card-plan-btn').count(),0,'no stale contact suggestion on model failure');
   await page.unroute('**/customer-insights');
   await page.request.post(base+'/__test__/priority-workflow?reset=1');
   assert.deepEqual(errors,[],`${width}px: no browser JavaScript errors`);
   console.log(`${width}px: priority, booking, attention, linked retry, snooze, permission, pagination and errors PASS`);
   await page.close();
  }
 } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
