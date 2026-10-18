// Execute the actual UI helpers: no duplicate rendering/filter implementation.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const html = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
function source(name) {
  const start = html.indexOf(`  function ${name}(`);
  assert.notEqual(start, -1, name);
  const end = html.indexOf('\n  }', start) + 4;
  assert.ok(end > start, name);
  return html.slice(start, end);
}
const functions = ['getCustomerListGuidance', 'getFilterOptionValue', 'getCardDateHtml', 'getDeliveryVolumeText',
  'getCardDatesHtml','planningDateFromKey','planningDateKeyFromIso','planningTimeFromIso',
  'getFilteredCustomers', 'renderFilterOptions'];
const constants = html.slice(html.indexOf('  const FOCUS_KEYS_BY_LABEL ='),
  html.indexOf('  function getFilterOptionValue('));
const context = vm.createContext({assert, console});
vm.runInContext(`
const esc = s => String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const escAttr = esc; const PLANNING_TIME_ZONE='Europe/Stockholm';
const getCustomerInsight = c => c;
const getCustomerGuidance = c => c.customer_guidance || {};
const filterState = Object.fromEntries(['segment','city','region','focus','risk','delivery','latestContact','emailProposal'].map(k=>[k,new Set()]));
let customers = [], searchQuery = '', emailClickNoOrderActive = false, listWorkFilter='all';
let _sheetKey = 'focus';
const filterBody = {innerHTML:'',querySelectorAll:()=>[]};
const document = {getElementById:()=>filterBody};
const getAvailableValues = () => FOCUS_LABELS;
${constants}
${functions.map(source).join('\n')}
`, context);
vm.runInContext(String.raw`
const g = {focus_key:'repeat_purchase',status_key:'wait'};
const data = {delivery_count:2,latest_delivery_date:'2026-09-08',latest_delivery_dfp:13,
  next_delivery_date:'2026-09-22',next_delivery_dfp:15,latest_order_dfp:99,
  customer_guidance:g};
const original = JSON.stringify(data);
let rendered = getCardDatesHtml(data);
assert.match(rendered,/8 sep[^<]*<\/time> \u00b7 13 DFP/);
assert.match(rendered,/22 sep[^<]*<\/time> \u00b7 15 DFP/);
assert.doesNotMatch(rendered,/99 DFP/);
assert.match(rendered,/datetime="2026-09-08"/);
assert.equal(JSON.stringify(data),original);
const noPast = getCardDatesHtml({...data,delivery_count:0,latest_delivery_date:'',latest_delivery_dfp:null,first_delivery_dfp:99,
  customer_guidance:{...g,focus_key:'second_purchase'}});
assert.doesNotMatch(noPast,/F\u00f6rsta leverans|99 DFP|13 DFP/);
assert.match(noPast,/15 DFP/);
const missing = getCardDatesHtml({...data,latest_delivery_dfp:null,next_delivery_dfp:null});
assert.doesNotMatch(missing,/DFP/);
for (const bad of [null,undefined,0,-1,'bad',NaN,Infinity,true,false]) {
  assert.equal(getDeliveryVolumeText('2026-09-08',bad),'');
}
for (const badDate of ['',null,'2026-02-30','2026-13-01','<img src=x>']) {
  assert.equal(getCardDateHtml(badDate,true),'\u2014');
  assert.equal(getDeliveryVolumeText(badDate,99),'');
}
assert.match(getDeliveryVolumeText('2026-09-08',13.5),/13,5 DFP/);
assert.match(getCardDateHtml('2025-12-31',true),/31 dec 2025/);
assert.match(getCardDateHtml('2026-09-08'),/>2026-09-08</);
assert.match(getCardDatesHtml({...data,delivery_count:1}),/F\u00f6rsta leverans/);
// Canonical dated history must not fall back to an unconfirmed legacy date.
const noVerified = getCardDatesHtml({...data,business_latest_verified_delivery_date:'',business_latest_verified_delivery_dfp:null,latest_unconfirmed_order_date:'2026-09-08'});
assert.match(noVerified,/leveransdatum saknas/);
assert.doesNotMatch(noVerified,/13 DFP/);
// Focus filter values use stable keys even while the card label changes.
assert.equal(getFilterOptionValue('focus','Andra k\u00f6pet'),'second_purchase');
assert.equal(getFilterOptionValue('city','G\u00f6teborg'),'G\u00f6teborg');
customers = [
  {customer:'Day 11',customer_guidance:{focus_key:'second_purchase',focus_label:'En leverans, 11\u201390 dagar sedan'}},
  {customer:'Future only',customer_guidance:{focus_key:'second_purchase',focus_label:'Andra k\u00f6pet'}},
  {customer:'Other',customer_guidance:{focus_key:'repeat_purchase',focus_label:'\u00c5terk\u00f6p'}},
];
filterState.focus.add('second_purchase');
assert.deepEqual(getFilteredCustomers().map(c=>c.customer),['Day 11','Future only']);
renderFilterOptions('');
assert.match(filterBody.innerHTML,/value="second_purchase" checked/);
filterState.focus.add('repeat_purchase');
assert.equal(getFilteredCustomers().length,3);
filterState.focus.clear();
assert.equal(getFilteredCustomers().length,3);
console.log('Guidance/delivery frontend: date-volume pairs, missing data, planning context and stable focus filters PASS');
`, context);
