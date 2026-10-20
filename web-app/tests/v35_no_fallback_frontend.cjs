// Execute the real UI helpers against stale/invalid model payloads.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const html = fs.readFileSync(path.join(__dirname,'..','index.html'),'utf8');
function source(name) {
  const start=html.indexOf(`  function ${name}(`); assert.ok(start>=0,name);
  const end=html.indexOf('\n  }',start)+4; assert.ok(end>start,name);
  return html.slice(start,end);
}
const functions=['sortedModelValue','getWorkPriorityValue','getCustomerGuidance','getCustomerListGuidance',
  'stableCustomerTieBreak','compareDescendingMissingLast','contactPrioritySort','mapPriorityValue','mapPrioritySort',
  'customerHasContactBan','customerCanStartContact'];
const context=vm.createContext({assert,console});
vm.runInContext(`const getCustomerInsight=c=>c||{};${functions.map(source).join('\n')}`,context);
vm.runInContext(`
 const old={priority_score:100,work_priority_raw:999,contact_priority_raw:999,
   customer_guidance:{status_key:'act_now',can_contact_now:true,can_start_unscheduled_contact:true}};
 for (const version of [undefined,'v2.2','v3.4-preview']) {
   const stale={...old,value_model_version:version};
   assert.equal(getWorkPriorityValue(stale,'raw'),null);
   assert.equal(mapPriorityValue(stale,'raw'),null);
   assert.equal(customerCanStartContact(stale),false);
   assert.deepEqual(getCustomerListGuidance(stale),{});
   assert.deepEqual(getCustomerGuidance(stale),{});
 }
 const good={customer:'Current',customer_id:'good',value_model_version:'v3.5',work_priority_precise:80,
   work_priority_score:80,customer_guidance:{can_start_unscheduled_contact:true}};
 assert.equal(customerCanStartContact(good),true);
 const ranked=[{...old,customer:'Old',customer_id:'old'},good].sort(contactPrioritySort);
 assert.equal(ranked[0].customer_id,'good');
 assert.equal([...ranked].sort(mapPrioritySort)[0].customer_id,'good');
 for (const bad of [null,undefined,'',NaN,Infinity])
   assert.equal(getWorkPriorityValue({...good,work_priority_precise:bad,priority_score:100},'raw'),null);
 console.log('V3.5 stale score/guidance rejection in list and map PASS');
`,context);
