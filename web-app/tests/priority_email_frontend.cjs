// Real email UI functions; transport is a pure in-process double and never sends mail.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync(path.join(__dirname,'..','index.html'),'utf8');
function source(name){const m=new RegExp(`  (?:async )?function ${name}\\(`).exec(html);assert.ok(m,name);return html.slice(m.index,html.indexOf('\n  }',m.index)+4);}
const context=vm.createContext({assert,console});
vm.runInContext(`
const API='',getCustomerInsight=c=>c||{},getCustomerListGuidance=c=>c?.list_guidance||{};
const planningActivityId=a=>a?.planned_activity_id||'';
const classes=()=>{const set=new Set();return {add:v=>set.add(v),remove:v=>set.delete(v),contains:v=>set.has(v)}};
const elements=new Map(['email-send','email-send-error','email-subject','email-intro','email-closing'].map(id=>[id,{value:id,textContent:'Skicka',disabled:false,classList:classes()}]));
const document={getElementById:id=>elements.get(id),querySelector:s=>s.includes('selected')?{checked:true}:{value:'Butiken'},body:{contains:()=>true,style:{}}};
const emailModalBackdrop={classList:classes(),setAttribute(){}};
let reminderDraft=null,reminderPlanningContext=null,reminderRetryPayload=null,reminderDirty=false,reminderSending=false,currentCustomer=null,planningData={};
const emailModalContent={querySelectorAll:()=>[elements.get('email-subject'),elements.get('email-intro')]};
const collectReminderOrderRows=()=>[],toasts=[],requests=[];
let refreshes=0,confirmations=0,responseBody={},responseStatus=409;
const window={confirm:()=>{confirmations++;throw new Error('Hard contact blocks must never offer an override')}};
const showToast=message=>toasts.push(message),refreshCustomerDecisions=async()=>{refreshes++;};
const refreshStats=async()=>{},refreshWorkflowViews=async()=>{};
const fetch=async(url,options)=>{requests.push({url,body:JSON.parse(options.body)});return {status:responseStatus,ok:responseStatus<400,json:async()=>responseBody}};
${['customerHasContactBan','customerCanExecutePlanned','getPlannedEmailContext','collectReminderPayload','reminderErrorMessage','setReminderRetryMode','sendReminderEmail','closeReminderModal'].map(source).join('\n')}
`,context);
vm.runInContext(`(async()=>{
const customer={customer_id:'customer-1',row:2,list_guidance:{can_execute_planned_contact:true},contact_permission:{status:'allowed'}};
const activity={planned_activity_id:'plan-1',customer_id:'customer-1',contact_type:'email',status:'planned',revision:7,obligation_id:'followup:source-1',obligation_revision:281474976710123,scheduled_at:'2026-10-18T10:00:00+02:00'};
assert.equal(getPlannedEmailContext({...customer,contact_permission:{status:'do_not_contact'}},activity),null);
assert.equal(getPlannedEmailContext(customer,{...activity,contact_type:'phone'}),null);
assert.equal(getPlannedEmailContext(customer,{...activity,customer_id:'another-customer'}),null);
assert.equal(getPlannedEmailContext(customer,{...activity,can_execute_planned_contact:false}),null);
assert.equal(getPlannedEmailContext(customer,{...activity,obligation_revision:null}),null);
reminderPlanningContext=getPlannedEmailContext(customer,activity);
activity.revision=9;activity.obligation_revision=12;
assert.equal(reminderPlanningContext.expected_planned_activity_revision,7,'an open draft keeps its original plan revision');
assert.equal(reminderPlanningContext.expected_obligation_revision,281474976710123,'an open draft keeps its exact obligation CAS token');
const draft={draft_id:'draft-1',recipients:[{email:'fixture@example.invalid'}],warnings:[],links:{}};
currentCustomer=customer;reminderDraft=draft;emailModalBackdrop.classList.add('open');
responseBody={ok:false,error:'email_contact_blocked',message:'En ny order kräver att kontakt avvaktas.'};
await sendReminderEmail(false);
assert.equal(requests.length,1);assert.equal(confirmations,0);assert.equal(refreshes,1);
assert.equal(requests[0].body.planned_activity_id,'plan-1');
assert.equal(requests[0].body.expected_planned_activity_revision,7);
assert.equal(requests[0].body.obligation_id,'followup:source-1');
assert.equal(requests[0].body.expected_obligation_revision,281474976710123);
assert.equal(reminderDraft,draft,'a blocked send retains the draft for review');
assert.equal(elements.get('email-send').disabled,true);
assert.match(elements.get('email-send-error').textContent,/ny order.*ladda om kundens aktuella åtgärd/s);
assert.equal(elements.get('email-send-error').classList.contains('visible'),true);
assert.match(reminderErrorMessage('do_not_contact'),/Kontakta inte.*Hantera/);
// A partially saved calendar log locks the already-sent email and retries an identical payload.
responseStatus=200;responseBody={ok:true,is_test:false,planning_completion_saved:false};
await sendReminderEmail(false);
assert.equal(reminderDraft,draft);assert.ok(reminderRetryPayload);
assert.equal(elements.get('email-subject').disabled,true);
assert.match(elements.get('email-send-error').textContent,/Mejlet är skickat.*kalenderstatus igen/s);
assert.equal(elements.get('email-send').textContent,'Försök spara kalenderstatus igen');
assert.equal(elements.get('email-send').disabled,false);
const savedPayload=requests.at(-1).body;
elements.get('email-subject').value='A DOM change must not change the retry payload';
responseBody={ok:true,duplicate:true,is_test:false,planning_completion_saved:true};
await sendReminderEmail(false);
assert.deepEqual(requests.at(-1).body,savedPayload,'retry only repairs calendar state using the exact sent payload');
assert.equal(reminderDraft,null);assert.equal(reminderPlanningContext,null);
assert.equal(reminderRetryPayload,null);
assert.equal(emailModalBackdrop.classList.contains('open'),false,'successful send closes the draft even while send cleanup is pending');
assert.equal(elements.get('email-send').disabled,false);
assert.equal(confirmations,0);
console.log('Planned email UI: immutable scope/CAS, hard-block feedback, no warning override, and mocked success PASS');
})()`,context).catch(error=>{console.error(error);process.exitCode=1});
