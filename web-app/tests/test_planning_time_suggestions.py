"""Exercise the actual browser helpers, controllers and all three entry points."""

from pathlib import Path
import os
import shutil
import subprocess
from unittest import TestCase, skipUnless


INDEX_PATH = Path(__file__).resolve().parents[1] / "index.html"


NODE_HARNESS = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[1], 'utf8');
const source = name => {
  const match = new RegExp(`  (?:async )?function ${name}\\(`).exec(html);
  assert.ok(match, `Missing real frontend function ${name}`);
  return html.slice(match.index, html.indexOf('\n  }', match.index) + 4);
};
const constant = name => {
  const match = new RegExp(`  const ${name} = [\\s\\S]+?;`).exec(html);
  assert.ok(match, name);
  return match[0];
};
const NativeDate = Date;
class FixedDate extends NativeDate {
  constructor(...args) { super(...(args.length ? args : ['2026-10-01T05:00:00Z'])); }
  static now() { return new NativeDate('2026-10-01T05:00:00Z').getTime(); }
}
const context = vm.createContext({assert, Date:FixedDate, URLSearchParams, console});
vm.runInContext(`
${['PLANNING_CONTACT_TYPES','PLANNING_TIME_ZONE','PLANNING_NOTE_MAX_LENGTH'].map(constant).join('\n')}
${['normalizeRouteIdentity','planningDateFromKey','planningDateKey','planningAddDays',
   'planningStockholmOffset','planningStockholmIso','planningDateKeyFromIso','planningTimeFromIso',
   'planningFirstAvailableTime','setupPlanningTimeSuggestion'].map(source).join('\n')}
let elements = new Map(), radioInputs = [], requests = [], replyActivities = [], pending = [];
let deferRequests = false, fetchFailure = null, replyOwner = null;
const node = (id, value = '') => {
  const listeners = new Map(), classes = new Set();
  const element = {id, value, checked:false, hidden:false, disabled:false, isConnected:true,
    textContent:'', innerHTML:'', style:{}, dataset:{},
    classList:{add:name=>classes.add(name),remove:name=>classes.delete(name),
      contains:name=>classes.has(name),toggle:(name,on)=>on?classes.add(name):classes.delete(name)},
    addEventListener(type, fn) { if (!listeners.has(type)) listeners.set(type, []); listeners.get(type).push(fn); },
    removeEventListener(type, fn) { listeners.set(type,(listeners.get(type)||[]).filter(item=>item!==fn)); },
    async dispatch(type) { for (const fn of [...(listeners.get(type)||[])]) await fn({target:element,preventDefault(){}}); },
    setAttribute(){}, removeAttribute(){}, focus(){},
  };
  elements.set(id, element);
  return element;
};
const document = {
  getElementById:id=>elements.get(id)||null,
  contains:element=>element?.isConnected && elements.get(element.id)===element,
  querySelectorAll:selector=>selector.includes("planning-editor-type") ? radioInputs : [],
  querySelector:selector=>selector.includes("planning-editor-type") ? radioInputs.find(input=>input.checked)||null : null,
};
const planningFetchJson = async (url, options={}) => {
  const request={url, options}; requests.push(request);
  if (deferRequests) return await new Promise((resolve,reject)=>pending.push({request,resolve,reject}));
  if (fetchFailure) throw new Error(fetchFailure);
  const query=new URLSearchParams(url.split('?')[1]);
  return {owner:{user_name:replyOwner || query.get('user_name') || currentUser.user_name},activities:replyActivities};
};
const activity = (time, type='visit', owner='olle', extra={}) => ({
  activity_id:'a-'+time, user_name:owner, contact_type:type, status:'planned',
  scheduled_at:planningStockholmIso('2026-10-02',time), ...extra,
});
const flush = async () => { for(let i=0;i<15;i++) await Promise.resolve(); };
let currentUser={user_name:'olle',name:'Olle',admin:false}, currentCustomer={row:2,customer:'Butik',sales_person:'Olle'};
let planningSelectedUserName='olle', planningSelectedDate='2026-10-02', planningData=null;
let contactTimeSuggestion=null, planningEditorTimeSuggestion=null, planningEditorActivity=null;
let planningEditorCustomer=null, planningEditorSeed=null, planningEditorRequestId='';
const planningTodayKey=()=>'2026-10-01';
const userIsAdmin=()=>Boolean(currentUser.admin);
`, context);
const run = async code => await vm.runInContext(`(async()=>{${code}\n})()`, context);
'''


ENTRY_HARNESS = r'''
vm.runInContext(`
let planningActiveUsersCache=[{user_name:'olle',name:'Olle'},{user_name:'johan',name:'Johan'}];
let formReturnView='', activePlannedActivityId='', contactClientRequestId='', contactRetryPayload=null;
let weekLoads=0;
let planningRecommendationPreviewLimit=0, planningRecommendationRequestSerial=0;
let planningRecommendation=null, planningRecommendationPreview=[], planningRecommendationPendingCount=0;
const esc=value=>String(value), escAttr=value=>String(value);
const planningClientRequestId=()=> 'test-request';
const planningSelectableCustomers=()=>[currentCustomer];
const isCancelledCustomer=()=>false, planningCustomerForItem=()=>currentCustomer;
const setupPlanningCustomerCombobox=()=>{}, savePlanningEditor=()=>{};
const normalizePlanningUser=value=>value;
const loadPlanningWeek=async()=>{weekLoads++;};
const renderPlanningRecommendation=()=>{};
const openPlanningView=async()=>{throw new Error('customer owner list should already exist');};
const getPrefilledSellerName=()=>currentUser.name;
const syncVisitCompetitorButtons=()=>{}, setFreezerSectionInvalid=()=>{}, setContactFormRetryMode=()=>{};
const showContactSaveError=message=>{elements.get('contact-save-error').textContent=message;};
const updateContactChannelFields=()=>{}, showView=()=>{}, showToast=()=>{};
const ensurePlanningActiveUsers=async()=>planningActiveUsersCache;
const closePlanningModal=()=>{planningEditorTimeSuggestion?.cancel();};
const openPlanningModal=(_title,_subtitle,body)=>{
  for (const [id,element] of elements) if (id.startsWith('planning-editor-')||id.startsWith('radio-')) {
    element.isConnected=false; elements.delete(id);
  }
  radioInputs=[];
  for (const match of body.matchAll(/<[^>]+>/g)) {
    const tag=match[0], id=/\\bid="([^"]+)"/.exec(tag)?.[1];
    if (id) node(id,/\\bvalue="([^"]*)"/.exec(tag)?.[1]||'');
    if (tag.includes('name="planning-editor-type"')) {
      const input=node('radio-'+radioInputs.length,/\\bvalue="([^"]*)"/.exec(tag)[1]);
      input.checked=/\\bchecked\\b/.test(tag); radioInputs.push(input);
    }
  }
};
${['planningActivityId','planningTruthy','normalizePlanningActivity','planningSelectedOwner',
   'renderContactFollowupOwner','updateFollowupFields','updateFollowupAppointmentField',
   'openContactFormForCustomer','selectPlanningOwnerForCustomer','openPlanningEditorForCustomer',
   'openPlanningEditor','syncPlanningEditorAppointment'].map(source).join('\n')}
const chooseType=async value=>{
  radioInputs.forEach(input=>{input.checked=input.value===value;});
  await radioInputs.find(input=>input.checked).dispatch('change'); await flush();
};
const installContactForm=()=>{
  for(const id of ['form-header-customer','f-date','f-planned-activity-id','f-client-request-id',
    'f-seller','f-contactPerson','f-channel','f-result','f-comment','f-followup-enabled','f-followup-owner',
    'f-followup-type','f-nextFollowUp','f-nextFollowUpTime','f-followup-note','f-followup-appointment',
    'f-followup-picking-help','f-followup-owner-field','f-followup-time-suggestion','followup-fields',
    'f-followup-appointment-field','f-followup-picking-help-field','contact-save-error']) node(id);
};
installContactForm();
node('planning-owner-select','olle');
`,context);
// Run the real static contact registration, including the actual change listeners.
const contactStart=html.indexOf('  document.getElementById("f-followup-enabled").addEventListener');
assert.ok(contactStart>=0);
const contactEnd=html.indexOf('\n  });',html.indexOf('  contactTimeSuggestion = setupPlanningTimeSuggestion(',contactStart))+7;
vm.runInContext(html.slice(contactStart,contactEnd),context);
const ownerStart=html.indexOf('  document.getElementById("planning-owner-select").addEventListener');
assert.ok(ownerStart>=0);
const ownerEnd=html.indexOf('\n  });',ownerStart)+7;
vm.runInContext(html.slice(ownerStart,ownerEnd),context);
'''


@skipUnless(shutil.which("node"), "Node.js required for real frontend regression tests")
class PlanningTimeSuggestionTests(TestCase):
    def run_browser_code(self, body):
        result = subprocess.run(
            ["node", "-e", NODE_HARNESS + body, str(INDEX_PATH)],
            capture_output=True, text=True, timeout=20,
            # These tests must pass even when the execution host is not in Sweden.
            env={**os.environ, "TZ": "Pacific/Honolulu"},
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_first_available_checks_real_intervals_durations_statuses_and_sellers(self):
        self.run_browser_code(r'''
run(`
  const suggest=(activities,contactType='visit',owner='olle')=>planningFirstAvailableTime({
    dateKey:'2026-10-02',owner,contactType,activities,now:new Date('2026-10-01T20:00:00Z')});
  assert.equal(suggest([]),'08:00');
  assert.equal(suggest([activity('08:00')]),'08:30');
  assert.equal(suggest([activity('07:55','phone')]),'08:30','activity beginning before 08:00 still overlaps');
  assert.equal(suggest([activity('07:40','visit')]),'08:00','touching endpoints are free');
  assert.equal(suggest([activity('08:15','visit')]),'09:00','visit spans and blocks both 08:00 and 08:30 proposals');
  assert.equal(suggest([activity('08:15','visit')],'phone'),'08:00','new phone lasts only ten minutes');
  assert.equal(suggest([activity('08:15','visit')],'email'),'08:00','new email lasts only ten minutes');
  assert.equal(suggest([activity('08:10','phone')],'phone'),'08:00','half-open intervals permit adjacency');
  assert.equal(suggest([activity('08:10','phone')],'visit'),'08:30');
  assert.equal(suggest([activity('08:00','visit','johan')]),'08:00','another seller must not block');
  assert.equal(suggest([activity('08:00','visit','  OlLe  ')]),'08:30','seller identity is canonical');
  assert.equal(suggest([activity('08:00','visit','olle',{calendar_column:'email'})]),'08:30');
  for (const status of ['cancelled','superseded','skipped']) {
    assert.equal(suggest([activity('08:00','visit','olle',{status})]),'08:00',status);
  }
  assert.equal(suggest([activity('08:00','visit','olle',{status:'completed'})]),'08:30');
  assert.equal(suggest([activity('08:00','visit','olle',{scheduled_at:'2026-10-03T08:00:00+02:00'})]),'08:00');
  assert.equal(suggest([activity('08:00','visit','olle',{scheduled_at:'2026-10-02T06:00:00Z'})]),'08:30','UTC timestamps use Stockholm date and time');
  const full=Array.from({length:32},(_,i)=>activity(String(8+Math.floor(i/2)).padStart(2,'0')+':'+(i%2?'30':'00')));
  assert.equal(suggest(full),'','no date rollover when the entire day is occupied');
  assert.equal(suggest(full.slice(0,-1)),'23:30','last same-day grid slot remains usable');
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_today_does_not_offer_elapsed_times_in_stockholm_or_roll_into_tomorrow(self):
        self.run_browser_code(r'''
run(`
  const suggest=(dateKey,iso)=>planningFirstAvailableTime({dateKey,owner:'olle',contactType:'visit',activities:[],now:new Date(iso)});
  assert.equal(suggest('2026-07-01','2026-07-01T05:59:59Z'),'08:00');
  assert.equal(suggest('2026-07-01','2026-07-01T06:00:00Z'),'08:00');
  assert.equal(suggest('2026-07-01','2026-07-01T06:00:01Z'),'08:30','seconds count as elapsed time');
  assert.equal(suggest('2026-07-01','2026-07-01T06:29:59Z'),'08:30');
  assert.equal(suggest('2026-12-01','2026-12-01T07:00:01Z'),'08:30','winter Stockholm offset');
  assert.equal(suggest('2026-07-01','2026-07-01T21:30:01Z'),'');
  assert.equal(suggest('2026-07-01','2026-07-01T22:15:00Z'),'','Stockholm already has the next date');
  assert.equal(suggest('2026-07-02','2026-07-01T22:15:00Z'),'08:00');
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_controller_reloads_current_day_and_seller_and_preserves_manual_time(self):
        self.run_browser_code(r'''
run(`
  const date=node('date','2026-10-02'), time=node('time','08:00'), owner=node('owner','olle');
  const type=node('type','visit'), message=node('message');
  replyActivities=[activity('08:15','phone')];
  const controller=setupPlanningTimeSuggestion({dateInput:date,timeInput:time,
    changeInputs:[date,owner,type],getOwner:()=>owner.value,getType:()=>type.value,message});
  await controller.refresh();
  assert.equal(time.value,'08:30');
  const first=new URLSearchParams(requests.at(-1).url.split('?')[1]);
  assert.equal(first.get('start'),'2026-10-01','fetch includes previous day rather than visible week cache');
  assert.equal(first.get('end'),'2026-10-02');
  assert.equal(first.get('user_name'),'olle');
  assert.equal(first.get('availability'),'1');
  assert.equal(requests.at(-1).options.cache,'no-store');
  type.value='phone'; await type.dispatch('change'); await flush();
  assert.equal(time.value,'08:00','changing new duration recalculates actual overlap');
  owner.value='johan'; await owner.dispatch('change'); await flush();
  assert.equal(time.value,'08:00');
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('user_name'),'johan');
  date.value='2026-10-03'; await date.dispatch('change'); await flush();
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('end'),'2026-10-03');
  time.value='11:17'; await time.dispatch('input');
  type.value='visit'; date.value='2026-10-02'; owner.value='olle';
  await type.dispatch('change'); await date.dispatch('change'); await owner.dispatch('change'); await controller.refresh();
  assert.equal(time.value,'11:17','manual choice survives every automatic refresh trigger');
  controller.reset(); await controller.refresh();
  assert.equal(time.value,'08:30','a fresh form can resume automatic suggestions');
  controller.cancel();
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_controller_rejects_stale_and_wrong_owner_responses_and_explains_no_slot(self):
        self.run_browser_code(r'''
run(`
  const date=node('date','2026-10-02'), time=node('time'), owner=node('owner','olle'), type=node('type','visit'), message=node('message');
  const controller=setupPlanningTimeSuggestion({dateInput:date,timeInput:time,
    changeInputs:[date,owner,type],getOwner:()=>owner.value,getType:()=>type.value,message});
  deferRequests=true;
  const old=controller.refresh();
  owner.value='johan'; const latest=controller.refresh();
  pending[1].resolve({owner:{user_name:'johan'},activities:[]}); await latest;
  assert.equal(time.value,'08:00');
  pending[0].resolve({owner:{user_name:'olle'},activities:[activity('08:00')]}); await old;
  assert.equal(time.value,'08:00','old seller response must never overwrite current suggestion');
  const whileManual=controller.refresh(); time.value='12:13'; await time.dispatch('change');
  pending[2].resolve({owner:{user_name:'johan'},activities:[]}); await whileManual;
  assert.equal(time.value,'12:13','manual edits during fetch win');
  controller.reset(); deferRequests=false; replyOwner='olle';
  await controller.refresh();
  assert.equal(time.value,'','a mismatched calendar must not be presented as free');
  assert.ok(message.textContent.trim(),'wrong-owner result needs visible feedback');
  replyOwner=null; fetchFailure='offline'; await controller.refresh();
  assert.equal(time.value,''); assert.ok(message.textContent.trim(),'network failure needs feedback');
  fetchFailure=null; owner.value='olle';
  replyActivities=Array.from({length:32},(_,i)=>activity(String(8+Math.floor(i/2)).padStart(2,'0')+':'+(i%2?'30':'00')));
  await controller.refresh();
  assert.equal(time.value,''); assert.equal(date.value,'2026-10-02');
  assert.match(message.textContent,/ledig|fullbok|tillgänglig/i);
  const detached=controller.refresh(); controller.cancel(); await detached;
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_log_contact_followup_uses_fresh_calendar_and_resets_manual_choice_between_customers(self):
        self.run_browser_code(ENTRY_HARNESS + r'''
run(`
  planningData={activities:[activity('08:00')]}; // deliberately stale visible calendar
  openContactFormForCustomer(currentCustomer); await flush();
  assert.equal(requests.length,0,'disabled follow-up needs no availability request');
  const enabled=elements.get('f-followup-enabled'), date=elements.get('f-nextFollowUp');
  const time=elements.get('f-nextFollowUpTime'), type=elements.get('f-followup-type');
  enabled.checked=true; await enabled.dispatch('change'); await flush();
  assert.equal(date.value,'2026-10-08'); assert.equal(time.value,'08:00','future follow-up starts at 08:00');
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('user_name'),'olle');
  replyActivities=[activity('08:15','phone')]; date.value='2026-10-02';
  await date.dispatch('change'); await flush(); assert.equal(time.value,'08:30');
  type.value='email'; await type.dispatch('change'); await flush(); assert.equal(time.value,'08:00');
  time.value='11:17'; await time.dispatch('input'); date.value='2026-10-03';
  await date.dispatch('change'); await flush(); assert.equal(time.value,'11:17');
  openContactFormForCustomer({...currentCustomer,row:3,customer:'Nästa butik'}); await flush();
  enabled.checked=true; await enabled.dispatch('change'); await flush();
  assert.equal(time.value,'08:00','next customer form resets manual intent');
  currentUser.admin=true; currentCustomer.sales_person='Johan';
  await updateFollowupFields();
  assert.equal(elements.get('f-followup-owner').value,'johan');
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('user_name'),'johan');
  date.value='2026-10-02'; await date.dispatch('change'); await flush();
  assert.equal(time.value,'08:00','other seller calendar does not consume Johan slots');
  elements.get('f-followup-owner').value='olle';
  await elements.get('f-followup-owner').dispatch('change'); await flush();
  assert.equal(time.value,'08:30','owner selector change reloads Olle availability');
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_calendar_editor_recalculates_types_and_dates_and_leaves_existing_booking_unchanged(self):
        self.run_browser_code(ENTRY_HARNESS + r'''
run(`
  replyActivities=[];
  openPlanningEditor({date:'2026-10-02'}); await flush();
  assert.equal(elements.get('planning-editor-time').value,'08:00');
  replyActivities=[activity('08:15','phone')];
  const date=elements.get('planning-editor-date'); date.value='2026-10-02';
  await date.dispatch('change'); await flush();
  assert.equal(elements.get('planning-editor-time').value,'08:30');
  await chooseType('phone'); assert.equal(elements.get('planning-editor-time').value,'08:00');
  await chooseType('email'); assert.equal(elements.get('planning-editor-time').value,'08:00');
  currentUser.admin=true;
  await chooseType('visit'); assert.equal(elements.get('planning-editor-time').value,'08:30');
  const owner=elements.get('planning-owner-select'); owner.value='johan';
  await owner.dispatch('change'); await flush();
  assert.equal(planningSelectedUserName,'johan');
  assert.equal(elements.get('planning-editor-time').value,'08:00','real calendar seller handler refreshes availability');
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('user_name'),'johan');
  const time=elements.get('planning-editor-time'); time.value='12:17'; await time.dispatch('change');
  owner.value='olle'; await owner.dispatch('change'); await flush();
  assert.equal(planningSelectedUserName,'olle');
  assert.equal(time.value,'12:17','real seller change preserves a manually chosen time');
  await chooseType('visit'); date.value='2026-10-03'; await date.dispatch('change'); await flush();
  assert.equal(time.value,'12:17');
  const booking=activity('10:17','phone','olle',{note:'Befintlig bokning'}), before=JSON.stringify(booking);
  const fetchCount=requests.length;
  openPlanningEditor({activity:booking}); await flush();
  assert.equal(elements.get('planning-editor-time').value,'10:17');
  assert.equal(elements.get('planning-editor-date').value,'2026-10-02');
  assert.equal(requests.length,fetchCount,'editing existing bookings does not propose a replacement');
  await chooseType('visit'); elements.get('planning-editor-date').value='2026-10-03';
  await elements.get('planning-editor-date').dispatch('change'); await flush();
  assert.equal(elements.get('planning-editor-time').value,'10:17');
  assert.equal(JSON.stringify(booking),before);
`).catch(error=>{console.error(error);process.exitCode=1;});
''')

    def test_customer_card_uses_selected_sellers_current_calendar_outside_calendar_view(self):
        self.run_browser_code(ENTRY_HARNESS + r'''
run(`
  currentUser={user_name:'admin',name:'Admin',admin:true};
  planningSelectedUserName='olle';
  planningData={available_users:planningActiveUsersCache,activities:[activity('08:00','visit','olle')]};
  replyActivities=[activity('08:00','email','johan'),activity('08:30','visit','olle')];
  const customer={row:3,customer:'Johans butik',sales_person:'Johan'};
  await openPlanningEditorForCustomer(customer,'2026-10-02'); await flush();
  assert.equal(planningSelectedUserName,'johan');
  assert.equal(elements.get('planning-editor-date').value,'2026-10-02');
  assert.equal(elements.get('planning-editor-time').value,'08:30','uses fetched Johan calendar instead of cached Olle activities');
  const query=new URLSearchParams(requests.at(-1).url.split('?')[1]);
  assert.equal(query.get('user_name'),'johan'); assert.equal(query.get('end'),'2026-10-02');
  assert.equal(requests.at(-1).options.cache,'no-store');
  const date=elements.get('planning-editor-date'); date.value='2026-10-03';
  await date.dispatch('change'); await flush(); assert.equal(elements.get('planning-editor-time').value,'08:00');
  const time=elements.get('planning-editor-time'); time.value='15:43'; await time.dispatch('input');
  await chooseType('phone'); assert.equal(time.value,'15:43');
  replyActivities=[];
  await openPlanningEditorForCustomer(customer,'2026-10-02'); await flush();
  assert.equal(elements.get('planning-editor-time').value,'08:00','reopened card gets fresh availability');
  currentUser={user_name:'olle',name:'Olle',admin:false};
  planningData=null; planningSelectedUserName='johan';
  replyActivities=[activity('08:00','phone','olle'),activity('08:30','visit','johan')];
  const previousRequests=requests.length;
  await openPlanningEditorForCustomer(currentCustomer,'2026-10-02'); await flush();
  assert.equal(planningData,null,'first customer-card open does not require a loaded calendar view');
  assert.ok(requests.length>previousRequests,'first customer-card open fetches availability');
  assert.equal(new URLSearchParams(requests.at(-1).url.split('?')[1]).get('user_name'),'olle');
  assert.equal(elements.get('planning-editor-time').value,'08:30','logged-in seller uses fresh own calendar despite stale owner selection');
`).catch(error=>{console.error(error);process.exitCode=1;});
''')
