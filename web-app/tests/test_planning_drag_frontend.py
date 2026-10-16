"""Run calendar drag persistence and layout regressions against real JS helpers."""

from pathlib import Path
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
const context = vm.createContext({assert, URLSearchParams, console, setImmediate, setTimeout, clearTimeout});
vm.runInContext(`
${['PLANNING_CONTACT_TYPES','PLANNING_TIME_ZONE','PLANNING_CALENDAR_PX_PER_MINUTE','PLANNING_STATUS_LABELS'].map(constant).join('\n')}
let currentUser={user_name:'olle',name:'Olle'}, planningSelectedUserName='olle';
let planningSelectedDate='2026-10-19', planningWeekStart='2026-10-19';
let planningData=null, planningLoading=false, planningLoadSerial=0, planningActiveUsersCache=[];
let planningDragState=null, planningDragFrame=0, planningDragAutoScrollFrame=0;
let planningDragMutationSerial=0;
const planningDragSavingIds=new Set(), planningDragRetries=new Map(), planningDragOperations=new Map();
const planningDragActivityVersions=new Map();
const planningDragSuppressedClicks=new Map();
const requests=[], toasts=[], insightCalls=[], domUpdates=[];
let agendaRenders=0, fullWeekLoads=0, fullRefreshes=0, recommendationLoads=0;
const userIsAdmin=()=>false, normalizePlanningUser=value=>value;
const normalizeRouteIdentity=value=>String(value||'').trim().toLowerCase();
const planningTodayKey=()=>'2026-10-19', planningFormatDate=value=>value;
const planningOwnerQuery=value=>value;
const esc=value=>String(value), escAttr=value=>String(value);
const showToast=message=>toasts.push(message);
const loadInsights=async options=>{insightCalls.push(options);};
const loadPlanningRecommendation=async()=>{recommendationLoads++;};
const refreshWorkflowViews=async()=>{fullRefreshes++;throw new Error('Unexpected full workflow refresh');};
const loadPlanningWeek=async()=>{fullWeekLoads++;throw new Error('Unexpected full week reload');};
const renderPlanningWeekStrip=()=>{}, renderPlanningOwnerSelect=()=>{}, renderPlanning=()=>{}, renderPlanningLoadError=()=>{};
const resumePlanningRoutePreviewRecovery=()=>{};
const planningFetchJson=(path,options={})=>new Promise((resolve,reject)=>requests.push({path,options,resolve,reject}));
let nextRequestId=0;
const planningClientRequestId=prefix=>prefix+'-'+(++nextRequestId);
const tick=async()=>{for(let i=0;i<20;i++) await Promise.resolve();};
let activeView='planning';
const activeViewName=()=>activeView;
const elements=new Map();
const document={getElementById:id=>elements.get(id)||null,querySelector:()=>null,
  addEventListener(){},removeEventListener(){},body:{classList:{add(){},remove(){}}}};
const window={scrollX:23,scrollY:1150,scrollBy(){throw new Error('Unexpected scrolling');},
  scrollTo(){throw new Error('Unexpected scrolling');},setTimeout,clearTimeout};
const requestAnimationFrame=()=>1, cancelAnimationFrame=()=>{};
${['planningDateFromKey','planningDateKey','planningAddDays','planningStartOfWeek',
   'planningStockholmOffset','planningStockholmIso','planningDateKeyFromIso','planningTimeFromIso',
   'planningActivityId','planningTruthy','normalizePlanningActivity','planningEffectiveStatus',
   'planningSelectedOwner','getPlanningActivity','planningActivitiesForDate','planningUnplannedForDate',
   'planningAgendaItemsForDate','planningStatusSummary'].map(source).join('\n')}
${html.slice(html.indexOf('  function planningActivityMinutes('),html.indexOf('  function planningOwnerIdentityKeys('))}
const realPlanningRenderDragUpdate=planningRenderDragUpdate;
renderPlanningAgenda=()=>{agendaRenders++;throw new Error('Unexpected agenda replacement');};
planningRenderDragUpdate=()=>{domUpdates.push(planningData);};
const activity=(id='a',time='09:00',extra={})=>normalizePlanningActivity({
  id,customer:'Butik '+id,user_name:'olle',contact_type:'visit',status:'planned',source:'manual',
  scheduled_at:planningStockholmIso(planningSelectedDate,time),revision:1,updated_at:'original-'+id,
  ...extra,
});
const data=(activities=[activity()])=>({owner:{user_name:'olle'},activities,available_users:[]});
const server=(row,time,revision,extra={})=>({...row,scheduled_at:planningStockholmIso(planningSelectedDate,time),
  revision,updated_at:'server-'+revision,...extra});
const payload=request=>JSON.parse(request.options.body);
`,context);
context.realLoadPlanningWeekSource=source('loadPlanningWeek');
const run = async code => await vm.runInContext(`(async()=>{${code}\n})()`,context);
'''


@skipUnless(shutil.which("node"), "Node.js required for real calendar drag helpers")
class PlanningDragFrontendTests(TestCase):
    def run_node(self, scenario):
        script = NODE_HARNESS + "\nrun(" + repr(scenario) + r''')
  .catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(
            ["node", "-e", script, str(INDEX_PATH)],
            capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_success_updates_local_server_revision_without_reloading_or_scrolling(self):
        self.run_node(r'''
const original=activity('a','09:00',{source:'route',time_is_estimated:true});
planningData=data([original]);
const save=planningCommitDraggedActivity(original,690);
await tick();
assert.equal(getPlanningActivity('a').scheduled_at,planningStockholmIso(planningSelectedDate,'11:30'));
assert.equal(getPlanningActivity('a').source,'manual');
assert.equal(getPlanningActivity('a').time_is_estimated,false);
assert.equal(requests.length,1); assert.equal(requests[0].path,'/planning/activities/a');
assert.equal(requests[0].options.method,'PATCH');
const body=payload(requests[0]);
assert.deepEqual(Object.keys(body).sort(),['client_request_id','expected_revision','expected_updated_at','scheduled_at']);
assert.equal(body.expected_revision,1); assert.equal(body.expected_updated_at,'original-a');
assert.ok(body.client_request_id); assert.equal(body.scheduled_at,planningStockholmIso(planningSelectedDate,'11:30'));
requests[0].resolve({activity:server(original,'11:30',7,{source:'manual',time_is_estimated:false,note:'canonical server note'})});
await save; await tick();
assert.equal(getPlanningActivity('a').revision,7);
assert.equal(getPlanningActivity('a').updated_at,'server-7');
assert.equal(getPlanningActivity('a').note,'canonical server note');
assert.equal(planningDragSavingIds.size,0); assert.equal(planningDragOperations.size,0);
assert.equal(planningDragRetries.size,0);
assert.equal(fullWeekLoads,0); assert.equal(fullRefreshes,0); assert.equal(agendaRenders,0);
assert.equal(recommendationLoads,1); assert.ok(domUpdates.length>=2);
assert.ok(insightCalls.length>=1); assert.ok(insightCalls.every(options=>options.render===false));
assert.equal(window.scrollX,23); assert.equal(window.scrollY,1150);
''')

    def test_rapid_drags_serialize_each_activity_and_keep_the_latest_visible_target(self):
        self.run_node(r'''
const a=activity('a'), b=activity('b','10:00',{revision:4}); planningData=data([a,b]);
const first=planningCommitDraggedActivity(a,630);
const other=planningCommitDraggedActivity(b,720);
await tick(); assert.equal(requests.length,2);
const second=planningCommitDraggedActivity(getPlanningActivity('a'),780);
await tick(); assert.equal(requests.length,2,'A second move must wait for the confirmed revision');
assert.equal(getPlanningActivity('a').scheduled_at,planningStockholmIso(planningSelectedDate,'13:00'));
requests.find(request=>request.path.endsWith('/b')).resolve({activity:server(b,'12:00',5)});
await other;
requests.find(request=>request.path.endsWith('/a')).resolve({activity:server(a,'10:30',2)});
await tick(); assert.equal(requests.length,3);
assert.equal(getPlanningActivity('a').scheduled_at,planningStockholmIso(planningSelectedDate,'13:00'),
  'The earlier response must not roll the newer optimistic move back');
assert.equal(payload(requests[2]).scheduled_at,planningStockholmIso(planningSelectedDate,'13:00'));
assert.equal(payload(requests[2]).expected_revision,2);
assert.equal(payload(requests[2]).expected_updated_at,'server-2');
assert.notEqual(payload(requests[2]).client_request_id,payload(requests[0]).client_request_id);
requests[2].resolve({activity:server(a,'13:00',3)});
await Promise.all([first,second]);
assert.equal(getPlanningActivity('a').revision,3); assert.equal(getPlanningActivity('b').revision,5);
assert.equal(getPlanningActivity('b').scheduled_at,planningStockholmIso(planningSelectedDate,'12:00'));
assert.equal(planningDragSavingIds.size,0); assert.equal(planningDragOperations.size,0);
assert.equal(fullWeekLoads+fullRefreshes+agendaRenders,0);
''')

    def test_ambiguous_save_retries_exact_body_before_a_different_new_target(self):
        self.run_node(r'''
const original=activity(); planningData=data([original]);
const failed=planningCommitDraggedActivity(original,600); await tick();
const firstBody=requests[0].options.body, firstId=payload(requests[0]).client_request_id;
requests[0].reject(new TypeError('Network connection lost after sending'));
await failed;
assert.equal(getPlanningActivity('a').scheduled_at,original.scheduled_at);
assert.equal(planningDragRetries.size,1); assert.equal(planningDragSavingIds.size,0);
assert.ok(toasts.at(-1));
const retry=planningCommitDraggedActivity(getPlanningActivity('a'),660); await tick();
assert.equal(requests.length,2); assert.equal(requests[1].options.body,firstBody);
assert.equal(payload(requests[1]).client_request_id,firstId);
requests[1].resolve({activity:server(original,'10:00',2)}); await tick();
assert.equal(requests.length,3,'The changed target is sent after recovering the uncertain result');
assert.equal(getPlanningActivity('a').scheduled_at,planningStockholmIso(planningSelectedDate,'11:00'));
assert.equal(payload(requests[2]).expected_revision,2);
assert.equal(payload(requests[2]).scheduled_at,planningStockholmIso(planningSelectedDate,'11:00'));
assert.notEqual(payload(requests[2]).client_request_id,firstId);
requests[2].resolve({activity:server(original,'11:00',3)}); await retry;
assert.equal(getPlanningActivity('a').revision,3); assert.equal(planningDragRetries.size,0);
assert.equal(fullWeekLoads+fullRefreshes+agendaRenders,0);
''')

    def test_retries_for_multiple_activities_remain_independent(self):
        self.run_node(r'''
const a=activity('a'), b=activity('b'); planningData=data([a,b]);
const saves=[planningCommitDraggedActivity(a,600),planningCommitDraggedActivity(b,660)];
await tick(); const bodies=requests.map(request=>request.options.body);
requests[0].reject(Object.assign(new Error('Server unavailable'),{status:503}));
requests[1].reject(new TypeError('Failed to fetch')); await Promise.all(saves);
assert.equal(planningDragRetries.size,2);
const retryA=planningCommitDraggedActivity(getPlanningActivity('a'),600); await tick();
assert.equal(requests[2].options.body,bodies[0]);
requests[2].resolve({activity:server(a,'10:00',2)}); await retryA;
assert.equal(planningDragRetries.size,1);
const retryB=planningCommitDraggedActivity(getPlanningActivity('b'),660); await tick();
assert.equal(requests[3].options.body,bodies[1]);
requests[3].resolve({activity:server(b,'11:00',2)}); await retryB;
assert.equal(planningDragRetries.size,0);
assert.equal(getPlanningActivity('a').revision,2); assert.equal(getPlanningActivity('b').revision,2);
''')

    def test_definitive_errors_rollback_and_conflicts_use_authoritative_activity(self):
        self.run_node(r'''
const original=activity(); planningData=data([original]);
let save=planningCommitDraggedActivity(original,600); await tick();
requests[0].reject(Object.assign(new Error('Forbidden'),{status:403,code:'forbidden'})); await save;
assert.equal(getPlanningActivity('a').scheduled_at,original.scheduled_at);
assert.equal(planningDragRetries.size,0); assert.equal(planningDragSavingIds.size,0);
assert.ok(toasts.at(-1));
save=planningCommitDraggedActivity(getPlanningActivity('a'),660); await tick();
const authoritative=server(original,'14:30',8,{note:'Changed by another device'});
requests[1].reject(Object.assign(new Error('Planning changed'),{status:409,code:'revision_conflict',payload:{activity:authoritative}}));
await save;
assert.equal(getPlanningActivity('a').scheduled_at,authoritative.scheduled_at);
assert.equal(getPlanningActivity('a').revision,8);
assert.equal(getPlanningActivity('a').note,'Changed by another device');
assert.equal(planningDragRetries.size,0); assert.equal(planningDragOperations.size,0);
assert.equal(fullWeekLoads+fullRefreshes+agendaRenders,0);
''')

    def test_old_responses_cannot_change_a_new_calendar_or_replace_newer_activity_data(self):
        self.run_node(r'''
for (const failure of [false,true]) {
  const old=activity('a'); planningData=data([old]);
  const save=planningCommitDraggedActivity(old,600); await tick();
  const request=requests.at(-1);
  const replacement=activity('a','15:00',{user_name:'johan',revision:20});
  planningLoadSerial++;
  planningData={owner:{user_name:'johan'},activities:[replacement],available_users:[]};
  planningSelectedUserName='johan';
  const visibleData=planningData, updateCount=domUpdates.length;
  if (failure) request.reject(new TypeError('Late network failure'));
  else request.resolve({activity:server(old,'10:00',2)});
  await save;
  assert.equal(planningData,visibleData); assert.equal(getPlanningActivity('a'),replacement);
  assert.equal(getPlanningActivity('a').revision,20); assert.equal(domUpdates.length,updateCount);
  if (failure) assert.match(toasts.at(-1),/kunde inte slutföras/,'A late failed save still needs visible feedback');
}
planningSelectedUserName='olle';
const old=activity('a'); planningData=data([old]);
const save=planningCommitDraggedActivity(old,600); await tick();
const newer=activity('a','16:00',{revision:12}); planningData.activities=[newer];
requests.at(-1).resolve({activity:server(old,'10:00',2)}); await save;
assert.equal(getPlanningActivity('a'),newer); assert.equal(getPlanningActivity('a').revision,12);
assert.equal(planningDragSavingIds.size,0); assert.equal(fullWeekLoads+fullRefreshes+agendaRenders,0);
''')

    def test_missing_server_confirmation_is_ambiguous_and_replaying_can_return_to_original_time(self):
        self.run_node(r'''
const original=activity(); planningData=data([original]);
const save=planningCommitDraggedActivity(original,600); await tick();
const originalBody=requests[0].options.body;
requests[0].resolve({ok:true}); await save;
assert.equal(getPlanningActivity('a').scheduled_at,original.scheduled_at);
assert.equal(planningDragRetries.size,1,'A response without revision cannot confirm a saved mutation');
const retry=planningCommitDraggedActivity(getPlanningActivity('a'),540); await tick();
assert.equal(requests.length,2,'An apparent no-op must recover an uncertain server result');
assert.equal(requests[1].options.body,originalBody);
requests[1].resolve({activity:server(original,'10:00',2)}); await tick();
assert.equal(requests.length,3);
assert.equal(payload(requests[2]).expected_revision,2);
assert.equal(payload(requests[2]).scheduled_at,original.scheduled_at);
requests[2].resolve({activity:server(original,'09:00',3)}); await retry;
assert.equal(getPlanningActivity('a').scheduled_at,original.scheduled_at);
assert.equal(getPlanningActivity('a').revision,3); assert.equal(planningDragRetries.size,0);
''')

    def test_queued_target_is_saved_after_navigation_without_changing_the_new_calendar(self):
        self.run_node(r'''
const original=activity();planningData=data([original]);
const first=planningCommitDraggedActivity(original,600);await tick();
const second=planningCommitDraggedActivity(getPlanningActivity('a'),660);await tick();
assert.equal(requests.length,1);
const replacement=activity('a','15:00',{user_name:'johan',revision:30});
planningData={owner:{user_name:'johan'},activities:[replacement],available_users:[]};
planningSelectedUserName='johan';planningLoadSerial++;
const visibleData=planningData, updateCount=domUpdates.length;
requests[0].resolve({activity:server(original,'10:00',2)});await tick();
assert.equal(requests.length,2,'The already dropped second target must still be saved after navigation');
assert.equal(payload(requests[1]).expected_revision,2);
assert.equal(payload(requests[1]).scheduled_at,planningStockholmIso(planningSelectedDate,'11:00'));
assert.equal(planningData,visibleData);assert.equal(getPlanningActivity('a'),replacement);
assert.equal(domUpdates.length,updateCount);
requests[1].resolve({activity:server(original,'11:00',3)});await Promise.all([first,second]);
assert.equal(planningData,visibleData);assert.equal(getPlanningActivity('a'),replacement);
assert.equal(domUpdates.length,updateCount);assert.equal(planningDragOperations.size,0);
assert.equal(planningDragSavingIds.size,0);assert.equal(toasts.length,0);
''')

    def test_week_get_started_before_drag_cannot_replace_confirmed_local_data(self):
        self.run_node(r'''
eval(realLoadPlanningWeekSource);
const original=activity(); planningData=data([original]);
const calendar={dataset:{calendarContext:planningCalendarContextKey()}};
let agendaWrites=0;
elements.set('planning-agenda',{querySelector:()=>calendar,
  set innerHTML(_value){agendaWrites++;throw new Error('Visible calendar removed');}});
elements.set('planning-backlog',{set innerHTML(_value){throw new Error('Visible backlog removed');}});
const weekGet=loadPlanningWeek(); await tick(); assert.equal(requests.length,1);
const save=planningCommitDraggedActivity(original,600); await tick();
requests[1].resolve({activity:server(original,'10:00',2)}); await save;
const confirmed=getPlanningActivity('a'), confirmedData=planningData;
requests[0].resolve(data([original]));
assert.equal(await weekGet,true);
assert.equal(planningData,confirmedData); assert.equal(getPlanningActivity('a'),confirmed);
assert.equal(getPlanningActivity('a').revision,2); assert.equal(planningLoading,false);
assert.equal(agendaWrites,0);
// A fresh same-context read may still synchronize data once no drag is pending.
const fresh=loadPlanningWeek(); await tick();
requests[2].resolve(data([server(original,'10:00',3)]));
assert.equal(await fresh,true); assert.equal(getPlanningActivity('a').revision,3);
assert.equal(agendaWrites,0);
''')

    def test_same_calendar_get_merges_unrelated_changes_while_preserving_a_pending_drag(self):
        self.run_node(r'''
eval(realLoadPlanningWeekSource);
const a=activity('a'),b=activity('b','12:00');planningData=data([a,b]);
const existingData=planningData;
const calendar={dataset:{calendarContext:planningCalendarContextKey()}};
elements.set('planning-agenda',{querySelector:()=>calendar,
  set innerHTML(_value){throw new Error('Visible calendar removed');}});
elements.set('planning-backlog',{set innerHTML(_value){throw new Error('Visible backlog removed');}});
const save=planningCommitDraggedActivity(a,600);await tick();
const pending=getPlanningActivity('a');
const get=loadPlanningWeek();await tick();assert.equal(requests.length,2);
const cancelled=server(b,'12:00',2,{status:'cancelled'});
const added=activity('c','14:00');
requests[1].resolve(data([a,cancelled,added]));assert.equal(await get,true);
assert.equal(planningData,existingData,'A retained calendar keeps its data context for active saves');
assert.equal(getPlanningActivity('a'),pending,'GET must not replace the pending optimistic row');
assert.equal(getPlanningActivity('a').scheduled_at,planningStockholmIso(planningSelectedDate,'10:00'));
assert.equal(getPlanningActivity('b').status,'cancelled');assert.equal(getPlanningActivity('b').revision,2);
assert.ok(getPlanningActivity('c'));
assert.deepEqual(planningAgendaItemsForDate(planningSelectedDate).map(row=>planningActivityId(row)),['a','c']);
requests[0].resolve({activity:server(a,'10:00',2)});await save;
assert.equal(getPlanningActivity('a').revision,2);assert.equal(getPlanningActivity('b').status,'cancelled');
assert.equal(planningDragOperations.size,0);assert.equal(fullWeekLoads+fullRefreshes+agendaRenders,0);
''')

    def test_invalidated_calendar_data_uses_the_normal_load_and_cannot_be_restored_by_an_old_get(self):
        self.run_node(r'''
eval(realLoadPlanningWeekSource);
const calendar={dataset:{calendarContext:planningCalendarContextKey()}};
let agendaWrites=0;
elements.set('planning-agenda',{querySelector:()=>calendar,set innerHTML(_value){agendaWrites++;}});
elements.set('planning-backlog',{set innerHTML(_value){}});
planningData=null;
const initial=loadPlanningWeek();await tick();
requests[0].resolve(data([activity()]));assert.equal(await initial,true);
assert.equal(agendaWrites,1,'Contact/route invalidation must be allowed to load a new model');
assert.ok(getPlanningActivity('a'));
const obsolete=loadPlanningWeek();await tick();
planningData=null;
requests[1].resolve(data([activity()]));assert.equal(await obsolete,false);
assert.equal(planningData,null,'An older GET must not undo later contact/route invalidation');
assert.equal(planningLoading,false);
''')

    def test_merge_accepts_newer_server_edits_and_preserves_a_locally_moved_row_missing_from_old_get(self):
        self.run_node(r'''
const a=activity('a'),b=activity('b','12:00');planningData=data([a,b]);
const before=planningDragMutationSerial;
const confirmed=server(a,'10:00',2);planningReplaceActivity('a',confirmed);
const missing=planningMergeLoadedActivities([b],before);
assert.equal(missing.find(row=>planningActivityId(row)==='a'),confirmed);
const canonical=server(a,'11:00',3,{note:'A newer server edit'});
const merged=planningMergeLoadedActivities([canonical,b],before);
assert.equal(merged[0],canonical,'A higher server revision wins over an already completed local move');
''')

    def test_real_dom_update_preserves_calendar_bounds_shells_cards_and_reserved_header_space(self):
        self.run_node(r'''
planningRenderDragUpdate=realPlanningRenderDragUpdate;
// Small DOM fixture: production rendering supplies every style, label and key.
// The fixture tracks identity and rejects wholesale calendar replacement.
class Node {
  constructor(className='',attributes={}) {
    this.className=className; this.attributes={...attributes}; this.dataset={};
    this.style={cssText:'',height:'',overflowY:''}; this.children=[]; this.parentElement=null;
    this.textContent=''; this.hidden=false; this.listeners=[];
    this.classList={contains:name=>this.className.split(/\s+/).includes(name),
      add:name=>{if(!this.classList.contains(name))this.className+=' '+name;},
      remove:name=>{this.className=this.className.split(/\s+/).filter(value=>value!==name).join(' ');}};
  }
  append(node) {node.remove();this.children.push(node);node.parentElement=this;}
  remove() {if(this.parentElement){const siblings=this.parentElement.children;siblings.splice(siblings.indexOf(this),1);this.parentElement=null;}}
  addEventListener(...args) {this.listeners.push(args);}
  setAttribute(name,value) {this.attributes[name]=value;}
  getAttribute(name) {return this.attributes[name]||null;}
  getBoundingClientRect() {return {height:this.className==='recommendation'?70:42};}
  matches(selector) {
    if(selector.startsWith('.'))return this.classList.contains(selector.slice(1));
    const name=/^\[([^\]]+)\]$/.exec(selector)?.[1];
    return name ? Object.hasOwn(this.attributes,name):false;
  }
  querySelectorAll(selector) {return this.children.flatMap(child=>[
    ...(child.matches(selector)?[child]:[]),...child.querySelectorAll(selector)]);}
  querySelector(selector) {return this.querySelectorAll(selector)[0]||null;}
}
const attribute=(tag,name)=>new RegExp('\\b'+name+'="([^"]*)"').exec(tag)?.[1]||'';
const parseShell=html=>{
  const tag=/<div\b[^>]*>/.exec(html)[0];
  const shell=new Node(attribute(tag,'class'),{'data-planning-calendar-key':attribute(tag,'data-planning-calendar-key')});
  shell.dataset.planningCalendarKey=attribute(tag,'data-planning-calendar-key');
  shell.style.cssText=attribute(tag,'style');
  const cardTag=/<(?:button|div)\b[^>]*class="planning-activity-card[^>]*>/.exec(html)[0];
  const card=new Node(attribute(cardTag,'class'),{'aria-label':attribute(cardTag,'aria-label')});
  const name=new Node('planning-activity-customer');
  name.textContent=/<span class="planning-activity-customer">([^<]*)<\/span>/.exec(html)[1];
  card.append(name);shell.append(card);
  const handleTag=/<button\b[^>]*data-planning-drag-handle[^>]*>/.exec(html)?.[0];
  if(handleTag)shell.append(new Node(attribute(handleTag,'class'),{
    'data-planning-drag-handle':attribute(handleTag,'data-planning-drag-handle'),
    'aria-label':attribute(handleTag,'aria-label')}));
  return shell;
};
document.createElement=tag=>{
  assert.equal(tag,'template');
  return {content:{firstElementChild:null},set innerHTML(value){this.content.firstElementChild=parseShell(value);}};
};
const agenda=new Node('agenda'), calendar=new Node('planning-calendar');
calendar.dataset={calendarContext:planningCalendarContextKey(),calendarStartMinutes:'240',calendarEndMinutes:'1080'};
calendar.style.height='1260px';
const remote=new Node('planning-calendar-column'), visits=new Node('planning-calendar-column');
const hours=new Node('planning-calendar-hours');calendar.append(hours);
calendar.append(remote);calendar.append(visits);agenda.append(calendar);
Object.defineProperty(agenda,'innerHTML',{set(){throw new Error('Agenda replaced');}});
elements.set('planning-agenda',agenda);
elements.set('planning-day-summary',new Node('summary'));
elements.set('planning-recommendation',new Node('recommendation'));
const early=activity('early','04:00'), phone=activity('phone','09:00',{contact_type:'phone'});
planningData=data([early,phone]);
const initial=planningCalendarLayout(planningData.activities,240);
initial.remote.forEach(item=>remote.append(parseShell(renderPlanningCalendarActivity(item))));
initial.visit.forEach(item=>visits.append(parseShell(renderPlanningCalendarActivity(item))));
const shell=visits.children[0], card=shell.querySelector('.planning-activity-card');
const phoneShell=remote.children[0], phoneCard=phoneShell.querySelector('.planning-activity-card');
planningDragSavingIds.add('early');
planningReplaceActivity('early',server(early,'11:30',2)); planningRenderDragUpdate();
assert.equal(agenda.querySelector('.planning-calendar'),calendar);
assert.equal(visits.children[0],shell);assert.equal(shell.querySelector('.planning-activity-card'),card);
assert.equal(remote.children[0],phoneShell);assert.equal(phoneShell.querySelector('.planning-activity-card'),phoneCard);
assert.equal(calendar.dataset.calendarStartMinutes,'240');assert.equal(calendar.dataset.calendarEndMinutes,'1080');
assert.equal(calendar.style.height,'1260px');
assert.ok(shell.style.cssText.includes('top:'+((690-240)*PLANNING_CALENDAR_PX_PER_MINUTE)+'px'));
assert.ok(card.getAttribute('aria-label').startsWith('11:30'));
assert.ok(shell.classList.contains('is-saving'));assert.ok(shell.querySelector('[data-planning-drag-handle]'));
assert.equal(elements.get('planning-recommendation').style.height,'70px');
assert.equal(elements.get('planning-day-summary').style.height,'42px');
assert.equal(elements.get('planning-recommendation').style.overflowY,'auto');
planningDragSavingIds.delete('early'); planningRenderDragUpdate();
assert.equal(visits.children[0],shell);assert.equal(shell.querySelector('.planning-activity-card'),card);
assert.equal(shell.classList.contains('is-saving'),false);
assert.equal(window.scrollX,23);assert.equal(window.scrollY,1150);assert.equal(agendaRenders,0);
// A separate edit may add an earlier time. Extend the existing grid, preserve
// its existing cards and compensate the added hour without a calendar rebuild.
const scrollAdjustments=[];window.innerHeight=900;window.scrollBy=(x,y)=>scrollAdjustments.push([x,y]);
calendar.getBoundingClientRect=()=>({height:1260,top:-90});
planningData.activities.push(activity('outside','03:00'));
planningRenderDragUpdate();
assert.equal(calendar.dataset.calendarStartMinutes,'180');
assert.equal(calendar.dataset.calendarEndMinutes,'1080');assert.equal(calendar.style.height,'1350px');
assert.deepEqual(scrollAdjustments,[[0,90]]);assert.equal(shell.querySelector('.planning-activity-card'),card);
assert.ok(hours.innerHTML.includes('03:00'));assert.equal(agendaRenders,0);
''')

    def test_pointer_activation_mobile_scroll_noop_drop_and_escape_keep_existing_semantics(self):
        self.run_node(r'''
const original=activity();planningData=data([original]);
for(const status of ['planned','skipped'])assert.equal(planningCanDragActivity({...original,status}),true);
for(const status of ['completed','cancelled','superseded'])assert.equal(planningCanDragActivity({...original,status}),false);
assert.equal(planningCanDragActivity({...original,unplanned:true}),false);
assert.equal(planningCanDragActivity({...original,user_name:'johan'}),false);
assert.equal(planningSnapDragMinutes(17,0,420,1080,20,1),450);
assert.equal(planningSnapDragMinutes(-100,0,420,1080,20,1),420);
assert.equal(planningSnapDragMinutes(10000,0,420,1080,20,1),1050);
const dragState=(mode='desktop',extra={})=>({pointerId:1,active:false,activationMode:mode,
  activity:original,activityId:'a',originX:0,originY:100,clientX:0,clientY:100,lastClientY:100,
  originalMinutes:540,targetMinutes:540,longPressTimer:0,scrolled:false,
  shell:{classList:{remove(){}}},sourceElement:{releasePointerCapture(){}},...extra});
let prevented=0,visualUpdates=0;
const move=(x,y,id=1)=>({pointerId:id,clientX:x,clientY:y,preventDefault(){prevented++;}});
planningActivateDrag=()=>{planningDragState.active=true;};
planningScheduleDragVisualUpdate=()=>{visualUpdates++;};
planningUpdateDragVisual=()=>{};
planningDragState=dragState();
planningDragPointerMove(move(2,100));assert.equal(planningDragState.active,false);
planningDragPointerMove(move(7,100));assert.equal(planningDragState.active,true);
assert.equal(visualUpdates,1);assert.equal(prevented,1);
planningDragPointerUp(move(7,100));assert.equal(planningDragState,null);assert.equal(requests.length,0);
assert.ok(planningDragSuppressedClicks.has('a'));
const scrolling=[];window.scrollBy=(x,y)=>scrolling.push([x,y]);
planningDragState=dragState('longpress');
planningDragPointerMove(move(0,91));assert.equal(planningDragState.activationMode,'scroll');
assert.equal(planningDragState.active,false);assert.equal(planningDragState.scrolled,true);
planningDragPointerMove(move(0,71));assert.deepEqual(scrolling,[[0,29]]);
planningDragPointerUp(move(0,71));assert.equal(planningDragState,null);assert.equal(requests.length,0);
planningDragState=dragState('handle');
planningDragPointerMove(move(0,110));assert.equal(planningDragState.active,true);
planningDragKeyDown({key:'Escape',preventDefault(){prevented++;}});
assert.equal(planningDragState,null);assert.equal(requests.length,0);assert.ok(toasts.at(-1));
await planningCommitDraggedActivity(original,540);assert.equal(requests.length,0);
''')
