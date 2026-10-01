from pathlib import Path
import re
import shutil
import subprocess
from unittest import TestCase, skipUnless


INDEX_PATH = Path(__file__).resolve().parents[1] / "index.html"


class PlanningFrontendContractTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = INDEX_PATH.read_text(encoding="utf-8")

    @skipUnless(shutil.which("node"), "Node.js required for real frontend helpers")
    def test_route_origin_gps_home_recovery_and_preview_use_real_frontend_functions(self):
        script = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[1], 'utf8');
const source = name => {
  const match = new RegExp(`  (?:async )?function ${name}\\(`).exec(html);
  assert.ok(match, name);
  return html.slice(match.index, html.indexOf('\n  }', match.index) + 4);
};
const context = vm.createContext({assert});
vm.runInContext(`
let currentUser = null, planningSelectedUserName = '', planningData = null;
let planningSelectedDate = '2026-10-02', planningRoutePreviewRequestId = '', planningRoutePreview = null;
let planningRouteRecoveryPromise = null, planningRouteRecoveryTimer = 0;
let gpsCalls = 0, posted = [], notices = [], errors = [], stored = null, switchOwnerOnGps = false;
const button = {disabled:false,textContent:'',addEventListener:()=>{}};
const document = {getElementById:()=>button};
const sessionStorage = {
  getItem:()=>stored, setItem:(_key,value)=>{stored=value;}, removeItem:()=>{stored=null;}
};
const PLANNING_ROUTE_RECOVERY_STORAGE_KEY = 'test', PLANNING_ROUTE_RECOVERY_TTL_MS = 86400000;
const PLANNING_ROUTE_RECOVERY_WINDOW_MS = 180000;
const planningTodayKey = () => '2026-10-01';
const planningDateFromKey = value => value === planningSelectedDate ? new Date(value) : null;
const normalizePlanningUser = value => value;
const planningRouteRecoveryForCurrentContext = () => null;
const resumePlanningRoutePreviewRecovery = () => {throw new Error('unexpected recovery');};
const clearPlanningRouteRecoveryState = () => {stored=null;};
const planningClientRequestId = () => 'request-1';
const getCurrentPositionForRoute = async () => {
  if (switchOwnerOnGps) planningSelectedUserName = 'johan';
  gpsCalls++; return {latitude:57.7,longitude:11.9,accuracy:10};
};
const runPlanningRouteRecoverySingleFlight = async (_id, callback) => callback();
const postPendingPlanningRoutePreview = async state => {posted.push(state.payload);};
const planningRouteResetPreviewButton = () => {button.disabled=false;};
const getRouteProposalFailureMessage = () => '';
const showToast = message => errors.push(message);
const esc = value => String(value), formatRouteMinutes = value => String(value);
const planningFormatDate = () => '', openPlanningModal = (_title,_subtitle,body) => notices.push(body);
const closePlanningModal = () => {}, applyPlanningRoutePreview = () => {};
${['userIsAdmin','normalizeRouteIdentity','planningSelectedOwner','planningRouteUsesOwnerHome',
   'planningRouteCurrentOwnerUserName','savePlanningRouteRecoveryState','readPlanningRouteRecoveryState',
   'openPlanningRoutePreview','planningRouteGpsCopy','renderPlanningRoutePreview'].map(source).join('\n')}
`, context);
const run = async () => {
  for (const [admin, actor, selected, home] of [
    [false,'olle','',false], [true,'olle','olle',false], [true,'admin','johan',true],
    [true,'admin','sofia',true],
  ]) {
    context.fixture = {admin,actor,selected,home};
    await vm.runInContext(`(async () => {
      currentUser = {user_name:fixture.actor,admin:fixture.admin};
      planningSelectedUserName = fixture.selected;
      stored=null; gpsCalls=0; posted=[]; errors=[];
      await openPlanningRoutePreview();
      assert.deepEqual(errors, []);
      assert.equal(gpsCalls, fixture.home ? 0 : 1);
      assert.equal(posted.length, 1);
      assert.equal(posted[0].user_name || '', fixture.selected);
      assert.equal(Boolean(posted[0].start), !fixture.home);
      const recovered = readPlanningRouteRecoveryState();
      assert.ok(recovered, 'saved request must survive reload without home GPS');
      assert.equal(JSON.stringify(recovered.payload), JSON.stringify(posted[0]));
      if (!fixture.home) assert.equal(posted[0].start.latitude,57.7);
    })()`, context);
  }
  vm.runInContext(`
    renderPlanningRoutePreview({origin_source:'selected_owner_home',gps_notice:'Rutten utgår från Johans hemadress',
      stops:[],summary:{},preview_token:'signed'});
    assert.ok(notices.at(-1).includes('Johans hemadress'));
    assert.ok(!notices.at(-1).includes('din position nu'));
    renderPlanningRoutePreview({origin_source:'current_position',stops:[],summary:{},preview_token:'signed'});
    assert.ok(notices.at(-1).includes('Rutten beräknas från din position nu'));
    currentUser = {user_name:'olle',admin:false};
    const state = {version:1,actor_user_name:'olle',owner_user_name:'johan',route_date:planningSelectedDate,
      created_at_ms:Date.now(),payload:{user_name:'johan',route_date:planningSelectedDate,
        route_mode:'automatic',client_request_id:'forged-no-gps'}};
    stored = JSON.stringify(state);
    assert.equal(readPlanningRouteRecoveryState(),null);
  `, context);
  await vm.runInContext(`(async () => {
    currentUser = {user_name:'olle',admin:true}; planningSelectedUserName='olle';
    switchOwnerOnGps=true; stored=null; posted=[];
    await openPlanningRoutePreview();
    assert.equal(posted.length,0,'changing calendars during GPS must not save mismatched recovery');
    assert.equal(stored,null);
  })()`, context);
};
run().catch(error => { console.error(error); process.exitCode = 1; });
'''
        result = subprocess.run(["node", "-e", script, str(INDEX_PATH)],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @skipUnless(shutil.which("node"), "Node.js required for real frontend helpers")
    def test_legacy_route_normalizer_accounts_for_lunch_and_waiting(self):
        script = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[1], 'utf8');
const source = name => {
  const start = html.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, name);
  return html.slice(start, html.indexOf('\n  }', start) + 4);
};
const constants = ['ROUTE_MAX_TOTAL_MINUTES', 'ROUTE_MAX_STOPS', 'ROUTE_SERVICE_MINUTES_PER_STOP']
  .map(name => html.match(new RegExp(`const ${name} = [^;]+;`))[0]).join('\n');
const context = vm.createContext({assert});
vm.runInContext(`${constants}
const customers = [{row:2,customer:'Store A'},{row:3,customer:'Store B'}];
${['parseRouteCoordinate','getRouteCoordinatePair','customerInsightKey','routeProposalError',
   'normalizeRouteProposalResponse'].map(source).join('\n')}`, context);
vm.runInContext(`
const stop = (row, sequence, drive, cumulativeDrive, cumulativeTotal) => ({
  row, sequence, customer: row === 2 ? 'Store A' : 'Store B', latitude:57.7, longitude:11.9,
  priority_score:80, leg_drive_minutes:drive, cumulative_drive_minutes:cumulativeDrive,
  cumulative_total_minutes:cumulativeTotal,
});
const payload = {
  ok:true, start:{latitude:57.7,longitude:11.9},
  stops:[stop(2,1,40,40,60),stop(3,2,20,60,300)],
  summary:{candidate_count:2,stop_count:2,total_priority_score:160,drive_minutes:90,
    return_drive_minutes:30,service_minutes:40,break_minutes:45,wait_minutes:155,
    return_wait_minutes:0,total_minutes:330},
  meta:{includes_return_to_start:true,max_route_stops:15},
};
assert.equal(normalizeRouteProposalResponse(payload, {}).summary.total_minutes,330);
const earlyReturn = {...payload,stops:[stop(2,1,40,40,60)],
  summary:{...payload.summary,stop_count:1,total_priority_score:80,drive_minutes:70,
    service_minutes:20,total_minutes:285,wait_minutes:150,return_wait_minutes:195}};
assert.equal(normalizeRouteProposalResponse(earlyReturn, {}).summary.break_minutes,45);
const afterLunch = {...earlyReturn,summary:{...earlyReturn.summary,total_minutes:90,
  break_minutes:0,wait_minutes:0,return_wait_minutes:0}};
assert.equal(normalizeRouteProposalResponse(afterLunch, {}).summary.total_minutes,90);
for (const changes of [{total_minutes:550},{break_minutes:-1},{wait_minutes:0},
                       {return_wait_minutes:20}]) {
  assert.throws(() => normalizeRouteProposalResponse({...payload,
    summary:{...payload.summary,...changes}}, {}), error => error.code === 'invalid_route_response');
}
`, context);
'''
        result = subprocess.run(
            ["node", "-e", script, str(INDEX_PATH)], capture_output=True, text=True, timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_contact_log_uses_the_three_supported_channels(self):
        match = re.search(
            r'<select id="f-channel">(.*?)</select>',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        options = re.findall(r"<option(?: [^>]*)?>(.*?)</option>", match.group(1))
        self.assertEqual(options, ["Välj...", "Telefon", "Mejl", "Besök"])

    def test_customer_follow_up_uses_the_stats_contract(self):
        self.assertIn("function formatNextFollowUp(nextFollowUp)", self.html)
        self.assertIn(
            "renderContactList(stats.contacts, stats.timeline, stats.next_follow_up)",
            self.html,
        )
        self.assertIn('nextFollowUp.source !== "planned_activity"', self.html)
        self.assertIn("`${date} · Tid ej satt`", self.html)
        self.assertIn('[date, time, type].filter(Boolean).join(" · ")', self.html)
        self.assertNotIn(
            'd-next-followup").textContent = latestContact.follow_up_date',
            self.html,
        )

    def test_partial_contact_save_keeps_a_retry_payload(self):
        self.assertIn('result?.status === "partial"', self.html)
        self.assertIn("contactRetryPayload = payload", self.html)
        self.assertIn("Försök slutföra sparningen", self.html)

    def test_workflow_mutations_refresh_customer_guidance_and_planning(self):
        refresher = re.search(
            r"async function refreshWorkflowViews\(\{ planning = false, recommendation = false \} = \{\}\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresher)
        self.assertIn("loadInsights()", refresher.group(1))
        self.assertIn("loadPlanningWeek()", refresher.group(1))
        self.assertIn("loadPlanningRecommendation({", refresher.group(1))

        load_week = re.search(
            r"async function loadPlanningWeek\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(load_week)
        self.assertIn("await loadPlanningRecommendation()", load_week.group(1))

        mutation = re.search(
            r"async function mutateVisibleRecommendation\(action\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(mutation)
        self.assertIn("await refreshWorkflowViews()", mutation.group(1))

        status_update = re.search(
            r"async function updatePlanningActivityStatus\(activity, status\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(status_update)
        self.assertIn(
            "await refreshWorkflowViews({ planning: true })",
            status_update.group(1),
        )

        editor_save = re.search(
            r"async function savePlanningEditor\(event\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(editor_save)
        self.assertIn("await refreshWorkflowViews()", editor_save.group(1))
        self.assertIn(
            "await refreshWorkflowViews({ planning: true })",
            editor_save.group(1),
        )

        drag_save = re.search(
            r"async function planningCommitDraggedActivity\(activity, targetMinutes\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(drag_save)
        self.assertIn(
            "await refreshWorkflowViews({ planning: true })",
            drag_save.group(1),
        )

    @skipUnless(shutil.which("node"), "Node.js required for real frontend helpers")
    def test_workflow_refresh_orders_real_requests_and_preserves_serial_guards(self):
        script = r'''
const fs = require('node:fs'), vm = require('node:vm'), assert = require('node:assert/strict');
const html = fs.readFileSync(process.argv[1], 'utf8');
const source = name => {
  const match = new RegExp(`  (?:async )?function ${name}\\(`).exec(html);
  assert.ok(match, name);
  const tail = html.slice(match.index);
  const end = /\n  }\r?\n/.exec(tail);
  assert.ok(end, name);
  return tail.slice(0, end.index + 4);
};
const unhandled = [];
process.on('unhandledRejection', error => unhandled.push(error));
const context = vm.createContext({assert, URLSearchParams, setImmediate});
vm.runInContext(`
const API = '/api', sharedJsonRequests = new Map(), requests = [], toasts = [];
let planningLoadSerial = 0, planningRecommendationRequestSerial = 0, insightsRequestSerial = 0;
let insightsForegroundRequests = 0;
let insightsBackgroundRequests = 0;
let insightsBackgroundRefreshPending = false;
let insightsListRenderPending = false;
let customersLoadSerial = 0, planningLoading = false, planningData = null, planningActiveUsersCache = [];
let planningWeekStart = '2026-10-05', planningSelectedDate = '2026-10-05', planningSelectedUserName = '';
let planningRecommendationPreviewLimit = 10, planningRecommendationLoading = false;
let planningRecommendation = null, planningRecommendationPreview = [], planningRecommendationPendingCount = 0;
let insights = {}, insightsLoaded = false, insightsLoadFailed = false, insightsRevision = 0;
let emailClickNoOrderActive = false, listRenders = 0;
const filterState = {emailProposal:new Set()};
const elements = new Map();
const element = id => {
  if (!elements.has(id)) {
    const classes = new Set();
    let html = '';
    elements.set(id, {
      id, classList:{add:value=>classes.add(value),remove:value=>classes.delete(value),contains:value=>classes.has(value)},
      get innerHTML() {return html;},
      set innerHTML(value) {
        html=value;
        if (id==='customer-list') {
          listRenders++;
          if (simulateListScrollJump) {window.scrollX=0;window.scrollY=0;}
        }
      },
      querySelectorAll:()=>[], addEventListener:()=>{},
    });
  }
  return elements.get(id);
};
const views = ['list','planning','detail','map'].map(name=>element('view-'+name));
element('view-planning').classList.add('active');
const document = {
  getElementById:id=>id==='load-more-btn'?null:element(id),
  querySelectorAll:()=>views,
  querySelector:()=>views.find(view=>view.classList.contains('active')),
};
const googleMap = null;
const customers = [{row:2,customer:'Test store',city:'Test city'}];
let lastListSignature = '', visibleListCount = 10, filteredCustomers = [];
const LIST_BATCH_SIZE = 10, getListSignature = () => 'test-list';
const getBaseFilteredCustomers = () => customers, getRouteProposalStop = () => null;
const isCancelledCustomer = () => false, currentUserCanPlan = () => true;
let simulateListScrollJump = false;
const scrollCalls = [], animationFrames = [];
const window = {scrollX:0,scrollY:0,scrollTo:(x,y)=>{scrollCalls.push([x,y]);window.scrollX=x;window.scrollY=y;}};
let deferAnimationFrames = false;
const requestAnimationFrame = callback => {if (deferAnimationFrames) animationFrames.push(callback);else callback();};
const tick = () => new Promise(resolve => setImmediate(resolve));
function controlledRequest(path) {
  let resolve, reject;
  const promise = new Promise((yes,no) => {resolve=yes;reject=no;});
  requests.push({path,resolve,reject});
  return promise;
}
const fetch = path => controlledRequest(path).then(payload => ({ok:true,json:async()=>payload}));
const planningFetchJson = controlledRequest;
const planningCancelDrag = () => {}, planningTodayKey = () => planningSelectedDate;
const planningStartOfWeek = value => value, planningAddDays = value => value;
const planningOwnerQuery = value => value, userIsAdmin = () => false;
const normalizePlanningUser = value => value, normalizePlanningActivity = value => value;
const renderPlanningWeekStrip = () => {}, renderPlanning = () => {};
const renderPlanningLoadError = () => {}, resumePlanningRoutePreviewRecovery = () => {};
const renderPlanningRecommendation = () => {}, renderPlanningCandidates = () => {};
const planningDragSavingIds = new Set();
let planningDragRetry = null;
const planningActivityId = activity => activity.id, planningActivityMinutes = () => 540;
const planningMinutesLabel = () => '10:00', planningStockholmIso = () => '2026-10-05T10:00:00+02:00';
const planningClientRequestId = () => 'drag-request', renderPlanningAgenda = () => {};
const planningReplaceActivity = (id, activity) => {
  planningData.activities = planningData.activities.map(row => row.id === id ? activity : row);
};
const buildInsightDropdowns = () => {}, updateEmailClickNoOrderChip = () => {}, updateChip = () => {};
const showToast = text => toasts.push(text);
${['fetchJsonShared','loadInsights','loadPlanningRecommendation','loadPlanningWeek','refreshWorkflowViews',
   'planningCommitDraggedActivity','activeViewName','showView','renderList','esc','escAttr',
   'customerInsightKey','getCustomerInsight','getCustomerGuidance','hasPriorityScore',
   'getGuidanceStatusClass','getNextActionColorClass','guidanceContactLabel','getNextActionHtml',
   'getPrioritySummaryHtml','getPotentialHtml','getCardDateHtml','getDeliveryVolumeText',
   'getCardDatesHtml','getPriorityCardClass','getCustomerCardAriaLabel','getCardPlanningButtonHtml']
   .map(source).join('\n')}
`, context);
vm.runInContext(`(async () => {
  const week = version => ({owner:{user_name:'olle'},activities:[{id:version}],available_users:[]});
  const suggestions = {suggestion:{id:'next'},queue_preview:[],pending_count:1};
  const begin = () => requests.length;

  // Week and its existing recommendation must finish before background insights start.
  let first = begin(), done = false;
  const critical = refreshWorkflowViews({planning:true,recommendation:true}).then(() => {done=true;});
  assert.equal(requests.length-first,1);
  assert.ok(requests[first].path.includes('/planning/activities'));
  await tick(); assert.equal(requests.length-first,1); assert.equal(done,false);
  requests[first].resolve(week('server'));
  await tick(); assert.equal(requests.length-first,2);
  assert.ok(requests[first+1].path.includes('/planning/suggestions'));
  assert.equal(done,false); assert.deepEqual(planningData.activities,[{id:'server'}]);
  requests[first+1].resolve(suggestions);
  await critical;
  assert.equal(done,true); assert.equal(requests.length-first,3);
  assert.equal(requests[first+2].path,'/api/customer-insights');
  assert.equal(planningRecommendation.id,'next');
  assert.equal(listRenders,0);
  requests[first+2].resolve({version:'background'});
  await tick(); assert.equal(insights.version,'background'); assert.equal(listRenders,0);

  // Ordinary refresh still waits for insights and renders the customer list.
  first=begin(); done=false;
  const ordinary = refreshWorkflowViews().then(() => {done=true;});
  assert.equal(requests.length-first,1); assert.equal(requests[first].path,'/api/customer-insights');
  await tick(); assert.equal(done,false);
  requests[first].resolve({version:'ordinary'}); await ordinary;
  assert.equal(done,true); assert.equal(listRenders,1);

  // Recommendation-only refresh also finishes without waiting for secondary insights.
  first=begin(); done=false;
  const recommendation = refreshWorkflowViews({recommendation:true}).then(() => {done=true;});
  assert.equal(requests.length-first,1); assert.ok(requests[first].path.includes('/planning/suggestions'));
  requests[first].resolve(suggestions); await recommendation;
  assert.equal(requests.length-first,2); assert.equal(requests[first+1].path,'/api/customer-insights');
  assert.equal(done,true);
  requests[first+1].resolve({version:'recommendation'}); await tick();
  assert.equal(listRenders,1);

  // A failed secondary request preserves valid insights and filters without a misleading save error.
  const validInsights = insights, validRevision = insightsRevision, toastCount = toasts.length;
  emailClickNoOrderActive = true;
  filterState.emailProposal.add('selected');
  first=begin();
  const failedBackground = refreshWorkflowViews({planning:true});
  requests[first].resolve(week('saved')); await tick();
  requests[first+1].resolve(suggestions); await failedBackground;
  requests[first+2].reject(new Error('insights unavailable')); await tick();
  assert.deepEqual(planningData.activities,[{id:'saved'}]); assert.equal(planningLoading,false);
  assert.equal(insights,validInsights); assert.equal(insightsRevision,validRevision);
  assert.equal(insightsLoaded,true); assert.equal(insightsLoadFailed,false);
  assert.equal(emailClickNoOrderActive,true); assert.ok(filterState.emailProposal.has('selected'));
  assert.equal(toasts.length,toastCount); assert.equal(listRenders,1);

  // An older week response must neither replace newer state nor trigger a competing insights request.
  first=begin();
  const older = refreshWorkflowViews({planning:true});
  const newer = refreshWorkflowViews({planning:true});
  assert.equal(requests.length-first,2);
  requests[first].resolve(week('obsolete')); await older;
  assert.equal(requests.length-first,2); assert.deepEqual(planningData.activities,[{id:'saved'}]);
  requests[first+1].resolve(week('newer')); await tick();
  assert.equal(requests.length-first,3); assert.ok(requests[first+2].path.includes('/planning/suggestions'));
  requests[first+2].resolve(suggestions); await newer;
  assert.equal(requests.length-first,4); assert.equal(requests[first+3].path,'/api/customer-insights');
  requests[first+3].resolve({version:'newer'}); await tick();
  assert.deepEqual(planningData.activities,[{id:'newer'}]);

  // Existing insight request serials still discard an older response.
  first=begin();
  let oldResolve, newResolve;
  const oldResponse = new Promise(resolve => {oldResolve=resolve;});
  const newResponse = new Promise(resolve => {newResolve=resolve;});
  const oldInsights = loadInsights({render:true,requestPromise:oldResponse});
  const newInsights = loadInsights({render:true,requestPromise:newResponse});
  newResolve({version:'latest'}); await newInsights;
  oldResolve({version:'obsolete'}); await oldInsights;
  assert.equal(insights.version,'latest'); assert.equal(requests.length,first);

  // Even an unexpected loader rejection is handled without failing a saved planning action.
  const originalLoader = loadInsights;
  loadInsights = async () => {throw new Error('unexpected background failure');};
  first=begin();
  const unexpected = refreshWorkflowViews({planning:true});
  requests[first].resolve(week('still-saved')); await tick();
  requests[first+1].resolve(suggestions); await unexpected; await tick();
  assert.deepEqual(planningData.activities,[{id:'still-saved'}]);
  loadInsights = originalLoader;

  // A planning background refresh must not supersede an inflight foreground insights request.
  first=begin();
  const foreground = loadInsights({render:true});
  const foregroundSerial = insightsRequestSerial, rendersBefore = listRenders;
  assert.equal(insightsForegroundRequests,1);
  const refreshDuringForeground = refreshWorkflowViews({planning:true});
  requests[first+1].resolve(week('foreground-inflight')); await tick();
  requests[first+2].resolve(suggestions); await refreshDuringForeground;
  assert.equal(requests.length-first,3);
  assert.equal(requests[first].path,'/api/customer-insights');
  assert.equal(insightsRequestSerial,foregroundSerial);
  assert.equal(insightsForegroundRequests,1);
  assert.equal(insightsBackgroundRefreshPending,true);
  await loadInsights({render:false}); await loadInsights({render:false});
  assert.equal(requests.length-first,3); assert.equal(insightsRequestSerial,foregroundSerial);
  requests[first].resolve({version:'foreground'});
  assert.equal(await foreground,true);
  assert.equal(insights.version,'foreground'); assert.equal(listRenders,rendersBefore+1);
  assert.equal(insightsForegroundRequests,0); assert.equal(toasts.length,toastCount);
  assert.equal(insightsBackgroundRefreshPending,false);
  assert.equal(requests.length-first,4); assert.equal(requests[first+3].path,'/api/customer-insights');
  requests[first+3].resolve({version:'fresh-background'}); await tick();
  assert.equal(insights.version,'fresh-background'); assert.equal(listRenders,rendersBefore+1);
  assert.equal(requests.length-first,4);

  // A failed week fetch prevents insights, but a handled suggestions failure still allows freshness.
  first=begin();
  const failedWeek = refreshWorkflowViews({planning:true});
  requests[first].reject(new Error('activities unavailable')); await failedWeek;
  assert.equal(requests.length-first,1);
  first=begin();
  const failedWeekRecommendation = refreshWorkflowViews({planning:true});
  requests[first].resolve(week('saved-without-recommendation')); await tick();
  requests[first+1].reject(new Error('suggestions unavailable')); await failedWeekRecommendation;
  assert.equal(requests.length-first,3); assert.equal(requests[first+2].path,'/api/customer-insights');
  requests[first+2].resolve({version:'fresh-background'}); await tick();
  first=begin();
  const failedRecommendation = refreshWorkflowViews({recommendation:true});
  requests[first].reject(new Error('suggestions unavailable')); await failedRecommendation;
  assert.equal(requests.length-first,2); assert.equal(requests[first+1].path,'/api/customer-insights');
  requests[first+1].resolve({version:'fresh-background'}); await tick();
  assert.equal(insights.version,'fresh-background'); assert.equal(toasts.length,toastCount);

  // A week superseded while waiting for suggestions is still obsolete and must not trigger insights.
  first=begin();
  const supersededWeek = refreshWorkflowViews({planning:true});
  requests[first].resolve(week('superseded-after-activities')); await tick();
  const replacingWeek = refreshWorkflowViews({planning:true});
  requests[first+1].resolve(suggestions); await supersededWeek;
  assert.equal(requests.length-first,3);
  requests[first+2].reject(new Error('new activities unavailable')); await replacingWeek;
  assert.equal(requests.length-first,3);

  // Obsolete recommendations and unexpected rejected loaders must not start insights.
  first=begin();
  const obsoleteRecommendation = refreshWorkflowViews({recommendation:true});
  const currentRecommendation = refreshWorkflowViews({recommendation:true});
  requests[first].reject(new Error('obsolete suggestions unavailable')); await obsoleteRecommendation;
  assert.equal(requests.length-first,2);
  requests[first+1].resolve(suggestions); await currentRecommendation;
  assert.equal(requests.length-first,3);
  requests[first+2].resolve({version:'fresh-background'}); await tick();
  const originalRecommendationLoader = loadPlanningRecommendation;
  loadPlanningRecommendation = async () => {throw new Error('unexpected loader rejection');};
  first=begin(); await refreshWorkflowViews({recommendation:true});
  assert.equal(requests.length,first);
  loadPlanningRecommendation = originalRecommendationLoader;

  // A newer foreground response still wins over a background request already in progress.
  let backgroundResolve, foregroundResolve;
  const backgroundResponse = new Promise(resolve => {backgroundResolve=resolve;});
  const foregroundResponse = new Promise(resolve => {foregroundResolve=resolve;});
  const backgroundFirst = loadInsights({render:false,requestPromise:backgroundResponse});
  const foregroundSecond = loadInsights({render:true,requestPromise:foregroundResponse});
  foregroundResolve({version:'foreground-wins'}); assert.equal(await foregroundSecond,true);
  backgroundResolve({version:'stale-background'}); assert.equal(await backgroundFirst,false);
  assert.equal(insights.version,'foreground-wins'); assert.equal(insightsForegroundRequests,0);

  // Foreground failures retain their existing visible error handling and release priority.
  first=begin();
  const failedForeground = loadInsights({render:true});
  requests[first].reject(new Error('insights unavailable')); assert.equal(await failedForeground,false);
  assert.equal(insightsForegroundRequests,0); assert.equal(insightsLoaded,false);
  assert.equal(insightsLoadFailed,true); assert.equal(emailClickNoOrderActive,false);
  assert.equal(filterState.emailProposal.size,0);
  assert.equal(toasts.at(-1),'Kunde inte ladda kundprioritering');

  // Foreground starts before a real planning mutation; its old result is followed by one fresh request.
  first=begin();
  const preMutationForeground = loadInsights({render:true});
  const preMutationSerial = insightsRequestSerial, preMutationRenders = listRenders;
  const activity = {id:'dragged',revision:1,scheduled_at:'2026-10-05T09:00:00+02:00'};
  planningData = week('dragged');
  const mutation = planningCommitDraggedActivity(activity,600);
  assert.equal(requests[first+1].path,'/planning/activities/dragged');
  requests[first+1].resolve({activity:{...activity,scheduled_at:'2026-10-05T10:00:00+02:00'}});
  await tick(); assert.ok(requests[first+2].path.includes('/planning/activities?'));
  requests[first+2].resolve(week('dragged')); await tick();
  requests[first+3].resolve(suggestions); await mutation;
  assert.equal(planningDragSavingIds.size,0);
  await loadInsights({render:false}); await loadInsights({render:false});
  assert.equal(requests.length-first,4); assert.equal(insightsRequestSerial,preMutationSerial);
  assert.equal(insightsForegroundRequests,1); assert.equal(insightsBackgroundRefreshPending,true);
  requests[first].resolve({version:'before-mutation'});
  assert.equal(await preMutationForeground,true);
  assert.equal(insights.version,'before-mutation'); assert.equal(listRenders,preMutationRenders+1);
  assert.equal(requests.length-first,5); assert.equal(requests[first+4].path,'/api/customer-insights');
  assert.equal(insightsForegroundRequests,0); assert.equal(insightsBackgroundRefreshPending,false);
  requests[first+4].resolve({version:'after-mutation'}); await tick();
  assert.equal(insights.version,'after-mutation'); assert.equal(requests.length-first,5);
  assert.equal(listRenders,preMutationRenders+1);

  // Only the last of multiple foreground requests drains the deferred refresh; failure remains silent.
  first=begin();
  let firstForegroundResolve, lastForegroundResolve;
  const firstForeground = loadInsights({render:true,requestPromise:new Promise(resolve => {firstForegroundResolve=resolve;})});
  const lastForeground = loadInsights({render:true,requestPromise:new Promise(resolve => {lastForegroundResolve=resolve;})});
  await loadInsights({render:false}); await loadInsights({render:false});
  assert.equal(insightsForegroundRequests,2); assert.equal(requests.length,first);
  firstForegroundResolve({version:'superseded'}); assert.equal(await firstForeground,false);
  assert.equal(insightsForegroundRequests,1); assert.equal(requests.length,first);
  lastForegroundResolve({version:'valid-foreground'}); assert.equal(await lastForeground,true);
  assert.equal(insightsForegroundRequests,0); assert.equal(requests.length-first,1);
  const retainedInsights = insights, retainedRevision = insightsRevision, retainedToasts = toasts.length;
  emailClickNoOrderActive = true; filterState.emailProposal.add('selected');
  requests[first].reject(new Error('deferred insights unavailable')); await tick();
  assert.equal(insights,retainedInsights); assert.equal(insightsRevision,retainedRevision);
  assert.equal(insightsLoaded,true); assert.equal(insightsLoadFailed,false);
  assert.equal(emailClickNoOrderActive,true); assert.ok(filterState.emailProposal.has('selected'));
  assert.equal(toasts.length,retainedToasts); assert.equal(requests.length-first,1);
  assert.equal(insightsBackgroundRefreshPending,false);

  // Real planning mutations B and C during background A require one new fetch after A, never reuse A.
  const commitMutation = async (id, failSuggestions = false) => {
    const start = begin();
    const row = {id,revision:1,scheduled_at:'2026-10-05T09:00:00+02:00'};
    planningData = week(id);
    const save = planningCommitDraggedActivity(row,600);
    assert.equal(requests[start].path,'/planning/activities/'+id);
    requests[start].resolve({activity:row}); await tick();
    assert.ok(requests[start+1].path.includes('/planning/activities?'));
    requests[start+1].resolve(week(id)); await tick();
    assert.ok(requests[start+2].path.includes('/planning/suggestions'));
    if (failSuggestions) requests[start+2].reject(new Error('suggestions unavailable after save'));
    else requests[start+2].resolve(suggestions);
    await save;
    assert.equal(planningDragSavingIds.size,0); assert.equal(planningLoading,false);
    return start;
  };
  first = await commitMutation('mutation-a');
  assert.equal(requests.length-first,4); assert.equal(requests[first+3].path,'/api/customer-insights');
  assert.equal(insightsBackgroundRequests,1); assert.equal(insightsBackgroundRefreshPending,false);
  const backgroundASerial = insightsRequestSerial, mutationRenders = listRenders;
  const beforeStaleA = insights, beforeStaleARevision = insightsRevision, beforeStaleADirty = insightsListRenderPending;
  await commitMutation('mutation-b');
  assert.equal(requests.length-first,7); assert.equal(insightsBackgroundRefreshPending,true);
  assert.equal(insightsRequestSerial,backgroundASerial); assert.equal(insightsBackgroundRequests,1);
  await commitMutation('mutation-c');
  assert.equal(requests.length-first,10); assert.equal(insightsBackgroundRefreshPending,true);
  assert.equal(insightsRequestSerial,backgroundASerial); assert.equal(insightsBackgroundRequests,1);
  requests[first+3].resolve({version:'before-b-and-c'}); await tick();
  assert.equal(insights,beforeStaleA); assert.equal(insightsRevision,beforeStaleARevision);
  assert.equal(insightsListRenderPending,beforeStaleADirty); assert.equal(requests.length-first,11);
  assert.equal(requests[first+10].path,'/api/customer-insights');
  assert.equal(insightsBackgroundRequests,1); assert.equal(insightsBackgroundRefreshPending,false);
  requests[first+10].resolve({version:'after-b-and-c'}); await tick();
  assert.equal(insights.version,'after-b-and-c'); assert.equal(requests.length-first,11);
  assert.equal(insightsBackgroundRequests,0); assert.equal(listRenders,mutationRenders);

  // Failed background A still drains the pending refresh silently and preserves valid state/filters.
  first = await commitMutation('failed-background-a');
  await commitMutation('mutation-after-a');
  const beforeFailure = insights, beforeFailureRevision = insightsRevision, beforeFailureToasts = toasts.length;
  requests[first+3].reject(new Error('background A unavailable')); await tick();
  assert.equal(requests.length-first,8); assert.equal(requests[first+7].path,'/api/customer-insights');
  assert.equal(insights, beforeFailure); assert.equal(insightsRevision,beforeFailureRevision);
  assert.equal(insightsLoaded,true); assert.equal(insightsLoadFailed,false);
  assert.equal(emailClickNoOrderActive,true); assert.ok(filterState.emailProposal.has('selected'));
  assert.equal(toasts.length,beforeFailureToasts);
  requests[first+7].reject(new Error('deferred background unavailable')); await tick();
  assert.equal(insights,beforeFailure); assert.equal(insightsRevision,beforeFailureRevision);
  assert.equal(insightsLoaded,true); assert.equal(insightsLoadFailed,false);
  assert.equal(emailClickNoOrderActive,true); assert.ok(filterState.emailProposal.has('selected'));
  assert.equal(toasts.length,beforeFailureToasts); assert.equal(requests.length-first,8);
  assert.equal(insightsBackgroundRequests,0); assert.equal(insightsBackgroundRefreshPending,false);

  // A foreground completing first cannot drain pending work while background is still in flight.
  first=begin();
  const mixedBackground = loadInsights({render:false});
  let mixedForegroundResolve;
  const mixedForeground = loadInsights({render:true,requestPromise:new Promise(resolve => {mixedForegroundResolve=resolve;})});
  await loadInsights({render:false});
  mixedForegroundResolve({version:'mixed-foreground'}); await mixedForeground;
  assert.equal(insightsForegroundRequests,0); assert.equal(insightsBackgroundRequests,1);
  assert.equal(insightsBackgroundRefreshPending,true); assert.equal(requests.length-first,1);
  requests[first].resolve({version:'superseded-background'}); assert.equal(await mixedBackground,false);
  assert.equal(insights.version,'mixed-foreground'); assert.equal(requests.length-first,2);
  assert.equal(requests[first+1].path,'/api/customer-insights');
  requests[first+1].resolve({version:'mixed-fresh'}); await tick();
  assert.equal(insights.version,'mixed-fresh'); assert.equal(insightsBackgroundRequests,0);
  assert.equal(insightsBackgroundRefreshPending,false); assert.equal(requests.length-first,2);

  // A successful background refresh dirties a hidden list, then navigation renders new guidance once.
  const guidancePayload = (id, date, status, label) => ({'test store':{customer_guidance:{
    focus_key:'repeat_purchase', focus_label:'Repeat purchase', status_key:status, status_label:label,
    action_label:label, planned_activity_id:id, next_contact_at:date, recommended_contact_type:'visit',
  }}});
  showView('list');
  await loadInsights({requestPromise:Promise.resolve(guidancePayload('old-activity','2026-10-05','planned','Initial guidance'))});
  const list = document.getElementById('customer-list');
  assert.ok(list.innerHTML.includes('Initial guidance'));
  assert.ok(list.innerHTML.includes('data-planned-activity-id="old-activity"'));
  assert.ok(list.innerHTML.includes('data-plan-date="2026-10-05"'));
  const initialRenders = listRenders, initialHtml = list.innerHTML;
  showView('planning');
  first = await commitMutation('hidden-list-mutation');
  requests[first+3].resolve(guidancePayload('new-activity','2026-10-06','planned','New guidance')); await tick();
  assert.equal(activeViewName(),'planning'); assert.equal(listRenders,initialRenders);
  assert.equal(list.innerHTML,initialHtml); assert.equal(insightsListRenderPending,true);
  showView('list');
  assert.equal(listRenders,initialRenders+1); assert.equal(insightsListRenderPending,false);
  assert.ok(list.innerHTML.includes('New guidance'));
  assert.ok(list.innerHTML.includes('data-planned-activity-id="new-activity"'));
  assert.ok(list.innerHTML.includes('data-plan-date="2026-10-06"'));
  assert.ok(!list.innerHTML.includes('old-activity')); assert.ok(!list.innerHTML.includes('2026-10-05'));
  assert.ok(!list.innerHTML.includes('Initial guidance'));
  showView('list'); showView('planning'); showView('list');
  assert.equal(listRenders,initialRenders+1);

  // Returning before the background response arrives renders immediately upon its success.
  showView('planning');
  first = await commitMutation('early-return-mutation');
  const beforeEarlyReturn = listRenders;
  showView('list'); assert.equal(listRenders,beforeEarlyReturn);
  requests[first+3].resolve(guidancePayload('','','act_now','Contact now')); await tick();
  assert.equal(listRenders,beforeEarlyReturn+1); assert.equal(insightsListRenderPending,false);
  assert.ok(list.innerHTML.includes('Contact now')); assert.ok(list.innerHTML.includes('next-action-red'));
  assert.ok(list.innerHTML.includes('data-planned-activity-id=""'));
  assert.ok(list.innerHTML.includes('data-plan-date=""'));
  assert.ok(list.innerHTML.includes('Planera kontakt'));
  assert.ok(!list.innerHTML.includes('new-activity')); assert.ok(!list.innerHTML.includes('2026-10-06'));
  assert.ok(!list.innerHTML.includes('Öppna planering')); assert.ok(!list.innerHTML.includes('status-planned'));
  showView('planning'); showView('list'); assert.equal(listRenders,beforeEarlyReturn+1);

  // Failed background refreshes neither dirty a clean list nor consume a previous successful update.
  showView('planning');
  first=begin(); const failedHidden = loadInsights({render:false});
  const beforeHiddenFailure = insights, beforeHiddenFailureRenders = listRenders, beforeHiddenFailureToasts = toasts.length;
  requests[first].reject(new Error('hidden insights unavailable')); await failedHidden;
  assert.equal(insights,beforeHiddenFailure); assert.equal(insightsListRenderPending,false);
  assert.equal(listRenders,beforeHiddenFailureRenders); assert.equal(toasts.length,beforeHiddenFailureToasts);
  showView('list'); assert.equal(listRenders,beforeHiddenFailureRenders);
  showView('planning');
  await loadInsights({render:false,requestPromise:Promise.resolve(guidancePayload('final-activity','2026-10-07','planned','Final guidance'))});
  assert.equal(insightsListRenderPending,true);
  first=begin(); const failedAfterSuccess = loadInsights({render:false});
  requests[first].reject(new Error('later insights unavailable')); await failedAfterSuccess;
  assert.equal(insightsListRenderPending,true); assert.equal(listRenders,beforeHiddenFailureRenders);
  assert.equal(toasts.length,beforeHiddenFailureToasts);
  showView('list'); assert.equal(listRenders,beforeHiddenFailureRenders+1);
  assert.ok(list.innerHTML.includes('Final guidance')); assert.equal(insightsListRenderPending,false);

  // A saved planning mutation still refreshes insights after its suggestions GET fails.
  showView('planning');
  const beforePartialSaveToasts = toasts.length, beforePartialSaveRenders = listRenders;
  first = await commitMutation('partial-recommendation-failure',true);
  assert.deepEqual(planningData.activities,[{id:'partial-recommendation-failure'}]);
  assert.equal(planningRecommendation,null); assert.equal(planningRecommendationLoading,false);
  assert.equal(toasts.length,beforePartialSaveToasts+1);
  assert.equal(toasts.at(-1),'Aktiviteten flyttades till 10:00');
  assert.equal(requests.length-first,4); assert.equal(requests[first+3].path,'/api/customer-insights');
  requests[first+3].resolve(guidancePayload('partial-saved','2026-10-08','planned','Saved despite suggestions error'));
  await tick(); assert.equal(listRenders,beforePartialSaveRenders); assert.equal(insightsListRenderPending,true);
  showView('list'); assert.ok(list.innerHTML.includes('Saved despite suggestions error'));
  assert.equal(toasts.length,beforePartialSaveToasts+1);

  // Known stale background A publishes nothing after mutation B, then the fresh result preserves scroll.
  first=begin();
  const knownStaleBackground = loadInsights({render:false});
  await commitMutation('known-stale-mutation-b');
  assert.equal(requests.length-first,4); assert.equal(insightsBackgroundRefreshPending,true);
  const beforeKnownStale = insights, beforeKnownStaleRevision = insightsRevision;
  const beforeKnownStaleHtml = list.innerHTML, beforeKnownStaleRenders = listRenders;
  requests[first].resolve(guidancePayload('stale-activity','2026-10-01','act_now','Known stale guidance'));
  assert.equal(await knownStaleBackground,false);
  assert.equal(insights,beforeKnownStale); assert.equal(insightsRevision,beforeKnownStaleRevision);
  assert.equal(list.innerHTML,beforeKnownStaleHtml); assert.equal(listRenders,beforeKnownStaleRenders);
  assert.equal(insightsListRenderPending,false); assert.equal(insightsBackgroundRefreshPending,false);
  assert.equal(requests.length-first,5); assert.equal(requests[first+4].path,'/api/customer-insights');
  window.scrollX=13; window.scrollY=487; scrollCalls.length=0;
  simulateListScrollJump=true; deferAnimationFrames=true;
  requests[first+4].resolve(guidancePayload('fresh-activity','2026-10-09','planned','Fresh after mutation B'));
  await tick();
  assert.equal(listRenders,beforeKnownStaleRenders+1); assert.ok(list.innerHTML.includes('Fresh after mutation B'));
  assert.ok(!list.innerHTML.includes('Known stale guidance')); assert.ok(!list.innerHTML.includes('stale-activity'));
  assert.equal(window.scrollX,13); assert.equal(window.scrollY,487);
  assert.deepEqual(scrollCalls,[[13,487]]); assert.equal(animationFrames.length,1);
  window.scrollX=0; window.scrollY=0; animationFrames.shift()();
  assert.equal(window.scrollX,13); assert.equal(window.scrollY,487);
  assert.deepEqual(scrollCalls,[[13,487],[13,487]]);
  assert.equal(insightsListRenderPending,false); assert.equal(insightsBackgroundRequests,0);
  assert.equal(requests.length-first,5);
  simulateListScrollJump=false; deferAnimationFrames=false;
})()`, context).then(() => assert.deepEqual(unhandled,[]))
  .catch(error => {console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(["node", "-e", script, str(INDEX_PATH)],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_planning_patch_flows_send_optimistic_version(self):
        self.assertIn(
            "payload.expected_updated_at = planningEditorActivity.updated_at",
            self.html,
        )
        self.assertIn("expected_updated_at: activity.updated_at", self.html)
        self.assertIn(
            "payload.expected_revision = Number(planningEditorActivity.revision || 1)",
            self.html,
        )
        self.assertIn("expected_revision: Number(activity.revision || 1)", self.html)

    def test_ambiguous_contact_activity_reuses_the_original_request(self):
        self.assertIn('result?.error === "ambiguous_planned_activity"', self.html)
        self.assertIn("function openAmbiguousContactActivityDialog(payload, candidates)", self.html)
        self.assertIn("...payload,", self.html)
        self.assertIn("planned_activity_id: activity.planned_activity_id", self.html)
        self.assertIn("contactRetryPayload = {", self.html)

    def test_customer_selector_is_accessible_search_combobox_using_customer_id(self):
        self.assertIn('role="combobox"', self.html)
        self.assertIn('aria-controls="planning-editor-customer-list"', self.html)
        self.assertIn('role="listbox"', self.html)
        self.assertIn('"ArrowDown" || event.key === "ArrowUp"', self.html)
        self.assertIn('event.key === "Enter"', self.html)
        self.assertIn('.normalize("NFD")', self.html)
        self.assertIn("customer.address_google", self.html)
        self.assertIn("customer.customer_number", self.html)
        self.assertIn(
            "if (customer.customer_id) payload.customer_id = customer.customer_id",
            self.html,
        )

    def test_planning_customer_binding_never_uses_row_only(self):
        binding = re.search(
            r"function planningCustomerForItem\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(binding)
        body = binding.group(1)
        self.assertIn("customer_id", body)
        self.assertIn("customer_number", body)
        self.assertIn("byVerifiedSnapshot", body)
        self.assertNotIn("byRow", body)
        self.assertNotIn("customer_row", body)
        self.assertIn(
            "Kunden kunde inte bindas säkert. Ladda om eller kontakta administratör.",
            self.html,
        )

    def test_planning_preview_uses_backend_queue_without_raw_score_backlog(self):
        self.assertIn("Fler nästa åtgärder", self.html)
        self.assertNotIn("Dagens fokus", self.html)
        self.assertNotIn("Gamla uppföljningar att planera in", self.html)
        self.assertNotIn("Kommande uppföljningar", self.html)
        renderer = re.search(
            r"function renderPlanningCandidates\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(renderer)
        body = renderer.group(1)
        self.assertIn("const visible = planningRecommendationPreview", body)
        self.assertIn("planning-backlog-load-more", body)
        self.assertIn("Ladda fler", body)
        self.assertIn(
            "const hasMore = visible.length < Math.max(",
            body,
        )
        self.assertIn(
            "0, planningRecommendationPendingCount - 1",
            body,
        )
        self.assertIn("${hasMore ?", body)
        self.assertNotIn("planningCandidateCustomers()", body)
        self.assertNotIn("priority_score", body)
        self.assertNotIn("expected_order_dfp", body)
        self.assertNotIn("Orderpotential", body)
        self.assertNotIn("Visa fler", body)
        self.assertIn("planning-backlog-overdue", body)
        self.assertIn("planning-backlog-overdue-actions", body)
        self.assertIn("@container planning-backlog (max-width: 420px)", self.html)

    def test_planning_preview_load_more_is_snapshot_based_and_resets_by_owner(self):
        self.assertIn("let planningRecommendationPreviewLimit = 10", self.html)
        self.assertIn("planningRecommendationPreviewLimit + 5", self.html)
        self.assertIn('params.set("preview_limit", String(previewLimit))', self.html)
        self.assertIn(
            "planningRecommendationPreview = Array.isArray(payload.queue_preview)",
            self.html,
        )
        owner_change = re.search(
            r'planning-owner-select"\)\.addEventListener\("change".*?\n  \}\);',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_change)
        self.assertIn("planningRecommendationPreviewLimit = 10", owner_change.group(0))

    def test_phase1_renders_one_nonblocking_recommendation_card(self):
        self.assertIn('id="planning-recommendation"', self.html)
        self.assertIn('<div class="planning-recommendation-heading">NÄSTA ÅTGÄRD</div>', self.html)
        self.assertNotIn("planningRecommendationPendingCount} kvar", self.html)
        for label in ("Ring nu", "Planera", "Snooza 7 dagar", "Dölj detta förslag"):
            self.assertIn(f">{label}</button>", self.html)
        render = re.search(
            r"function renderPlanningRecommendation\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(render)
        self.assertNotIn(".map(", render.group(1))
        self.assertNotIn("Orderpotential", render.group(1))
        self.assertIn("planningRecommendation.recommended_contact_type", render.group(1))
        self.assertIn("!planningRecommendation.can_call", render.group(1))
        button_positions = [
            render.group(1).index(f">{label}</button>")
            for label in ("Ring nu", "Planera", "Snooza 7 dagar", "Dölj detta förslag")
        ]
        self.assertEqual(button_positions, sorted(button_positions))
        self.assertIn("Kalendern och övrig planering fungerar fortfarande", self.html)
        self.assertIn("loadPlanningRecommendation();", self.html)

    def test_phase1_actions_wait_for_success_and_lock_suggestion_customer(self):
        self.assertIn("Kunden är låst för den här rekommendationen", self.html)
        self.assertIn('contact_type: suggestion.recommended_contact_type || "visit"', self.html)
        self.assertIn("expected_suggestion_revision", self.html)
        self.assertIn("suggestionSeed.expected_suggestion_revision ?? 0", self.html)
        self.assertIn("source_suggestion_id", self.html)
        self.assertIn("replaceWithNextRecommendation(payload)", self.html)
        self.assertIn("replaceWithNextRecommendation(result)", self.html)
        self.assertIn("if (!suggestionLocked) setupPlanningCustomerCombobox", self.html)
        open_planner = re.search(
            r"function openRecommendationPlanner\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(open_planner)
        self.assertNotIn("planningFetchJson", open_planner.group(1))
        self.assertIn("loadPlanningRecommendation().finally", self.html)
        self.assertRegex(
            self.html,
            r"(?s)@media \(max-width: 620px\).*?planning-recommendation-actions.*?repeat\(2",
        )

    def test_recommendation_customer_binding_never_falls_back_to_customer_row(self):
        binding = re.search(
            r"function planningRecommendationCustomer\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(binding)
        body = binding.group(1)
        self.assertIn("suggestion.customer_id", body)
        self.assertIn("if (!customerId) return null", body)
        self.assertNotIn("customer_row", body)
        self.assertNotIn("customer.row", body)

    def test_planning_calendar_has_two_compact_time_lanes(self):
        self.assertIn("Telefon/Email", self.html)
        self.assertIn('aria-label="Besök"', self.html)
        self.assertIn("function planningCalendarLayout(activities, startMinutes)", self.html)
        self.assertIn('class="planning-calendar-hour"', self.html)
        self.assertNotIn('class="planning-calendar-event-time"', self.html)
        self.assertIn("const PLANNING_CALENDAR_PX_PER_MINUTE = 1.5", self.html)
        self.assertIn("contact-phone", self.html)
        self.assertIn("contact-email", self.html)
        card = re.search(
            r"function renderPlanningCalendarActivity\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(card)
        self.assertIn("planning-activity-customer", card.group(1))
        self.assertNotIn("planning-activity-type", card.group(1))
        self.assertNotIn("planning-activity-note", card.group(1))
        self.assertNotIn("planning-activity-time", card.group(1))

    def test_planning_appointment_editor_normalizes_resets_and_sends_boolean(self):
        normalizer = re.search(
            r"function normalizePlanningActivity\(activity\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(normalizer)
        self.assertIn("planningTruthy(source.appointment_confirmed)", normalizer.group(1))
        self.assertIn('["visit", "phone"].includes(contactType) && planningTruthy(source.appointment_confirmed)', normalizer.group(1))
        self.assertIn('id="planning-editor-appointment"', self.html)
        self.assertIn("planningEditorActivity?.appointment_confirmed", self.html)
        sync = re.search(
            r"function syncPlanningEditorAppointment\(contactType\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(sync)
        self.assertIn('["visit", "phone"].includes(contactType)', sync.group(1))
        self.assertIn('pickingHelpField.hidden = contactType !== "visit"', sync.group(1))
        self.assertIn("checkbox.checked = false", sync.group(1))
        self.assertIn("syncPlanningEditorAppointment(event.target.value)", self.html)
        self.assertIn(
            'appointment_confirmed: ["visit", "phone"].includes(contactType) && document.getElementById("planning-editor-appointment").checked',
            self.html,
        )

    def test_superseded_activity_keeps_status_without_actions(self):
        normalizer = re.search(
            r"function normalizePlanningActivity\(activity\) \{(.*?)\n  \}",
            self.html, flags=re.DOTALL,
        )
        self.assertIsNotNone(normalizer)
        self.assertIn(
            '["planned", "completed", "skipped", "cancelled", "superseded"].includes(source.status)',
            normalizer.group(1),
        )
        self.assertIn('superseded: "Ersatt av kontakt"', self.html)
        self.assertIn(
            '["completed", "cancelled", "superseded"].includes(activity.status)',
            self.html,
        )
        self.assertIn(
            '!["cancelled", "superseded"].includes(activity.status)',
            self.html,
        )
        self.assertIn(
            '!["cancelled", "skipped", "superseded"].includes(activity.status)',
            self.html,
        )

    def test_followup_calendar_accessibility_and_detail_show_appointments(self):
        self.assertIn('id="f-followup-appointment"', self.html)
        self.assertIn(
            'document.getElementById("f-followup-type").addEventListener("change", updateFollowupAppointmentField)',
            self.html,
        )
        followup_sync = re.search(
            r"function updateFollowupAppointmentField\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(followup_sync)
        self.assertIn('["visit", "phone"].includes(contactType)', followup_sync.group(1))
        self.assertIn('pickingHelpField.hidden = contactType !== "visit"', followup_sync.group(1))
        self.assertIn("checkbox.checked = false", followup_sync.group(1))
        self.assertIn(
            'appointment_confirmed: ["visit", "phone"].includes(followupType) && document.getElementById("f-followup-appointment").checked',
            self.html,
        )
        card = re.search(
            r"function renderPlanningCalendarActivity\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(card)
        self.assertIn('" appointment-confirmed"', card.group(1))
        self.assertIn('["visit", "phone"].includes(activity.contact_type) && activity.appointment_confirmed', card.group(1))
        self.assertIn('${["visit", "phone"].includes(activity.contact_type) && activity.appointment_confirmed ?', self.html)
        self.assertIn('"Tidsbokat med butiken"', card.group(1))
        self.assertIn(".planning-activity-card.appointment-confirmed", self.html)
        self.assertIn(
            '<div class="planning-appointment-badge">Tidsbokat med butiken</div>',
            self.html,
        )

    def test_picking_help_controls_calendar_and_detail_contract(self):
        self.assertIn('id="planning-editor-picking-help"', self.html)
        self.assertIn('id="f-followup-picking-help"', self.html)
        self.assertIn("planningEditorActivity?.picking_help", self.html)
        self.assertIn("planningTruthy(source.picking_help)", self.html)
        self.assertIn(
            'picking_help: contactType === "visit" && document.getElementById("planning-editor-picking-help").checked',
            self.html,
        )
        self.assertIn(
            'picking_help: followupType === "visit" && document.getElementById("f-followup-picking-help").checked',
            self.html,
        )
        card = re.search(
            r"function renderPlanningCalendarActivity\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(card)
        self.assertIn('" picking-help"', card.group(1))
        self.assertIn('pickingHelp ? "Hjälp med plock"', card.group(1))
        self.assertIn(
            ".planning-activity-card.status-planned.picking-help",
            self.html,
        )
        self.assertIn("#7a61b8", self.html)
        self.assertIn("#f7f3ff", self.html)
        self.assertIn(
            '<div class="planning-picking-help-badge">Hjälp med plock</div>',
            self.html,
        )

    def test_planning_week_omits_legacy_followup_payload(self):
        self.assertIn('include_followups: "0"', self.html)

    def test_drag_drop_uses_pointer_events_handle_and_half_hour_snapping(self):
        self.assertIn('addEventListener("pointerdown", planningDragPointerDown)', self.html)
        self.assertIn('document.addEventListener("pointermove", planningDragPointerMove', self.html)
        self.assertIn('handle ? "handle" : "longpress"', self.html)
        self.assertIn("}, 340)", self.html)
        self.assertNotIn('event.pointerType !== "mouse" && !handle', self.html)
        self.assertRegex(
            self.html,
            r"(?s)@media \(pointer: coarse\).*?\.planning-drag-handle\s*\{.*?width:\s*44px;.*?height:\s*44px;",
        )
        self.assertIn("Math.round(rawMinutes / 30) * 30", self.html)
        self.assertIn("planning-drop-indicator", self.html)
        self.assertIn("planning-drag-ghost", self.html)
        self.assertIn("planningStartDragAutoScroll", self.html)

    def test_mobile_whole_card_long_press_preserves_scroll_and_prevents_selection(self):
        self.assertIn("-webkit-touch-callout: none", self.html)
        self.assertIn("-webkit-user-select: none", self.html)
        self.assertIn("window.getSelection()?.removeAllRanges()", self.html)
        self.assertIn('state.activationMode = "scroll"', self.html)
        self.assertIn("window.scrollBy(0, state.lastClientY - event.clientY)", self.html)
        self.assertIn("state.active || state.scrolled", self.html)
        self.assertIn('button.addEventListener("contextmenu"', self.html)

    def test_drag_drop_allows_only_owned_planned_or_skipped_activities(self):
        can_drag = re.search(
            r"function planningCanDragActivity\(activity\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(can_drag)
        self.assertIn('["planned", "skipped"]', can_drag.group(1))
        self.assertIn("activity.unplanned", can_drag.group(1))
        self.assertIn("activityOwner === loadedOwner", can_drag.group(1))

    def test_drag_patch_is_minimal_idempotent_and_conflict_safe(self):
        commit = re.search(
            r"async function planningCommitDraggedActivity\(activity, targetMinutes\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(commit)
        body = commit.group(1)
        for field in (
            "scheduled_at",
            "client_request_id",
            "expected_revision",
            "expected_updated_at",
        ):
            self.assertIn(field, body)
        self.assertNotIn("customer_id", body)
        self.assertNotIn("contact_type:", body)
        self.assertIn("retry?.requestBody || JSON.stringify(payload)", body)
        self.assertIn('method: "PATCH"', body)
        self.assertIn('["revision_conflict", "planning_changed"]', body)
        self.assertIn("await loadPlanningWeek()", body)
        self.assertIn('activity.source === "route" ? "manual"', body)

    def test_drag_cleanup_covers_escape_reload_and_view_switch(self):
        self.assertIn('event.key === "Escape"', self.html)
        self.assertIn('if (name !== "planning") planningCancelDrag()', self.html)
        load_week = re.search(
            r"async function loadPlanningWeek\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(load_week)
        self.assertIn("planningCancelDrag()", load_week.group(1))
        self.assertIn('document.removeEventListener("pointermove", planningDragPointerMove)', self.html)
        pointer_move = re.search(
            r"function planningDragPointerMove\(event\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(pointer_move)
        self.assertNotIn("planningFetchJson", pointer_move.group(1))

    def test_planning_header_map_uses_ordered_day_visits(self):
        self.assertNotIn('id="planning-new-btn"', self.html)
        self.assertIn('id="planning-day-map-btn"', self.html)
        self.assertIn("function planningVisitStopsForDate(dateKey)", self.html)
        self.assertIn('activity.contact_type === "visit"', self.html)
        self.assertIn(
            '!["cancelled", "skipped", "superseded"].includes(activity.status)',
            self.html,
        )
        self.assertIn("routeInMapStops = [...visitStops]", self.html)
        self.assertIn('mapReturnView = "planning"', self.html)
        self.assertIn("showPlanningDayMap", self.html)

    def test_list_view_hides_legacy_route_proposal_flow(self):
        list_view = re.search(
            r'<div class="view active" id="view-list">(.*?)<!-- .*?FOLLOW-UP INSIGHTS VIEW',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(list_view)
        self.assertNotIn('id="chip-route-proposal"', list_view.group(1))
        self.assertNotIn('id="route-proposal-panel"', list_view.group(1))
        self.assertIn('id="route-mode-btn"', list_view.group(1))
        self.assertIn(
            'id="planning-route-preview-btn" type="button">Skapa ruttförslag</button>',
            self.html,
        )
        self.assertIn("LEGACY ROLLBACK SUPPORT", self.html)

    def test_planning_error_preserves_admin_owner_and_backend_message(self):
        self.assertIn(
            "error?.details || error?.payload || {}",
            self.html,
        )
        owner_select = re.search(
            r"function renderPlanningOwnerSelect\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_select)
        self.assertIn("planningActiveUsersCache", owner_select.group(1))
        load_week = re.search(
            r"async function loadPlanningWeek\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(load_week)
        error_handler = load_week.group(1).split("} catch (error) {", 1)[1]
        self.assertNotIn("planningData = null", error_handler)

    def test_route_edit_explains_manual_conversion(self):
        self.assertIn(
            "När du sparar blir aktiviteten manuellt planerad och behålls vid nästa automatiska ruttberäkning.",
            self.html,
        )

    def test_route_preview_persists_and_replays_the_exact_pending_request(self):
        self.assertIn(
            '"store-tracker:route-preview-recovery:v1"',
            self.html,
        )
        self.assertIn("sessionStorage.setItem(", self.html)
        self.assertIn("sessionStorage.removeItem(", self.html)
        self.assertIn("PLANNING_ROUTE_RECOVERY_TTL_MS = 30 * 60 * 1000", self.html)
        self.assertIn("PLANNING_ROUTE_RECOVERY_WINDOW_MS = 20 * 60 * 1000", self.html)
        self.assertIn("PLANNING_ROUTE_RECOVERY_POLL_MS = 15 * 1000", self.html)
        read = self.html.split("function readPlanningRouteRecoveryState", 1)[1].split(
            "function planningRouteRecoveryForCurrentContext", 1
        )[0]
        self.assertIn("if (!valid)", read)
        self.assertIn("const actorUserName = String(currentUser?.user_name || \"\").trim()", read)
        self.assertIn("if (!actorUserName) return null", read)
        self.assertIn("if (state.actor_user_name !== actorUserName)", read)
        self.assertEqual(read.count("clearPlanningRouteRecoveryState()"), 3)
        save = self.html.split("function savePlanningRouteRecoveryState", 1)[1].split(
            "function planningRouteError", 1
        )[0]
        self.assertIn("try {", save)
        self.assertIn("sessionStorage.setItem(", save)
        self.assertIn("state.storage_persisted = false", save)
        self.assertIn("return state", save)

        create = self.html.split("async function openPlanningRoutePreview()", 1)[1].split(
            "function renderPlanningRoutePreview", 1
        )[0]
        self.assertLess(create.index("getCurrentPositionForRoute()"), create.index("savePlanningRouteRecoveryState(payload)"))
        self.assertLess(create.index("savePlanningRouteRecoveryState(payload)"), create.index("postPendingPlanningRoutePreview(state"))

        recovery = self.html.split("function resumePlanningRoutePreviewRecovery", 1)[1].split(
            "async function openPlanningRoutePreview", 1
        )[0]
        self.assertIn("planningRoutePreviewStatus(state.payload.client_request_id)", recovery)
        self.assertIn('status.state === "completed"', recovery)
        self.assertIn('status.state === "fallback_ready"', recovery)
        self.assertIn("completedReplay: true", recovery)
        self.assertNotIn("getCurrentPositionForRoute", recovery)
        self.assertNotIn("planningClientRequestId", recovery)
        self.assertIn('window.addEventListener("online"', self.html)
        self.assertIn('document.addEventListener("visibilitychange"', self.html)

    def test_route_preview_recovery_clears_after_render_and_never_applies(self):
        recovery = self.html.split("function planningRouteCurrentOwnerUserName", 1)[1].split(
            "function renderPlanningRoutePreview", 1
        )[0]
        rendered = recovery.split("function renderRecoveredPlanningRoutePreview", 1)[1].split(
            "async function postPendingPlanningRoutePreview", 1
        )[0]
        self.assertLess(rendered.index("renderPlanningRoutePreview(payload)"), rendered.index("planningRouteApplyRequestId"))
        self.assertLess(rendered.index("planningRouteApplyRequestId"), rendered.index("clearPlanningRouteRecoveryState()"))
        self.assertIn('kind: "ambiguous_transport_or_body_failure"', recovery)
        self.assertIn('outcome.kind === "in_progress"', recovery)
        self.assertIn('outcome.kind === "fallback_ready"', recovery)
        self.assertIn('outcome.kind === "terminal_backend_error"', recovery)
        self.assertIn("planningRoutePreviewFetch(state.payload)", recovery)
        self.assertNotIn("/planning/route-apply", recovery)

    def test_route_fallback_ready_is_nonterminal_and_resumes_same_payload(self):
        fetcher = self.html.split(
            "async function planningRoutePreviewFetch", 1
        )[1].split("async function planningRoutePreviewStatus", 1)[0]
        self.assertIn('result?.state === "fallback_ready"', fetcher)
        self.assertIn('kind: "fallback_ready"', fetcher)

        recovery = self.html.split(
            "async function postPendingPlanningRoutePreview", 1
        )[1].split("async function openPlanningRoutePreview", 1)[0]
        fallback_branch = recovery.split(
            'outcome.kind === "fallback_ready"', 1
        )[1].split('outcome.kind === "in_progress"', 1)[0]
        self.assertIn("schedulePlanningRouteRecovery(state, deadlineMs)", fallback_branch)
        self.assertNotIn("clearPlanningRouteRecoveryState", fallback_branch)
        self.assertNotIn("planningClientRequestId", fallback_branch)
        self.assertNotIn("getCurrentPositionForRoute", fallback_branch)

        status_branch = recovery.split(
            'status.state === "fallback_ready"', 1
        )[1].split('status.state === "completed"', 1)[0]
        self.assertIn("postPendingPlanningRoutePreview(state", status_branch)
        self.assertNotIn("clearPlanningRouteRecoveryState", status_branch)
        self.assertNotIn("planningClientRequestId", status_branch)
        self.assertNotIn("getCurrentPositionForRoute", status_branch)

    def test_route_traffic_infeasible_has_shared_post_and_recovery_message(self):
        expected = (
            "Trafiken gör att rutten inte ryms inom dagens fasta tider och "
            "arbetsdagens slut 17:00. Justera planeringen och försök igen."
        )
        mapper = self.html.split(
            "function getRouteProposalFailureMessage", 1
        )[1].split("async function proposeRoute", 1)[0]
        self.assertIn('normalizedCode === "route_traffic_infeasible"', mapper)
        self.assertIn(expected, mapper)

        recovery = self.html.split(
            "async function postPendingPlanningRoutePreview", 1
        )[1].split("async function openPlanningRoutePreview", 1)[0]
        self.assertIn("getRouteProposalFailureMessage(outcome.error)", recovery)
        self.assertIn("getRouteProposalFailureMessage(error)", recovery)
        self.assertNotIn("getCurrentPositionForRoute", recovery)
        self.assertNotIn("/planning/route-apply", recovery)

        self.assertIn(
            'normalizedCode === "route_fallback_exhausted"', mapper
        )
        self.assertIn(
            "Ruttoptimeringen hittade ingen verifierat genomförbar rutt inom de automatiska försöken.",
            mapper,
        )

    def test_route_preview_context_switch_resets_ui_and_does_not_share_single_flight(self):
        recovery = self.html.split("function planningRouteRecoveryForCurrentContext", 1)[1].split(
            "async function openPlanningRoutePreview", 1
        )[0]
        resume = recovery.split("function resumePlanningRoutePreviewRecovery", 1)[1]
        self.assertIn("!planningRouteRecoveryStateMatchesCurrentContext(storedState)", resume)
        self.assertIn("window.clearTimeout(planningRouteRecoveryTimer)", resume)
        self.assertIn("planningRouteResetPreviewButton()", resume)
        self.assertIn("planningRouteRecoveryStateIsActive(state)", recovery)
        self.assertIn("planningRouteRecoveryPromiseKey === requestKey", recovery)
        self.assertIn("planningRouteRecoveryPromise === promise", recovery)
        self.assertIn(
            "runPlanningRouteRecoverySingleFlight(state.payload.client_request_id",
            recovery,
        )

    def test_legacy_followup_keeps_its_source_link(self):
        self.assertIn('payload.source = "follow_up"', self.html)
        self.assertIn(
            "payload.source_contact_id = planningEditorSeed.source_contact_id",
            self.html,
        )

    def test_unplanned_contacts_render_in_the_historical_agenda(self):
        self.assertIn("function planningUnplannedForDate", self.html)
        self.assertIn("function planningAgendaItemsForDate", self.html)
        self.assertIn("planningAgendaItemsForDate(planningSelectedDate)", self.html)

    def test_contact_types_are_three_touch_sized_radio_chips(self):
        self.assertIn('name="planning-editor-type"', self.html)
        self.assertIn("grid-template-columns: repeat(3, minmax(0, 1fr))", self.html)
        self.assertRegex(
            self.html,
            r"\.planning-type-choice span\s*\{[^}]*min-height:\s*52px",
        )

    def test_admin_owner_comes_from_active_seller_response(self):
        self.assertIn(
            "planningSelectedUserName = responseOwner.user_name",
            self.html,
        )
        owner_select = re.search(
            r"function renderPlanningOwnerSelect\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_select)
        self.assertNotIn("available.unshift(current)", owner_select.group(1))
        self.assertIn('id="f-followup-owner"', self.html)
        self.assertIn("followupEnabled && userIsAdmin()", self.html)

    def test_planning_entry_points_are_hidden_without_planning_role(self):
        self.assertIn("function currentUserCanPlan()", self.html)
        self.assertIn(
            'currentUserCanPlan() ? "" : "none"',
            self.html,
        )

    def test_frontend_accepts_every_backend_seller_role(self):
        seller_check = re.search(
            r"function currentUserIsSeller\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(seller_check)
        for role in ("säljare", "saljare", "account manager", "accountmanager"):
            self.assertIn(f'"{role}"', seller_check.group(1))

    def test_adjusted_map_route_cannot_silently_import_original_stops(self):
        self.assertIn("function routeMapSelectionDiffersFromProposal", self.html)
        self.assertIn("Kartans stopp har ändrats", self.html)
