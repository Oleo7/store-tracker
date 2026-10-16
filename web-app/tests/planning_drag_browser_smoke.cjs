// Run against the isolated planning or sales-coaching browser harness only.
// BROWSER_HARNESS_PORT=5086 python web-app/tests/planning_browser_harness.py
// NODE_PATH=<Playwright node_modules> node web-app/tests/planning_drag_browser_smoke.cjs
const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const base = process.env.PLANNING_DRAG_HARNESS_URL || "http://127.0.0.1:5086";
assert.ok(["127.0.0.1", "localhost"].includes(new URL(base).hostname), "Use an isolated local harness");
const baseline = process.env.PLANNING_DRAG_BASELINE === "1";
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

function idOf(activity) { return activity.planned_activity_id || activity.activity_id || activity.id; }

async function openFixture(browser, mode) {
  const context = await browser.newContext({
    viewport: mode === "mobile" ? { width: 390, height: 844 } : { width: 1440, height: 900 },
    isMobile: mode === "mobile", hasTouch: mode === "mobile",
  });
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(String(error)));
  const login = await page.request.post(`${base}/login`, { data: { user_name: "olle", password: "secret" } });
  assert.equal(login.status(), 200, await login.text());
  await page.goto(base, { waitUntil: "networkidle" });
  // Pick the first unused future day so repeated runs remain deterministic.
  let date;
  for (let offset = 3; offset < 183; offset++) {
    date = await page.evaluate(offset => planningAddDays(planningTodayKey(), offset), offset);
    const response = await page.request.get(`${base}/planning/activities?start=${date}&end=${date}&include_followups=0`);
    assert.equal(response.status(), 200, await response.text());
    const payload = await response.json();
    if (!(payload.data?.activities || payload.activities || []).length) break;
    assert.ok(offset < 182, "Restart the in-memory harness to clear old fixtures");
  }
  const fixture = {};
  for (const [name, time, type, row] of [
    ["early", "06:30", "visit", 2], ["phone", "10:00", "phone", 2],
    ["visit", "12:00", "visit", 4], ["email", "14:00", "email", 4],
  ]) {
    const scheduled = await page.evaluate(({ date, time }) => planningStockholmIso(date, time), { date, time });
    const response = await page.request.post(`${base}/planning/activities`, { data: {
      client_request_id: `drag-browser-${mode}-${name}-${Date.now()}`,
      customer_row: row, contact_type: type, scheduled_at: scheduled,
      customer_id: row === 2 ? "11111111-1111-4111-8111-111111111111" : "33333333-3333-4333-8333-333333333333",
      note: `Isolated drag regression ${name}`,
    } });
    assert.equal(response.status(), 201, await response.text());
    fixture[name] = (await response.json()).activity;
  }
  await page.locator("#planning-mode-btn").click();
  await page.waitForFunction(() => Boolean(planningData) && !planningLoading);
  await page.locator("#planning-date-jump").fill(date);
  await page.locator("#planning-date-jump").dispatchEvent("change");
  await page.locator(`[data-planning-shell-id="${idOf(fixture.early)}"]`).waitFor();
  await page.waitForFunction(() => !planningLoading);
  const cdp = mode === "mobile" ? await context.newCDPSession(page) : null;
  const env = { page, context, cdp, fixture, date, errors, mode, requests: [], policies: new Map() };
  await page.route("**/planning/activities/*", async route => {
    if (route.request().method() !== "PATCH") return route.continue();
    const id = decodeURIComponent(new URL(route.request().url()).pathname.split("/").pop());
    env.requests.push({ id, body: route.request().postData() });
    const policy = env.policies.get(id);
    if (policy) return policy(route);
    return route.continue();
  });
  return env;
}

async function watchCalendar(page) {
  await page.evaluate(() => {
    window.dragBrowserWatch?.observer?.disconnect();
    const calendar = document.querySelector(".planning-calendar");
    const rect = calendar.getBoundingClientRect();
    const watch = window.dragBrowserWatch = {
      calendar, start: calendar.dataset.calendarStartMinutes,
      end: calendar.dataset.calendarEndMinutes, height: rect.height,
      top: rect.top + scrollY, scroll: scrollY, loaders: 0, removed: 0,
      documentHeight: document.documentElement.scrollHeight, minHeight: document.documentElement.scrollHeight,
    };
    watch.observer = new MutationObserver(records => {
      for (const record of records) for (const node of record.addedNodes) {
        if (node.textContent?.includes("Laddar planering")) watch.loaders += 1;
      }
      if (!calendar.isConnected) watch.removed += 1;
      watch.minHeight = Math.min(watch.minHeight, document.documentElement.scrollHeight);
    });
    watch.observer.observe(document.querySelector("#planning-agenda"), { childList: true, subtree: true });
  });
}

async function readWatch(page) {
  return page.evaluate(() => {
    const watch = window.dragBrowserWatch;
    const calendar = document.querySelector(".planning-calendar");
    const rect = calendar?.getBoundingClientRect();
    return {
      sameNode: calendar === watch.calendar, loaders: watch.loaders, removed: watch.removed,
      start: calendar?.dataset.calendarStartMinutes, initialStart: watch.start,
      end: calendar?.dataset.calendarEndMinutes, initialEnd: watch.end,
      height: rect?.height, initialHeight: watch.height,
      top: rect ? rect.top + scrollY : null, initialTop: watch.top,
      scroll: scrollY, initialScroll: watch.scroll, minHeight: watch.minHeight,
      initialDocumentHeight: watch.documentHeight,
    };
  });
}

function assertStable(result, label, { scroll = true } = {}) {
  assert.equal(result.sameNode, true, `${label}: calendar node retained`);
  assert.equal(result.loaders, 0, `${label}: no loading placeholder`);
  assert.equal(result.removed, 0, `${label}: calendar always connected`);
  assert.equal(result.start, result.initialStart, `${label}: start bound retained`);
  assert.equal(result.end, result.initialEnd, `${label}: end bound retained`);
  assert.equal(result.height, result.initialHeight, `${label}: height retained`);
  assert.ok(Math.abs(result.top - result.initialTop) <= 1, `${label}: calendar document position retained`);
  assert.ok(result.minHeight >= result.initialDocumentHeight - 1, `${label}: document never collapses`);
  if (scroll) assert.ok(Math.abs(result.scroll - result.initialScroll) <= 1, `${label}: scroll ${result.initialScroll} -> ${result.scroll}`);
}

async function drag(env, activity, targetMinutes, { longPress = false, edge = false, watch = true } = {}) {
  const { page, cdp } = env;
  const id = idOf(activity);
  const card = page.locator(`[data-planning-open-id="${id}"]`);
  await card.scrollIntoViewIfNeeded();
  // Put the source and target away from sticky controls and auto-scroll edges.
  const initial = await page.evaluate(({ id, targetMinutes }) => {
    const card = document.querySelector(`[data-planning-open-id="${id}"]`);
    const calendar = card.closest(".planning-calendar");
    const cardTop = card.getBoundingClientRect().top + scrollY;
    const targetTop = calendar.getBoundingClientRect().top + scrollY +
      (targetMinutes - Number(calendar.dataset.calendarStartMinutes)) * Number(calendar.dataset.calendarPxPerMinute);
    const top = Math.min(cardTop, targetTop);
    window.scrollTo(0, Math.max(0, top - 230));
    return { delta: targetTop - cardTop };
  }, { id, targetMinutes });
  if (watch) await watchCalendar(page);
  const box = await card.boundingBox();
  const x = box.x + Math.min(24, box.width / 4);
  const y = box.y + Math.min(4, box.height / 3);
  const targetY = edge ? 830 : y + initial.delta;
  if (cdp) {
    await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x, y, id: 1 }] });
    if (longPress) {
      await page.waitForFunction(() => Boolean(planningDragState?.active));
      assert.equal(await page.evaluate(() => planningDragState.activationMode), "longpress");
    }
    for (let step = 1; step <= 8; step++) {
      await cdp.send("Input.dispatchTouchEvent", { type: "touchMove", touchPoints: [{ x, y: y + (targetY - y) * step / 8, id: 1 }] });
    }
    if (edge) await pause(250);
    await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    env.lastDropScroll = await page.evaluate(() => scrollY);
  } else {
    await page.mouse.move(x, y);
    await page.mouse.down();
    await page.mouse.move(x, targetY, { steps: 8 });
    if (edge) await pause(250);
    await page.mouse.up();
    env.lastDropScroll = await page.evaluate(() => scrollY);
  }
  return id;
}

async function waitSaved(env, id, targetTime) {
  await env.page.waitForFunction(({ id, targetTime }) => {
    const activity = getPlanningActivity(id);
    return !planningDragSavingIds.has(id) && planningTimeFromIso(activity?.scheduled_at) === targetTime;
  }, { id, targetTime });
  return env.page.evaluate(id => ({ ...getPlanningActivity(id) }), id);
}

async function waitSettled(env, id) {
  await env.page.waitForFunction(id => !planningDragSavingIds.has(id), id);
  return env.page.evaluate(id => ({ ...getPlanningActivity(id) }), id);
}

function holdResponse(env, activity) {
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  env.policies.set(idOf(activity), async route => {
    const response = await route.fetch();
    await gate;
    await route.fulfill({ response });
  });
  return release;
}

async function ordinaryTouchScroll(env) {
  const { page, cdp } = env;
  const card = page.locator(`[data-planning-open-id="${idOf(env.fixture.phone)}"]`);
  await card.scrollIntoViewIfNeeded();
  const box = await card.boundingBox();
  const x = box.x + 12, y = box.y + 4;
  const before = await page.evaluate(() => scrollY);
  const requests = env.requests.length;
  await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x, y, id: 1 }] });
  for (let step = 1; step <= 12; step++) {
    await cdp.send("Input.dispatchTouchEvent", { type: "touchMove", touchPoints: [{ x, y: y - step * 15, id: 1 }] });
  }
  await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
  const after = await page.evaluate(() => ({ scroll: scrollY, active: Boolean(planningDragState), ghost: Boolean(document.querySelector(".planning-drag-ghost")) }));
  assert.ok(after.scroll > before + 50, "mobile: ordinary short touch gesture scrolls");
  assert.equal(after.active, false, "mobile: ordinary scroll ends gesture");
  assert.equal(after.ghost, false, "mobile: ordinary scroll never starts drag");
  assert.equal(env.requests.length, requests, "mobile: ordinary scroll does not save");
}

async function testSuccessfulMoves(env) {
  const { page, fixture, mode } = env;
  env.policies.set(idOf(fixture.early), async route => { await pause(450); await route.continue(); });
  const id = await drag(env, fixture.early, 8 * 60 + 30, { longPress: mode === "mobile" });
  const during = await readWatch(page);
  const saved = await waitSaved(env, id, "08:30");
  await page.waitForLoadState("networkidle");
  const after = await readWatch(page);
  if (baseline) {
    console.log(JSON.stringify({ mode, during, after, revision: saved.revision }));
    return;
  }
  assertStable(during, `${mode}: optimistic early activity`);
  assertStable(after, `${mode}: confirmed early activity`);
  assert.ok(Number(saved.revision) > Number(fixture.early.revision), `${mode}: server revision`);
  const release = holdResponse(env, fixture.phone);
  try {
    const phoneId = await drag(env, fixture.phone, 11 * 60, { longPress: mode === "mobile" });
    assert.equal(await page.evaluate(id => planningDragSavingIds.has(id), phoneId), true);
    await drag(env, fixture.phone, 11 * 60 + 30, { longPress: mode === "mobile" });
    assert.equal(await page.evaluate(id => planningTimeFromIso(getPlanningActivity(id).scheduled_at), phoneId), "11:30", `${mode}: newer same-card drop is immediately visible`);
    // Move a different activity while the first save is still awaiting its response.
    const visitId = await drag(env, fixture.visit, 13 * 60, { longPress: mode === "mobile" });
    await waitSaved(env, visitId, "13:00");
    assertStable(await readWatch(page), `${mode}: second move during first save`);
    release();
    await waitSaved(env, phoneId, "11:30");
    assertStable(await readWatch(page), `${mode}: older save response`);
    assert.equal(await page.evaluate(id => planningTimeFromIso(getPlanningActivity(id).scheduled_at), visitId), "13:00");
    const writes = env.requests.filter(request => request.id === phoneId).map(request => JSON.parse(request.body));
    assert.deepEqual(writes.map(write => write.expected_revision), [1, 2], `${mode}: queued same-card move uses server revisions`);
    assert.notEqual(writes[0].client_request_id, writes[1].client_request_id, `${mode}: different move gets new idempotency key`);
  } finally { release(); env.policies.clear(); }
  console.log(`${mode}: real pointer drag, delayed save, repeated/overlapping moves, early06:30 bounds, stable calendar/scroll and server revision PASS`);
}

async function testFailuresAndRetry(env) {
  const { page, fixture, mode } = env;
  const activity = await page.evaluate(id => ({ ...getPlanningActivity(id) }), idOf(fixture.visit));
  env.policies.set(idOf(activity), route => route.fulfill({ status: 422, contentType: "application/json", body: JSON.stringify({ ok: false, error: "validation_error", message: "Injected local validation error" }) }));
  const id = await drag(env, activity, 14 * 60, { longPress: mode === "mobile" });
  const rolledBack = await waitSettled(env, id);
  assert.equal(rolledBack.scheduled_at, activity.scheduled_at, `${mode}: failed move rolls back`);
  assert.match(await page.locator("#toast").innerText(), /kunde inte sparas|Injected local validation error/i);
  assertStable(await readWatch(page), `${mode}: failed move rollback`);
  env.policies.clear();
  const uncertain = fixture.email;
  env.policies.set(idOf(uncertain), async route => {
    const response = await route.fetch();
    assert.equal(response.status(), 200, await response.text());
    await route.abort("failed");
  });
  await drag(env, uncertain, 15 * 60, { longPress: mode === "mobile" });
  const unconfirmed = await waitSettled(env, idOf(uncertain));
  assert.equal(unconfirmed.scheduled_at, uncertain.scheduled_at, `${mode}: uncertain result restores previous visual data`);
  const firstRequest = env.requests.filter(request => request.id === idOf(uncertain)).at(-1);
  env.policies.clear();
  await drag(env, unconfirmed, 15 * 60, { longPress: mode === "mobile" });
  const retried = await waitSaved(env, idOf(uncertain), "15:00");
  const retryRequest = env.requests.filter(request => request.id === idOf(uncertain)).at(-1);
  assert.equal(retryRequest.body, firstRequest.body, `${mode}: uncertain retry preserves idempotency body`);
  assert.equal(Number(retried.revision), Number(uncertain.revision) + 1, `${mode}: uncertain retry does not update twice`);
  assertStable(await readWatch(page), `${mode}: uncertain retry`);
  console.log(`${mode}: failed-save rollback/message and backend-saved lost response with idempotent retry PASS`);
}

async function testConflict(env) {
  const { page, fixture, mode } = env;
  const id = idOf(fixture.early);
  const stale = await page.evaluate(id => ({ ...getPlanningActivity(id) }), id);
  const scheduled = await page.evaluate(date => planningStockholmIso(date, "09:00"), env.date);
  const response = await page.request.patch(`${base}/planning/activities/${id}`, { data: {
    client_request_id: `drag-browser-conflict-${Date.now()}`, expected_revision: stale.revision,
    scheduled_at: scheduled, note: "Concurrent browser harness editor",
  } });
  assert.equal(response.status(), 200, await response.text());
  const authoritative = (await response.json()).activity;
  await drag(env, stale, 9 * 60 + 30, { longPress: mode === "mobile" });
  const settled = await waitSettled(env, id);
  assert.equal(settled.scheduled_at, authoritative.scheduled_at, `${mode}: conflict converges to authoritative time`);
  assert.equal(settled.revision, authoritative.revision, `${mode}: conflict converges to authoritative revision`);
  assertStable(await readWatch(page), `${mode}: conflict sync`);
  console.log(`${mode}: real backend revision conflict converges without layout or scroll loss PASS`);
}

async function testTouchEdgeScroll(env) {
  await ordinaryTouchScroll(env);
  env.policies.set(idOf(env.fixture.phone), async route => { await pause(450); await route.continue(); });
  const id = await drag(env, env.fixture.phone, 12 * 60, { longPress: true, edge: true });
  const moved = await waitSettled(env, id);
  const result = await readWatch(env.page);
  assert.ok(result.scroll > result.initialScroll + 40, "mobile: active edge drag auto-scrolls");
  assertStable(result, "mobile: edge-scroll drop", { scroll: false });
  assert.ok(Math.abs(result.scroll - env.lastDropScroll) <= 1, `mobile: saving retains edge-scroll position ${env.lastDropScroll} -> ${result.scroll}`);
  assert.ok(Number(moved.revision) > 1, "mobile: edge-scroll drop saved");
  env.policies.clear();
  console.log("mobile: actual short touch scrolling and long-press edge auto-scroll retained after save PASS");
}

async function testDelayedNavigation(env) {
  const { page, fixture, date, mode } = env;
  const release = holdResponse(env, fixture.visit);
  try {
    const activity = await page.evaluate(id => ({ ...getPlanningActivity(id) }), idOf(fixture.visit));
    const id = await drag(env, activity, 14 * 60 + 30, { longPress: mode === "mobile" });
    const otherDate = await page.evaluate(date => planningAddDays(date, 1), date);
    await page.locator("#planning-date-jump").fill(otherDate);
    await page.locator("#planning-date-jump").dispatchEvent("change");
    await page.waitForFunction(() => !planningLoading);
    const before = await page.locator("#planning-agenda").innerHTML();
    release();
    await page.waitForFunction(id => !planningDragSavingIds.has(id), id);
    assert.equal(await page.evaluate(() => planningSelectedDate), otherDate, `${mode}: delayed response keeps selected date`);
    assert.equal(await page.locator("#planning-agenda").innerHTML(), before, `${mode}: delayed response leaves new date view alone`);
    console.log(`${mode}: delayed response after date navigation leaves current view unchanged PASS`);
  } finally { release(); env.policies.clear(); }
}

(async () => {
  let browser;
  if (process.env.CHROMIUM_EXECUTABLE) {
    browser = await chromium.launch({ headless: true, executablePath: process.env.CHROMIUM_EXECUTABLE });
  } else {
    try { browser = await chromium.launch({ headless: true }); }
    catch { browser = await chromium.launch({ headless: true, channel: "msedge" }); }
  }
  try {
    for (const mode of ["desktop", "mobile"]) {
      const env = await openFixture(browser, mode);
      try {
        await testSuccessfulMoves(env);
        if (baseline) continue;
        await testFailuresAndRetry(env);
        await testConflict(env);
        if (mode === "mobile") await testTouchEdgeScroll(env);
        await testDelayedNavigation(env);
        assert.deepEqual(env.errors, [], `${mode}: JavaScript errors`);
      } finally { await env.context.close(); }
    }
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
