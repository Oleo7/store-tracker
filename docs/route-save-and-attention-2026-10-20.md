# Route save coordination and attention follow-ups — 2026-10-20

Base: `origin/master` at `0a23f54` (PR48 and PR49). This work does not merge,
change production configuration, deploy, migrate or write to CRM_DATABASE.
Commercial V3.5 coefficients, routing objective and channel rules are unchanged.

## Writer inventory

| Process | Reads | Writes | Coordination |
| --- | --- | --- | --- |
| Store Tracker | Customer master, users, contacts, orders, calendar, state events, email and suggestion inputs | CRM contacts, calendar, source-scoped events, proposals, email state | Existing planning lock now nests a shared renewable Redis lease; low-level schema/append/update helpers also serialize. Actual HTTP mutations check ownership before and after. |
| `stockfiller_orders/sync.py` | Stockfiller and CRM orders/master/history | `order_rows`, customer email, sync state; delegates customer matching | One lease encloses the read/merge/write operation, including nested customer sync. |
| `customer_master_sync/sync.py` | Orders, master and linked history | Safe master-name/ID/email changes, linked contact/calendar snapshots, review/state | Same reentrant lease; direct state writes also guarded. |
| `data-enrichment/enrich_stores.py` | Customer master, Google geocoder | Geography cells plus `enriched`/`modified` only | Geocoding outside lease; fresh stable-ID lookup inside lease; changed/ambiguous geography skipped. Never rewrites the master sheet. |
| `storepoint_sync/sync.py` | CRM `order_rows` | Separate Storepoint spreadsheet/template | No CRM writes, so it does not need the CRM writer lease. It can read while other jobs run; export freshness is independent. |
| Recovery/backfill scripts, MCP/third-party writers | Varies | Potentially any CRM sheet | Not certified by this change. Inventory/disable or adapt any scheduled writer before activation. Explicit maintenance scripts require a maintenance window. |

The repository has no scheduled Stockfiller/enrichment workflow in `.github`; the
only checked-in Render service is the web app. Deployment inventory must therefore
also inspect external cron jobs/services; code inventory alone is insufficient.

Read-only Render inventory, 20 October 2026, in user-confirmed workspace
**CRM Polarbär** (`tea-d7c9rs3bc2fs73eqm9lg`): all four services use this repository,
branch `master`, region **Oregon**, Starter plan. No Key Value instances exist in
this workspace. No service or environment setting was changed.

| Active service | ID | Root directory | Build / command | Current live commit |
| --- | --- | --- | --- | --- |
| Store Tracker web | `srv-d7d0rppkh4rs739glhj0` | `web-app` | `pip install -r requirements.txt` / `gunicorn --workers 1 --threads 4 --timeout 240 --bind 0.0.0.0:$PORT app:app` | `0a23f54` (PR49), manual deployment |
| Stockfiller cron | `crn-d8c2qkurnols739gdr20` | repository root | `pip install -r web-app/requirements.txt` / `python scripts/sync_stockfiller_orders.py` | `6b97c4d` (PR47) |
| Geotagging cron | `crn-d7cfhqho3t8c73dp9heg` | repository root | `pip install -r data-enrichment/requirements.txt` / `python data-enrichment/enrich_stores.py` | `6b97c4d` (PR47) |
| Storepoint cron | `crn-d8lclet8nd3s73bkpu20` | repository root | `pip install -r web-app/requirements.txt` / `python scripts/sync_storepoint_customers.py` | `6b97c4d` (PR47) |

These verified roots already include the required modules; no root-directory
change is needed for these services. The Stockfiller and geotagging jobs must be
explicitly deployed to the coordinated revision before route activation. The
Storepoint job writes a different spreadsheet and does not require the CRM lease.
No standalone customer-master service appeared; it is called by Stockfiller.
External desktop/MCP/third-party automation is outside Render's service inventory
and still needs operational confirmation before declaring every writer coordinated.

## Lease and recovery

`crm_write_coordination.py` uses Redis SET NX PX with a random owner token, Lua
owner-checked renewal and release, a 180-second lease, renewal every 30 seconds,
30-second acquisition timeout, bounded connection/read timeouts, and a local
monotonic deadline with a 60-second write margin. Redis failures/lost ownership
abort, never fall back to local locking. Exceptions and cancellation release only
the caller's token. Crash recovery uses expiry. No mutation HTTP retry is hidden
inside the guard. Stable IDs and fresh read-back reconcile an unknown response.

Every participating process derives the same key from the CRM spreadsheet ID.
`CRM_LOCK_SHEET_KEY` is an explicit common identity if job APP_ENV differs from
web APP_ENV. A staging copy must use a different ID. All use the same URL.

Route save verifies fresh actor/owner, identity, selected-stop V3.5 contact rules,
required visits, same-day phone/email and current calendar. Current signed previews
bind a hash of relevant contact/permission/order/customer/email/suggestion inputs.
The calendar has an additional positional hash to detect sort/insert conflicts.
Only the route customers are rescored; full-portfolio scoring is not repeated on
current-preview saves. Preview generation still uses the unchanged route model.

New visits first have internal status `route_pending`; this is never exposed as
`planned` or authorized for contact. Existing bookings are changed only at
activation. Sources are re-read before staging and activation, and verified after
activation. Same request-ID retries reuse deterministic row IDs and finish an
interrupted activation/receipt without appending duplicates. Pending rows remain
as evidence if a conflict makes recovery unsafe. Do not bulk delete them: inspect
the route group/request and either retry the same unchanged request or explicitly
cancel the inactive rows during approved maintenance.

## Explicit limits of Google Sheets and Redis

This is cooperative coordination, not exclusive ownership of Sheets. User edit
rights, direct Sheets editing and essential jobs remain available.

Fresh pre/post checks detect relevant edits observed around a save, including new
bans, bookings and row movement. A detected post-write conflict returns an error
and makes newly created optional visits inactive. Existing customer promises are
retained. Contact execution must still use the existing fresh authority checks.

Sheets has no compare-and-swap or fencing-token support. A manual edit can land
between the last check and the HTTP write, or after verification; an already
in-flight request cannot be recalled after a timeout, process suspension or Redis
restart/failover. A crash after activation but before verification can leave rows
for the idempotent retry to inspect. Sparse customer edits avoid full-sheet
clobbering; they cannot make manual row sorting atomic. Stockfiller's order-table
rewrite also cannot protect concurrent direct manual edits to those order cells.
Do not claim these risks are eliminated by a Python lock.

For strict serializability under arbitrary manual edits/Redis restarts, this
architecture is insufficient. The smallest operational supplement is to avoid
sorting/inserting rows or editing the affected booking/contact data during the
short save, and drain automated writers before Redis restart/replacement. If
those operational constraints are unacceptable, a single authority supporting
conditional writes is needed; adding a Sheets cell or increasing TTL cannot
supply that guarantee. No such architecture or paid resource is created here.

## Activation after approval

1. Inventory running services, external cron jobs and manual automation. All
   automated writers of these CRM inputs must run the coordinated revision.
2. Reuse a compatible existing Key Value instance only after verifying same
   workspace/region, isolation and `noeviction`. Do not reuse an evicting cache
   without an approved policy change. Otherwise obtain approval for a dedicated
   instance. No resource is provisioned by this PR.
3. Install web/job dependencies including `redis==6.4.0`. For geocoder, retain its
   existing requirements plus Redis. Repository-level jobs must deploy the root
   coordination/geography modules along with their packages. Do not deploy only a
   copied standalone sync.py. The canonical lock module is
   `web-app/crm_write_coordination.py`; the repository-root shim aliases that same
   module for jobs, so globals/leases are not duplicated. The web Render rootDir
   remains `web-app` and works without repository-parent files. For Stockfiller,
   customer-master and enrichment jobs, deploy from the repository root, install
   their requirements, and invoke the package/script from there. The actual
   Render build roots/commands are verified in the inventory above and already
   meet this requirement. Preserve them during the approved rollout.
4. On staging with a **copy** of CRM data set the same `CRM_WRITE_LOCK_URL` on all
   writers (internal URL when in the same workspace/region), and set
   `CRM_LOCK_SHEET_KEY` to the copy ID. Production web uses
   `PRODUCTION_SHEET_KEY`; root jobs use their actual `SHEET_KEY`. Verify these are
   the same intended book. Set `CRM_AUTOMATED_WRITERS_COORDINATED=true` only after
   all writer revisions/configurations are verified.
5. Leave `CRM_EXCLUSIVE_WRITER_CONFIRMED=false`. It no longer bypasses the route
   safety gate. `CRM_RISKY_OPERATIONS_STOPPED=false` permits normal operations.
   Legacy `PLANNING_DISTRIBUTED_LOCK_URL` remains a URL alias; use one consistent
   variable rather than conflicting values.
6. Check `/health`: `planning_write_lock.mode=redis_lease`, configured identity,
   automated writers confirmed, `manual_edits_exclusive=false`. Health describes
   configuration, not live Redis ownership; an actual isolated save verifies
   connectivity and acquisition. Keep current worker/instance sizing until load
   tests justify changes.
7. Run isolated writer contention, manual-conflict, lease-loss and partial-write
   recovery checks. Then, after explicit deployment approval, configure production
   with its CRM identity and URL and repeat a controlled seller smoke test.
8. Emergency stop: `CRM_RISKY_OPERATIONS_STOPPED=true`. For rollback, drain all
   writers first. Do not roll back only one writer while route saves remain on.

Local QA uses `requirements-test.txt` (Redis emulator with actual Lua scripts).
Production adds only redis-py, not pandas/testing libraries to the web service.
Detailed acceptance evidence and final GO/NO-GO are appended after both changes.

## Attention grouping

Grouping is a read projection in `customer_obligations.attention_items`, before
search, sorting, total and pagination. The stable key is customer ID plus owner.
The latest scheduled instant wins across ordinary tasks, including terminal,
snoozed and future representatives. Thus handling the newest task cannot reveal
an older generic task. No older plan is written, deleted, completed or cancelled
by this projection. Source-scoped decisions remain attached to their own IDs.

Confirmed promises, explicit follow-ups, uncertain service/complaint evidence and
ambiguous identities do not join the generic group. An unverified `route_pending`
row is not a scheduling decision and cannot hide existing missed work. It also
cannot be activated through generic PATCH or completed as a contact; only a
successful validated route save can activate it. A seller can cancel it.

The UI displays the latest activity and a discreet older-history count. Search is
performed on the grouped backend results before pagination. Owner/request serial
checks still prevent old filter/page responses from replacing a newer view.
A bounded cache of pure date conversions avoids parsing the same instants again
on every page; it contains no customer data and all date inputs form its key.

## Read-only CRM evidence

Snapshot capture: **20 October 2026, 14:25:23 Europe/Stockholm**, using a Google
credential scoped to `spreadsheets.readonly`. This is a captured set of sheet
reads, not an atomic Google multi-sheet transaction. Source SHA-256:
`d49cedc8b5367128f68a1884bd7d7911bcb82c222f8a67fd0fa1d390d6e60449`.
Raw customer data and credentials remain outside Git. Only aggregate evidence is
committed in `route-save-attention-qa-2026-10-20.json`.

| Evidence | Before | After |
| --- | ---: | ---: |
| Attention entries, all owners | 64 | 57 |
| Sofia | 46 | 39 |
| Johan | 16 | 16 |
| Olle | 1 | 1 |
| Daniel | 1 | 1 |
| Original derived obligations at capture instant | 1,762 | 1,762 |
| Source calendar activities | 1,034 | 1,034 |
| Source contacts | 2,027 | 2,027 |
| Source order rows | 3,351 | 3,351 |
| Customer master rows | 2,110 | 2,110 |

The pure attention projection preserves the complete input state. Offline route
writes use separate in-memory copies of this snapshot; they verify all original
activity IDs remain and that customer master, contacts, orders and state-event
sheets are unchanged. Every tested successful route is retried with the same
request identity and produces no duplicate activity IDs. Four real seller
profiles were exercised. A pre-existing required visit on the next day correctly
blocked generation because of an unanswered-contact pause; normal-save scenarios
use a future weekday with no pre-existing appointments, without deleting them.

V3.5 customer/score tuples are identical between master and the implementation at
the capture instant. Their SHA-256 is
`3a2e899f20e23623378785d56b634912b42eca5fd02627f166bd17dfd450b4a8`.
No coefficient, channel preference, routing objective or V2 scoring fallback was
changed. The priority/routing engine source files are unchanged.

## Performance evidence and limits

Local Windows/Python 3.14.4, identical private snapshot, real Flask/model code,
offline Sheets boundary and deterministic travel times. Timings measure local
CPU/memory and API overhead; **they are not production Google/Redis latency**.
Route benchmark: three independent copies/saves per seller, 12 saves per revision,
with the same 7–8 selected stores and route day. Business time is fixed at
20 October 2026 10:00 for reproducible comparison.

| Seller | Save median before → after | CPU median before → after | Customers rescored before → after |
| --- | --- | --- | --- |
| Olle | 7.74 → 2.44 s | 7.42 → 2.41 s | 2,110 → 8 |
| Johan | 8.18 → 2.56 s | 7.70 → 2.44 s | 2,110 → 8 |
| Daniel | 8.26 → 2.99 s | 7.69 → 2.80 s | 2,110 → 7 |
| Sofia | 7.51 → 2.52 s | 7.31 → 2.44 s | 2,110 → 7 |

Maximum observed process RSS across these route samples: **194.7 MiB before,
187.2 MiB after**. This is a measured sample maximum, not a bound on production
memory or concurrent route-preview memory.

Attention performance uses alternating old/new projections in the **same warmed
Flask process and immutable source snapshot**, ten pairs at each concurrency.
Only the projection changes; its TTL clock is held for this offline comparison.
This controls Windows scheduling/load differences seen in independent runs.

| API/profile | Median before → after | CPU/request before → after |
| --- | --- | --- |
| Attention/admin, 1 request | 126.6 → 131.6 ms | 125.0 → 132.8 ms |
| Attention/admin, 4 concurrent | 379.4 → 387.3 ms | 130.9 → 132.8 ms |
| Attention/seller, 1 request | 126.7 → 122.2 ms | 132.8 → 125.0 ms |
| Attention/seller, 4 concurrent | 356.5 → 362.0 ms | 119.1 → 127.0 ms |

No warm request rebuilt the portfolio priority model. Observed process RSS during
paired runs was 133.1–138.0 MiB. The new grouping adds a small bounded CPU cost;
these samples do not show a material memory increase. Broader independent GET
benchmarks were also run, but their overlapping host load makes them unsuitable
for a causal performance claim. Retest latency, quotas and peak RSS on staging
with the actual Render instance and real Sheets transport before production GO.

Fresh source checks on the real gspread adapter batch the dependency ranges:
one metadata read plus one values batch per verification round. A batch failure
or incomplete response aborts; there is no stale-data fallback. Bulk reads do not
claim atomicity against manual edits. Sparse calendar writes and receipt writes
add network calls compared with the previous single unverified write.

## Test evidence

- Master baseline: **1,124 web tests and 106 root tests passed**. Windows test runs
  use `PYTHONUTF8=1`; the first attempt without UTF-8 exposed an existing Windows
  source-file encoding test issue, and the unchanged baseline passed with UTF-8.
- Final local suites: **1,161 web tests and 123 root tests passed**; Python compile
  and `git diff --check` passed.
- New route coverage includes normal save, two concurrent saves, Stockfiller
  contention, geography/master edits, changed bans/ownership/bookings, stale
  previews, both engines, required stops, partial activation, lost responses,
  receipt recovery, lost leases and inactive-row execution prevention. Existing
  source-scoped promise, order and contact-policy regressions remain green.
- A subprocess with **only the web-app root directory**, with repository files
  and PYTHONPATH unavailable, verifies that the Render build can import the app
  and its canonical shared lock. Root job imports alias the same module state.
- Independent Python processes coordinate over the real Redis client protocol to
  a **local Redis emulator**, including Lua owner checks. Renewal, deadlines,
  transport failures, cancellation and successor-safe unlock are covered.
- Attention tests cover the three-date Coop example, owner and stable-ID
  separation, mixed channels/statuses, time ordering, rescheduling, completion,
  cancellation, promises/services, snooze, backend search and pagination.
- Actual Edge/Playwright: sales coaching desktop/mobile, guidance/delivery at
  320/390/1440 px, priority/commitment/seller flows at 390/1280 px, and desktop/touch
  calendar drag/error/retry tests passed. New route/attention smoke at
  **390 and 1440 px** passed: grouped history count, backend search, separate
  service, route preview, failed activation, retry with the same request ID,
  duplicate retry, new ban after preview and no resurrection of older tasks.
- CI now installs test-only Redis/Lua dependencies and runs the new isolated
  browser harness in addition to the existing checks.

These tests use isolated local copies. A live Google test-sheet/Render/Valkey
integration run and a real process-restart/network-partition exercise remain
release checks; they have not been represented as completed production tests.

Reproduction:

```text
python -m pip install -r requirements-test.txt
PYTHONUTF8=1 python -m unittest discover -s web-app/tests -p 'test_*.py'
PYTHONUTF8=1 python -m unittest discover -s tests -p 'test_*.py'
python scripts/validate_route_attention_snapshot.py --snapshot PRIVATE.json --baseline-root BASELINE --output PRIVATE-qa.json
python scripts/benchmark_read_views.py --snapshot PRIVATE.json --attention-baseline BASELINE --rounds 10 --output PRIVATE-perf.json
python web-app/tests/route_attention_browser_harness.py
node web-app/tests/route_attention_browser_smoke.cjs
```

On PowerShell set `$env:PYTHONUTF8='1'` before running Python. Use the installed
Chromium or set `CHROMIUM_EXECUTABLE` for browser tests. Never point a harness at a
production service; it binds only to loopback and always uses synthetic CRM.

## Render resources and release decision

No Render resource, setting, permission, migration or deployment was changed.
The read-only inventory in the user-confirmed workspace **CRM Polarbär** found
**no Key Value instance to reuse**. The four existing services and live versions
are recorded above. Provisioning a dedicated **256 MB Key Value instance in
Oregon, same workspace, with `noeviction`** requires approval before creation;
its plan is approximately **US$10/month**, before applicable taxes. Verify the
quote at approval time. Use its internal URL for the web, Stockfiller and
geotagging services. No paid resource was created. The web production dependency
change is only `redis==6.4.0`. Fakeredis/Lua/psutil are test/benchmark dependencies.
Official sources checked 20 October 2026: [pricing](https://render.com/pricing),
[compute plans](https://render.com/docs/compute-plans),
[Key Value](https://render.com/docs/key-value),
[private network](https://render.com/docs/private-network),
[Render root-directory isolation](https://render.com/docs/monorepo-support), and
[Redis lease limitations](https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/).

**Merge: GO after the PR's GitHub checks pass and review accepts the documented
Sheets/lease limits.** Default production route activation still requires the
coordinated-writer configuration; the exclusive-writer flag does not bypass it.
The attention fix needs no data migration.

**Production activation: NO-GO now.** Required remaining steps are
an approved `noeviction` instance, coordinated rollout of
all actual writers, live staging verification including quotas/restart recovery,
and explicit approval for production deployment/activation. A strict guarantee
against arbitrary concurrent manual edits cannot be supplied by this design; if
that is a release requirement, use the operational constraints or conditional-write
authority described above before granting GO. No merge or deployment is performed
by this task.
