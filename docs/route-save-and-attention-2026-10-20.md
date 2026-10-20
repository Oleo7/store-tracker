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
   their requirements, and invoke the package/script from there. If enrichment
   currently uses rootDir `data-enrichment`, its later approved deployment must
   clear that rootDir and use `pip install -r data-enrichment/requirements.txt`
   with `python data-enrichment/enrich_stores.py`. Confirm actual build roots in
   the pending infrastructure inventory; do not assume parent files exist.
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
