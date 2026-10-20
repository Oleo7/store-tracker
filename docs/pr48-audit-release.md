# PR48 audit corrections and release gate

Baseline: `5a7d1029cfd782ffa510450839f7774e852675c2`. Active model remains
v3.5, policy `2026-10-audit-corrected`; route ranking contract is
`v35-priority-lexicographic-2`. No v2 scoring fallback is added.

## Corrections

| Finding | Implementation | Permanent evidence |
| --- | --- | --- |
| F1 | Route apply invalidates shared caches, reads each decision sheet freshly into a request-local scope, resolves the owner again, validates and writes inside the planning lock. Read failures fail closed. Production route apply additionally requires the exclusive-writer gate below. | `test_revision_release.FreshRouteAuditTests`: warm cache, external refusal, owner change, same-day booking, both route engines, required/optional stops, repeated requests, read failure. |
| F2 | Ten-day positive buyer waiting period precedes first/second-purchase routine follow-up and internal bookings. Separately documented promises retain their scope; order/delivery and refusal guards remain. | Days 0–11, repeat buyers, separate promise and refusal cases; existing order/delivery tests. |
| F3 | Linked calendar activities reject customer reassignment with HTTP 409. Reducer quarantines historical foreign-customer links instead of borrowing the source promise. | API rejection and independent legacy-reducer counterexample. |
| F4 | Evidence, review and calendar status are separate. A booking cannot establish an uncertain service promise. Existing Att hantera rescheduling requires documented clarification and explicit agreement to activate it. Independent commercial work can remain eligible. | Old/current/future bookings including confirmed times; manual clarification with actor, reason and time. |
| F5 | One shared timestamp comparison makes refusal win equal-time conflicts, independent of row order. Later evidence/manual permission decisions retain explicit chronology. | Permuted contradictory rows and later allowance. |
| F6 | Drive legs, return, blocking intervals, scheduling and objective comparisons preserve seconds. Presentation minutes are separate. Anchor results distinguish exact search over the provided pool from global optimality. | Equal-score routes of 238 versus 122 driving seconds; existing fixed-time/lunch/Google tests. |
| F7 | Inclusive day-end feasibility throughout candidate filtering, solving and final checking. | One second before, exact boundary and one second after; updated independent exact-fit cases. |
| F8 | Unanswered retry sequences expire after 30 calendar days; ordinary value-based prioritization resumes. Independent promises persist. No commercial weights changed. | Days 5, 30, 31, 60, 90 and 3650, separate promise; full existing contact/order suite. |
| F9 | Existing cluster ranking rotates through geographic cells in pairs before deepening dense cells. Existing priority/efficiency rankings and final lexicographic objective remain. | `scripts/benchmark_route_shortlist.py` and ordinary-suite sparse-pair regression. |

The F1–F8 counterexample tests all fail on the baseline checkout and pass on
the corrected checkout. No production CRM records are included in tests.

## Route benchmark scope

The original 50-candidate star-road example improves from 90 to 140 points,
with 221 seconds driving plus 40 seconds service. The full two-stop oracle is
140, so the score gap closes from 50 to zero. Analogous 100/500-candidate cases
also reach 140; measured shortlist/solve time is approximately 0.013/0.156 s
locally. Twelve deterministic eight-candidate problems match exhaustive
enumeration for both score and driving time. Reproduce with:

```text
python scripts/benchmark_route_shortlist.py --output benchmark.json
```

These are synthetic matrices and at most two stops. They do not prove quality
on arbitrary larger multi-stop instances. Geographic cells can still miss
useful boundary-spanning combinations. Provider request/response/feasibility
tests cover the Google engine, but no live Google optimization benchmark was
run and no private representative road matrix was available. Neither engine
has an unconditional global-optimality guarantee.

## Exclusive-writer release protocol (mandatory)

Google Sheets offers no atomic read/check/write transaction. The application
lock serializes participating application writers; it cannot lock an external
Sheets editor, importer, separate service or unsynchronized app instance.

1. Inventory every writer: human editors, imports, sync jobs, API keys/service
   accounts, webhooks and all app instances. Keep an accountable owner/list.
2. Restrict spreadsheet editing to the app identity during normal operation.
   Human users use CRM APIs. Disable scheduled import/sync writers during app
   write windows. An environment variable alone is not enforcement.
3. Verify the existing planning lock health/configuration. All app workers must
   share the supported lock scope. Do not run independent write-capable
   instances without a shared supported lock; use one instance if uncertain.
4. For external imports/edits: stop incoming writes and drain all app workers,
   pause background writers, take a complete export, perform the controlled
   import, verify it, then restart workers with empty caches and resume writes.
5. Only after ACLs and the protocol have been verified in staging may the
   operator set `CRM_EXCLUSIVE_WRITER_CONFIRMED=true`. It defaults to unset;
   route apply returns 503 `exclusive_writer_required` outside test mode.

There remains a race if this protocol is violated. Production is NO-GO until
the deployment operator demonstrates enforcement; fresh reads alone do not
remove that risk. A transactional datastore/shared writer service is the
long-term alternative if continuous external writing is required.

## Stop, backup and recovery

Set `CRM_RISKY_OPERATIONS_STOPPED=true` and restart/drain **all** workers. The
server rejects unsafe HTTP methods (except login/logout) and route generation
with 503, including direct API callers. It also checks the flag inside route
apply. Authentication and read access remain available. Pause external jobs
and inbound/background webhook processing too; do not interpret this flag as
revoking in-flight requests or already-sent messages. Confirm blocked contact,
calendar, mail and route requests before beginning maintenance.

With all writers stopped, export **all** worksheets, metadata, headers and
rows. Include customers, users/settings, orders, contacts, calendar, source
links, customer_state_events when present, suggestions, score/workflow events,
email message/recipient/events, route proposals/runs and sync state. Retain
row order, schemas, IDs and timestamps. Securely back up credentials/secrets
separately, retain checksums, and test restoration into an isolated workbook
before release. Never commit any real export to GitHub.

The local readonly review snapshot contains all 14 available worksheets,
with credentials redacted. `customer_state_events` was absent. It is useful
for data rehearsal, but is **not** a restorable production credentials backup
or an atomic live backup. The offline recovery tool verifies a serialization
round trip and preservation of simulated later contacts, calendar and state
events; it rejects a changed or incomplete latest export:

```text
python scripts/verify_crm_recovery.py --snapshot PRIVATE.json --output report.json
```

Recovery after new CRM writes must start from a complete export taken **after
stopping and draining all writers**. Preserve both original backup and latest
export. Use the latest data with a compatible v3 build/forward fix. The tool
deliberately preserves the entire latest state, including edits/deletions;
it does not invent a conflict merge. There is no verified durable production
journal here. If latest data are corrupt or unavailable, remain stopped until
later events are recovered and reconciled. Never overwrite them with an old
backup. The first v3 release has no compatible earlier v3 build: keep affected
operations stopped while repairing forward; never reactivate v2.2.

## Launch checklist and monitoring

- Verify the final PR SHA and passing GitHub CI, then separately authorize merge
  and deployment. This task performs neither.
- Rehearse the exclusive-writer protocol, worker drain, redacted-secret backup
  replacement, isolated workbook restore and forward recovery in the actual
  staging/deployment topology. Keep production stopped until these pass.
- Deploy with stop enabled, verify `/health` reports the intended release SHA,
  score version, ranking contract and lock health. A successful health request
  alone does not certify the writer protocol. Verify old preview rejection.
- Check all active reads use v3, owner boundaries, current contact refusals,
  booked visits, service review state and existing source links against the
  stopped snapshot. Do not bulk-rewrite historic activities to make tests pass.
- Start a controlled seller pilot; compare contacted versus holdout customers
  within eligible strata. Measure incremental DFP per seller hour, repeat
  orders, refusals/complaints and contact burden. Historical purchase prediction
  is not proof of incremental sales attributable to contact.
- Monitor 409 codes for stale preview, owner/calendar changes and source-link
  conflicts; 503 stop/writer-gate/lock failures; Sheets read failures, save
  failures, duplicate activity/request IDs, uncertain-service execution and
  denied-contact attempts. Record counts/latency, never customer notes or tokens.
- Capture p50/p95 for customer list, priority, calendar, preview and apply, plus
  Sheets read count, lock wait and provider failures. Cold fresh route saves
  cost additional Sheets I/O; local in-memory timings do not measure this.
- Stop immediately on an unauthorized contact, cross-customer link, duplicate
  committed activity, unexplained data loss or broken writer isolation. Retain
  evidence and latest export; repair forward with compatible v3.

## Validation and remaining limits

Local: 1,102 backend tests, 106 repository tests and all five browser commands
(desktop/mobile coaching, guidance, priority workflows and calendar drag)
pass. CRM replay uses actual backend parsers/model/Flask handlers and a
readonly Sheets adapter; all six active user scopes are checked. Readonly
historical replay and aggregate analysis remain private outside the repository.

Not exercised: live deployment/drain/ACL enforcement, real workbook restore,
post-backup production journal reconstruction, live external-provider traffic,
large multi-stop optimality proofs and production network/load p95. These are
explicit deployment gates or benchmark limitations, not passing test claims.
