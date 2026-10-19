> Historical contract. Active ranking, routing and recovery are superseded by [v3.5](value-contact-priority-v35.md).

# V3.4: reconciled history, one seller priority and aligned routing

This document supersedes the v3.3 interpretation of legacy follow-up dates.
The branch is a release candidate, not a claim of measured sales uplift. Deployment,
CRM migration and merge remain separate approvals. No production data migration is
required by this change and no historical CRM rows are rewritten by a GET.

## Seller workflow

Butiker remains one page. Alla, Att agera, Planerade and Avvakta are filters on that
page, not separate pages. There is no Granska filter. The primary displayed number
is Prioritet. Affarsvarde remains a secondary explanation/alternative sort.
Relevant booked work stays in Alla with its work priority; the booking suppresses
an extra unscheduled contact, not the underlying commercial opportunity. Only a
real calendar activity can create green planned status. A date-only reminder
is not a booking. Missing calendar identity fails closed rather than fabricating
an executable appointment.

An old independent service issue can be annotated and handled without suppressing
otherwise valid commercial work. That never authorizes executing the uncertain
issue itself. The list and every scoped action apply the same customer-level
negative, retry, order and refusal restrictions. A future internal reminder or a
snoozed actual promise remains binding even when an older issue is nonblocking.

## Four kinds of historical evidence

1. **Internal reminder**: a follow_up_date alone, or an unconfirmed source-linked
   follow-up booking. The field is evidence of seller scheduling, not customer consent.
2. **Planned task**: a calendar activity without separate customer-promise evidence.
3. **Customer promise**: explicitly recorded follow-up purpose or confirmed agreement.
   Only the independent agreement flag/time can create an exception to a negative
   dialogue; purpose or date alone cannot.
4. **Uncertain service issue**: conservative clues about a complaint, credit,
   replacement delivery or an actually promised/booked demonstration. These clues
   preserve an administrative item, not a proven customer agreement. Mentioning
   a possible demo alone is not enough.

Old internal reminders are projected as handled/superseded when a later reached
contact on/after the due date, a new internal schedule, an appropriate later
booking or a later commercial order supersedes their purpose. An unsuccessful
attempt alone does not fulfill them. A closed linked internal calendar activity
is not resurrected as an outstanding promise. Actual promises are not resolved
by unrelated contact/order/booking events. Source-linked explicit outcomes,
replacement activities and saved decisions remain authoritative.

Every derivation retains its source, resolution basis and event reference. Raw
contact/calendar history stays unchanged. The UI displays a short history note.
This is an interpretation of historical scheduling evidence, not proof of exactly
what was verbally promised. Ambiguous service histories still require judgement.

## Identity and writes

New events use persisted IDs. A legacy source without an ID uses a deterministic
anchor of canonical customer ID, timestamp and normalized contact channel. Notes,
seller display names, result and spreadsheet position are not identity fields.
They still affect optimistic concurrency tokens when relevant.

Same-anchor collisions retain all source variants, are marked ambiguous and reject
mutation rather than selecting the first row. Previously saved v3.3 legacy keys
are bridged only through an unambiguous saved source timestamp/customer. Explicit
writes can persist that stable alias. A comment correction does not silently
create a new promise; rescheduling with the old saved alias is regression-tested.
Arbitrary changes to customer identity/timestamp/channel still require explicit
mapping. No fuzzy merge or bulk ID backfill runs automatically.

The contact form has an explicit follow-up agreement checkbox. The calendar editor
also records agreement separately from scheduling. Rescheduling an existing
promise preserves its original agreement time; moving an internal date does not
manufacture a new agreement after a negative reply. Separate same-time events do
not gain a negative-dialogue exception from spreadsheet row order.

The first-delivery form can record that onboarding was actually completed. A
plain positive dialogue on delivery day does not prove completion. An explicit
completed onboarding on that day does count. Unsuccessful attempts remain open.
Source revisions, owner checks, idempotency and partial-write repair are retained.

## Order evidence and other commercial rules

- A recent reported order without an import match receives the existing two
  weekday sync pause, then internal order-status verification. No order/volume or
  reached dialogue is invented from the report.
- A dated delivery after the report supersedes generic acquisition waiting. This
  is not labelled an exact order-reference match. A historical report older than
  60 days becomes an annotation, not a perpetual sales prohibition.
- A registered but undated order retains the existing hold through day 14 and
  order-status follow-up through day 60. Beyond that it is historical evidence,
  excluded from completed-delivery value, with an annotation. Other current
  restrictions and actual customer promises still apply.
- A later order stops generic acquisition retry. It does not remove the retry
  pause of a separate outstanding promise.
- Self-ordering is taken from the latest reached positive/neutral dialogue, not
  erased by a subsequent failed attempt. A relevant future reminder waits; a due
  reminder can prompt checking the customer's own order. A later order clears
  the obsolete internal cue.
- Positive prospect follow-up starts at day 7 and positive buyer follow-up at
  day 10 in the provisional policy. An appropriate explicit promise/internal
  due date can be earlier subject to all applicable hard gates. This differs
  intentionally from v2.2's day-3 positive-buyer trigger and needs pilot validation.
- First-delivery onboarding days 7-10 and second-purchase phases days 11-90 retain
  the documented phase curve and one-SKU signal. A newer reached contact enforces
  the short pause even if an older same-purpose contact exists.
- Reactivation remains distinct from active reorder. Unknown history is not proof
  of never contacting a customer. Real opt-out stays separate from ordinary
  negative commercial dialogue; this change does not redefine consent.

## Att hantera

Only missed calendar work and overdue real/uncertain separate follow-ups with a
Stockholm date strictly before today are included. Today's appointments remain
in the calendar. Date-only internal reminders are prioritized on Butiker, not
materialized as independent promises here. Completed/superseded internal history
is not a queue item. Actionable work sorts before administrative review items,
then by due date and stable identity; pagination is retained.

Each item is authorized in its own scope. It cannot borrow permission from an
unrelated customer action. Administrative resolution, rescheduling and snooze
remain available through the customer/attention controls without a separate
review filter. Current unimported order reports are not customer promises and
therefore are not added to Att hantera merely to produce a second generic queue.
They remain visible on the relevant customer. The underlying order discrepancy
still needs operational verification when it is recent.

## Both route engines

Optional candidates now carry the new unrounded contact-priority raw value as
`commercial_value`, not v2.2's numeric score or the rounded 0-100 display index.
Mandatory booked visits use the work-priority raw value for diagnostics; their
mandatory status/time windows determine inclusion. Eligibility is still checked
first. Invalid modern values fail closed, rather than silently reverting to v2.2.

The legacy engine uses raw commercial values in preselection, insertion,
beam/subset objectives and value per incremental time. Google Route Optimization
uses a linear skipped-shipment penalty: raw value x 10. Travel/time cost, workday,
fixed activities, visit duration, maximum stops, traffic validation and fallback
limits remain separate and unchanged. These abstract cost units are not SEK.
The factor of 10 is provisional; raw-value scaling can change the value/time
trade-off compared with the old bounded score and needs actual route/pilot review.

Map ranking and the non-route map point display use the same new priority.
Displayed score totals remain descriptive, not the optimizer's raw objective.
Fingerprints/cache contracts include commercial value and ranking mode. Old
route payloads cannot silently be reused after the model switch.

Preserved route choices that require explicit commercial evaluation:
- channel recommendation is a preference, not a strict ban on a nearby visit;
- phone/email planned on the same day blocks an extra optional visit;
- the existing other-day phone/email exception can expose the already-authorized
  work alternative, never lifting a real negative/order/retry/opt-out restriction;
- mandatory visits retain their independent scope and time constraints;
- a route for a future day is a projection from currently recorded information,
  not knowledge of later contact/order outcomes. Apply revalidates current inputs.

## Safe rollback procedure

`PRIORITY_RANKING_MODE=v3` is the new commercial ranking. Setting
`PRIORITY_RANKING_MODE=v2.2` switches the list and both route engines to the retained
v2.2 numerical ranking **under the same current contact/identity/commitment rules**.
It does not discard newly stored commitments, opt-outs, comments or outcomes.
The UI explains reserve sorting and cache keys/fingerprints include the mode.
Unit tests exercise list and route adapters across the switch and back.

Operational sequence before enabling for all sellers:
1. Keep the previous deployment artifact and a complete private CRM backup,
   including any customer_state_events and added source columns.
2. Deploy the candidate only after approval. Verify the chosen mode, user/owner
   boundaries, today's calendar and a synthetic/private pilot lifecycle.
3. On ranking problems, change the mode to v2.2 and restart the service. Generate
   fresh route previews; verify the UI reserve-sort annotation and that state
   and contact restrictions persist. Switching back to v3 uses the same data.
4. On a broader application problem, stop writes and diagnose or forward-fix.
   Do not blindly run the old v2.2 binary after v3 state has been written; it lacks
   the newer state contract. Restore data only with an explicit reconciliation of
   writes made since the backup. The mode switch is not a complete binary downgrade.

`VALUE_PRIORITY_ENABLED=false` remains only a view-level fail-closed switch, not a
full rollback. Neither setting changes commercial weights in an admin UI. Those
weights remain code-versioned; a future admin editor should support preview,
validation, audit and explicit publication, not unrestricted live mutation.

## Verification and interpretation

`test_history_priority_v34.py` covers actual reducer/model/write behavior including
comment-edit identity, duplicate-source refusal, old alias rescheduling, later
contact/order/booking reconciliation, exact promises, first-order phases,
self-ordering, stale-order annotations, scope pauses and rollback. Existing tests
that represent real customer agreements now carry explicit evidence; their mere
historical date is no longer treated as sufficient proof.

`replay_priority_snapshot.py` runs the actual checkout and Flask endpoints with
only a read-only Sheets adapter and frozen clock. Outbound HTTP and all writes
are blocked. Full portfolios are computed before owner filtering; six users can
be verified in independent shards against the same full admin result. The script
records commit, source hashes, snapshot hash, policy, time and cache reads.
`--user-name` selects a verification shard, not a smaller scoring population.
`--verification-only` omits repeated private output payloads after the same checks.
`compare_priority_replays.cjs` executes the actual checked-in JavaScript sorters.

No private CRM snapshot, customer list, notes, contact details or credentials are
part of Git or CI. Full-data results are delivered privately. Regression tests,
model replay and deterministic route tests cannot demonstrate causal sales uplift
or guarantee optimal real-road tours. Validate realized second purchases,
reorders, delivered volume, margin per contact time, ignored recommendations and
visit productivity in a controlled pilot. V3 should not be declared commercially
superior merely because it maximizes its own score.
