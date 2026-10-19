# V3.5 release contract: one model, priority-first routing

This supersedes the v3.3/v3.4 ranking and rollback sections. Deployment and merge
require separate approval. CRM history is not rewritten by this change.

## Exactly one active scoring model

`value_prioritization.VERSION` and `priority.SCORE_VERSION` are `v3.5`. The active
feature-to-decision path contains no v2.2 formula, weighted legacy calculation,
runtime model selector, or fallback to a previous priority value. The obsolete
ranking/scoring environment variables are not read. Explicit calls requesting an
obsolete scoring version fail, rather than selecting it.

`priority_score` is now a compatibility FIELD NAME for the current v3 work index;
`priority_score_precise` is its six-decimal counterpart. It is not an invocation
of the former algorithm. Existing API consumers, logging and saved suggestions
receive current v3 values. Historical `*_at_creation`/`*_at_contact` records remain
unaltered as audit evidence; no live recommendation uses them as its score.

The former `/planning/suggestions` adapters use only current v3 candidates.
Existing suggestion IDs, context aliases, snoozes, activities, customer promises,
owner controls and explicit outcomes remain usable. Missing current model data
is unavailable, not an opportunity to reuse a stored score. The obsolete stub
candidate path is removed. Archived numeric-reference tests live ONLY under tests;
a textual historical analysis script lives under docs/archive. Neither is imported
by runtime code. Static and behavioral regression tests enforce this boundary.

## Retained business rules

The DFP potential, confirmed dated delivery history, shrinkage and segment/format
priors remain unchanged. First-delivery onboarding, second-purchase timing and
one-SKU signal are independent v3 rules. The high-value onboarding indicator is
computed independently from portfolio order sizes, without a legacy score call.

Buyer-positive dialogue uses 10 days by default. Actual documented follow-up
agreements can be earlier, subject to their own identity, evidence and hard
contact restrictions. A-prospect protection, self-ordering reminders, reached vs
unreached dialogue, retry timing, later-order resolution, negative dialogue,
contact prohibitions, scope-specific customer promises and manual service review
remain as in the corrected v3.4 contract.

Butiker is one page with Alla, Att agera, Planerade, Avvakta. There is no Granska
filter. Relevant genuinely booked work remains green and ranked in Alla; booking
prevents an extra unscheduled contact, not the customer's commercial importance.
Date-only reminders do not masquerade as calendar bookings. Business potential
remains secondary explanatory data / optional portfolio sorting.

## One route objective

For all eligible optional stores, both routing engines use the SAME normalized
v3 contact-priority index displayed in Butiker, with six decimal places internally.
They no longer optimize raw DFP potential, the former score, or value per minute.
The objective, in order, is:

1. Highest SUM of current normalized priority across selected stops.
2. Lowest DRIVING time, including return to origin, when priority sums are equal.
3. Stable customer identity/order for reproducibility of remaining exact ties.

One micro-point (0.000001) is the internal precision boundary. A difference below
that precision is a tie; displayed integer points can be equal while the internal
values differ. The displayed score is an index, not a conversion probability or
SEK. Example: 95 points beats 85 when only one stop fits, even if it takes longer;
two 65-point stores beat one 95-point store if the pair fits the day.

Contact-channel recommendation is never a route feature or candidate exclusion.
Actual calendar events still matter: a phone/email appointment on the route's
DATE blocks an additional visit, even if a required-visit conflict would otherwise
try to override it. A telephone booking for a different date follows the existing
other-day rule, without lifting negative/order/retry/contact-ban restrictions.
Mandatory visits retain their own scope, fixed times and priority diagnostics.
Workday end, travel, service duration, lunch, existing fixed events, maximum stops,
trusted coordinates and customer ownership remain feasibility constraints.

### Implementation and limits

The local engine's complete-tour selection compares integer micro-point sums,
then driving seconds. Small problems use exact dynamic programming / exhaustive
anchor-aware search. Large pools use bounded shortlist/beam search; geography can
help that search find candidate combinations, but is not an extra commercial
score in the final objective. Metadata identifies heuristic/shortlisted results;
we do NOT claim global optimality across a whole seller portfolio in these cases.

Google uses a skipped-stop penalty of `micro_points * (hard_day_seconds + 1)` and
travel cost of one unit per driving second. Other optional time/soft-duration
costs are zero. Thus losing one micro-point costs more than ALL driving that could
be saved during a feasible day. This is a lexicographic encoding, NOT the former
provisional factor 10 or a commercial/time calibration knob.

The bounded traffic-recovery sequence remains. Its safety margin is now a HARD
feasibility limit, never a soft cost competing with priority; provider responses
are validated against that reduced limit too. The external solver is time-bounded
and can return a feasible, non-proven-optimal tour. The request objective is exact;
a provider's achieved global optimum is not guaranteed or claimed.

## Cached data, preview and apply safety

Both engines use the new `v35-priority-lexicographic-1` ranking contract. Old route
caches/preview tokens are rejected and must be recalculated; they cannot reintroduce
v2.2. Google cache reuse also requires the current solver version and exact current
request fingerprint. Client/server routes do not infer a current score from a
historical `priority_score` if current v3 fields are absent, invalid or inconsistent.

Before saving a local-engine route, the backend re-evaluates every selected store's
current contact permission and same-date phone/email exclusion. Required visits
are authorized in their own scope. Google apply revalidates its full input
fingerprint. Existing completed/idempotently applied routes are not duplicated.
Historic calendar entries remain historical entries, not newly ranked proposals.

## Manual historical service handling

Identified independent service issues remain under Planering / Att hantera or the
customer's Hantera action. The responsible seller checks original notes and later
work, then records performed, cancelled with basis, or a linked reschedule. Unknown
history is not automatically declared fulfilled. Ordinary obsolete reminders stay
reconciled with their evidence, and do not erase valid separate commercial work.
This release does not bulk-resolve, backfill or delete production CRM rows.

## Deployment / recovery

Before an approved deployment, take a complete private CRM backup (including
customer_state_events and added source columns) and record the new commit. Remove
obsolete ranking environment variables from the deployment configuration when
convenient; they cannot select an older engine in this code.

There is NO v2.2 reserve mode. Recovery is a compatible tested v3 build or forward
fix. On the first v3 deployment, no previous compatible v3 build may exist: stop
affected automated recommendations/routes while diagnosing, rather than run an
old binary over new state. `VALUE_PRIORITY_ENABLED=false` remains a list-view
fail-closed control, not a global kill switch or an old-model selector. Do not claim
it disables all backend automation. Operational incident response must stop the
affected service/entry point if broader isolation is needed. Preserve all writes
made since the backup; never blindly restore an old database over them.

Release gates: full backend/root regression suites; browser desktop/mobile and
calendar workflows; actual full CRM replay with six-user owner checks and zero
network/writes; stale/missing-score tests; archived-version non-invocation tests;
exact small-route comparisons against independent exhaustive enumeration; and
same-day booking / required-scope regression tests. Public CI uses synthetic data
only. No live Google route optimization or real email is required for these tests.
Successful tests/replay are not evidence of causal sales uplift.
