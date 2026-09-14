# Commercial Prioritization v2.2

Implemented from freshly fetched `origin/master` at `b98ed5d2af3291ac4aa1aaa4e4f2daf2a3a3deea` (the same commit as the development brief). Branch: `codex/commercial-prioritization-v22`. No merge or production deploy is part of this change. PR #20 / Instagram is untouched.

## Behavior

- Commercial volume uses `Total weight` through one shared parser. The Stockfiller `flatten_order` contract was verified: delivered, then received, then ordered; explicit zero is preserved. No raw data is rewritten. Missing/invalid volume remains identifiable; positive revenue can establish a purchase even when its volume is unknown. Zero-volume and credit-only orders do not establish positive commercial history. Logical orders net their product rows before being classified.
- Completed, distinct commercial delivery dates supply `history_index` 0 / 60 / 100. Multiple references or SKUs on one delivery date count once. Future deliveries suppress recommendations and do not increase completed history. Reactivation retains history.
- Default pilot weights: intent/timing 62%, value 26%, history 7%, strategic 5%. Every component and the final score are bounded to 0–100. Existing deterministic ranking tie-breakers are preserved.
- One-time and repeat customers have separate reactivation triggers after 30 calendar days without human contact (or if never contacted), independent of segment and score. Existing explicit follow-ups and stronger signals still apply; real suppressions always win.
- Positive dialogue keeps the existing earliest follow-up and expires after day 30. Relevant Stockfiller/product clicks are active on days 3–14; later human contact/order handles them. The stable click identity survives expiry so clock aging alone does not reset dismissed/snoozed workflows. Open-only signals remain historical information, without a score bonus, trigger, or visit recommendation.
- Optional route candidates use the broad owner portfolio, positive priority, trusted coordinates and shared contact/workflow restrictions. No action trigger or recommended channel is required. The route provider, geographic scoring, time budgets, mandatory visits, confirmed appointments, retries, ownership and identity checks are retained. Both automatic optimization and the older optional route-proposal path respect the relevant restrictions.
- The effective overdue queue is reused by routing. Only a later human activity with a strictly later timestamp, or a new future `planned` activity, replaces an overdue item. Seconds are preserved for this comparison; `completed`, `cancelled`, `skipped`, opens and clicks do not replace it. History is not deleted or marked complete. Legacy follow-up fields retain their existing reconciliation behavior.
- Reorder/reactivation normally recommend phone if available, with existing availability fallbacks and manual channel choice. Existing NÄSTA ÅTGÄRD now includes available contact result/comment/person/phone, latest order volume and relevant follow-up. No new required seller fields or dashboard.

## Calibration contract and schema

The JSON calibration export keeps all legacy fields/events. The legacy `order_outcome` and `first_order_*_after_event` remain unbounded for compatibility; use the new fields for 10-day evaluation:

- `order_within_10d`, `first_order_date_within_10d`, `first_order_reference_within_10d`, `first_order_dfp_within_10d` include day 0 and day 10, exclude day 11 and future orders.
- `window_closed_10d` becomes true after the full tenth calendar day has elapsed. An open window with no observed order is not a final negative outcome.
- `observation_unit=score_event` and `is_human_contact=false` distinguish technical observations from contacts. `event_type` preserves created versus planned versus resolved. Human contact attribution remains in Sales Coaching and still uses the latest qualified human contact, order date, and inclusive 0–10 calendar days.
- Multiple event rows may observe the same order. For additive order totals, use `credited_order_count_10d` / `credited_order_dfp_10d`: each logical order is credited once to the latest **suggestion_created** event in its window. Planning/resolution events get no extra credit. This is a diagnostic association, not seller/contact attribution or evidence of causality. Missing volumes are flagged on logical orders; aggregate DFP includes known volume only.

Two append-only worksheet columns are added through the existing schema mechanism: `planning_suggestions.history_index_at_creation` and `score_events.history_index`. Existing cells/events are not backfilled. No live schema migration was run.

## Switching weights / rollback

Set `PRIORITY_SCORING_POLICY=v2.2` (default) or `PRIORITY_SCORING_POLICY=v2.1` in the process environment or local `.env`, then restart the app. v2.1 means 65/30/0/5. Both policies use all corrected v2.2 volume, trigger, signal-aging and suppression rules. Do not revert the implementation to change weights. Policies are centralized in `priority.SCORING_POLICIES`; offline comparison can pass `scoring_version` to `build_priority_customers`. The configured policy is recorded in score events. Unknown policy names fail startup explicitly.

## Validation

Using `.venv/Scripts/python.exe` on Windows:

- `-m unittest discover -s web-app/tests -p 'test_*.py'`: **671 passed**. Includes ownership, safe customer matching/special characters, idempotency/double-clicks, cache invalidation, unchanged planning/confirmed-visit/picking-help flows and route optimization.
- `-m unittest discover -s tests -p test_stockfiller_sync.py`: **23 passed**.
- `-m compileall -q web-app`: passed.
- Final focused checks after the configuration review: `-m unittest test_commercial_v22 test_priority_identity test_action_queue_v21` with `PYTHONPATH=web-app;web-app/tests`: **32 passed**. Fresh-process v2.1 configuration/score check passed.
- Desktop (1440×1000) and mobile (390×844): **passed** with the existing in-memory planning browser harness and real scoring. Verified reactivation reason, 120 DFP instead of Quantity 24, contact context, phone advice, manual visit override, and saving a confirmed visit with picking help. No production CRM or routing provider calls.
- `git diff --check`: passed.

Browser reproduction: start `web-app/tests/planning_browser_harness.py` with `BROWSER_HARNESS_COMMERCIAL=1` and `BROWSER_HARNESS_PORT=5072`; run `node web-app/tests/commercial_browser_smoke.cjs desktop`. Restart the harness with fresh in-memory data before `mobile`. `COMMERCIAL_HARNESS_URL` overrides the default URL. Uses Playwright, with installed Chrome as fallback.

## Synthetic before/after comparison

No current complete authorized local CRM snapshot was available. Only older July customer/order CSV fragments were found; **no real CRM replay was performed**. `python scripts/commercial_v22_replay.py` prints the reproducible, read-only comparison in `synthetic-replay.json`. The baseline executes the actual priority and route-input functions from the verified master commit. Each seller name below labels an identical **fabricated** 36-store portfolio, not that seller's real performance.

| Variant | Seller | Reactivation with trigger, before suppression | NÄSTA ÅTGÄRD after suppression | Previous customers / prospects in up-to-top-30 | Overdue | Optional route pool |
|---|---|---:|---:|---:|---:|---:|
| Original v2.1 | Johan | 12 | 16 | 4 / 12 | 3 | 24 |
| Corrected rules, v2.1 weights | Johan | 18 | 13 | 4 / 9 | 3 | 16 |
| Full v2.2 | Johan | 18 | 10 | 4 / 6 | 3 | 16 |
| Original v2.1 | Daniel | 12 | 16 | 4 / 12 | 3 | 24 |
| Corrected rules, v2.1 weights | Daniel | 18 | 13 | 4 / 9 | 3 | 16 |
| Full v2.2 | Daniel | 18 | 10 | 4 / 6 | 3 | 16 |
| Original v2.1 | Sofia | 12 | 16 | 4 / 12 | 3 | 24 |
| Corrected rules, v2.1 weights | Sofia | 18 | 13 | 4 / 9 | 3 | 16 |
| Full v2.2 | Sofia | 18 | 10 | 4 / 6 | 3 | 16 |

Reactivation counts include any relevant trigger for an already-reactivation customer, including the old strategic trigger. Overdue activities are listed separately from recommendations. Each corrected portfolio excludes 3 recent contacts, 3 negative cooldowns, 3 future deliveries, 3 future planned activities, 3 effective overdue activities, 1 snooze, 1 dismissal and 3 untrusted coordinates from the optional route pool. Three superseded overdue records are no longer route blockers. Smaller queues reflect removal of stale interest and actual restrictions; this synthetic mix is not a target ratio or an estimate of production impact.

## Limits

62/26/7/5 remains an uncalibrated pilot. Real seller impact requires a fresh complete replay. Unknown volumes do not acquire a Quantity fallback; known-volume totals can consequently be lower than before. A delivery date is the existing conservative purchase-occasion boundary, so separate same-day deliveries are counted once. Google road optimization was covered by the existing test doubles/provider contract tests; no paid/live routing call was made.
