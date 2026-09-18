# Guidance and delivery integrity correction

Base: `f967920cbf54cb322014320ec1613e5a2742e084` (master, 2026-09-18).

## Scope

- Canonical trigger explanations are built in `priority.py` and passed into
  `build_customer_guidance`, then reused by customer cards and planning.
- A real, unresolved follow-up due today gets the due-today explanation. Other
  triggers never use that fallback. Plans, overdue activities and suppressions
  retain their existing precedence.
- A-prospect trigger/context identity stays stable while its explanation can
  include a live click or dialogue. Reactivation focus cannot mask a primary
  click, dialogue or strategic-contact trigger.
- The approved **11-90 day** first-delivery reorder window replaces **24-90**.
  Scoring weights and timing curves are unchanged. Some day-11-23 customers
  therefore become actionable earlier without a score change.
- Only the factual one-delivery, day-11-90 state gets the longer focus label.
  Focus filtering uses stable keys, not the mutable labels.
- New `latest_delivery_dfp` and `next_delivery_dfp` are looked up using the
  respective latest completed / nearest future delivery date, including summed
  order references on the same date. Missing or partial volumes, or conflicting
  dates within one logical order, do not produce a misleading total.
- `latest_order_dfp`, forecasts and historical first-delivery quantities retain
  their original semantics. They are not fallbacks for the new display fields.
- Existing commercial-order eligibility, identity resolution, delivery-date
  fallback to order date, and completed-date boundary (`<= today`) are retained.
  This change does not add shipment-status evidence or redesign that contract.

## Validation

- Baseline web-app suite: **742 passed**.
- Updated web-app suite: **768 passed**, including 26 new unit, API and executing
  JavaScript regression tests. Existing expectation changes are limited to the
  new copy, fixed predicted-date display and approved day-11 trigger boundary.
- Deterministic before/after replay: **3,000 synthetic customers** across history,
  segment, contact, email, explicit follow-up and planned-activity combinations.
  Zero unexpected differences: scores, score components, history, forecasts,
  existing dates/volumes, suppression eligibility and decision-context hashes
  are identical. Trigger/status/contactability changes are confined to the
  approved day-11-23 window; A-prospect identity never changes due to copy.
- Root `tests/` suite: **104 run; 8 failures and 1 error**, with exactly the same
  failing tests on the unchanged baseline. These older reminder-email tests are
  outside this correction; their failures must not be represented as new ones
  or described as passing.
- New Chromium smoke covers 15 scenarios at 320, 390 and 1440 pixels, real focus
  checkbox interactions, planning/card explanation parity and delivery pairs.
  It is included in the existing GitHub Actions QA workflow.

No production CRM rows, score policies, context-hash schemas, snooze/dismiss
history, follow-up resolution rules or route algorithms are migrated or edited.
