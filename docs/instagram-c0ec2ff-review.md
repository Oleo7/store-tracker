# Integration review: c0ec2ff + Instagram PR #20

Prepared locally on codex/instagram-feed. No merge commit, push or deployment performed.

## Exact scope

Applied the complete diff from 5378ac8 to c0ec2ff8f75c290dc18c9052fc85f961d9e0da2a
without conflicts. All production files from c0ec2ff are preserved byte-for-byte except:
- .env.example and render.yaml retain the existing Instagram configuration additions.
- web-app/app.py retains the existing Instagram blueprint import/registration, separate
  CORS handling for its two endpoints, and their public endpoint allowlisting.

All Instagram implementation files, JS/CSS, tests, environment defaults (limit 18) and
feed logic are unchanged from the existing branch. No Render environment changes.

The integrated commit adds settings to the sales-coaching summary, sales-linked results
and activity trends in sales_coaching.py/JS/CSS, their Python tests, browser harness,
smoke tests and six reference screenshots. Existing Store Tracker code is not reverted.

The only adjustment to the imported code is in tests/sales_coaching_browser_smoke.cjs:
authenticate using the shared browser context request API before first page navigation.
Previously the unauthenticated first page emitted a legitimate /session 401 and caused
the browser-console assertion to fail. All console-error checks remain enabled.

## Validation

- web-app/tests: 702 tests, all pass (including Instagram and new sales-linked tests).
- tests: 104 tests, 96 pass; 7 failures and 1 error in test_reminder_email.py.
- An isolated, unmodified archive of c0ec2ff reproduces the same eight root-suite errors.
  They are pre-existing and were not changed as part of this integration.
- Desktop (1440x1000) and mobile (390x844) browser smoke tests pass.
- JavaScript syntax checks pass for sales_coaching.js and instagram-feed.js.
- git diff --check passes.
- Byte comparison against c0ec2ff confirms only the retained Instagram configuration /
  app registration and the described browser-test adjustment differ among target files.

Root-suite failing tests (same on baseline and integration):
- EmailInsightsEndpointTests.test_customer_insights_exposes_click_followup_fields (error)
- EmailPerformanceTests.test_invalid_order_rows_are_ignored_and_reused_references_stay_separate
- EmailPerformanceTests.test_report_counts_unique_stores_and_attributes_only_day_ten
- EmailPriorityScoringTests.test_automated_email_updates_latest_contact_but_preserves_human_signals
- ReminderSendRouteTests.test_draft_selects_template_link_and_order_mix_for_all_relationships
- ReminderSendRouteTests.test_hard_bounced_address_is_unselected_and_cannot_be_forced
- ReminderSendRouteTests.test_live_mode_uses_intended_addresses_and_creates_one_sales_activity
- TimelineAndWebhookTests.test_timeline_aggregates_events_and_attributes_day_ten_only_once

## Approval gate

Await user approval before merge/deploy. Proposed next steps: commit/push this integrated
branch, deploy its specific reviewed commit on the existing Render service without
merging PR #20, then verify live feed HTTP 200 with 18 items (12 UGC + 6 own when source
pools permit), static JS HTTP 200, health, CORS and CRM access protection.
Live checks of this new version cannot be completed before the approved deployment.
