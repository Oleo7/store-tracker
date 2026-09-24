# Repository test suite baseline and fixes

## Baseline

The baseline was run from a clean worktree at `71acf3bf0d5bd3b91036a37b69ee647f68876717`, fetched and confirmed as `origin/master`. The original user workspace was left untouched. All nine root-suite failures below reproduce on that unchanged commit.

| Command | Baseline result |
| --- | --- |
| `python -m unittest discover -s web-app/tests -p 'test_*.py'` | 784 tests, OK |
| `python -m unittest discover -s tests -p 'test_*.py'` | 104 tests, 8 failures, 1 error |
| `python -m unittest web-app.tests.test_sales_coaching_frontend` | 39 tests, OK |

The desktop and mobile Sales Coaching browser smoke tests and the guidance/delivery browser regression also passed. This Windows environment had no `npm` or Playwright-managed Chromium; the unchanged smoke scripts were run with the bundled Playwright package against the installed Microsoft Edge browser. Guidance/delivery passed at 320, 390, and 1440 px (15 scenarios per width).

## Root-suite failures on the unchanged baseline

Every baseline failure was in `tests/test_reminder_email.py`. The traceback locations and final assertion/error are recorded below; the omitted frames are the normal `unittest`/Flask dispatch frames between the test and its fixture or assertion.

| Test and relevant traceback | Baseline assertion/error | Classification and cause |
| --- | --- | --- |
| `EmailInsightsEndpointTests.test_customer_insights_exposes_click_followup_fields`, `tests/test_reminder_email.py:1438` -> `FakeSpreadsheet.worksheet`, `tests/test_reminder_email.py:103` | `KeyError: 'planned_activities'` | **3. Incorrect fixture/setup.** The endpoint now reads the planning activity and suggestion worksheets as part of its authoritative snapshot. The mock omitted those worksheets; after adding them, the test also needed an admin session to inspect both Sofia's and Olle's customers. |
| `EmailPerformanceTests.test_frontend_contains_filter_report_and_compact_timeline_rules`, `tests/test_reminder_email.py:1098` | `AssertionError: '<span class="card-date-primary">Leverans ${delivery}</span>' not found` | **2. Outdated test contract.** The UI now renders a computed delivery label and optional volume: `${deliveryLabel} ${delivery}${volumeText}`. |
| `EmailPerformanceTests.test_invalid_order_rows_are_ignored_and_reused_references_stay_separate`, `tests/test_reminder_email.py:1080` | `AssertionError: 0.0 != 5` | **2. Outdated test contract.** The fixture supplied `Quantity` and `Unit` as DFP. The current `commercial_orders.order_volume` contract deliberately reads `Total weight` and never infers DFP from `Quantity`. |
| `EmailPerformanceTests.test_report_counts_unique_stores_and_attributes_only_day_ten`, `tests/test_reminder_email.py:943` | `AssertionError: 0.0 != 3.0` | **2. Outdated test contract.** The order fixtures lacked the current `Total weight` field, so attribution correctly reported no DFP volume. |
| `EmailPriorityScoringTests.test_automated_email_updates_latest_contact_but_preserves_human_signals`, `tests/test_reminder_email.py:1151` | `AssertionError: 0 != 2` | **3. Incorrect fixture/setup.** The hard-coded July 2026 activities were 62 days old on the baseline date (2026-09-24), outside the 30-day count window. The test now dates activities relative to `date.today()`. |
| `ReminderSendRouteTests.test_draft_selects_template_link_and_order_mix_for_all_relationships`, `tests/test_reminder_email.py:1604` | `AssertionError: 'reactivation' != 'reminder'` | **3. Incorrect fixture/setup.** The test relied on the wall clock while its June 5 delivery fixture was 111 days old on the baseline date. The route fixture now pins Stockholm's date to 2026-07-22, the date its historical scenario was written for; the existing 60-day relationship rule is unchanged. |
| `ReminderSendRouteTests.test_hard_bounced_address_is_unselected_and_cannot_be_forced`, `tests/test_reminder_email.py:1850` | `AssertionError: True is not false` | **3. Incorrect fixture/setup.** The bounced recipient used `email_id="old"` without a matching `email_messages` row. The API scopes recipient history through its parent email message, so the orphan was correctly excluded. The fixture now includes the linked message. |
| `ReminderSendRouteTests.test_live_mode_uses_intended_addresses_and_creates_one_sales_activity`, `tests/test_reminder_email.py:1769` | Expected `Mejlförslag skickat – Påminnelse`; got `Mejlförslag skickat – Återaktivering` | **3. Incorrect fixture/setup.** Same implicit-wall-clock issue as the draft test; the pinned test date restores the intended recent-delivery case. |
| `TimelineAndWebhookTests.test_timeline_aggregates_events_and_attributes_day_ten_only_once`, `tests/test_reminder_email.py:746` | Expected `{'label': 'Antal DFP', 'value': '3'}`; details contained only order reference and order value | **2. Outdated test contract.** The fixture provided quantity but omitted `Total weight`, which the current shared order-volume contract uses. |

There were no baseline failures in the other root test files, and no failures classified as a production bug or an import/environment problem. A test intentionally logs an error when suggestion resolution is mocked to fail; that case passes and is not a test-suite error.

## Changes made

- Updated DFP fixtures to use the current `Total weight` data field. Production volume logic was not changed.
- Updated the HTML assertion to check the current dynamic delivery/volume markup.
- Completed the insights and hard-bounce fixtures with the worksheets, admin scope, and parent email message required by the current API.
- Made date-dependent scoring and proposal tests deterministic without changing their expected business rules.
- Added the repository root test command to `.github/workflows/sales-coaching.yml`, retaining the web-app, frontend, and browser checks.

No production code changed. The `superseded` terminal-history, scoring, suppression, planning, and Sales Coaching regression coverage remained untouched. Tests use in-memory fixtures; no live Google Sheet was written and no Render deployment was made.

## Verification after fixes

Final counts and commit identity are recorded in the task report. The required verification is run again from this clean branch after all edits: web-app Python tests, repository root tests, frontend contract tests, Python compile check, desktop and mobile smoke, and guidance/delivery browser regression.
