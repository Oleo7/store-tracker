from datetime import date, datetime, time
from pathlib import Path
import sys
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module  # noqa: E402
from planning_suggestions import SUGGESTIONS_SHEET  # noqa: E402
from test_planning import PlanningApiTestCase  # noqa: E402


class PlanningSuggestionV4IntegrationTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        customer_sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = customer_sheet.row_values(1)
        customer_col = headers.index("customer")
        cancelled_col = headers.index("cancelled_flag") + 1
        segment_col = headers.index("customer_segment") + 1
        for row_index, row in enumerate(customer_sheet.get_all_values()[1:], start=2):
            if row[customer_col] == "Butik A":
                customer_sheet.update_cell(row_index, segment_col, "C")
            else:
                customer_sheet.update_cell(row_index, cancelled_col, "Y")

    @staticmethod
    def clock(day):
        instant = datetime.combine(day, time(9, 0), tzinfo=app_module.STOCKHOLM_ZONE)
        return (
            patch.object(app_module, "stockholm_today", return_value=day),
            patch.object(app_module, "stockholm_now", return_value=instant),
        )

    def append_click(
        self, kind="stockfiller", *, email_id="email-v4",
        sent_at="2026-08-01 09:00:00", clicked_at="2026-08-02 08:00:00"
    ):
        app_module.ensure_email_worksheets(self.spreadsheet, include_events=False)
        message = {
            "email_id": email_id, "customer_id": "11111111-1111-4111-8111-111111111111",
            "customer_number": "C-1", "customer": "Butik A",
            "sent_at": sent_at, "status": "sent", "is_test": "N",
        }
        recipient = {
            "email_id": email_id, "send_status": "sent",
            "intended_email": "buyer@example.com", "actual_email": "buyer@example.com",
        }
        prefix = "stockfiller" if kind == "stockfiller" else "product_sheet"
        recipient[f"{prefix}_click_count"] = "1"
        recipient[f"{prefix}_first_clicked_at"] = clicked_at
        recipient[f"{prefix}_last_clicked_at"] = clicked_at
        self.spreadsheet.worksheet(app_module.EMAIL_MESSAGES_SHEET).append_row(
            [message.get(column, "") for column in app_module.EMAIL_MESSAGES_COLUMNS]
        )
        self.spreadsheet.worksheet(app_module.EMAIL_RECIPIENTS_SHEET).append_row(
            [recipient.get(column, "") for column in app_module.EMAIL_RECIPIENTS_COLUMNS]
        )

    def append_order(self, reference, delivered):
        row = {
            "Reference": reference, "Order date": delivered, "Delivery date": delivered,
            "Customer": "Butik A", "Customer number": "C-1",
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "Quantity": "20", "Total weight": "20", "Total": "400",
        }
        self.spreadsheet.worksheet("order_rows").append_row(
            [row.get(column, "") for column in app_module.ORDER_COLUMNS]
        )

    def append_legacy_followup(self):
        row = {
            "contact_id": "legacy-source", "date_time": "2026-07-01 10:00:00",
            "sales_person": "Olle", "customer": "Butik A",
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "contact_channel": "Telefon", "result": "Neutral",
            "follow_up_date": "2026-07-10",
        }
        self.spreadsheet.worksheet("sales_activities").append_row(
            [row.get(column, "") for column in app_module.CONTACT_COLUMNS]
        )

    def test_click_materializes_one_stable_context_and_dismiss_is_terminal(self):
        self.append_click()
        today_patch, now_patch = self.clock(date(2026, 8, 5))
        with today_patch, now_patch:
            first = self.client.get("/planning/suggestions").get_json()["suggestion"]
            dismissed = self.client.post(
                f"/planning/suggestions/{first['suggestion_id']}/dismiss",
                json={"client_request_id": "dismiss-v4-click", "expected_revision": first["revision"]},
            )
        self.assertEqual(first["trigger_key"], "stockfiller_click_followup")
        self.assertEqual(dismissed.status_code, 200, dismissed.get_json())

        today_patch, now_patch = self.clock(date(2026, 8, 20))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
            candidates = app_module.planning_suggestion_candidates(
                self.spreadsheet, {"user_name": "olle", "name": "Olle"}
            )
        self.assertIsNone(payload["suggestion"])
        self.assertEqual(len(candidates), 1)
        expected_id = app_module.deterministic_suggestion_id(
            "olle", candidates[0]["customer_id"], candidates[0]["decision_context_hash"]
        )
        self.assertEqual(expected_id, first["suggestion_id"])

    def test_later_order_resolves_click_context(self):
        self.append_click()
        today_patch, now_patch = self.clock(date(2026, 8, 5))
        with today_patch, now_patch:
            suggestion = self.client.get("/planning/suggestions").get_json()["suggestion"]
        self.append_order("ORDER-AFTER", "2026-08-06")
        today_patch, now_patch = self.clock(date(2026, 8, 6))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
        self.assertIsNone(payload["suggestion"])
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["suggestion_id"], suggestion["suggestion_id"])
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "business_context")

    def test_new_click_creates_new_context_after_old_click_was_dismissed(self):
        self.append_click()
        today_patch, now_patch = self.clock(date(2026, 8, 5))
        with today_patch, now_patch:
            first = self.client.get("/planning/suggestions").get_json()["suggestion"]
            self.client.post(
                f"/planning/suggestions/{first['suggestion_id']}/dismiss",
                json={"client_request_id": "dismiss-first-click", "expected_revision": first["revision"]},
            )
        self.append_click(
            email_id="email-v4-2", sent_at="2026-08-06 09:00:00",
            clicked_at="2026-08-06 10:00:00",
        )
        today_patch, now_patch = self.clock(date(2026, 8, 9))
        with today_patch, now_patch:
            second = self.client.get("/planning/suggestions").get_json()["suggestion"]
        self.assertEqual(second["trigger_key"], "stockfiller_click_followup")
        self.assertNotEqual(second["suggestion_id"], first["suggestion_id"])

    def test_legacy_followup_is_suggestion_not_planned_activity(self):
        self.append_legacy_followup()
        today_patch, now_patch = self.clock(date(2026, 8, 2))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
        self.assertEqual(payload["suggestion"]["trigger_key"], "legacy_missed_followup")
        self.assertEqual(self.planning_rows(), [])

    def test_dismissed_legacy_context_does_not_hide_later_email_business_event(self):
        self.append_legacy_followup()
        today_patch, now_patch = self.clock(date(2026, 8, 4))
        with today_patch, now_patch:
            waiting = self.client.get("/planning/suggestions").get_json()["suggestion"]
            response = self.client.post(
                f"/planning/suggestions/{waiting['suggestion_id']}/dismiss",
                json={
                    "client_request_id": "dismiss-legacy-wait",
                    "expected_revision": waiting["revision"],
                },
            )
        self.assertEqual(waiting["trigger_key"], "legacy_missed_followup")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.append_click()

        today_patch, now_patch = self.clock(date(2026, 8, 5))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
            candidate = app_module.planning_suggestion_candidates(
                self.spreadsheet, {"user_name": "olle", "name": "Olle"}
            )[0]
        self.assertEqual(payload["suggestion"]["trigger_key"], "stockfiller_click_followup")
        self.assertEqual(candidate["primary_trigger_key"], "stockfiller_click_followup")
        self.assertEqual(
            app_module.deterministic_suggestion_id(
                "olle", candidate["customer_id"], candidate["decision_context_hash"]
            ),
            payload["suggestion"]["suggestion_id"],
        )
        self.assertNotEqual(payload["suggestion"]["suggestion_id"], waiting["suggestion_id"])

    def test_snoozed_legacy_context_does_not_hide_later_email_business_event(self):
        self.append_legacy_followup()
        today_patch, now_patch = self.clock(date(2026, 8, 4))
        with today_patch, now_patch:
            waiting = self.client.get("/planning/suggestions").get_json()["suggestion"]
            response = self.client.post(
                f"/planning/suggestions/{waiting['suggestion_id']}/snooze",
                json={
                    "client_request_id": "snooze-legacy-wait",
                    "expected_revision": waiting["revision"],
                },
            )
        self.assertEqual(response.status_code, 200, response.get_json())
        self.append_click()

        today_patch, now_patch = self.clock(date(2026, 8, 5))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
            candidate = app_module.planning_suggestion_candidates(
                self.spreadsheet, {"user_name": "olle", "name": "Olle"}
            )[0]
        self.assertEqual(payload["suggestion"]["trigger_key"], "stockfiller_click_followup")
        self.assertEqual(candidate["primary_trigger_key"], "stockfiller_click_followup")
        self.assertEqual(
            app_module.deterministic_suggestion_id(
                "olle", candidate["customer_id"], candidate["decision_context_hash"]
            ),
            payload["suggestion"]["suggestion_id"],
        )
        self.assertNotEqual(payload["suggestion"]["suggestion_id"], waiting["suggestion_id"])

    def test_empty_calibration_export_is_read_only(self):
        before = set(self.spreadsheet.sheets)
        response = self.client.get("/planning/calibration-export")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["rows"], [])
        self.assertEqual(set(self.spreadsheet.sheets), before)
