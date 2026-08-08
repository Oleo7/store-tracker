from datetime import date, datetime, time
import json
from pathlib import Path
import sys
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module  # noqa: E402
from planning_suggestions import SCORE_EVENTS_SHEET, SUGGESTIONS_SHEET  # noqa: E402
from test_planning import PlanningApiTestCase  # noqa: E402


class PlanningSuggestionV3IntegrationTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        self._cancel_customer("Butik C")

    def _cancel_customer(self, customer_name):
        sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = sheet.row_values(1)
        customer_col = headers.index("customer")
        cancelled_col = headers.index("cancelled_flag") + 1
        for row_index, row in enumerate(sheet.get_all_values()[1:], start=2):
            if row[customer_col] == customer_name:
                sheet.update_cell(row_index, cancelled_col, "Y")
                return

    @staticmethod
    def clock(day):
        instant = datetime.combine(
            day, time(9, 0), tzinfo=app_module.STOCKHOLM_ZONE
        )
        return (
            patch.object(app_module, "stockholm_today", return_value=day),
            patch.object(app_module, "stockholm_now", return_value=instant),
        )

    def _append_order(self, reference, delivered, quantity=20, sku="SKU-1"):
        sheet = self.spreadsheet.worksheet("order_rows")
        row = {
            "Reference": reference,
            "Order date": delivered,
            "Delivery date": delivered,
            "Customer": "Butik A",
            "Customer number": "C-1",
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "SKU": sku,
            "Product": f"Product {sku}",
            "Quantity": str(quantity),
            "Total weight": str(quantity),
            "Total": str(quantity * 20),
            "Currency": "SEK",
        }
        sheet.append_row([row.get(column, "") for column in app_module.ORDER_COLUMNS])

    def _append_contact(self, when, result="Positiv", contact_id="contact-v3"):
        sheet = self.spreadsheet.worksheet("sales_activities")
        row = {
            "contact_id": contact_id,
            "date_time": when,
            "sales_person": "Olle",
            "customer": "Butik A",
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "contact_channel": "Telefon",
            "result": result,
        }
        sheet.append_row([row.get(column, "") for column in app_module.CONTACT_COLUMNS])

    def test_live_trigger_and_plan_attribution_follow_current_candidate(self):
        self._append_order("FIRST-1", "2026-01-01", sku="ONLY-SKU")
        day_8 = date(2026, 1, 9)
        day_24 = date(2026, 1, 25)

        today_patch, now_patch = self.clock(day_8)
        with today_patch, now_patch:
            onboarding = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]
        self.assertEqual(onboarding["trigger_key"], "first_order_onboarding")

        today_patch, now_patch = self.clock(day_24)
        with today_patch, now_patch:
            current = self.client.get("/planning/suggestions").get_json()["suggestion"]
            self.assertEqual(current["suggestion_id"], onboarding["suggestion_id"])
            self.assertEqual(current["trigger_key"], "first_order_reorder")
            self.assertEqual(current["reason_text"], "Dags att följa upp andra ordern")
            planned = self.client.post(
                f"/planning/suggestions/{current['suggestion_id']}/plan",
                json={
                    "client_request_id": "plan-live-first-order-trigger",
                    "expected_suggestion_revision": current["revision"],
                    "customer_id": current["customer_id"],
                    "contact_type": "email",
                    "scheduled_at": "2026-01-26T09:00:00+01:00",
                    "note": "Följ upp andra ordern",
                },
            )

        self.assertEqual(planned.status_code, 201, planned.get_json())
        activity = self.planning_rows()[0]
        self.assertEqual(activity["source_trigger_key"], "first_order_reorder")
        self.assertEqual(activity["contact_type"], "email")
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["primary_trigger_key"], "first_order_onboarding")
        planned_event = next(
            row for row in self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
            if row["status_after"] == "planned"
        )
        self.assertEqual(planned_event["primary_trigger_key"], "first_order_reorder")

    def test_queue_sort_uses_score_value_precedence_then_stable_customer(self):
        candidates = [
            {"customer_id": "c", "customer_row": 4, "priority_score": 80,
             "expected_order_dfp": 20, "trigger_precedence": 3},
            {"customer_id": "b", "customer_row": 3, "priority_score": 80,
             "expected_order_dfp": 20, "trigger_precedence": 2},
            {"customer_id": "a", "customer_row": 2, "priority_score": 80,
             "expected_order_dfp": 30, "trigger_precedence": 3},
            {"customer_id": "d", "customer_row": 5, "priority_score": 90,
             "expected_order_dfp": 10, "trigger_precedence": 3},
        ]
        ordered = sorted(candidates, key=app_module.planning_suggestion_sort_key)
        self.assertEqual([item["customer_id"] for item in ordered], ["d", "a", "b", "c"])

    def test_dismissed_onboarding_suppresses_later_reorder_same_context(self):
        self._append_order("FIRST-1", "2026-01-01", sku="ONLY-SKU")
        day_8 = date(2026, 1, 9)
        day_24 = date(2026, 1, 25)

        today_patch, now_patch = self.clock(day_8)
        with today_patch, now_patch:
            onboarding = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]
            response = self.client.post(
                f"/planning/suggestions/{onboarding['suggestion_id']}/dismiss",
                json={
                    "client_request_id": "dismiss-first-order-context",
                    "expected_revision": onboarding["revision"],
                },
            )
            self.assertEqual(response.status_code, 200, response.get_json())

        today_patch, now_patch = self.clock(day_24)
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()
            candidate = app_module.planning_suggestion_candidates(
                self.spreadsheet, {"user_name": "olle", "name": "Olle"}
            )[0]

        self.assertIsNone(payload["suggestion"])
        row = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(row["status"], "dismissed")
        self.assertEqual(candidate["primary_trigger_key"], "first_order_reorder")
        expected_id = app_module.deterministic_suggestion_id(
            "olle", candidate["customer_id"], candidate["decision_context_hash"]
        )
        self.assertEqual(expected_id, onboarding["suggestion_id"])

    def test_second_order_resolves_first_order_context(self):
        self._append_order("FIRST-1", "2026-01-01", sku="ONLY-SKU")
        day_8 = date(2026, 1, 9)
        today_patch, now_patch = self.clock(day_8)
        with today_patch, now_patch:
            suggestion = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]

        self._append_order("SECOND-1", "2026-01-10", sku="SKU-2")
        today_patch, now_patch = self.clock(date(2026, 1, 10))
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()

        self.assertIsNone(payload["suggestion"])
        row = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(row["suggestion_id"], suggestion["suggestion_id"])
        self.assertEqual(row["status"], "resolved")
        self.assertEqual(row["resolved_by_type"], "business_context")

    def test_simultaneous_dialogue_and_strategic_signal_materialize_one_context(self):
        self._append_contact("2026-06-01 09:00:00", contact_id="warm-old")
        today = date(2026, 7, 20)
        today_patch, now_patch = self.clock(today)
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()

        suggestion = payload["suggestion"]
        self.assertEqual(payload["pending_count"], 1)
        self.assertEqual(suggestion["trigger_key"], "positive_dialogue_followup")
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            json.loads(rows[0]["covered_trigger_keys_json"]),
            ["positive_dialogue_followup", "strategic_contact_due"],
        )

        self._append_contact("2026-07-20 10:00:00", "Neutral", "later-contact")
        today_patch, now_patch = self.clock(today)
        with today_patch, now_patch:
            self.client.get("/planning/suggestions")
        old = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(old["status"], "resolved")
        self.assertEqual(old["resolved_by_type"], "business_context")
