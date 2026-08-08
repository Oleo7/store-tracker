from datetime import date, datetime, time
from pathlib import Path
import sys
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module  # noqa: E402
from planning_suggestions import (  # noqa: E402
    SCORE_EVENTS_SHEET,
    SUGGESTIONS_SHEET,
    deterministic_suggestion_id,
)
from test_planning import PlanningApiTestCase  # noqa: E402


class PlanningSuggestionV2IntegrationTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)

    def append_repeat_orders(self):
        sheet = self.spreadsheet.worksheet("order_rows")
        for reference, delivered, dfp in (
            ("V2-1", "2026-05-01", 20),
            ("V2-2", "2026-05-21", 30),
            ("V2-3", "2026-06-10", 40),
        ):
            row = {
                "Reference": reference,
                "Order date": delivered,
                "Delivery date": delivered,
                "Customer": "Butik A",
                "Customer number": "C-1",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "Quantity": str(dfp),
                "Total weight": str(dfp),
                "Total": str(dfp * 20),
                "Currency": "SEK",
            }
            sheet.append_row([
                row.get(column, "") for column in app_module.ORDER_COLUMNS
            ])

    def append_boundary_repeat_orders(self):
        sheet = self.spreadsheet.worksheet("order_rows")
        for reference, delivered, dfp in (
            ("BOUNDARY-1", "2026-01-01", 20),
            ("BOUNDARY-2", "2026-01-21", 30),
            ("BOUNDARY-3", "2026-02-10", 40),
        ):
            row = {
                "Reference": reference,
                "Order date": delivered,
                "Delivery date": delivered,
                "Customer": "Butik A",
                "Customer number": "C-1",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "Quantity": str(dfp),
                "Total weight": str(dfp),
                "Total": str(dfp * 20),
                "Currency": "SEK",
            }
            sheet.append_row([
                row.get(column, "") for column in app_module.ORDER_COLUMNS
            ])

    @staticmethod
    def clock(day):
        instant = datetime.combine(
            day, time(9, 0), tzinfo=app_module.STOCKHOLM_ZONE
        )
        return (
            patch.object(app_module, "stockholm_today", return_value=day),
            patch.object(app_module, "stockholm_now", return_value=instant),
        )

    def test_overdue_established_customer_materializes_one_v2_card(self):
        self.append_repeat_orders()

        response = self.client.get("/planning/suggestions")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200, payload)
        self.assertEqual(payload["score_version"], "v2")
        self.assertGreaterEqual(payload["pending_count"], 1)
        self.assertEqual(payload["suggestion"]["customer"], "Butik A")
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["primary_trigger_type"], "established_reorder_due")
        self.assertEqual(rows[0]["score_version"], "v2")
        event = self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()[0]
        self.assertEqual(event["score_version"], "v2")
        self.assertEqual(event["lifecycle"], "established")
        self.assertEqual(event["recommendation_eligible"], "Y")
        for field in (
            "priority_score", "intent_timing", "value_index",
            "strategic_index", "expected_order_dfp",
        ):
            self.assertNotEqual(event[field], "")

    def test_future_manual_activity_suppresses_card_but_not_priority_customer(self):
        self.append_repeat_orders()
        self.append_planning_row(
            customer_row=2,
            scheduled_at="2026-07-30T09:00:00+02:00",
            source="manual",
            client_request_id="v2-future-manual",
        )

        response = self.client.get("/planning/suggestions")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200, payload)
        self.assertNotEqual(
            (payload.get("suggestion") or {}).get("customer"), "Butik A"
        )
        priorities, _ = app_module.build_current_priority_snapshot(
            customers=app_module.get_customer_rows(self.spreadsheet),
            order_rows=app_module.get_order_rows(self.spreadsheet),
            contact_rows=app_module.get_contact_rows(self.spreadsheet),
            message_rows=[],
            recipient_rows=[],
            today=app_module.stockholm_today(),
            planned_activity_rows=[row for row in self.planning_rows()],
        )
        customer = next(item for item in priorities if item["customer"] == "Butik A")
        self.assertFalse(customer["recommendation_eligible"])
        self.assertGreater(customer["priority_score"], 0)

    def test_new_order_resolves_old_context_without_materializing_ineligible_context(self):
        self.append_repeat_orders()
        first = self.client.get("/planning/suggestions").get_json()["suggestion"]
        sheet = self.spreadsheet.worksheet("order_rows")
        row = {
            "Reference": "V2-NEW",
            "Order date": "2026-07-27",
            "Delivery date": "2026-07-27",
            "Customer": "Butik A",
            "Customer number": "C-1",
            "customer_id": first["customer_id"],
            "Quantity": "35",
            "Total weight": "35",
            "Total": "700",
            "Currency": "SEK",
        }
        sheet.append_row([row.get(column, "") for column in app_module.ORDER_COLUMNS])

        payload = self.client.get("/planning/suggestions").get_json()

        self.assertNotEqual(
            (payload.get("suggestion") or {}).get("customer_id"),
            first["customer_id"],
        )
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        old = next(
            row for row in rows if row["suggestion_id"] == first["suggestion_id"]
        )
        self.assertEqual(old["status"], "resolved")
        self.assertEqual(old["resolved_by_type"], "business_context")

    def test_snoozed_context_is_noneligible_but_remains_in_customer_insights(self):
        self.append_repeat_orders()
        suggestion = self.client.get("/planning/suggestions").get_json()["suggestion"]
        snoozed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "v2-snooze-insights",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())

        insights = self.client.get("/customer-insights").get_json()

        customer = insights["butik a"]
        self.assertFalse(customer["recommendation_eligible"])
        self.assertEqual(customer["recommendation_suppression_reason"], "snoozed")
        self.assertGreater(customer["priority_score"], 0)

    def test_pending_context_is_not_closed_when_only_time_crosses_day_90(self):
        self.append_boundary_repeat_orders()
        day_90 = date(2026, 5, 31)
        day_91 = date(2026, 6, 1)
        today_patch, now_patch = self.clock(day_90)
        with today_patch, now_patch:
            suggestion = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]

        today_patch, now_patch = self.clock(day_91)
        with today_patch, now_patch:
            payload = self.client.get("/planning/suggestions").get_json()

        self.assertEqual(
            payload["suggestion"]["suggestion_id"], suggestion["suggestion_id"]
        )
        self.assertEqual(
            payload["suggestion"]["trigger_key"], "strategic_contact_due"
        )
        row = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(row["suggestion_id"], suggestion["suggestion_id"])
        self.assertEqual(row["status"], "pending")
        self.assertEqual(row["resolved_by_type"], "")

    def test_dismissed_context_still_suppresses_after_live_reactivation(self):
        self.append_boundary_repeat_orders()
        day_90 = date(2026, 5, 31)
        day_91 = date(2026, 6, 1)
        today_patch, now_patch = self.clock(day_90)
        with today_patch, now_patch:
            suggestion = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]
            dismissed = self.client.post(
                f"/planning/suggestions/{suggestion['suggestion_id']}/dismiss",
                json={
                    "client_request_id": "dismiss-across-reactivation",
                    "expected_revision": suggestion["revision"],
                },
            )
            self.assertEqual(dismissed.status_code, 200, dismissed.get_json())

        today_patch, now_patch = self.clock(day_91)
        with today_patch, now_patch:
            insights = self.client.get("/customer-insights").get_json()

        customer = insights["butik a"]
        self.assertEqual(customer["lifecycle"], "reactivation")
        self.assertFalse(customer["recommendation_eligible"])
        self.assertEqual(
            customer["recommendation_suppression_reason"], "dismissed"
        )

    def test_snoozed_context_keeps_suggestion_id_after_live_reactivation(self):
        self.append_boundary_repeat_orders()
        day_90 = date(2026, 5, 31)
        day_91 = date(2026, 6, 1)
        today_patch, now_patch = self.clock(day_90)
        with today_patch, now_patch:
            suggestion = self.client.get(
                "/planning/suggestions"
            ).get_json()["suggestion"]
            snoozed = self.client.post(
                f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
                json={
                    "client_request_id": "snooze-across-reactivation",
                    "expected_revision": suggestion["revision"],
                },
            )
            self.assertEqual(snoozed.status_code, 200, snoozed.get_json())

        today_patch, now_patch = self.clock(day_91)
        with today_patch, now_patch:
            owner = {"user_name": "olle", "name": "Olle"}
            candidate = next(
                item for item in app_module.planning_suggestion_candidates(
                    self.spreadsheet, owner
                )
                if item["customer_id"] == suggestion["customer_id"]
            )
            day_91_id = deterministic_suggestion_id(
                owner["user_name"], candidate["customer_id"],
                candidate["decision_context_hash"],
            )
            self.client.get("/planning/suggestions")

        row = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(day_91_id, suggestion["suggestion_id"])
        self.assertEqual(row["suggestion_id"], suggestion["suggestion_id"])
        self.assertEqual(row["status"], "snoozed")
