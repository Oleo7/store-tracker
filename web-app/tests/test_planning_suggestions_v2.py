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

    def test_production_owner_identity_materializes_only_johans_queue(self):
        user = {
            "user_name": "Johan",
            "name": "Johan Persson",
            "role": "Säljare",
            "email": "johan@example.com",
            "password": "secret",
            "active": "Y",
            "admin": "N",
        }
        users = self.spreadsheet.worksheet(app_module.USERS_SHEET)
        users.append_row([user.get(column, "") for column in app_module.USER_COLUMNS])

        customers = self.spreadsheet.worksheet("customers_enriched")
        sales_person_column = customers.values[0].index("sales_person") + 1
        customers.update_cell(2, sales_person_column, "Johan")
        self.append_repeat_orders()

        self.login("Johan")
        response = self.client.get("/planning/suggestions")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200, payload)
        self.assertEqual(payload["pending_count"], 1)
        self.assertEqual(payload["suggestion"]["customer"], "Butik A")
        owner = {"user_name": "Johan", "name": "Johan Persson"}
        candidates = app_module.planning_suggestion_candidates(
            self.spreadsheet, owner
        )
        self.assertEqual(
            [candidate["customer_id"] for candidate in candidates],
            ["11111111-1111-4111-8111-111111111111"],
        )

        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["user_name"], "Johan")
        self.assertEqual(rows[0]["sales_person"], "Johan Persson")

        self.login("admin")
        admin_response = self.client.get("/planning/suggestions?owner=Johan")
        admin_payload = admin_response.get_json()
        self.assertEqual(admin_response.status_code, 200, admin_payload)
        self.assertEqual(admin_payload["pending_count"], 1)
        self.assertEqual(
            admin_payload["suggestion"]["suggestion_id"],
            payload["suggestion"]["suggestion_id"],
        )

    def test_candidate_scoring_uses_global_universe_for_all_seller_shapes(self):
        seller_shapes = (
            ("Johan", "Johan Persson"),
            ("Daniel", "Daniel Andersson"),
            ("Sofia", "Sofia Andersson"),
            ("Olle", "Olle Rönningberg"),
        )
        for user_name, name in seller_shapes:
            with self.subTest(user_name=user_name):
                with patch.object(
                    app_module,
                    "build_current_priority_snapshot",
                    return_value=([], {}),
                ) as build_snapshot:
                    app_module.planning_suggestion_candidates(
                        self.spreadsheet,
                        {"user_name": user_name, "name": name},
                    )
                self.assertNotIn(
                    "responsible", build_snapshot.call_args.kwargs
                )

    @staticmethod
    def clock(day):
        instant = datetime.combine(
            day, time(9, 0), tzinfo=app_module.STOCKHOLM_ZONE
        )
        return (
            patch.object(app_module, "stockholm_today", return_value=day),
            patch.object(app_module, "stockholm_now", return_value=instant),
        )

    def materialize_boundary_customer(self):
        owner = {"user_name": "olle", "name": "Olle"}
        candidate = next(
            item for item in app_module.planning_suggestion_candidates(
                self.spreadsheet, owner
            )
            if item["customer_id"] == "11111111-1111-4111-8111-111111111111"
        )
        row, _created = app_module.planning_suggestion_service(
            self.spreadsheet
        ).materialize_candidate(owner, candidate)
        return app_module.public_suggestion(row, candidate)

    def test_overdue_established_customer_materializes_one_v2_card(self):
        self.append_repeat_orders()

        response = self.client.get("/planning/suggestions")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200, payload)
        self.assertEqual(payload["score_version"], "v2.1")
        self.assertGreaterEqual(payload["pending_count"], 1)
        self.assertEqual(payload["suggestion"]["customer"], "Butik A")
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["primary_trigger_type"], "established_reorder_due")
        self.assertEqual(rows[0]["score_version"], "v2.1")
        event = self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()[0]
        self.assertEqual(event["score_version"], "v2.1")
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

    def test_cached_snapshot_does_not_reuse_empty_planning_input_for_queue(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()
        self.append_repeat_orders()
        customer_id = "11111111-1111-4111-8111-111111111111"
        self.append_planning_row(
            customer_row=2,
            scheduled_at="2026-07-28T09:00:00+02:00",
            source="manual",
            client_request_id="cached-planning-input",
        )
        try:
            # A prior caller can legitimately supply no planning rows. Its
            # cached scoring result must not be reused by the planner's live
            # activity snapshot for the same spreadsheet and day.
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet,
                today=app_module.stockholm_today(),
                planned_activity_rows=(),
            )
            response = self.client.get("/planning/suggestions")
            payload = response.get_json()

            self.assertEqual(response.status_code, 200, payload)
            self.assertNotEqual(
                (payload.get("suggestion") or {}).get("customer_id"),
                customer_id,
            )
            self.assertNotIn(
                customer_id,
                [item["customer_id"] for item in payload["queue_preview"]],
            )
        finally:
            app_module._sheet_read_cache.clear()
            app_module.invalidate_priority_snapshot()

    def test_future_activity_becomes_one_overdue_queue_item_at_exact_time(self):
        self.append_repeat_orders()
        activity = self.append_planning_row(
            planned_activity_id="manual-same-day-overdue",
            scheduled_at="2026-07-27T09:00:00+02:00",
            source="manual",
        )
        before = datetime(2026, 7, 27, 8, 59, tzinfo=app_module.STOCKHOLM_ZONE)
        after = datetime(2026, 7, 27, 9, 1, tzinfo=app_module.STOCKHOLM_ZONE)

        with patch.object(app_module, "stockholm_now", return_value=before):
            before_payload = self.client.get("/planning/suggestions").get_json()
        before_items = [before_payload.get("suggestion")] + before_payload["queue_preview"]
        self.assertFalse(any(
            item and item.get("customer_id") == activity["customer_id"]
            for item in before_items
        ))

        with patch.object(app_module, "stockholm_now", return_value=after):
            after_payload = self.client.get("/planning/suggestions").get_json()
        after_items = [after_payload.get("suggestion")] + after_payload["queue_preview"]
        customer_items = [
            item for item in after_items
            if item and item.get("customer_id") == activity["customer_id"]
        ]
        self.assertEqual(len(customer_items), 1)
        self.assertEqual(customer_items[0]["queue_item_type"], "overdue_activity")
        self.assertEqual(
            customer_items[0]["planned_activity_id"],
            activity["planned_activity_id"],
        )
        self.assertEqual(self.planning_rows()[0]["status"], "planned")

    def test_suggestion_planned_activity_uses_same_overdue_semantics(self):
        self.append_repeat_orders()
        before = datetime(2026, 7, 27, 8, 59, tzinfo=app_module.STOCKHOLM_ZONE)
        after = datetime(2026, 7, 27, 9, 1, tzinfo=app_module.STOCKHOLM_ZONE)
        with patch.object(app_module, "stockholm_now", return_value=before):
            suggestion = self.client.get("/planning/suggestions").get_json()["suggestion"]
            planned = self.client.post(
                f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
                json={
                    "client_request_id": "system-suggestion-same-day-overdue",
                    "expected_suggestion_revision": suggestion["revision"],
                    "customer_id": suggestion["customer_id"],
                    "contact_type": "phone",
                    "scheduled_at": "2026-07-27T09:00:00+02:00",
                    "note": "Ring kunden",
                },
            )
            self.assertEqual(planned.status_code, 201, planned.get_json())
            activity_id = planned.get_json()["activity"]["planned_activity_id"]
            hidden = self.client.get("/planning/suggestions").get_json()
        hidden_items = [hidden.get("suggestion")] + hidden["queue_preview"]
        self.assertFalse(any(
            item and item.get("customer_id") == suggestion["customer_id"]
            for item in hidden_items
        ))

        with patch.object(app_module, "stockholm_now", return_value=after):
            overdue = self.client.get("/planning/suggestions").get_json()
        overdue_items = [overdue.get("suggestion")] + overdue["queue_preview"]
        customer_items = [
            item for item in overdue_items
            if item and item.get("customer_id") == suggestion["customer_id"]
        ]
        self.assertEqual(len(customer_items), 1)
        self.assertEqual(customer_items[0]["queue_item_type"], "overdue_activity")
        self.assertEqual(customer_items[0]["planned_activity_id"], activity_id)
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["status"], "planned")
        self.assertEqual(stored["planned_activity_id"], activity_id)

    def test_priority_cache_identity_changes_at_scheduled_at_transition(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()
        self.append_repeat_orders()
        activity = self.append_planning_row(
            planned_activity_id="cached-same-day-overdue",
            scheduled_at="2026-07-27T09:00:00+02:00",
            source="manual",
        )
        before = datetime(2026, 7, 27, 8, 59, tzinfo=app_module.STOCKHOLM_ZONE)
        after = datetime(2026, 7, 27, 9, 1, tzinfo=app_module.STOCKHOLM_ZONE)
        original = app_module.build_current_priority_snapshot
        try:
            with patch.object(
                app_module, "build_current_priority_snapshot", wraps=original
            ) as build_snapshot:
                with patch.object(app_module, "stockholm_now", return_value=before):
                    self.client.get("/planning/suggestions")
                with patch.object(app_module, "stockholm_now", return_value=after):
                    payload = self.client.get("/planning/suggestions").get_json()
            self.assertEqual(build_snapshot.call_count, 2)
            self.assertEqual(payload["suggestion"]["queue_item_type"], "overdue_activity")
            self.assertEqual(
                payload["suggestion"]["planned_activity_id"],
                activity["planned_activity_id"],
            )
        finally:
            app_module._sheet_read_cache.clear()
            app_module.invalidate_priority_snapshot()

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
            suggestion = self.materialize_boundary_customer()

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
            suggestion = self.materialize_boundary_customer()
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
            suggestion = self.materialize_boundary_customer()
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
