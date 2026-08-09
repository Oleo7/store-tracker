from datetime import date
from pathlib import Path
import sys
import threading
from unittest import TestCase
from unittest.mock import patch

WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module
from contact_channel import normalized_phone, recommend_contact_channel
from planning_suggestions import SCORE_EVENTS_SHEET, SUGGESTIONS_SHEET
from priority import SCORE_VERSION, established_intent_timing
from sheets_availability import SheetReadCache
from test_planning import PlanningApiTestCase


class ContactChannelV21Tests(TestCase):
    def test_business_precedence_and_contactability_fallback(self):
        cases = [
            ({"lifecycle": "prospect", "has_human_contact": False}, "visit"),
            ({"lifecycle": "prospect", "has_human_contact": True}, "phone"),
            ({"lifecycle": "prospect", "trigger_key": "stockfiller_click_followup"}, "phone"),
            ({"lifecycle": "prospect", "trigger_key": "product_sheet_click_followup"}, "phone"),
            ({"lifecycle": "established", "overdue_days": 30}, "phone"),
            ({"lifecycle": "established", "overdue_days": 31}, "visit"),
            ({"lifecycle": "established", "overdue_days": 60}, "visit"),
            ({"lifecycle": "established", "overdue_days": 61}, "visit"),
            ({"lifecycle": "reactivation"}, "visit"),
            ({"lifecycle": "first_order"}, "phone"),
        ]
        for fields, expected in cases:
            with self.subTest(fields=fields):
                result = recommend_contact_channel(
                    phone="+46 (70) 123-45-67", email_available=True,
                    **fields,
                )
                self.assertEqual(result["recommended_contact_type"], expected)

    def test_phone_base_falls_back_to_email_then_visit(self):
        email = recommend_contact_channel(
            lifecycle="first_order", phone="saknas", email_available=True
        )
        visit = recommend_contact_channel(
            lifecycle="first_order", phone="saknas", email_available=False
        )
        self.assertEqual(email["recommended_contact_type"], "email")
        self.assertFalse(email["can_call"])
        self.assertEqual(visit["recommended_contact_type"], "visit")
        self.assertFalse(visit["can_call"])

    def test_visit_base_never_falls_back_and_phone_is_tel_safe(self):
        result = recommend_contact_channel(
            lifecycle="reactivation", phone="070-123 45 67", email_available=True
        )
        self.assertEqual(result["recommended_contact_type"], "visit")
        self.assertTrue(result["can_call"])
        self.assertEqual(result["phone_tel"], "0701234567")
        for invalid in ("123", "+46+701234567", "070 CALL ME", "070/1234567"):
            with self.subTest(invalid=invalid):
                self.assertEqual(normalized_phone(invalid), "")


class ActionQueueV21ApiTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True

    def tearDown(self):
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        super().tearDown()

    def queue(self, query=""):
        response = self.client.get(f"/planning/suggestions{query}")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def test_top_and_preview_share_one_sparse_queue(self):
        payload = self.queue()
        self.assertEqual(payload["pending_count"], 2)
        self.assertEqual(payload["preview_limit"], 10)
        self.assertEqual(len(payload["queue_preview"]), 1)
        self.assertNotEqual(
            payload["suggestion"]["suggestion_id"],
            payload["queue_preview"][0]["suggestion_id"],
        )
        self.assertTrue(payload["suggestion"]["materialized"])
        self.assertFalse(payload["queue_preview"][0]["materialized"])
        self.assertEqual(payload["queue_preview"][0]["revision"], 0)
        self.assertEqual(
            len(self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()), 1
        )
        created = [
            row for row in self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
            if row["event_type"] == "suggestion_created"
        ]
        self.assertEqual(len(created), 1)

    def test_ten_preview_items_create_no_extra_rows_or_events(self):
        sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = sheet.row_values(1)
        for index in range(10):
            row = {
                "customer": f"Extra butik {index}",
                "customer_id": f"extra-customer-{index}",
                "customer_number": f"X-{index}",
                "sales_person": "Olle",
                "customer_segment": "C",
            }
            sheet.append_row([row.get(column, "") for column in headers])
        payload = self.queue()
        self.assertEqual(payload["pending_count"], 12)
        self.assertEqual(len(payload["queue_preview"]), 10)
        self.assertEqual(
            len(self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()), 1
        )
        created = [
            row for row in self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
            if row["event_type"] == "suggestion_created"
        ]
        self.assertEqual(len(created), 1)

    def test_recommendation_queue_never_invokes_route_or_external_geo(self):
        with patch.object(
            app_module,
            "GoogleRoutesTravelTimeProvider",
            side_effect=AssertionError("recommendations must not use Routes"),
        ), patch.object(
            app_module.requests,
            "get",
            side_effect=AssertionError("recommendations must not geocode"),
        ):
            payload = self.queue()
        self.assertIsNotNone(payload["suggestion"])

    def test_preview_plan_materializes_only_selected_item_and_logs_live_vs_actual(self):
        payload = self.queue()
        preview = payload["queue_preview"][0]
        response = self.client.post(
            f"/planning/suggestions/{preview['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-preview-v21",
                "expected_suggestion_revision": 0,
                "customer_id": preview["customer_id"],
                "contact_type": "email",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "Vald kanal skiljer sig",
            },
        )
        self.assertEqual(response.status_code, 201, response.get_json())
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 2)
        selected = next(row for row in rows if row["suggestion_id"] == preview["suggestion_id"])
        self.assertEqual(selected["status"], "planned")
        activity = self.planning_rows()[0]
        self.assertEqual(activity["contact_type"], "email")
        self.assertEqual(
            activity["recommended_contact_type"],
            preview["recommended_contact_type"],
        )
        planned_event = next(
            row for row in self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
            if row["event_type"] == "suggestion_planned"
            and row["suggestion_id"] == preview["suggestion_id"]
        )
        self.assertEqual(planned_event["actual_planned_contact_type"], "email")
        self.assertEqual(
            planned_event["recommended_contact_type"],
            preview["recommended_contact_type"],
        )
        result = response.get_json()
        self.assertEqual(result["pending_count"], 1)
        self.assertEqual(result["queue_preview"], [])

    def test_preview_plan_allows_rank_change_with_same_context(self):
        preview = self.queue()["queue_preview"][0]
        owner = {"user_name": "olle", "name": "Olle"}
        candidate = app_module.suggestion_candidates_by_id(
            owner,
            app_module.planning_suggestion_candidates(self.spreadsheet, owner),
        )[preview["suggestion_id"]]
        changed_rank = {**candidate, "priority_score": 1}
        with patch.object(
            app_module, "planning_suggestion_candidates",
            return_value=[changed_rank],
        ):
            response = self.client.post(
                f"/planning/suggestions/{preview['suggestion_id']}/plan",
                json={
                    "client_request_id": "plan-preview-after-rank-change",
                    "expected_suggestion_revision": 0,
                    "customer_id": preview["customer_id"],
                    "contact_type": "visit",
                    "scheduled_at": "2026-07-30T09:00:00+02:00",
                },
            )
        self.assertEqual(response.status_code, 201, response.get_json())
        self.assertEqual(len(self.planning_rows()), 1)

    def test_preview_context_change_and_owner_mismatch_are_stale(self):
        preview = self.queue()["queue_preview"][0]
        self.append_contact_row(
            customer="Butik C",
            customer_id=preview["customer_id"],
            contact_id="new-context-for-preview",
            follow_up_date="",
        )
        stale = self.client.post(
            f"/planning/suggestions/{preview['suggestion_id']}/plan",
            json={
                "client_request_id": "stale-preview-context",
                "expected_suggestion_revision": 0,
                "customer_id": preview["customer_id"],
                "contact_type": "visit",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
            },
        )
        self.assertEqual(stale.status_code, 409)
        self.assertEqual(stale.get_json()["code"], "suggestion_stale")

        self.login("admin")
        olle_preview = self.queue("?user_name=olle")["queue_preview"][0]
        wrong_owner = self.client.post(
            f"/planning/suggestions/{olle_preview['suggestion_id']}/plan",
            json={
                "client_request_id": "wrong-preview-owner",
                "expected_suggestion_revision": 0,
                "customer_id": olle_preview["customer_id"],
                "user_name": "sofia",
                "contact_type": "visit",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
            },
        )
        self.assertEqual(wrong_owner.status_code, 409)
        self.assertEqual(wrong_owner.get_json()["code"], "suggestion_stale")

    def test_revision_zero_cannot_plan_terminal_persisted_preview(self):
        preview = self.queue()["queue_preview"][0]
        owner = {"user_name": "olle", "name": "Olle"}
        candidate = app_module.suggestion_candidates_by_id(
            owner,
            app_module.planning_suggestion_candidates(self.spreadsheet, owner),
        )[preview["suggestion_id"]]
        row, _created = app_module.planning_suggestion_service(
            self.spreadsheet
        ).materialize_candidate(owner, candidate)
        app_module.planning_suggestion_service(self.spreadsheet).transition(
            preview["suggestion_id"],
            owner_name="olle",
            action="dismiss",
            expected_revision=1,
            request_id="terminal-preview-dismiss",
            fingerprint=app_module.suggestion_mutation_fingerprint(
                "dismiss", preview["suggestion_id"], "terminal-preview-dismiss"
            ),
        )
        response = self.client.post(
            f"/planning/suggestions/{preview['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-terminal-preview",
                "expected_suggestion_revision": 0,
                "customer_id": preview["customer_id"],
                "contact_type": "visit",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
            },
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "suggestion_not_pending")
        self.assertEqual(self.planning_rows(), [])

    def test_manual_future_activity_is_reversible_suppression(self):
        top = self.queue()["suggestion"]
        activity = self.append_planning_row(
            customer_row=2,
            planned_activity_id="manual-future-suppression",
            source="manual",
            scheduled_at="2026-07-30T09:00:00+02:00",
        )
        hidden = self.queue()
        self.assertNotEqual(
            (hidden["suggestion"] or {}).get("suggestion_id"), top["suggestion_id"]
        )
        stored = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == top["suggestion_id"]
        )
        self.assertEqual(stored["status"], "pending")

        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        status_column = sheet.row_values(1).index("status") + 1
        sheet.update_cell(2, status_column, "cancelled")
        visible_again = self.queue()
        self.assertEqual(
            visible_again["suggestion"]["suggestion_id"], top["suggestion_id"]
        )

    def test_version_and_live_channel_change_do_not_revive_dismissed_context(self):
        top = self.queue()["suggestion"]
        dismissed = self.client.post(
            f"/planning/suggestions/{top['suggestion_id']}/dismiss",
            json={
                "client_request_id": "dismiss-before-v21",
                "expected_revision": top["revision"],
            },
        )
        self.assertEqual(dismissed.status_code, 200, dismissed.get_json())
        owner = {"user_name": "olle", "name": "Olle"}
        candidates = app_module.planning_suggestion_candidates(
            self.spreadsheet, owner
        )
        live = next(
            item for item in candidates
            if item["customer_id"] == top["customer_id"]
        )
        changed_live = {
            **live,
            "score_version": "v2.1",
            "priority_score": 100,
            "recommended_contact_type": "email",
        }
        _next, preview, _count = app_module.planning_suggestion_service(
            self.spreadsheet
        ).queue(owner, [changed_live])
        rows = [
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == top["suggestion_id"]
        ]
        self.assertEqual(preview, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "dismissed")


class ScoreAndCacheV21Tests(TestCase):
    def test_exact_established_v21_boundaries(self):
        expected = {30: 90, 31: 75, 60: 75, 61: 50, 90: 50, 91: 60}
        for days, score in expected.items():
            with self.subTest(days=days):
                self.assertEqual(established_intent_timing(days), score)
        self.assertEqual(SCORE_VERSION, "v2.1")

    def test_score_version_and_channel_are_not_context_inputs(self):
        base = dict(
            owner="olle", customer_id="customer-1", lifecycle="established",
            order_count=3, latest_order_reference="O-3",
            latest_order_date="2026-06-01", latest_contact_id="contact-1",
            latest_contact_result="Neutral", latest_contact_date="2026-06-15",
        )
        first = app_module.decision_context_hash(**base)
        second = app_module.decision_context_hash(**base)
        self.assertEqual(first, second)
        self.assertEqual(
            app_module.deterministic_suggestion_id("olle", "customer-1", first),
            app_module.deterministic_suggestion_id("olle", "customer-1", second),
        )

    def test_inflight_read_cannot_refill_cache_after_invalidation(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        started = threading.Event()
        release = threading.Event()
        result = []

        def stale_loader():
            started.set()
            release.wait(2)
            return [["stale"]]

        thread = threading.Thread(
            target=lambda: result.append(
                cache.values(spreadsheet, "customers", loader=stale_loader)
            )
        )
        thread.start()
        self.assertTrue(started.wait(1))
        cache.invalidate(spreadsheet, "customers")
        release.set()
        thread.join(2)
        self.assertEqual(result[0][0], [["stale"]])

        fresh, cache_hit = cache.values(
            spreadsheet, "customers", loader=lambda: [["fresh"]]
        )
        self.assertEqual(fresh, [["fresh"]])
        self.assertFalse(cache_hit)
