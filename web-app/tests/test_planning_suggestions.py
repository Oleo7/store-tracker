from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch

WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

from test_planning import (
    NOW,
    PlanningApiTestCase,
    default_spreadsheet,
)

import app as app_module
from planning_suggestions import (
    SCORE_EVENTS_SHEET,
    SUGGESTIONS_SHEET,
    mutation_fingerprint,
)


class PlanningSuggestionApiTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True

    def tearDown(self):
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        super().tearDown()

    def current(self):
        response = self.client.get("/planning/suggestions")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def mutate(self, suggestion, action, request_id):
        return self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/{action}",
            json={
                "client_request_id": request_id,
                "expected_revision": suggestion["revision"],
            },
        )

    def test_sparse_queue_materializes_exactly_one_card(self):
        body = self.current()

        self.assertEqual(body["pending_count"], 2)
        self.assertEqual(body["suggestion"]["customer"], "Butik A")
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "pending")
        self.assertIn(SCORE_EVENTS_SHEET, self.spreadsheet.sheets)

    def test_snooze_is_exactly_seven_days_and_dismiss_is_terminal_for_hash(self):
        first = self.current()["suggestion"]
        snoozed = self.mutate(first, "snooze", "snooze-once")
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        self.assertEqual(snoozed.get_json()["next_suggestion"]["customer"], "Butik C")
        row = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(
            app_module.parse_planning_instant(row["snooze_until"]),
            NOW + timedelta(days=7),
        )

        second = snoozed.get_json()["next_suggestion"]
        dismissed = self.mutate(second, "dismiss", "dismiss-once")
        self.assertEqual(dismissed.status_code, 200, dismissed.get_json())
        self.assertIsNone(dismissed.get_json()["next_suggestion"])
        after = self.current()
        self.assertIsNone(after["suggestion"])
        self.assertEqual(after["pending_count"], 0)

    def test_snoozed_context_is_resolved_when_new_business_context_arrives(self):
        suggestion = self.current()["suggestion"]
        response = self.mutate(suggestion, "snooze", "snooze-before-order")
        self.assertEqual(response.status_code, 200, response.get_json())
        order_sheet = self.spreadsheet.worksheet("order_rows")
        order = {
            "Reference": "ORDER-DURING-SNOOZE",
            "Order date": "2026-07-28",
            "Delivery date": "2026-07-29",
            "Customer": suggestion["customer"],
            "Quantity": "4",
            "Total": "400",
            "Currency": "SEK",
            "customer_id": suggestion["customer_id"],
        }
        order_sheet.append_row([
            order.get(column, "") for column in app_module.ORDER_COLUMNS
        ])

        self.current()

        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        old = next(row for row in rows if row["suggestion_id"] == suggestion["suggestion_id"])
        self.assertEqual(old["status"], "resolved")
        self.assertEqual(old["resolved_by_type"], "business_context")
        self.assertFalse(any(
            row["suggestion_id"] == suggestion["suggestion_id"]
            and row["status"] in {"pending", "snoozed", "planned"}
            for row in rows
        ))

    def test_snoozed_context_expires_when_trigger_disappears(self):
        suggestion = self.current()["suggestion"]
        response = self.mutate(suggestion, "snooze", "snooze-before-trigger-loss")
        self.assertEqual(response.status_code, 200, response.get_json())
        owner = {"user_name": "olle", "name": "Olle"}
        candidates = [
            candidate
            for candidate in app_module.planning_suggestion_candidates(
                self.spreadsheet, owner
            )
            if candidate["customer_id"] != suggestion["customer_id"]
        ]

        app_module.planning_suggestion_service(self.spreadsheet).queue(
            owner, candidates
        )

        old = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(old["status"], "expired")

    def test_unchanged_snoozed_context_reopens_at_exactly_seven_days(self):
        suggestion = self.current()["suggestion"]
        response = self.mutate(suggestion, "snooze", "snooze-exactly-seven-days")
        self.assertEqual(response.status_code, 200, response.get_json())

        with patch.object(app_module, "stockholm_now", return_value=NOW + timedelta(days=7)):
            current = self.current()

        self.assertEqual(current["suggestion"]["suggestion_id"], suggestion["suggestion_id"])
        old = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(old["status"], "pending")
        self.assertEqual(old["snooze_until"], "")

    def test_mutation_revision_is_stale(self):
        suggestion = self.current()["suggestion"]
        response = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "stale-snooze",
                "expected_revision": suggestion["revision"] + 1,
            },
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "suggestion_stale")

    def test_parametrized_primary_state_transitions(self):
        expected = {
            "snooze": "snoozed",
            "plan": "planned",
            "dismiss": "dismissed",
            "resolve": "resolved",
            "expire": "expired",
        }
        for action, target in expected.items():
            with self.subTest(action=action):
                self.spreadsheet = default_spreadsheet()
                self.spreadsheet_patcher.stop()
                self.spreadsheet_patcher = patch.object(
                    app_module,
                    "get_spreadsheet_with_retry",
                    return_value=self.spreadsheet,
                )
                self.spreadsheet_patcher.start()
                self.login()
                suggestion = self.current()["suggestion"]
                service = app_module.planning_suggestion_service(self.spreadsheet)
                request_id = f"transition-{action}"
                updated, duplicate = service.transition(
                    suggestion["suggestion_id"],
                    owner_name="olle",
                    action=action,
                    expected_revision=suggestion["revision"],
                    request_id=request_id,
                    fingerprint=mutation_fingerprint(
                        action, suggestion["suggestion_id"], request_id
                    ),
                    planned_activity_id="activity-1" if action == "plan" else "",
                    resolved_by_type="contact" if action == "resolve" else "",
                    resolved_by_id="contact-1" if action == "resolve" else "",
                )
                self.assertFalse(duplicate)
                self.assertEqual(updated["status"], target)

    def _plan_case(self, contact_type):
        self.spreadsheet = default_spreadsheet()
        self.spreadsheet_patcher.stop()
        self.spreadsheet_patcher = patch.object(
            app_module,
            "get_spreadsheet_with_retry",
            return_value=self.spreadsheet,
        )
        self.spreadsheet_patcher.start()
        self.login()
        suggestion = self.current()["suggestion"]
        payload = {
            "client_request_id": f"plan-{contact_type}",
            "expected_suggestion_revision": suggestion["revision"],
            "customer_id": suggestion["customer_id"],
            "contact_type": contact_type,
            "scheduled_at": "2026-07-30T09:00:00+02:00",
            "duration_minutes": 20,
            "note": "Valfri aktivitetstyp",
        }
        first = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json=payload,
        )
        retry = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json=payload,
        )
        self.assertIn(first.status_code, {200, 201}, first.get_json())
        self.assertEqual(retry.status_code, 200, retry.get_json())
        rows = self.planning_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["contact_type"], contact_type)
        self.assertEqual(rows[0]["source"], "system_suggestion")
        self.assertEqual(
            rows[0]["source_suggestion_id"], suggestion["suggestion_id"]
        )
        self.assertEqual(
            rows[0]["recommended_contact_type"],
            suggestion["recommended_contact_type"],
        )
        suggestion_rows = self.spreadsheet.worksheet(
            SUGGESTIONS_SHEET
        ).dict_rows()
        linked = next(
            row for row in suggestion_rows
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(linked["status"], "planned")
        self.assertEqual(linked["planned_activity_id"], rows[0]["planned_activity_id"])

    def test_plan_preserves_actual_contact_type_and_is_idempotent(self):
        for contact_type in ("visit", "phone", "email"):
            with self.subTest(contact_type=contact_type):
                self._plan_case(contact_type)

    def test_stale_plan_creates_no_activity(self):
        suggestion = self.current()["suggestion"]
        response = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "stale-plan",
                "expected_suggestion_revision": suggestion["revision"] + 1,
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        )
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(response.get_json()["code"], "suggestion_stale")
        self.assertEqual(self.planning_rows(), [])

    def test_cancelled_linked_activity_reopens_suggestion(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-before-cancel",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        ).get_json()
        activity = planned["activity"]
        cancelled = self.client.patch(
            f"/planning/activities/{activity['planned_activity_id']}",
            json={
                "client_request_id": "cancel-linked",
                "expected_revision": activity["revision"],
                "status": "cancelled",
            },
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.get_json())
        current = self.current()
        self.assertEqual(
            current["suggestion"]["suggestion_id"], suggestion["suggestion_id"]
        )

    def test_external_future_activity_suppression_is_reversible(self):
        first = self.current()["suggestion"]
        external = self.append_planning_row(
            customer_row=2,
            scheduled_at="2026-07-30T09:00:00+02:00",
            source="manual",
            client_request_id="external-plan",
        )
        suppressed = self.current()
        self.assertEqual(suppressed["suggestion"]["customer"], "Butik C")

        response = self.client.patch(
            f"/planning/activities/{external['planned_activity_id']}",
            json={
                "client_request_id": "cancel-external",
                "expected_revision": 1,
                "status": "cancelled",
            },
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        visible_again = self.current()["suggestion"]
        self.assertEqual(visible_again["suggestion_id"], first["suggestion_id"])

    def test_completed_linked_activity_resolves_suggestion(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-before-complete",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        ).get_json()
        completed = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                "client_request_id": "complete-linked-suggestion",
                "planned_activity_id": planned["activity"]["planned_activity_id"],
                "date_time": "2026-07-30 09:10",
                "contact_channel": "Telefon",
                "result": "Positiv",
                "comment": "Genomförd",
                "customer_contact_person": "Klara",
            },
        )
        self.assertEqual(completed.status_code, 200, completed.get_json())
        stored = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "contact")

    def test_superseded_linked_activity_resolves_suggestion(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-before-supersede",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        )
        self.assertEqual(planned.status_code, 201, planned.get_json())
        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        sheet.update_cell(
            2, app_module.PLANNED_ACTIVITY_COLUMNS.index("status") + 1,
            "superseded",
        )
        app_module.invalidate_sheet_for_write(sheet)

        self.current()
        stored = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "activity")
        self.assertEqual(
            stored["resolved_by_id"], planned.get_json()["activity"]["planned_activity_id"]
        )

    def test_later_contact_supersedes_linked_plan_and_resolves_suggestion(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-before-later-contact",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        )
        self.assertEqual(planned.status_code, 201, planned.get_json())
        contact = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                "client_request_id": "later-contact-resolves-suggestion",
                "customer_id": suggestion["customer_id"],
                "date_time": "2026-07-31 10:00",
                "contact_channel": "Telefon",
                "result": "Positiv",
                "comment": "Senare kontakt",
            },
        )
        self.assertEqual(contact.status_code, 200, contact.get_json())
        self.assertEqual(self.planning_rows()[0]["status"], "superseded")
        stored = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "contact")

    def test_new_business_context_resolves_planned_suggestion(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "plan-before-order",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "",
            },
        )
        self.assertIn(planned.status_code, {200, 201}, planned.get_json())
        order_sheet = self.spreadsheet.worksheet("order_rows")
        order = {
            "Reference": "NEW-ORDER",
            "Order date": "2026-07-28",
            "Delivery date": "2026-07-29",
            "Customer": "Butik A",
            "Quantity": "12",
            "Total": "1200",
            "Currency": "SEK",
            "customer_id": suggestion["customer_id"],
        }
        order_sheet.append_row([
            order.get(column, "") for column in app_module.ORDER_COLUMNS
        ])

        self.current()
        stored = next(
            row for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "business_context")

    def test_seller_cannot_read_or_mutate_another_owner_queue(self):
        forbidden_read = self.client.get("/planning/suggestions?owner=sofia")
        self.assertEqual(forbidden_read.status_code, 403)

        self.login("sofia")
        other = self.current()["suggestion"]
        self.login("olle")
        forbidden_mutation = self.mutate(other, "dismiss", "wrong-owner")
        self.assertEqual(forbidden_mutation.status_code, 403)

    def test_admin_selected_owner_can_read_and_mutate(self):
        self.login("admin")
        body = self.client.get("/planning/suggestions?owner=sofia").get_json()
        self.assertEqual(body["suggestion"]["customer"], "Butik B")
        response = self.client.post(
            f"/planning/suggestions/{body['suggestion']['suggestion_id']}/dismiss",
            json={
                "client_request_id": "admin-dismiss-sofia",
                "expected_revision": body["suggestion"]["revision"],
            },
        )
        self.assertEqual(response.status_code, 200, response.get_json())


class PlanningSuggestionProductionGuardTests(TestCase):
    def test_stub_never_enables_in_production(self):
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        try:
            with patch.object(app_module, "application_environment", return_value="production"):
                self.assertFalse(app_module.planning_suggestion_stub_enabled())
        finally:
            app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)


class PersistentAProspectSuggestionTests(PlanningApiTestCase):
    def current(self):
        response = self.client.get("/planning/suggestions")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def test_new_a_prospect_can_snooze_but_not_be_permanently_hidden(self):
        suggestion = self.current()["suggestion"]
        self.assertEqual(
            suggestion["customer_guidance"]["focus_key"], "a_prospect"
        )

        dismissed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/dismiss",
            json={
                "client_request_id": "reject-a-dismiss",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(dismissed.status_code, 409, dismissed.get_json())
        self.assertEqual(
            dismissed.get_json()["code"], "a_prospect_requires_snooze"
        )

        snoozed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "snooze-a-prospect",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["status"], "snoozed")
        self.assertEqual(
            app_module.parse_planning_instant(stored["snooze_until"]),
            NOW + timedelta(days=7),
        )

    def test_resolved_a_prospect_reopens_after_contact_pause(self):
        suggestion = self.current()["suggestion"]
        self.assertEqual(
            suggestion["customer_guidance"]["focus_key"], "a_prospect"
        )

        saved = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                "client_request_id": "a-prospect-contact",
                "date_time": "2026-07-27 10:15",
                "contact_channel": "Telefon",
                "result": "Neutral",
                "comment": "Kontakt genomförd utan order",
                "customer_contact_person": "Klara",
            },
        )
        self.assertEqual(saved.status_code, 200, saved.get_json())
        stored = next(
            row
            for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "contact")

        later = NOW + timedelta(days=3)
        owner = {"user_name": "olle", "name": "Olle"}
        with (
            patch.object(app_module, "stockholm_now", return_value=later),
            patch.object(app_module, "stockholm_today", return_value=later.date()),
        ):
            activities = self.planning_rows()
            candidates = app_module.planning_suggestion_candidates(
                self.spreadsheet, owner, activities
            )
            app_module.planning_suggestion_service(self.spreadsheet).queue(
                owner,
                candidates,
                activity_rows=activities,
            )

        reopened = next(
            row
            for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        self.assertEqual(reopened["status"], "pending")
        self.assertEqual(reopened["resolved_by_type"], "")

    def _rewrite_suggestion_as_pre_segment_context(self, suggestion):
        owner = {"user_name": "olle", "name": "Olle"}
        candidate = next(
            item
            for item in app_module.planning_suggestion_candidates(
                self.spreadsheet, owner, self.planning_rows()
            )
            if item["customer_id"] == suggestion["customer_id"]
        )
        legacy_hashes = candidate.get("compatible_decision_context_hashes") or []
        self.assertTrue(legacy_hashes)
        legacy_hash = legacy_hashes[0]
        legacy_id = app_module.deterministic_suggestion_id(
            owner["user_name"], suggestion["customer_id"], legacy_hash
        )
        sheet = self.spreadsheet.worksheet(SUGGESTIONS_SHEET)
        rows = sheet.dict_rows()
        row_offset = next(
            index
            for index, row in enumerate(rows, start=2)
            if row["suggestion_id"] == suggestion["suggestion_id"]
        )
        app_module.update_sheet_row(
            sheet,
            row_offset,
            sheet.row_values(1),
            {
                "suggestion_id": legacy_id,
                "decision_context_hash": legacy_hash,
            },
        )
        return legacy_id

    def test_pre_segment_snooze_is_preserved_in_queue_and_customer_list(self):
        suggestion = self.current()["suggestion"]
        snoozed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "legacy-schema-snooze",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        legacy_id = self._rewrite_suggestion_as_pre_segment_context(suggestion)

        insights = self.client.get("/customer-insights")
        self.assertEqual(insights.status_code, 200, insights.get_json())
        butik = insights.get_json()["butik a"]
        self.assertEqual(
            butik["recommendation_suppression_reason"], "snoozed"
        )
        self.assertEqual(
            butik["customer_guidance"]["status_key"], "wait"
        )

        queue = self.current()
        self.assertNotEqual(
            (queue.get("suggestion") or {}).get("customer_id"),
            suggestion["customer_id"],
        )
        stored = [
            row
            for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["customer_id"] == suggestion["customer_id"]
        ]
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["suggestion_id"], legacy_id)
        self.assertEqual(stored[0]["status"], "snoozed")

    def test_explicit_legacy_state_wins_if_canonical_duplicate_exists(self):
        suggestion = self.current()["suggestion"]
        snoozed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "legacy-state-before-duplicate",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        legacy_id = self._rewrite_suggestion_as_pre_segment_context(suggestion)

        owner = {"user_name": "olle", "name": "Olle"}
        candidate = next(
            item
            for item in app_module.planning_suggestion_candidates(
                self.spreadsheet, owner, self.planning_rows()
            )
            if item["customer_id"] == suggestion["customer_id"]
        )
        service = app_module.planning_suggestion_service(self.spreadsheet)
        canonical = service._candidate_row(owner, candidate)
        sheet = self.spreadsheet.worksheet(SUGGESTIONS_SHEET)
        headers = sheet.row_values(1)
        sheet.append_row([canonical.get(header, "") for header in headers])

        insights = self.client.get("/customer-insights")
        self.assertEqual(insights.status_code, 200, insights.get_json())
        butik = insights.get_json()["butik a"]
        self.assertEqual(
            butik["recommendation_suppression_reason"], "snoozed"
        )

        current = self.current()
        self.assertNotEqual(
            (current.get("suggestion") or {}).get("customer_id"),
            suggestion["customer_id"],
        )
        rows = [
            row
            for row in sheet.dict_rows()
            if row["customer_id"] == suggestion["customer_id"]
        ]
        self.assertEqual(len(rows), 2)
        legacy = next(row for row in rows if row["suggestion_id"] == legacy_id)
        self.assertEqual(legacy["status"], "snoozed")

    def test_pre_segment_planned_link_can_cancel_and_reopen(self):
        suggestion = self.current()["suggestion"]
        planned = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json={
                "client_request_id": "legacy-plan-before-schema-change",
                "expected_suggestion_revision": suggestion["revision"],
                "customer_id": suggestion["customer_id"],
                "contact_type": "phone",
                "scheduled_at": "2026-07-30T09:00:00+02:00",
                "note": "Historisk planering",
            },
        )
        self.assertIn(planned.status_code, {200, 201}, planned.get_json())
        activity = planned.get_json()["activity"]
        legacy_id = self._rewrite_suggestion_as_pre_segment_context(suggestion)

        activity_sheet = self.spreadsheet.worksheet("planned_activities")
        activity_rows = activity_sheet.dict_rows()
        activity_offset = next(
            index
            for index, row in enumerate(activity_rows, start=2)
            if row["planned_activity_id"] == activity["planned_activity_id"]
        )
        app_module.update_sheet_row(
            activity_sheet,
            activity_offset,
            activity_sheet.row_values(1),
            {"source_suggestion_id": legacy_id},
        )

        cancelled = self.client.patch(
            f"/planning/activities/{activity['planned_activity_id']}",
            json={
                "client_request_id": "cancel-pre-segment-plan",
                "expected_revision": activity["revision"],
                "status": "cancelled",
            },
        )
        self.assertEqual(cancelled.status_code, 200, cancelled.get_json())

        reopened = self.current()["suggestion"]
        self.assertEqual(reopened["customer_id"], suggestion["customer_id"])
        self.assertEqual(reopened["suggestion_id"], legacy_id)
        self.assertEqual(reopened["status"], "pending")

    def test_pre_segment_a_prospect_state_does_not_survive_segment_change(self):
        suggestion = self.current()["suggestion"]
        snoozed = self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/snooze",
            json={
                "client_request_id": "legacy-schema-before-segment-change",
                "expected_revision": suggestion["revision"],
            },
        )
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        legacy_id = self._rewrite_suggestion_as_pre_segment_context(suggestion)

        customer_sheet = self.spreadsheet.worksheet("customers_enriched")
        customer_rows = customer_sheet.dict_rows()
        customer_offset = next(
            index
            for index, row in enumerate(customer_rows, start=2)
            if row["customer_id"] == suggestion["customer_id"]
        )
        app_module.update_sheet_row(
            customer_sheet,
            customer_offset,
            customer_sheet.row_values(1),
            {"customer_segment": "B"},
        )

        insights = self.client.get("/customer-insights")
        self.assertEqual(insights.status_code, 200, insights.get_json())
        butik = insights.get_json()["butik a"]
        self.assertNotEqual(
            butik["recommendation_suppression_reason"], "snoozed"
        )

        self.current()
        old = next(
            row
            for row in self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
            if row["suggestion_id"] == legacy_id
        )
        self.assertIn(old["status"], {"resolved", "expired"})

    def test_historical_hidden_a_prospect_is_visible_and_explicitly_resumable(self):
        suggestion = self.current()["suggestion"]
        owner = {"user_name": "olle", "name": "Olle"}
        candidates = app_module.planning_suggestion_candidates(
            self.spreadsheet, owner
        )
        live = next(
            item for item in candidates
            if item["customer_id"] == suggestion["customer_id"]
        )
        service = app_module.planning_suggestion_service(self.spreadsheet)
        service.transition(
            suggestion["suggestion_id"],
            owner_name="olle",
            action="dismiss",
            expected_revision=suggestion["revision"],
            request_id="historical-dismiss",
            fingerprint=mutation_fingerprint(
                "dismiss", suggestion["suggestion_id"], "historical-dismiss"
            ),
            live_candidate=live,
        )

        hidden = self.current()["suggestion"]
        self.assertEqual(hidden["suggestion_id"], suggestion["suggestion_id"])
        self.assertEqual(hidden["status"], "dismissed")
        self.assertEqual(
            hidden["customer_guidance"]["reason_text"], "Tidigare förslag dolt"
        )

        resumed = self.client.post(
            f"/planning/suggestions/{hidden['suggestion_id']}/resume",
            json={
                "client_request_id": "resume-historical-a",
                "expected_revision": hidden["revision"],
            },
        )
        self.assertEqual(resumed.status_code, 200, resumed.get_json())
        self.assertEqual(
            resumed.get_json()["next_suggestion"]["status"], "pending"
        )
