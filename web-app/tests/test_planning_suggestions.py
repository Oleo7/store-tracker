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





    def test_mutation_revision_is_stale(self):
        suggestion=self.current()['suggestion']
        response=self.client.post('/planning/suggestions/'+suggestion['suggestion_id']+'/plan',json={'client_request_id':'stale-plan','expected_suggestion_revision':999,'customer_id':suggestion['customer_id'],'contact_type':'phone','scheduled_at':'2026-07-28T14:00+02:00'})
        self.assertEqual(response.status_code,409,response.get_json())
        self.assertEqual(self.planning_rows(),[])

    def test_parametrized_primary_state_transitions(self):
        from planning_suggestions import SuggestionError
        suggestion=self.current()['suggestion'];before={k:[list(r) for r in v.values] for k,v in self.spreadsheet.sheets.items()}
        for action in ['snooze','dismiss']:
            with self.assertRaises(SuggestionError) as error:
                app_module.planning_suggestion_service(self.spreadsheet).transition(suggestion['suggestion_id'],owner_name='olle',action=action,expected_revision=1,request_id='removed-'+action,fingerprint='removed')
            self.assertEqual(error.exception.status,410)
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})

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
                "date_time": NOW.isoformat(),
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
        self.assertEqual(stored["status"], "pending")  # Persistent A prospect returns without a contact/order.
        events = self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
        self.assertTrue(any(event["event_type"] == "suggestion_resolved" and event["suggestion_id"] == suggestion["suggestion_id"] for event in events))

    def test_later_unlinked_contact_preserves_calendar_while_resolving_commercial_episode(self):
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
                "date_time": NOW.isoformat(),
                "contact_channel": "Telefon",
                "result": "Positiv",
                "comment": "Senare kontakt",
            },
        )
        self.assertEqual(contact.status_code, 200, contact.get_json())
        self.assertEqual(self.planning_rows()[0]["status"], "planned")
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
            "Order date": "2026-07-27",
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
        self.assertEqual(forbidden_mutation.status_code, 410)

    def test_admin_selected_owner_can_read_and_mutate(self):
        self.login('admin')
        suggestion=self.client.get('/planning/suggestions?user_name=olle').get_json()['suggestion']
        response=self.client.post('/planning/suggestions/'+suggestion['suggestion_id']+'/plan',json={'user_name':'olle','client_request_id':'admin-plan','expected_suggestion_revision':suggestion['revision'],'customer_id':suggestion['customer_id'],'contact_type':'phone','scheduled_at':'2026-07-28T14:00+02:00'})
        self.assertEqual(response.status_code,201,response.get_json())
        self.assertEqual(self.planning_rows()[0]['user_name'],'olle')


class PlanningSuggestionProductionGuardTests(TestCase):
    def test_stub_never_enables_in_production(self):
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        try:
            with patch.object(app_module, "application_environment", return_value="production"):
                self.assertFalse(hasattr(app_module, "planning_suggestion_stub_enabled"))
        finally:
            app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)


class PersistentAProspectSuggestionTests(PlanningApiTestCase):
    def current(self):
        response = self.client.get("/planning/suggestions")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()


    def test_resolved_a_prospect_reopens_after_contact_pause(self):
        suggestion = self.current()["suggestion"]
        self.assertEqual(
            suggestion["customer_guidance"]["focus_key"], "a_prospect"
        )

        saved = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                "client_request_id": "a-prospect-contact",
                "date_time": NOW.isoformat(),
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

        # A completed dialogue uses the new routine contact interval. The
        # persistent opportunity must return when that interval has elapsed.
        later = NOW + timedelta(days=21)
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




class HistoricalSuggestionStateTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config['PLANNING_SUGGESTIONS_STUB']=True
        self.addCleanup(app_module.app.config.pop,'PLANNING_SUGGESTIONS_STUB',None)
    current=PlanningSuggestionApiTests.current
    def test_old_manual_states_do_not_suppress_live_queue(self):
        suggestion=self.current()['suggestion']
        sheet=self.spreadsheet.worksheet(SUGGESTIONS_SHEET)
        for status in ['snoozed','dismissed']:
            sheet.values[1][sheet.values[0].index('status')]=status
            sheet.values[1][sheet.values[0].index('snooze_until')]='2099-01-01T09:00+01:00'
            app_module.invalidate_sheet_for_write(sheet)
            current=self.current()
            self.assertEqual(current['suggestion']['customer_id'],suggestion['customer_id'])
            self.assertEqual(current['suggestion']['status'],'pending')
