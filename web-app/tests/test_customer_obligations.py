from datetime import timedelta
import copy
import json
from unittest.mock import patch

from test_planning import PlanningApiTestCase, NOW
import app as app_module
from customer_obligations import build_customer_state, attention_items

CID = "11111111-1111-4111-8111-111111111111"


class CustomerObligationApiTests(PlanningApiTestCase):
    def source(self, contact_id="promise", due="2026-07-25", **extra):
        return self.append_contact_row(contact_id=contact_id, customer_id=CID,
                                       date_time="2026-07-20 09:00", follow_up_date=due,
                                       follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y", **extra)

    def attention(self):
        response = self.client.get("/planning/attention?user_name=olle")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def state(self):
        return app_module.customer_state_snapshot(self.spreadsheet)

    def item(self, oid="followup:promise"):
        return next(row for row in self.state()["obligations"][CID] if row["obligation_id"] == oid)

    def insight(self):
        response = self.client.get("/customer-insights", headers={"X-PB-Value-Model": "v3.3"})
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()["__by_customer_id"][CID]

    def action(self, action, *, oid="followup:promise", request_id=None, **extra):
        data = {"customer_id": CID, "obligation_id": oid, "action": action,
                "expected_revision": self.item(oid)["revision"], "client_request_id": request_id or action, **extra}
        return self.client.post("/customer-obligations/action", json=data)

    def contact(self, **extra):
        if extra.get("obligation_id") and "expected_obligation_revision" not in extra:
            extra["expected_obligation_revision"] = self.item(extra["obligation_id"])["revision"]
        data = {"customer_id": CID, "contact_channel": "Telefon", "result": "Ej anträffbar",
                "comment": "Ett nytt försök; samma åtagande kvarstår.", "date_time": "2026-07-27 10:12",
                "client_request_id": "contact-attempt", **extra}
        return self.client.post("/customers/Butik%20A/contacts", json=data)

    def test_read_only_attention_strict_dates_and_separate_scopes(self):
        self.source()
        self.source("today-promise", "2026-07-27")
        self.append_planning_row(planned_activity_id="today", scheduled_at="2026-07-27T08:00:00+02:00")
        self.append_planning_row(planned_activity_id="missed", scheduled_at="2026-07-24T08:00:00+02:00")
        self.append_planning_row(planned_activity_id="unrelated-future", scheduled_at="2026-07-29T08:00:00+02:00")
        before = {name: copy.deepcopy(sheet.values) for name, sheet in self.spreadsheet.sheets.items()}
        payload = self.attention()
        self.insight()
        self.assertEqual(payload["total"], 2)
        self.assertEqual({row["item_type"] for row in payload["items"]},
                         {"missed_calendar_activity", "overdue_explicit_follow_up"})
        self.assertEqual(before, {name: sheet.values for name, sheet in self.spreadsheet.sheets.items()})
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)

    def test_same_source_today_booking_deduplicates_only_its_promise(self):
        self.source()
        self.source("another-promise", "2026-07-24")
        self.append_planning_row(planned_activity_id="linked", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-27T08:00:00+02:00")
        self.assertEqual([row["obligation_id"] for row in self.attention()["items"]], ["followup:another-promise"])

    def test_actual_failed_post_preserves_source_then_snapshot_and_attention(self):
        self.source()
        self.append_planning_row(planned_activity_id="linked", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-25T08:00:00+02:00")
        response = self.contact(planned_activity_id="linked", obligation_id="followup:promise", obligation_outcome="resolved")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.contact_rows()[0]["follow_up_date"], "2026-07-25")
        self.assertEqual(self.planning_rows()[0]["status"], "completed")
        self.assertEqual(self.item()["status"], "open")
        items = self.attention()["items"]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["item_type"], "overdue_explicit_follow_up")
        self.assertFalse(items[0]["can_contact_now"])
        insight = self.insight()
        self.assertFalse(insight["list_guidance"]["can_contact_now"])
        self.assertEqual(insight["contact_priority_score"], 0)
        duplicate = self.contact(planned_activity_id="linked", obligation_id="followup:promise", obligation_outcome="resolved")
        self.assertEqual(duplicate.status_code, 200, duplicate.get_json())
        self.assertEqual(len(self.contact_rows()), 2)

    def test_explicit_resolved_post_does_not_reappear_from_source_date(self):
        self.source()
        self.append_planning_row(planned_activity_id="linked", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-25T08:00:00+02:00")
        response = self.contact(planned_activity_id="linked", obligation_id="followup:promise",
                                obligation_outcome="resolved", result="Positiv")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.item()["status"], "resolved")
        self.assertEqual(self.attention()["total"], 0)
        self.assertEqual(self.insight()["open_obligations"], [])

    def test_contact_partial_save_retry_repairs_one_event_and_one_contact(self):
        self.source()
        real_append = app_module.append_customer_state_event
        calls = []
        def fail_once(*args, **kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("Injected state append interruption")
            return real_append(*args, **kwargs)
        with patch.object(app_module, "append_customer_state_event", side_effect=fail_once):
            first = self.contact(obligation_id="followup:promise")
            self.assertEqual(first.status_code, 207, first.get_json())
            retry = self.contact(obligation_id="followup:promise")
            self.assertEqual(retry.status_code, 200, retry.get_json())
        self.assertEqual(len(self.contact_rows()), 2)
        self.assertEqual(len(app_module.read_customer_state_events(self.spreadsheet)), 1)
        self.assertEqual(self.item()["status"], "open")

    def test_snooze_restore_and_expiry_do_not_change_source_due(self):
        self.source()
        response = self.action("snooze", snooze_until=(NOW + timedelta(days=2)).isoformat())
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.attention()["total"], 0)
        self.assertEqual(self.item()["original_due_date"], "2026-07-25")
        later = NOW + timedelta(days=3)
        self.assertEqual(len(attention_items(self.state(), as_of=later)), 1)
        response = self.action("restore")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.attention()["total"], 1)

    def test_reschedule_retains_scope_and_creates_only_one_activity_on_retry(self):
        self.source()
        data = {"customer_id": CID, "obligation_id": "followup:promise", "action": "reschedule",
                "expected_revision": self.item()["revision"], "client_request_id": "reschedule-one",
                "scheduled_at": "2026-07-28T10:00+02:00", "contact_type": "phone",
                "appointment_confirmed": True, "picking_help": False}
        first = self.client.post("/customer-obligations/action", json=data)
        self.assertEqual(first.status_code, 200, first.get_json())
        second = self.client.post("/customer-obligations/action", json=data)
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertEqual(len(self.planning_rows()), 1)
        self.assertEqual(self.item()["due_date"], "2026-07-28")
        self.assertEqual(self.item()["original_due_date"], "2026-07-25")
        self.assertEqual(self.attention()["total"], 0)

    def test_reschedule_partial_retry_finishes_without_revision_dead_end(self):
        self.source()
        self.append_planning_row(planned_activity_id="old-plan", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-25T08:00:00+02:00")
        data = {"customer_id": CID, "obligation_id": "followup:promise", "action": "reschedule",
                "expected_revision": self.item()["revision"], "client_request_id": "partial-reschedule",
                "scheduled_at": "2026-07-28T10:00+02:00", "contact_type": "phone"}
        with patch.object(app_module, "append_customer_state_event", side_effect=RuntimeError("Injected event failure")):
            response = self.client.post("/customer-obligations/action", json=data)
            self.assertEqual(response.status_code, 503, response.get_json())
        response = self.client.post("/customer-obligations/action", json=data)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(self.planning_rows()), 2)
        self.assertEqual(self.item()["status"], "scheduled")

    def test_cancel_and_restore_are_scoped_and_revision_conflicts_are_visible(self):
        self.source()
        self.source("separate", "2026-07-24")
        revision = self.item()["revision"]
        response = self.action("cancel")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual([row["obligation_id"] for row in self.attention()["items"]], ["followup:separate"])
        stale = self.action("restore", expected_revision=revision)
        self.assertEqual(stale.status_code, 409)
        restored = self.action("restore", request_id="restore-current")
        self.assertEqual(restored.status_code, 200, restored.get_json())
        self.assertEqual(self.attention()["total"], 2)

    def test_legacy_calendar_cancel_does_not_cancel_explicit_promise(self):
        self.source()
        self.append_planning_row(planned_activity_id="cancelled-attempt", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-25T08:00:00+02:00", status="cancelled")
        self.assertEqual(self.item()["status"], "needs_review")
        self.assertEqual(self.attention()["total"], 1)

    def test_virtual_workflow_snooze_dismiss_restore_and_idempotency(self):
        sheet = self.spreadsheet.worksheet("customers_enriched")
        sheet.update_cell(2, sheet.values[0].index("customer_segment") + 1, "B")
        for action, extra in (("snooze", {"snooze_until": (NOW + timedelta(days=7)).isoformat()}), ("dismiss", {}), ("restore", {})):
            workflow = self.insight()["customer_workflow"]
            data = {"customer_id": CID, "action": action, "expected_revision": workflow["revision"],
                    "decision_context_hash": workflow["decision_context_hash"], "client_request_id": "workflow-" + action, **extra}
            response = self.client.post("/customer-workflow/action", json=data)
            self.assertEqual(response.status_code, 200, response.get_json())
            duplicate = self.client.post("/customer-workflow/action", json=data)
            self.assertEqual(duplicate.status_code, 200, duplicate.get_json())
            refreshed = self.insight()
            self.assertEqual(refreshed["customer_workflow"]["status"], {"snooze": "snoozed", "dismiss": "dismissed", "restore": "active"}[action])
            if action != "restore":
                self.assertFalse(refreshed["list_guidance"]["can_contact_now"])
        self.assertEqual(len(app_module.read_customer_state_events(self.spreadsheet)), 3)

    def test_a_prospect_requires_dated_snooze_and_can_restore(self):
        workflow = self.insight()["customer_workflow"]
        self.assertFalse(workflow["can_dismiss"])
        self.assertTrue(workflow["requires_snooze"])
        data = {"customer_id": CID, "action": "dismiss", "expected_revision": workflow["revision"],
                "decision_context_hash": workflow["decision_context_hash"], "client_request_id": "a-dismiss"}
        rejected = self.client.post("/customer-workflow/action", json=data)
        self.assertEqual(rejected.status_code, 409, rejected.get_json())
        self.assertEqual(rejected.get_json()["code"], "a_prospect_requires_snooze")
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)
        snoozed = self.client.post("/customer-workflow/action", json={**data, "action": "snooze",
                                   "client_request_id": "a-snooze", "snooze_until": (NOW + timedelta(days=7)).isoformat()})
        self.assertEqual(snoozed.status_code, 200, snoozed.get_json())
        workflow = self.insight()["customer_workflow"]
        restored = self.client.post("/customer-workflow/action", json={**data, "action": "restore",
                                    "client_request_id": "a-restore", "expected_revision": workflow["revision"]})
        self.assertEqual(restored.status_code, 200, restored.get_json())
        self.assertEqual(self.insight()["customer_workflow"]["status"], "active")

    def test_permission_event_blocks_email_candidates_and_required_route(self):
        blocked = self.client.post("/customer-contact-permission", json={"customer_id": CID,
                                  "status": "do_not_contact", "reason": "Uttrycklig begäran", "expected_revision": 0,
                                  "client_request_id": "ban-for-route"})
        self.assertEqual(blocked.status_code, 200, blocked.get_json())
        snapshot = app_module.get_authoritative_priority_snapshot(self.spreadsheet, today=NOW.date())
        customer = next(row for row in snapshot["customers"] if row["customer_id"] == CID)
        priority = next(row for row in snapshot["priorities"] if row["customer_id"] == CID)
        proposal = app_module.build_email_proposal_status({**customer, "email": "fixture@example.com"}, priority,
                                                         {"email_type": "introduction"}, {}, set(), NOW.date())
        self.assertFalse(proposal["due"])
        self.assertIn("do_not_contact", proposal["blockers"])
        with app_module.app.app_context():
            payload, error = app_module.calculate_route_proposal_for_user(
                spreadsheet=self.spreadsheet, start=app_module.Coordinate(57.7, 11.9),
                client_requested_rows=(2,), required_rows=(2,), respect_requested_rows=True,
                user={"user_name": "olle", "name": "Olle", "role": "Säljare", "admin": False}, route_date=NOW.date())
            self.assertIsNone(payload)
            self.assertEqual(error[1], 409)
            self.assertEqual(error[0].get_json()["code"], "do_not_contact")

    def test_explicit_contact_permission_survives_positive_contact_and_can_be_lifted(self):
        self.source()
        refusal = self.contact(result="Återkom ej", client_request_id="refuse", comment="Kunden vill inte kontaktas.")
        self.assertEqual(refusal.status_code, 200, refusal.get_json())
        self.assertEqual(self.insight()["contact_permission"]["status"], "do_not_contact")
        self.assertFalse(self.attention()["items"][0]["can_contact_now"])
        positive = self.contact(result="Positiv", client_request_id="positive-later", date_time="2026-07-27 10:12")
        self.assertEqual(positive.status_code, 200, positive.get_json())
        self.assertEqual(self.insight()["contact_permission"]["status"], "do_not_contact")
        permission = self.insight()["contact_permission"]
        allowed = self.client.post("/customer-contact-permission", json={"customer_id": CID, "status": "allowed",
                                   "expected_revision": permission["revision"], "reason": "Kunden har uttryckligen bett oss ringa igen.",
                                   "client_request_id": "allow"})
        self.assertEqual(allowed.status_code, 200, allowed.get_json())
        self.assertEqual(self.insight()["contact_permission"]["status"], "allowed")

    def test_owner_mismatch_never_creates_state_storage(self):
        self.source()
        self.login("sofia")
        response = self.client.post("/customer-obligations/action", json={"customer_id": CID, "obligation_id": "followup:promise",
                                    "action": "cancel", "client_request_id": "wrong-owner", "expected_revision": 0})
        self.assertEqual(response.status_code, 404)
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)

    def test_contact_form_cannot_grant_permission_without_current_permission_revision(self):
        refusal = self.contact(result="Återkom ej", client_request_id="ban-before-grant", comment="Kunden avböjer kontakt.")
        self.assertEqual(refusal.status_code, 200, refusal.get_json())
        response = self.contact(result="Kontakt tillåten", client_request_id="grant-without-cas", comment="Ny tillåtelse")
        self.assertEqual(response.status_code, 400, response.get_json())
        self.assertEqual(response.get_json()["error"], "expected_revision_required")
        self.assertEqual(self.insight()["contact_permission"]["status"], "do_not_contact")
        self.assertEqual(len(self.contact_rows()), 1)

    def test_legacy_source_date_change_rejects_stale_cas_revision(self):
        self.source()
        original = self.item()
        sheet = self.spreadsheet.worksheet("sales_activities")
        sheet.update_cell(2, sheet.values[0].index("follow_up_date") + 1, "2026-07-30")
        response = self.client.post("/customer-obligations/action", json={
            "customer_id": CID, "obligation_id": original["obligation_id"], "action": "cancel",
            "client_request_id": "stale-date", "expected_revision": original["revision"]})
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(response.get_json()["code"], "revision_conflict")
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)

    def test_failed_post_with_next_booking_moves_same_promise_without_duplicates(self):
        self.source()
        self.append_planning_row(planned_activity_id="old-attempt", source="follow_up", source_contact_id="promise",
                                 scheduled_at="2026-07-25T08:00:00+02:00")
        response = self.contact(planned_activity_id="old-attempt", obligation_id="followup:promise",
                                follow_up={"enabled": True, "contact_type": "phone",
                                           "scheduled_at": "2026-07-29T10:00+02:00", "note": "Samma uppföljning vid ny tid."})
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(self.state()["obligations"][CID]), 1)
        self.assertEqual(self.item()["status"], "scheduled")
        self.assertEqual(self.item()["due_date"], "2026-07-29")
        self.assertEqual(self.attention()["total"], 0)
        self.assertEqual(len(self.planning_rows()), 2)

    def test_exact_linked_legacy_reached_completion_is_resolved_but_order_is_review(self):
        for suffix, result, expected in (("reached", "Positiv", "resolved"), ("order", "Order lagd!", "needs_review")):
            self.source(suffix)
            self.append_planning_row(planned_activity_id="old-" + suffix, source="follow_up", source_contact_id=suffix,
                                     scheduled_at="2026-07-25T08:00:00+02:00", status="completed", completed_contact_id="done-" + suffix)
            self.append_contact_row(contact_id="done-" + suffix, customer_id=CID, date_time="2026-07-26 09:00",
                                    follow_up_date="", planned_activity_id="old-" + suffix, result=result)
            self.assertEqual(self.item("followup:" + suffix)["status"], expected)

    def test_feature_disabled_returns_explicit_model_error(self):
        with patch.dict(app_module.app.config, {"VALUE_PRIORITY_ENABLED": False}):
            response = self.client.get("/customer-insights", headers={"X-PB-Value-Model": "v3.3"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["code"], "value_priority_disabled")

    def test_recorded_after_as_of_cannot_introduce_permission_or_obligation(self):
        customers = app_module.get_customer_rows(self.spreadsheet)
        contact = {"customer_id": CID, "date_time": "2026-07-20 09:00", "recorded_at": "2026-07-28T09:00+02:00",
                   "contact_id": "late-import", "result": "Återkom ej", "follow_up_date": "2026-07-24"}
        state = build_customer_state(customers, [contact], [], as_of=NOW)
        self.assertEqual(state["obligations"], {})
        self.assertEqual(state["contact_permissions"], {})

    def test_identity_conflict_is_quarantined_and_unique_number_fallback_is_shared(self):
        customers = app_module.get_customer_rows(self.spreadsheet)
        conflict = {"customer_id": CID, "customer_number": "C-2", "customer": "Butik A", "date_time": "2026-07-20 09:00",
                    "contact_id": "conflict", "result": "Återkom ej", "follow_up_date": "2026-07-24"}
        fallback = {**conflict, "customer_id": "", "customer_number": "C-1", "contact_id": "valid-number"}
        state = build_customer_state(customers, [conflict, fallback], [], as_of=NOW)
        self.assertEqual([item["obligation_id"] for item in state["obligations"][CID]], ["followup:valid-number"])
        self.assertEqual(state["contact_permissions"][CID]["source_contact_id"], "valid-number")

    def test_permission_identical_timestamps_refusal_wins(self):
        contacts = [{"customer_id": CID, "date_time": "2026-07-20 09:00", "contact_id": "z-refusal", "result": "Återkom ej"},
                    {"customer_id": CID, "date_time": "2026-07-20 09:00", "contact_id": "a-permission", "result": "Kontakt tillåten"}]
        state = build_customer_state(app_module.get_customer_rows(self.spreadsheet), contacts, [], as_of=NOW)
        self.assertEqual(state["contact_permissions"][CID]["status"], "do_not_contact")

    def test_stale_contact_form_cannot_reopen_resolved_obligation(self):
        self.source()
        old_revision = self.item()["revision"]
        self.assertEqual(self.action("resolve").status_code, 200)
        response = self.contact(obligation_id="followup:promise", expected_obligation_revision=old_revision)
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(len(self.contact_rows()), 1)
        self.assertEqual(self.item()["status"], "resolved")

    def test_partial_permission_event_retry_uses_matching_saved_contact(self):
        real_append = app_module.append_customer_state_event
        count = []
        def fail_permission_once(*args, **kwargs):
            if kwargs.get("kind") == "permission" and not count:
                count.append(1)
                raise RuntimeError("Injected permission write interruption")
            return real_append(*args, **kwargs)
        payload = {"result": "Återkom ej", "contact_permission": {"status": "do_not_contact", "reason": "Uttryckligt besked", "expected_revision": 0}}
        with patch.object(app_module, "append_customer_state_event", side_effect=fail_permission_once):
            first = self.contact(**payload)
            self.assertEqual(first.status_code, 207, first.get_json())
            retry = self.contact(**payload)
            self.assertEqual(retry.status_code, 200, retry.get_json())
        self.assertEqual(len(self.contact_rows()), 1)
        self.assertEqual(self.insight()["contact_permission"]["status"], "do_not_contact")

    def test_permission_legacy_change_invalidates_open_grant_form(self):
        self.append_contact_row(contact_id="first-refusal", customer_id=CID, result="Återkom ej", follow_up_date="", date_time="2026-07-20 09:00")
        previous = self.insight()["contact_permission"]
        self.append_contact_row(contact_id="renewed-refusal", customer_id=CID, result="Återkom ej", follow_up_date="", date_time="2026-07-26 09:00")
        response = self.client.post("/customer-contact-permission", json={"customer_id": CID, "status": "allowed", "reason": "Tidigare besked",
                                   "expected_revision": previous["revision"], "client_request_id": "stale-grant"})
        self.assertEqual(response.status_code, 409, response.get_json())

    def test_workflow_requires_exact_context_for_virtual_revision_zero(self):
        response = self.client.post("/customer-workflow/action", json={"customer_id": CID, "action": "dismiss",
                                   "expected_revision": 0, "client_request_id": "missing-context"})
        self.assertEqual(response.status_code, 400, response.get_json())

    def test_old_scheduled_event_does_not_reopen_later_explicit_completion(self):
        self.source()
        plan = self.append_planning_row(planned_activity_id="linked", source="follow_up", source_contact_id="promise",
                                       scheduled_at="2026-07-25T08:00:00+02:00", status="completed", completed_contact_id="resolution")
        self.append_contact_row(contact_id="resolution", customer_id=CID, date_time="2026-07-27 09:00", follow_up_date="",
                                planned_activity_id="linked", obligation_id="followup:promise", obligation_outcome="resolved")
        event = {"event_id": "old-event", "kind": "obligation", "entity_id": "followup:promise", "customer_id": CID,
                 "revision": 2, "occurred_at": "2026-07-23T10:00+02:00",
                 "payload_json": json.dumps({"status": "scheduled", "planned_activity_id": "linked"})}
        state = build_customer_state(app_module.get_customer_rows(self.spreadsheet), self.contact_rows(), [plan], [event], as_of=NOW)
        self.assertEqual(state["obligations"][CID][0]["status"], "resolved")
        self.assertEqual(attention_items(state, as_of=NOW), [])
