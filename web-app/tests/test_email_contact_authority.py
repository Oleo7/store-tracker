"""Actual model + POST guards, with only the external email transport mocked."""
from datetime import timedelta
from unittest.mock import patch

from test_planning import PlanningApiTestCase, NOW
import app as app_module


CID = "11111111-1111-4111-8111-111111111111"


class EmailContactAuthorityTests(PlanningApiTestCase):
    def snapshot(self):
        return app_module.get_authoritative_priority_snapshot(self.spreadsheet, today=NOW.date(), as_of=NOW)

    def customer_priority(self):
        snapshot = self.snapshot()
        return next(row for row in snapshot["priorities"] if row["customer_id"] == CID)

    def payload(self, **extra):
        return {"draft_id": "fixture-email", "subject": "Fixture subject", "intro_text": "Fixture body",
                "closing_text": "", "order_rows": [], "latest_order_reference": "", "confirm_warnings": True,
                "recipients": [{"email": "fixture@example.com", "selected": True, "greeting_name": "Klara"}], **extra}

    def draft(self):
        return {"customer": {"customer_id": CID, "customer": "Butik A", "customer_number": "C-1", "cancelled": False},
                "email_type": "new_customer", "email_type_label": "Nykund", "latest_order_reference": "",
                "recipients": [{"email": "fixture@example.com", "valid": True, "blocked_reason": ""}],
                "warnings": [], "links": {"product_sheet_url": "https://example.com/products", "stockfiller_url": "https://example.com/order"},
                "cta_labels": {"product_sheet": "Produktblad", "stockfiller": "Beställ"}}

    def seed_email_plan(self, *, rescheduled=False):
        self.append_contact_row(contact_id="email-promise", customer_id=CID, date_time="2026-07-20 09:00",
                                follow_up_date="2026-07-25", result="Positiv",
                                follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")
        if rescheduled:
            source = app_module.customer_state_snapshot(self.spreadsheet)["obligations"][CID][0]
            response = self.client.post("/customer-obligations/action", json={"customer_id": CID,
                "obligation_id": source["obligation_id"], "expected_revision": source["revision"], "action": "reschedule",
                "client_request_id": "reschedule-email", "scheduled_at": "2026-07-27T11:00:00+02:00", "contact_type": "email"})
            self.assertEqual(response.status_code, 200, response.get_json())
            activity = response.get_json()["activity"]
        else:
            activity = self.append_planning_row(planned_activity_id="email-plan", contact_type="email",
                source="follow_up", source_contact_id="email-promise", scheduled_at="2026-07-27T09:00:00+02:00")
        priority = self.customer_priority()
        guidance = priority["list_guidance"]
        self.assertFalse(guidance["can_start_unscheduled_contact"])
        self.assertTrue(guidance["can_execute_planned_contact"])
        return self.payload(planned_activity_id=activity["planned_activity_id"],
            expected_planned_activity_revision=activity["revision"], obligation_id=guidance["obligation_id"],
            expected_obligation_revision=guidance["obligation_revision"])

    def test_negative_order_retry_and_dnc_block_indicators_and_actual_send_without_writes(self):
        cases = [
            ("negative", {"result": "Negativ", "date_time": (NOW - timedelta(days=30)).strftime("%Y-%m-%d %H:%M")}, None, "negative_contact_cooldown"),
            ("retry", {"result": "Ej anträffbar", "date_time": "2026-07-27 09:00"}, None, "unanswered_retry_wait"),
            ("reported-order", {"result": "Order lagd!", "date_time": "2026-07-27 09:00"}, None, "reported_order_sync_wait"),
            ("recent-delivery", None, {"Order date": "2026-07-26", "Delivery date": "2026-07-26"}, "first_order_new"),
            ("unconfirmed-order", None, {"Order date": "2026-07-26", "Delivery date": ""}, "recent_order_pending_confirmation"),
            ("dnc", {"result": "Återkom ej", "date_time": "2026-06-01 09:00"}, None, "do_not_contact"),
        ]
        for name, contact, order, reason in cases:
            with self.subTest(name=name):
                for title in ("sales_activities", "order_rows"):
                    self.spreadsheet.worksheet(title).values = [self.spreadsheet.worksheet(title).values[0]]
                if contact:
                    self.append_contact_row(customer_id=CID, contact_id="guard", follow_up_date="", **contact)
                if order:
                    sheet = self.spreadsheet.worksheet("order_rows")
                    source = {"Reference": "order-guard", "customer_id": CID, "Customer": "Butik A", "Customer number": "C-1",
                              "Quantity": "20", "Total": "400", "SKU": "fixture-sku", **order}
                    sheet.append_row([source.get(column, "") for column in app_module.ORDER_COLUMNS])
                app_module.invalidate_priority_snapshot()
                priority = self.customer_priority()
                self.assertEqual(priority["list_guidance"]["reason_code"], reason)
                status = app_module.build_email_proposal_status({"customer": "Butik A", "email": "fixture@example.com"},
                        priority, {"email_type": "new_customer"}, {}, set(), NOW.date())
                self.assertFalse(status["due"])
                self.assertIn(reason, status["blockers"])
                before = {title: sheet.get_all_values() for title, sheet in self.spreadsheet.sheets.items()}
                with patch.object(app_module, "send_brevo_transactional_email") as transport:
                    response = self.client.post("/customers/2/email-proposal/send", json=self.payload(draft_id=name))
                self.assertEqual(response.status_code, 409, response.get_json())
                self.assertEqual(response.get_json()["reason_code"], reason)
                transport.assert_not_called()
                self.assertEqual(before, {title: sheet.get_all_values() for title, sheet in self.spreadsheet.sheets.items()})

    def test_booked_email_requires_exact_scope_and_revisions(self):
        payload = self.seed_email_plan()
        for changes in ({"planned_activity_id": "different-plan"}, {"obligation_id": "followup:other"},
                        {"expected_planned_activity_revision": 999}, {"expected_obligation_revision": 0},
                        {"planned_activity_id": ""}):
            with self.subTest(changes=changes), patch.object(app_module, "send_brevo_transactional_email") as transport:
                response = self.client.post("/customers/2/email-proposal/send", json={**payload, **changes})
                self.assertEqual(response.status_code, 409, response.get_json())
                transport.assert_not_called()

    def test_actual_booked_email_completes_attempt_preserves_rescheduled_promise_and_retries_once(self):
        payload = self.seed_email_plan(rescheduled=True)
        with patch.object(app_module, "build_email_proposal_draft", return_value=self.draft()), \
             patch.object(app_module, "EMAIL_SEND_MODE", "live"), \
             patch.object(app_module, "send_brevo_transactional_email", return_value="fixture-message") as transport:
            response = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertTrue(response.get_json()["planning_completion_saved"])
            duplicate = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(duplicate.status_code, 200, duplicate.get_json())
            self.assertTrue(duplicate.get_json()["duplicate"])
            changed = self.client.post("/customers/2/email-proposal/send", json={**payload, "intro_text": "Changed body"})
            self.assertEqual(changed.status_code, 409, changed.get_json())
            self.assertEqual(changed.get_json()["error"], "idempotency_payload_mismatch")
            self.assertEqual(transport.call_count, 1)
        self.assertEqual(self.planning_rows()[0]["status"], "completed")
        self.assertEqual(len(self.contact_rows()), 2)
        obligation = app_module.customer_state_snapshot(self.spreadsheet)["obligations"][CID][0]
        self.assertEqual(obligation["status"], "open")
        self.assertEqual(obligation["due_date"], "2026-07-27")
        self.assertEqual(obligation["planned_activity_id"], "")
        self.assertEqual(self.contact_rows()[-1]["purpose"], "scheduled_email_attempt")
        self.assertEqual(self.contact_rows()[-1]["obligation_outcome"], "open")

    def test_secondary_email_booking_uses_its_own_calendar_scope_through_send_and_reload(self):
        payload = self.seed_email_plan()
        self.append_planning_row(planned_activity_id="primary-phone", contact_type="phone",
            scheduled_at="2026-07-27T08:00:00+02:00", created_at="2026-07-20T09:00:00+02:00")
        priority = self.customer_priority()
        self.assertEqual(priority["list_guidance"]["planned_activity_id"], "primary-phone")
        before = {title: sheet.get_all_values() for title, sheet in self.spreadsheet.sheets.items()}
        calendar = self.client.get("/planning/activities?start=2026-07-27&end=2026-07-27&include_followups=0")
        self.assertEqual(calendar.status_code, 200, calendar.get_json())
        self.assertEqual(before, {title: sheet.get_all_values() for title, sheet in self.spreadsheet.sheets.items()})
        activity = next(item for item in calendar.get_json()["activities"] if item["planned_activity_id"] == "email-plan")
        self.assertTrue(activity["can_execute_planned_contact"])
        self.assertEqual(activity["obligation_id"], "followup:email-promise")
        payload.update(expected_planned_activity_revision=activity["revision"],
                       expected_obligation_revision=activity["obligation_revision"], obligation_id=activity["obligation_id"])
        with patch.object(app_module, "build_email_proposal_draft", return_value=self.draft()), \
             patch.object(app_module, "EMAIL_SEND_MODE", "live"), \
             patch.object(app_module, "send_brevo_transactional_email", return_value="fixture-message") as transport:
            response = self.client.post("/customers/2/email-proposal/send", json=payload)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertTrue(response.get_json()["planning_completion_saved"])
        self.assertEqual(transport.call_count, 1)
        statuses = {item["planned_activity_id"]: item["status"] for item in self.planning_rows()}
        self.assertEqual(statuses, {"email-plan": "completed", "primary-phone": "planned"})
        state = app_module.customer_state_snapshot(self.spreadsheet)
        obligation = next(item for item in state["obligations"][CID] if item["obligation_id"] == "followup:email-promise")
        self.assertEqual(obligation["status"], "open")
        self.assertEqual(obligation["planned_activity_id"], "")

    def test_partial_calendar_completion_retry_repairs_without_resending(self):
        payload = self.seed_email_plan()
        self.spreadsheet.worksheet("planned_activities").fail_next_batch_update = RuntimeError("calendar unavailable")
        with patch.object(app_module, "build_email_proposal_draft", return_value=self.draft()), \
             patch.object(app_module, "EMAIL_SEND_MODE", "live"), \
             patch.object(app_module, "send_brevo_transactional_email", return_value="fixture-message") as transport:
            response = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertFalse(response.get_json()["planning_completion_saved"])
            retry = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(retry.status_code, 200, retry.get_json())
            self.assertTrue(retry.get_json()["planning_completion_saved"])
            self.assertEqual(transport.call_count, 1)
        self.assertEqual(len(self.contact_rows()), 2)
        self.assertEqual(self.planning_rows()[0]["status"], "completed")

    def test_partial_contact_log_retry_recovers_from_saved_outbox_and_recipient_evidence(self):
        payload = self.seed_email_plan()
        with patch.object(app_module, "build_email_proposal_draft", return_value=self.draft()), \
             patch.object(app_module, "EMAIL_SEND_MODE", "live"), \
             patch.object(app_module, "send_brevo_transactional_email", return_value="fixture-message") as transport:
            with patch.object(app_module, "build_sales_activity_for_email", side_effect=RuntimeError("contact log unavailable")):
                response = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertFalse(response.get_json()["planning_completion_saved"])
            retry = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(retry.status_code, 200, retry.get_json())
            self.assertTrue(retry.get_json()["planning_completion_saved"])
            self.assertEqual(transport.call_count, 1)
        self.assertEqual(len(self.contact_rows()), 2)
        self.assertEqual(self.planning_rows()[0]["status"], "completed")

    def test_partial_promise_event_retry_does_not_duplicate_completed_attempt(self):
        payload = self.seed_email_plan()
        with patch.object(app_module, "build_email_proposal_draft", return_value=self.draft()), \
             patch.object(app_module, "EMAIL_SEND_MODE", "live"), \
             patch.object(app_module, "send_brevo_transactional_email", return_value="fixture-message") as transport:
            with patch.object(app_module, "append_customer_state_event", side_effect=RuntimeError("event store unavailable")):
                response = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertFalse(response.get_json()["planning_completion_saved"])
            self.assertEqual(self.planning_rows()[0]["status"], "completed")
            retry = self.client.post("/customers/2/email-proposal/send", json=payload)
            self.assertEqual(retry.status_code, 200, retry.get_json())
            self.assertTrue(retry.get_json()["planning_completion_saved"])
            self.assertEqual(transport.call_count, 1)
        self.assertEqual(len(self.contact_rows()), 2)
        events = [row for row in app_module.read_customer_state_events(self.spreadsheet) if row["kind"] == "obligation"]
        self.assertEqual(len(events), 1)
