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
        item=self.item()
        data={"customer_id":CID,"obligation_id":item['obligation_id'],"action":"reschedule","expected_revision":item['revision'],"client_request_id":"partial-virtual", "scheduled_at":"2026-07-28T10:00+02:00","contact_type":"phone"}
        with patch.object(app_module,"append_customer_state_event",side_effect=RuntimeError("Injected event failure")):
            self.assertEqual(self.client.post("/customer-obligations/action",json=data).status_code,503)
        response=self.client.post("/customer-obligations/action",json=data)
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(len(self.planning_rows()),1)
        self.assertEqual(self.item()['status'],'scheduled')


    def test_owner_mismatch_never_creates_state_storage(self):
        self.source()
        self.login("sofia")
        response = self.client.post("/customer-obligations/action", json={"customer_id": CID, "obligation_id": "followup:promise",
                                    "action": "skip", "client_request_id": "wrong-owner", "expected_revision": 0})
        self.assertEqual(response.status_code, 404)
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)


    def test_legacy_source_date_change_rejects_stale_cas_revision(self):
        self.source()
        original = self.item()
        sheet = self.spreadsheet.worksheet("sales_activities")
        sheet.update_cell(2, sheet.values[0].index("follow_up_date") + 1, "2026-07-30")
        response = self.client.post("/customer-obligations/action", json={
            "customer_id": CID, "obligation_id": original["obligation_id"], "action": "skip",
            "client_request_id": "stale-date", "expected_revision": original["revision"]})
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(response.get_json()["code"], "revision_conflict")
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET, self.spreadsheet.sheets)


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
        self.assertEqual(state["contact_permissions"], {})


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
