"""A customer's allowed promise cannot authorize another contact scope."""
from copy import deepcopy
from unittest.mock import patch

import app as app_module
from test_planning import PlanningApiTestCase, NOW


CID = "11111111-1111-4111-8111-111111111111"
OLDER = "followup:older-promise"
NEWER = "followup:new-agreement"


class ObligationContactScopeTests(PlanningApiTestCase):
    def sources(self):
        self.append_contact_row(customer_id=CID, contact_id="older-promise", date_time="2026-07-12 09:00",
                                result="Positiv", follow_up_date="2026-07-26")
        # Negative dialogue can include one explicit new agreement. It does not
        # grant permission for every earlier promise still in the CRM.
        self.append_contact_row(customer_id=CID, contact_id="new-agreement", date_time="2026-07-20 09:00",
                                result="Negativ", follow_up_date="2026-07-25")

    def snapshot(self):
        return app_module.get_authoritative_priority_snapshot(self.spreadsheet, today=NOW.date(), as_of=NOW)

    def attention(self, suffix=""):
        response = self.client.get("/planning/attention?user_name=olle" + suffix)
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def test_attention_practises_each_promises_own_negative_exception(self):
        self.sources()
        original_sources = {name: deepcopy(sheet.values) for name, sheet in self.spreadsheet.sheets.items()}
        snapshot = self.snapshot()
        primary = next(row for row in snapshot["priorities"] if row["customer_id"] == CID)
        self.assertEqual(primary["obligation_id"], NEWER)
        self.assertTrue(primary["can_start_unscheduled_contact"])
        self.assertTrue(primary["list_guidance"]["explicit_agreement_overrides_negative"])
        items = {item["obligation_id"]: item for item in self.attention()["items"]}
        self.assertTrue(items[NEWER]["can_contact_now"])
        self.assertFalse(items[NEWER]["review_only"])
        self.assertFalse(items[OLDER]["can_contact_now"])
        self.assertFalse(items[OLDER]["can_execute_planned_contact"])
        self.assertEqual(items[OLDER]["contact_block_reason"], "negative_contact_cooldown")
        self.assertTrue(items[OLDER]["review_only"])
        self.assertEqual(original_sources, {name: sheet.values for name, sheet in self.spreadsheet.sheets.items()})

    def test_missed_secondary_booking_cannot_inherit_a_primary_agreement(self):
        self.sources()
        activity = self.append_planning_row(planned_activity_id="older-visit", contact_type="phone",
            source="follow_up", source_contact_id="older-promise", scheduled_at="2026-07-26T09:00:00+02:00")
        snapshot = self.snapshot()
        original = deepcopy(snapshot)
        primary = next(row for row in snapshot["priorities"] if row["customer_id"] == CID)
        self.assertEqual(primary["obligation_id"], NEWER)
        decision = app_module.route_planned_contact_decision(primary, activity, snapshot=snapshot, route_date=NOW.date())
        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason_code"], "negative_contact_cooldown")
        self.assertEqual(decision["guidance"]["obligation_id"], OLDER)
        self.assertEqual(snapshot, original)
        items = {item["obligation_id"]: item for item in self.attention()["items"]}
        self.assertEqual(items[OLDER]["item_type"], "missed_calendar_activity")
        self.assertFalse(items[OLDER]["can_execute_planned_contact"])
        self.assertFalse(items[OLDER]["can_contact_now"])
        self.assertTrue(items[NEWER]["can_contact_now"])
        response = self.client.get("/planning/activities?user_name=olle&start=2026-07-26&end=2026-07-26&include_followups=0")
        self.assertEqual(response.status_code, 200, response.get_json())
        actual_activity = next(row for row in response.get_json()["activities"] if row["planned_activity_id"] == "older-visit")
        self.assertFalse(actual_activity["can_execute_planned_contact"])

    def test_primary_and_secondary_checks_share_features_and_do_not_mutate_snapshot(self):
        self.sources()
        snapshot = self.snapshot()
        original = deepcopy(snapshot)
        primary = next(row for row in snapshot["priorities"] if row["customer_id"] == CID)
        items = {item["obligation_id"]: item for item in snapshot["customer_state"]["obligations"][CID]}
        prepared = {}
        with patch.object(app_module, "build_order_features", wraps=app_module.build_order_features) as orders, patch.object(
            app_module, "build_priority_customers", wraps=app_module.build_priority_customers
        ) as model, patch.object(app_module, "get_spreadsheet_with_retry", side_effect=AssertionError("Pure decision must not perform I/O")):
            allowed = app_module.obligation_contact_decision(primary, items[NEWER], snapshot=snapshot, as_of=NOW, prepared=prepared)
            blocked = app_module.obligation_contact_decision(primary, items[OLDER], snapshot=snapshot, as_of=NOW, prepared=prepared)
        self.assertTrue(allowed["allowed"])
        self.assertFalse(blocked["allowed"])
        self.assertEqual(orders.call_count, 1)
        self.assertEqual(model.call_count, 1)
        self.assertEqual(len(model.call_args.args[0]), 1)
        self.assertEqual(model.call_args.kwargs["obligation_states"][CID][0]["obligation_id"], OLDER)
        self.assertEqual(snapshot, original)

    def test_attention_only_calculates_the_requested_page(self):
        self.sources()
        with patch.object(app_module, "obligation_contact_decision", wraps=app_module.obligation_contact_decision) as decision:
            result = self.attention("&limit=1&offset=1")
        self.assertEqual(result["total"], 2)
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(decision.call_count, 1)

    def test_existing_inbound_history_can_be_recorded_while_outbound_scope_is_paused(self):
        self.sources()
        older = next(item for item in self.attention()["items"] if item["obligation_id"] == OLDER)
        self.assertFalse(older["can_contact_now"])
        response = self.client.post("/customers/Butik%20A/contacts", json={
            "customer_id": CID, "contact_channel": "Telefon", "result": "Neutral",
            "date_time": "2026-07-15 10:00", "comment": "Historik: kunden ringde oss den15 juli.",
            "client_request_id": "record-inbound-history", "obligation_id": OLDER,
            "expected_obligation_revision": older["revision"], "obligation_outcome": "open",
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        logged = next(row for row in self.contact_rows() if row["comment"] == "Historik: kunden ringde oss den15 juli.")
        self.assertEqual(logged["date_time"], "2026-07-15 10:00")
        self.assertEqual(logged["result"], "Neutral")
        self.assertEqual(logged["obligation_id"], OLDER)
        refreshed = next(item for item in self.attention()["items"] if item["obligation_id"] == OLDER)
        self.assertFalse(refreshed["can_contact_now"])
        self.assertEqual(refreshed["contact_block_reason"], "negative_contact_cooldown")
