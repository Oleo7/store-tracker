"""Skipped appointments are closed attempts, not new outbound instructions."""
from copy import deepcopy
from datetime import timedelta
import json
from unittest import TestCase

from customer_obligations import attention_items, build_customer_state
from test_value_priority_v33_regressions import NOW, booking, contact, customer, replay


class SkippedObligationTests(TestCase):
    def state(self, contacts, plans, events=()):
        return build_customer_state([customer()], contacts, plans, events,
                                    as_of=NOW, owner_user_names={"a": "olle"})

    def model(self, contacts, plans, state):
        return replay(contacts=contacts, plans=plans, obligations=state["obligations"],
                      owners={"a": "olle"})["a"]

    def test_skipped_manual_appointment_cannot_replace_old_review_as_primary(self):
        source = contact(130, contact_id="old", follow_up_date=(NOW.date() - timedelta(days=100)).isoformat())
        skipped = booking(-3, status="skipped", source="manual", contact_type="visit")
        inputs = deepcopy(([source], [skipped]))
        state = self.state(*inputs)
        items = {item["obligation_id"]: item for item in state["obligations"]["a"]}
        self.assertEqual(items["calendar:plan"]["status"], "cancelled")
        self.assertEqual(items["followup:old"]["status"], "needs_review")
        self.assertEqual([item["obligation_id"] for item in attention_items(state, as_of=NOW)], ["followup:old"])
        row = self.model(*inputs, state)
        self.assertNotEqual(row["obligation_id"], "followup:old")
        self.assertGreater(row["work_priority_raw"], 0)
        self.assertTrue(row["can_start_unscheduled_contact"])
        self.assertFalse(row["can_execute_planned_contact"])
        self.assertEqual(inputs, ([source], [skipped]))

        restored = dict(skipped, status="planned")
        restored_state = self.state([source], [restored])
        row = self.model([source], [restored], restored_state)
        self.assertEqual(row["obligation_id"], "calendar:plan")
        self.assertEqual(row["planned_activity_id"], "plan")
        self.assertEqual(row["recommended_contact_type"], "visit")
        self.assertGreater(row["work_priority_raw"], 0)
        self.assertTrue(row["can_execute_planned_contact"])
        self.assertIn("followup:old", row["review_obligation_ids"])

    def test_skipped_linked_appointment_preserves_explicit_promise_for_review(self):
        source = contact(30, contact_id="promise", follow_up_date=(NOW.date() - timedelta(days=3)).isoformat())
        skipped = booking(-3, status="skipped", source="follow_up", source_contact_id="promise")
        state = self.state([source], [skipped])
        item = state["obligations"]["a"][0]
        self.assertEqual(item["obligation_id"], "followup:promise")
        self.assertEqual(item["status"], "needs_review")
        self.assertEqual(item["source_contact_id"], "promise")
        self.assertEqual(item["original_planned_activity_id"], "plan")
        self.assertEqual(item["evidence_quality"], "calendar_closed_promise_unresolved")
        attention = attention_items(state, as_of=NOW)
        self.assertEqual(len(attention), 1)
        self.assertTrue(attention[0]["review_only"])
        row = self.model([source], [skipped], state)
        self.assertGreater(row["work_priority_raw"], 0)
        self.assertTrue(row["requires_handling"])
        self.assertIn("followup:promise", row["review_obligation_ids"])

        restored = dict(skipped, status="planned")
        restored_state = self.state([source], [restored])
        self.assertEqual(restored_state["obligations"]["a"][0]["status"], "scheduled")
        row = self.model([source], [restored], restored_state)
        self.assertEqual(row["obligation_id"], "followup:promise")
        self.assertEqual(row["source_contact_id"], "promise")
        self.assertEqual(row["planned_activity_id"], "plan")
        self.assertTrue(row["can_execute_planned_contact"])

    def test_saved_scheduled_snapshot_cannot_resurrect_a_skipped_activity(self):
        for explicit in (False, True):
            with self.subTest(explicit=explicit):
                moment = (NOW - timedelta(days=2)).isoformat()
                contacts = ([contact(30, contact_id="promise", follow_up_date=(NOW.date() - timedelta(days=3)).isoformat())]
                            if explicit else [])
                skipped = booking(-3, status="skipped", source="follow_up" if explicit else "manual",
                                  source_contact_id="promise" if explicit else "", updated_at=moment)
                oid = "followup:promise" if explicit else "calendar:plan"
                event = {"event_id": "scheduled-event", "kind": "obligation", "entity_id": oid,
                         "customer_id": "a", "occurred_at": moment, "revision": 1,
                         "payload_json": json.dumps({"status": "scheduled", "planned_activity_id": "plan"})}
                state = self.state(contacts, [skipped], [event])
                self.assertEqual(state["obligations"]["a"][0]["status"], "needs_review" if explicit else "cancelled")
                row = self.model(contacts, [skipped], state)
                self.assertNotEqual(row["list_guidance"]["reason_code"], "overdue_planned_activity")
                self.assertFalse(row["can_execute_planned_contact"])
                if explicit:
                    self.assertTrue(row["can_start_unscheduled_contact"])
                restored = dict(skipped, status="planned", updated_at=(NOW - timedelta(days=1)).isoformat())
                restored_state = self.state(contacts, [restored], [event])
                self.assertEqual(restored_state["obligations"]["a"][0]["status"], "scheduled")
                self.assertTrue(self.model(contacts, [restored], restored_state)["can_execute_planned_contact"])

    def test_skipping_one_manual_activity_preserves_another_valid_explicit_scope(self):
        source = contact(30, contact_id="separate", follow_up_date=(NOW.date() - timedelta(days=1)).isoformat())
        skipped = booking(-3, status="skipped", source="manual")
        state = self.state([source], [skipped])
        items = {item["obligation_id"]: item for item in state["obligations"]["a"]}
        self.assertEqual(items["calendar:plan"]["status"], "cancelled")
        self.assertEqual(items["followup:separate"]["status"], "open")
        row = self.model([source], [skipped], state)
        self.assertEqual(row["obligation_id"], "followup:separate")
        self.assertEqual(row["source_contact_id"], "separate")
        self.assertEqual(row["planned_activity_id"], "")
        self.assertGreater(row["work_priority_raw"], 0)
        self.assertTrue(row["can_start_unscheduled_contact"])
