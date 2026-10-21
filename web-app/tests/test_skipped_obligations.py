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

    def test_skipped_service_booking_has_no_separate_open_case(self):
        model=self.state([], [booking(-2,status='skipped',note='Reklamation: lovat demo')])
        self.assertEqual(attention_items(model,as_of=NOW),[])
        self.assertEqual(model['latest_reached'],{})

    def test_legacy_scheduled_event_cannot_reopen_skipped_calendar(self):
        event={'kind':'obligation','customer_id':'a','entity_id':'calendar:plan',
               'occurred_at':(NOW-timedelta(days=5)).isoformat(),
               'payload_json':json.dumps({'status':'scheduled','planned_activity_id':'plan'})}
        model=self.state([], [booking(-2,status='skipped')], [event])
        self.assertEqual(model['obligations']['a'][0]['status'],'skipped')
        self.assertEqual(attention_items(model,as_of=NOW),[])

    def test_older_skip_cannot_dispose_of_newer_miss(self):
        model=self.state([], [booking(-5,status='skipped',planned_activity_id='old'),booking(-2)])
        self.assertEqual([r['planned_activity_id'] for r in attention_items(model,as_of=NOW)],['plan'])

    def test_future_skip_does_not_clear_past_work_or_record_contact(self):
        model=self.state([], [booking(-2,planned_activity_id='old'),booking(2,status='skipped')])
        self.assertEqual([r['planned_activity_id'] for r in attention_items(model,as_of=NOW)],['old'])
        self.assertEqual(model['latest_attempts'],{})
