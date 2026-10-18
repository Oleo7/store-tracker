"""Actual backend contact decisions must govern both route builders.

Only sheet/network infrastructure is replaced. Orders, contacts, obligations
and scoped appointment decisions run through the production model.
"""
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from zoneinfo import ZoneInfo
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as app_module
from tests.test_planning import ConstantRoadProvider, default_spreadsheet


NOW = datetime(2026, 10, 16, 10, tzinfo=ZoneInfo("Europe/Stockholm"))
CID = "11111111-1111-4111-8111-111111111111"
OWNER = {"user_name": "olle", "name": "Olle", "role": "Säljare", "admin": False}
GPS = app_module.Coordinate(57.7, 11.9)


def contact(days=30, result="Neutral", **extra):
    return {"customer_id": CID, "customer": "Butik A", "customer_number": "C-1",
            "contact_id": "contact-" + str(days), "sales_person": "Olle", "contact_channel": "Telefon",
            "date_time": (NOW - timedelta(days=days)).isoformat(), "result": result, **extra}


def order(days=1, **extra):
    day = (NOW.date() - timedelta(days=days)).isoformat()
    return {"customer_id": CID, "Customer": "Butik A", "Customer number": "C-1", "Reference": "order-1",
            "Order date": day, "Delivery date": day, "Total weight": 21, "Unit": "Dfp",
            "Total": 4000, "SKU": "Vanilj", **extra}


def appointment(days=0, **extra):
    return {"planned_activity_id": "visit-a", "customer_id": CID, "customer": "Butik A",
            "customer_row": 2, "customer_number": "C-1", "user_name": "olle", "sales_person": "Olle",
            "scheduled_at": (NOW.replace(hour=13) + timedelta(days=days)).isoformat(),
            "status": "planned", "contact_type": "visit", "source": "manual",
            "appointment_confirmed": "N", "time_is_estimated": "N", "duration_minutes": 20,
            "revision": 1, **extra}


class RouteContactPolicyTests(TestCase):
    def setUp(self):
        self.spreadsheet = default_spreadsheet()
        self.context = app_module.app.test_request_context()
        self.context.push()
        self.addCleanup(self.context.pop)
        for name, value in (("stockholm_now", NOW), ("current_user", OWNER)):
            clock = patch.object(app_module, name, return_value=value)
            clock.start()
            self.addCleanup(clock.stop)

    def snapshot(self, *, orders=(), contacts=(), plans=()):
        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        sheet.values = sheet.values[:1] + [[plan.get(key, "") for key in app_module.PLANNED_ACTIVITY_COLUMNS]
                                          for plan in plans]
        customers = app_module.get_customer_rows(self.spreadsheet)
        priorities, engagement = app_module.build_current_priority_snapshot(
            customers=customers, users=app_module.get_user_rows(self.spreadsheet),
            order_rows=list(orders), contact_rows=list(contacts), message_rows=[], recipient_rows=[],
            planned_activity_rows=list(plans), today=NOW.date(), as_of=NOW,
        )
        return {"customers": customers, "priorities": priorities, "order_rows": list(orders),
                "contact_rows": list(contacts), "planned_activity_rows": list(plans),
                "customer_state_events": [], "message_rows": [], "recipient_rows": [],
                "email_engagement": engagement, "snapshot_created_at": NOW.isoformat()}

    def priority(self, snapshot):
        return next(row for row in snapshot["priorities"] if row["customer_id"] == CID)

    def route(self, snapshot, *, legacy=False, required=(), day=None):
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(
            app_module, "priority_workflow_suppressions", return_value={}
        ), patch.object(app_module, "get_route_travel_time_provider", return_value=ConstantRoadProvider()):
            if legacy:
                return app_module.calculate_route_proposal_for_user(
                    spreadsheet=self.spreadsheet, start=GPS, client_requested_rows=(2, 4), user=OWNER,
                    owner=OWNER, route_date=day or NOW.date(), required_rows=required,
                )
            return app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet, owner=OWNER, route_date=day or NOW.date(), start=GPS,
            )

    def test_required_visits_cannot_override_contact_restrictions(self):
        cases = [
            ("do_not_contact", [], [contact(30, "Återkom ej")]),
            ("negative_contact_cooldown", [], [contact(30, "Negativ")]),
            ("unanswered_retry_wait", [], [contact(0, "Ej anträffbar")]),
            ("recent_order_pending_confirmation", [order(**{"Delivery date": ""})], []),
            ("future_delivery", [order(**{"Delivery date": (NOW.date() + timedelta(days=2)).isoformat()})], []),
        ]
        for reason, orders, contacts in cases:
            snapshot = self.snapshot(orders=orders, contacts=contacts, plans=[appointment()])
            priority = self.priority(snapshot)
            self.assertEqual(priority["list_guidance"]["reason_code"], reason)
            self.assertFalse(priority["can_execute_planned_contact"])
            self.assertEqual(priority["work_priority_raw"], 0)
            for legacy in (False, True):
                with self.subTest(reason=reason, legacy=legacy):
                    result, error = self.route(snapshot, legacy=legacy, required=(2,))
                    self.assertIsNone(result)
                    self.assertEqual(error[1], 409)
                    self.assertEqual(error[0].get_json()["code"],
                                     "do_not_contact" if reason == "do_not_contact" else "route_required_contact_blocked")

    def test_other_day_phone_does_not_bypass_first_delivery_wait(self):
        snapshot = self.snapshot(orders=[order()], plans=[appointment(1, contact_type="phone")])
        priority = self.priority(snapshot)
        self.assertEqual(priority["list_guidance"]["status_key"], "planned")
        self.assertGreater(priority["priority_score"], 0)  # Old numeric score is not permission.
        self.assertEqual(priority["work_priority_raw"], 0)
        self.assertFalse(priority["work_can_start_unscheduled_contact"])
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                result, error = self.route(snapshot, legacy=legacy)
                self.assertIsNone(error)
                rows = result["stops"] if legacy else result["shipments"]
                self.assertFalse(any(row.get("row") == 2 or row.get("customer_id") == CID for row in rows))

    def test_other_day_phone_preserves_eligible_optional_visit(self):
        snapshot = self.snapshot(plans=[appointment(1, contact_type="phone")])
        self.assertTrue(self.priority(snapshot)["work_can_start_unscheduled_contact"])
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                result, error = self.route(snapshot, legacy=legacy)
                self.assertIsNone(error)
                rows = result["stops"] if legacy else result["shipments"]
                self.assertTrue(any(row.get("row") == 2 or row.get("customer_id") == CID for row in rows))

    def test_secondary_valid_visit_uses_its_own_decision_and_obligation(self):
        phone = appointment(contact_type="phone", planned_activity_id="phone-a",
                            scheduled_at=NOW.replace(hour=11).isoformat())
        visit = appointment()
        snapshot = self.snapshot(plans=[phone, visit])
        priority = self.priority(snapshot)
        self.assertEqual(priority["list_guidance"]["planned_activity_id"], "phone-a")
        self.assertEqual(priority["contact_priority_raw"], 0)
        decision = app_module.route_planned_contact_decision(priority, visit, snapshot=snapshot, route_date=NOW.date())
        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["guidance"]["planned_activity_id"], "visit-a")
        self.assertEqual(decision["priority"]["planning_seed"]["planned_activity_id"], "visit-a")
        self.assertGreater(decision["priority"]["planning_seed"]["expected_revision"], 0)
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                result, error = self.route(snapshot, legacy=legacy, required=(2,))
                self.assertIsNone(error, error)
                rows = result["stops"] if legacy else result["shipments"]
                self.assertTrue(any(row["required"] and (row.get("row") == 2 or row.get("customer_id") == CID)
                                    for row in rows))

    def test_newer_agreement_cannot_grant_permission_to_an_unrelated_visit(self):
        source = contact(30, "Negativ", contact_id="new-agreement", follow_up_date=NOW.date().isoformat())
        phone = appointment(contact_type="phone", planned_activity_id="phone-a", source_contact_id="new-agreement",
                            scheduled_at=NOW.replace(hour=11).isoformat())
        visit = appointment()
        snapshot = self.snapshot(contacts=[source], plans=[phone, visit])
        priority = self.priority(snapshot)
        self.assertTrue(priority["can_execute_planned_contact"])
        self.assertEqual(priority["list_guidance"]["planned_activity_id"], "phone-a")
        decision = app_module.route_planned_contact_decision(priority, visit, snapshot=snapshot, route_date=NOW.date())
        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason_code"], "negative_contact_cooldown")
        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                result, error = self.route(snapshot, legacy=legacy, required=(2,))
                self.assertIsNone(result)
                self.assertEqual(error[0].get_json()["code"], "route_required_contact_blocked")

    def test_future_visit_rechecks_expiring_pause_on_its_own_day(self):
        for reason, contacts, days in (("negative_contact_cooldown", [contact(30, "Negativ")], 35),
                                       ("unanswered_retry_wait", [contact(0, "Ej anträffbar")], 7)):
            visit = appointment(days)
            snapshot = self.snapshot(contacts=contacts, plans=[visit])
            priority = self.priority(snapshot)
            self.assertEqual(priority["list_guidance"]["reason_code"], reason)
            self.assertFalse(priority["can_execute_planned_contact"])
            future_day = (NOW + timedelta(days=days)).date()
            for legacy in (False, True):
                with self.subTest(reason=reason, legacy=legacy):
                    result, error = self.route(snapshot, legacy=legacy, required=(2,), day=future_day)
                    self.assertIsNone(error, error)
                    rows = result["stops"] if legacy else result["shipments"]
                    self.assertTrue(any(row["required"] and (row.get("row") == 2 or row.get("customer_id") == CID)
                                        for row in rows))

    def test_manual_required_flag_cannot_bypass_wait_without_an_appointment(self):
        snapshot = self.snapshot(contacts=[contact(30, "Negativ")])
        result, error = self.route(snapshot, legacy=True, required=(2,))
        self.assertIsNone(result)
        self.assertEqual(error[0].get_json()["code"], "route_required_contact_blocked")

    def test_scoped_decisions_reuse_features_and_never_recalculate_the_portfolio(self):
        plans = [appointment(planned_activity_id="first", contact_type="phone", scheduled_at=NOW.replace(hour=11).isoformat()),
                 appointment(planned_activity_id="second"), appointment(planned_activity_id="third", contact_type="email")]
        snapshot = self.snapshot(plans=plans)
        original = deepcopy(snapshot)
        prepared = {}
        with patch.object(app_module, "build_order_features", wraps=app_module.build_order_features) as orders, patch.object(
            app_module, "build_priority_customers", wraps=app_module.build_priority_customers
        ) as model:
            for plan in plans[1:]:
                decision = app_module.route_planned_contact_decision(self.priority(snapshot), plan, snapshot=snapshot,
                    route_date=NOW.date(), prepared=prepared)
                self.assertTrue(decision["allowed"])
        self.assertEqual(orders.call_count, 1)
        self.assertEqual(model.call_count, 2)
        self.assertTrue(all(len(call.args[0]) == 1 for call in model.call_args_list))
        self.assertEqual(snapshot, original)
