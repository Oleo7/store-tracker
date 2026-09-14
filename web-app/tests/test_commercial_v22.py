"""Focused v2.2 boundaries using the existing priority and Sheets fixtures."""
from datetime import date, datetime, timedelta
from unittest import TestCase
from unittest.mock import patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as app_module
from commercial_orders import order_volume
from priority import build_order_features, build_priority_customers, calculate_priority_score_v2
from sales_coaching import group_logical_orders
from test_priority_v2 import customer, order, contact, priorities
from test_priority_v4 import scored, email_rows, customer as email_customer
from test_planning import default_spreadsheet


class CommercialV22Tests(TestCase):
    today = date(2026, 8, 10)

    def test_history_delivery_identity_future_and_repeat_reactivation(self):
        stores = [customer("Store", "s", "C"), customer("Benchmark", "b", row=3)]
        for count, history in ((0, 0), (1, 60), (2, 100)):
            rows = [order(str(i), "Store", f"2026-01-{1 + i * 20:02d}", 1, "s") for i in range(count)]
            if count:
                rows.append(order("other-reference-same-delivery", "Store", "2026-01-01", 1, "s"))
            rows.append(order("benchmark", "Benchmark", "2026-07-01", 1000, "b"))
            ranked = priorities(stores, rows, today=self.today)
            item = next(row for row in ranked if row["customer_id"] == "s")
            self.assertEqual(item["history_index"], history)
            self.assertLess(item["priority_score"], 70)
            if count:
                self.assertEqual(item["primary_trigger_type"], "single_order_reactivation_due" if count == 1 else "repeat_reactivation_due")
            self.assertEqual(ranked, priorities(stores, rows, today=self.today))
            future = rows + [order("future", "Store", "2026-09-01", 10, "s")]
            item = next(row for row in priorities(stores, future, today=self.today) if row["customer_id"] == "s")
            self.assertEqual(item["history_index"], history)
            self.assertFalse(item["recommendation_eligible"])
            self.assertEqual(item["recommendation_suppression_reason"], "future_delivery")

    def test_reactivation_contact_gap_and_real_suppressions(self):
        stores = [customer("Store", "s", "B")]
        rows = [order("old", "Store", "2026-01-01", 10, "s")]
        for age in (2, 29, 30):
            item = priorities(stores, rows, [contact("Store", self.today - timedelta(days=age))], today=self.today)[0]
            self.assertEqual("single_order_reactivation_due" in item["covered_trigger_keys"], age >= 30)
        for reason in ("snoozed", "dismissed"):
            item = build_priority_customers(stores, build_order_features(rows), {}, "Olle", self.today, workflow_suppressions={"s": reason})[0]
            self.assertEqual(item["history_index"], 60)
            self.assertFalse(item["recommendation_eligible"])
        negative = priorities(stores, rows, [contact("Store", self.today - timedelta(days=30), "Negativ")], today=self.today)[0]
        self.assertFalse(negative["recommendation_eligible"])

    def test_total_weight_contract_consistent_across_calculations(self):
        stores = [customer("Store", "s")]
        for volume, expected, history in (("7", 7, 60), ("0", 0, 0), (None, None, 60), ("invalid", None, 60), ("-7", -7, 0)):
            raw = {**order("ref", "Store", "2026-01-01", 99, "s"), "Total weight": volume, "customer_id": "s"}
            self.assertEqual(order_volume(raw), expected)
            features = build_order_features([raw])["store"]
            self.assertEqual(features["order_identity_count"], 1)
            self.assertEqual(features["latest_order_dfp"], expected)
            item = priorities(stores, [raw], today=self.today)[0]
            self.assertEqual(item["history_index"], history)
            coaching = group_logical_orders([raw], stores)["orders"]
            calibration = app_module.group_customer_orders([raw])
            self.assertEqual(len(coaching), len(calibration))
            if history:
                self.assertEqual(coaching[0]["dfp"], expected or 0)
                self.assertEqual(calibration[0]["dfp"], expected or 0)
                self.assertEqual(coaching[0]["volume_missing"], expected is None)
            else:
                self.assertFalse(coaching)
        credit = {**raw, "Total": "-100", "Total weight": "7"}
        self.assertEqual(priorities(stores, [credit], today=self.today)[0]["history_index"], 0)
        self.assertFalse(group_logical_orders([credit], stores)["orders"])

    def test_policy_rollback_changes_only_weights(self):
        stores = [customer("Store", "s")]
        orders = build_order_features([order("old", "Store", "2026-01-01", 10, "s")])
        old = build_priority_customers(stores, orders, {}, "Olle", self.today, scoring_version="v2.1")[0]
        new = build_priority_customers(stores, orders, {}, "Olle", self.today, scoring_version="v2.2")[0]
        for key in ("history_index", "primary_trigger_type", "recommendation_suppression_reason", "delivery_count"):
            self.assertEqual(old[key], new[key])
        self.assertEqual(calculate_priority_score_v2(60, 100, 100, policy="v2.1"), 74)
        self.assertEqual(calculate_priority_score_v2(60, 100, 100, 60, policy="v2.2"), 72)

    def test_positive_dialogue_day_30_31(self):
        for age in (30, 31):
            item = priorities([customer("Store", "s", "C")], contacts=[contact("Store", self.today - timedelta(days=age), "Positiv")], today=self.today)[0]
            self.assertEqual("positive_dialogue_followup" in item["covered_trigger_keys"], age == 30)
            self.assertEqual(item["intent_timing"], 55 if age == 30 else 45)
            if age == 31:
                self.assertNotIn("Positiv dialog", item["next_action"].get("reason", ""))

    def test_click_window_open_only_and_later_handling(self):
        for kind in ("stockfiller", "product", "open"):
            messages, recipients = email_rows(kind=kind)
            for age in (2, 3, 14, 15):
                today = date(2026, 8, 2) + timedelta(days=age)
                feature = app_module.build_email_engagement_snapshot(messages, recipients, [], today=today, customers=[email_customer()])["id:cid-1"]
                result = scored(today=today, email_feature=feature)[0]
                active = kind != "open" and 3 <= age <= 14
                self.assertEqual(bool(result["email_intent_trigger"]), active)
                self.assertEqual(result["intent_timing"], 60 + ((8 if kind == "stockfiller" else 4) if active else 0))
                later_contact = {"customer_id": "cid-1", "customer": "Butik", "date_time": "2026-08-03 10:00", "result": "Neutral"}
                self.assertFalse(scored(today=today, email_feature=feature, contacts=[later_contact])[0]["email_intent_trigger"])
                later_order = {**order("later", "Butik", "2026-08-03", 2, "100"), "customer_id": "cid-1"}
                self.assertFalse(scored(today=today, email_feature=feature, orders=[later_order])[0]["email_intent_trigger"])

    def test_calibration_calendar_boundaries_and_no_event_double_count(self):
        stores = [customer("Store", "s")]
        events = [{"event_id": "created", "event_type": "suggestion_created", "customer_id": "s", "occurred_at": "2026-08-01 10:00"},
                  {"event_id": "planned", "event_type": "suggestion_planned", "customer_id": "s", "occurred_at": "2026-08-01 11:00"}]
        for days in (0, 10, 11):
            rows = [order("ref", "Store", (date(2026, 8, 1) + timedelta(days=days)).isoformat(), 7, "s")]
            outcomes = app_module.build_calibration_rows(events, rows, stores, today=date(2026, 9, 1))
            self.assertTrue(all(row["order_within_10d"] == (days <= 10) for row in outcomes))
            self.assertEqual(sum(row["credited_order_count_10d"] for row in outcomes), int(days <= 10))
            self.assertTrue(all(row["window_closed_10d"] and not row["is_human_contact"] for row in outcomes))
        newer = {**events[0], "event_id": "newer", "occurred_at": "2026-08-02 15:00"}
        outcomes = app_module.build_calibration_rows(
            events + [newer], [order("ref", "Store", "2026-08-03", 7, "s")], stores,
            today=date(2026, 9, 1),
        )
        self.assertEqual([row["event_id"] for row in outcomes if row["credited_order_count_10d"]], ["newer"])
        for today, closed in ((date(2026, 8, 11), False), (date(2026, 8, 12), True)):
            self.assertEqual(app_module.build_calibration_rows(events, [], stores, today=today)[0]["window_closed_10d"], closed)


class RouteAndPlanningV22Tests(TestCase):
    def test_channel_invariant_suppression_and_replaced_overdue(self):
        sheet = default_spreadsheet()
        owner = {"user_name": "olle", "name": "Olle"}
        now = datetime(2026, 8, 10, 12, tzinfo=app_module.STOCKHOLM_ZONE)
        stores = app_module.get_customer_rows(sheet)
        owned = [row for row in stores if row["sales_person"] == "Olle"]
        target = owned[0]["customer_id"]
        scored_rows = [{**row, "priority_score": 50, "primary_trigger_type": "", "recommendation_eligible": True} for row in owned]
        past = {"customer_id": target, "planned_activity_id": "old", "scheduled_at": "2026-08-09T09:00:00+02:00", "status": "planned", "contact_type": "phone", **owner}
        later_contact = {"customer_id": target, "date_time": "2026-08-09 09:00:01"}
        snapshot = {"customers": stores, "priorities": scored_rows, "contact_rows": [later_contact]}
        with app_module.app.test_request_context(), patch.object(app_module, "stockholm_now", return_value=now), patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(app_module, "priority_workflow_suppressions", return_value={}), patch.object(app_module, "read_planned_activity_snapshot", return_value=(None, [], [(2, past)])):
            pools = []
            for channel in ("phone", "email", "visit", None):
                for row in scored_rows:
                    row["recommended_contact_type"] = channel
                result, error = app_module.build_route_optimization_inputs(spreadsheet=sheet, owner=owner, route_date=now.date(), start=app_module.Coordinate(57.7, 11.9))
                self.assertIsNone(error)
                pools.append(result["shipments"])
            self.assertTrue(all(pool == pools[0] for pool in pools))
            self.assertIn(target, {row["customer_id"] for row in pools[0]})
            scored_rows[0]["recommendation_suppression_reason"] = "recent_human_contact"
            result, error = app_module.build_route_optimization_inputs(spreadsheet=sheet, owner=owner, route_date=now.date(), start=app_module.Coordinate(57.7, 11.9))
            self.assertIsNone(error)
            self.assertNotIn(target, {row["customer_id"] for row in result["shipments"]})

    def test_effective_overdue_exact_timestamp_status_and_email(self):
        owner = {"user_name": "olle", "name": "Olle"}
        now = datetime(2026, 8, 10, 12, tzinfo=app_module.STOCKHOLM_ZONE)
        past = {"customer_id": "s", "planned_activity_id": "old", "scheduled_at": "2026-08-10T09:00:00+02:00", "status": "planned", **owner}
        for timestamp, email, visible in (("08:59:59", "", True), ("09:00:00", "", True), ("09:00:01", "", False), ("09:00:01", "event", True)):
            contacts = [{"customer_id": "s", "date_time": f"2026-08-10 {timestamp}", "email_id": email}]
            active, overdue = app_module.active_planned_activity_queue_state([past], owner, contact_rows=contacts, now=now)
            self.assertEqual(bool(overdue), visible)
            self.assertEqual("s" in active, visible)
        for status in ("planned", "completed", "cancelled", "skipped"):
            future = {**past, "planned_activity_id": "future", "scheduled_at": "2026-08-11T09:00:00+02:00", "status": status}
            _, overdue = app_module.active_planned_activity_queue_state([past, future], owner, now=now)
            self.assertEqual(bool(overdue), status != "planned")
            self.assertEqual(past["status"], "planned")
