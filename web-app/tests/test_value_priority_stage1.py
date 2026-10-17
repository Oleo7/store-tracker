"""Value/contact ranking read-model regression tests. No production CRM writes."""
from datetime import date
from pathlib import Path
from unittest import TestCase
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from priority import (build_priority_customers, build_order_features,
                      build_contact_features, apply_workflow_suppressions)
from value_prioritization import contact_factor, business_days_elapsed, complete_deliveries

TODAY = date(2026, 10, 17)


def customer(name="Butik", cid="a", segment="A"):
    return {"customer": name, "customer_id": cid, "customer_segment": segment,
            "sales_person": "Olle", "row": 2}


def delivery(cid, name, day, dfp, reference="one"):
    return {"customer_id": cid, "Customer": name, "Reference": reference,
            "Delivery date": day, "Order date": day, "Total weight": str(dfp), "Total": "4000"}


def contact(cid, name, day, result):
    return {"customer_id": cid, "customer": name, "date_time": day + " 12:00",
            "result": result, "contact_channel": "Telefon", "sales_person": "Olle"}


def get(customers, orders=(), contacts=(), planned=()):
    orders = build_order_features(list(orders))
    contacts = build_contact_features(list(contacts), orders)
    return build_priority_customers(customers, orders, contacts,
                                    "Olle", TODAY, limit=len(customers),
                                    planned_activities=planned)


class ValuePriorityStage1Tests(TestCase):
    def test_unanswered_is_not_real_dialogue_and_creates_retry_action(self):
        row = get([customer()],
                  [delivery("a", "Butik", "2025-12-19", 30)],
                  [contact("a", "Butik", "2026-06-30", "Positiv"),
                   contact("a", "Butik", "2026-09-22", "Ej anträffbar")])[0]
        self.assertEqual(row["days_since_reached"], 109)
        self.assertEqual(row["latest_reached_date"], "2026-06-30")
        self.assertEqual(row["latest_human_contact_date"], "2026-09-22")
        self.assertEqual(row["primary_trigger_type"], "unanswered_retry_due")
        self.assertEqual(row["customer_guidance"]["status_key"], "act_now")
        self.assertGreater(row["contact_priority_score"], 0)

    def test_recent_dialogue_very_low_contact_priority(self):
        recent = get([customer()], contacts=[contact("a", "Butik", "2026-10-15", "Positiv")])[0]
        old = get([customer()], contacts=[contact("a", "Butik", "2026-06-15", "Positiv")])[0]
        self.assertGreater(recent["business_value_score"], 0)
        self.assertLess(recent["contact_priority_score"], 10)
        self.assertGreater(old["contact_priority_score"], recent["contact_priority_score"])

    def test_big_c_buyer_can_be_worth_more_than_a_prospect(self):
        stores = [customer("Aprospekt", "a", "A"), customer("Cstor", "c", "C")]
        orders = [
            delivery("c", "Cstor", "2026-01-10", 50, "1"),
            delivery("c", "Cstor", "2026-02-10", 50, "2"),
            delivery("c", "Cstor", "2026-03-10", 50, "3"),
            delivery("c", "Cstor", "2026-04-10", 50, "4"),
        ]
        rows = {r["customer_id"]: r for r in get(stores, orders)}
        self.assertGreater(rows["c"]["business_value_raw"], rows["a"]["business_value_raw"])

    def test_future_delivery_not_historical(self):
        row = get([customer()], [delivery("a", "Butik", "2026-11-01", 900)])[0]
        self.assertEqual(row["delivery_count"], 0)
        self.assertEqual(row["business_deliveries_used"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "wait")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_plan_keeps_value_and_removes_actionable_score(self):
        planned = [{"customer_id": "a", "customer": "Butik", "status": "planned",
                    "scheduled_at": "2026-10-20T11:00:00", "sales_person": "Olle", "contact_type": "phone"}]
        row = get([customer()], planned=planned)[0]
        self.assertGreater(row["business_value_score"], 0)
        self.assertEqual(row["contact_priority_score"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "planned")

    def test_snooze_never_changes_value(self):
        row = get([customer()])[0]
        updated = apply_workflow_suppressions([row], {"a": "snoozed"})[0]
        self.assertEqual(updated["business_value_score"], row["business_value_score"])
        self.assertEqual(updated["contact_priority_score"], 0)

    def test_negative_contact_still_suppressed(self):
        row = get([customer()], contacts=[contact("a", "Butik", "2026-10-12", "Negativ")])[0]
        self.assertEqual(row["customer_guidance"]["status_key"], "wait")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_three_failed_attempts_request_contact_verification(self):
        rows = [contact("a", "Butik", day, "Ej anträffbar")
                for day in ("2026-09-25", "2026-10-02", "2026-10-12")]
        row = get([customer()], contacts=rows)[0]
        self.assertEqual(row["primary_trigger_type"], "unanswered_verify_contact_due")
        self.assertEqual(row["customer_guidance"]["action_label"], "Kontrollera kontaktväg")

    def test_business_day_retry(self):
        self.assertEqual(business_days_elapsed(date(2026, 10, 16), TODAY), 0)
        self.assertEqual(business_days_elapsed(date(2026, 10, 14), TODAY), 2)

    def test_missing_or_future_volumes_excluded(self):
        self.assertEqual(complete_deliveries({"delivery_dfp_by_date": {
            "2026-01-01": None, "2026-02-01": 5, "2026-11-01": 999}}, TODAY), [5.0])

    def test_separate_fields_and_legacy_score_preserved(self):
        row = get([customer()])[0]
        self.assertIn("priority_score", row)
        self.assertIn("business_value_raw", row)
        self.assertIn("contact_priority_raw", row)
        self.assertEqual(row["value_model_version"], "v3.1-preview")


class ValuePriorityFrontendStage1Tests(TestCase):
    def test_frontend_sort_and_stage2_remains(self):
        html = (Path(__file__).resolve().parents[1] / "index.html").read_text(encoding="utf-8")
        for expected in ['id="list-sort-mode"', 'value="contact">Kontaktprioritet',
                         'value="value">Affärsvärde', 'contactPrioritySort(a, b)',
                         'businessValueSort(a, b)', 'score("contact_priority_score")',
                         'score("business_value_score")', 'id="planning-recommendation"',
                         'id="planning-backlog"']:
            self.assertIn(expected, html)
