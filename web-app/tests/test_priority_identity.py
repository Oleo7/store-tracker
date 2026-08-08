from datetime import date
from pathlib import Path
import sys
from unittest import TestCase


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from priority import build_contact_features, build_order_features, build_priority_customers  # noqa: E402


TODAY = date(2026, 8, 8)


def customer(customer_id, number, name, row):
    return {
        "row": row,
        "customer_id": customer_id,
        "customer_number": number,
        "customer": name,
        "sales_person": "Olle",
        "customer_segment": "B",
    }


def order(customer_id, number, name, reference, delivered):
    return {
        "customer_id": customer_id,
        "Customer number": number,
        "Customer": name,
        "Reference": reference,
        "Order date": delivered,
        "Delivery date": delivered,
        "Quantity": "20",
        "Total weight": "20",
        "Total": "400",
    }


def contact(customer_id, name, contact_id, when):
    return {
        "customer_id": customer_id,
        "customer": name,
        "contact_id": contact_id,
        "date_time": when,
        "sales_person": "Olle",
        "contact_channel": "Telefon",
        "result": "Neutral",
    }


def score(customers, orders=(), contacts=()):
    order_features = build_order_features(list(orders))
    contact_features = build_contact_features(list(contacts), order_features)
    return {
        row["customer_id"]: row
        for row in build_priority_customers(
            list(customers), order_features, contact_features, "Olle", TODAY,
            limit=len(customers),
        )
    }


class CanonicalPriorityIdentityTests(TestCase):
    def test_same_name_customers_keep_orders_and_contacts_separate_by_id(self):
        customers = [
            customer("cid-1", "100", "Samma butik", 2),
            customer("cid-2", "200", "Samma butik", 3),
        ]
        result = score(
            customers,
            orders=[order("cid-1", "100", "Samma butik", "A", "2026-07-01")],
            contacts=[contact("cid-2", "Samma butik", "contact-2", "2026-08-01 10:00")],
        )

        self.assertEqual(result["cid-1"]["order_count"], 1)
        self.assertEqual(result["cid-1"]["latest_human_contact_id"], "")
        self.assertEqual(result["cid-2"]["order_count"], 0)
        self.assertEqual(result["cid-2"]["latest_human_contact_id"], "contact-2")

    def test_positive_dialogue_does_not_cross_same_name_collision(self):
        customers = [
            customer("cid-1", "100", "Samma butik", 2),
            customer("cid-2", "200", "Samma butik", 3),
        ]
        warm = contact("cid-1", "Samma butik", "warm-a", "2026-08-01 10:00")
        warm["result"] = "Positiv"
        result = score(customers, contacts=[warm])
        self.assertEqual(result["cid-1"]["primary_trigger_type"], "positive_dialogue_followup")
        self.assertNotEqual(result["cid-2"]["primary_trigger_type"], "positive_dialogue_followup")

    def test_negative_cooldown_does_not_cross_same_name_collision(self):
        customers = [
            customer("cid-1", "100", "Samma butik", 2),
            customer("cid-2", "200", "Samma butik", 3),
        ]
        negative = contact("cid-1", "Samma butik", "negative-a", "2026-08-01 10:00")
        negative["result"] = "Negativ"
        result = score(customers, contacts=[negative])
        self.assertEqual(result["cid-1"]["recommendation_suppression_reason"], "negative_contact_cooldown")
        self.assertNotEqual(result["cid-2"]["recommendation_suppression_reason"], "negative_contact_cooldown")

    def test_renamed_customer_binds_history_by_customer_id(self):
        result = score(
            [customer("cid-1", "100", "Nytt namn", 2)],
            orders=[order("cid-1", "100", "Gammalt namn", "A", "2026-07-01")],
            contacts=[contact("cid-1", "Gammalt namn", "contact-1", "2026-08-01 10:00")],
        )["cid-1"]

        self.assertEqual(result["order_count"], 1)
        self.assertEqual(result["latest_human_contact_id"], "contact-1")

    def test_ambiguous_legacy_name_fallback_is_rejected(self):
        customers = [
            customer("cid-1", "100", "Samma butik", 2),
            customer("cid-2", "200", "Samma butik", 3),
        ]
        result = score(
            customers,
            orders=[order("", "", "Samma butik", "legacy", "2026-07-01")],
            contacts=[contact("", "Samma butik", "legacy-contact", "2026-08-01 10:00")],
        )

        for item in result.values():
            self.assertEqual(item["order_count"], 0)
            self.assertEqual(item["latest_human_contact_id"], "")

    def test_unambiguous_legacy_name_fallback_remains_supported(self):
        result = score(
            [customer("cid-1", "100", "Ensam butik", 2)],
            orders=[order("", "", "Ensam butik", "legacy", "2026-07-01")],
            contacts=[contact("", "Ensam butik", "legacy-contact", "2026-08-01 10:00")],
        )["cid-1"]

        self.assertEqual(result["order_count"], 1)
        self.assertEqual(result["latest_human_contact_id"], "legacy-contact")

    def test_other_customer_order_does_not_resolve_followup(self):
        customers = [
            customer("cid-1", "100", "Samma butik", 2),
            customer("cid-2", "200", "Samma butik", 3),
        ]
        followup = contact("cid-1", "Samma butik", "contact-1", "2026-07-01 10:00")
        followup["follow_up_date"] = "2026-07-10"
        result = score(
            customers,
            orders=[order("cid-2", "200", "Samma butik", "B", "2026-07-15")],
            contacts=[followup],
        )

        self.assertTrue(result["cid-1"]["follow_up_due"])
        self.assertFalse(result["cid-1"]["has_order_after_latest_contact"])

    def test_renamed_later_order_resolves_followup_by_customer_id(self):
        followup = contact("cid-1", "Gammalt namn", "contact-1", "2026-07-01 10:00")
        followup["follow_up_date"] = "2026-07-10"
        result = score(
            [customer("cid-1", "100", "Nytt namn", 2)],
            orders=[order("cid-1", "100", "Ännu ett namn", "later", "2026-07-15")],
            contacts=[followup],
        )["cid-1"]
        self.assertFalse(result["follow_up_due"])
        self.assertTrue(result["has_order_after_latest_contact"])
