from datetime import date, timedelta
from pathlib import Path
import sys
from unittest import TestCase


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

from test_priority_v2 import (  # noqa: E402
    contact,
    context_identity,
    customer,
    order,
    priorities,
)


def sku_order(reference, name, delivered, dfp, customer_number, sku):
    row = order(reference, name, delivered, dfp, customer_number)
    row["SKU"] = sku
    row["Product"] = f"Product {sku}"
    return row


def selected(result, customer_id):
    return next(item for item in result if item["customer_id"] == customer_id)


class FirstOrderPhase3Tests(TestCase):
    def test_exactly_one_commercial_delivery_is_first_order(self):
        result = priorities(
            [customer("First", "first")],
            [sku_order("F1", "First", "2026-07-20", 20, "first", "SKU-1")],
            today=date(2026, 7, 25),
        )[0]
        self.assertEqual(result["delivery_count"], 1)
        self.assertEqual(result["lifecycle"], "first_order")
        self.assertEqual(result["decision_context_lifecycle"], "first_order")

    def test_first_order_timing_and_trigger_boundaries(self):
        delivered = date(2026, 1, 1)
        customers = [customer("First", "first", "B")]
        orders = [sku_order("F1", "First", delivered.isoformat(), 20, "first", "S1")]
        expected = {
            7: ("first_order_onboarding", 43),
            10: ("first_order_onboarding", 43),
            11: ("", 28),
            23: ("", 28),
            24: ("first_order_reorder", 83),
            30: ("first_order_reorder", 83),
            31: ("first_order_reorder", 98),
            45: ("first_order_reorder", 98),
            46: ("first_order_reorder", 88),
            60: ("first_order_reorder", 88),
            61: ("first_order_reorder", 60),
            90: ("first_order_reorder", 60),
            91: ("strategic_contact_due", 60),
        }
        for days, (trigger, timing) in expected.items():
            with self.subTest(days=days):
                item = priorities(
                    customers, orders, today=delivered + timedelta(days=days)
                )[0]
                self.assertEqual(item["primary_trigger_type"], trigger)
                self.assertEqual(item["intent_timing"], timing)
                self.assertEqual(
                    item["lifecycle"], "reactivation" if days == 91 else "first_order"
                )

    def test_onboarding_risk_groups_and_high_value_threshold(self):
        today = date(2026, 7, 9)
        delivered = "2026-07-01"
        customers = [
            customer("Low C", "low-c", "C", row=2),
            customer("Benchmark", "benchmark", "C", row=3),
        ]
        low_orders = [
            sku_order("L1", "Low C", delivered, 10, "low-c", "L1"),
            sku_order("L1", "Low C", delivered, 10, "low-c", "L2"),
            sku_order("B1", "Benchmark", delivered, 100, "benchmark", "B1"),
            sku_order("B1", "Benchmark", delivered, 100, "benchmark", "B2"),
        ]
        low = selected(priorities(customers, low_orders, today=today), "low-c")
        self.assertLess(low["value_index"], 70)
        self.assertEqual(low["primary_trigger_type"], "")

        high_orders = [
            sku_order("H1", "Low C", delivered, 100, "low-c", "H1"),
            sku_order("H1", "Low C", delivered, 100, "low-c", "H2"),
            sku_order("B1", "Benchmark", delivered, 10, "benchmark", "B1"),
            sku_order("B1", "Benchmark", delivered, 10, "benchmark", "B2"),
        ]
        high = selected(priorities(customers, high_orders, today=today), "low-c")
        self.assertGreaterEqual(high["value_index"], 70)
        self.assertEqual(high["primary_trigger_type"], "first_order_onboarding")

    def test_one_sku_qualifies_and_adds_eight_without_changing_potential(self):
        delivered = date(2026, 1, 1)
        customers = [
            customer("One SKU", "one", "C", row=2),
            customer("Two SKU", "two", "C", row=3),
        ]
        orders = [
            sku_order("O1", "One SKU", delivered.isoformat(), 20, "one", "A"),
            sku_order("T1", "Two SKU", delivered.isoformat(), 10, "two", "A"),
            sku_order("T1", "Two SKU", delivered.isoformat(), 10, "two", "B"),
        ]
        day_8 = priorities(customers, orders, today=delivered + timedelta(days=8))
        one = selected(day_8, "one")
        two = selected(day_8, "two")
        self.assertEqual(one["first_order_sku_count"], 1)
        self.assertEqual(two["first_order_sku_count"], 2)
        self.assertEqual(one["intent_timing"], two["intent_timing"] + 8)
        self.assertEqual(one["expected_order_dfp"], two["expected_order_dfp"])
        self.assertEqual(one["primary_trigger_type"], "first_order_onboarding")

        day_61 = priorities(customers, orders, today=delivered + timedelta(days=61))
        self.assertEqual(
            selected(day_61, "one")["intent_timing"],
            selected(day_61, "two")["intent_timing"],
        )

    def test_onboarding_to_reorder_keeps_business_context_identity(self):
        customers = [customer("First", "first", "B")]
        orders = [sku_order("F1", "First", "2026-01-01", 20, "first", "S1")]
        onboarding = priorities(customers, orders, today=date(2026, 1, 9))[0]
        reorder = priorities(customers, orders, today=date(2026, 1, 25))[0]
        self.assertEqual(onboarding["primary_trigger_type"], "first_order_onboarding")
        self.assertEqual(reorder["primary_trigger_type"], "first_order_reorder")
        self.assertEqual(context_identity(onboarding), context_identity(reorder))


class DialogueAndStrategicPhase3Tests(TestCase):
    def test_positive_dialogue_boundaries_for_existing_and_prospect(self):
        today = date(2026, 7, 20)
        existing = customer("Existing", "existing", "C")
        existing_orders = [
            sku_order("E1", "Existing", "2026-07-01", 20, "existing", "S1")
        ]
        for days, expected in ((2, ""), (3, "positive_dialogue_followup")):
            with self.subTest(kind="existing", days=days):
                item = priorities(
                    [existing], existing_orders,
                    contacts=[contact("Existing", f"{today - timedelta(days=days)} 09:00", "Positiv", f"e-{days}")],
                    today=today,
                )[0]
                self.assertEqual(item["primary_trigger_type"], expected)

        prospect = customer("Prospect", "prospect", "C")
        for days, expected in ((6, ""), (7, "positive_dialogue_followup")):
            with self.subTest(kind="prospect", days=days):
                item = priorities(
                    [prospect], contacts=[contact(
                        "Prospect", f"{today - timedelta(days=days)} 09:00",
                        "Positiv", f"p-{days}"
                    )], today=today,
                )[0]
                self.assertEqual(item["primary_trigger_type"], expected)

    def test_explicit_followup_suppresses_positive_dialogue(self):
        row = contact("Prospect", "2026-07-10 09:00", "Positiv", "followup")
        row["follow_up_date"] = "2026-07-25"
        item = priorities(
            [customer("Prospect", "prospect", "A")],
            contacts=[row], today=date(2026, 7, 20),
        )[0]
        self.assertFalse(item["recommendation_eligible"])
        self.assertEqual(item["recommendation_suppression_reason"], "explicit_follow_up")
        self.assertNotIn("positive_dialogue_followup", item["covered_trigger_keys"])

        activity_contact = contact(
            "Prospect", "2026-07-10 09:00", "Positiv", "activity"
        )
        with_activity = priorities(
            [customer("Prospect", "prospect", "A")],
            contacts=[activity_contact],
            planned=[{
                "customer_id": "prospect",
                "customer": "Prospect",
                "status": "planned",
                "scheduled_at": "2026-07-25T09:00:00+02:00",
                "source_suggestion_id": "",
                "contact_type": "phone",
            }],
            today=date(2026, 7, 20),
        )[0]
        self.assertFalse(with_activity["recommendation_eligible"])
        self.assertEqual(
            with_activity["recommendation_suppression_reason"],
            "future_planned_activity",
        )
        self.assertEqual(with_activity["intent_timing"], 25)

    def test_strategic_rules_and_suppressions(self):
        never = priorities(
            [customer("Strategic", "strategic", "A")], today=date(2026, 7, 20)
        )[0]
        self.assertEqual(never["primary_trigger_type"], "strategic_contact_due")
        self.assertEqual(never["primary_reason_text"], "Strategisk kund – aldrig kontaktad")

        recent = priorities(
            [customer("Strategic", "strategic", "A")],
            contacts=[contact("Strategic", "2026-06-20 09:00", "Neutral", "recent")],
            today=date(2026, 7, 20),
        )[0]
        self.assertEqual(recent["primary_trigger_type"], "")

        stale = priorities(
            [customer("Strategic", "strategic", "A")],
            contacts=[contact("Strategic", "2026-06-01 09:00", "Neutral", "stale")],
            today=date(2026, 7, 20),
        )[0]
        self.assertEqual(stale["primary_trigger_type"], "strategic_contact_due")
        self.assertIn("49 dagar", stale["primary_reason_text"])

        negative = priorities(
            [customer("Strategic", "strategic", "A")],
            contacts=[contact("Strategic", "2026-06-01 09:00", "Negativ", "negative")],
            today=date(2026, 7, 20),
        )[0]
        self.assertFalse(negative["recommendation_eligible"])
        self.assertEqual(
            negative["recommendation_suppression_reason"],
            "negative_contact_cooldown",
        )

    def test_future_activity_and_followup_suppress_strategic_contact(self):
        today = date(2026, 7, 20)
        customer_row = customer("Strategic", "strategic", "A")
        planned = priorities(
            [customer_row], today=today,
            planned=[{
                "customer_id": "strategic",
                "customer": "Strategic",
                "status": "planned",
                "scheduled_at": "2026-07-25T09:00:00+02:00",
                "source_suggestion_id": "",
                "contact_type": "phone",
            }],
        )[0]
        self.assertEqual(planned["primary_trigger_type"], "strategic_contact_due")
        self.assertFalse(planned["recommendation_eligible"])
        self.assertEqual(
            planned["recommendation_suppression_reason"],
            "future_planned_activity",
        )

        followup_contact = contact(
            "Strategic", "2026-06-01 09:00", "Neutral", "followup"
        )
        followup_contact["follow_up_date"] = "2026-07-25"
        followup = priorities(
            [customer_row], contacts=[followup_contact], today=today
        )[0]
        self.assertEqual(followup["primary_trigger_type"], "strategic_contact_due")
        self.assertFalse(followup["recommendation_eligible"])
        self.assertEqual(
            followup["recommendation_suppression_reason"], "explicit_follow_up"
        )

    def test_priority_70_qualifies_without_segment_a_and_name_does_not(self):
        today = date(2026, 7, 20)
        high_customers = [
            customer("High B prospect", "high-b", "B", row=2),
            customer("B seed", "b-seed", "B", row=3),
        ]
        high_orders = [
            sku_order("B1", "B seed", "2026-07-01", 100, "b-seed", "B")
        ]
        high = selected(
            priorities(high_customers, high_orders, today=today), "high-b"
        )
        self.assertGreaterEqual(high["priority_score"], 70)
        self.assertEqual(high["primary_trigger_type"], "strategic_contact_due")

        low_customers = [
            customer("ICA Maxi only by name", "ica-name", "", row=2),
            customer("Low seed", "low-seed", "B", row=3),
            customer("High seed", "high-seed", "A", row=4),
        ]
        low_orders = [
            sku_order("L1", "Low seed", "2026-07-01", 10, "low-seed", "L"),
            sku_order("H1", "High seed", "2026-07-01", 100, "high-seed", "H"),
        ]
        named = selected(
            priorities(low_customers, low_orders, today=today), "ica-name"
        )
        self.assertLess(named["priority_score"], 70)
        self.assertEqual(named["strategic_index"], 15)
        self.assertEqual(named["primary_trigger_type"], "")

    def test_trigger_precedence_and_covered_keys(self):
        today = date(2026, 7, 20)
        established = priorities(
            [customer("Repeat", "repeat", "A")],
            orders=[
                sku_order("R1", "Repeat", "2026-05-01", 20, "repeat", "A"),
                sku_order("R2", "Repeat", "2026-05-21", 20, "repeat", "A"),
                sku_order("R3", "Repeat", "2026-06-10", 20, "repeat", "A"),
            ],
            contacts=[contact("Repeat", "2026-07-17 09:00", "Positiv", "warm")],
            today=today,
        )[0]
        self.assertEqual(established["primary_trigger_type"], "established_reorder_due")
        self.assertEqual(
            established["covered_trigger_keys"],
            ["established_reorder_due", "positive_dialogue_followup"],
        )

        reactivation = priorities(
            [customer("Warm strategic", "warm-strategic", "A")],
            contacts=[contact(
                "Warm strategic", "2026-06-01 09:00", "Positiv", "warm-old"
            )],
            today=today,
        )[0]
        self.assertEqual(
            reactivation["covered_trigger_keys"],
            ["positive_dialogue_followup", "strategic_contact_due"],
        )
        self.assertEqual(
            reactivation["primary_trigger_type"], "positive_dialogue_followup"
        )
