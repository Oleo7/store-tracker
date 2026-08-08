from datetime import date, timedelta
from pathlib import Path
import sys
from unittest import TestCase


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from planning_suggestions import (  # noqa: E402
    decision_context_hash,
    deterministic_suggestion_id,
)
from priority import (  # noqa: E402
    build_contact_features,
    build_order_features,
    build_priority_customers,
    calculate_priority_score_v2,
    established_intent_timing,
    expected_reorder_cycle,
    prospect_reactivation_intent_timing,
)


def customer(name, customer_id, segment="A", row=2):
    return {
        "row": row,
        "customer": name,
        "customer_id": customer_id,
        "customer_number": customer_id,
        "sales_person": "Olle",
        "customer_segment": segment,
        "cancelled_flag": "",
    }


def order(reference, name, delivered, dfp, customer_number):
    return {
        "Reference": reference,
        "Customer": name,
        "Customer number": customer_number,
        "Order date": delivered,
        "Delivery date": delivered,
        "Quantity": str(dfp),
        "Total weight": str(dfp),
        "Total": str(float(dfp) * 20),
        "Currency": "SEK",
    }


def contact(name, when, result="Neutral", contact_id="contact-1"):
    return {
        "contact_id": contact_id,
        "customer": name,
        "date_time": when,
        "sales_person": "Olle",
        "contact_channel": "Telefon",
        "result": result,
    }


def priorities(customers, orders=(), contacts=(), *, today, planned=()):
    order_features = build_order_features(list(orders))
    return build_priority_customers(
        list(customers),
        order_features,
        build_contact_features(list(contacts), order_features),
        "Olle",
        today,
        limit=len(customers),
        planned_activities=list(planned),
    )


def context_identity(item, owner="olle"):
    context_hash = decision_context_hash(
        owner=owner,
        customer_id=item["customer_id"],
        lifecycle=item["decision_context_lifecycle"],
        order_count=item["order_count"],
        latest_order_reference=item["latest_order_reference"],
        latest_order_date=item["latest_delivery_date"],
        latest_contact_id=item["latest_human_contact_id"],
        latest_contact_result=item["latest_contact_result"],
        latest_contact_date=item["latest_human_contact_date"],
    )
    return context_hash, deterministic_suggestion_id(
        owner, item["customer_id"], context_hash
    )


class ScoringV2Tests(TestCase):
    def test_control_case_scores_74(self):
        self.assertEqual(calculate_priority_score_v2(60, 100, 100), 74)

    def test_score_is_calculated_when_recent_contact_suppresses_recommendation(self):
        result = priorities(
            [customer("Prospekt", "prospect-1")],
            contacts=[contact("Prospekt", "2026-07-26 10:00")],
            today=date(2026, 7, 27),
        )[0]

        self.assertIsInstance(result["priority_score"], int)
        self.assertEqual(result["intent_timing"], 15)
        self.assertFalse(result["recommendation_eligible"])
        self.assertEqual(
            result["recommendation_suppression_reason"],
            "recent_human_contact",
        )

    def test_established_timing_boundaries(self):
        cases = {
            -15: 10, -14: 25, 0: 65, 7: 65, 8: 90, 30: 90,
            31: 80, 60: 80, 61: 60, 90: 60,
        }
        for overdue, expected in cases.items():
            with self.subTest(overdue=overdue):
                self.assertEqual(established_intent_timing(overdue), expected)

    def test_prospect_reactivation_timing_boundaries(self):
        cases = {1: 15, 8: 25, 22: 45, 46: 60, None: 60}
        for days, expected in cases.items():
            with self.subTest(days=days):
                self.assertEqual(
                    prospect_reactivation_intent_timing(days), expected
                )

    def test_three_deliveries_use_median_without_factor(self):
        self.assertEqual(expected_reorder_cycle([
            date(2026, 1, 1), date(2026, 1, 21), date(2026, 2, 20)
        ]), 25)

    def test_at_least_four_intervals_use_only_latest_four(self):
        dates = [date(2026, 1, 1)]
        for gap in (70, 10, 20, 30, 40):
            dates.append(dates[-1] + timedelta(days=gap))
        self.assertEqual(expected_reorder_cycle(dates), 25)

    def test_two_deliveries_blend_observed_and_segment_median(self):
        self.assertEqual(expected_reorder_cycle([
            date(2026, 1, 1), date(2026, 1, 21)
        ], segment_median=40), 30)

    def test_expected_cycle_is_clamped_14_to_75(self):
        self.assertEqual(expected_reorder_cycle([
            date(2026, 1, 1), date(2026, 1, 3), date(2026, 1, 5)
        ]), 14)
        self.assertEqual(expected_reorder_cycle([
            date(2026, 1, 1), date(2026, 5, 1), date(2026, 9, 1)
        ]), 75)

    def test_live_overdue_score_changes_without_new_context(self):
        customers = [customer("Repeat", "repeat-1")]
        orders = [
            order("R1", "Repeat", "2026-01-01", 20, "repeat-1"),
            order("R2", "Repeat", "2026-01-21", 30, "repeat-1"),
            order("R3", "Repeat", "2026-02-10", 40, "repeat-1"),
        ]
        due = priorities(customers, orders, today=date(2026, 3, 2))[0]
        late = priorities(customers, orders, today=date(2026, 3, 10))[0]

        self.assertEqual(context_identity(due), context_identity(late))
        self.assertNotEqual(due["priority_score"], late["priority_score"])
        self.assertNotEqual(due["primary_reason_text"], late["primary_reason_text"])

    def test_established_context_is_stable_across_day_90_to_reactivation(self):
        customers = [customer("Boundary repeat", "repeat-boundary")]
        orders = [
            order("B1", "Boundary repeat", "2026-01-01", 20, "repeat-boundary"),
            order("B2", "Boundary repeat", "2026-01-21", 30, "repeat-boundary"),
            order("B3", "Boundary repeat", "2026-02-10", 40, "repeat-boundary"),
        ]
        day_90 = priorities(customers, orders, today=date(2026, 5, 31))[0]
        day_91 = priorities(customers, orders, today=date(2026, 6, 1))[0]

        self.assertEqual(day_90["overdue_days"], 90)
        self.assertEqual(day_91["overdue_days"], 91)
        self.assertEqual(day_90["lifecycle"], "established")
        self.assertEqual(day_91["lifecycle"], "reactivation")
        self.assertEqual(day_90["decision_context_lifecycle"], "established")
        self.assertEqual(day_91["decision_context_lifecycle"], "established")
        self.assertNotEqual(day_90["primary_reason_text"], day_91["primary_reason_text"])
        self.assertEqual(context_identity(day_90), context_identity(day_91))

    def test_first_order_context_is_stable_across_day_90_to_reactivation(self):
        customers = [customer("Boundary first", "first-boundary")]
        orders = [
            order("F1", "Boundary first", "2026-01-01", 20, "first-boundary"),
        ]
        day_90 = priorities(customers, orders, today=date(2026, 4, 1))[0]
        day_91 = priorities(customers, orders, today=date(2026, 4, 2))[0]

        self.assertEqual(day_90["days_since_delivery"], 90)
        self.assertEqual(day_91["days_since_delivery"], 91)
        self.assertEqual(day_90["lifecycle"], "first_order")
        self.assertEqual(day_91["lifecycle"], "reactivation")
        self.assertEqual(day_90["decision_context_lifecycle"], "first_order")
        self.assertEqual(day_91["decision_context_lifecycle"], "first_order")
        self.assertEqual(context_identity(day_90), context_identity(day_91))

    def test_real_order_and_human_contact_events_still_change_context(self):
        customers = [customer("Event customer", "event-customer")]
        original_orders = [
            order("E1", "Event customer", "2026-01-01", 20, "event-customer"),
            order("E2", "Event customer", "2026-01-21", 30, "event-customer"),
            order("E3", "Event customer", "2026-02-10", 40, "event-customer"),
        ]
        baseline = priorities(
            customers, original_orders, today=date(2026, 6, 1)
        )[0]
        after_order = priorities(
            customers,
            original_orders + [
                order("E4", "Event customer", "2026-05-30", 35, "event-customer")
            ],
            today=date(2026, 6, 1),
        )[0]
        after_contact = priorities(
            customers,
            original_orders,
            contacts=[contact(
                "Event customer", "2026-06-01 10:00", contact_id="event-contact"
            )],
            today=date(2026, 6, 1),
        )[0]

        self.assertNotEqual(context_identity(baseline), context_identity(after_order))
        self.assertNotEqual(context_identity(baseline), context_identity(after_contact))

    def test_more_than_90_days_overdue_changes_to_reactivation(self):
        result = priorities(
            [customer("Old repeat", "old-repeat")],
            orders=[
                order("O1", "Old repeat", "2025-01-01", 20, "old-repeat"),
                order("O2", "Old repeat", "2025-01-21", 20, "old-repeat"),
                order("O3", "Old repeat", "2025-02-10", 20, "old-repeat"),
            ],
            today=date(2026, 7, 27),
        )[0]
        self.assertEqual(result["lifecycle"], "reactivation")
        self.assertEqual(result["intent_timing"], 60)
        self.assertEqual(result["primary_trigger_type"], "strategic_contact_due")

    def test_suppressed_customers_remain_and_sort_score_dfp_stable_row(self):
        customers = [
            customer("Lower", "lower", "B", row=4),
            customer("Higher potential", "higher", "B", row=3),
            customer("Stable first", "stable-a", "B", row=2),
        ]
        orders = [
            order("L1", "Lower", "2026-07-20", 99.8, "lower"),
            order("H1", "Higher potential", "2026-07-20", 100, "higher"),
            order("S1", "Stable first", "2026-07-20", 99.8, "stable-a"),
        ]
        planned = [{
            "customer_id": "higher",
            "customer": "Higher potential",
            "status": "planned",
            "scheduled_at": "2026-07-30T10:00:00+02:00",
            "contact_type": "phone",
            "source_suggestion_id": "",
        }]

        result = priorities(
            customers, orders, today=date(2026, 7, 27), planned=planned
        )

        self.assertEqual(len(result), 3)
        suppressed = next(item for item in result if item["customer_id"] == "higher")
        self.assertFalse(suppressed["recommendation_eligible"])
        self.assertEqual(
            suppressed["recommendation_suppression_reason"],
            "future_planned_activity",
        )
        keys = [
            (-item["priority_score"], -item["expected_order_dfp"], item["row"])
            for item in result
        ]
        self.assertEqual(keys, sorted(keys))

    def test_expected_dfp_is_65_35_and_segment_uses_source_field(self):
        result = priorities(
            [customer("ICA Maxi by name", "value-1", "", row=2)],
            orders=[
                order("V1", "ICA Maxi by name", "2026-01-01", 20, "value-1"),
                order("V2", "ICA Maxi by name", "2026-01-21", 40, "value-1"),
            ],
            today=date(2026, 2, 15),
        )[0]
        self.assertEqual(result["expected_order_dfp"], 36.5)
        self.assertEqual(result["strategic_index"], 15)

    def test_phase_4_triggers_are_not_created(self):
        results = priorities(
            [
                customer("Prospect", "p-1", row=2),
                customer("First", "f-1", row=3),
            ],
            orders=[order("F1", "First", "2026-07-01", 20, "f-1")],
            today=date(2026, 7, 27),
        )
        forbidden = {
            "stockfiller_clicked_no_order",
            "product_sheet_clicked_no_order",
            "email_opened_no_order",
            "email_delivered_no_order",
            "legacy_missed_followup",
        }
        self.assertTrue(forbidden.isdisjoint(
            {item["primary_trigger_type"] for item in results}
        ))
