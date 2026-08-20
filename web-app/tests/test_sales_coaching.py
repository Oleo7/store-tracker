from datetime import datetime
from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from sales_coaching import (  # noqa: E402
    CustomerIdentityIndex,
    attribute_orders_to_contacts,
    build_pre_contact_snapshot,
    build_sales_coaching_summary,
    canonicalize_activities,
    group_logical_orders,
    normalize_result_class,
)


CUSTOMERS = [
    {
        "customer": "Nytt namn",
        "customer_id": "customer-1",
        "customer_number": "100",
        "sales_person": "Sofia",
        "customer_segment": "A",
    },
    {
        "customer": "Andra butiken",
        "customer_id": "customer-2",
        "customer_number": "200",
        "sales_person": "Olle",
        "customer_segment": "B",
    },
]
USERS = [
    {"user_name": "olle", "name": "Olle", "active": "Y"},
    {"user_name": "sofia", "name": "Sofia", "active": "Y"},
]


def activity(contact_id, when, *, customer_id="customer-1", customer="Gammalt namn",
             seller="olle", channel="Telefon", result="Positiv", **extra):
    return {
        "contact_id": contact_id,
        "date_time": when,
        "sales_user_name": seller,
        "sales_person": "Olle",
        "customer": customer,
        "customer_id": customer_id,
        "contact_channel": channel,
        "result": result,
        "activity_source": "manual",
        **extra,
    }


def order(reference, order_date, *, customer_id="customer-1", customer="Nytt namn",
          quantity="1", unit="DFP", total="100"):
    return {
        "Reference": reference,
        "Order date": order_date,
        "Customer": customer,
        "customer_id": customer_id,
        "Customer number": "100" if customer_id == "customer-1" else "200",
        "Quantity": quantity,
        "Unit": unit,
        "Total": total,
        "Currency": "SEK",
    }


class IdentityAndNormalizationTests(TestCase):
    def test_customer_id_survives_name_change_and_historical_seller_survives_owner_change(self):
        result = canonicalize_activities(
            [activity("c-1", "2026-08-01 10:00")],
            CUSTOMERS,
            USERS,
        )["activities"][0]

        self.assertEqual(result["customer_record"]["customer"], "Nytt namn")
        self.assertEqual(result["sales_user_name"], "olle")
        self.assertEqual(result["customer_record"]["sales_person"], "Sofia")

    def test_ambiguous_legacy_name_is_excluded(self):
        customers = [
            {"customer": "Dublett", "customer_id": "a"},
            {"customer": "Dublett", "customer_id": "b"},
        ]
        row = activity(
            "legacy", "2026-08-01 10:00",
            customer_id="", customer="Dublett",
        )

        canonical = canonicalize_activities([row], customers, USERS)["activities"][0]

        self.assertEqual(canonical["customer_identity_key"], "")
        self.assertEqual(canonical["identity_exclusion_reason"], "ambiguous_customer_name")

    def test_strong_identity_conflict_does_not_fall_back_to_name(self):
        index = CustomerIdentityIndex(CUSTOMERS)
        customer, reason = index.resolve({
            "customer_id": "customer-1",
            "customer_number": "200",
            "customer": "Nytt namn",
        })
        self.assertIsNone(customer)
        self.assertEqual(reason, "customer_identity_conflict")

    def test_old_and_new_result_labels_are_normalized(self):
        cases = {
            "Order lagd!": "order",
            "Intresserad/Återkom :)": "positive",
            "Uppföljning behövs": "neutral",
            "Kräver mer bearbetning!": "negative",
            "Ej anträffbar": "unreachable",
            "positive": "positive",
        }
        for label, expected in cases.items():
            with self.subTest(label=label):
                self.assertEqual(normalize_result_class(label), expected)


class AttributionTests(TestCase):
    def canonical(self, activities, orders, generated="2026-08-20 12:00"):
        canonical = canonicalize_activities(activities, CUSTOMERS, USERS)["activities"]
        grouped = group_logical_orders(orders, CUSTOMERS)["orders"]
        return canonical, grouped, attribute_orders_to_contacts(
            canonical, grouped, generated_at=generated
        )

    def test_sku_rows_form_one_logical_order_and_latest_touch_gets_it_once(self):
        activities = [
            activity("early", "2026-08-01 10:00"),
            activity("latest", "2026-08-05 10:00", result="Neutral"),
        ]
        orders = [
            order("REF-1", "2026-08-06", quantity="2", total="200"),
            order("REF-1", "2026-08-06", quantity="3", total="300"),
        ]

        _canonical, grouped, attribution = self.canonical(activities, orders)

        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped[0]["dfp"], 5)
        self.assertEqual(grouped[0]["total"], 500)
        self.assertEqual(attribution["order_to_contact"][grouped[0]["order_id"]]["contact_id"], "latest")
        self.assertNotIn("early", attribution["contact_to_orders"])

    def test_day_zero_and_day_ten_are_included_but_day_eleven_is_not(self):
        activities = [
            activity("day-0", "2026-08-01 09:00", customer_id="customer-1"),
            activity("day-10", "2026-07-22 09:00", customer_id="customer-2", customer="Andra butiken"),
        ]
        orders = [
            order("DAY-0", "2026-08-01", customer_id="customer-1"),
            order("DAY-10", "2026-08-01", customer_id="customer-2", customer="Andra butiken"),
            order("DAY-11", "2026-08-02", customer_id="customer-2", customer="Andra butiken"),
        ]

        _canonical, grouped, attribution = self.canonical(activities, orders)
        by_reference = {row["reference"]: row for row in grouped}

        self.assertIn(by_reference["DAY-0"]["order_id"], attribution["order_to_contact"])
        self.assertIn(by_reference["DAY-10"]["order_id"], attribution["order_to_contact"])
        self.assertNotIn(by_reference["DAY-11"]["order_id"], attribution["order_to_contact"])

    def test_contact_week_and_maturity_denominator_are_contact_based(self):
        activities = [
            activity("mature", "2026-08-10 10:00"),
            activity("fresh", "2026-08-15 10:00", customer_id="customer-2", customer="Andra butiken"),
        ]
        orders = [order("MATURE", "2026-08-20")]
        canonical, _grouped, attribution = self.canonical(activities, orders)

        self.assertEqual(attribution["maturity"]["mature"], "mature")
        self.assertEqual(attribution["maturity"]["fresh"], "waiting_outcome")
        mature_attribution = attribution["contact_to_orders"]["mature"][0]
        self.assertEqual(mature_attribution["contact_week"], "2026-W33")

    def test_system_email_is_never_a_human_touch(self):
        activities = [
            activity("human", "2026-08-01 09:00"),
            activity(
                "email", "2026-08-05 09:00", channel="Mejl",
                result="Mejlförslag skickat", email_id="mail-1",
                activity_source="crm_email",
            ),
        ]
        _canonical, grouped, attribution = self.canonical(
            activities, [order("ORDER", "2026-08-06")]
        )

        self.assertEqual(attribution["order_to_contact"][grouped[0]["order_id"]]["contact_id"], "human")
        self.assertIn({"contact_id": "email", "reason": "system_email"}, attribution["excluded_contacts"])


class SnapshotAndAggregateTests(TestCase):
    def test_snapshot_quality_exact_approximate_and_missing(self):
        rows = [
            activity(
                "exact", "2026-08-01 10:00",
                priority_snapshot_quality="exact",
                analytics_snapshot_version="sales_coaching_v1",
                priority_score_at_contact="82",
                priority_percentile_at_contact="91",
            ),
            activity("approx", "2026-08-02 10:00", planned_activity_id="planned-1"),
            activity("missing", "2026-08-03 10:00"),
        ]
        result = canonicalize_activities(
            rows,
            CUSTOMERS,
            USERS,
            planned_activities=[{
                "planned_activity_id": "planned-1",
                "source_suggestion_id": "suggestion-1",
            }],
            planning_suggestions=[{
                "suggestion_id": "suggestion-1",
                "priority_score_at_creation": "71",
                "intent_timing_at_creation": "75",
                "value_index_at_creation": "60",
                "strategic_index_at_creation": "100",
                "score_version": "v2.1",
            }],
        )["activities"]

        self.assertEqual([row["priority_snapshot_quality"] for row in result], ["exact", "approximate", "missing"])
        self.assertEqual(result[1]["priority_score_at_contact"], 71)

    def test_pre_contact_snapshot_is_deterministic_and_uses_owner_portfolio(self):
        priorities = [
            {"customer_id": "customer-1", "sales_person": "Olle", "priority_score": 80, "score_version": "v2.1", "intent_timing": 90, "value_index": 50, "strategic_index": 100, "expected_order_dfp": 12, "lifecycle": "established", "segment": "A"},
            {"customer_id": "customer-2", "sales_person": "Olle", "priority_score": 20},
            {"customer_id": "other", "sales_person": "Sofia", "priority_score": 100},
        ]
        args = dict(customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities, score_version="v2.1")

        first = build_pre_contact_snapshot(**args)
        second = build_pre_contact_snapshot(**args)

        self.assertEqual(first, second)
        self.assertEqual(first["priority_snapshot_quality"], "exact")
        self.assertEqual(first["seller_portfolio_size_at_contact"], 2)
        self.assertEqual(first["priority_percentile_at_contact"], 100)

    def test_bom_ratio_repeat_high_priority_and_small_sample(self):
        rows = [
            activity("bom-1", "2026-08-01 10:00", channel="Besök", result="Ej anträffbar", priority_snapshot_quality="exact", analytics_snapshot_version="sales_coaching_v1", priority_score_at_contact="80", priority_percentile_at_contact="90"),
            activity("bom-2", "2026-08-02 10:00", channel="Besök", result="Ej anträffbar", priority_snapshot_quality="exact", analytics_snapshot_version="sales_coaching_v1", priority_score_at_contact="75", priority_percentile_at_contact="80"),
            activity("visit", "2026-08-03 10:00", channel="Besök", result="Neutral"),
            activity("auto-email", "2026-08-04 10:00", channel="Mejl", result="Positiv", email_id="mail", activity_source="crm_email"),
        ]

        summary = build_sales_coaching_summary(
            activities=rows,
            customers=CUSTOMERS,
            users=USERS,
            order_rows=[],
            start="2026-08-01",
            end="2026-08-10",
            generated_at="2026-08-20 12:00",
            score_version="v2.1",
        )

        bom = summary["kpis"]["bom_ratio"]
        self.assertEqual((bom["numerator"], bom["denominator"]), (2, 3))
        self.assertEqual(bom["status"], "small_sample")
        self.assertEqual(summary["visit_efficiency"]["repeat_boms"], {"customers": 1, "visits": 2})
        self.assertEqual(summary["visit_efficiency"]["high_priority_boms"], 2)
        self.assertEqual(summary["kpis"]["human_activities"]["value"], 3)


if __name__ == "__main__":
    main()
