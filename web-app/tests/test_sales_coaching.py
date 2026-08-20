from datetime import datetime
from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from sales_coaching import (  # noqa: E402
    CustomerIdentityIndex,
    attribute_orders_to_contacts,
    build_drilldown,
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
    def summary(self, rows, orders=(), **kwargs):
        return build_sales_coaching_summary(
            activities=rows,
            customers=CUSTOMERS,
            users=USERS,
            order_rows=orders,
            start="2026-08-01",
            end="2026-08-20",
            generated_at="2026-08-20 12:00",
            score_version="v2.1",
            **kwargs,
        )

    def test_two_orders_from_one_contact_count_as_one_conversion(self):
        summary = self.summary(
            [activity("converted", "2026-08-01 10:00")],
            [order("ORDER-1", "2026-08-02"), order("ORDER-2", "2026-08-03")],
        )

        order_kpi = summary["kpis"]["order_10d"]
        self.assertEqual((order_kpi["numerator"], order_kpi["denominator"]), (1, 1))
        self.assertEqual(order_kpi["value"], 1)
        self.assertEqual(order_kpi["attributed_orders"], 2)

    def test_unresolved_identity_does_not_lower_mature_order_conversion(self):
        summary = self.summary(
            [
                activity("converted", "2026-08-01 10:00"),
                activity(
                    "unresolved", "2026-08-02 10:00",
                    customer_id="", customer="Okänd butik",
                ),
            ],
            [order("ORDER-1", "2026-08-03")],
        )

        order_kpi = summary["kpis"]["order_10d"]
        identity_coverage = summary["data_quality"]["order_attribution_identity_coverage"]
        self.assertEqual((order_kpi["numerator"], order_kpi["denominator"]), (1, 1))
        self.assertEqual((identity_coverage["numerator"], identity_coverage["denominator"]), (1, 2))
        self.assertEqual(summary["kpis"]["positive_dialogue"]["denominator"], 2)
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "order_10d")["rows"]],
            ["converted"],
        )

    def test_fresh_converted_contact_is_not_in_mature_order_drilldown(self):
        summary = self.summary(
            [activity("fresh", "2026-08-15 10:00")],
            [order("FRESH", "2026-08-16")],
        )

        self.assertEqual(summary["kpis"]["order_10d"]["denominator"], 0)
        self.assertEqual(build_drilldown(summary, "order_10d")["total_count"], 0)
        self.assertEqual(build_drilldown(summary, "order_10d_sync")["total_count"], 0)

    def test_positive_manual_email_does_not_enter_synchronous_funnel(self):
        summary = self.summary([
            activity("phone", "2026-08-01 10:00", channel="Telefon", result="Neutral"),
            activity("manual-email", "2026-08-02 10:00", channel="Mejl", result="Positiv"),
        ])

        self.assertEqual(summary["funnel"]["attempts"], 1)
        self.assertEqual(summary["funnel"]["reached"], 1)
        self.assertEqual(summary["funnel"]["positive"], 0)
        self.assertEqual(summary["channel_effectiveness"]["email"]["positive_dialogue"]["numerator"], 1)

    def test_positive_drilldowns_match_kpi_and_synchronous_populations(self):
        summary = self.summary([
            activity("manual-email", "2026-08-01 10:00", channel="Mejl", result="Positiv"),
            activity("sync-positive", "2026-08-02 10:00", channel="Telefon", result="Positiv"),
            activity("unknown-channel", "2026-08-03 10:00", channel="SMS", result="Positiv"),
        ])

        self.assertEqual(summary["kpis"]["positive_dialogue"]["numerator"], 2)
        self.assertEqual(
            {row["contact_id"] for row in build_drilldown(summary, "positive_dialogue")["rows"]},
            {"manual-email", "sync-positive"},
        )
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "positive_sync")["rows"]],
            ["sync-positive"],
        )

    def test_approximate_snapshot_without_percentile_is_not_priority_focus_denominator(self):
        summary = build_sales_coaching_summary(
            activities=[
                activity("exact", "2026-08-01 10:00", priority_snapshot_quality="exact", priority_percentile_at_contact="80"),
                activity("approx", "2026-08-02 10:00", planned_activity_id="planned-1"),
            ],
            customers=CUSTOMERS,
            users=USERS,
            order_rows=[],
            planned_activities=[{"planned_activity_id": "planned-1", "source_suggestion_id": "suggestion-1"}],
            planning_suggestions=[{"suggestion_id": "suggestion-1", "priority_score_at_creation": "71"}],
            start="2026-08-01",
            end="2026-08-20",
            generated_at="2026-08-20 12:00",
        )

        focus = summary["kpis"]["priority_focus"]
        coverage = summary["priority_allocation"]["priority_percentile_coverage"]
        self.assertEqual((focus["numerator"], focus["denominator"]), (1, 1))
        self.assertEqual((coverage["numerator"], coverage["denominator"]), (1, 2))
        self.assertEqual(summary["priority_allocation"]["snapshot_coverage"]["numerator"], 2)

    def test_small_sample_seller_does_not_affect_team_rate_median(self):
        rows = []
        for seller, reached, attempts in (("alice", 5, 10), ("bob", 7, 10), ("tiny", 1, 1)):
            for index in range(attempts):
                rows.append(activity(
                    f"{seller}-{index}",
                    f"2026-08-{index + 1:02d} 10:00",
                    seller=seller,
                    result="Neutral" if index < reached else "Ej anträffbar",
                ))

        summary = self.summary(rows, seller="alice")
        comparisons = {item["seller"]: item for item in summary["seller_comparison"]}

        self.assertEqual(comparisons["tiny"]["reach"]["value"], 1)
        self.assertEqual(comparisons["tiny"]["reach"]["status"], "small_sample")
        self.assertEqual(summary["kpis"]["reach"]["comparisons"]["team_median"], 0.6)

    def test_team_rate_median_requires_two_sufficient_sellers(self):
        rows = [
            activity(
                f"alice-{index}", f"2026-08-{index + 1:02d} 10:00",
                seller="alice", result="Ej anträffbar",
            )
            for index in range(10)
        ] + [activity("tiny", "2026-08-11 10:00", seller="tiny", result="Neutral")]

        summary = self.summary(rows, seller="alice")

        self.assertIsNone(summary["kpis"]["reach"]["comparisons"]["team_median"])
        self.assertNotIn("reach_low", {card["code"] for card in summary["coaching_cards"]})

    def test_reach_drilldown_contains_only_reached_contacts(self):
        summary = self.summary([
            activity("reached", "2026-08-01 10:00", result="Neutral"),
            activity("unreachable", "2026-08-02 10:00", result="Ej anträffbar"),
        ])

        self.assertEqual([row["contact_id"] for row in build_drilldown(summary, "reach")["rows"]], ["reached"])
        self.assertEqual({row["contact_id"] for row in build_drilldown(summary, "attempts")["rows"]}, {"reached", "unreachable"})

    def test_missing_historical_segment_does_not_fall_back_to_current_segment(self):
        rows = [activity("legacy", "2026-08-01 10:00")]

        current_segment = self.summary(rows, segment="A")
        missing_segment = self.summary(rows, segment="missing")

        self.assertEqual(current_segment["kpis"]["human_activities"]["value"], 0)
        self.assertEqual(missing_segment["kpis"]["human_activities"]["value"], 1)

    def test_data_quality_counts_flagged_rows_separately_from_issue_count(self):
        summary = self.summary([
            activity(
                "multi-issue", "2026-08-01 10:00",
                customer_id="", customer="Okänd butik",
                channel="SMS", result="Oklassificerat",
            ),
        ])

        quality = summary["data_quality"]
        self.assertEqual(quality["flagged_activity_rows"], 1)
        self.assertGreaterEqual(quality["quality_issue_count"], 3)
        self.assertEqual(quality["excluded_legacy_rows"], 0)

    def test_matrix_uses_percentile_coverage_not_approximate_snapshot_coverage(self):
        rows = []
        planned = []
        suggestions = []
        for index in range(10):
            if index < 6:
                rows.append(activity(
                    f"exact-{index}", f"2026-08-{index + 1:02d} 10:00",
                    priority_snapshot_quality="exact",
                    priority_percentile_at_contact="80",
                ))
            else:
                planned_id = f"planned-{index}"
                suggestion_id = f"suggestion-{index}"
                rows.append(activity(
                    f"approx-{index}", f"2026-08-{index + 1:02d} 10:00",
                    planned_activity_id=planned_id,
                ))
                planned.append({"planned_activity_id": planned_id, "source_suggestion_id": suggestion_id})
                suggestions.append({"suggestion_id": suggestion_id, "priority_score_at_creation": "70"})

        summary = build_sales_coaching_summary(
            activities=rows,
            customers=CUSTOMERS,
            users=USERS,
            order_rows=[],
            planned_activities=planned,
            planning_suggestions=suggestions,
            start="2026-08-01",
            end="2026-08-20",
            generated_at="2026-08-20 12:00",
        )

        self.assertEqual(summary["seller_comparison"][0]["snapshot_coverage"]["value"], 1)
        self.assertEqual(summary["seller_comparison"][0]["priority_percentile_coverage"]["value"], 0.6)
        self.assertEqual(summary["coaching_matrix"]["sellers"], [])
        self.assertIn(
            "priority_percentile_coverage_below_70",
            summary["coaching_matrix"]["insufficient_sample"][0]["reasons"],
        )

    def test_api_model_exposes_definitions_and_deterministic_coaching_cards(self):
        rows = [
            activity("a-1", "2026-08-01 10:00", channel="Besök", result="Ej anträffbar", customer_id="customer-1"),
            activity("a-2", "2026-08-02 10:00", channel="Besök", result="Ej anträffbar", customer_id="customer-1"),
            activity("b-1", "2026-08-03 10:00", channel="Besök", result="Ej anträffbar", customer_id="customer-2", customer="Andra butiken"),
            activity("b-2", "2026-08-04 10:00", channel="Besök", result="Ej anträffbar", customer_id="customer-2", customer="Andra butiken"),
        ]

        first = self.summary(rows)
        second = self.summary(rows)

        self.assertEqual(first["coaching_cards"], second["coaching_cards"])
        self.assertLessEqual(len(first["coaching_cards"]), 4)
        self.assertEqual(first["coaching_cards"][0]["code"], "repeat_boms")
        self.assertEqual(
            set(first["coaching_cards"][0]),
            {"code", "severity", "title", "diagnosis", "evidence", "comparison", "recommendation", "drilldown_metric", "drilldown_filters"},
        )
        for key, kpi in first["kpis"].items():
            with self.subTest(kpi=key):
                self.assertTrue(kpi["definition"])
                self.assertTrue(kpi["drilldown_metric"])
        self.assertEqual(
            [step["drilldown_metric"] for step in first["funnel"]["steps"]],
            ["attempts", "reach", "positive_sync", "order_10d_sync"],
        )

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
        self.assertIsNone(result[1]["recommendation_eligible_at_contact"])

    def test_pre_contact_snapshot_is_deterministic_and_uses_owner_portfolio(self):
        priorities = [
            {"customer_id": "customer-1", "sales_person": "Olle", "priority_score": 80, "score_version": "v2.1", "intent_timing": 90, "value_index": 50, "strategic_index": 100, "expected_order_dfp": 12, "lifecycle": "established", "segment": "A", "recommendation_eligible": True},
            {"customer_id": "customer-2", "sales_person": "Olle", "priority_score": 20, "recommendation_eligible": True},
            {"customer_id": "other", "sales_person": "Sofia", "priority_score": 100, "recommendation_eligible": True},
        ]
        args = dict(customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities, score_version="v2.1")

        first = build_pre_contact_snapshot(**args)
        second = build_pre_contact_snapshot(**args)

        self.assertEqual(first, second)
        self.assertEqual(first["priority_snapshot_quality"], "exact")
        self.assertEqual(first["seller_portfolio_size_at_contact"], 2)
        self.assertEqual(first["priority_percentile_at_contact"], 100)
        self.assertIs(first["recommendation_eligible_at_contact"], True)

    def test_suppressed_customer_snapshot_has_no_priority_percentile(self):
        priorities = [
            {"customer_id": "customer-1", "sales_person": "Olle", "priority_score": 99, "recommendation_eligible": False, "recommendation_suppression_reason": "recent_contact"},
            {"customer_id": "customer-2", "sales_person": "Olle", "priority_score": 20, "recommendation_eligible": True},
        ]

        snapshot = build_pre_contact_snapshot(
            customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities,
            score_version="v2.1",
        )

        self.assertIs(snapshot["recommendation_eligible_at_contact"], False)
        self.assertEqual(snapshot["suppression_reason_at_contact"], "recent_contact")
        self.assertEqual(snapshot["priority_percentile_at_contact"], "")
        self.assertEqual(snapshot["seller_portfolio_size_at_contact"], 1)

    def test_priority_gap_contains_only_recommendation_eligible_customers(self):
        summary = build_sales_coaching_summary(
            activities=[], customers=CUSTOMERS, users=USERS, order_rows=[],
            current_priorities=[
                {"customer_id": "customer-1", "customer": "Nytt namn", "sales_person": "Olle", "priority_score": 100, "value_index": 80, "segment": "A", "recommendation_eligible": False, "recommendation_suppression_reason": "recent_contact"},
                {"customer_id": "customer-2", "customer": "Andra butiken", "sales_person": "Olle", "priority_score": 80, "value_index": 60, "segment": "B", "recommendation_eligible": True},
            ],
            start="2026-08-01", end="2026-08-20",
            generated_at="2026-08-20 12:00",
        )

        gap = summary["priority_allocation"]["priority_gap"]
        self.assertEqual(gap["count"], 1)
        self.assertEqual(gap["customers"][0]["customer_id"], "customer-2")

    def test_followup_and_planning_drilldowns_match_each_card(self):
        rows = [
            activity("success", "2026-08-01 10:00", follow_up_date="2026-08-05"),
            activity("gap", "2026-08-02 10:00"),
        ]
        planned = [
            {"planned_activity_id": "on-time", "scheduled_at": "2026-08-03T10:00:00", "status": "completed", "completed_contact_id": "success", "user_name": "olle", "customer": "Nytt namn", "customer_id": "customer-1", "contact_type": "phone"},
            {"planned_activity_id": "overdue", "scheduled_at": "2026-08-04T10:00:00", "status": "planned", "user_name": "olle", "customer": "Nytt namn", "customer_id": "customer-1", "contact_type": "visit"},
            {"planned_activity_id": "skipped", "scheduled_at": "2026-08-05T10:00:00", "status": "skipped", "user_name": "olle", "customer": "Nytt namn", "customer_id": "customer-1", "contact_type": "email"},
        ]
        summary = build_sales_coaching_summary(
            activities=rows, customers=CUSTOMERS, users=USERS, order_rows=[],
            planned_activities=planned,
            start="2026-08-01", end="2026-08-20",
            generated_at="2026-08-20 12:00",
        )

        expected = {
            "followup_success": 1,
            "followup_gap": 1,
            "followup_gap_10d": 1,
            "planned_on_time": 1,
            "planned_overdue": 1,
            "planned_skipped": 1,
        }
        for metric, count in expected.items():
            with self.subTest(metric=metric):
                self.assertEqual(build_drilldown(summary, metric)["total_count"], count)

    def test_bom_ratio_repeat_high_priority_and_small_sample(self):
        rows = [
            activity("bom-1", "2026-08-01 10:00", channel="Besök", result="Ej anträffbar", planned_activity_id="planned-bom", priority_snapshot_quality="exact", analytics_snapshot_version="sales_coaching_v1", priority_score_at_contact="80", priority_percentile_at_contact="90"),
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
        self.assertEqual(build_drilldown(summary, "bom_ratio")["total_count"], 2)
        self.assertEqual(build_drilldown(summary, "planned_boms")["total_count"], 1)
        self.assertEqual(build_drilldown(summary, "unplanned_boms")["total_count"], 1)


if __name__ == "__main__":
    main()
