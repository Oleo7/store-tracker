from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from sales_coaching import (  # noqa: E402
    ANALYTICS_SNAPSHOT_VERSION,
    MAIN_KPI_KEYS,
    METRIC_DEFINITIONS,
    PRIORITY_PERCENTILE_BASIS,
    CustomerIdentityIndex,
    attribute_orders_to_contacts,
    build_drilldown,
    build_pre_contact_snapshot,
    build_sales_coaching_summary,
    canonicalize_activities,
    group_logical_orders,
    normalize_contact_type,
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


class OutcomeResolutionTests(TestCase):
    def build_summary(
        self, contact_specs, order_specs=(), *, start="2026-08-01",
        end="2026-08-20", generated="2026-08-20 12:00", seller="",
    ):
        customers = []
        activities = []
        customer_by_contact = {}
        sellers = {"olle"}
        for index, spec in enumerate(contact_specs, start=1):
            contact_id, when = spec[:2]
            options = dict(spec[2]) if len(spec) > 2 else {}
            customer_id = f"resolution-customer-{index}"
            customer_name = f"Resolution customer {index}"
            customer_number = str(9000 + index)
            contact_seller = options.pop("seller", "olle")
            sellers.add(contact_seller)
            customers.append({
                "customer": customer_name,
                "customer_id": customer_id,
                "customer_number": customer_number,
                "sales_person": contact_seller,
                "customer_segment": "A",
            })
            activities.append(activity(
                contact_id, when,
                customer_id=customer_id,
                customer=customer_name,
                seller=contact_seller,
                **options,
            ))
            customer_by_contact[contact_id] = (
                customer_id, customer_name, customer_number
            )
        orders = []
        for reference, order_date, contact_id in order_specs:
            customer_id, customer_name, customer_number = customer_by_contact[
                contact_id
            ]
            row = order(
                reference, order_date,
                customer_id=customer_id,
                customer=customer_name,
            )
            row["Customer number"] = customer_number
            orders.append(row)
        users = [
            {"user_name": name, "name": name.title(), "active": "Y"}
            for name in sorted(sellers)
        ]
        return build_sales_coaching_summary(
            activities=activities,
            customers=customers,
            users=users,
            order_rows=orders,
            start=start,
            end=end,
            generated_at=generated,
            score_version="v2.1",
            seller=seller,
        )

    def test_early_converted_is_immediately_resolved_and_never_reverts(self):
        contacts = [("early", "2026-08-16 10:00", {})]
        orders = [("EARLY", "2026-08-18", "early")]

        early = self.build_summary(contacts, orders)
        later = self.build_summary(
            contacts, orders, end="2026-08-31", generated="2026-08-31 12:00"
        )

        self.assertEqual(
            early["_analysis"]["attribution"]["maturity"]["early"],
            "waiting_outcome",
        )
        self.assertEqual(
            early["_analysis"]["attribution"]["outcome_status"]["early"],
            "converted",
        )
        self.assertEqual(
            (early["kpis"]["order_10d"]["numerator"],
             early["kpis"]["order_10d"]["denominator"]),
            (1, 1),
        )
        self.assertEqual(
            later["_analysis"]["attribution"]["outcome_status"]["early"],
            "converted",
        )

    def test_early_non_converted_is_denominator_and_pending(self):
        summary = self.build_summary([("pending", "2026-08-16 10:00", {})])

        self.assertEqual(summary["kpis"]["order_10d"]["denominator"], 1)
        self.assertEqual(summary["kpis"]["order_10d"]["numerator"], 0)
        self.assertEqual(summary["kpis"]["order_10d"]["waiting_outcome_count"], 1)
        self.assertEqual(
            summary["_analysis"]["attribution"]["outcome_status"]["pending"],
            "pending",
        )

    def test_mature_non_converted_is_denominator_only(self):
        summary = self.build_summary([("mature-miss", "2026-08-01 10:00", {})])

        metric = summary["kpis"]["order_10d"]
        self.assertEqual((metric["numerator"], metric["denominator"]), (0, 1))
        self.assertNotIn("comparable", metric)
        self.assertEqual(
            summary["_analysis"]["attribution"]["outcome_status"]["mature-miss"],
            "resolved_without_order",
        )

    def test_mature_converted_is_numerator_and_denominator(self):
        summary = self.build_summary(
            [("mature-hit", "2026-08-01 10:00", {})],
            [("MATURE-HIT", "2026-08-05", "mature-hit")],
        )

        metric = summary["kpis"]["order_10d"]
        self.assertEqual((metric["numerator"], metric["denominator"]), (1, 1))
        self.assertNotIn("comparable", metric)

    def test_order_on_day_ten_is_converted_by_existing_inclusive_window(self):
        summary = self.build_summary(
            [("day-ten", "2026-08-10 10:00", {})],
            [("DAY-TEN", "2026-08-20", "day-ten")],
        )

        metric = summary["kpis"]["order_10d"]
        self.assertEqual((metric["numerator"], metric["denominator"]), (1, 1))
        self.assertNotIn("comparable", metric)

    def test_order_outside_window_does_not_convert_resolved_contact(self):
        summary = self.build_summary(
            [("day-eleven", "2026-08-09 10:00", {})],
            [("DAY-ELEVEN", "2026-08-20", "day-eleven")],
        )

        metric = summary["kpis"]["order_10d"]
        self.assertEqual((metric["numerator"], metric["denominator"]), (0, 1))
        self.assertEqual(
            summary["_analysis"]["attribution"]["outcome_status"]["day-eleven"],
            "resolved_without_order",
        )

    def test_waiting_count_excludes_early_converted(self):
        summary = self.build_summary(
            [
                ("early-hit", "2026-08-16 10:00", {}),
                ("early-wait", "2026-08-17 10:00", {}),
            ],
            [("EARLY-HIT", "2026-08-18", "early-hit")],
        )

        self.assertEqual(summary["kpis"]["order_10d"]["waiting_outcome_count"], 1)
        self.assertEqual(summary["data_quality"]["waiting_outcome_count"], 1)
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "waiting_outcome")["rows"]],
            ["early-wait"],
        )

    def test_denominator_is_stable_when_pending_contact_converts(self):
        contacts = [("stable", "2026-08-16 10:00", {})]

        pending = self.build_summary(contacts)
        converted = self.build_summary(
            contacts, [("STABLE", "2026-08-18", "stable")]
        )

        self.assertEqual(
            (pending["kpis"]["order_10d"]["numerator"],
             pending["kpis"]["order_10d"]["denominator"]),
            (0, 1),
        )
        self.assertEqual(
            (converted["kpis"]["order_10d"]["numerator"],
             converted["kpis"]["order_10d"]["denominator"]),
            (1, 1),
        )

    def test_sample_status_uses_total_eligible_denominator(self):
        contacts = []
        orders = []
        for index in range(7):
            contact_id = f"early-hit-{index}"
            contacts.append((contact_id, "2026-08-16 10:00", {}))
            orders.append((f"EARLY-{index}", "2026-08-18", contact_id))
        contacts.extend(
            (f"mature-miss-{index}", "2026-08-01 10:00", {})
            for index in range(5)
        )
        contacts.extend(
            (f"pending-{index}", "2026-08-17 10:00", {})
            for index in range(20)
        )

        kpis = self.build_summary(contacts, orders)["kpis"]

        for metric_key in ("order_10d", "positive_to_order_10d"):
            with self.subTest(metric_key=metric_key):
                metric = kpis[metric_key]
                self.assertEqual((metric["numerator"], metric["denominator"]), (7, 32))
                self.assertEqual(metric["status"], "sufficient")
                self.assertEqual(metric["waiting_outcome_count"], 20)
                self.assertNotIn("comparable", metric)

    def test_team_comparison_uses_live_rates_and_pending_counts(self):
        contacts = [
            (f"team-{index}", "2026-08-16 10:00", {"result": "Positiv"})
            for index in range(10)
        ]
        orders = [
            (f"TEAM-ORDER-{index}", "2026-08-18", f"team-{index}")
            for index in range(2)
        ]

        summary = self.build_summary(contacts, orders)
        team = summary["team_comparison"]["sellers"][0]

        for metric_key in ("order_10d", "positive_to_order_10d"):
            with self.subTest(metric_key=metric_key):
                self.assertEqual(
                    (team[metric_key]["numerator"],
                     team[metric_key]["denominator"],
                     team[metric_key]["value"]),
                    (2, 10, .2),
                )
        self.assertEqual(team["waiting_outcome_count"], 8)
        self.assertEqual(team["waiting_positive_dialogues_count"], 8)
        self.assertIn("order_10d_median", summary["team_comparison"]["benchmarks"])
        self.assertNotIn(
            "order_10d_comparable_median",
            summary["team_comparison"]["benchmarks"],
        )

    def test_requested_twenty_of_one_hundred_regression(self):
        contacts = []
        orders = []
        for index in range(20):
            contact_id = f"converted-{index}"
            contact_date = "2026-08-16 10:00" if index < 8 else "2026-08-01 10:00"
            contacts.append((contact_id, contact_date, {}))
            order_date = "2026-08-18" if index < 8 else "2026-08-03"
            orders.append((f"CONVERTED-{index}", order_date, contact_id))
        contacts.extend(
            (f"resolved-miss-{index}", "2026-08-01 10:00", {})
            for index in range(50)
        )
        contacts.extend(
            (f"pending-{index}", "2026-08-17 10:00", {})
            for index in range(30)
        )

        metric = self.build_summary(contacts, orders)["kpis"]["order_10d"]

        self.assertEqual((metric["numerator"], metric["denominator"]), (20, 100))
        self.assertEqual(metric["value"], .2)
        self.assertEqual(metric["waiting_outcome_count"], 30)
        self.assertEqual(
            self.build_summary(contacts, orders)["outcome_10d"]["resolved_contact_count"],
            70,
        )

    def test_live_kpi_is_the_only_public_ten_day_performance_rate(self):
        contacts = []
        orders = []
        for index in range(4):
            contact_id = f"mature-hit-{index}"
            contacts.append((contact_id, "2026-08-01 10:00", {}))
            orders.append((f"MATURE-{index}", "2026-08-03", contact_id))
        contacts.extend(
            (f"mature-miss-{index}", "2026-08-01 11:00", {})
            for index in range(6)
        )
        for index in range(3):
            contact_id = f"early-hit-{index}"
            contacts.append((contact_id, "2026-08-16 10:00", {}))
            orders.append((f"EARLY-{index}", "2026-08-18", contact_id))
        contacts.extend(
            (f"pending-{index}", "2026-08-17 10:00", {})
            for index in range(22)
        )

        summary = self.build_summary(contacts, orders)
        live = summary["kpis"]["order_10d"]

        self.assertEqual((live["numerator"], live["denominator"]), (7, 35))
        self.assertEqual(live["waiting_outcome_count"], 22)
        self.assertNotIn("comparable", live)
        self.assertNotIn("order_10d_comparable", summary["outcome_10d"])
        drilldown = build_drilldown(summary, "order_10d")
        self.assertEqual(drilldown["total_count"], 35)
        self.assertEqual(
            sum(row["cohort_role"] == "pending" for row in drilldown["rows"]),
            22,
        )

        later = self.build_summary(
            contacts, orders, end="2026-08-31", generated="2026-08-31 12:00"
        )
        self.assertEqual(
            (later["kpis"]["order_10d"]["numerator"],
             later["kpis"]["order_10d"]["denominator"]),
            (7, 35),
        )

    def test_positive_metric_uses_all_existing_eligible_positive_population(self):
        summary = self.build_summary(
            [
                ("positive-hit", "2026-08-16 10:00", {"result": "Positiv", "channel": "Telefon"}),
                ("positive-wait", "2026-08-17 10:00", {"result": "Positiv", "channel": "Besök"}),
                ("positive-miss", "2026-08-01 10:00", {"result": "Positiv", "channel": "Telefon"}),
                ("email-hit", "2026-08-16 10:00", {"result": "Positiv", "channel": "Mejl"}),
            ],
            [
                ("POSITIVE-HIT", "2026-08-18", "positive-hit"),
                ("EMAIL-HIT", "2026-08-18", "email-hit"),
            ],
        )

        positive = summary["kpis"]["positive_to_order_10d"]
        order_metric = summary["kpis"]["order_10d"]
        self.assertEqual((positive["numerator"], positive["denominator"]), (1, 3))
        self.assertEqual(positive["waiting_outcome_count"], 1)
        self.assertEqual((order_metric["numerator"], order_metric["denominator"]), (2, 4))

    def test_manual_email_still_affects_order_but_not_positive_to_order(self):
        summary = self.build_summary(
            [
                ("phone", "2026-08-16 10:00", {"result": "Positiv", "channel": "Telefon"}),
                ("email", "2026-08-16 10:00", {"result": "Positiv", "channel": "Mejl"}),
            ],
            [
                ("PHONE", "2026-08-18", "phone"),
                ("EMAIL", "2026-08-18", "email"),
            ],
        )

        self.assertEqual(
            (summary["kpis"]["order_10d"]["numerator"],
             summary["kpis"]["order_10d"]["denominator"]),
            (2, 2),
        )
        self.assertEqual(
            (summary["kpis"]["positive_to_order_10d"]["numerator"],
             summary["kpis"]["positive_to_order_10d"]["denominator"]),
            (1, 1),
        )

    def test_manual_email_affects_live_order_but_not_live_positive_to_order(self):
        summary = self.build_summary(
            [
                ("phone", "2026-08-01 10:00", {"result": "Positiv", "channel": "Telefon"}),
                ("email", "2026-08-01 10:00", {"result": "Positiv", "channel": "Mejl"}),
            ],
            [
                ("PHONE", "2026-08-03", "phone"),
                ("EMAIL", "2026-08-03", "email"),
            ],
        )

        self.assertEqual(
            (
                summary["kpis"]["order_10d"]["numerator"],
                summary["kpis"]["order_10d"]["denominator"],
            ),
            (2, 2),
        )
        self.assertEqual(
            (
                summary["kpis"]["positive_to_order_10d"]["numerator"],
                summary["kpis"]["positive_to_order_10d"]["denominator"],
            ),
            (1, 1),
        )

    def test_seller_comparison_uses_total_eligible_rate(self):
        contacts = []
        orders = []
        for index in range(10):
            alice_id = f"alice-hit-{index}"
            contacts.append((alice_id, "2026-08-16 10:00", {"seller": "alice"}))
            orders.append((f"ALICE-{index}", "2026-08-18", alice_id))
            contacts.append((f"alice-pending-{index}", "2026-08-17 10:00", {"seller": "alice"}))
            contacts.append((f"bob-miss-{index}", "2026-08-01 10:00", {"seller": "bob"}))

        sellers = {
            item["seller"]: item
            for item in self.build_summary(contacts, orders)["seller_comparison"]
        }

        self.assertEqual(sellers["alice"]["order_10d"]["value"], .5)
        self.assertEqual(sellers["alice"]["order_10d"]["denominator"], 20)
        self.assertEqual(sellers["bob"]["order_10d"]["value"], 0)
        self.assertEqual(sellers["alice"]["positive_to_order_10d"]["value"], .5)
        self.assertEqual(sellers["bob"]["positive_to_order_10d"]["value"], 0)

    def test_peer_median_uses_live_rates_with_pending_in_denominator(self):
        contacts = []
        orders = []
        for index in range(10):
            alice_hit = f"alice-hit-{index}"
            contacts.append((alice_hit, "2026-08-01 10:00", {"seller": "alice"}))
            orders.append((f"ALICE-{index}", "2026-08-03", alice_hit))
            contacts.append((f"alice-miss-{index}", "2026-08-01 11:00", {"seller": "alice"}))
            contacts.append((f"alice-pending-{index}", "2026-08-17 10:00", {"seller": "alice"}))
            contacts.append((f"bob-miss-{index}", "2026-08-01 10:00", {"seller": "bob"}))
            carol_hit = f"carol-hit-{index}"
            contacts.append((carol_hit, "2026-08-01 10:00", {"seller": "carol"}))
            orders.append((f"CAROL-{index}", "2026-08-03", carol_hit))

        summary = self.build_summary(contacts, orders, seller="bob")

        self.assertAlmostEqual(
            summary["kpis"]["order_10d"]["comparisons"]["peer_median"],
            .66665,
        )
        self.assertAlmostEqual(
            summary["kpis"]["positive_to_order_10d"]["comparisons"]["peer_median"],
            .66665,
        )

    def test_current_pending_suppresses_live_previous_period_comparisons(self):
        contacts = []
        orders = []
        for index in range(10):
            current_id = f"current-{index}"
            previous_id = f"previous-{index}"
            contacts.append((current_id, "2026-08-16 10:00", {}))
            contacts.append((previous_id, "2026-08-01 10:00", {}))
            if index < 2:
                orders.append((f"CURRENT-{index}", "2026-08-18", current_id))
            if index < 4:
                orders.append((f"PREVIOUS-{index}", "2026-08-03", previous_id))

        metric = self.build_summary(
            contacts,
            orders,
            start="2026-08-11",
            end="2026-08-20",
            seller="olle",
        )["kpis"]["order_10d"]

        self.assertEqual(
            (
                metric["numerator"], metric["denominator"],
                metric["waiting_outcome_count"], metric["value"],
            ),
            (2, 10, 8, .2),
        )
        self.assertIsNone(metric["comparisons"]["previous_period"])
        self.assertIsNone(metric["comparisons"]["delta_previous"])
        self.assertEqual(
            metric["comparisons"]["previous_period_status"], "sufficient"
        )
        self.assertEqual(
            metric["comparisons"]["previous_period_suppressed_reason"],
            "pending_10d_outcomes",
        )
        self.assertNotIn("comparable", metric)
        positive = self.build_summary(
            contacts,
            orders,
            start="2026-08-11",
            end="2026-08-20",
            seller="olle",
        )["kpis"]["positive_to_order_10d"]
        self.assertEqual(
            (
                positive["numerator"], positive["denominator"],
                positive["waiting_outcome_count"], positive["value"],
            ),
            (2, 10, 8, .2),
        )
        self.assertIsNone(positive["comparisons"]["previous_period"])
        self.assertIsNone(positive["comparisons"]["delta_previous"])
        self.assertEqual(
            positive["comparisons"]["previous_period_suppressed_reason"],
            "pending_10d_outcomes",
        )

    def test_previous_pending_suppresses_both_live_metrics(self):
        contacts = []
        orders = []
        for index in range(10):
            current_id = f"current-converted-{index}"
            contacts.append((current_id, "2026-08-18 10:00", {}))
            orders.append((f"CURRENT-{index}", "2026-08-19", current_id))
            contacts.append((f"previous-pending-{index}", "2026-08-16 10:00", {}))

        summary = self.build_summary(
            contacts,
            orders,
            start="2026-08-18",
            end="2026-08-20",
            seller="olle",
        )

        for metric_key in ("order_10d", "positive_to_order_10d"):
            with self.subTest(metric_key=metric_key):
                comparison = summary["kpis"][metric_key]["comparisons"]
                self.assertIsNone(comparison["previous_period"])
                self.assertIsNone(comparison["delta_previous"])
                self.assertEqual(
                    comparison["previous_period_suppressed_reason"],
                    "pending_10d_outcomes",
                )

    def test_complete_live_periods_keep_previous_comparisons(self):
        contacts = []
        orders = []
        for index in range(10):
            current_id = f"current-{index}"
            previous_id = f"previous-{index}"
            contacts.append((current_id, "2026-08-01 10:00", {}))
            contacts.append((previous_id, "2026-07-20 10:00", {}))
            if index < 4:
                orders.append((f"CURRENT-{index}", "2026-08-03", current_id))
            if index < 6:
                orders.append((f"PREVIOUS-{index}", "2026-07-22", previous_id))

        summary = self.build_summary(contacts, orders, seller="olle")

        for metric_key in ("order_10d", "positive_to_order_10d"):
            with self.subTest(metric_key=metric_key):
                metric = summary["kpis"][metric_key]
                self.assertEqual(metric["waiting_outcome_count"], 0)
                self.assertEqual(metric["comparisons"]["previous_period"], .6)
                self.assertAlmostEqual(
                    metric["comparisons"]["delta_previous"], -.2
                )
                self.assertNotIn(
                    "previous_period_suppressed_reason", metric["comparisons"]
                )
        self.assertEqual(
            summary["kpis"]["positive_dialogue"]["comparisons"][
                "previous_period"
            ],
            1,
        )

    def test_live_previous_period_keeps_small_sample_status_without_value(self):
        contacts = [
            (f"current-{index}", "2026-08-01 10:00", {})
            for index in range(10)
        ] + [
            (f"previous-{index}", "2026-07-20 10:00", {})
            for index in range(5)
        ]

        metric = self.build_summary(
            contacts, seller="olle"
        )["kpis"]["order_10d"]

        self.assertIsNone(metric["comparisons"]["previous_period"])
        self.assertEqual(
            metric["comparisons"]["previous_period_status"], "small_sample"
        )

    def test_weekly_trend_uses_new_pending_definition(self):
        converted_only = self.build_summary(
            [("weekly-hit", "2026-08-17 10:00", {})],
            [("WEEKLY-HIT", "2026-08-19", "weekly-hit")],
        )
        summary = self.build_summary(
            [
                ("weekly-hit", "2026-08-17 10:00", {}),
                ("weekly-wait", "2026-08-18 10:00", {}),
            ],
            [("WEEKLY-HIT", "2026-08-19", "weekly-hit")],
        )

        complete_week = converted_only["weekly_trend"][0]
        week = summary["weekly_trend"][0]
        self.assertEqual(complete_week["resolved_converted_contacts"], 1)
        self.assertEqual(complete_week["waiting_outcome_count"], 0)
        self.assertTrue(complete_week["outcome_complete"])
        self.assertEqual(week["resolved_converted_contacts"], 1)
        self.assertEqual(week["waiting_outcome_count"], 1)
        self.assertEqual(
            (week["order_10d"]["numerator"], week["order_10d"]["denominator"]),
            (1, 2),
        )
        self.assertFalse(week["outcome_complete"])

    def test_kpi_drilldowns_match_total_eligible_denominator_and_show_outcomes(self):
        summary = self.build_summary(
            [
                ("early-hit", "2026-08-16 10:00", {"result": "Positiv"}),
                ("early-wait", "2026-08-17 10:00", {"result": "Positiv"}),
                ("mature-miss", "2026-08-01 10:00", {"result": "Positiv"}),
            ],
            [("EARLY-HIT", "2026-08-18", "early-hit")],
        )

        for metric_key in ("order_10d", "positive_to_order_10d"):
            with self.subTest(metric_key=metric_key):
                metric = summary["kpis"][metric_key]
                drilldown = build_drilldown(summary, metric_key)
                self.assertEqual(drilldown["total_count"], metric["denominator"])
                self.assertEqual(
                    {row["contact_id"] for row in drilldown["rows"]},
                    {"early-hit", "early-wait", "mature-miss"},
                )
                self.assertEqual(
                    sum(row["cohort_role"] == "numerator" for row in drilldown["rows"]),
                    metric["numerator"],
                )
                self.assertEqual(
                    {row["cohort_role"] for row in drilldown["rows"]},
                    {"numerator", "pending", "resolved_without_order"},
                )
                self.assertEqual(
                    next(row for row in drilldown["rows"] if row["contact_id"] == "early-wait")["outcome_status"],
                    "pending",
                )
        self.assertEqual(
            build_drilldown(summary, "resolved_order_10d")["total_count"],
            summary["outcome_10d"]["resolved_contact_count"],
        )
        self.assertEqual(
            build_drilldown(summary, "converted_order_10d")["total_count"],
            summary["outcome_10d"]["attributed_order_contact_count"],
        )


class Team10dTrendTests(TestCase):
    def build_summary(
        self, specs=(), converted=(), *, generated="2026-08-31 12:00",
        start="2026-08-24", end="2026-08-31", seller="olle",
        channel="visit", segment="all", lifecycle="all",
    ):
        customers, rows, orders = [], [], []
        customer_by_contact = {}
        for index, spec in enumerate(specs, start=1):
            contact_id, when = spec[:2]
            options = dict(spec[2]) if len(spec) > 2 else {}
            customer_id = f"trend-customer-{index}"
            customer_name = f"Trend customer {index}"
            customer_number = f"TREND-{index}"
            contact_seller = options.pop("seller", "olle")
            customer_segment = options.pop("segment", "A")
            contact_lifecycle = options.pop("lifecycle", "prospect")
            customers.append({
                "customer": customer_name,
                "customer_id": customer_id,
                "customer_number": customer_number,
                "sales_person": contact_seller,
                "customer_segment": customer_segment,
            })
            rows.append(activity(
                contact_id,
                when,
                customer_id=customer_id,
                customer=customer_name,
                seller=contact_seller,
                priority_snapshot_quality="exact",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                customer_segment_at_contact=customer_segment,
                lifecycle_at_contact=contact_lifecycle,
                **options,
            ))
            customer_by_contact[contact_id] = (
                customer_id, customer_name, customer_number
            )
        for order_index, (contact_id, order_date) in enumerate(converted, start=1):
            customer_id, customer_name, customer_number = customer_by_contact[
                contact_id
            ]
            row = order(
                f"TREND-ORDER-{order_index}",
                order_date,
                customer_id=customer_id,
                customer=customer_name,
            )
            row["Customer number"] = customer_number
            orders.append(row)
        users = [
            {"user_name": "olle", "name": "Olle", "active": "Y"},
            {"user_name": "sofia", "name": "Sofia", "active": "Y"},
            {"user_name": "viewer", "name": "Viewer", "active": "Y"},
            {"user_name": "admin", "name": "Admin", "active": "Y", "admin": "Y"},
            {"user_name": "inactive", "name": "Inactive", "active": "N"},
        ]
        return build_sales_coaching_summary(
            activities=rows,
            customers=customers,
            users=users,
            order_rows=orders,
            start=start,
            end=end,
            generated_at=generated,
            score_version="v2.1",
            seller=seller,
            channel=channel,
            segment=segment,
            lifecycle=lifecycle,
        )

    @staticmethod
    def point(summary, metric, seller, week):
        seller_series = next(
            item for item in summary["team_10d_trends"]["metrics"][metric]["series"]
            if item["seller"] == seller
        )
        return next(point for point in seller_series["points"] if point["week"] == week)

    def test_cutoff_uses_only_whole_weeks_and_includes_exact_boundary(self):
        exact = self.build_summary(
            [("exact", "2027-01-10 10:00", {})],
            generated="2027-01-20 12:00",
            start="2027-01-18",
            end="2027-01-20",
        )
        trend = exact["team_10d_trends"]

        self.assertEqual(trend["weeks"], 16)
        self.assertEqual(len(trend["week_axis"]), 16)
        self.assertEqual(trend["latest_complete_week"], "2027-W01")
        self.assertEqual(trend["period"]["end"], "2027-01-10")
        self.assertEqual(
            (self.point(exact, "order_10d", "olle", "2027-W01")["numerator"],
             self.point(exact, "order_10d", "olle", "2027-W01")["denominator"]),
            (0, 1),
        )
        starts = [date.fromisoformat(slot["period"]["start"]) for slot in trend["week_axis"]]
        self.assertTrue(all(
            right - left == timedelta(days=7)
            for left, right in zip(starts, starts[1:])
        ))
        self.assertIn("2026-W53", [slot["week"] for slot in trend["week_axis"]])
        self.assertIn("2027-W01", [slot["week"] for slot in trend["week_axis"]])

        before_cutoff = self.build_summary(
            [],
            generated="2027-01-19 12:00",
            start="2027-01-18",
            end="2027-01-19",
        )
        self.assertEqual(
            before_cutoff["team_10d_trends"]["latest_complete_week"],
            "2026-W53",
        )

    def test_individually_mature_contact_does_not_open_partial_week(self):
        summary = self.build_summary(
            [("partial", "2027-01-11 09:00", {})],
            generated="2027-01-21 12:00",
            start="2027-01-18",
            end="2027-01-21",
        )

        self.assertEqual(
            summary["_analysis"]["attribution"]["maturity"]["partial"],
            "mature",
        )
        trend = summary["team_10d_trends"]
        self.assertEqual(trend["latest_complete_week"], "2027-W01")
        self.assertNotIn("2027-W02", [slot["week"] for slot in trend["week_axis"]])

    def test_points_are_exact_weeks_and_live_metrics_reconcile_with_drilldowns(self):
        summary = self.build_summary(
            [
                ("phone-hit", "2026-08-02 10:00", {"channel": "Telefon"}),
                ("manual-email", "2026-08-02 11:00", {"channel": "Mejl"}),
            ],
            [("phone-hit", "2026-08-03")],
        )
        order_point = self.point(summary, "order_10d", "olle", "2026-W31")
        positive_point = self.point(
            summary, "positive_to_order_10d", "olle", "2026-W31"
        )
        next_point = self.point(summary, "order_10d", "olle", "2026-W32")

        self.assertEqual(
            (order_point["numerator"], order_point["denominator"],
             order_point["value"], order_point["status"]),
            (1, 2, 0.5, "small_sample"),
        )
        self.assertEqual(
            (positive_point["numerator"], positive_point["denominator"],
             positive_point["value"], positive_point["status"]),
            (1, 1, 1, "small_sample"),
        )
        self.assertEqual(
            (next_point["numerator"], next_point["denominator"],
             next_point["value"], next_point["status"]),
            (0, 0, None, "not_computable"),
        )
        self.assertNotIn("waiting_outcome_count", order_point)

        week_summary = self.build_summary(
            [
                ("phone-hit", "2026-08-02 10:00", {"channel": "Telefon"}),
                ("manual-email", "2026-08-02 11:00", {"channel": "Mejl"}),
            ],
            [("phone-hit", "2026-08-03")],
            start=order_point["period"]["start"],
            end=order_point["period"]["end"],
            channel="all",
        )
        for metric, point in (
            ("order_10d", order_point),
            ("positive_to_order_10d", positive_point),
        ):
            with self.subTest(metric=metric):
                central = week_summary["outcome_10d"][metric]
                self.assertEqual(
                    {key: point[key] for key in (
                        "value", "numerator", "denominator", "status"
                    )},
                    {key: central[key] for key in (
                        "value", "numerator", "denominator", "status"
                    )},
                )
                drilldown = build_drilldown(week_summary, metric)
                self.assertEqual(drilldown["total_count"], point["denominator"])
                self.assertEqual(
                    sum(
                        row["cohort_role"] == "numerator"
                        for row in drilldown["rows"]
                    ),
                    point["numerator"],
                )
                self.assertNotIn(
                    "pending", {row["cohort_role"] for row in drilldown["rows"]}
                )
        drilldown = build_drilldown(week_summary, "order_10d")
        self.assertEqual(
            sum(row["cohort_role"] == "numerator" for row in drilldown["rows"]),
            order_point["numerator"],
        )
        self.assertEqual({row["channel"] for row in drilldown["rows"]}, {"phone", "email"})

    def test_team_population_ignores_period_seller_channel_but_applies_dimensions(self):
        specs, converted = [], []
        for index in range(8):
            contact_id = f"olle-{index}"
            specs.append((contact_id, "2026-08-02 10:00", {
                "seller": "olle",
                "channel": "Mejl" if index == 0 else "Telefon",
                "segment": "A",
                "lifecycle": "prospect",
            }))
            if index < 4:
                converted.append((contact_id, "2026-08-03"))
        for index in range(10):
            contact_id = f"sofia-{index}"
            specs.append((contact_id, "2026-08-02 10:00", {
                "seller": "sofia",
                "channel": "Telefon",
                "segment": "A",
                "lifecycle": "prospect",
            }))
            if index < 3:
                converted.append((contact_id, "2026-08-03"))
        specs.extend([
            ("wrong-segment", "2026-08-02 10:00", {
                "seller": "olle", "segment": "B", "lifecycle": "prospect",
            }),
            ("wrong-lifecycle", "2026-08-02 10:00", {
                "seller": "olle", "segment": "A", "lifecycle": "established",
            }),
        ])

        summary = self.build_summary(
            specs,
            converted,
            start="2026-08-30",
            end="2026-08-31",
            seller="olle",
            channel="visit",
            segment="A",
            lifecycle="prospect",
        )
        trend = summary["team_10d_trends"]

        self.assertEqual(trend["selected_seller"], "olle")
        self.assertEqual(
            [
                item["seller"]
                for item in trend["metrics"]["order_10d"]["series"]
            ],
            ["olle", "sofia", "viewer"],
        )
        self.assertEqual(
            (self.point(summary, "order_10d", "olle", "2026-W31")["numerator"],
             self.point(summary, "order_10d", "olle", "2026-W31")["denominator"],
             self.point(summary, "order_10d", "olle", "2026-W31")["status"]),
            (4, 8, "small_sample"),
        )
        self.assertEqual(
            (self.point(summary, "order_10d", "sofia", "2026-W31")["numerator"],
             self.point(summary, "order_10d", "sofia", "2026-W31")["denominator"],
             self.point(summary, "order_10d", "sofia", "2026-W31")["status"]),
            (3, 10, "sufficient"),
        )
        self.assertEqual(
            (self.point(summary, "order_10d", "viewer", "2026-W31")["value"],
             self.point(summary, "order_10d", "viewer", "2026-W31")["denominator"],
             self.point(summary, "order_10d", "viewer", "2026-W31")["status"]),
            (None, 0, "not_computable"),
        )

    def test_both_live_trends_share_axis_but_keep_distinct_populations(self):
        specs = []
        converted = []
        for index in range(10):
            contact_id = f"deterministic-{index}"
            specs.append((contact_id, "2026-07-20 10:00", {
                "result": "Positiv" if index < 6 else "Neutral",
                "channel": "Telefon",
            }))
            if index < 3 or index == 6:
                converted.append((contact_id, "2026-07-22"))

        summary = self.build_summary(specs, converted)
        trends = summary["team_10d_trends"]
        order_point = self.point(summary, "order_10d", "olle", "2026-W30")
        positive_point = self.point(
            summary, "positive_to_order_10d", "olle", "2026-W30"
        )

        self.assertEqual(set(trends["metrics"]), {
            "order_10d", "positive_to_order_10d",
        })
        self.assertEqual(
            (order_point["numerator"], order_point["denominator"], order_point["value"]),
            (4, 10, .4),
        )
        self.assertEqual(
            (positive_point["numerator"], positive_point["denominator"],
             positive_point["value"]),
            (3, 6, .5),
        )
        axis = [slot["week"] for slot in trends["week_axis"]]
        for metric in trends["metrics"].values():
            self.assertEqual(
                [point["week"] for point in metric["series"][0]["points"]],
                axis,
            )
        self.assertNotIn("comparable", repr(trends))


class SnapshotAndAggregateTests(TestCase):
    def summary(self, rows, orders=(), users=None, customers=None, **kwargs):
        return build_sales_coaching_summary(
            activities=rows,
            customers=customers or CUSTOMERS,
            users=users or USERS,
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

        order_kpi = summary["outcome_10d"]["order_10d"]
        self.assertEqual((order_kpi["numerator"], order_kpi["denominator"]), (1, 1))
        self.assertEqual(order_kpi["value"], 1)
        self.assertEqual(summary["outcome_10d"]["attributed_order_contact_count"], 1)

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

        order_kpi = summary["outcome_10d"]["order_10d"]
        identity_coverage = summary["data_quality"]["order_attribution_identity_coverage"]
        self.assertEqual((order_kpi["numerator"], order_kpi["denominator"]), (1, 1))
        self.assertEqual((identity_coverage["numerator"], identity_coverage["denominator"]), (1, 2))
        self.assertEqual(summary["kpis"]["positive_dialogue"]["denominator"], 2)
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "order_10d")["rows"]],
            ["converted"],
        )

    def test_fresh_converted_contact_is_in_resolved_order_drilldown(self):
        summary = self.summary(
            [activity("fresh", "2026-08-15 10:00")],
            [order("FRESH", "2026-08-16")],
        )

        self.assertEqual(summary["outcome_10d"]["order_10d"]["denominator"], 1)
        self.assertEqual(build_drilldown(summary, "order_10d")["total_count"], 1)
        self.assertEqual(build_drilldown(summary, "order_10d_sync")["total_count"], 1)

    def test_positive_manual_email_does_not_enter_synchronous_funnel(self):
        summary = self.summary([
            activity("phone", "2026-08-01 10:00", channel="Telefon", result="Neutral"),
            activity("manual-email", "2026-08-02 10:00", channel="Mejl", result="Positiv"),
        ])

        self.assertEqual(summary["funnel"]["attempts"], 1)
        self.assertEqual(summary["funnel"]["reached"], 1)
        self.assertEqual(summary["funnel"]["positive"], 0)
        self.assertEqual(sum(week["positive"] for week in summary["weekly_trend"]), 0)
        email_dialogue = summary["channel_effectiveness"]["email"]["positive_dialogue"]
        self.assertEqual((email_dialogue["numerator"], email_dialogue["denominator"]), (0, 0))
        self.assertIsNone(email_dialogue["value"])
        self.assertEqual(email_dialogue["status"], "not_computable")
        email_closing = summary["channel_effectiveness"]["email"]["positive_to_order_10d"]
        self.assertEqual((email_closing["numerator"], email_closing["denominator"]), (0, 0))
        self.assertIsNone(email_closing["value"])
        self.assertEqual(email_closing["status"], "not_computable")

    def test_positive_to_order_excludes_email_but_includes_visit_and_phone(self):
        customers = CUSTOMERS + [{
            "customer": "Tredje butiken",
            "customer_id": "customer-3",
            "customer_number": "300",
            "sales_person": "Olle",
            "customer_segment": "A",
        }]
        phone = activity(
            "phone-positive", "2026-08-01 10:00",
            customer_id="customer-1", result="Positiv",
        )
        email = activity(
            "email-positive", "2026-08-03 10:00",
            customer_id="customer-2", customer="Andra butiken",
            channel="Mejl", result="Positiv",
        )
        visit = activity(
            "visit-positive", "2026-08-05 10:00",
            customer_id="customer-3", customer="Tredje butiken",
            channel="Besök", result="Positiv",
        )
        phone_order = order("PHONE", "2026-08-02", customer_id="customer-1")
        email_order = order(
            "EMAIL", "2026-08-04", customer_id="customer-2",
            customer="Andra butiken",
        )
        visit_order = order(
            "VISIT", "2026-08-06", customer_id="customer-3",
            customer="Tredje butiken",
        )
        visit_order["Customer number"] = "300"

        phone_summary = self.summary(
            [phone], [phone_order], customers=customers,
        )
        email_summary = self.summary(
            [phone, email], [phone_order, email_order], customers=customers,
        )
        visit_summary = self.summary(
            [phone, email, visit],
            [phone_order, email_order, visit_order],
            customers=customers,
        )

        self.assertEqual(
            (phone_summary["kpis"]["positive_to_order_10d"]["numerator"],
             phone_summary["kpis"]["positive_to_order_10d"]["denominator"]),
            (1, 1),
        )
        self.assertEqual(
            (email_summary["kpis"]["positive_to_order_10d"]["numerator"],
             email_summary["kpis"]["positive_to_order_10d"]["denominator"]),
            (1, 1),
        )
        self.assertEqual(
            (visit_summary["kpis"]["positive_to_order_10d"]["numerator"],
             visit_summary["kpis"]["positive_to_order_10d"]["denominator"]),
            (2, 2),
        )
        email_channel = visit_summary["channel_effectiveness"]["email"]
        self.assertEqual(
            (email_channel["positive_to_order_10d"]["numerator"],
             email_channel["positive_to_order_10d"]["denominator"]),
            (0, 0),
        )
        self.assertEqual(email_channel["positive_to_order_10d"]["status"], "not_computable")
        self.assertEqual(
            (email_channel["order_10d"]["numerator"], email_channel["order_10d"]["denominator"]),
            (1, 1),
        )
        self.assertEqual(email_channel["attributed_orders"], 1)
        self.assertEqual(email_channel["dfp"], 1)
        self.assertEqual(email_channel["order_value_by_currency"], {"SEK": 100.0})
        closing_rows = build_drilldown(visit_summary, "positive_to_order_10d")["rows"]
        self.assertEqual(
            {row["contact_id"] for row in closing_rows},
            {"phone-positive", "visit-positive"},
        )
        self.assertEqual({row["channel"] for row in closing_rows}, {"visit", "phone"})

        waiting_summary = self.summary(
            [
                phone,
                activity(
                    "email-waiting", "2026-08-15 10:00",
                    customer_id="customer-2", customer="Andra butiken",
                    channel="Mejl", result="Positiv",
                ),
            ],
            [phone_order],
            customers=customers,
        )
        self.assertEqual(
            waiting_summary["kpis"]["positive_to_order_10d"]["waiting_outcome_count"],
            0,
        )
        self.assertEqual(waiting_summary["outcome_10d"]["waiting_outcome_count"], 1)

    def test_positive_drilldowns_match_kpi_and_synchronous_populations(self):
        summary = self.summary([
            activity("manual-email", "2026-08-01 10:00", channel="Mejl", result="Positiv"),
            activity("sync-positive", "2026-08-02 10:00", channel="Telefon", result="Positiv"),
            activity("unknown-channel", "2026-08-03 10:00", channel="SMS", result="Positiv"),
        ])

        self.assertEqual(summary["kpis"]["positive_dialogue"]["numerator"], 1)
        self.assertEqual(
            {row["contact_id"] for row in build_drilldown(summary, "positive_dialogue")["rows"]},
            {"sync-positive"},
        )
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "positive_sync")["rows"]],
            ["sync-positive"],
        )

    def test_positive_dialogue_uses_only_reached_visits_and_phone_calls(self):
        base = [
            activity("phone-neutral", "2026-08-01 10:00", channel="Telefon", result="Neutral"),
        ]
        email_added = base + [
            activity("email-positive", "2026-08-02 10:00", channel="Mejl", result="Positiv"),
        ]
        phone_added = email_added + [
            activity("phone-positive", "2026-08-03 10:00", channel="Telefon", result="Positiv"),
        ]
        visit_added = phone_added + [
            activity("visit-positive", "2026-08-04 10:00", channel="Besök", result="Positiv"),
            activity("visit-bom", "2026-08-05 10:00", channel="Besök", result="Ej anträffbar"),
        ]

        base_metric = self.summary(base)["kpis"]["positive_dialogue"]
        email_metric = self.summary(email_added)["kpis"]["positive_dialogue"]
        phone_metric = self.summary(phone_added)["kpis"]["positive_dialogue"]
        visit_metric = self.summary(visit_added)["kpis"]["positive_dialogue"]

        self.assertEqual((base_metric["numerator"], base_metric["denominator"]), (0, 1))
        self.assertEqual((email_metric["numerator"], email_metric["denominator"]), (0, 1))
        self.assertEqual((phone_metric["numerator"], phone_metric["denominator"]), (1, 2))
        self.assertEqual((visit_metric["numerator"], visit_metric["denominator"]), (2, 3))
        self.assertEqual(visit_metric["value"], .6667)

    def test_positive_dialogue_drilldown_reconciles_numerator_and_denominator(self):
        summary = self.summary([
            activity("phone-positive", "2026-08-01 10:00", channel="Telefon", result="Positiv"),
            activity("visit-neutral", "2026-08-02 10:00", channel="Besök", result="Neutral"),
            activity("phone-negative", "2026-08-03 10:00", channel="Telefon", result="Negativ"),
            activity("email-positive", "2026-08-04 10:00", channel="Mejl", result="Positiv"),
            activity("visit-bom", "2026-08-05 10:00", channel="Besök", result="Ej anträffbar"),
        ])

        metric = summary["kpis"]["positive_dialogue"]
        drilldown = build_drilldown(summary, "positive_dialogue")
        roles = [row["cohort_role"] for row in drilldown["rows"]]

        self.assertEqual((metric["numerator"], metric["denominator"]), (1, 3))
        self.assertEqual(drilldown["total_count"], metric["denominator"])
        self.assertEqual(roles.count("numerator"), metric["numerator"])
        self.assertEqual(roles.count("denominator_only"), 2)
        self.assertEqual(
            {row["channel"] for row in drilldown["rows"]}, {"visit", "phone"}
        )

    def test_email_filter_makes_synchronous_dialogue_metrics_not_computable(self):
        summary = self.summary(
            [activity("email-positive", "2026-08-01 10:00", channel="Mejl", result="Positiv")],
            channel="email",
        )

        for key in ("positive_dialogue", "positive_to_order_10d"):
            with self.subTest(key=key):
                metric = summary["kpis"][key]
                self.assertEqual((metric["numerator"], metric["denominator"]), (0, 0))
                self.assertIsNone(metric["value"])
                self.assertEqual(metric["status"], "not_computable")
                channel_metric = summary["channel_effectiveness"]["email"][key]
                self.assertEqual(channel_metric["status"], "not_computable")

    def test_positive_dialogue_contract_propagates_to_comparison_previous_and_rules(self):
        rows = []
        users = [
            {"user_name": seller, "active": "Y", "admin": "N"}
            for seller in ("sofia", "olle", "maja")
        ]
        for seller, current_positive, previous_positive, email_positive in (
            ("sofia", 2, 4, 8),
            ("olle", 6, 5, 0),
            ("maja", 8, 7, 0),
        ):
            for index in range(10):
                rows.append(activity(
                    f"{seller}-current-{index}",
                    f"2026-08-{index + 1:02d} 10:00",
                    seller=seller,
                    result="Positiv" if index < current_positive else "Neutral",
                ))
                rows.append(activity(
                    f"{seller}-previous-{index}",
                    f"2026-07-{12 + index:02d} 10:00",
                    seller=seller,
                    result="Positiv" if index < previous_positive else "Neutral",
                ))
            for index in range(email_positive):
                rows.append(activity(
                    f"{seller}-email-{index}",
                    f"2026-08-{index + 1:02d} 12:00",
                    seller=seller,
                    channel="Mejl",
                    result="Positiv",
                ))

        summary = self.summary(rows, users=users, seller="sofia")
        comparison = {
            item["seller"]: item for item in summary["seller_comparison"]
        }
        positive_card = next(
            card for card in summary["coaching_cards"]
            if card["metric_key"] == "positive_dialogue"
        )

        self.assertEqual(comparison["sofia"]["positive_dialogue"]["value"], .2)
        self.assertEqual(comparison["sofia"]["positive_dialogues_count"], 2)
        self.assertEqual(summary["kpis"]["positive_dialogue"]["value"], .2)
        self.assertEqual(
            summary["kpis"]["positive_dialogue"]["comparisons"]["previous_period"],
            .4,
        )
        self.assertEqual(
            summary["kpis"]["positive_dialogue"]["comparisons"]["peer_median"],
            .7,
        )
        self.assertEqual(positive_card["evidence"]["value"], .2)
        self.assertEqual(positive_card["code"], "positive_dialogue_low")

    def test_sync_positive_to_order_live_cohort_propagates_everywhere(self):
        users = [
            {"user_name": seller, "active": "Y", "admin": "N"}
            for seller in ("sofia", "olle", "maja")
        ]
        customer_specs = [
            ("customer-1", "Sofias butik", "100"),
            ("customer-2", "Olles butik", "200"),
            ("customer-3", "Majas butik", "300"),
            ("customer-4", "Sofias mejlbutik", "400"),
            ("customer-5", "Sofias tidigare mejlbutik", "500"),
        ]
        customers = [
            {
                "customer": name,
                "customer_id": customer_id,
                "customer_number": customer_number,
                "sales_person": "Sofia",
                "customer_segment": "A",
            }
            for customer_id, name, customer_number in customer_specs
        ]
        rows = []
        for seller, customer_id, customer_name in (
            ("sofia", "customer-1", "Sofias butik"),
            ("olle", "customer-2", "Olles butik"),
            ("maja", "customer-3", "Majas butik"),
        ):
            for index in range(10):
                rows.append(activity(
                    f"{seller}-sync-{index}",
                    f"2026-08-{index + 1:02d} 10:00",
                    seller=seller,
                    customer_id=customer_id,
                    customer=customer_name,
                    channel="Telefon",
                    result="Positiv",
                ))
        for index in range(10):
            rows.append(activity(
                f"sofia-previous-sync-{index}",
                f"2026-07-{index + 12:02d} 10:00",
                seller="sofia",
                customer_id="customer-1",
                customer="Sofias butik",
                channel="Telefon",
                result="Positiv",
            ))
        for index in range(5):
            rows.append(activity(
                f"sofia-current-email-{index}",
                f"2026-08-{index + 1:02d} 12:00",
                seller="sofia",
                customer_id="customer-4",
                customer="Sofias mejlbutik",
                channel="Mejl",
                result="Positiv",
            ))
            rows.append(activity(
                f"sofia-previous-email-{index}",
                f"2026-07-{index + 12:02d} 12:00",
                seller="sofia",
                customer_id="customer-5",
                customer="Sofias tidigare mejlbutik",
                channel="Mejl",
                result="Positiv",
            ))

        def identified_order(reference, order_date, customer_id, customer_name, number):
            row = order(
                reference, order_date,
                customer_id=customer_id, customer=customer_name,
            )
            row["Customer number"] = number
            return row

        orders = [
            identified_order("OLLE", "2026-08-11", "customer-2", "Olles butik", "200"),
            identified_order("MAJA", "2026-08-11", "customer-3", "Majas butik", "300"),
            identified_order("EMAIL-CURRENT", "2026-08-06", "customer-4", "Sofias mejlbutik", "400"),
            identified_order("EMAIL-PREVIOUS", "2026-07-18", "customer-5", "Sofias tidigare mejlbutik", "500"),
        ]
        summary = self.summary(
            rows, orders, users=users, customers=customers, seller="sofia",
        )
        comparison = {item["seller"]: item for item in summary["seller_comparison"]}
        closing_card = next(
            card for card in summary["coaching_cards"]
            if card["code"] == "closing_gap"
        )
        expected = (0, 10)

        self.assertEqual(
            (summary["kpis"]["positive_to_order_10d"]["numerator"],
             summary["kpis"]["positive_to_order_10d"]["denominator"]),
            expected,
        )
        self.assertEqual(
            (comparison["sofia"]["positive_to_order_10d"]["numerator"],
             comparison["sofia"]["positive_to_order_10d"]["denominator"]),
            expected,
        )
        self.assertEqual(
            (closing_card["evidence"]["numerator"], closing_card["evidence"]["denominator"]),
            expected,
        )
        self.assertEqual(
            summary["kpis"]["positive_to_order_10d"]["comparisons"]["previous_period"],
            0,
        )
        self.assertEqual(closing_card["metric_key"], "positive_to_order_10d")
        self.assertEqual(closing_card["drilldown_metric"], "positive_to_order_10d")
        self.assertEqual(
            sum(week["positive_to_order_10d"]["denominator"] for week in summary["weekly_trend"]),
            10,
        )
        drilldown = build_drilldown(summary, "positive_to_order_10d")
        self.assertEqual(drilldown["total_count"], 10)
        self.assertEqual({row["channel"] for row in drilldown["rows"]}, {"phone"})
        email_channel = summary["channel_effectiveness"]["email"]
        self.assertEqual(email_channel["positive_to_order_10d"]["status"], "not_computable")
        self.assertEqual(
            (email_channel["order_10d"]["numerator"], email_channel["order_10d"]["denominator"]),
            (1, 5),
        )
        self.assertEqual(email_channel["attributed_orders"], 1)

    def test_metric_registry_is_public_complete_and_shared_with_main_kpis(self):
        required = {
            "human_activities", "reach", "positive_dialogue",
            "positive_to_order_10d", "order_10d", "bom_ratio",
            "high_priority_boms", "median_days_to_order",
            "positive_next_step_coverage", "planned_completed_in_time",
            "priority_focus", "strategic_coverage",
        }
        summary = self.summary([])

        self.assertTrue(required.issubset(METRIC_DEFINITIONS))
        self.assertEqual(summary["meta"]["definitions_version"], "sales_coaching_v8")
        self.assertNotIn("positive_to_order_10d_comparable", METRIC_DEFINITIONS)
        self.assertNotIn("order_10d_comparable", METRIC_DEFINITIONS)
        self.assertNotIn("comparable", summary["kpis"]["positive_to_order_10d"])
        self.assertNotIn("comparable", summary["kpis"]["order_10d"])
        self.assertEqual(
            MAIN_KPI_KEYS,
            (
                "human_activities", "reach", "positive_dialogue",
                "positive_to_order_10d", "order_10d",
            ),
        )
        self.assertEqual(list(summary["kpis"]), list(MAIN_KPI_KEYS))
        self.assertEqual(summary["metric_definitions"], METRIC_DEFINITIONS)
        self.assertEqual(METRIC_DEFINITIONS["human_activities"]["label"], "Aktiviteter")
        self.assertEqual(
            METRIC_DEFINITIONS["order_10d"]["label"],
            "Kontakt – order inom 10 dagar",
        )
        self.assertEqual(
            [step["label"] for step in summary["funnel"]["steps"]],
            [
                "Kontaktförsök via Besök/Telefon",
                "Nådda kontakter",
                "Positiva dialoger",
            ],
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_dialogue"]["channels"],
            ["visit", "phone"],
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_dialogue"]["numerator_label"],
            "positiva nådda besök/telefonsamtal",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_dialogue"]["denominator_label"],
            "nådda besök/telefonsamtal",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_to_order_10d"]["channels"],
            ["visit", "phone"],
        )
        self.assertEqual(
            METRIC_DEFINITIONS["human_activities"]["definition"],
            "Antal mänskliga aktiviteter som inte är automatiska CRM-mejl. Besök, telefon och manuella mejl redovisas som kanaler.",
        )
        for key in MAIN_KPI_KEYS:
            with self.subTest(key=key):
                definition = summary["metric_definitions"][key]
                self.assertEqual(summary["kpis"][key]["definition"], definition["definition"])
                self.assertEqual(summary["kpis"][key]["label"], definition["label"])
        for key, definition in METRIC_DEFINITIONS.items():
            if definition["metric_type"] != "rate":
                continue
            with self.subTest(key=key):
                self.assertTrue(definition.get("numerator_label"))
                self.assertTrue(definition.get("denominator_label"))

    def test_registry_user_copy_contains_no_internal_analysis_names(self):
        forbidden = {
            "sync_reached", "sync_positive", "mature_positive",
            "qualified_dialogue", "attribution_eligible",
            "historical_snapshot", "v2_contacts",
        }
        visible_copy = " ".join(
            str(value)
            for definition in METRIC_DEFINITIONS.values()
            for key, value in definition.items()
            if key in {
                "label", "definition", "numerator_label",
                "denominator_label", "not_computable_text",
            }
        ).casefold()

        for term in forbidden:
            with self.subTest(term=term):
                self.assertNotIn(term, visible_copy)

    def test_approximate_snapshot_without_percentile_is_not_priority_focus_denominator(self):
        summary = build_sales_coaching_summary(
            activities=[
                activity(
                    "exact", "2026-08-01 10:00",
                    priority_snapshot_quality="exact",
                    analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                    priority_percentile_at_contact="80",
                    priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
                ),
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

        focus = summary["priority_allocation"]["priority_focus"]
        coverage = summary["priority_allocation"]["priority_percentile_coverage"]
        self.assertEqual((focus["numerator"], focus["denominator"]), (1, 1))
        self.assertEqual((coverage["numerator"], coverage["denominator"]), (1, 1))
        self.assertEqual(summary["priority_allocation"]["snapshot_coverage"]["numerator"], 1)

    def test_small_sample_seller_does_not_count_as_peer(self):
        rows = []
        for seller, reached, attempts in (("alice", 5, 10), ("bob", 7, 10), ("tiny", 1, 1)):
            for index in range(attempts):
                rows.append(activity(
                    f"{seller}-{index}",
                    f"2026-08-{index + 1:02d} 10:00",
                    seller=seller,
                    result="Neutral" if index < reached else "Ej anträffbar",
                ))

        users = [
            {"user_name": name, "active": "Y", "admin": "N"}
            for name in ("alice", "bob", "tiny")
        ]
        summary = self.summary(rows, seller="alice", users=users)
        comparisons = {item["seller"]: item for item in summary["seller_comparison"]}

        self.assertEqual(comparisons["tiny"]["reach"]["value"], 1)
        self.assertEqual(comparisons["tiny"]["reach"]["status"], "small_sample")
        self.assertIsNone(summary["kpis"]["reach"]["comparisons"]["peer_median"])
        self.assertEqual(summary["kpis"]["reach"]["comparisons"]["peer_count"], 1)

    def test_team_rate_median_requires_two_sufficient_sellers(self):
        rows = [
            activity(
                f"alice-{index}", f"2026-08-{index + 1:02d} 10:00",
                seller="alice", result="Ej anträffbar",
            )
            for index in range(10)
        ] + [activity("tiny", "2026-08-11 10:00", seller="tiny", result="Neutral")]

        users = [
            {"user_name": name, "active": "Y", "admin": "N"}
            for name in ("alice", "tiny")
        ]
        summary = self.summary(rows, seller="alice", users=users)

        self.assertIsNone(summary["kpis"]["reach"]["comparisons"]["peer_median"])
        self.assertNotIn("reach_low", {card["code"] for card in summary["coaching_cards"]})

    def test_summary_uses_self_excluding_peers_and_previous_period_per_seller(self):
        rows = []
        for seller, current_reached, previous_reached in (
            ("sofia", 8, 6), ("olle", 5, 4), ("maja", 7, 9),
        ):
            for index in range(10):
                rows.append(activity(
                    f"{seller}-current-{index}",
                    f"2026-08-{index + 1:02d} 10:00", seller=seller,
                    result="Neutral" if index < current_reached else "Ej anträffbar",
                ))
                rows.append(activity(
                    f"{seller}-previous-{index}",
                    f"2026-07-{12 + index:02d} 10:00", seller=seller,
                    result="Neutral" if index < previous_reached else "Ej anträffbar",
                ))
        users = [
            {"user_name": name, "active": "Y", "admin": "N"}
            for name in ("sofia", "olle", "maja")
        ]

        summary = self.summary(rows, seller="sofia", users=users)
        comparison = summary["kpis"]["reach"]["comparisons"]

        self.assertEqual(comparison["peer_median"], .6)
        self.assertEqual(comparison["peer_count"], 2)
        self.assertAlmostEqual(comparison["delta_peer"], .2)
        self.assertEqual(comparison["previous_period"], .6)
        self.assertAlmostEqual(comparison["delta_previous"], .2)

    def test_channel_filtered_seller_coaching_uses_same_channel_peers_and_drilldown(self):
        users = [
            {"user_name": name, "active": "Y", "admin": "N"}
            for name in ("olle", "sofia", "maja")
        ]
        customers = []
        rows = []
        orders = []

        def add_contacts(seller, channel, count, converted, prefix):
            for index in range(count):
                customer_id = f"{prefix}-customer-{index}"
                customer_name = f"{prefix.title()} customer {index}"
                customer_number = f"{prefix[:2]}{index:03d}"
                customers.append({
                    "customer": customer_name,
                    "customer_id": customer_id,
                    "customer_number": customer_number,
                    "sales_person": seller.title(),
                    "customer_segment": "A",
                })
                rows.append(activity(
                    f"{prefix}-contact-{index}",
                    f"2026-08-{index + 1:02d} 10:00",
                    seller=seller,
                    channel=channel,
                    result="Positiv",
                    customer_id=customer_id,
                    customer=customer_name,
                ))
                if index < converted:
                    order_row = order(
                        f"{prefix.upper()}-{index}",
                        f"2026-08-{index + 2:02d}",
                        customer_id=customer_id,
                        customer=customer_name,
                    )
                    order_row["Customer number"] = customer_number
                    orders.append(order_row)

        add_contacts("olle", "Besök", 10, 2, "olle-visit")
        add_contacts("olle", "Telefon", 10, 8, "olle-phone")
        add_contacts("sofia", "Besök", 10, 5, "sofia-visit")
        add_contacts("maja", "Besök", 10, 5, "maja-visit")

        summary = self.summary(
            rows,
            orders,
            users=users,
            customers=customers,
            seller="olle",
            channel="visit",
        )
        all_channel = self.summary(
            rows,
            orders,
            users=users,
            customers=customers,
            seller="olle",
            channel="all",
        )

        metric = summary["kpis"]["positive_to_order_10d"]
        self.assertEqual(
            (metric["numerator"], metric["denominator"], metric["value"]),
            (2, 10, .2),
        )
        self.assertEqual(metric["comparisons"]["peer_median"], .5)
        self.assertEqual(metric["comparisons"]["peer_count"], 2)
        self.assertNotEqual(metric["value"], .5, "must not use Olle's all-channel rate")

        closing = next(
            card for card in summary["coaching_cards"]
            if card["code"] == "closing_gap"
        )
        self.assertEqual(
            (
                closing["evidence"]["numerator"],
                closing["evidence"]["denominator"],
                closing["evidence"]["value"],
            ),
            (2, 10, .2),
        )
        self.assertEqual(closing["evidence"]["comparisons"]["peer_median"], .5)

        drilldown = build_drilldown(
            summary, closing["drilldown_metric"]
        )
        self.assertEqual(drilldown["total_count"], closing["evidence"]["denominator"])
        self.assertEqual(
            sum(row["cohort_role"] == "numerator" for row in drilldown["rows"]),
            closing["evidence"]["numerator"],
        )
        self.assertEqual({row["channel"] for row in drilldown["rows"]}, {"visit"})

        team_olle = next(
            item for item in summary["seller_comparison"]
            if item["seller"] == "olle"
        )
        self.assertEqual(team_olle["positive_to_order_10d"]["value"], .5)
        self.assertEqual(summary["seller_comparison"], all_channel["seller_comparison"])
        self.assertEqual(summary["team_10d_trends"], all_channel["team_10d_trends"])

    def test_channel_filtered_activity_count_uses_only_filtered_previous_period(self):
        rows = [
            activity(f"phone-current-{index}", f"2026-08-0{index + 1} 10:00")
            for index in range(3)
        ] + [
            activity("phone-previous", "2026-07-15 10:00"),
            activity("visit-current", "2026-08-04 10:00", channel="Besök"),
            activity("visit-previous", "2026-07-16 10:00", channel="Besök"),
            activity("peer-phone", "2026-08-05 10:00", seller="sofia"),
        ]

        metric = self.summary(rows, seller="olle", channel="phone")["kpis"]["human_activities"]

        self.assertEqual(metric["metric_type"], "count")
        self.assertEqual(metric["unit"], "aktiviteter")
        self.assertEqual(metric["value"], 3)
        self.assertEqual(metric["comparisons"]["previous_period"], 1)
        self.assertEqual(metric["comparisons"]["delta_previous"], 2)
        self.assertIsNone(metric["comparisons"]["peer_median"])
        self.assertEqual(metric["comparisons"]["peer_count"], 0)

    def test_coached_team_is_only_active_non_admin_users_and_includes_zero_activity(self):
        users = [
            {"user_name": "alice", "active": "Y", "admin": "N"},
            {"user_name": "zero", "active": "Y", "admin": ""},
            {"user_name": "admin", "active": "Y", "admin": "Y"},
            {"user_name": "inactive", "active": "N", "admin": "N"},
        ]
        rows = [
            activity("alice", "2026-08-01 10:00", seller="alice", result="Neutral"),
            activity("admin", "2026-08-02 10:00", seller="admin", result="Neutral"),
            activity("inactive", "2026-08-02 11:00", seller="inactive", result="Neutral"),
            activity("legacy", "2026-08-02 12:00", seller="legacy", result="Neutral"),
        ]

        summary = self.summary(
            rows, [order("ORDER", "2026-08-03")], users=users,
        )

        self.assertEqual(summary["options"]["sellers"], ["alice", "zero"])
        self.assertEqual(
            [item["seller"] for item in summary["team_comparison"]["sellers"]],
            ["alice", "zero"],
        )
        self.assertEqual(summary["kpis"]["human_activities"]["value"], 1)
        self.assertEqual(summary["outcome_10d"]["order_10d"]["numerator"], 1)
        self.assertEqual(
            summary["team_comparison"]["benchmarks"]["human_activities_median"],
            0.5,
        )

    def test_team_comparison_uses_contact_level_outcomes_and_channel_counts(self):
        users = [
            {"user_name": name, "active": "Y", "admin": "N"}
            for name in ("alice", "bob", "zero")
        ]
        rows = [
            activity("alice-visit", "2026-08-01 10:00", seller="alice", channel="Besök", result="Positiv"),
            activity("alice-phone", "2026-08-02 10:00", seller="alice", channel="Telefon", result="Neutral"),
            activity("alice-email", "2026-08-03 10:00", seller="alice", channel="Mejl", result="Positiv"),
            activity("alice-bom", "2026-08-04 10:00", seller="alice", channel="Besök", result="Ej anträffbar"),
            activity("bob-waiting", "2026-08-15 10:00", seller="bob", channel="Telefon", result="Positiv"),
        ]

        summary = self.summary(
            rows,
            [order("ORDER-1", "2026-08-04"), order("ORDER-2", "2026-08-05")],
            users=users,
            seller="alice",
            channel="visit",
        )
        team = {item["seller"]: item for item in summary["team_comparison"]["sellers"]}

        self.assertEqual(team["alice"]["human_activities_total"], 4)
        self.assertEqual(team["alice"]["channel_mix"], {"visit": 2, "phone": 1, "email": 1})
        self.assertEqual(
            team["alice"]["visit_breakdown"],
            {"analysable": 2, "reached": 1, "boms": 1},
        )
        self.assertEqual(
            team["alice"]["visit_breakdown"]["reached"]
            + team["alice"]["visit_breakdown"]["boms"],
            team["alice"]["visit_breakdown"]["analysable"],
        )
        self.assertEqual(team["alice"]["positive_dialogues_count"], 1)
        self.assertEqual(team["alice"]["mature_positive_dialogues_count"], 1)
        self.assertEqual(team["alice"]["converted_positive_contacts_count"], 0)
        self.assertEqual(team["alice"]["waiting_positive_dialogues_count"], 0)
        self.assertEqual(team["alice"]["order_10d_converted_contacts"], 1)
        self.assertEqual(team["alice"]["attributed_orders"], 2)
        self.assertEqual(team["bob"]["waiting_outcome_count"], 1)
        self.assertEqual(team["bob"]["positive_dialogues_count"], 1)
        self.assertEqual(team["bob"]["mature_positive_dialogues_count"], 0)
        self.assertEqual(team["bob"]["converted_positive_contacts_count"], 0)
        self.assertEqual(team["bob"]["waiting_positive_dialogues_count"], 1)
        self.assertEqual(team["bob"]["positive_to_order_10d"]["denominator"], 1)
        self.assertEqual(team["bob"]["positive_to_order_10d"]["status"], "small_sample")
        self.assertLessEqual(
            team["alice"]["converted_positive_contacts_count"],
            team["alice"]["mature_positive_dialogues_count"],
        )
        self.assertEqual(team["zero"]["human_activities_total"], 0)

    def test_only_priority_matrix_is_built(self):
        summary = self.summary([])

        self.assertEqual(set(summary["coaching_matrices"]), {"priority"})
        self.assertEqual(summary["coaching_matrix"]["type"], "priority")
        self.assertNotIn("sales", repr(summary["coaching_matrices"]))

    def test_tie_aware_value_percentile_does_not_make_zero_values_strategic(self):
        priorities = [
            {
                "customer_id": f"customer-{index}",
                "customer": f"Kund {index}",
                "sales_person": "Olle",
                "segment": "B",
                "value_index": 0,
                "priority_score": index,
                "recommendation_eligible": True,
            }
            for index in range(1, 9)
        ]

        summary = self.summary([], current_priorities=priorities)

        self.assertEqual(summary["priority_allocation"]["strategic_coverage"]["denominator"], 0)

    def test_legacy_meeting_is_analysed_as_visit(self):
        summary = self.summary([
            activity("meeting", "2026-08-01 10:00", channel="Möte", result="Neutral"),
        ])

        self.assertEqual(normalize_contact_type("meeting"), "visit")
        self.assertEqual(summary["kpis"]["human_activities"]["channel_mix"]["visit"], 1)
        self.assertEqual(summary["funnel"]["attempts"], 1)
        self.assertEqual(summary["funnel"]["reached"], 1)

    def test_funnel_is_a_sequential_denominator_chain(self):
        rows = [
            activity("mature", "2026-08-01 10:00", result="Neutral"),
            activity("fresh", "2026-08-15 10:00", customer_id="customer-2", customer="Andra butiken", result="Neutral"),
            activity("unresolved", "2026-08-02 10:00", customer_id="", customer="Okänd butik", result="Neutral"),
            activity("missed", "2026-08-03 10:00", result="Ej anträffbar"),
        ]
        summary = self.summary(rows, [order("ORDER", "2026-08-04")])
        funnel = summary["funnel"]

        self.assertEqual([step["count"] for step in funnel["steps"]], [4, 3, 0])
        self.assertEqual(funnel["steps"][1]["rate"]["denominator"], 4)
        self.assertEqual(funnel["steps"][2]["rate"]["denominator"], 3)
        self.assertEqual(summary["outcome_10d"]["mature_contact_count"], 1)
        self.assertEqual(summary["outcome_10d"]["attributed_order_contact_count"], 1)
        self.assertEqual(
            [row["contact_id"] for row in build_drilldown(summary, "mature_reached_sync")["rows"]],
            ["mature"],
        )

    def test_high_priority_boms_are_unknown_without_historical_coverage(self):
        summary = self.summary([
            activity("bom", "2026-08-01 10:00", channel="Besök", result="Ej anträffbar"),
        ])

        metric = summary["visit_efficiency"]["high_priority_boms_metric"]
        self.assertIsNone(metric["value"])
        self.assertEqual(metric["status"], "limited_coverage")

    def test_high_priority_bom_drilldown_matches_only_comparable_v2_population(self):
        comparable = [
            activity(
                f"v2-bom-{percentile}", f"2026-08-0{index + 1} 10:00",
                channel="Besök", result="Ej anträffbar",
                priority_snapshot_quality="exact",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_percentile_at_contact=str(percentile),
                priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
            )
            for index, percentile in enumerate((90, 80, 75))
        ]
        legacy = activity(
            "legacy-bom", "2026-08-04 10:00", channel="Besök",
            result="Ej anträffbar", priority_snapshot_quality="exact",
            analytics_snapshot_version="sales_coaching_v1",
            priority_score_at_contact="99", priority_percentile_at_contact="95",
        )

        summary = self.summary([*comparable, legacy])
        metric = summary["visit_efficiency"]["high_priority_boms_metric"]
        drilldown = build_drilldown(summary, "high_priority_boms")

        self.assertEqual(metric["value"], 3)
        self.assertEqual(drilldown["total_count"], metric["value"])
        self.assertEqual(
            {row["contact_id"] for row in drilldown["rows"]},
            {"v2-bom-90", "v2-bom-80", "v2-bom-75"},
        )
        self.assertNotIn("high_priority_score_fallback", summary["visit_efficiency"])

    def test_overdue_planning_generates_deterministic_coaching_card(self):
        planned = [
            {
                "planned_activity_id": f"planned-{index}",
                "scheduled_at": f"2026-08-{index + 1:02d}T10:00:00",
                "status": "planned" if index < 2 else "completed",
                "user_name": "olle",
                "customer": "Nytt namn",
                "customer_id": "customer-1",
                "contact_type": "Telefon",
            }
            for index in range(10)
        ]

        summary = self.summary([], planned_activities=planned)
        card = next(card for card in summary["coaching_cards"] if card["code"] == "team_planning_discipline")

        self.assertEqual(summary["follow_up_discipline"]["accountable_planned"], 10)
        self.assertEqual((card["evidence"]["numerator"], card["evidence"]["denominator"]), (0, 10))
        self.assertEqual(card["drilldown_metric"], "planned_overdue")

    def test_data_quality_separates_core_from_historical_priority(self):
        quality = self.summary([
            activity("legacy", "2026-08-01 10:00", result="Neutral"),
        ])["data_quality"]

        self.assertEqual(quality["core_analytics"]["status"], "small_sample")
        self.assertEqual(quality["historical_priority"]["status"], "not_computable")
        self.assertEqual(quality["core_analytics"]["secure_customer_identity"]["value"], 1)

    def test_reach_drilldown_contains_the_full_attempt_denominator(self):
        summary = self.summary([
            activity("reached", "2026-08-01 10:00", result="Neutral"),
            activity("unreachable", "2026-08-02 10:00", result="Ej anträffbar"),
        ])

        reach_rows = build_drilldown(summary, "reach")["rows"]
        self.assertEqual({row["contact_id"] for row in reach_rows}, {"reached", "unreachable"})
        self.assertEqual(
            {row["contact_id"]: row["cohort_role"] for row in reach_rows},
            {"reached": "numerator", "unreachable": "denominator_only"},
        )
        self.assertEqual({row["contact_id"] for row in build_drilldown(summary, "attempts")["rows"]}, {"reached", "unreachable"})

    def test_rate_drilldowns_return_full_denominators_with_cohort_roles(self):
        rows = [
            activity("positive-order", "2026-08-01 10:00", customer_id="customer-1", result="Positiv"),
            activity("positive-miss", "2026-08-02 10:00", customer_id="customer-2", customer="Andra butiken", result="Positiv"),
            activity("neutral", "2026-08-03 10:00", result="Neutral"),
            activity("priority-top", "2026-08-04 10:00", result="Neutral", analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION, priority_snapshot_quality="exact", priority_percentile_at_contact="90", priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS),
            activity("priority-low", "2026-08-05 10:00", result="Neutral", analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION, priority_snapshot_quality="exact", priority_percentile_at_contact="40", priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS),
        ]
        summary = self.summary(rows, orders=[order("ORDER", "2026-08-02")])

        positive = build_drilldown(summary, "positive_dialogue")["rows"]
        closing = build_drilldown(summary, "positive_to_order_10d")["rows"]
        priority = build_drilldown(summary, "priority_focus")["rows"]

        self.assertEqual(len(positive), 5)
        self.assertEqual({row["contact_id"]: row["cohort_role"] for row in closing}, {
            "positive-order": "numerator",
            "positive-miss": "resolved_without_order",
        })
        self.assertEqual({row["contact_id"]: row["cohort_role"] for row in priority}, {
            "priority-top": "numerator", "priority-low": "denominator_only",
        })

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
                    analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                    priority_percentile_at_contact="80",
                    priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
                ))
            else:
                planned_id = f"planned-{index}"
                suggestion_id = f"suggestion-{index}"
                rows.append(activity(
                    f"approx-{index}", f"2026-08-{index + 1:02d} 10:00",
                    planned_activity_id=planned_id,
                    analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
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

        self.assertEqual(summary["seller_comparison"][0]["snapshot_coverage"]["value"], 0.6)
        self.assertEqual(summary["seller_comparison"][0]["priority_percentile_coverage"]["value"], 0.6)
        self.assertEqual(summary["coaching_matrix"]["sellers"], [])
        self.assertEqual(
            summary["coaching_matrices"]["priority"]["build_up"]["coverage"]["value"],
            0.6,
        )
        self.assertEqual(
            summary["coaching_matrices"]["priority"]["build_up"]["minimum_coverage"],
            0.7,
        )
        self.assertEqual(
            summary["coaching_matrices"]["priority"]["axes"]["x"]["key"],
            "order_10d",
        )
        self.assertIn(
            "priority_percentile_coverage_below_70",
            summary["coaching_matrix"]["insufficient_sample"][0]["reasons"],
        )

    def test_priority_matrix_sample_uses_live_order_denominator(self):
        users = [
            {"user_name": seller, "active": "Y", "admin": "N"}
            for seller in ("alice", "bob")
        ]
        rows = [
            activity(
                f"{seller}-{index}", "2026-08-16 10:00",
                seller=seller,
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_snapshot_quality="exact",
                priority_percentile_at_contact="80",
                priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
            )
            for seller in ("alice", "bob")
            for index in range(10)
        ]

        matrix = self.summary(rows, users=users)["coaching_matrix"]

        self.assertTrue(matrix["available"])
        self.assertEqual(matrix["axes"]["x"]["key"], "order_10d")
        self.assertEqual(
            {item["order_10d"]["denominator"] for item in matrix["sellers"]},
            {10},
        )
        self.assertEqual(
            {
                item["order_10d"]["waiting_outcome_count"]
                for item in matrix["sellers"]
            },
            {10},
        )
        self.assertEqual(
            {item["order_10d_comparable"]["denominator"] for item in matrix["sellers"]},
            {0},
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
        self.assertLessEqual(len(first["coaching_cards"]), 3)
        self.assertEqual(first["coaching_cards"][0]["code"], "team_repeat_boms")
        self.assertEqual(
            set(first["coaching_cards"][0]),
            {"code", "dimension", "polarity", "severity", "metric_key", "title", "observation", "evidence", "benchmark", "next_action", "recommendation", "target", "drilldown_metric", "drilldown_filters", "ranking_score"},
        )
        for key, kpi in first["kpis"].items():
            with self.subTest(kpi=key):
                self.assertTrue(kpi["definition"])
                self.assertTrue(kpi["drilldown_metric"])
        self.assertEqual(
            [step["drilldown_metric"] for step in first["funnel"]["steps"]],
            ["attempts", "reach", "positive_sync"],
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
        args = dict(
            customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities,
            score_version="v2.1", contact_at="2026-08-20 10:00",
            snapshot_created_at="2026-08-20T12:00:00+02:00",
        )

        first = build_pre_contact_snapshot(**args)
        second = build_pre_contact_snapshot(**args)

        self.assertEqual(first, second)
        self.assertEqual(first["priority_snapshot_quality"], "exact")
        self.assertEqual(first["seller_portfolio_size_at_contact"], 2)
        self.assertEqual(first["priority_percentile_at_contact"], 75)
        self.assertEqual(
            first["priority_percentile_basis_at_contact"],
            PRIORITY_PERCENTILE_BASIS,
        )
        self.assertEqual(first["snapshot_lag_hours"], 2)
        self.assertIs(first["recommendation_eligible_at_contact"], True)

    def test_suppressed_customer_snapshot_keeps_numeric_priority_percentile(self):
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
        self.assertEqual(snapshot["priority_percentile_at_contact"], 75)
        self.assertEqual(snapshot["seller_portfolio_size_at_contact"], 2)

    def test_current_planned_activity_keeps_percentile_and_traces_suppression(self):
        priorities = [
            {
                "customer_id": "customer-1", "sales_person": "Olle",
                "priority_score": 92, "recommendation_eligible": False,
                "recommendation_suppression_reason": "future_planned_activity",
                "recommendation_suppression_source_type": "planned_activity",
                "recommendation_suppression_source_id": "planned-current",
            },
            {
                "customer_id": "customer-2", "sales_person": "Olle",
                "priority_score": 20, "recommendation_eligible": True,
            },
        ]

        snapshot = build_pre_contact_snapshot(
            customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities,
            planned_row={
                "planned_activity_id": "planned-current",
                "source": "manual",
            },
            contact_at="2026-08-20 10:00",
            snapshot_created_at="2026-08-20T10:01:00+02:00",
        )

        self.assertIs(snapshot["recommendation_eligible_at_contact"], False)
        self.assertEqual(snapshot["priority_percentile_at_contact"], 75)
        self.assertEqual(
            snapshot["suppression_reason_at_contact"],
            "current_planned_activity",
        )
        self.assertEqual(
            snapshot["suppression_source_id_at_contact"], "planned-current"
        )

    def test_separate_future_activity_keeps_future_suppression_reason(self):
        snapshot = build_pre_contact_snapshot(
            customer=CUSTOMERS[0], owner=USERS[0],
            priorities=[{
                "customer_id": "customer-1", "sales_person": "Olle",
                "priority_score": 92, "recommendation_eligible": False,
                "recommendation_suppression_reason": "future_planned_activity",
                "recommendation_suppression_source_id": "planned-future",
            }],
            planned_row={"planned_activity_id": "planned-current"},
            snapshot_created_at="2026-08-20T10:00:00+02:00",
        )

        self.assertEqual(
            snapshot["suppression_reason_at_contact"], "future_planned_activity"
        )

    def test_percentile_uses_all_scored_owner_customers_and_midrank_ties(self):
        priorities = [
            {
                "customer_id": "customer-1", "sales_person": "Olle",
                "priority_score": 50, "recommendation_eligible": False,
            },
            {
                "customer_id": "customer-2", "sales_person": "Olle",
                "priority_score": 50, "recommendation_eligible": True,
            },
            {
                "customer_id": "customer-3", "sales_person": "Olle",
                "priority_score": 10, "recommendation_eligible": False,
            },
            {
                "customer_id": "customer-4", "sales_person": "Olle",
                "priority_score": 90, "recommendation_eligible": True,
            },
            {
                "customer_id": "other", "sales_person": "Sofia",
                "priority_score": 100, "recommendation_eligible": True,
            },
        ]

        snapshot = build_pre_contact_snapshot(
            customer=CUSTOMERS[0], owner=USERS[0], priorities=priorities,
            snapshot_created_at="2026-08-20T10:00:00+02:00",
        )

        self.assertEqual(snapshot["seller_portfolio_size_at_contact"], 4)
        self.assertEqual(snapshot["priority_percentile_at_contact"], 50)

    def test_v1_percentile_is_legacy_and_not_priority_focus(self):
        summary = self.summary([
            activity(
                "legacy", "2026-08-01 10:00",
                analytics_snapshot_version="sales_coaching_v1",
                priority_snapshot_quality="exact",
                priority_score_at_contact="90",
                priority_percentile_at_contact="95",
            ),
            activity(
                "v2", "2026-08-02 10:00",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_snapshot_quality="exact",
                priority_score_at_contact="80",
                priority_percentile_at_contact="80",
                priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
            ),
        ])

        self.assertEqual(
            (
                summary["priority_allocation"]["priority_focus"]["numerator"],
                summary["priority_allocation"]["priority_focus"]["denominator"],
            ),
            (1, 1),
        )
        rows = build_drilldown(summary, "human_activities")["rows"]
        legacy = next(row for row in rows if row["contact_id"] == "legacy")
        self.assertEqual(
            legacy["priority_percentile_comparability"], "legacy_incomparable"
        )

    def test_historical_coverage_denominator_contains_only_v2_contacts(self):
        rows = [
            activity(f"legacy-{index}", "2026-08-01 10:00")
            for index in range(20)
        ] + [
            activity(
                "v2-exact", "2026-08-02 10:00",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_snapshot_quality="exact",
                priority_percentile_at_contact="80",
                priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS,
            ),
            activity(
                "v2-missing", "2026-08-03 10:00",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_snapshot_quality="missing",
            ),
        ]

        history = self.summary(rows)["data_quality"]["historical_priority"]

        self.assertEqual(history["v2_contact_count"], 2)
        self.assertEqual(history["comparable_percentile_count"], 1)
        self.assertEqual(history["comparable_percentile_rate"]["value"], 0.5)

    def test_missing_percentile_is_not_a_core_data_quality_error(self):
        quality = self.summary([
            activity(
                "v2-missing", "2026-08-01 10:00", result="Neutral",
                analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION,
                priority_snapshot_quality="missing",
            )
        ])["data_quality"]

        self.assertEqual(quality["core_flagged_activity_rows"], 0)
        self.assertNotIn("missing_priority_percentile", quality["exclusion_reasons"])
        self.assertEqual(
            quality["historical_priority"]["comparable_percentile_count"], 0
        )

    def test_visit_patterns_use_canonical_contact_at(self):
        rows = [
            activity(
                f"monday-{index}", "2026-08-03 09:00",
                channel="Besök",
                result="Ej anträffbar" if index < 4 else "Neutral",
            )
            for index in range(10)
        ]

        efficiency = self.summary(rows)["visit_efficiency"]

        self.assertEqual(efficiency["weekday_patterns"][0]["label"], "Måndag")
        self.assertEqual(efficiency["time_band_patterns"][0]["label"], "Före 10")
        self.assertEqual(
            efficiency["weekday_patterns"][0]["bom_ratio"]["value"], 0.4
        )

    def test_previous_week_is_incomplete_while_ten_day_outcome_is_waiting(self):
        summary = self.summary([
            activity("previous-week", "2026-08-14 10:00", result="Neutral")
        ])

        week = summary["weekly_trend"][0]
        self.assertEqual(week["week"], "2026-W33")
        self.assertFalse(week["outcome_complete"])
        self.assertEqual(week["waiting_outcome_count"], 1)
        self.assertEqual(week["mature_contact_count"], 0)

    def test_planning_discipline_respects_current_segment_and_lifecycle(self):
        priorities = [
            {
                "customer_id": "customer-1", "customer_number": "100",
                "customer": "Nytt namn", "sales_person": "Olle",
                "segment": "A", "lifecycle": "prospect",
                "priority_score": 80, "recommendation_eligible": True,
            },
            {
                "customer_id": "customer-2", "customer_number": "200",
                "customer": "Andra butiken", "sales_person": "Olle",
                "segment": "B", "lifecycle": "established",
                "priority_score": 60, "recommendation_eligible": True,
            },
        ]
        planned = [
            {
                "planned_activity_id": f"a-{index}",
                "scheduled_at": "2026-08-05T10:00:00",
                "status": "planned", "user_name": "olle",
                "customer_id": "customer-1", "customer_number": "100",
            }
            for index in range(10)
        ] + [{
            "planned_activity_id": "b-1",
            "scheduled_at": "2026-08-05T10:00:00",
            "status": "planned", "user_name": "olle",
            "customer_id": "customer-2", "customer_number": "200",
        }]

        selected = self.summary(
            [], planned_activities=planned, current_priorities=priorities,
            segment="A", lifecycle="prospect",
        )
        excluded = self.summary(
            [], planned_activities=planned, current_priorities=priorities,
            segment="A", lifecycle="established",
        )

        self.assertEqual(selected["follow_up_discipline"]["accountable_planned"], 10)
        self.assertEqual(excluded["follow_up_discipline"]["accountable_planned"], 0)
        self.assertEqual(
            selected["meta"]["planned_metric_dimension_basis"],
            "current_customer_state",
        )

    def test_future_plans_are_not_accountable_until_due_unless_already_closed(self):
        rows = [activity("completed-early", "2026-08-19 10:00", result="Neutral")]
        planned = [
            {"planned_activity_id": "past-open", "scheduled_at": "2026-08-01T10:00:00", "status": "planned", "user_name": "olle", "customer_id": "customer-1"},
            {"planned_activity_id": "future-open", "scheduled_at": "2026-08-20T18:00:00", "status": "planned", "user_name": "olle", "customer_id": "customer-1"},
            {"planned_activity_id": "future-completed", "scheduled_at": "2026-08-20T18:00:00", "status": "completed", "completed_contact_id": "completed-early", "user_name": "olle", "customer_id": "customer-1"},
            {"planned_activity_id": "future-skipped", "scheduled_at": "2026-08-20T18:00:00", "status": "skipped", "user_name": "olle", "customer_id": "customer-1"},
            {"planned_activity_id": "future-cancelled", "scheduled_at": "2026-08-20T18:00:00", "status": "cancelled", "user_name": "olle", "customer_id": "customer-1"},
        ]

        discipline = self.summary(rows, planned_activities=planned)["follow_up_discipline"]

        self.assertEqual(discipline["accountable_planned"], 3)
        self.assertEqual(
            (discipline["planned_completed_in_time"]["numerator"], discipline["planned_completed_in_time"]["denominator"]),
            (1, 3),
        )
        self.assertEqual(discipline["overdue_planned"], 1)
        self.assertEqual(discipline["skipped"], 1)
        self.assertEqual(discipline["cancelled_excluded"], 1)

    def test_only_planned_or_completed_linked_followups_are_valid_next_steps(self):
        statuses = ("planned", "completed", "skipped", "cancelled")
        rows = [
            activity(f"source-{status}", f"2026-08-0{index + 1} 10:00")
            for index, status in enumerate(statuses)
        ]
        planned = [
            {
                "planned_activity_id": f"followup-{status}",
                "source_contact_id": f"source-{status}",
                "scheduled_at": "2026-08-08T10:00:00",
                "status": status,
                "user_name": "olle",
                "customer_id": "customer-1",
            }
            for status in statuses
        ]

        summary = self.summary(rows, planned_activities=planned)
        coverage = summary["follow_up_discipline"]["positive_next_step_coverage"]

        self.assertEqual((coverage["numerator"], coverage["denominator"]), (2, 4))
        self.assertEqual(
            {row["contact_id"] for row in build_drilldown(summary, "followup_gap")["rows"]},
            {"source-skipped", "source-cancelled"},
        )
        self.assertEqual(
            {row["contact_id"] for row in build_drilldown(summary, "followup_gap_10d")["rows"]},
            {"source-skipped", "source-cancelled"},
        )

    def test_priority_diagnostics_do_not_expose_an_operational_customer_gap(self):
        summary = build_sales_coaching_summary(
            activities=[], customers=CUSTOMERS, users=USERS, order_rows=[],
            current_priorities=[
                {"customer_id": "customer-1", "customer": "Nytt namn", "sales_person": "Olle", "priority_score": 100, "value_index": 80, "segment": "A", "recommendation_eligible": False, "recommendation_suppression_reason": "recent_contact"},
                {"customer_id": "customer-2", "customer": "Andra butiken", "sales_person": "Olle", "priority_score": 80, "value_index": 60, "segment": "B", "recommendation_eligible": True},
            ],
            start="2026-08-01", end="2026-08-20",
            generated_at="2026-08-20 12:00",
        )

        self.assertNotIn("priority_gap", summary["priority_allocation"])
        self.assertNotIn("customers", str(summary["priority_allocation"]))

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
            "planned_on_time": 3,
            "planned_overdue": 3,
            "planned_skipped": 3,
        }
        for metric, count in expected.items():
            with self.subTest(metric=metric):
                self.assertEqual(build_drilldown(summary, metric)["total_count"], count)

    def test_bom_ratio_repeat_high_priority_and_small_sample(self):
        rows = [
            activity("bom-1", "2026-08-01 10:00", channel="Besök", result="Ej anträffbar", planned_activity_id="planned-bom", priority_snapshot_quality="exact", analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION, priority_score_at_contact="80", priority_percentile_at_contact="90", priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS),
            activity("bom-2", "2026-08-02 10:00", channel="Besök", result="Ej anträffbar", priority_snapshot_quality="exact", analytics_snapshot_version=ANALYTICS_SNAPSHOT_VERSION, priority_score_at_contact="75", priority_percentile_at_contact="80", priority_percentile_basis_at_contact=PRIORITY_PERCENTILE_BASIS),
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

        bom = summary["visit_efficiency"]["bom_ratio"]
        self.assertEqual((bom["numerator"], bom["denominator"]), (2, 3))
        self.assertEqual(bom["status"], "small_sample")
        self.assertEqual(summary["visit_efficiency"]["repeat_boms"], {"customers": 1, "visits": 2})
        self.assertEqual(summary["visit_efficiency"]["high_priority_boms"], 2)
        self.assertEqual(summary["kpis"]["human_activities"]["value"], 3)
        self.assertEqual(build_drilldown(summary, "bom_ratio")["total_count"], 3)
        self.assertEqual(build_drilldown(summary, "planned_boms")["total_count"], 1)
        self.assertEqual(build_drilldown(summary, "unplanned_boms")["total_count"], 1)


if __name__ == "__main__":
    main()
