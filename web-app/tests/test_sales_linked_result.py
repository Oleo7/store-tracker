from datetime import date, datetime
from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from sales_coaching import (  # noqa: E402
    SALES_CONTRIBUTION_DEFAULTS,
    attribute_orders_to_contacts,
    build_sales_coaching_summary,
    build_sales_linked_results,
    calculate_order_tb,
    canonicalize_activities,
    group_logical_orders,
)


CUSTOMERS = [
    {
        "customer": "Butik Ett",
        "customer_id": "customer-1",
        "customer_number": "100",
        "sales_person": "johan",
        "customer_segment": "A",
    },
    {
        "customer": "Butik Två",
        "customer_id": "customer-2",
        "customer_number": "200",
        "sales_person": "daniel",
        "customer_segment": "B",
    },
]
USERS = [
    {"user_name": "johan", "name": "Johan Persson", "active": "Y", "admin": "N"},
    {"user_name": "daniel", "name": "Daniel Andersson", "active": "Y", "admin": "N"},
]


def order_row(reference, when, *, customer_id="customer-1", sku="10001",
              dfp="1", quantity="999", total="348", currency="SEK",
              placed_by="Johan Persson", placed_as="supplier", **extra):
    customer = CUSTOMERS[0] if customer_id == "customer-1" else CUSTOMERS[1]
    return {
        "Reference": reference,
        "Order date": when,
        "Customer": customer["customer"],
        "Customer number": customer["customer_number"],
        "customer_id": customer_id,
        "placedBy": placed_by,
        "placedAs": placed_as,
        "SKU": sku,
        "Quantity": quantity,
        "Total weight": dfp,
        "Total": total,
        "Total (Pre-discount)": "999999",
        "Currency": currency,
        **extra,
    }


def contact(contact_id, when, *, seller="johan", customer_id="customer-1",
            channel="Besök", result="Positiv", **extra):
    customer = CUSTOMERS[0] if customer_id == "customer-1" else CUSTOMERS[1]
    return {
        "contact_id": contact_id,
        "date_time": when,
        "sales_user_name": seller,
        "sales_person": seller,
        "customer": customer["customer"],
        "customer_id": customer_id,
        "contact_channel": channel,
        "result": result,
        "activity_source": "manual",
        **extra,
    }


class ContributionCalculationTests(TestCase):
    def grouped(self, rows):
        return group_logical_orders(rows, CUSTOMERS)["all_orders"][0]

    def test_tb_is_correct_for_every_sku(self):
        for sku, eur_cost in SALES_CONTRIBUTION_DEFAULTS["sku_eur_per_kfp"].items():
            with self.subTest(sku=sku):
                result = calculate_order_tb(self.grouped([
                    order_row("SKU-" + sku, "2026-06-01", sku=sku, dfp="10", total="3480")
                ]))
                expected = 3480 - 120 * eur_cost * 10.77 - 120 * 3.50 - 3480 * 0.02
                self.assertTrue(result["computable"])
                self.assertAlmostEqual(result["tb_sek"], expected, places=2)
                self.assertEqual(result["kfp"], 120)

    def test_total_weight_and_discounted_total_are_the_only_inputs(self):
        result = calculate_order_tb(self.grouped([
            order_row(
                "FIELDS", "2026-06-01", dfp="2", quantity="10000",
                total="500", **{"Total (Pre-discount)": "50000"},
            )
        ]))
        expected = 500 - 24 * 1.43 * 10.77 - 24 * 3.50 - 500 * 0.02
        self.assertAlmostEqual(result["tb_sek"], expected, places=2)
        self.assertEqual(result["dfp"], 2)
        self.assertEqual(result["kfp"], 24)
        self.assertEqual(result["warehouse_distribution_sek"], 84)
        self.assertEqual(result["stockfiller_fee_sek"], 10)

    def test_unknown_sku_and_non_sek_are_safely_excluded(self):
        unknown = calculate_order_tb(self.grouped([
            order_row("UNKNOWN", "2026-06-01", sku="99999")
        ]))
        foreign = calculate_order_tb(self.grouped([
            order_row("EUR", "2026-06-01", currency="EUR")
        ]))
        self.assertEqual((unknown["computable"], unknown["reason"]), (False, "unknown_sku"))
        self.assertEqual((foreign["computable"], foreign["reason"]), (False, "non_sek_currency"))

    def test_missing_total_weight_and_total_are_rejected(self):
        missing_weight = calculate_order_tb(self.grouped([
            order_row("NO-WEIGHT", "2026-06-01", dfp="")
        ]))
        missing_total = calculate_order_tb(self.grouped([
            order_row("NO-TOTAL", "2026-06-01", total="")
        ]))
        self.assertEqual(missing_weight["reason"], "invalid_total_weight")
        self.assertEqual(missing_total["reason"], "invalid_total")


class SalesLinkedCreditTests(TestCase):
    def results(self, activities, rows):
        canonical = canonicalize_activities(activities, CUSTOMERS, USERS)["activities"]
        grouped = group_logical_orders(rows, CUSTOMERS)["orders"]
        attribution = attribute_orders_to_contacts(
            canonical, grouped, generated_at="2026-07-20 12:00",
        )
        return build_sales_linked_results(grouped, attribution, canonical, USERS)

    def test_contact_credit_wins_and_multi_sku_order_is_counted_once(self):
        results = self.results(
            [contact("contact-1", "2026-06-01 10:00", seller="daniel")],
            [
                order_row("MULTI", "2026-06-05", sku="10001", placed_by="Johan Persson"),
                order_row("MULTI", "2026-06-05", sku="10002", placed_by="Johan Persson"),
            ],
        )
        self.assertEqual(len(results["credited_orders"]), 1)
        credit = results["credited_orders"][0]
        self.assertEqual((credit["seller"], credit["source"]), ("daniel", "contact_10d"))
        self.assertEqual(credit["credited_week"], "2026-W23")
        self.assertEqual(credit["dfp"], 2)

    def test_unmatched_own_supplier_order_is_fallback_credited(self):
        results = self.results([], [
            order_row("OWN", "2026-06-12", placed_by="  JOHAN   PÉRSSON ")
        ])
        credit = results["credited_orders"][0]
        self.assertEqual((credit["seller"], credit["source"]), ("johan", "own_order_unmatched"))
        self.assertEqual(credit["credited_week"], "2026-W24")

    def test_same_seller_contact_and_placer_still_gets_one_credit(self):
        results = self.results(
            [contact("same", "2026-06-01 10:00", seller="johan")],
            [order_row("SAME", "2026-06-05", placed_by="Johan Persson")],
        )
        self.assertEqual(len(results["credited_orders"]), 1)
        self.assertEqual(results["credited_orders"][0]["source"], "contact_10d")

    def test_substring_buyer_and_conflicting_or_ambiguous_placer_are_not_credited(self):
        rows = [
            order_row("BUYER", "2026-06-12", placed_by="Johan Boman", placed_as="buyer"),
            order_row("CONFLICT", "2026-06-13", placed_by="Johan Persson"),
            order_row("CONFLICT", "2026-06-13", placed_by="Daniel Andersson"),
        ]
        results = self.results([], rows)
        self.assertEqual(results["credited_orders"], [])
        reasons = {item["reason"] for item in results["excluded_orders"]}
        self.assertEqual(reasons, {"placed_as_not_supplier", "conflicting_placed_by"})

    def test_alias_shared_by_two_active_sellers_is_ambiguous(self):
        grouped = group_logical_orders([
            order_row("AMBIGUOUS", "2026-06-12", placed_by="Gemensamt Namn")
        ], CUSTOMERS)["orders"]
        users = USERS + [
            {"user_name": "seller-a", "name": "Gemensamt Namn", "active": "Y", "admin": "N"},
            {"user_name": "seller-b", "name": "Gemensamt Namn", "active": "Y", "admin": "N"},
        ]
        results = build_sales_linked_results(
            grouped, {"order_to_contact": {}}, [], users,
        )
        self.assertEqual(results["credited_orders"], [])
        self.assertEqual(results["excluded_orders"][0]["reason"], "ambiguous_placed_by")


class SalesAndActivityTrendTests(TestCase):
    def summary(self, *, segment="all", lifecycle="all"):
        activities = [
            contact(
                "contact-result", "2026-06-01 10:00", seller="daniel",
                customer_segment_at_contact="A", lifecycle_at_contact="established",
                analytics_snapshot_version="sales_coaching_v2",
                priority_snapshot_quality="exact", priority_score_at_contact="1",
            ),
            contact("reached", "2026-08-10 10:00", channel="Besök", result="Neutral"),
            contact("bom", "2026-08-11 10:00", channel="Besök", result="Ej anträffbar"),
            contact("phone", "2026-08-12 10:00", channel="Telefon", result="Negativ"),
            contact("current", "2026-08-17 10:00", channel="Telefon", result="Neutral"),
            contact(
                "auto", "2026-08-13 10:00", channel="Mejl", result="Positiv",
                email_id="email-1", activity_source="crm_email",
            ),
        ]
        rows = [
            order_row("CONTACT", "2026-06-05", placed_by="Johan Persson"),
            order_row("OWN", "2026-06-12", customer_id="customer-2", placed_by="Johan Persson"),
        ]
        return build_sales_coaching_summary(
            activities=activities,
            customers=CUSTOMERS,
            users=USERS,
            order_rows=rows,
            start="2026-06-01",
            end="2026-08-19",
            generated_at="2026-08-19 12:00",
            segment=segment,
            lifecycle=lifecycle,
        )

    @staticmethod
    def point(summary, trend_key, metric, seller, week):
        trend = summary[trend_key]["metrics"][metric]["series"]
        series = next(item for item in trend if item["seller"] == seller)
        return next(item for item in series["points"] if item["week"] == week)

    def test_sales_tb_uses_contact_week_and_own_order_week_and_reconciles(self):
        summary = self.summary()
        contact_point = self.point(summary, "team_10d_trends", "sales_linked_result", "daniel", "2026-W23")
        own_point = self.point(summary, "team_10d_trends", "sales_linked_result", "johan", "2026-W24")
        self.assertGreater(contact_point["contact_tb"], 0)
        self.assertEqual(contact_point["own_order_tb"], 0)
        self.assertGreater(own_point["own_order_tb"], 0)
        self.assertEqual(own_point["contact_tb"], 0)
        weekly_total = sum(
            point["value"]
            for series in summary["team_10d_trends"]["metrics"]["sales_linked_result"]["series"]
            for point in series["points"]
        )
        table_total = sum(
            seller["sales_linked_result"]["value"]
            for seller in summary["team_comparison"]["sellers"]
        )
        self.assertAlmostEqual(weekly_total, table_total, places=2)

    def test_historical_filter_excludes_unmatched_own_orders(self):
        summary = self.summary(segment="A", lifecycle="established")
        sellers = {item["seller"]: item for item in summary["team_comparison"]["sellers"]}
        self.assertGreater(sellers["daniel"]["sales_linked_result"]["contact_tb"], 0)
        self.assertEqual(sellers["johan"]["sales_linked_result"]["own_order_tb"], 0)
        self.assertTrue(summary["sales_linked_result"]["metadata"]["own_order_component_limited_by_historical_filter"])

    def test_activity_series_are_canonical_and_current_week_is_excluded(self):
        summary = self.summary()
        self.assertEqual(summary["human_activity_trends"]["latest_complete_week"], "2026-W33")
        axis = [item["week"] for item in summary["human_activity_trends"]["week_axis"]]
        self.assertNotIn("2026-W34", axis)
        self.assertEqual(self.point(summary, "human_activity_trends", "all", "johan", "2026-W33")["value"], 3)
        self.assertEqual(self.point(summary, "human_activity_trends", "reached_visits", "johan", "2026-W33")["value"], 1)
        self.assertEqual(self.point(summary, "human_activity_trends", "bom", "johan", "2026-W33")["value"], 1)
        self.assertEqual(self.point(summary, "human_activity_trends", "phone", "johan", "2026-W33")["value"], 1)


class SalesTrendV12Tests(TestCase):
    def summary(self, *, seller="", segment="all"):
        return build_sales_coaching_summary(
            activities=[contact("linked", "2026-06-07 10:00", seller="daniel", customer_segment_at_contact="A"),
                        contact("open", "2026-08-18 10:00", seller="johan"),
                        contact("resolved-open", "2026-08-18 10:00", seller="daniel", customer_id="customer-2")],
            customers=CUSTOMERS, users=USERS,
            order_rows=[order_row("LINK", "2026-06-08", dfp="85", quantity="9999"),
                        order_row("OWN", "2026-06-22", dfp="24"),
                        order_row("RECENT", "2026-08-19", customer_id="customer-2"),
                        order_row("BAD", "2026-06-23", sku="unknown"),
                        order_row("OLD-BAD", "2025-01-01", sku="unknown"),
                        order_row("INTERNAL", "2026-06-24", dfp="100", total="0")],
            start="2026-06-01", end="2026-08-19", generated_at="2026-08-19 12:00",
            seller=seller, segment=segment,
        )

    def test_total_uses_weight_order_week_and_is_independent_of_seller(self):
        summary = self.summary()
        trend = summary["team_10d_trends"]
        total = trend["metrics"]["total_dfp"]
        points = {p["week"]: p["value"] for p in total["series"][0]["points"]}
        self.assertEqual(points["2026-W23"], 0)
        self.assertEqual(points["2026-W24"], 85)
        self.assertEqual(points["2026-W26"], 25)  # Commercial DFP includes unknown SKU, excludes internal order.
        self.assertEqual(total, self.summary(seller="daniel")["team_10d_trends"]["metrics"]["total_dfp"])
        self.assertEqual(total["target"], 400)

    def test_linked_dfp_reuses_tb_credit_and_shared_axis(self):
        trend = self.summary()["team_10d_trends"]
        metrics = trend["metrics"]
        self.assertEqual(metrics["sales_linked_dfp"]["target"], 120)
        self.assertEqual(metrics["sales_linked_result"]["target"], 15000)
        axis = [slot["week"] for slot in trend["week_axis"]]
        for metric in metrics.values():
            for series in metric["series"]:
                self.assertEqual([p["week"] for p in series["points"]], axis)
        total = 0
        for dfp_series, tb_series in zip(metrics["sales_linked_dfp"]["series"], metrics["sales_linked_result"]["series"]):
            for point, tb in zip(dfp_series["points"], tb_series["points"]):
                self.assertEqual(point["credited_order_count"], tb["credited_order_count"])
                self.assertEqual(point["value"], point["contact_dfp"] + point["own_order_dfp"])
                total += point["value"]
                if point["contact_dfp"]:
                    self.assertEqual((dfp_series["seller"], point["week"], point["contact_dfp"]), ("daniel", "2026-W23", 85))
                if point["own_order_dfp"]:
                    self.assertEqual((dfp_series["seller"], point["week"], point["own_order_dfp"]), ("johan", "2026-W26", 24))
        self.assertEqual(total, 109)

    def test_quality_scope_and_open_windows_including_converted_contacts(self):
        summary = self.summary()
        quality = summary["data_quality"]["sales_contribution"]
        self.assertEqual(quality["credited_order_count"], 3)
        self.assertEqual(quality["excluded_order_count"], 2)
        self.assertEqual(sum(quality["exclusion_reasons"].values()), 2)
        self.assertEqual(quality["exclusion_reasons"]["unknown_sku"], 1)
        for seller in summary["team_comparison"]["sellers"]:
            self.assertEqual(seller["open_outcome_window_count"], 1)
        filtered = self.summary(segment="B")["data_quality"]["sales_contribution"]
        self.assertEqual(filtered["excluded_order_count"], 0)
        self.assertEqual(filtered["exclusion_reasons"], {})
        self.assertEqual(quality["config"]["eur_sek"], 10.77)
        self.assertEqual(quality["config_hash"], self.summary()["data_quality"]["sales_contribution"]["config_hash"])


if __name__ == "__main__":
    main()
