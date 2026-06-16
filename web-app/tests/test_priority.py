from datetime import date
from io import BytesIO
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch
from zipfile import ZipFile
import re
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from priority import (
    build_contact_features,
    build_order_features,
    build_priority_customers,
    normalize_contact_result,
    normalize_customer_key,
)


TODAY = date(2026, 5, 6)


class PriorityTests(TestCase):
    def test_contact_result_classification(self):
        cases = [
            ("Order lagd!", "Order lagd"),
            ("Order lagd", "Order lagd"),
            ("Intresserad/Återkom :)", "Positiv"),
            ("Positivt", "Positiv"),
            ("Positiv", "Positiv"),
            ("Kräver mer bearbetning!", "Negativ"),
            ("Kräver mer bearbetning!\xa0", "Negativ"),
            ("Återkom ej", "Negativ"),
            ("Neutral", "Neutral"),
            ("Neutralt", "Neutral"),
            ("Uppföljning behövs", "Neutral"),
            ("Negativ", "Negativ"),
            ("Negativt", "Negativ"),
            ("Ej anträffbar", "Ej anträffbar"),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(normalize_contact_result(value), expected)

    def test_order_rows_are_grouped_to_customer_order(self):
        features = build_order_features(
            [
                _order("REF1", "Customer A", "2026-04-01", "2026-04-08", 10, 100),
                _order("REF1", "Customer A", "2026-04-01", "2026-04-08", 5, 50),
                _order("REF2", "Polarbär - Inköp", "2026-04-01", "2026-04-08", 99, 999),
                _order("REF3", "Customer B", "2026-04-01", "2026-04-08", 120, 0),
            ]
        )

        customer = features[normalize_customer_key("Customer A")]
        self.assertEqual(customer["order_count"], 1)
        self.assertEqual(customer["total_dfp"], 15)
        self.assertEqual(customer["latest_order_dfp"], 15)
        self.assertNotIn(normalize_customer_key("Polarbär - Inköp"), features)
        self.assertNotIn(normalize_customer_key("Customer B"), features)

    def test_order_features_prefer_total_weight_for_dfp(self):
        features = build_order_features(
            [
                _order("REF1", "Customer A", "2026-04-01", "2026-04-08", 7, 100, total_weight=6),
            ]
        )

        customer = features[normalize_customer_key("Customer A")]
        self.assertEqual(customer["total_dfp"], 6)
        self.assertEqual(customer["latest_order_dfp"], 6)
        self.assertEqual(customer["expected_order_dfp"], 6)

    def test_reorder_cycle_from_three_deliveries(self):
        features = build_order_features(
            [
                _order("REF1", "Customer A", "2026-01-01", "2026-01-01", 10, 100),
                _order("REF2", "Customer A", "2026-01-22", "2026-01-22", 10, 100),
                _order("REF3", "Customer A", "2026-02-12", "2026-02-12", 10, 100),
            ]
        )

        customer = features[normalize_customer_key("Customer A")]
        self.assertEqual(customer["median_reorder_gap_days"], 21)
        self.assertEqual(customer["expected_cycle_days"], 26)

    def test_previous_customer_over_normal_reorder_time(self):
        order_features = build_order_features(
            [
                _order("REF1", "Customer A", "2026-01-01", "2026-01-01", 30, 1000),
                _order("REF2", "Customer A", "2026-01-22", "2026-01-22", 30, 1000),
                _order("REF3", "Customer A", "2026-03-01", "2026-03-01", 30, 1000),
            ]
        )

        customers = [_customer("Customer A", "Daniel", "A", 2)]
        priority = build_priority_customers(customers, order_features, {}, "Daniel", TODAY)

        self.assertEqual(priority[0]["priority_type"], "Rädda återorder")
        self.assertGreaterEqual(priority[0]["priority_score"], 50)
        self.assertTrue(any("Över normal återköpstid" in reason for reason in priority[0]["reasons"]))
        self.assertEqual(priority[0]["next_action"]["action_type"], "reorder")
        self.assertEqual(priority[0]["next_action"]["label"], "Ring för återorder")

    def test_single_order_uses_segment_cycle_fallback_for_followup(self):
        order_features = build_order_features(
            [
                _order("REF1", "Customer A", "2025-12-01", "2025-12-01", 30, 1000),
            ]
        )

        customers = [_customer("Customer A", "Daniel", "A", 2)]
        priority = build_priority_customers(customers, order_features, {}, "Daniel", TODAY)

        self.assertEqual(priority[0]["priority_type"], "Återaktivera provorder")
        self.assertEqual(priority[0]["expected_cycle_source"], "segment")
        self.assertIsNotNone(priority[0]["expected_cycle_days"])
        self.assertGreater(priority[0]["overdue_days"], 0)
        self.assertEqual(priority[0]["next_action"]["action_type"], "trial_reorder")

    def test_high_expected_order_value_beats_small_stale_reorder(self):
        order_features = build_order_features(
            [
                _order("LOW1", "Small A", "2026-01-01", "2026-01-01", 5, 100),
                _order("LOW2", "Small A", "2026-02-01", "2026-02-01", 5, 100),
                _order("LOW3", "Small A", "2026-03-01", "2026-03-01", 5, 100),
                _order("HIGH1", "Large B", "2026-01-01", "2026-01-01", 70, 5000),
                _order("HIGH2", "Large B", "2026-03-10", "2026-03-10", 80, 6000),
            ]
        )
        contact_features = build_contact_features(
            [
                _contact("Small A", "2026-04-01 10:00", "Daniel", "Positiv"),
            ],
            order_features,
        )

        customers = [
            _customer("Small A", "Daniel", "A", 2),
            _customer("Large B", "Daniel", "B", 3),
        ]
        priority = build_priority_customers(customers, order_features, contact_features, "Daniel", TODAY)

        self.assertEqual(priority[0]["customer"], "Large B")
        self.assertGreater(priority[0]["expected_order_dfp"], priority[1]["expected_order_dfp"])

    def test_positive_dialog_without_order(self):
        contact_features = build_contact_features(
            [
                _contact("Customer B", "2026-05-01 10:00", "Daniel", "Positiv"),
            ],
            {},
        )

        customers = [_customer("Customer B", "Daniel", "B", 2)]
        priority = build_priority_customers(customers, {}, contact_features, "Daniel", TODAY)

        self.assertEqual(priority[0]["priority_type"], "Varm chans")
        self.assertIn("Positiv dialog utan order", priority[0]["reasons"])
        self.assertEqual(priority[0]["recommended_action"], "Följ upp positiv dialog")
        self.assertEqual(priority[0]["next_action"]["action_type"], "warm_lead")
        self.assertEqual(priority[0]["next_action"]["primary_cta"], "Följ upp")

    def test_overdue_followup(self):
        contact_features = build_contact_features(
            [
                _contact("Customer C", "2026-05-01 10:00", "Daniel", "Neutral", follow_up_date="2026-05-03"),
            ],
            {},
        )

        customers = [_customer("Customer C", "Daniel", "C", 2)]
        priority = build_priority_customers(customers, {}, contact_features, "Daniel", TODAY)

        self.assertEqual(priority[0]["priority_type"], "Försenad uppföljning")
        self.assertEqual(priority[0]["recommended_action"], "Följ upp")
        self.assertEqual(priority[0]["next_action"]["action_type"], "follow_up")
        self.assertEqual(priority[0]["next_action"]["tone"], "urgent")

    def test_negative_contact_cools_customer_down(self):
        customers = [
            _customer("Customer D", "Daniel", "A", 2),
            _customer("Customer E", "Daniel", "A", 3),
        ]
        contact_features = build_contact_features(
            [
                _contact("Customer D", "2026-05-01 10:00", "Daniel", "Negativ"),
            ],
            {},
        )

        priority = build_priority_customers(customers, {}, contact_features, "Daniel", TODAY)
        by_customer = {item["customer"]: item for item in priority}

        self.assertLess(by_customer["Customer D"]["priority_score"], by_customer["Customer E"]["priority_score"])
        self.assertIn("Negativ kontakt senaste 30 dagarna", by_customer["Customer D"]["reasons"])

    def test_responsible_filter(self):
        customers = [
            _customer("Customer Daniel", "Daniel", "A", 2),
            _customer("Customer Johan", "Johan", "A", 3),
        ]

        daniel = build_priority_customers(customers, {}, {}, "Daniel", TODAY)
        all_responsible = build_priority_customers(customers, {}, {}, None, TODAY)

        self.assertEqual({customer["sales_person"] for customer in daniel}, {"Daniel"})
        self.assertEqual({customer["sales_person"] for customer in all_responsible}, {"Daniel", "Johan"})

    def test_empty_data_does_not_crash(self):
        self.assertEqual(build_priority_customers([], build_order_features([]), build_contact_features([], {}), None, TODAY), [])

    def test_next_action_handles_new_ab_recent_order_and_fallback(self):
        customers = [
            _customer("New A", "Daniel", "A", 2),
            _customer("Recent Order", "Daniel", "C", 3),
            _customer("Fallback", "Daniel", "C", 4),
        ]
        order_features = build_order_features(
            [
                _order("RECENT", "Recent Order", TODAY.isoformat(), TODAY.isoformat(), 10, 1000),
            ]
        )

        priority = build_priority_customers(customers, order_features, {}, "Daniel", TODAY)
        by_customer = {item["customer"]: item for item in priority}

        self.assertEqual(by_customer["New A"]["next_action"]["action_type"], "new_ab")
        self.assertEqual(by_customer["New A"]["next_action"]["reason"], "Segment A · ingen order ännu")
        self.assertEqual(by_customer["Recent Order"]["next_action"]["action_type"], "monitor")
        self.assertEqual(by_customer["Fallback"]["next_action"]["action_type"], "route_fill")

    def test_followup_insights_endpoint_returns_priority_customers(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_number",
                "name",
                "phone",
                "email",
                "city_google",
                "region_google",
                "latitude_google",
                "longitude_google",
                "comment",
            ],
            ["Customer Daniel", "", "Daniel", "A", "1001", "Anna", "0701111111", "daniel@example.com", "", "", "", "", ""],
            ["Customer Johan", "", "Johan", "A", "1002", "Bo", "0702222222", "johan@example.com", "", "", "", "", ""],
        ]
        orders = [
            app_module.ORDER_COLUMNS,
            _row(app_module.ORDER_COLUMNS, _order("D1", "Customer Daniel", "2026-03-01", "2026-03-01", 40, 1000, customer_number="1001")),
            _row(app_module.ORDER_COLUMNS, _order("J1", "Customer Johan", "2026-03-01", "2026-03-01", 40, 1000, customer_number="1002")),
        ]
        contacts = [
            app_module.CONTACT_COLUMNS,
            _row(app_module.CONTACT_COLUMNS, _contact("Customer Daniel", "2026-05-01 10:00", "Daniel", "Neutral")),
        ]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "order_rows": orders,
                "sales_activities": contacts,
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/followup-insights?responsible=Daniel")
            data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIn("priority_customers", data)
        self.assertEqual(data["selected_responsible"], "Daniel")
        self.assertTrue(all(customer["sales_person"] == "Daniel" for customer in data["priority_customers"]))

    def test_followup_insights_returns_freezer_summary_by_sales_person(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_number",
            ],
        ]
        orders = [app_module.ORDER_COLUMNS]
        contacts = [
            app_module.CONTACT_COLUMNS,
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("ICA Kvantum A", "2026-06-01 10:00", "Daniel", "Neutral"),
                    "Franui": "1",
                    "polarbar": "1",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("ica kvantum a", "2026-06-02 10:00", "Daniel", "Neutral"),
                    "Franui": "1",
                    "Boujee": "1",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("Store B", "2026-06-03 10:00", "Johan", "Neutral"),
                    "polarbar": "true",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("Store C", "2026-06-04 10:00", "Johan", "Neutral"),
                    "none": "1",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("Store D", "2026-06-05 10:00", "johan", "Neutral"),
                    "none": "yes",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("ICA Supermarket Medborgarplatsen", "2026-05-04 10:00", "Sofia", "Neutral"),
                    "Boujee": "1",
                    "polarbar": "1",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("ica supermarket medborgarplatsen", "2026-06-12 10:00", "Sofia", "Neutral"),
                    "none": "1",
                },
            ),
        ]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "order_rows": orders,
                "sales_activities": contacts,
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/followup-insights")
            data = response.get_json()

        summary = data["freezer_summary"]
        rows = {row["field"]: row for row in summary["rows"]}
        seller_keys = {person["label"]: person["key"] for person in summary["sales_people"]}

        self.assertEqual(response.status_code, 200)
        self.assertEqual([person["label"] for person in summary["sales_people"]], ["Daniel", "Johan", "Sofia"])
        self.assertEqual(rows["Franui"]["total"], 1)
        self.assertEqual(rows["Franui"]["counts"][seller_keys["Daniel"]], 1)
        self.assertEqual(rows["Boujee"]["total"], 1)
        self.assertEqual(rows["Boujee"]["counts"][seller_keys["Sofia"]], 0)
        self.assertEqual(rows["polarbar"]["total"], 1)
        self.assertEqual(rows["polarbar"]["counts"][seller_keys["Daniel"]], 0)
        self.assertEqual(rows["polarbar"]["counts"][seller_keys["Johan"]], 1)
        self.assertEqual(rows["none"]["counts"][seller_keys["Johan"]], 2)
        self.assertEqual(rows["none"]["counts"][seller_keys["Sofia"]], 1)
        self.assertEqual(summary["sum_row"]["total"], 6)
        self.assertEqual(summary["sum_row"]["counts"][seller_keys["Daniel"]], 2)
        self.assertEqual(summary["sum_row"]["counts"][seller_keys["Johan"]], 3)
        self.assertEqual(summary["sum_row"]["counts"][seller_keys["Sofia"]], 1)
        self.assertEqual(summary["polarbar_share_row"]["total"], 17)
        self.assertEqual(summary["polarbar_share_row"]["counts"][seller_keys["Daniel"]], 0)
        self.assertEqual(summary["polarbar_share_row"]["counts"][seller_keys["Johan"]], 33)
        self.assertEqual(summary["polarbar_share_row"]["counts"][seller_keys["Sofia"]], 0)

    def test_followup_insights_dfp_team_total_includes_all_salespeople(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_number",
                "name",
                "phone",
                "email",
                "city_google",
                "region_google",
                "latitude_google",
                "longitude_google",
                "comment",
            ],
            ["Customer Daniel", "", "Daniel", "A", "1001", "", "", "", "", "", "", "", ""],
            ["Customer Johan", "", "Johan", "A", "1002", "", "", "", "", "", "", "", ""],
            ["Customer Sara", "", "Sara", "A", "1003", "", "", "", "", "", "", "", ""],
            ["Customer Lisa", "", "Lisa", "A", "1004", "", "", "", "", "", "", "", ""],
        ]
        order_date = date.today().isoformat()
        orders = [
            app_module.ORDER_COLUMNS,
            _row(app_module.ORDER_COLUMNS, _order("D1", "Customer Daniel", order_date, order_date, 50, 1000, customer_number="1001")),
            _row(app_module.ORDER_COLUMNS, _order("J1", "Customer Johan", order_date, order_date, 40, 1000, customer_number="1002")),
            _row(app_module.ORDER_COLUMNS, _order("S1", "Customer Sara", order_date, order_date, 30, 1000, customer_number="1003")),
            _row(app_module.ORDER_COLUMNS, _order("L1", "Customer Lisa", order_date, order_date, 20, 1000, customer_number="1004")),
            _row(app_module.ORDER_COLUMNS, _order("M1", "Customer Missing", order_date, order_date, 15, 1000, customer_number="9999")),
        ]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "order_rows": orders,
                "sales_activities": [app_module.CONTACT_COLUMNS],
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/followup-insights?responsible=Daniel")
            data = response.get_json()

        current_week = next(
            week for week in data["dfp_leaderboard"]
            if week["week_key"] == app_module.week_key(date.today())
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(current_week["team_total_dfp"], 155)
        self.assertEqual([leader["sales_person"] for leader in current_week["leaders"]], ["Daniel", "Johan", "Sara"])
        self.assertEqual(sum(leader["dfp_count"] for leader in current_week["leaders"]), 120)

    def test_followup_insights_returns_top_five_dfp_weeks_2026_from_total_weight(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_number",
                "name",
                "phone",
                "email",
                "city_google",
                "region_google",
                "latitude_google",
                "longitude_google",
                "comment",
            ],
        ]
        orders = [
            app_module.ORDER_COLUMNS,
            _row(app_module.ORDER_COLUMNS, _order("OLD", "Customer A", "2025-12-31", "2026-01-02", 999, 1000, total_weight=999)),
            _row(app_module.ORDER_COLUMNS, _order("W1A", "Customer A", "2026-01-01", "2026-01-02", 999, 1000, total_weight=10)),
            _row(app_module.ORDER_COLUMNS, _order("W1B", "Customer A", "2026-01-04", "2026-01-05", 999, 1000, total_weight=15)),
            _row(app_module.ORDER_COLUMNS, _order("W2", "Customer A", "2026-01-05", "2026-01-06", 999, 1000, total_weight=30)),
            _row(app_module.ORDER_COLUMNS, _order("W6", "Customer A", "2026-02-02", "2026-02-03", 999, 1000, total_weight=70)),
            _row(app_module.ORDER_COLUMNS, _order("W10A", "Customer A", "2026-03-02", "2026-03-03", 999, 1000, total_weight=35)),
            _row(app_module.ORDER_COLUMNS, _order("W10B", "Customer A", "2026-03-08", "2026-03-09", 999, 1000, total_weight=30)),
            _row(app_module.ORDER_COLUMNS, _order("W11", "Customer A", "2026-03-09", "2026-03-10", 999, 1000, total_weight=50)),
            _row(app_module.ORDER_COLUMNS, _order("W15", "Customer A", "2026-04-06", "2026-04-07", 999, 1000, total_weight=40)),
            _row(app_module.ORDER_COLUMNS, _order("W16", "Customer A", "2026-04-13", "2026-04-14", 999, 1000, total_weight=20)),
        ]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "order_rows": orders,
                "sales_activities": [app_module.CONTACT_COLUMNS],
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/followup-insights")
            data = response.get_json()

        top_weeks = data["dfp_top_weeks_2026"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual([week["week_key"] for week in top_weeks], ["2026-W06", "2026-W10", "2026-W11", "2026-W15", "2026-W02"])
        self.assertEqual([week["dfp_count"] for week in top_weeks], [70, 65, 50, 40, 30])
        self.assertEqual(top_weeks[1]["start_date"], "2026-03-02")
        self.assertEqual(top_weeks[1]["end_date"], "2026-03-08")
        self.assertEqual(top_weeks[0]["share_of_top"], 100)

    def test_customer_insights_endpoint_returns_priority_level(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_number",
                "city_google",
                "region_google",
            ],
            ["Customer A", "", "Daniel", "A", "1001", "Stockholm", "Stockholms län"],
        ]
        orders = [
            app_module.ORDER_COLUMNS,
            _row(app_module.ORDER_COLUMNS, _order("A1", "Customer A", "2026-03-01", "2026-03-01", 40, 1000, customer_number="1001")),
        ]
        contacts = [app_module.CONTACT_COLUMNS]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "order_rows": orders,
                "sales_activities": contacts,
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/customer-insights")
            data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIn("customer a", data)
        self.assertIn(data["customer a"]["priority_level"], {"Hög prio", "Medel prio", "Låg prio"})
        self.assertIn("recommended_action", data["customer a"])
        self.assertIn("reasons", data["customer a"])
        self.assertIn("next_action", data["customer a"])
        self.assertIn("order_count", data["customer a"])
        self.assertIn("total_dfp", data["customer a"])
        self.assertIn("expected_order_dfp", data["customer a"])
        self.assertNotIn("expected_order_value", data["customer a"])
        self.assertIn("latest_order_date", data["customer a"])
        self.assertIn("expected_cycle_days", data["customer a"])
        self.assertIn("expected_next_order_date", data["customer a"])
        self.assertIn("latest_contact_class", data["customer a"])
        self.assertIn("follow_up_due", data["customer a"])

    def test_customers_endpoint_reads_name_without_shifting_phone_or_email(self):
        customers = [
            [
                "customer",
                "cancelled_flag",
                "sales_person",
                "customer_segment",
                "customer_reference",
                "customer_number",
                "name",
                "phone",
                "email",
                "city_google",
                "address_google",
                "address_number_google",
                "postal_code_google",
                "region_google",
                "latitude_google",
                "longitude_google",
                "comment",
            ],
            ["Store A", "", "Daniel", "A", "REF1", "1001", "Anna Andersson", "0701234567", "anna@example.com", "Göteborg", "Avenyn", "1", "41136", "VG", "57.7", "11.9", "Ring igen"],
        ]
        fake_spreadsheet = FakeSpreadsheet(
            {
                "customers_enriched": customers,
                "sales_activities": [app_module.CONTACT_COLUMNS],
            }
        )

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/customers")
            data = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(data[0]["name"], "Anna Andersson")
        self.assertEqual(data[0]["phone"], "0701234567")
        self.assertEqual(data[0]["email"], "anna@example.com")
        self.assertEqual(data[0]["customer"], "Store A")

    def test_customer_name_column_is_inserted_left_of_phone(self):
        values = [["customer", "phone", "email"], ["Store A", "0701234567", "a@example.com"]]
        sheet = FakeWorksheet("customers_enriched", values)

        headers = app_module.ensure_customer_name_column(sheet, values[0])

        self.assertEqual(headers, ["customer", "name", "phone", "email"])
        self.assertEqual(values[0], ["customer", "name", "phone", "email"])
        self.assertEqual(values[1], ["Store A", "", "0701234567", "a@example.com"])

    def test_contact_log_payload_formats_filters_and_freezer_labels(self):
        contacts = [
            {
                **_contact("Store A", "2026-06-03 14:30", "Sofia", "Positiv", "2026-06-10"),
                "comment": "Bra möte",
                "Franui": "1",
                "Boujee": "yes",
                "polarbar": "true",
            },
            {
                **_contact("Store B", "2026-05-28 09:10", "Daniel", "Neutral"),
                "comment": "Tom disk",
                "none": "1",
            },
        ]

        all_payload = app_module.build_contact_log_payload(contacts)
        filtered_payload = app_module.build_contact_log_payload(
            contacts,
            {
                "responsible": {"Sofia"},
                "month": {"2026-06"},
                "week": {"2026-W23"},
                "result": {"Positiv"},
            },
        )

        self.assertEqual(all_payload["total_count"], 2)
        self.assertEqual(all_payload["rows"][0]["Datum"], "2026-06-03")
        self.assertEqual(all_payload["rows"][0]["Nästa uppföljning"], "2026-06-10")
        self.assertEqual(all_payload["rows"][0]["I frysdisken"], "Franui, Boujee, Polarbär")
        self.assertEqual(all_payload["rows"][1]["I frysdisken"], "Ingen")
        self.assertEqual(filtered_payload["filtered_count"], 1)
        self.assertEqual(filtered_payload["rows"][0]["Kund"], "Store A")
        self.assertIn({"value": "2026-06", "label": "2026-06"}, all_payload["filters"]["month"])
        self.assertIn({"value": "2026-W23", "label": "Vecka 23 (2026)"}, all_payload["filters"]["week"])

    def test_contact_log_customer_and_comment_filters_are_loose(self):
        contacts = [
            {
                **_contact("ICA Kvantum Åhus", "2026-06-03 14:30", "Sofia", "Positiv"),
                "comment": "Bra möte om midsommarplatsen",
            },
            {
                **_contact("ICA Supermarket Kivik", "2026-06-02 09:10", "Sofia", "Neutral"),
                "comment": "Tom disk",
            },
        ]

        payload = app_module.build_contact_log_payload(
            contacts,
            {
                "customer": "kvnt ahus",
                "comment": "bra mote",
            },
        )

        self.assertEqual(payload["filtered_count"], 1)
        self.assertEqual(payload["rows"][0]["Kund"], "ICA Kvantum Åhus")

    def test_contact_log_endpoint_and_export_use_same_filters(self):
        contacts = [
            app_module.CONTACT_COLUMNS,
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("Store A", "2026-06-03 14:30", "Sofia", "Positiv", "2026-06-10"),
                    "comment": "Bra möte",
                    "polarbar": "1",
                },
            ),
            _row(
                app_module.CONTACT_COLUMNS,
                {
                    **_contact("Store B", "2026-05-28 09:10", "Daniel", "Neutral"),
                    "comment": "Tom disk",
                    "none": "1",
                },
            ),
        ]
        fake_spreadsheet = FakeSpreadsheet({"sales_activities": contacts})

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.get("/contact-log?responsible=Sofia&month=2026-06&customer=stor+a&comment=bra+mote")
            export_response = client.get("/contact-log/export?responsible=Sofia&month=2026-06&customer=stor+a&comment=bra+mote")

        data = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data["filtered_count"], 1)
        self.assertEqual(data["rows"][0]["Kund"], "Store A")
        self.assertEqual(data["rows"][0]["I frysdisken"], "Polarbär")
        self.assertEqual(export_response.status_code, 200)
        self.assertEqual(
            export_response.mimetype,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        with ZipFile(BytesIO(export_response.data)) as workbook:
            worksheet = workbook.read("xl/worksheets/sheet1.xml").decode("utf-8")
        self.assertIn("Datum", worksheet)
        self.assertIn("Store A", worksheet)
        self.assertIn("Polarbär", worksheet)
        self.assertNotIn("Store B", worksheet)

    def test_contact_rows_merge_duplicate_polarbar_header(self):
        headers = list(app_module.CONTACT_COLUMNS) + ["polarbar"]
        contacts = [
            headers,
            _row(app_module.CONTACT_COLUMNS, _contact("Store A", "2026-06-03 14:30", "Sofia", "Positiv")) + ["true"],
        ]
        fake_spreadsheet = FakeSpreadsheet({"sales_activities": contacts})

        rows = app_module.get_contact_rows(fake_spreadsheet)

        self.assertEqual(rows[0]["polarbar"], "1")

    def test_add_contact_merges_and_removes_duplicate_polarbar_header(self):
        headers = list(app_module.CONTACT_COLUMNS) + ["polarbar"]
        contacts = [
            headers,
            _row(app_module.CONTACT_COLUMNS, _contact("Existing Store", "2026-06-03 14:30", "Sofia", "Positiv")) + ["true"],
        ]
        fake_spreadsheet = FakeSpreadsheet({"sales_activities": contacts})

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.post(
                "/customers/Store%20A/contacts",
                json={
                    "sales_person": "Sofia",
                    "contact_channel": "Besök",
                    "result": "Positiv",
                    "comment": "Test",
                    "polarbar": "1",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(contacts[0].count("polarbar"), 1)
        polarbar_idx = contacts[0].index("polarbar")
        self.assertEqual(contacts[1][polarbar_idx], "1")
        self.assertEqual(contacts[2][contacts[0].index("customer")], "Store A")
        self.assertEqual(contacts[2][polarbar_idx], "1")
        self.assertEqual(len(contacts[2]), len(contacts[0]))

    def test_add_contact_creates_and_logs_none_column(self):
        headers_without_none = [column for column in app_module.CONTACT_COLUMNS if column != "none"]
        contacts = [headers_without_none]
        fake_spreadsheet = FakeSpreadsheet({"sales_activities": contacts})

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.post(
                "/customers/Store%20A/contacts",
                json={
                    "sales_person": "Sofia",
                    "contact_channel": "Besök",
                    "result": "Neutral",
                    "comment": "Test",
                    "none": "1",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(contacts[0][-1], "none")
        appended = contacts[1]
        self.assertEqual(appended[contacts[0].index("customer")], "Store A")
        self.assertEqual(appended[contacts[0].index("none")], "1")

    def test_add_contact_requires_freezer_selection(self):
        contacts = [list(app_module.CONTACT_COLUMNS)]
        fake_spreadsheet = FakeSpreadsheet({"sales_activities": contacts})

        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=fake_spreadsheet):
            client = app_module.app.test_client()
            response = client.post("/customers/Store%20A/contacts", json={"comment": "Test"})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "freezer_selection_required")
        self.assertEqual(len(contacts), 1)


def _customer(name, sales_person, segment, row):
    return {
        "row": row,
        "customer": name,
        "cancelled_flag": "",
        "sales_person": sales_person,
        "customer_segment": segment,
        "customer_number": "",
    }


def _order(reference, customer, order_date, delivery_date, quantity, total, customer_number="", total_weight=None):
    return {
        "Reference": reference,
        "Order date": order_date,
        "Delivery date": delivery_date,
        "Customer": customer,
        "Customer number": customer_number,
        "Product": "DFP",
        "Quantity": str(quantity),
        "Total weight": str(quantity if total_weight is None else total_weight),
        "Total": str(total),
        "Unit": "DFP",
        "Currency": "SEK",
    }


def _contact(customer, date_time, sales_person, result, follow_up_date=""):
    return {
        "date_time": date_time,
        "sales_person": sales_person,
        "customer": customer,
        "contact_channel": "Besök",
        "result": result,
        "comment": "Ska inte påverka scoring",
        "customer_contact_person": "",
        "follow_up_date": follow_up_date,
    }


def _row(columns, values):
    return [values.get(column, "") for column in columns]


class FakeWorksheet:
    def __init__(self, title, values):
        self.title = title
        self._values = values

    def get_all_values(self):
        return self._values

    def row_values(self, row):
        row_idx = row - 1
        return list(self._values[row_idx]) if row_idx < len(self._values) else []

    def append_row(self, row):
        self._values.append(row)

    def update(self, values, range_name=None, **kwargs):
        match = re.match(r"([A-Z]+)(\d+):([A-Z]+)(\d+)$", range_name or "")
        if not match:
            raise ValueError(f"Unsupported range: {range_name}")

        start_col, start_row, end_col, _end_row = match.groups()
        if start_col != end_col:
            raise ValueError("FakeWorksheet.update only supports single-column updates")

        col_idx = _a1_column_to_index(start_col) - 1
        row_idx = int(start_row) - 1
        for offset, value_row in enumerate(values):
            target_row_idx = row_idx + offset
            while len(self._values) <= target_row_idx:
                self._values.append([])
            target_row = self._values[target_row_idx]
            while len(target_row) <= col_idx:
                target_row.append("")
            target_row[col_idx] = value_row[0] if value_row else ""

    def delete_columns(self, start_index, end_index=None):
        start_idx = start_index - 1
        end_idx = end_index if end_index is not None else start_index
        for row in self._values:
            del row[start_idx:end_idx]

    def insert_cols(self, columns, col=1):
        insert_idx = col - 1
        max_rows = max(len(self._values), *(len(column) for column in columns))
        while len(self._values) < max_rows:
            self._values.append([])

        for offset, column in enumerate(columns):
            for row_idx, row in enumerate(self._values):
                while len(row) < insert_idx + offset:
                    row.append("")
                value = column[row_idx] if row_idx < len(column) else ""
                row.insert(insert_idx + offset, value)


class FakeSpreadsheet:
    def __init__(self, worksheets):
        self._worksheets = worksheets

    def worksheet(self, name):
        return FakeWorksheet(name, self._worksheets[name])


def _a1_column_to_index(label):
    index = 0
    for char in label:
        index = index * 26 + ord(char) - ord("A") + 1
    return index


if __name__ == "__main__":
    main()
