from datetime import date
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch
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
            ("Intresserad/Återkom :)", "Positivt"),
            ("Positivt", "Positivt"),
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

    def test_single_order_does_not_show_reorder_time(self):
        order_features = build_order_features(
            [
                _order("REF1", "Customer A", "2025-12-01", "2025-12-01", 30, 1000),
            ]
        )

        customers = [_customer("Customer A", "Daniel", "A", 2)]
        priority = build_priority_customers(customers, order_features, {}, "Daniel", TODAY)

        self.assertIsNone(priority[0]["expected_cycle_days"])
        self.assertIsNone(priority[0]["overdue_days"])
        self.assertFalse(any("Över normal återköpstid" in reason for reason in priority[0]["reasons"]))

    def test_positive_dialog_without_order(self):
        contact_features = build_contact_features(
            [
                _contact("Customer B", "2026-05-01 10:00", "Daniel", "Intresserad/Återkom :)"),
            ],
            {},
        )

        customers = [_customer("Customer B", "Daniel", "B", 2)]
        priority = build_priority_customers(customers, {}, contact_features, "Daniel", TODAY)

        self.assertEqual(priority[0]["priority_type"], "Varm chans")
        self.assertIn("Positiv dialog utan order", priority[0]["reasons"])
        self.assertEqual(priority[0]["recommended_action"], "Följ upp positiv dialog")

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


def _customer(name, sales_person, segment, row):
    return {
        "row": row,
        "customer": name,
        "cancelled_flag": "",
        "sales_person": sales_person,
        "customer_segment": segment,
        "customer_number": "",
    }


def _order(reference, customer, order_date, delivery_date, quantity, total, customer_number=""):
    return {
        "Reference": reference,
        "Order date": order_date,
        "Delivery date": delivery_date,
        "Customer": customer,
        "Customer number": customer_number,
        "Product": "DFP",
        "Quantity": str(quantity),
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


if __name__ == "__main__":
    main()
