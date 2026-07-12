from argparse import Namespace
from datetime import datetime, timezone
import re
from unittest import TestCase
from unittest.mock import patch

from stockfiller_orders.sync import (
    ORDER_COLUMNS,
    STATE_SHEET_NAME,
    StockfillerClient,
    SyncConfig,
    SyncWindow,
    apply_crm_customer_numbers,
    build_sync_window,
    dedupe_orders_by_reference,
    ensure_customer_email_column,
    flatten_order,
    latest_order_emails_by_customer,
    merge_headers,
    order_params_for_window,
    split_street_address,
    sync_orders,
    values_to_dicts,
    write_rows,
)


class StockfillerSyncTests(TestCase):
    def test_flatten_order_maps_stockfiller_fields_to_order_rows(self):
        rows = flatten_order(
            {
                "reference": "KGRT25BR1YBAO4",
                "createdAtDateTime": "2026-05-28T08:15:00Z",
                "updatedAtDateTime": "2026-05-28T08:20:00Z",
                "deliveryDate": "2026-06-01",
                "buyerName": "Store A",
                "placedBy": "Anna Andersson",
                "buyerEmail": "anna@example.com",
                "placedAs": "buyer",
                "buyerGln": "7350000000000",
                "buyerExternalId": "1001",
                "buyerExternalLogisticsId": "L1001",
                "customerReference": "Anna",
                "deliveryAddress": "Hantverkargatan 1A",
                "deliveryZipCode": "11152",
                "deliveryCity": "Stockholm",
                "deliveryCountryCode": "SE",
                "currency": "SEK",
                "orderRows": [
                    {
                        "productSku": "DFP-001",
                        "productName": "Dubai Fresh Pack",
                        "ordered": 3,
                        "delivered": 3,
                        "unit": "DFP",
                        "price": 12900,
                        "priceDiscounted": 9900,
                        "discountPercentage": 23.26,
                    }
                ],
            }
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["Reference"], "KGRT25BR1YBAO4")
        self.assertEqual(row["Order date"], "2026-05-28")
        self.assertEqual(row["Delivery date"], "2026-06-01")
        self.assertEqual(row["Customer"], "Store A")
        self.assertEqual(row["placedBy"], "Anna Andersson")
        self.assertEqual(row["buyerEmail"], "anna@example.com")
        self.assertEqual(row["placedAs"], "buyer")
        self.assertEqual(row["Customer number"], "1001")
        self.assertEqual(row["Address"], "Hantverkargatan")
        self.assertEqual(row["Number"], "1A")
        self.assertEqual(row["SKU"], "DFP-001")
        self.assertEqual(row["Quantity"], "3")
        self.assertEqual(row["Total weight"], "3")
        self.assertEqual(row["Total (Pre-discount)"], "387")
        self.assertEqual(row["Product Discount"], "90")
        self.assertEqual(row["Total"], "297")
        self.assertEqual(row["Currency"], "SEK")

    def test_new_order_columns_are_immediately_after_customer(self):
        customer_index = ORDER_COLUMNS.index("Customer")

        self.assertEqual(
            ORDER_COLUMNS[customer_index + 1 : customer_index + 5],
            ["placedBy", "buyerEmail", "placedAs", "Customer Reference"],
        )

    def test_flatten_order_uses_delivered_quantity_for_financial_totals(self):
        rows = flatten_order(
            {
                "reference": "PARTIAL",
                "createdAtDateTime": "2026-02-12T15:22:42Z",
                "deliveryDate": "2026-03-30",
                "buyerName": "Store A",
                "currency": "SEK",
                "orderRows": [
                    {
                        "productSku": "10005",
                        "productName": "Blabar",
                        "ordered": 30,
                        "delivered": 15,
                        "unit": "Dfp",
                        "price": 42000,
                        "priceDiscounted": 39600,
                        "discountAmount": 2400,
                        "note": "07626K",
                    }
                ],
            }
        )

        self.assertEqual(rows[0]["Quantity"], "30")
        self.assertEqual(rows[0]["Total weight"], "15")
        self.assertEqual(rows[0]["Total (Pre-discount)"], "6300")
        self.assertEqual(rows[0]["Product Discount"], "360")
        self.assertEqual(rows[0]["Total"], "5940")
        self.assertEqual(rows[0]["Batch"], "07626K")

    def test_flatten_order_keeps_cancelled_rows_as_zero_value_and_skips_deposits(self):
        cancelled_rows = flatten_order(
            {
                "reference": "REF",
                "createdAtDateTime": "2026-02-12T15:22:42Z",
                "deliveryDate": "2026-03-30",
                "buyerName": "Store A",
                "currency": "SEK",
                "orderRows": [
                    {
                        "productSku": "10003",
                        "productName": "Cancelled product",
                        "cancelled": True,
                        "ordered": 15,
                        "delivered": 0,
                        "unit": "Dfp",
                        "price": 42000,
                        "priceDiscounted": 39600,
                    }
                ],
            }
        )
        self.assertEqual(cancelled_rows[0]["Quantity"], "15")
        self.assertEqual(cancelled_rows[0]["Total weight"], "0")
        self.assertEqual(cancelled_rows[0]["Total"], "0")

        deposit_rows = flatten_order(
            {
                "reference": "REF",
                "currency": "SEK",
                "orderRows": [{"ordered": 1, "deposit": True, "price": 1000}],
            }
        )
        self.assertEqual(deposit_rows, [])

    def test_split_street_address_keeps_unsplittable_address_intact(self):
        self.assertEqual(split_street_address("Box 123"), ("Box", "123"))
        self.assertEqual(split_street_address("Industrial Estate"), ("Industrial Estate", ""))

    def test_values_to_dicts_preserves_existing_extra_columns(self):
        headers, rows = values_to_dicts([ORDER_COLUMNS + ["extra"], ["REF1"] + [""] * (len(ORDER_COLUMNS) - 1) + ["keep"]])

        self.assertEqual(headers[-1], "extra")
        self.assertEqual(rows[0]["Reference"], "REF1")
        self.assertEqual(rows[0]["extra"], "keep")
        self.assertEqual(merge_headers(headers)[-1], "extra")

    def test_write_rows_writes_numeric_columns_as_numbers(self):
        worksheet = FakeWorksheet([])

        write_rows(
            worksheet,
            ORDER_COLUMNS,
            [
                {
                    "Reference": "REF",
                    "Quantity": "2",
                    "Total weight": "2",
                    "Total (Pre-discount)": "840",
                    "Product Discount": "48.04",
                    "Total": "791.96",
                    "Currency": "SEK",
                }
            ],
        )

        written = dict(zip(ORDER_COLUMNS, worksheet.get_all_values()[1]))
        self.assertEqual(written["Reference"], "REF")
        self.assertEqual(written["Quantity"], 2)
        self.assertEqual(written["Product Discount"], 48.04)
        self.assertEqual(written["Total"], 791.96)

    def test_apply_crm_customer_numbers_prefers_crm_number_by_customer_name(self):
        rows = [
            {"Customer": " Store A  ", "Customer number": "stockfiller-external"},
            {"Customer": "Unknown Store", "Customer number": "stockfiller-unknown"},
        ]

        apply_crm_customer_numbers(rows, {"store a": "CRM-1001"})

        self.assertEqual(rows[0]["Customer number"], "CRM-1001")
        self.assertEqual(rows[1]["Customer number"], "stockfiller-unknown")

    def test_latest_order_emails_uses_last_physical_row_per_customer(self):
        emails = latest_order_emails_by_customer(
            [
                {"Customer": "Store A", "buyerEmail": "old@example.com"},
                {"Customer": "Store B", "buyerEmail": "b@example.com"},
                {"Customer": " store a ", "buyerEmail": "latest@example.com"},
            ]
        )

        self.assertEqual(emails, {"store a": "latest@example.com", "store b": "b@example.com"})

    def test_customer_email_column_is_inserted_between_email_and_city(self):
        worksheet = FakeWorksheet(
            [["customer", "email", "city_google"], ["Store A", "contact@example.com", "Goteborg"]]
        )

        headers = ensure_customer_email_column(worksheet, worksheet.get_all_values()[0])

        self.assertEqual(headers, ["customer", "email", "email_last_order", "city_google"])
        self.assertEqual(
            worksheet.get_all_values()[1],
            ["Store A", "contact@example.com", "", "Goteborg"],
        )

        second_headers = ensure_customer_email_column(worksheet, worksheet.get_all_values()[0])
        self.assertEqual(second_headers, headers)
        self.assertEqual(worksheet.get_all_values()[0].count("email_last_order"), 1)

    def test_stockfiller_client_paginates_until_short_page(self):
        session = FakeSession(
            [
                FakeResponse({"meta": {"pageSize": 2, "returned": 2}, "data": [{"reference": "A"}, {"reference": "B"}]}),
                FakeResponse({"meta": {"pageSize": 2, "returned": 1}, "data": [{"reference": "C"}]}),
            ]
        )
        config = SyncConfig(
            base_url="https://example.test/v1",
            api_token="token",
            supplier_identifier="supplierGln",
            supplier_id="7350179830001",
            sheet_key="sheet",
            google_credentials={},
        )

        orders = list(StockfillerClient(config, session=session).iter_orders({"updatedDateTimeStart": "2026-05-28T00:00:00Z"}))

        self.assertEqual([order["reference"] for order in orders], ["A", "B", "C"])
        self.assertEqual(session.calls[0]["params"]["page"], "1")
        self.assertEqual(session.calls[1]["params"]["page"], "2")
        self.assertEqual(session.calls[0]["headers"]["Authorization"], "Bearer token")

    def test_stockfiller_client_treats_404_as_no_orders(self):
        session = FakeSession([FakeResponse(None, status_code=404)])
        config = _config()

        orders = list(StockfillerClient(config, session=session).iter_orders({}))

        self.assertEqual(orders, [])

    def test_order_params_for_backfill_chunks_large_created_window(self):
        window = SyncWindow(
            mode="backfill",
            params={
                "createdDateTimeStart": "2026-01-01T00:00:00Z",
                "createdDateTimeStop": "2026-03-05T12:00:00Z",
            },
            stop_value="2026-03-05T12:00:00Z",
        )

        params = order_params_for_window(window)

        self.assertEqual(len(params), 3)
        self.assertEqual(params[0]["createdDateTimeStart"], "2026-01-01T00:00:00Z")
        self.assertEqual(params[0]["createdDateTimeStop"], "2026-01-31T23:59:59Z")
        self.assertEqual(params[1]["createdDateTimeStart"], "2026-02-01T00:00:00Z")
        self.assertEqual(params[-1]["createdDateTimeStop"], "2026-03-05T12:00:00Z")

    def test_order_params_for_incremental_keeps_original_window(self):
        window = SyncWindow(
            mode="incremental",
            params={
                "updatedDateTimeStart": "2026-05-28T08:00:00Z",
                "updatedDateTimeStop": "2026-05-28T12:00:00Z",
            },
            stop_value="2026-05-28T12:00:00Z",
        )

        self.assertEqual(order_params_for_window(window), [window.params])

    def test_dedupe_orders_by_reference_keeps_latest_updated_order(self):
        orders = dedupe_orders_by_reference(
            [
                {"reference": "REF", "updatedAtDateTime": "2026-01-01T10:00:00Z", "version": "old"},
                {"reference": "OTHER", "updatedAtDateTime": "2026-01-01T09:00:00Z", "version": "only"},
                {"reference": "REF", "updatedAtDateTime": "2026-01-01T11:00:00Z", "version": "new"},
            ]
        )

        by_reference = {order["reference"]: order for order in orders}
        self.assertEqual(len(orders), 2)
        self.assertEqual(by_reference["REF"]["version"], "new")
        self.assertEqual(by_reference["OTHER"]["version"], "only")

    def test_sync_orders_replaces_existing_reference_and_preserves_manual_columns(self):
        spreadsheet = FakeSpreadsheet(
            {
                "order_rows": [
                    ORDER_COLUMNS + ["Manual Note"],
                    _sheet_row("OLDREF", customer="Existing Store", manual_note="keep me"),
                    _sheet_row("REF-REPLACE", customer="Old Store", manual_note="replace me"),
                ],
                "customers_enriched": [
                    ["customer", "customer_number", "email", "city_google"],
                    ["Store A", "CRM-1001", "contact@example.com", "Stockholm"],
                ],
                STATE_SHEET_NAME: [
                    ["key", "value", "updated_at"],
                    ["last_successful_updated_stop", "2026-05-28T08:00:00Z", "then"],
                ],
            }
        )
        window = SyncWindow(
            mode="incremental",
            params={"updatedDateTimeStart": "2026-05-28T08:00:00Z", "updatedDateTimeStop": "2026-05-28T12:00:00Z"},
            stop_value="2026-05-28T12:00:00Z",
        )

        with patch("stockfiller_orders.sync.StockfillerClient", return_value=FakeClient([_api_order("REF-REPLACE")])):
            result = sync_orders(_config(), window, spreadsheet=spreadsheet)

        order_rows = spreadsheet.worksheet("order_rows").get_all_values()
        headers = order_rows[0]
        rows = [dict(zip(headers, row + [""] * (len(headers) - len(row)))) for row in order_rows[1:]]
        state_rows = spreadsheet.worksheet(STATE_SHEET_NAME).get_all_values()

        self.assertEqual(result.existing_rows, 2)
        self.assertEqual(result.output_rows, 1)
        self.assertEqual(result.final_rows, 2)
        self.assertEqual(headers[-1], "Manual Note")
        self.assertEqual(rows[0]["Reference"], "OLDREF")
        self.assertEqual(rows[0]["Manual Note"], "keep me")
        self.assertEqual(rows[1]["Reference"], "REF-REPLACE")
        self.assertEqual(rows[1]["Customer"], "Store A")
        self.assertEqual(rows[1]["Customer number"], "CRM-1001")
        self.assertEqual(rows[1]["placedBy"], "Anna Andersson")
        self.assertEqual(rows[1]["buyerEmail"], "latest@example.com")
        self.assertEqual(rows[1]["placedAs"], "buyer")
        self.assertEqual(rows[1]["Manual Note"], "")
        customer_values = spreadsheet.worksheet("customers_enriched").get_all_values()
        self.assertEqual(
            customer_values[0],
            ["customer", "customer_number", "email", "email_last_order", "city_google"],
        )
        self.assertEqual(customer_values[1][3], "latest@example.com")
        self.assertIn(["last_successful_updated_stop", "2026-05-28T12:00:00Z", state_rows[1][2]], state_rows)

    def test_sync_orders_dry_run_does_not_write_rows_or_state(self):
        spreadsheet = FakeSpreadsheet(
            {
                "order_rows": [
                    ORDER_COLUMNS,
                    _sheet_row("OLDREF", customer="Existing Store"),
                ],
                "customers_enriched": [["customer", "customer_number"], ["Store A", "CRM-1001"]],
                STATE_SHEET_NAME: [["key", "value", "updated_at"], ["last_successful_updated_stop", "old", "then"]],
            }
        )
        window = SyncWindow(mode="incremental", params={}, stop_value="2026-05-28T12:00:00Z")

        with patch("stockfiller_orders.sync.StockfillerClient", return_value=FakeClient([_api_order("NEWREF")])):
            result = sync_orders(_config(), window, dry_run=True, spreadsheet=spreadsheet)

        self.assertTrue(result.dry_run)
        self.assertEqual(spreadsheet.worksheet("order_rows").get_all_values()[1][0], "OLDREF")
        self.assertEqual(spreadsheet.worksheet(STATE_SHEET_NAME).get_all_values()[1][1], "old")

    def test_sync_orders_preview_target_does_not_update_state(self):
        spreadsheet = FakeSpreadsheet(
            {
                "order_rows_preview": [ORDER_COLUMNS],
                "customers_enriched": [["customer", "customer_number"], ["Store A", "CRM-1001"]],
                STATE_SHEET_NAME: [["key", "value", "updated_at"], ["last_successful_updated_stop", "old", "then"]],
            }
        )
        window = SyncWindow(mode="backfill", params={}, stop_value="2026-05-28T12:00:00Z")

        with patch("stockfiller_orders.sync.StockfillerClient", return_value=FakeClient([_api_order("NEWREF")])):
            result = sync_orders(
                _config(),
                window,
                spreadsheet=spreadsheet,
                target_worksheet="order_rows_preview",
                update_state=False,
            )

        self.assertFalse(result.dry_run)
        self.assertEqual(result.target_worksheet, "order_rows_preview")
        self.assertEqual(spreadsheet.worksheet("order_rows_preview").get_all_values()[1][0], "NEWREF")
        self.assertEqual(spreadsheet.worksheet(STATE_SHEET_NAME).get_all_values()[1][1], "old")

    def test_build_incremental_window_uses_state_with_overlap(self):
        args = Namespace(
            mode="incremental",
            start=None,
            stop="2026-05-28T12:00:00Z",
            lookback_hours=48,
            overlap_hours=2,
        )

        window = build_sync_window(args, {"last_successful_updated_stop": "2026-05-28T08:00:00Z"})

        self.assertEqual(window.params["updatedDateTimeStart"], "2026-05-28T06:00:00Z")
        self.assertEqual(window.params["updatedDateTimeStop"], "2026-05-28T12:00:00Z")

    def test_build_incremental_window_without_state_uses_lookback(self):
        args = Namespace(
            mode="incremental",
            start=None,
            stop=None,
            lookback_hours=48,
            overlap_hours=2,
        )

        window = build_sync_window(args, {}, now=datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc))

        self.assertEqual(window.params["updatedDateTimeStart"], "2026-05-26T12:00:00Z")
        self.assertEqual(window.params["updatedDateTimeStop"], "2026-05-28T12:00:00Z")


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, headers, params, timeout):
        self.calls.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return self.responses.pop(0)


class FakeClient:
    def __init__(self, orders):
        self.orders = list(orders)

    def iter_orders(self, params):
        return iter(self.orders)


class FakeWorksheet:
    def __init__(self, values):
        self.values = values

    def get_all_values(self, **kwargs):
        return self.values

    def clear(self):
        self.values = []

    def update(self, values, range_name="A1", raw=True):
        if range_name == "A1":
            self.values = values
            return

        match = re.fullmatch(r"([A-Z]+)(\d+):([A-Z]+)(\d+)", range_name)
        if not match or match.group(1) != match.group(3):
            raise ValueError(f"Unsupported fake range: {range_name}")
        column_index = _column_index(match.group(1))
        start_row = int(match.group(2)) - 1
        for offset, value_row in enumerate(values):
            row_index = start_row + offset
            while len(self.values) <= row_index:
                self.values.append([])
            while len(self.values[row_index]) <= column_index:
                self.values[row_index].append("")
            self.values[row_index][column_index] = value_row[0] if value_row else ""

    def insert_cols(self, columns, col=1):
        insert_index = col - 1
        width = len(columns)
        for row_index, row in enumerate(self.values):
            inserted = [columns[offset][row_index] if row_index < len(columns[offset]) else "" for offset in range(width)]
            row[insert_index:insert_index] = inserted


class FakeSpreadsheet:
    def __init__(self, worksheets):
        self.worksheets = {name: FakeWorksheet(values) for name, values in worksheets.items()}

    def worksheet(self, name):
        return self.worksheets[name]


def _config():
    return SyncConfig(
        base_url="https://example.test/v1",
        api_token="token",
        supplier_identifier="supplierGln",
        supplier_id="7350179830001",
        sheet_key="sheet",
        google_credentials={},
    )


def _sheet_row(reference, customer="", manual_note=""):
    values = {column: "" for column in ORDER_COLUMNS}
    values.update(
        {
            "Reference": reference,
            "Order date": "2026-05-01",
            "Delivery date": "2026-05-02",
            "Customer": customer,
            "Quantity": "1",
            "Total": "100",
            "Currency": "SEK",
        }
    )
    return [values[column] for column in ORDER_COLUMNS] + ([manual_note] if manual_note else [""])


def _api_order(reference):
    return {
        "reference": reference,
        "createdAtDateTime": "2026-05-28T08:15:00Z",
        "deliveryDate": "2026-06-01",
        "buyerName": "Store A",
        "placedBy": "Anna Andersson",
        "buyerEmail": "latest@example.com",
        "placedAs": "buyer",
        "buyerExternalId": "stockfiller-customer-id",
        "deliveryAddress": "Hantverkargatan 1",
        "deliveryZipCode": "11152",
        "deliveryCity": "Stockholm",
        "deliveryCountryCode": "SE",
        "currency": "SEK",
        "orderRows": [
            {
                "productSku": "DFP-001",
                "productName": "Dubai Fresh Pack",
                "ordered": 2,
                "unit": "DFP",
                "price": 5000,
            }
        ],
    }


def _column_index(name):
    number = 0
    for character in name:
        number = number * 26 + ord(character) - ord("A") + 1
    return number - 1
