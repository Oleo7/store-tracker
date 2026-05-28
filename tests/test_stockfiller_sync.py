from argparse import Namespace
from datetime import datetime, timezone
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
    flatten_order,
    merge_headers,
    split_street_address,
    sync_orders,
    values_to_dicts,
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
        self.assertEqual(row["Customer number"], "1001")
        self.assertEqual(row["Address"], "Hantverkargatan")
        self.assertEqual(row["Number"], "1A")
        self.assertEqual(row["SKU"], "DFP-001")
        self.assertEqual(row["Quantity"], "3")
        self.assertEqual(row["Total (Pre-discount)"], "387")
        self.assertEqual(row["Product Discount"], "23.26%")
        self.assertEqual(row["Total"], "297")
        self.assertEqual(row["Currency"], "SEK")

    def test_flatten_order_skips_cancelled_order_and_deposit_rows(self):
        cancelled_order_rows = flatten_order({"reference": "REF", "cancelled": True, "orderRows": [{"ordered": 1}]})
        self.assertEqual(cancelled_order_rows, [])

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

    def test_apply_crm_customer_numbers_prefers_crm_number_by_customer_name(self):
        rows = [
            {"Customer": " Store A  ", "Customer number": "stockfiller-external"},
            {"Customer": "Unknown Store", "Customer number": "stockfiller-unknown"},
        ]

        apply_crm_customer_numbers(rows, {"store a": "CRM-1001"})

        self.assertEqual(rows[0]["Customer number"], "CRM-1001")
        self.assertEqual(rows[1]["Customer number"], "stockfiller-unknown")

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

    def test_sync_orders_replaces_existing_reference_and_preserves_manual_columns(self):
        spreadsheet = FakeSpreadsheet(
            {
                "order_rows": [
                    ORDER_COLUMNS + ["Manual Note"],
                    _sheet_row("OLDREF", customer="Existing Store", manual_note="keep me"),
                    _sheet_row("REF-REPLACE", customer="Old Store", manual_note="replace me"),
                ],
                "customers_enriched": [
                    ["customer", "customer_number"],
                    ["Store A", "CRM-1001"],
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
        self.assertEqual(rows[1]["Manual Note"], "")
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

    def get_all_values(self):
        return self.values

    def clear(self):
        self.values = []

    def update(self, values, range_name="A1"):
        self.values = values


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
