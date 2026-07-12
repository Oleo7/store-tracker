from datetime import date
from unittest import TestCase

from storepoint_sync.sync import (
    DateWindow,
    StorepointConfig,
    build_storepoint_rows,
    column_name,
    contiguous_index_groups,
    join_address,
    parse_delivery_date,
    subtract_months,
    sync_storepoint_customers,
)


class StorepointSyncTests(TestCase):
    def test_build_storepoint_rows_filters_dedupes_and_formats_address(self):
        rows = [
            {
                "Delivery date": "2026-03-10",
                "Customer": "Too Old",
                "Address": "Old Street",
                "Number": "1",
                "Postal code": "11111",
                "City": "Stockholm",
            },
            {
                "Delivery date": "2026-03-11",
                "Customer": "Store A",
                "Address": "Main Street",
                "Number": "4B",
                "Postal code": "12345",
                "City": "Goteborg",
            },
            {
                "Delivery date": "2026-06-11",
                "Customer": " Store A ",
                "Address": "Main  Street",
                "Number": "4B",
                "Postal code": "12345",
                "City": "Goteborg",
            },
            {
                "Delivery date": "2026-06-12",
                "Customer": "Future Store",
                "Address": "Next Street",
                "Number": "2",
                "Postal code": "22222",
                "City": "Malmo",
            },
            {
                "Delivery date": "2026-05-01",
                "Customer": "",
                "Address": "No Name Street",
                "Number": "1",
                "Postal code": "33333",
                "City": "Uppsala",
            },
        ]

        output_rows, filtered_rows = build_storepoint_rows(rows, DateWindow(date(2026, 3, 11), date(2026, 6, 11)))

        self.assertEqual(filtered_rows, 2)
        self.assertEqual(len(output_rows), 1)
        self.assertEqual(output_rows[0].as_list(), ["Store A", "Main Street 4B", "Goteborg", "12345"])

    def test_parse_delivery_date_accepts_sheet_serial_and_common_text_dates(self):
        self.assertEqual(parse_delivery_date(46100), date(2026, 3, 19))
        self.assertEqual(parse_delivery_date("2026-06-11"), date(2026, 6, 11))
        self.assertEqual(parse_delivery_date("11/06/2026"), date(2026, 6, 11))
        self.assertIsNone(parse_delivery_date("not a date"))

    def test_subtract_months_clamps_month_end(self):
        self.assertEqual(subtract_months(date(2026, 5, 31), 3), date(2026, 2, 28))
        self.assertEqual(subtract_months(date(2026, 6, 11), 3), date(2026, 3, 11))

    def test_join_address_skips_empty_parts(self):
        self.assertEqual(join_address("Main Street", "4"), "Main Street 4")
        self.assertEqual(join_address("Main Street", ""), "Main Street")
        self.assertEqual(join_address("", "4"), "4")

    def test_column_helpers_build_expected_ranges(self):
        self.assertEqual(column_name(0), "A")
        self.assertEqual(column_name(27), "AB")
        self.assertEqual(contiguous_index_groups([5, 0, 3, 2]), [(0, 0), (2, 3), (5, 5)])

    def test_sync_storepoint_customers_writes_only_target_columns_and_verifies(self):
        source_values = [
            [
                "Delivery date", "Customer", "placedBy", "buyerEmail", "placedAs",
                "Customer Reference", "Address", "Number", "Postal code", "City", "Other",
            ],
            ["2026-06-01", "Store A", "Anna", "anna@example.com", "buyer", "A1", "Main Street", "1", "11111", "Stockholm", "keep"],
            ["2026-06-01", "Store B", "Bo", "bo@example.com", "buyer", "B1", "Side Street", "2", "22222", "Goteborg", "keep"],
            ["2026-01-01", "Old Store", "Olle", "olle@example.com", "buyer", "O1", "Old Street", "3", "33333", "Malmo", "keep"],
        ]
        target_values = [
            ["name", "description", "address", "city", "state", "postcode"],
            ["Old Name", "preserve", "Old Address", "Old City", "state", "99999"],
            ["Stale", "preserve stale", "Stale Address", "Stale City", "state", "88888"],
            ["Very Stale", "preserve very stale", "Very Stale Address", "Very Stale City", "state", "77777"],
        ]
        client = FakeClient(
            {
                "source": FakeSpreadsheet({"order_rows": FakeWorksheet(source_values)}),
                "target": FakeSpreadsheet({"storepoint_template_49b0fd29731a": FakeWorksheet(target_values)}),
            }
        )
        config = StorepointConfig(source_sheet_key="source", target_sheet_key="target", google_credentials={})

        result = sync_storepoint_customers(config, today=date(2026, 6, 11), client=client)

        target_sheet = client.spreadsheets["target"].worksheet("storepoint_template_49b0fd29731a")
        self.assertFalse(result.dry_run)
        self.assertEqual(result.filtered_rows, 2)
        self.assertEqual(result.unique_rows, 2)
        self.assertEqual(result.stale_rows, 1)
        self.assertEqual(
            target_sheet.values,
            [
                ["name", "description", "address", "city", "state", "postcode"],
                ["Store A", "preserve", "Main Street 1", "Stockholm", "state", "11111"],
                ["Store B", "preserve stale", "Side Street 2", "Goteborg", "state", "22222"],
                ["", "preserve very stale", "", "", "state", ""],
            ],
        )

    def test_sync_storepoint_customers_dry_run_does_not_write(self):
        source_values = [
            ["Delivery date", "Customer", "Address", "Number", "Postal code", "City"],
            ["2026-06-01", "Store A", "Main Street", "1", "11111", "Stockholm"],
        ]
        target_values = [["name", "description", "address", "city", "state", "postcode"]]
        client = FakeClient(
            {
                "source": FakeSpreadsheet({"order_rows": FakeWorksheet(source_values)}),
                "target": FakeSpreadsheet({"storepoint_template_49b0fd29731a": FakeWorksheet(target_values)}),
            }
        )
        config = StorepointConfig(source_sheet_key="source", target_sheet_key="target", google_credentials={})

        result = sync_storepoint_customers(config, dry_run=True, today=date(2026, 6, 11), client=client)

        self.assertTrue(result.dry_run)
        self.assertEqual(client.spreadsheets["target"].worksheet("storepoint_template_49b0fd29731a").values, target_values)


class FakeClient:
    def __init__(self, spreadsheets):
        self.spreadsheets = spreadsheets

    def open_by_key(self, key):
        return self.spreadsheets[key]


class FakeSpreadsheet:
    def __init__(self, worksheets):
        self.worksheets = worksheets

    def worksheet(self, name):
        return self.worksheets[name]


class FakeWorksheet:
    def __init__(self, values):
        self.values = [row[:] for row in values]
        self.row_count = len(values)

    def get_all_values(self, **kwargs):
        return [row[:] for row in self.values]

    def add_rows(self, rows):
        for _ in range(rows):
            self.values.append([])
        self.row_count += rows

    def batch_clear(self, ranges):
        for range_name in ranges:
            start_col, end_col = _parse_open_range(range_name)
            for row_index in range(1, len(self.values)):
                _ensure_width(self.values[row_index], end_col + 1)
                for col_index in range(start_col, end_col + 1):
                    self.values[row_index][col_index] = ""

    def update(self, values, range_name="A1", raw=True):
        start_col, start_row = _parse_bounded_range_start(range_name)
        while len(self.values) < start_row - 1 + len(values):
            self.values.append([])
        for row_offset, row_values in enumerate(values):
            row = self.values[start_row - 1 + row_offset]
            _ensure_width(row, start_col + len(row_values))
            for col_offset, value in enumerate(row_values):
                row[start_col + col_offset] = value


def _parse_open_range(range_name):
    left, right = range_name.split(":")
    start_col = _col_index(left.rstrip("0123456789"))
    end_col = _col_index(right.rstrip("0123456789"))
    return start_col, end_col


def _parse_bounded_range_start(range_name):
    left = range_name.split(":")[0]
    col = left.rstrip("0123456789")
    row = int(left[len(col) :])
    return _col_index(col), row


def _col_index(col):
    number = 0
    for char in col:
        number = number * 26 + ord(char) - 64
    return number - 1


def _ensure_width(row, width):
    while len(row) < width:
        row.append("")
