"""Regression coverage for appending schema headers at the Sheets grid boundary."""
from copy import deepcopy
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gspread.exceptions import APIError
from gspread.utils import a1_to_rowcol, rowcol_to_a1

import app as app_module
from planning_suggestions import (
    SCORE_EVENT_COLUMNS, SCORE_EVENTS_SHEET,
    SUGGESTION_COLUMNS, SUGGESTIONS_SHEET, _ensure_columns,
)
from test_planning import FakeWorksheet, PlanningApiTestCase


SCHEMAS = (
    (SUGGESTIONS_SHEET, SUGGESTION_COLUMNS, 36, "history_index_at_creation"),
    (SCORE_EVENTS_SHEET, SCORE_EVENT_COLUMNS, 26, "history_index"),
)


class GridBoundedWorksheet(FakeWorksheet):
    """Unlike the shared fake, reject insertion/writes outside the current grid."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resize_calls = []
        self.header_writes = []

    def insert_cols(self, columns, col=1):
        if col - 1 >= self.col_count:
            response = Mock()
            response.json.return_value = {"error": {
                "code": 400,
                "message": "Invalid requests[0].insertDimension: range.startIndex "
                           "must be less than the grid size if inheritFromBefore is false.",
            }}
            raise APIError(response)
        raise AssertionError("Schema migration must not shift existing columns")

    def resize(self, rows=None, cols=None):
        self.resize_calls.append((rows, cols))
        super().resize(rows=rows, cols=cols)

    def batch_update(self, data, value_input_option=None):
        for item in data:
            _, end_col = a1_to_rowcol(item["range"].split(":")[-1])
            if end_col > self.col_count:
                raise AssertionError("Header write exceeds worksheet column capacity")
        self.header_writes.append((deepcopy(data), value_input_option))
        super().batch_update(data, value_input_option=value_input_option)


class PlanningSchemaMigrationTests(TestCase):
    def test_full_grid_appends_history_header_and_preserves_data_idempotently(self):
        for title, columns, old_count, added in SCHEMAS:
            with self.subTest(sheet=title):
                headers = columns[:-1]
                self.assertEqual(len(headers), old_count)
                self.assertEqual(columns[-1], added)
                rows = [[f"existing-{index}" for index in range(old_count)],
                        ["=SUM(A2:B2)"] + [""] * (old_count - 1)]
                sheet = GridBoundedWorksheet(title, headers, rows)
                self.assertEqual(sheet.col_count, old_count)
                before = sheet.get_all_values()
                row_count = sheet.row_count
                cache = {title: sheet.get_all_values()}
                def reader(target):
                    return cache.setdefault(target.title, target.get_all_values())
                invalidator = Mock(side_effect=lambda target: cache.pop(target.title, None))

                self.assertEqual(_ensure_columns(sheet, columns, reader, invalidator), columns)
                self.assertEqual(sheet.col_count, old_count + 1)
                self.assertEqual(sheet.row_count, row_count)
                self.assertEqual(sheet.row_values(1), columns)
                self.assertEqual(sheet.values[0][:-1], before[0])
                self.assertEqual(sheet.values[1:], before[1:])
                self.assertEqual(sheet.resize_calls, [(None, old_count + 1)])
                cell = rowcol_to_a1(1, old_count + 1)
                self.assertEqual(sheet.header_writes, [([{
                    "range": f"{cell}:{cell}", "values": [[added]],
                }], "RAW")])
                invalidator.assert_called_once_with(sheet)

                migrated = sheet.get_all_values()
                self.assertEqual(_ensure_columns(sheet, columns, reader, invalidator), columns)
                self.assertEqual(sheet.get_all_values(), migrated)
                self.assertEqual(sheet.batch_update_count, 1)
                self.assertEqual(len(sheet.resize_calls), 1)
                invalidator.assert_called_once_with(sheet)

    def test_spare_capacity_and_multiple_missing_columns_preserve_unheaded_data(self):
        for title, columns, _, _ in SCHEMAS:
            with self.subTest(sheet=title):
                headers = columns[:-2] + ["custom_column"]
                data = [f"value-{i}" for i in range(len(columns) + 5)]
                sheet = GridBoundedWorksheet(title, headers, [data])
                sheet.col_count = len(data)
                expected = headers + columns[-2:]
                self.assertEqual(_ensure_columns(sheet, columns), expected)
                self.assertEqual(sheet.row_values(1), expected)
                self.assertEqual(sheet.row_values(2), data)
                self.assertEqual(sheet.resize_calls, [])
                self.assertEqual(sheet.col_count, len(data))
                self.assertEqual(_ensure_columns(sheet, columns), expected)
                self.assertEqual(sheet.batch_update_count, 1)

    def test_retry_after_resize_and_failed_header_write_does_not_duplicate_columns(self):
        for title, columns, old_count, _ in SCHEMAS:
            with self.subTest(sheet=title):
                sheet = GridBoundedWorksheet(title, columns[:-1], [["preserved"]])
                before = sheet.get_all_values()
                sheet.fail_next_batch_update = RuntimeError("transient header write failure")
                with self.assertRaisesRegex(RuntimeError, "transient"):
                    _ensure_columns(sheet, columns)
                self.assertEqual(sheet.get_all_values(), before)
                self.assertEqual(_ensure_columns(sheet, columns), columns)
                self.assertEqual(sheet.resize_calls, [(None, old_count + 1)])
                self.assertEqual(sheet.row_values(1), columns)
                self.assertEqual(sheet.row_values(2), ["preserved"])


class PlanningSchemaApiTests(PlanningApiTestCase):
    def test_suggestions_endpoint_migrates_both_full_legacy_grids(self):
        for title, columns, _, _ in SCHEMAS:
            self.spreadsheet.sheets[title] = GridBoundedWorksheet(title, columns[:-1])
        app_module._sheet_read_cache.clear()
        self.addCleanup(app_module._sheet_read_cache.clear)
        with patch.dict(app_module.app.config, {"PLANNING_SUGGESTIONS_STUB": True}):
            for _ in range(2):
                response = self.client.get("/planning/suggestions")
                self.assertEqual(response.status_code, 200, response.get_json())
                self.assertIsNotNone(response.get_json()["suggestion"])
        for title, columns, _, _ in SCHEMAS:
            sheet = self.spreadsheet.worksheet(title)
            self.assertEqual(sheet.row_values(1), columns)
            self.assertEqual(sheet.col_count, len(columns))
            self.assertEqual(sheet.resize_calls, [(None, len(columns))])
