from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import sys
import threading
from unittest import TestCase, main
from unittest.mock import patch

from flask import g


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from test_planning import FakeWorksheet, default_spreadsheet


class RejectingLock:
    def acquire(self):
        raise AssertionError("read path acquired planning write lock")

    def release(self):
        raise AssertionError("read path released planning write lock")


class TrackingRLock:
    def __init__(self):
        self.lock = threading.RLock()
        self.acquire_count = 0
        self.release_count = 0

    def acquire(self):
        acquired = self.lock.acquire()
        self.acquire_count += 1
        return acquired

    def release(self):
        self.release_count += 1
        return self.lock.release()


class StaticSuggestionService:
    def queue(self, *args, **kwargs):
        return None, [], 0


class PlanningReadPathTests(TestCase):
    def setUp(self):
        app_module.app.config.update(
            TESTING=True,
            SECRET_KEY="planning-read-path-test",
        )
        app_module._sheet_read_cache.clear()
        self.spreadsheet = default_spreadsheet()
        self.client = app_module.app.test_client()
        spreadsheet_patcher = patch.object(
            app_module,
            "get_spreadsheet_with_retry",
            return_value=self.spreadsheet,
        )
        user_patcher = patch.object(
            app_module,
            "current_user",
            return_value={"user_name": "olle", "role": "Säljare"},
        )
        spreadsheet_patcher.start()
        user_patcher.start()
        self.addCleanup(spreadsheet_patcher.stop)
        self.addCleanup(user_patcher.stop)
        self.addCleanup(app_module._sheet_read_cache.clear)

    @staticmethod
    def activity_url():
        return "/planning/activities?start=2026-07-27&end=2026-08-02"

    @staticmethod
    def create_payload(request_id="planning-read-path-create"):
        return {
            "client_request_id": request_id,
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "contact_type": "Besök",
            "scheduled_at": "2026-08-25T09:30:00+02:00",
            "note": "Planerat butiksbesök",
        }

    def test_planning_activities_get_does_not_acquire_write_lock_or_mutate_schema(self):
        planning_sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        contact_sheet = self.spreadsheet.worksheet("sales_activities")
        with patch.object(app_module, "_planning_write_lock", RejectingLock()):
            response = self.client.get(self.activity_url())

        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(planning_sheet.update_cell_count, 0)
        self.assertEqual(planning_sheet.batch_update_count, 0)
        self.assertEqual(contact_sheet.update_cell_count, 0)
        self.assertEqual(contact_sheet.batch_update_count, 0)

    def test_planning_suggestions_activity_read_does_not_acquire_write_lock(self):
        with patch.object(
            app_module, "_planning_write_lock", RejectingLock()
        ), patch.object(
            app_module,
            "planning_suggestion_candidates",
            return_value=[],
        ), patch.object(
            app_module,
            "planning_suggestion_service",
            return_value=StaticSuggestionService(),
        ):
            response = self.client.get("/planning/suggestions")

        self.assertEqual(response.status_code, 200, response.get_json())

    def test_read_snapshot_never_ensures_or_mutates_and_defaults_optional_columns(self):
        sheet = FakeWorksheet(
            app_module.PLANNED_ACTIVITIES_SHEET,
            ["planned_activity_id", "user_name", "scheduled_at"],
            [["activity-1", "olle", "2026-08-25T09:30:00+02:00"]],
        )
        self.spreadsheet.sheets[app_module.PLANNED_ACTIVITIES_SHEET] = sheet
        with patch.object(
            app_module,
            "ensure_planned_activities_worksheet",
            side_effect=AssertionError("read snapshot ensured schema"),
        ), patch.object(
            app_module,
            "get_or_create_worksheet",
            side_effect=AssertionError("read snapshot created schema"),
        ):
            returned_sheet, headers, rows = (
                app_module.read_planned_activity_snapshot(self.spreadsheet)
            )

        self.assertIs(returned_sheet, sheet)
        self.assertEqual(
            headers,
            ["planned_activity_id", "user_name", "scheduled_at"],
        )
        self.assertEqual(rows[0][0], 2)
        self.assertEqual(rows[0][1]["planned_activity_id"], "activity-1")
        self.assertEqual(rows[0][1]["customer_id"], "")
        self.assertEqual(rows[0][1]["revision"], "")
        self.assertEqual(sheet.update_cell_count, 0)
        self.assertEqual(sheet.batch_update_count, 0)

    def test_missing_planning_sheet_is_empty_on_get_without_creation(self):
        self.spreadsheet.sheets.pop(app_module.PLANNED_ACTIVITIES_SHEET)

        response = self.client.get(self.activity_url())
        with patch.object(
            app_module,
            "planning_suggestion_candidates",
            return_value=[],
        ), patch.object(
            app_module,
            "planning_suggestion_service",
            return_value=StaticSuggestionService(),
        ):
            suggestion_response = self.client.get("/planning/suggestions")

        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()["activities"], [])
        self.assertEqual(
            suggestion_response.status_code,
            200,
            suggestion_response.get_json(),
        )
        self.assertNotIn(
            app_module.PLANNED_ACTIVITIES_SHEET,
            self.spreadsheet.added_sheets,
        )
        self.assertNotIn(
            app_module.PLANNED_ACTIVITIES_SHEET,
            self.spreadsheet.sheets,
        )

    def test_missing_planning_sheet_is_ensured_by_write_under_lock(self):
        self.spreadsheet.sheets.pop(app_module.PLANNED_ACTIVITIES_SHEET)
        tracking_lock = TrackingRLock()
        with patch.object(app_module, "_planning_write_lock", tracking_lock):
            response = self.client.post(
                "/planning/activities", json=self.create_payload()
            )

        self.assertEqual(response.status_code, 201, response.get_json())
        self.assertIn(
            app_module.PLANNED_ACTIVITIES_SHEET,
            self.spreadsheet.added_sheets,
        )
        sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        self.assertEqual(sheet.row_values(1), app_module.PLANNED_ACTIVITY_COLUMNS)
        self.assertGreater(tracking_lock.acquire_count, 0)
        self.assertEqual(
            tracking_lock.acquire_count, tracking_lock.release_count
        )

    def test_missing_optional_contact_columns_remain_blank_without_schema_write(self):
        contact_sheet = FakeWorksheet(
            "sales_activities", app_module.CONTACT_REQUIRED_COLUMNS
        )
        self.spreadsheet.sheets["sales_activities"] = contact_sheet

        response = self.client.get(self.activity_url())

        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertNotIn("contact_id", contact_sheet.row_values(1))
        self.assertEqual(contact_sheet.update_cell_count, 0)
        self.assertEqual(contact_sheet.batch_update_count, 0)

    def test_missing_required_contact_column_fails_safely_without_schema_write(self):
        headers = [
            column
            for column in app_module.CONTACT_REQUIRED_COLUMNS
            if column != "result"
        ]
        contact_sheet = FakeWorksheet("sales_activities", headers)
        self.spreadsheet.sheets["sales_activities"] = contact_sheet

        response = self.client.get(self.activity_url())

        self.assertEqual(response.status_code, 503, response.get_json())
        self.assertEqual(response.get_json()["code"], "planning_store_unavailable")
        self.assertNotIn("result", contact_sheet.row_values(1))
        self.assertEqual(contact_sheet.update_cell_count, 0)

    def test_duplicate_legacy_freezer_columns_merge_in_memory_without_mutation(self):
        headers = list(app_module.CONTACT_COLUMNS) + ["polarbar"]
        row = ["" for _column in headers]
        row[headers.index("date_time")] = "2026-07-28 11:00"
        row[headers.index("sales_person")] = "Olle"
        row[headers.index("customer")] = "Butik A"
        row[headers.index("contact_channel")] = "Telefon"
        row[headers.index("result")] = "Positiv"
        row[-1] = "true"
        sheet = FakeWorksheet("sales_activities", headers, [row])

        returned_headers, rows = app_module.worksheet_snapshot(
            sheet,
            expected_columns=app_module.CONTACT_COLUMNS,
            required_columns=app_module.CONTACT_REQUIRED_COLUMNS,
        )

        self.assertEqual(returned_headers, headers)
        self.assertEqual(rows[0][0], 2)
        self.assertEqual(rows[0][1]["polarbar"], "1")
        self.assertEqual(sheet.row_values(1).count("polarbar"), 2)
        self.assertEqual(sheet.update_cell_count, 0)
        self.assertEqual(sheet.batch_update_count, 0)

    def test_sheet_read_count_counts_one_miss_and_zero_for_following_hit(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        cache = app_module.SheetReadCache(ttl_seconds=60)
        sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        sheet._store_tracker_spreadsheet = self.spreadsheet
        with patch.object(app_module, "_sheet_read_cache", cache), \
                app_module.app.test_request_context(self.activity_url()):
            g.performance_request_id = "read-count-test"
            g.performance_steps = []
            g.google_sheets_read_count = 0

            app_module.worksheet_snapshot(sheet)
            count_after_miss = g.google_sheets_read_count
            app_module.worksheet_snapshot(sheet)
            count_after_hit = g.google_sheets_read_count

        self.assertEqual(count_after_miss, 1)
        self.assertEqual(count_after_hit, 1)

    def test_write_lock_telemetry_is_outermost_only_and_contains_no_pii(self):
        with patch.dict(
            os.environ,
            {"PERFORMANCE_LOGGING_ENABLED": "true"},
            clear=False,
        ), self.assertLogs(
            app_module.PERFORMANCE_LOGGER_NAME,
            level=logging.INFO,
        ) as captured:
            response = self.client.post(
                "/planning/activities",
                json=self.create_payload("lock-telemetry-create"),
            )

        self.assertEqual(response.status_code, 201, response.get_json())
        records = [json.loads(record.getMessage()) for record in captured.records]
        lock_steps = [
            record for record in records
            if record["step"].startswith("lock.")
        ]
        self.assertEqual(
            [record["step"] for record in lock_steps],
            ["lock.wait.planning_write", "lock.hold.planning_write"],
        )
        for record in lock_steps:
            self.assertEqual(record["event"], "performance")
            self.assertEqual(record["endpoint"], "/planning/activities")
            self.assertIn("request_id", record)
            self.assertIn("total_ms", record)
            self.assertIn("duration_ms", record)
            self.assertIn("row_count", record)
            self.assertIn("google_sheets_read_count", record)
        output = " ".join(captured.output)
        self.assertNotIn("Butik A", output)
        self.assertNotIn("olle", output.casefold())
        self.assertNotIn(
            "11111111-1111-4111-8111-111111111111", output
        )


if __name__ == "__main__":
    main()
