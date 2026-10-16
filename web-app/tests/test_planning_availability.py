"""Fresh, seller-scoped reads for automatic activity time suggestions."""

from pathlib import Path
import sys
from unittest import main
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app as app_module
from test_planning import FakeWorksheet, PlanningApiTestCase
from test_planning_read_path import RejectingLock


class PlanningAvailabilityTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module._sheet_read_cache.clear()
        self.addCleanup(app_module._sheet_read_cache.clear)

    @staticmethod
    def availability_url(owner="", start="2026-07-27", end="2026-07-28"):
        query = f"/planning/activities?availability=1&start={start}&end={end}"
        return query + (f"&user_name={owner}" if owner else "")

    def test_availability_reads_current_values_without_changing_regular_cache(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        self.append_planning_row(scheduled_at="2026-07-28T08:00:00+02:00")
        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        with patch.object(sheet, "get_all_values", wraps=sheet.get_all_values) as read:
            _sheet, _headers, cached = app_module.read_planned_activity_snapshot(
                self.spreadsheet
            )
            self.assertEqual(len(cached), 1)
            self.append_planning_row(scheduled_at="2026-07-28T08:30:00+02:00")

            response = self.client.get(self.availability_url())

            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertEqual(
                [item["scheduled_at"] for item in response.get_json()["activities"]],
                ["2026-07-28T08:00+02:00", "2026-07-28T08:30+02:00"],
            )
            self.assertEqual(response.headers["Cache-Control"], "no-store")
            self.assertEqual(read.call_count, 2)
            _sheet, _headers, still_cached = app_module.read_planned_activity_snapshot(
                self.spreadsheet
            )
            self.assertEqual(len(still_cached), 1)
            self.assertEqual(read.call_count, 2)

    def test_booked_login_controls_availability_after_customer_transfer(self):
        self.append_planning_row(
            user_name=" OLLE ", sales_person="Old display name",
            scheduled_at="2026-07-28T08:00:00+02:00",
        )
        self.append_planning_row(
            owner={"user_name": "sofia", "name": "Olle"},
            customer_row=4,
            scheduled_at="2026-07-28T09:00:00+02:00",
        )
        customers = self.spreadsheet.worksheet("customers_enriched")
        customers.update_cell(2, customers.values[0].index("sales_person") + 1, "Sofia")

        own_response = self.client.get(self.availability_url())
        self.assertEqual(own_response.status_code, 200, own_response.get_json())
        own = own_response.get_json()
        self.assertEqual(own["owner"]["user_name"], "olle")
        self.assertEqual(len(own["activities"]), 1)
        self.assertEqual(own["activities"][0]["scheduled_at"], "2026-07-28T08:00+02:00")
        self.assertEqual(set(own["activities"][0]), {
            "user_name", "scheduled_at", "contact_type", "status", "duration_minutes",
        })

        self.login("admin")
        other_response = self.client.get(self.availability_url("sofia"))
        self.assertEqual(other_response.status_code, 200, other_response.get_json())
        other = other_response.get_json()
        self.assertEqual(other["owner"]["user_name"], "sofia")
        self.assertEqual(len(other["activities"]), 1)
        self.assertEqual(other["activities"][0]["scheduled_at"], "2026-07-28T09:00+02:00")

    def test_seller_cannot_request_another_sellers_availability(self):
        response = self.client.get(self.availability_url("sofia"))
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["code"], "planning_owner_forbidden")

    def test_availability_is_read_only_and_does_not_require_contact_history(self):
        self.append_planning_row()
        self.spreadsheet.sheets.pop("sales_activities")
        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        before = sheet.get_all_values()
        with patch.object(app_module, "_planning_write_lock", RejectingLock()):
            response = self.client.get(self.availability_url())
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(sheet.get_all_values(), before)
        self.assertEqual(sheet.update_cell_count, 0)
        self.assertEqual(sheet.batch_update_count, 0)

    def test_missing_planning_sheet_is_empty_without_creating_it(self):
        self.spreadsheet.sheets.pop(app_module.PLANNED_ACTIVITIES_SHEET)
        response = self.client.get(self.availability_url())
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()["activities"], [])
        self.assertNotIn(app_module.PLANNED_ACTIVITIES_SHEET, self.spreadsheet.added_sheets)

    def test_legacy_optional_columns_remain_blank_without_schema_writes(self):
        sheet = FakeWorksheet(
            app_module.PLANNED_ACTIVITIES_SHEET,
            ["planned_activity_id", "user_name", "scheduled_at", "contact_type"],
            [["legacy", "olle", "2026-07-28T08:00:00+02:00", "Telefon"]],
        )
        self.spreadsheet.sheets[app_module.PLANNED_ACTIVITIES_SHEET] = sheet
        before = sheet.get_all_values()
        with patch.object(app_module, "_planning_write_lock", RejectingLock()):
            response = self.client.get(self.availability_url())
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()["activities"], [{
            "user_name": "olle", "scheduled_at": "2026-07-28T08:00+02:00",
            "contact_type": "phone", "status": "planned", "duration_minutes": 0,
        }])
        self.assertEqual(sheet.get_all_values(), before)
        self.assertEqual(sheet.update_cell_count, 0)
        self.assertEqual(sheet.batch_update_count, 0)

    def test_date_range_preserves_status_type_duration_and_stockholm_time(self):
        for index, status in enumerate((
            "planned", "completed", "cancelled", "skipped", "superseded",
        )):
            self.append_planning_row(
                scheduled_at=f"2026-07-28T{8 + index:02d}:00:00+02:00",
                contact_type="phone" if index else "visit",
                status=status,
            )
        self.append_planning_row(scheduled_at="2026-07-26T23:55:00+02:00")
        self.append_planning_row(scheduled_at="2026-07-29T08:00:00+02:00")
        self.append_planning_row(
            scheduled_at="2026-07-27T21:55:00Z", contact_type="email",
        )

        response = self.client.get(self.availability_url())
        self.assertEqual(response.status_code, 200, response.get_json())
        activities = response.get_json()["activities"]
        self.assertEqual(len(activities), 6)
        self.assertEqual(activities[0]["scheduled_at"], "2026-07-27T23:55+02:00")
        self.assertEqual(activities[0]["contact_type"], "email")
        self.assertEqual(activities[0]["duration_minutes"], 10)
        self.assertEqual([item["status"] for item in activities[1:]], [
            "planned", "completed", "cancelled", "skipped", "superseded",
        ])
        self.assertEqual(activities[1]["duration_minutes"], 20)

    def test_availability_uses_existing_date_validation(self):
        for start, end, expected in (
            ("bad", "2026-07-28", "invalid_start_date"),
            ("2026-07-27", "bad", "invalid_end_date"),
            ("2026-07-28", "2026-07-27", "invalid_date_range"),
            ("2026-07-01", "2026-10-01", "invalid_date_range"),
        ):
            with self.subTest(start=start, end=end):
                response = self.client.get(self.availability_url(start=start, end=end))
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.get_json()["code"], expected)

    def test_availability_read_failure_fails_closed(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        self.append_planning_row()
        app_module.read_planned_activity_snapshot(self.spreadsheet)
        sheet = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        with self.assertLogs(app_module.app.logger, level="ERROR"), patch.object(
            sheet, "get_all_values", side_effect=RuntimeError("unavailable"),
        ):
            response = self.client.get(self.availability_url())
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["code"], "planning_store_unavailable")


if __name__ == "__main__":
    main()
