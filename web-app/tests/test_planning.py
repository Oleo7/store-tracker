from __future__ import annotations

from datetime import date, datetime
import json
from pathlib import Path
import re
import sys
import threading
from unittest import TestCase, main
from unittest.mock import patch
from zoneinfo import ZoneInfo


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from route_proposal import TravelTimeResult


STOCKHOLM = ZoneInfo("Europe/Stockholm")
NOW = datetime(2026, 7, 27, 10, 12, tzinfo=STOCKHOLM)
WEEK_START = date(2026, 7, 27)
WEEK_END = date(2026, 8, 2)


def column_number(label):
    result = 0
    for char in label:
        result = result * 26 + ord(char.upper()) - ord("A") + 1
    return result


class FakeWorksheet:
    def __init__(self, title, headers=(), rows=()):
        self.title = title
        self.values = [list(headers)] if headers else []
        self.values.extend(
            [
                [row.get(header, "") for header in headers]
                if isinstance(row, dict)
                else list(row)
                for row in rows
            ]
        )
        self.row_count = max(100, len(self.values) + 20)
        self.col_count = max(10, len(headers))
        self.batch_update_count = 0
        self.update_cell_count = 0
        self.fail_next_batch_update = None
        self.fail_batch_update_at = None
        self.fail_next_update_cell = None

    def get_all_values(self):
        return [list(row) for row in self.values]

    def row_values(self, row):
        return list(self.values[row - 1]) if 0 < row <= len(self.values) else []

    def append_row(self, values, value_input_option=None):
        self.values.append(list(values))

    def append_rows(self, values, value_input_option=None):
        self.values.extend([list(row) for row in values])

    def update_cell(self, row, column, value):
        self.update_cell_count += 1
        if self.fail_next_update_cell is not None:
            error = self.fail_next_update_cell
            self.fail_next_update_cell = None
            raise error
        self._write_cell(row, column, value)

    def update(self, values, range_name=None, **kwargs):
        if isinstance(values, str):
            values, range_name = range_name, values
        self._write_range(range_name, values)

    def batch_update(self, data, value_input_option=None):
        self.batch_update_count += 1
        if self.fail_batch_update_at == self.batch_update_count:
            self.fail_batch_update_at = None
            raise RuntimeError("injected batch update failure")
        if self.fail_next_batch_update is not None:
            error = self.fail_next_batch_update
            self.fail_next_batch_update = None
            raise error
        for item in data:
            self._write_range(item["range"], item["values"])

    def resize(self, rows=None, cols=None):
        if rows is not None:
            self.row_count = max(self.row_count, int(rows))
        if cols is not None:
            self.col_count = max(self.col_count, int(cols))

    def insert_cols(self, columns, col=1):
        insert_at = int(col) - 1
        required_rows = max(
            len(self.values),
            max((len(column) for column in columns), default=0),
        )
        while len(self.values) < required_rows:
            self.values.append([])
        for column_offset, column in enumerate(columns):
            for row_index, row in enumerate(self.values):
                target = insert_at + column_offset
                while len(row) < target:
                    row.append("")
                value = column[row_index] if row_index < len(column) else ""
                row.insert(target, value)
        self.col_count = max(self.col_count, max(map(len, self.values), default=0))

    def delete_columns(self, start_index, end_index=None):
        start = int(start_index) - 1
        end = int(end_index or start_index)
        for row in self.values:
            del row[start:end]

    def dict_rows(self):
        if not self.values:
            return []
        headers = self.values[0]
        result = []
        for row in self.values[1:]:
            padded = row + [""] * (len(headers) - len(row))
            result.append(dict(zip(headers, padded)))
        return result

    def _write_cell(self, row, column, value):
        while len(self.values) < row:
            self.values.append([])
        target = self.values[row - 1]
        while len(target) < column:
            target.append("")
        target[column - 1] = value

    def _write_range(self, range_name, values):
        match = re.fullmatch(
            r"([A-Z]+)(\d+):([A-Z]+)(\d+)",
            str(range_name or ""),
        )
        if not match:
            raise ValueError(f"Unsupported A1 range: {range_name}")
        start_column, start_row, _end_column, _end_row = match.groups()
        first_column = column_number(start_column)
        first_row = int(start_row)
        for row_offset, value_row in enumerate(values):
            for column_offset, value in enumerate(value_row):
                self._write_cell(
                    first_row + row_offset,
                    first_column + column_offset,
                    value,
                )


class FakeSpreadsheet:
    def __init__(self, sheets):
        self.sheets = {sheet.title: sheet for sheet in sheets}
        self.added_sheets = []

    def worksheet(self, title):
        try:
            return self.sheets[title]
        except KeyError:
            raise app_module.WorksheetNotFound(title)

    def add_worksheet(self, title, rows, cols):
        sheet = FakeWorksheet(title)
        sheet.row_count = int(rows)
        sheet.col_count = int(cols)
        self.sheets[title] = sheet
        self.added_sheets.append(title)
        return sheet


class ConstantRoadProvider:
    def __init__(self, seconds=60):
        self.seconds = seconds
        self.call_shapes = []

    def get_matrix_seconds(
        self,
        origins,
        destinations,
        *,
        ephemeral_origin_indexes=frozenset(),
    ):
        self.call_shapes.append((len(origins), len(destinations)))
        matrix = []
        for origin in origins:
            matrix.append(tuple(
                0 if origin == destination else self.seconds
                for destination in destinations
            ))
        return TravelTimeResult(
            seconds=tuple(matrix),
            pair_count=len(origins) * len(destinations),
            request_count=1,
            routing_preference="TRAFFIC_UNAWARE",
        )


def default_spreadsheet(*, include_planning_sheet=True, legacy_contacts=False):
    customer_headers = [
        *app_module.CUSTOMER_COLUMNS,
        "city_google",
        "region_google",
        "latitude_google",
        "longitude_google",
    ]
    customers = FakeWorksheet(
        "customers_enriched",
        customer_headers,
        [
            {
                "customer": "Butik A",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "sales_person": "Olle",
                "customer_segment": "A",
                "customer_number": "C-1",
                "latitude_google": "57.7001",
                "longitude_google": "11.9001",
            },
            {
                "customer": "Butik B",
                "customer_id": "22222222-2222-4222-8222-222222222222",
                "sales_person": "Sofia",
                "customer_segment": "A",
                "customer_number": "C-2",
                "latitude_google": "57.7101",
                "longitude_google": "11.9101",
            },
            {
                "customer": "Butik C",
                "customer_id": "33333333-3333-4333-8333-333333333333",
                "sales_person": "Olle",
                "customer_segment": "B",
                "customer_number": "C-3",
                "latitude_google": "57.7201",
                "longitude_google": "11.9201",
            },
            {
                "customer": "Avslutad butik",
                "customer_id": "44444444-4444-4444-8444-444444444444",
                "cancelled_flag": "Y",
                "sales_person": "Olle",
                "customer_number": "C-4",
                "latitude_google": "57.7301",
                "longitude_google": "11.9301",
            },
        ],
    )
    users = FakeWorksheet(
        app_module.USERS_SHEET,
        app_module.USER_COLUMNS,
        [
            {
                "user_name": "olle",
                "name": "Olle",
                "role": "Säljare",
                "email": "olle@eatpolarbar.com",
                "password": "secret",
                "active": "Y",
                "admin": "N",
            },
            {
                "user_name": "sofia",
                "name": "Sofia",
                "role": "Säljare",
                "email": "sofia@eatpolarbar.com",
                "password": "secret",
                "active": "Y",
                "admin": "N",
            },
            {
                "user_name": "admin",
                "name": "Admin",
                "role": "Administratör",
                "email": "admin@eatpolarbar.com",
                "password": "secret",
                "active": "Y",
                "admin": "Y",
            },
            {
                "user_name": "viewer",
                "name": "Viewer",
                "role": "Analys",
                "email": "viewer@eatpolarbar.com",
                "password": "secret",
                "active": "Y",
                "admin": "N",
            },
        ],
    )
    contact_headers = (
        app_module.CONTACT_REQUIRED_COLUMNS
        if legacy_contacts
        else app_module.CONTACT_COLUMNS
    )
    sheets = [
        customers,
        users,
        FakeWorksheet("sales_activities", contact_headers),
        FakeWorksheet("order_rows", app_module.ORDER_COLUMNS),
    ]
    if include_planning_sheet:
        sheets.append(
            FakeWorksheet(
                app_module.PLANNED_ACTIVITIES_SHEET,
                app_module.PLANNED_ACTIVITY_COLUMNS,
            )
        )
    return FakeSpreadsheet(sheets)


class PlanningApiTestCase(TestCase):
    def setUp(self):
        app_module.app.config.update(
            TESTING=True,
            SECRET_KEY="planning-test-secret",
        )
        self.original_email_sheets_cache = app_module._email_sheets_cache
        app_module._email_sheets_cache = None
        self.spreadsheet = default_spreadsheet()
        self.spreadsheet_patcher = patch.object(
            app_module,
            "get_spreadsheet_with_retry",
            return_value=self.spreadsheet,
        )
        self.now_patcher = patch.object(
            app_module,
            "stockholm_now",
            return_value=NOW,
        )
        self.today_patcher = patch.object(
            app_module,
            "stockholm_today",
            return_value=NOW.date(),
        )
        self.spreadsheet_patcher.start()
        self.now_patcher.start()
        self.today_patcher.start()
        self.client = app_module.app.test_client()
        self.login()

    def tearDown(self):
        self.today_patcher.stop()
        self.now_patcher.stop()
        self.spreadsheet_patcher.stop()
        app_module._email_sheets_cache = self.original_email_sheets_cache

    def login(self, user_name="olle"):
        user = next(
            row
            for row in self.spreadsheet.worksheet(
                app_module.USERS_SHEET
            ).dict_rows()
            if row["user_name"] == user_name
        )
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(user)

    def planning_rows(self):
        return self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        ).dict_rows()

    def contact_rows(self):
        return self.spreadsheet.worksheet("sales_activities").dict_rows()

    def append_contact_row(self, **overrides):
        row = {
            "date_time": "2026-07-27 09:00",
            "sales_person": "Olle",
            "customer": "Butik A",
            "contact_channel": "Telefon",
            "result": "Positiv",
            "comment": "Ring igen",
            "follow_up_date": "2026-07-30",
            "contact_id": "source-contact-1",
            **overrides,
        }
        sheet = self.spreadsheet.worksheet("sales_activities")
        sheet.append_row([
            row.get(column, "")
            for column in app_module.CONTACT_COLUMNS
        ])
        return row

    def append_planning_row(self, **overrides):
        owner = overrides.pop("owner", {
            "user_name": "olle",
            "name": "Olle",
        })
        customer_row = int(overrides.pop("customer_row", 2))
        customer = app_module.get_customer_by_row(
            self.spreadsheet,
            customer_row,
        )
        activity_id = overrides.pop(
            "planned_activity_id",
            app_module.stable_planning_uuid(
                "test-activity",
                owner["user_name"],
                len(self.planning_rows()) + 1,
            ),
        )
        row = app_module.build_planned_activity_row(
            activity_id=activity_id,
            owner=owner,
            customer=customer,
            contact_type=overrides.pop("contact_type", "visit"),
            scheduled_at=overrides.pop(
                "scheduled_at",
                "2026-07-28T09:00:00+02:00",
            ),
            note=overrides.pop("note", "Planerad aktivitet"),
            status=overrides.pop("status", "planned"),
            source=overrides.pop("source", "manual"),
            source_contact_id=overrides.pop("source_contact_id", ""),
            completed_contact_id=overrides.pop("completed_contact_id", ""),
            route_group_id=overrides.pop("route_group_id", ""),
            route_sequence=overrides.pop("route_sequence", ""),
            client_request_id=overrides.pop(
                "client_request_id",
                f"seed-{activity_id}",
            ),
            time_is_estimated=overrides.pop("time_is_estimated", False),
            created_at=overrides.pop("created_at", None),
            updated_at=overrides.pop("updated_at", None),
        )
        row.update(overrides)
        sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        sheet.append_row(
            [row.get(column, "") for column in app_module.PLANNED_ACTIVITY_COLUMNS]
        )
        return row


class PlanningHelperTests(TestCase):
    def test_stockholm_datetime_normalizes_utc_and_rejects_dst_gap(self):
        utc = app_module.parse_planning_datetime("2026-07-28T07:30:00Z")
        nonexistent = app_module.parse_planning_datetime("2026-03-29T02:30")

        self.assertEqual(
            utc.isoformat(timespec="minutes"),
            "2026-07-28T09:30+02:00",
        )
        self.assertIsNone(nonexistent)

    def test_stockholm_datetime_rejects_aware_gap_and_accepts_both_folds(self):
        aware_gap = app_module.parse_planning_datetime(
            "2026-03-29T02:30:00+02:00"
        )
        summer_fold = app_module.parse_planning_datetime(
            "2026-10-25T02:30:00+02:00"
        )
        winter_fold = app_module.parse_planning_datetime(
            "2026-10-25T02:30:00+01:00"
        )

        self.assertIsNone(aware_gap)
        self.assertEqual(
            summer_fold.isoformat(timespec="minutes"),
            "2026-10-25T02:30+02:00",
        )
        self.assertEqual(
            winter_fold.isoformat(timespec="minutes"),
            "2026-10-25T02:30+01:00",
        )

    def test_production_requires_explicit_flask_secret(self):
        with self.assertRaises(RuntimeError):
            app_module.resolve_flask_secret_key({"RENDER": "true"})
        with self.assertRaises(RuntimeError):
            app_module.resolve_flask_secret_key({"APP_ENV": "production"})

        self.assertEqual(
            app_module.resolve_flask_secret_key({}),
            app_module.LOCAL_SESSION_SECRET,
        )
        self.assertEqual(
            app_module.resolve_flask_secret_key({
                "RENDER": "true",
                "FLASK_SECRET_KEY": "production-secret",
            }),
            "production-secret",
        )

    def test_today_route_start_rounds_seconds_up_to_next_five_minutes(self):
        now = datetime(
            2026,
            7,
            27,
            10,
            15,
            1,
            tzinfo=STOCKHOLM,
        )

        start = app_module.route_start_datetime(now.date(), now=now)

        self.assertEqual(
            start.isoformat(timespec="minutes"),
            "2026-07-27T10:20+02:00",
        )

    def test_public_activity_keeps_legacy_rows_backward_compatible(self):
        public = app_module.public_planned_activity(
            {
                "planned_activity_id": "legacy-1",
                "user_name": "olle",
                "customer": "Butik A",
                "contact_type": "Telefon",
                "scheduled_at": "2026-07-27 09:00",
                "duration_minutes": "10",
                "time_is_estimated": "Y",
            },
            now=NOW,
        )

        self.assertEqual(public["contact_type"], "phone")
        self.assertTrue(public["time_is_estimated"])
        self.assertEqual(public["status"], "planned")
        self.assertTrue(public["overdue"])


class PlanningActivityApiTests(PlanningApiTestCase):
    def manual_payload(self, **overrides):
        customer_row = overrides.get("customer_row", 2)
        customer_ids = {
            2: "11111111-1111-4111-8111-111111111111",
            3: "22222222-2222-4222-8222-222222222222",
            4: "33333333-3333-4333-8333-333333333333",
            5: "44444444-4444-4444-8444-444444444444",
        }
        payload = {
            "client_request_id": "manual-create-1",
            "customer_row": customer_row,
            "customer_id": customer_ids.get(customer_row, ""),
            "contact_type": "Besök",
            "scheduled_at": "2026-07-28T09:30:00+02:00",
            "note": "Planerat butiksbesök",
        }
        payload.update(overrides)
        return payload

    def test_manual_create_autocreates_schema_and_retry_is_idempotent(self):
        self.spreadsheet.sheets.pop(app_module.PLANNED_ACTIVITIES_SHEET)
        payload = self.manual_payload()

        first = self.client.post("/planning/activities", json=payload)
        second = self.client.post("/planning/activities", json=payload)

        self.assertEqual(first.status_code, 201, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        first_body = first.get_json()
        second_body = second.get_json()
        self.assertFalse(first_body["duplicate"])
        self.assertTrue(second_body["duplicate"])
        self.assertEqual(
            first_body["activity"]["planned_activity_id"],
            second_body["activity"]["planned_activity_id"],
        )
        sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        self.assertEqual(sheet.row_values(1), app_module.PLANNED_ACTIVITY_COLUMNS)
        self.assertEqual(len(sheet.dict_rows()), 1)
        saved = sheet.dict_rows()[0]
        self.assertEqual(saved["source"], "manual")
        self.assertEqual(saved["duration_minutes"], 20)
        self.assertEqual(saved["status"], "planned")

    def test_create_normalizes_stockholm_time_and_ignores_client_duration(self):
        response = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="timezone-create",
                contact_type="Telefon",
                scheduled_at="2026-07-28T07:30:00Z",
                duration_minutes=999,
            ),
        )

        self.assertEqual(response.status_code, 201, response.get_json())
        activity = response.get_json()["activity"]
        self.assertEqual(activity["contact_type"], "phone")
        self.assertEqual(activity["scheduled_at"], "2026-07-28T09:30+02:00")
        self.assertEqual(activity["duration_minutes"], 10)
        self.assertIs(activity["time_is_estimated"], False)

    def test_create_validates_request_type_time_note_and_customer(self):
        cases = [
            (
                {"client_request_id": ""},
                "client_request_id",
                400,
            ),
            (
                {
                    "client_request_id": "bad-type",
                    "contact_type": "SMS",
                },
                "contact_type",
                400,
            ),
            (
                {
                    "client_request_id": "bad-time",
                    "scheduled_at": "inte-en-tid",
                },
                "scheduled_at",
                400,
            ),
            (
                {
                    "client_request_id": "dst-gap",
                    "scheduled_at": "2026-03-29T02:30",
                },
                "scheduled_at",
                400,
            ),
            (
                {
                    "client_request_id": "long-note",
                    "note": "x" * 301,
                },
                "note",
                400,
            ),
            (
                {
                    "client_request_id": "unknown-customer",
                    "customer_row": 999,
                },
                "customer_id",
                422,
            ),
        ]
        for overrides, expected_field, expected_status in cases:
            with self.subTest(field=expected_field, payload=overrides):
                response = self.client.post(
                    "/planning/activities",
                    json=self.manual_payload(**overrides),
                )
                self.assertEqual(
                    response.status_code,
                    expected_status,
                    response.get_json(),
                )
                self.assertEqual(response.get_json().get("field"), expected_field)

        self.assertEqual(self.planning_rows(), [])

    def test_seller_can_plan_other_customer_but_not_read_another_calendar(self):
        create = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="other-customer",
                customer_row=3,
            ),
        )
        read = self.client.get(
            "/planning/activities"
            "?start=2026-07-27&end=2026-08-02&user_name=sofia"
        )

        self.assertEqual(create.status_code, 201, create.get_json())
        self.assertEqual(create.get_json()["activity"]["user_name"], "olle")
        self.assertEqual(create.get_json()["activity"]["customer"], "Butik B")
        self.assertEqual(read.status_code, 403, read.get_json())
        self.assertNotIn("Sofia", str(read.get_json()))
        self.assertEqual(len(self.planning_rows()), 1)

    def test_admin_can_create_and_read_for_another_seller(self):
        self.login("admin")
        create = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="admin-for-sofia",
                customer_row=3,
                user_name="sofia",
            ),
        )
        read = self.client.get(
            "/planning/activities"
            "?start=2026-07-27&end=2026-08-02&user_name=sofia"
        )

        self.assertEqual(create.status_code, 201, create.get_json())
        self.assertEqual(create.get_json()["activity"]["user_name"], "sofia")
        self.assertEqual(read.status_code, 200, read.get_json())
        self.assertEqual(read.get_json()["owner"]["user_name"], "sofia")
        self.assertEqual(len(read.get_json()["activities"]), 1)

    def test_admin_defaults_get_to_seller_but_cannot_write_admin_calendar(self):
        self.login("admin")

        create_for_admin = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="admin-self-calendar",
            ),
        )
        default_read = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )
        explicit_self_read = self.client.get(
            "/planning/activities"
            "?start=2026-07-27&end=2026-08-02&user_name=admin"
        )

        self.assertEqual(
            create_for_admin.status_code,
            422,
            create_for_admin.get_json(),
        )
        self.assertEqual(
            create_for_admin.get_json()["error"],
            "planning_owner_not_sales_user",
        )
        self.assertEqual(default_read.status_code, 200, default_read.get_json())
        self.assertEqual(
            default_read.get_json()["owner"]["user_name"],
            "olle",
        )
        self.assertEqual(
            explicit_self_read.get_json()["owner"]["user_name"],
            "olle",
        )
        self.assertEqual(
            [
                user["user_name"]
                for user in default_read.get_json()["available_users"]
            ],
            ["olle", "sofia"],
        )
        self.assertEqual(self.planning_rows(), [])

    def test_non_sales_user_cannot_use_planning(self):
        self.login("viewer")
        response = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )

        self.assertEqual(response.status_code, 403, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "planning_access_forbidden",
        )

    def test_get_week_sorts_history_and_builds_day_summaries(self):
        late = self.append_planning_row(
            planned_activity_id="late",
            scheduled_at="2026-07-28T15:00:00+02:00",
            contact_type="phone",
        )
        early = self.append_planning_row(
            planned_activity_id="early",
            scheduled_at="2026-07-28T09:00:00+02:00",
            status="completed",
            completed_contact_id="contact-complete",
        )
        cancelled = self.append_planning_row(
            planned_activity_id="cancelled",
            scheduled_at="2026-07-29T10:00:00+02:00",
            status="cancelled",
        )
        self.append_planning_row(
            planned_activity_id="outside",
            scheduled_at="2026-08-03T09:00:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="sofia-private",
            owner={"user_name": "sofia", "name": "Sofia"},
            customer_row=3,
            scheduled_at="2026-07-28T08:00:00+02:00",
        )

        response = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertEqual(
            [item["planned_activity_id"] for item in body["activities"]],
            [early["planned_activity_id"], late["planned_activity_id"], cancelled["planned_activity_id"]],
        )
        summary_by_date = {
            item["date"]: item for item in body["summaries"]
        }
        self.assertEqual(summary_by_date["2026-07-28"]["activity_count"], 2)
        self.assertEqual(summary_by_date["2026-07-28"]["completed"], 1)
        self.assertEqual(summary_by_date["2026-07-28"]["phone"], 1)
        self.assertEqual(summary_by_date["2026-07-29"]["activity_count"], 0)
        self.assertEqual(summary_by_date["2026-07-29"]["cancelled"], 1)
        self.assertEqual(len(body["days"]), 7)

    def test_get_includes_unplanned_history_and_unscheduled_legacy_followups(self):
        legacy_headers = list(app_module.CONTACT_REQUIRED_COLUMNS)
        legacy = FakeWorksheet(
            "sales_activities",
            legacy_headers,
            [
                {
                    "date_time": "2026-07-28 11:00",
                    "sales_person": "Olle",
                    "customer": "Butik A",
                    "contact_channel": "Telefon",
                    "result": "Positiv",
                    "comment": "Oplanerat samtal",
                    "follow_up_date": "2026-07-31",
                },
                {
                    "date_time": "2026-07-28 12:00",
                    "sales_person": "Sofia",
                    "customer": "Butik B",
                    "contact_channel": "Telefon",
                    "result": "Positiv",
                    "comment": "Privat",
                    "follow_up_date": "2026-07-31",
                },
            ],
        )
        self.spreadsheet.sheets["sales_activities"] = legacy

        response = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertEqual(len(body["unplanned_contacts"]), 1)
        self.assertEqual(
            body["unplanned_contacts"][0]["customer"],
            "Butik A",
        )
        self.assertEqual(len(body["unscheduled_followups"]), 1)
        self.assertEqual(
            body["unscheduled_followups"][0]["follow_up_date"],
            "2026-07-31",
        )
        day_summary = next(
            item
            for item in body["day_summaries"]
            if item["date"] == "2026-07-28"
        )
        self.assertEqual(day_summary["unplanned_count"], 1)
        self.assertEqual(day_summary["planned_activity_count"], 0)
        self.assertEqual(day_summary["activity_count"], 1)
        headers = legacy.row_values(1)
        self.assertIn("contact_id", headers)
        self.assertIn("planned_activity_id", headers)

    def test_patch_moves_activity_and_retry_is_idempotent(self):
        seeded = self.append_planning_row(planned_activity_id="move-me")
        payload = {
            "client_request_id": "move-request-1",
            "expected_updated_at": seeded["updated_at"],
            "scheduled_at": "2026-07-29T13:15:00+02:00",
            "contact_type": "Telefon",
            "note": "Flyttad efter samtal",
        }

        first = self.client.patch(
            f"/planning/activities/{seeded['planned_activity_id']}",
            json=payload,
        )
        second = self.client.patch(
            f"/planning/activities/{seeded['planned_activity_id']}",
            json=payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertFalse(first.get_json()["duplicate"])
        self.assertTrue(second.get_json()["duplicate"])
        self.assertEqual(len(self.planning_rows()), 1)
        updated = self.planning_rows()[0]
        self.assertEqual(updated["scheduled_at"], "2026-07-29T13:15+02:00")
        self.assertEqual(updated["contact_type"], "phone")
        self.assertEqual(updated["duration_minutes"], 10)
        self.assertEqual(updated["note"], "Flyttad efter samtal")

    def test_patch_enforces_owner_and_terminal_status_transitions(self):
        own = self.append_planning_row(planned_activity_id="terminal")
        other = self.append_planning_row(
            planned_activity_id="sofia-terminal",
            owner={"user_name": "sofia", "name": "Sofia"},
            customer_row=3,
        )
        completed = self.append_planning_row(
            planned_activity_id="completed-terminal",
            status="completed",
            completed_contact_id="completed-contact",
        )
        cancelled = self.append_planning_row(
            planned_activity_id="cancelled-terminal",
            status="cancelled",
        )

        forbidden = self.client.patch(
            f"/planning/activities/{other['planned_activity_id']}",
            json={
                "client_request_id": "forbidden-patch",
                "expected_updated_at": other["updated_at"],
                "note": "Läck inte",
            },
        )
        completed_directly = self.client.patch(
            f"/planning/activities/{own['planned_activity_id']}",
            json={
                "client_request_id": "bad-completion",
                "expected_updated_at": own["updated_at"],
                "status": "completed",
            },
        )
        skipped = self.client.patch(
            f"/planning/activities/{own['planned_activity_id']}",
            json={
                "client_request_id": "skip-once",
                "expected_updated_at": own["updated_at"],
                "status": "skipped",
            },
        )
        reopen = self.client.patch(
            f"/planning/activities/{own['planned_activity_id']}",
            json={
                "client_request_id": "reopen-terminal",
                "expected_updated_at": skipped.get_json()["activity"]["updated_at"],
                "status": "planned",
            },
        )
        edit_completed = self.client.patch(
            f"/planning/activities/{completed['planned_activity_id']}",
            json={
                "client_request_id": "edit-completed",
                "expected_updated_at": completed["updated_at"],
                "note": "Ska inte Ã¤ndras",
            },
        )
        reopen_cancelled = self.client.patch(
            f"/planning/activities/{cancelled['planned_activity_id']}",
            json={
                "client_request_id": "reopen-cancelled",
                "expected_updated_at": cancelled["updated_at"],
                "status": "planned",
            },
        )

        self.assertEqual(forbidden.status_code, 403, forbidden.get_json())
        self.assertNotIn("Sofia", str(forbidden.get_json()))
        self.assertIn(completed_directly.status_code, {400, 409})
        self.assertEqual(skipped.status_code, 200, skipped.get_json())
        self.assertEqual(reopen.status_code, 200, reopen.get_json())
        self.assertIn(edit_completed.status_code, {400, 409})
        self.assertIn(reopen_cancelled.status_code, {400, 409})
        by_id = {
            row["planned_activity_id"]: row for row in self.planning_rows()
        }
        self.assertEqual(by_id["terminal"]["status"], "planned")
        self.assertEqual(by_id["sofia-terminal"]["note"], "Planerad aktivitet")
        self.assertEqual(
            by_id["completed-terminal"]["note"],
            "Planerad aktivitet",
        )
        self.assertEqual(by_id["cancelled-terminal"]["status"], "cancelled")

    def test_patch_rejects_stale_retry_after_newer_update(self):
        seeded = self.append_planning_row(planned_activity_id="stale-patch")
        first_payload = {
            "client_request_id": "patch-a",
            "expected_updated_at": seeded["updated_at"],
            "note": "Version A",
        }
        first = self.client.patch(
            "/planning/activities/stale-patch",
            json=first_payload,
        )
        second = self.client.patch(
            "/planning/activities/stale-patch",
            json={
                "client_request_id": "patch-b",
                "expected_updated_at": first.get_json()["activity"]["updated_at"],
                "note": "Version B",
            },
        )
        stale_retry = self.client.patch(
            "/planning/activities/stale-patch",
            json=first_payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertEqual(stale_retry.status_code, 409, stale_retry.get_json())
        self.assertEqual(stale_retry.get_json()["error"], "planning_changed")
        self.assertEqual(self.planning_rows()[0]["note"], "Version B")

    def test_followup_create_retry_repairs_legacy_date_mirror(self):
        self.append_contact_row()
        contact_sheet = self.spreadsheet.worksheet("sales_activities")
        contact_sheet.fail_next_batch_update = RuntimeError(
            "temporary mirror failure"
        )
        payload = self.manual_payload(
            client_request_id="followup-create-repair",
            source="follow_up",
            source_contact_id="source-contact-1",
            scheduled_at="2026-08-01T10:00:00+02:00",
        )

        partial = self.client.post("/planning/activities", json=payload)
        repaired = self.client.post("/planning/activities", json=payload)

        self.assertEqual(partial.status_code, 503, partial.get_json())
        self.assertEqual(
            partial.get_json()["error"],
            "follow_up_mirror_failed",
        )
        self.assertEqual(repaired.status_code, 200, repaired.get_json())
        self.assertTrue(repaired.get_json()["duplicate"])
        self.assertEqual(len(self.planning_rows()), 1)
        self.assertEqual(
            self.contact_rows()[0]["follow_up_date"],
            "2026-08-01",
        )

    def test_followup_patch_retry_repairs_legacy_date_mirror(self):
        self.append_contact_row()
        seeded = self.append_planning_row(
            planned_activity_id="followup-patch-repair",
            source="follow_up",
            source_contact_id="source-contact-1",
        )
        contact_sheet = self.spreadsheet.worksheet("sales_activities")
        contact_sheet.fail_next_batch_update = RuntimeError(
            "temporary mirror failure"
        )
        payload = {
            "client_request_id": "followup-patch-repair-request",
            "expected_updated_at": seeded["updated_at"],
            "scheduled_at": "2026-08-02T11:00:00+02:00",
        }

        partial = self.client.patch(
            "/planning/activities/followup-patch-repair",
            json=payload,
        )
        repaired = self.client.patch(
            "/planning/activities/followup-patch-repair",
            json=payload,
        )

        self.assertEqual(partial.status_code, 503, partial.get_json())
        self.assertEqual(
            partial.get_json()["error"],
            "follow_up_mirror_failed",
        )
        self.assertEqual(repaired.status_code, 200, repaired.get_json())
        self.assertTrue(repaired.get_json()["duplicate"])
        self.assertEqual(
            self.contact_rows()[0]["follow_up_date"],
            "2026-08-02",
        )


    def test_customer_id_wins_over_tampered_row_cache(self):
        response = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="tampered-row-cache",
                customer_row=3,
                customer_id="11111111-1111-4111-8111-111111111111",
            ),
        )

        self.assertEqual(response.status_code, 201, response.get_json())
        activity = response.get_json()["activity"]
        self.assertEqual(activity["customer"], "Butik A")
        self.assertEqual(activity["customer_row"], 2)

    def test_create_same_request_with_changed_payload_is_rejected(self):
        first = self.client.post(
            "/planning/activities",
            json=self.manual_payload(client_request_id="bound-create"),
        )
        conflicting = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="bound-create",
                note="Annat innehåll",
            ),
        )

        self.assertEqual(first.status_code, 201, first.get_json())
        self.assertEqual(conflicting.status_code, 409, conflicting.get_json())
        self.assertEqual(
            conflicting.get_json()["error"],
            "idempotency_payload_mismatch",
        )
        self.assertEqual(len(self.planning_rows()), 1)

    def test_same_random_request_id_is_scoped_per_actor(self):
        first = self.client.post(
            "/planning/activities",
            json=self.manual_payload(client_request_id="shared-random-id"),
        )
        self.login("admin")
        second = self.client.post(
            "/planning/activities",
            json=self.manual_payload(
                client_request_id="shared-random-id",
                user_name="olle",
            ),
        )

        self.assertEqual(first.status_code, 201, first.get_json())
        self.assertEqual(second.status_code, 201, second.get_json())
        self.assertEqual(len(self.planning_rows()), 2)

    def test_route_content_edit_converts_atomically_to_manual(self):
        activity = self.append_planning_row(
            planned_activity_id="route-to-manual",
            source="route",
            route_group_id="route-group",
            route_sequence=3,
            time_is_estimated=True,
        )
        sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        calls_before = sheet.batch_update_count

        response = self.client.patch(
            "/planning/activities/route-to-manual",
            json={
                "client_request_id": "route-edit",
                "expected_revision": 1,
                "note": "Fast tid efter kundbesked",
            },
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        updated = response.get_json()["activity"]
        self.assertEqual(updated["source"], "manual")
        self.assertEqual(updated["route_group_id"], "")
        self.assertIsNone(updated["route_sequence"])
        self.assertFalse(updated["time_is_estimated"])
        self.assertEqual(updated["revision"], 2)
        self.assertEqual(sheet.batch_update_count - calls_before, 1)

    def test_route_status_change_keeps_route_source(self):
        self.append_planning_row(
            planned_activity_id="route-skip",
            source="route",
            route_group_id="route-group",
            route_sequence=1,
            time_is_estimated=True,
        )

        response = self.client.patch(
            "/planning/activities/route-skip",
            json={
                "client_request_id": "route-skip-request",
                "expected_revision": 1,
                "status": "skipped",
            },
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        activity = response.get_json()["activity"]
        self.assertEqual(activity["source"], "route")
        self.assertEqual(activity["route_group_id"], "route-group")

    def test_stale_expected_revision_returns_revision_conflict(self):
        activity = self.append_planning_row(
            planned_activity_id="revision-conflict"
        )
        first = self.client.patch(
            "/planning/activities/revision-conflict",
            json={
                "client_request_id": "revision-first",
                "expected_revision": 1,
                "note": "Första",
            },
        )
        stale = self.client.patch(
            "/planning/activities/revision-conflict",
            json={
                "client_request_id": "revision-stale",
                "expected_revision": 1,
                "expected_updated_at": first.get_json()["activity"]["updated_at"],
                "note": "Ska inte vinna",
            },
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(stale.status_code, 409, stale.get_json())
        self.assertEqual(stale.get_json()["error"], "revision_conflict")
        self.assertEqual(self.planning_rows()[0]["note"], "Första")

    def test_concurrent_updates_allow_exactly_one_revision_winner(self):
        self.append_planning_row(planned_activity_id="concurrent-revision")
        barrier = threading.Barrier(2)
        results = []
        result_lock = threading.Lock()

        def update_activity(request_id, note):
            client = app_module.app.test_client()
            user = next(
                row
                for row in self.spreadsheet.worksheet(
                    app_module.USERS_SHEET
                ).dict_rows()
                if row["user_name"] == "olle"
            )
            with client.session_transaction() as flask_session:
                flask_session["user"] = app_module.public_user(user)
            barrier.wait()
            response = client.patch(
                "/planning/activities/concurrent-revision",
                json={
                    "client_request_id": request_id,
                    "expected_revision": 1,
                    "note": note,
                },
            )
            with result_lock:
                results.append((response.status_code, response.get_json()))

        threads = [
            threading.Thread(
                target=update_activity,
                args=("concurrent-a", "Anteckning A"),
            ),
            threading.Thread(
                target=update_activity,
                args=("concurrent-b", "Anteckning B"),
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(
            sorted(status for status, _body in results),
            [200, 409],
        )
        conflict = next(body for status, body in results if status == 409)
        self.assertEqual(conflict["error"], "revision_conflict")
        self.assertEqual(int(self.planning_rows()[0]["revision"]), 2)
        self.assertIn(
            self.planning_rows()[0]["note"],
            {"Anteckning A", "Anteckning B"},
        )

    def test_concurrent_creates_for_different_customers_do_not_collide(self):
        barrier = threading.Barrier(2)
        results = []
        result_lock = threading.Lock()

        def create_activity(payload):
            client = app_module.app.test_client()
            user = next(
                row
                for row in self.spreadsheet.worksheet(
                    app_module.USERS_SHEET
                ).dict_rows()
                if row["user_name"] == "olle"
            )
            with client.session_transaction() as flask_session:
                flask_session["user"] = app_module.public_user(user)
            barrier.wait()
            response = client.post("/planning/activities", json=payload)
            with result_lock:
                results.append((response.status_code, response.get_json()))

        payloads = [
            self.manual_payload(
                client_request_id="concurrent-create-a",
                customer_row=2,
            ),
            self.manual_payload(
                client_request_id="concurrent-create-b",
                customer_row=3,
            ),
        ]
        threads = [
            threading.Thread(target=create_activity, args=(payload,))
            for payload in payloads
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(
            sorted(status for status, _body in results),
            [201, 201],
        )
        self.assertEqual(len(self.planning_rows()), 2)
        self.assertEqual(
            {row["customer_id"] for row in self.planning_rows()},
            {
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
            },
        )

    def test_global_followup_queue_keeps_separate_same_customer_items(self):
        self.append_contact_row(
            contact_id="followup-overdue",
            customer_id="11111111-1111-4111-8111-111111111111",
            follow_up_date="2026-06-01",
            comment="Äldre separat uppföljning",
        )
        self.append_contact_row(
            contact_id="followup-outside-week",
            customer_id="11111111-1111-4111-8111-111111111111",
            follow_up_date="2026-08-15",
            comment="Kommande separat uppföljning",
        )

        response = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertEqual(
            [item["source_contact_id"] for item in body["unscheduled_followups_overdue"]],
            ["followup-overdue"],
        )
        self.assertIn(
            "followup-outside-week",
            [
                item["source_contact_id"]
                for item in body["unscheduled_followups_upcoming"]
            ],
        )


class PlanningContactCompletionTests(PlanningApiTestCase):
    def contact_payload(self, activity, **overrides):
        payload = {
            "client_request_id": "complete-contact-1",
            "planned_activity_id": activity["planned_activity_id"],
            "date_time": "2026-07-28 09:10",
            "contact_channel": "Telefon",
            "result": "Positiv",
            "comment": "Bra dialog med butiken",
            "customer_contact_person": "Klara",
        }
        payload.update(overrides)
        return payload

    def test_contact_without_id_rejects_duplicate_customer_names(self):
        customer_sheet = self.spreadsheet.worksheet(
            "customers_enriched"
        )
        headers = customer_sheet.row_values(1)
        duplicate = dict(customer_sheet.dict_rows()[0])
        duplicate.update({
            "customer_id": "55555555-5555-4555-8555-555555555555",
            "customer_number": "500",
            "Address": "Annan gata 5",
            "City": "Stockholm",
        })
        customer_sheet.append_row([
            duplicate.get(header, "")
            for header in headers
        ])

        response = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                "contact_channel": "Telefon",
                "result": "Neutral",
                "comment": "Ska inte kunna bindas till fel butik",
            },
        )

        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(response.get_json()["error"], "ambiguous_customer")
        self.assertEqual(self.contact_rows(), [])

    def test_phone_completion_does_not_require_freezer_and_retry_is_idempotent(self):
        activity = self.append_planning_row(
            planned_activity_id="phone-completion",
            contact_type="phone",
        )
        payload = self.contact_payload(activity)

        first = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )
        second = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertFalse(first.get_json()["duplicate"])
        self.assertTrue(second.get_json()["duplicate"])
        self.assertTrue(first.get_json()["activity_completed"])
        self.assertEqual(len(self.contact_rows()), 1)
        contact = self.contact_rows()[0]
        self.assertEqual(contact["contact_channel"], "Telefon")
        self.assertEqual(
            contact["planned_activity_id"],
            activity["planned_activity_id"],
        )
        self.assertTrue(contact["contact_id"])
        stored_activity = self.planning_rows()[0]
        self.assertEqual(stored_activity["status"], "completed")
        self.assertEqual(
            stored_activity["completed_contact_id"],
            contact["contact_id"],
        )

    def test_visit_completion_without_freezer_is_rejected(self):
        activity = self.append_planning_row(
            planned_activity_id="visit-needs-freezer",
            contact_type="visit",
        )
        response = self.client.post(
            "/customers/Butik%20A/contacts",
            json=self.contact_payload(
                activity,
                client_request_id="visit-without-freezer",
                contact_channel="Besök",
            ),
        )

        self.assertEqual(response.status_code, 400, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "freezer_selection_required",
        )
        self.assertEqual(self.contact_rows(), [])
        self.assertEqual(self.planning_rows()[0]["status"], "planned")

    def test_completion_and_nested_followup_are_linked_exactly_once(self):
        activity = self.append_planning_row(
            planned_activity_id="complete-with-followup",
            contact_type="phone",
        )
        payload = self.contact_payload(
            activity,
            client_request_id="completion-followup-1",
            follow_up={
                "enabled": True,
                "contact_type": "Besök",
                "scheduled_at": "2026-08-01T10:30:00+02:00",
                "note": "Ta med produktblad",
            },
        )

        first = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )
        second = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertEqual(len(self.contact_rows()), 1)
        self.assertEqual(len(self.planning_rows()), 2)
        contact = self.contact_rows()[0]
        followup_id = first.get_json()["follow_up"]["planned_activity_id"]
        by_id = {
            row["planned_activity_id"]: row for row in self.planning_rows()
        }
        self.assertEqual(
            by_id[activity["planned_activity_id"]]["status"],
            "completed",
        )
        followup = by_id[followup_id]
        self.assertEqual(followup["source"], "follow_up")
        self.assertEqual(followup["source_contact_id"], contact["contact_id"])
        self.assertEqual(followup["contact_type"], "visit")
        self.assertEqual(followup["scheduled_at"], "2026-08-01T10:30+02:00")
        self.assertEqual(contact["follow_up_date"], "2026-08-01")

    def test_partial_followup_failure_resumes_without_duplicate_contact(self):
        activity = self.append_planning_row(
            planned_activity_id="partial-followup",
            contact_type="phone",
        )
        planned_sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        planned_sheet.fail_batch_update_at = (
            planned_sheet.batch_update_count + 2
        )
        payload = self.contact_payload(
            activity,
            client_request_id="partial-followup-request",
            follow_up={
                "enabled": True,
                "contact_type": "Telefon",
                "scheduled_at": "2026-08-01T11:00:00+02:00",
                "note": "Ring igen",
            },
        )

        partial = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )
        resumed = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )

        self.assertEqual(partial.status_code, 207, partial.get_json())
        self.assertEqual(partial.get_json()["error"], "partial_save")
        self.assertTrue(partial.get_json()["contact_saved"])
        self.assertFalse(partial.get_json()["follow_up"]["saved"])
        self.assertEqual(resumed.status_code, 200, resumed.get_json())
        self.assertTrue(resumed.get_json()["duplicate"])
        self.assertTrue(resumed.get_json()["follow_up"]["saved"])
        self.assertEqual(len(self.contact_rows()), 1)
        self.assertEqual(len(self.planning_rows()), 2)

    def test_calendar_contact_requires_active_seller_owner(self):
        payload = {
            "client_request_id": "calendar-owner-check",
            "date_time": "2026-07-28 09:10",
            "contact_channel": "Telefon",
            "result": "Positiv",
            "comment": "Behörighetskontroll",
            "follow_up": {
                "enabled": True,
                "contact_type": "Telefon",
                "scheduled_at": "2026-08-01T11:00:00+02:00",
                "note": "Ring igen",
            },
        }

        self.login("viewer")
        viewer = self.client.post(
            "/customers/Butik%20A/contacts",
            json=payload,
        )
        self.login("admin")
        admin_without_owner = self.client.post(
            "/customers/Butik%20A/contacts",
            json={**payload, "client_request_id": "admin-no-owner"},
        )
        admin_for_seller = self.client.post(
            "/customers/Butik%20A/contacts",
            json={
                **payload,
                "client_request_id": "admin-for-sofia",
                "user_name": "sofia",
                "customer_id": app_module.get_customer_by_row(
                    self.spreadsheet,
                    2,
                )["customer_id"],
            },
        )

        self.assertEqual(viewer.status_code, 403, viewer.get_json())
        self.assertEqual(
            admin_without_owner.status_code,
            422,
            admin_without_owner.get_json(),
        )
        self.assertEqual(
            admin_without_owner.get_json()["error"],
            "planning_owner_required",
        )
        self.assertEqual(
            admin_for_seller.status_code,
            200,
            admin_for_seller.get_json(),
        )
        self.assertEqual(len(self.contact_rows()), 1)
        self.assertEqual(self.contact_rows()[0]["sales_person"], "Sofia")
        followups = [
            row for row in self.planning_rows()
            if row["source"] == "follow_up"
        ]
        self.assertEqual(len(followups), 1)
        self.assertEqual(followups[0]["user_name"], "sofia")

    def test_planned_contact_rejects_non_seller_activity_owner(self):
        activity = self.append_planning_row(
            planned_activity_id="viewer-owned-activity",
            owner={"user_name": "viewer", "name": "Viewer"},
            contact_type="phone",
        )
        self.login("viewer")

        response = self.client.post(
            "/customers/Butik%20A/contacts",
            json=self.contact_payload(
                activity,
                client_request_id="viewer-owned-completion",
            ),
        )

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "activity_owner_not_sales_user",
        )
        self.assertEqual(self.contact_rows(), [])

    def test_planned_contact_rejects_inactive_seller_owner(self):
        activity = self.append_planning_row(
            planned_activity_id="inactive-seller-activity",
            owner={"user_name": "sofia", "name": "Sofia"},
            customer_row=3,
            contact_type="phone",
        )
        users = self.spreadsheet.worksheet(app_module.USERS_SHEET)
        users.update_cell(
            3,
            app_module.USER_COLUMNS.index("active") + 1,
            "N",
        )
        self.login("admin")

        response = self.client.post(
            "/customers/Butik%20B/contacts",
            json=self.contact_payload(
                activity,
                client_request_id="inactive-owner-completion",
            ),
        )

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "activity_owner_not_sales_user",
        )
        self.assertEqual(self.contact_rows(), [])

    def test_partial_completion_new_request_id_repairs_without_new_contact(self):
        activity = self.append_planning_row(
            planned_activity_id="partial-new-request",
            contact_type="phone",
        )
        planned_sheet = self.spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        )
        planned_sheet.fail_next_batch_update = RuntimeError(
            "temporary completion failure"
        )
        first_payload = self.contact_payload(
            activity,
            client_request_id="completion-request-a",
        )
        retry_payload = {
            **first_payload,
            "client_request_id": "completion-request-b",
        }

        partial = self.client.post(
            "/customers/Butik%20A/contacts",
            json=first_payload,
        )
        repaired = self.client.post(
            "/customers/Butik%20A/contacts",
            json=retry_payload,
        )

        self.assertEqual(partial.status_code, 207, partial.get_json())
        self.assertFalse(partial.get_json()["activity_completed"])
        self.assertEqual(repaired.status_code, 200, repaired.get_json())
        self.assertTrue(repaired.get_json()["duplicate"])
        self.assertTrue(repaired.get_json()["activity_completed"])
        self.assertEqual(len(self.contact_rows()), 1)
        stored_activity = self.planning_rows()[0]
        self.assertEqual(stored_activity["status"], "completed")
        self.assertEqual(
            stored_activity["completed_contact_id"],
            self.contact_rows()[0]["contact_id"],
        )

    def test_planned_contact_changed_payload_conflicts_without_duplicate(self):
        activity = self.append_planning_row(
            planned_activity_id="contact-payload-binding",
            contact_type="phone",
        )
        first = self.client.post(
            "/customers/Butik%20A/contacts",
            json=self.contact_payload(
                activity,
                client_request_id="payload-binding-a",
            ),
        )
        conflicting = self.client.post(
            "/customers/Butik%20A/contacts",
            json=self.contact_payload(
                activity,
                client_request_id="payload-binding-b",
                comment="Andra kontaktuppgifter",
            ),
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(
            conflicting.status_code,
            409,
            conflicting.get_json(),
        )
        self.assertEqual(
            conflicting.get_json()["error"],
            "planned_activity_contact_conflict",
        )
        self.assertEqual(len(self.contact_rows()), 1)


class PlanningRouteApiTests(PlanningApiTestCase):
    def route_stop(
        self,
        customer_row,
        sequence,
        cumulative_total_minutes,
        *,
        customer=None,
        priority_score=50,
    ):
        customer_data = app_module.get_customer_by_row(
            self.spreadsheet,
            customer_row,
        )
        return {
            "row": customer_row,
            "customer_row": customer_row,
            "customer_id": customer_data["customer_id"],
            "customer_number": customer_data.get("customer_number", ""),
            "customer": customer or customer_data["customer"],
            "address": customer_data.get("Address", ""),
            "city": customer_data.get("City", ""),
            "latitude": float(customer_data["latitude_google"]),
            "longitude": float(customer_data["longitude_google"]),
            "sequence": sequence,
            "priority_score": priority_score,
            "required": False,
            "leg_drive_minutes": 1,
            "cumulative_drive_minutes": sequence,
            "cumulative_total_minutes": cumulative_total_minutes,
        }

    def route_payload(self, stops):
        return {
            "ok": True,
            "cached": False,
            "generated_at": NOW.isoformat(timespec="seconds"),
            "route_date": "2026-07-28",
            "route_owner": "Olle",
            "start": {"latitude": 57.7, "longitude": 11.9},
            "stops": stops,
            "summary": {
                "candidate_count": len(stops),
                "stop_count": len(stops),
                "total_priority_score": sum(
                    stop["priority_score"] for stop in stops
                ),
                "drive_minutes": len(stops),
                "return_drive_minutes": 1,
                "service_minutes": len(stops) * 20,
                "total_minutes": max(
                    (
                        stop["cumulative_total_minutes"]
                        for stop in stops
                    ),
                    default=0,
                ),
            },
            "meta": {"max_total_minutes": 420},
        }

    def create_preview(self, stops, *, route_date="2026-07-28"):
        payload = self.route_payload(stops)
        payload["route_date"] = route_date
        provider = ConstantRoadProvider(
            seconds=max(
                (int(stop.get("leg_drive_minutes") or 1) for stop in stops),
                default=1,
            ) * 60
        )
        with patch.object(
            app_module,
            "calculate_route_proposal_for_user",
            return_value=(payload, None),
        ) as calculate, patch.object(
            app_module,
            "get_route_travel_time_provider",
            return_value=provider,
        ):
            response = self.client.post(
                "/planning/route-preview",
                json={
                    "route_date": route_date,
                    "start": {"latitude": 57.7, "longitude": 11.9},
                    "candidate_rows": [
                        stop["row"] for stop in stops
                    ],
                },
            )
        return response, calculate

    def test_preview_requires_fixed_visit_and_reserves_phone_email_capacity(self):
        self.append_planning_row(
            planned_activity_id="fixed-phone",
            contact_type="phone",
            scheduled_at="2026-07-28T08:00:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="fixed-email",
            contact_type="email",
            scheduled_at="2026-07-28T08:30:00+02:00",
        )
        required = self.append_planning_row(
            planned_activity_id="required-zero-score",
            customer_row=4,
            contact_type="visit",
            scheduled_at="2026-07-28T10:30:00+02:00",
        )
        priorities = [
            {"row": 2, "customer": "Butik A", "priority_score": 0},
            {"row": 4, "customer": "Butik C", "priority_score": 0},
        ]
        provider = ConstantRoadProvider()

        with (
            patch.object(
                app_module,
                "build_current_priority_snapshot",
                return_value=(priorities, {}),
            ),
            patch.object(
                app_module,
                "get_route_travel_time_provider",
                return_value=provider,
            ),
        ):
            response = self.client.post(
                "/planning/route-preview",
                json={
                    "route_date": "2026-07-28",
                    "start": {"latitude": 57.7, "longitude": 11.9},
                    "candidate_rows": [2],
                },
            )

        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertEqual(body["route_start_at"], "2026-07-28T09:00+02:00")
        self.assertEqual(body["summary"]["non_route_minutes"], 20)
        self.assertEqual(
            body["route_payload"]["meta"]["max_total_minutes"],
            (app_module.MAX_TOTAL_SECONDS - 20 * 60) // 60,
        )
        self.assertEqual(len(body["stops"]), 1)
        stop = body["stops"][0]
        self.assertEqual(stop["customer_row"], 4)
        self.assertEqual(stop["priority_score"], 0)
        self.assertTrue(stop["required"])
        self.assertEqual(
            stop["planned_activity_id"],
            required["planned_activity_id"],
        )
        self.assertEqual(stop["scheduled_at"], "2026-07-28T10:30+02:00")
        self.assertEqual(stop["estimated_at"], "2026-07-28T10:30+02:00")
        self.assertFalse(body["conflicts"])
        self.assertTrue(body["preview_token"])

    def test_preview_schedules_long_drive_around_fixed_phone_interval(self):
        self.append_planning_row(
            planned_activity_id="fixed-phone-during-drive",
            contact_type="phone",
            scheduled_at="2026-07-28T10:00:00+02:00",
        )
        stop = self.route_stop(2, 1, 140)
        stop["leg_drive_minutes"] = 120

        response, _ = self.create_preview([stop])

        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        planned_stop = body["stops"][0]
        self.assertEqual(
            planned_stop["leg_departure_at"],
            "2026-07-28T10:10+02:00",
        )
        self.assertEqual(
            planned_stop["arrival_at"],
            "2026-07-28T12:10+02:00",
        )
        drive_segment = next(
            segment
            for segment in body["timeline"]["segments"]
            if segment["kind"] == "drive"
        )
        self.assertEqual(
            drive_segment["start"],
            "2026-07-28T10:10+02:00",
        )
        self.assertFalse(body["conflicts"])

    def test_preview_rejects_required_visit_missed_by_blocked_long_drive(self):
        self.append_planning_row(
            planned_activity_id="fixed-phone-before-required",
            contact_type="phone",
            scheduled_at="2026-07-28T10:00:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="required-after-long-drive",
            customer_row=2,
            scheduled_at="2026-07-28T10:00:00+02:00",
        )
        stop = self.route_stop(2, 1, 140, priority_score=0)
        stop["leg_drive_minutes"] = 120

        response, _ = self.create_preview([stop])

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "required_schedule_not_feasible",
        )

    def test_preview_rejects_duplicate_required_visits_for_same_customer(self):
        self.append_planning_row(
            planned_activity_id="duplicate-required-1",
            customer_row=2,
            scheduled_at="2026-07-28T10:00:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="duplicate-required-2",
            customer_row=2,
            scheduled_at="2026-07-28T14:00:00+02:00",
            source="follow_up",
        )

        response = self.client.post(
            "/planning/route-preview",
            json={
                "route_date": "2026-07-28",
                "start": {"latitude": 57.7, "longitude": 11.9},
                "candidate_rows": [2],
            },
        )

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "duplicate_required_customer",
        )

    def test_preview_validates_date_gps_candidates_and_permissions(self):
        cases = [
            (
                {
                    "route_date": "not-a-date",
                    "start": {"latitude": 57.7, "longitude": 11.9},
                },
                400,
                "invalid_route_date",
            ),
            (
                {
                    "route_date": "2026-07-26",
                    "start": {"latitude": 57.7, "longitude": 11.9},
                },
                409,
                "route_date_in_past",
            ),
            (
                {
                    "route_date": "2026-07-28",
                    "start": {"latitude": 999, "longitude": 11.9},
                },
                400,
                "invalid_start",
            ),
            (
                {
                    "route_date": "2026-07-28",
                    "start": {"latitude": 57.7, "longitude": 11.9},
                    "candidate_rows": [True],
                },
                400,
                "invalid_candidate_rows",
            ),
        ]
        for payload, status, error in cases:
            with self.subTest(error=error):
                response = self.client.post(
                    "/planning/route-preview",
                    json=payload,
                )
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.get_json()["error"], error)

        valid = {
            "route_date": "2026-07-28",
            "start": {"latitude": 57.7, "longitude": 11.9},
            "candidate_rows": [2],
        }
        other_calendar = self.client.post(
            "/planning/route-preview",
            json={**valid, "user_name": "sofia"},
        )
        self.login("viewer")
        non_sales = self.client.post(
            "/planning/route-preview",
            json=valid,
        )

        self.assertEqual(other_calendar.status_code, 403)
        self.assertEqual(non_sales.status_code, 403)

    def test_apply_is_idempotent_and_replaces_only_open_route_rows(self):
        required = self.append_planning_row(
            planned_activity_id="manual-required",
            customer_row=4,
            scheduled_at="2026-07-28T10:30:00+02:00",
        )
        old_open = self.append_planning_row(
            planned_activity_id="old-open-route",
            source="route",
            route_group_id="old-group",
            scheduled_at="2026-07-28T08:00:00+02:00",
            time_is_estimated=True,
        )
        old_completed = self.append_planning_row(
            planned_activity_id="old-completed-route",
            source="route",
            route_group_id="old-group",
            status="completed",
            completed_contact_id="old-contact",
            scheduled_at="2026-07-28T07:00:00+02:00",
            time_is_estimated=True,
        )
        stops = [
            self.route_stop(4, 1, 21, priority_score=0),
            self.route_stop(2, 2, 42),
        ]
        preview, _ = self.create_preview(stops)
        self.assertEqual(preview.status_code, 200, preview.get_json())
        token = preview.get_json()["preview_token"]
        request_payload = {
            "client_request_id": "apply-route-once",
            "preview_token": token,
        }

        first = self.client.post(
            "/planning/route-apply",
            json=request_payload,
        )
        second = self.client.post(
            "/planning/route-apply",
            json=request_payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        first_body = first.get_json()
        second_body = second.get_json()
        self.assertFalse(first_body["duplicate"])
        self.assertTrue(second_body["duplicate"])
        self.assertEqual(first_body["route_group_id"], second_body["route_group_id"])
        self.assertEqual(first_body["imported_count"], 1)
        self.assertEqual(second_body["imported_count"], 0)
        self.assertEqual(first_body["cancelled_route_activity_count"], 1)

        rows = {
            row["planned_activity_id"]: row for row in self.planning_rows()
        }
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[required["planned_activity_id"]]["status"], "planned")
        self.assertEqual(
            rows[required["planned_activity_id"]]["route_sequence"],
            2,
        )
        self.assertEqual(rows[old_open["planned_activity_id"]]["status"], "cancelled")
        self.assertEqual(
            rows[old_completed["planned_activity_id"]]["status"],
            "completed",
        )
        new_rows = [
            row
            for row in rows.values()
            if (
                row["source"] == "route"
                and row["status"] == "planned"
                and row["route_group_id"] == first_body["route_group_id"]
            )
        ]
        self.assertEqual(len(new_rows), 1)
        self.assertEqual(new_rows[0]["customer_row"], 2)
        proposals = self.spreadsheet.worksheet(
            app_module.ROUTE_PROPOSALS_SHEET
        ).dict_rows()
        self.assertEqual(len(proposals), 1)

    def test_apply_rejects_same_request_id_for_different_preview(self):
        first_preview, _ = self.create_preview([
            self.route_stop(2, 1, 21),
        ])
        second_preview, _ = self.create_preview([
            self.route_stop(4, 1, 21),
        ])
        request_id = "route-preview-conflict"

        first = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": request_id,
                "preview_token": first_preview.get_json()["preview_token"],
            },
        )
        conflicting = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": request_id,
                "preview_token": second_preview.get_json()["preview_token"],
            },
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(
            conflicting.status_code,
            409,
            conflicting.get_json(),
        )
        self.assertEqual(
            conflicting.get_json()["error"],
            "client_request_id_conflict",
        )
        route_rows = [
            row for row in self.planning_rows()
            if row["source"] == "route"
        ]
        self.assertEqual(len(route_rows), 1)
        self.assertEqual(route_rows[0]["customer_row"], 2)

    def test_apply_uses_customer_id_after_customer_row_insert(self):
        preview, _ = self.create_preview([
            self.route_stop(2, 1, 21),
        ])
        self.assertEqual(preview.status_code, 200, preview.get_json())
        customers = self.spreadsheet.worksheet("customers_enriched")
        headers = customers.values[0]
        inserted = {
            "customer": "Ny butik ovanför A",
            "customer_id": "44444444-4444-4444-8444-444444444444",
            "sales_person": "Olle",
            "customer_segment": "B",
            "customer_number": "C-4",
            "latitude_google": "57.6900",
            "longitude_google": "11.8900",
        }
        customers.values.insert(
            1,
            [inserted.get(header, "") for header in headers],
        )

        applied = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "row-insert-apply",
                "preview_token": preview.get_json()["preview_token"],
            },
        )

        self.assertEqual(applied.status_code, 200, applied.get_json())
        route_rows = [
            row for row in self.planning_rows()
            if row["source"] == "route"
        ]
        self.assertEqual(len(route_rows), 1)
        self.assertEqual(
            route_rows[0]["customer_id"],
            "11111111-1111-4111-8111-111111111111",
        )
        self.assertEqual(route_rows[0]["customer"], "Butik A")
        self.assertEqual(route_rows[0]["customer_row"], 3)

    def test_apply_uses_customer_id_after_customer_sheet_sort(self):
        preview, _ = self.create_preview([
            self.route_stop(2, 1, 21),
        ])
        customers = self.spreadsheet.worksheet("customers_enriched")
        customers.values[1:] = list(reversed(customers.values[1:]))

        applied = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "row-sort-apply",
                "preview_token": preview.get_json()["preview_token"],
            },
        )

        self.assertEqual(applied.status_code, 200, applied.get_json())
        route_row = next(
            row for row in self.planning_rows()
            if row["source"] == "route"
        )
        self.assertEqual(
            route_row["customer_id"],
            "11111111-1111-4111-8111-111111111111",
        )
        self.assertEqual(route_row["customer"], "Butik A")

    def test_apply_rejects_legacy_preview_without_customer_id(self):
        preview, _ = self.create_preview([
            self.route_stop(2, 1, 21),
        ])
        serializer = app_module.planning_preview_serializer()
        token_payload = serializer.loads(
            preview.get_json()["preview_token"]
        )
        for stop in token_payload["stops"]:
            stop.pop("customer_id", None)
        legacy_token = serializer.dumps(token_payload)

        response = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "legacy-row-only-preview",
                "preview_token": legacy_token,
            },
        )

        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "route_preview_expired_or_legacy",
        )
        self.assertFalse(any(
            row["source"] == "route" for row in self.planning_rows()
        ))

    def test_apply_never_falls_back_when_customer_id_disappears(self):
        preview, _ = self.create_preview([
            self.route_stop(2, 1, 21),
        ])
        serializer = app_module.planning_preview_serializer()
        token_payload = serializer.loads(
            preview.get_json()["preview_token"]
        )
        token_payload["stops"][0]["customer_id"] = (
            "99999999-9999-4999-8999-999999999999"
        )
        stale_token = serializer.dumps(token_payload)

        response = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "missing-id-no-row-fallback",
                "preview_token": stale_token,
            },
        )

        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "customer_identity_conflict",
        )
        self.assertFalse(any(
            row["source"] == "route" for row in self.planning_rows()
        ))

    def test_apply_rejects_tampered_expired_wrong_owner_and_stale_preview(self):
        preview, _ = self.create_preview([self.route_stop(2, 1, 21)])
        self.assertEqual(preview.status_code, 200, preview.get_json())
        token = preview.get_json()["preview_token"]

        tampered = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "tampered-route",
                "preview_token": token + "x",
            },
        )

        class ExpiredSerializer:
            def loads(self, _token, max_age=None):
                raise app_module.SignatureExpired("expired")

        with patch.object(
            app_module,
            "planning_preview_serializer",
            return_value=ExpiredSerializer(),
        ):
            expired = self.client.post(
                "/planning/route-apply",
                json={
                    "client_request_id": "expired-route",
                    "preview_token": token,
                },
            )

        self.login("sofia")
        wrong_owner = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "wrong-owner-route",
                "preview_token": token,
            },
        )
        self.login("olle")
        self.append_planning_row(
            planned_activity_id="changed-after-preview",
            scheduled_at="2026-07-28T12:00:00+02:00",
        )
        stale = self.client.post(
            "/planning/route-apply",
            json={
                "client_request_id": "stale-route",
                "preview_token": token,
            },
        )

        self.assertEqual(tampered.status_code, 400, tampered.get_json())
        self.assertEqual(tampered.get_json()["error"], "invalid_route_preview")
        self.assertEqual(expired.status_code, 409, expired.get_json())
        self.assertEqual(expired.get_json()["error"], "route_preview_expired")
        self.assertEqual(wrong_owner.status_code, 403, wrong_owner.get_json())
        self.assertEqual(stale.status_code, 409, stale.get_json())
        self.assertEqual(stale.get_json()["error"], "planning_changed")
        self.assertFalse(any(
            row["source"] == "route" for row in self.planning_rows()
        ))

    def test_legacy_daily_route_import_retry_does_not_duplicate_activities(self):
        saved = self.route_payload([self.route_stop(2, 1, 21)])
        saved["route_date"] = NOW.date().isoformat()
        app_module.save_route_proposal(
            self.spreadsheet,
            user_name="olle",
            user_display_name="Olle",
            route_date=NOW.date(),
            payload=saved,
        )
        request_payload = {"client_request_id": "legacy-import-once"}

        first = self.client.post(
            "/planning/route-import",
            json=request_payload,
        )
        second = self.client.post(
            "/planning/route-import",
            json=request_payload,
        )

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(second.status_code, 200, second.get_json())
        self.assertFalse(first.get_json()["duplicate"])
        self.assertTrue(second.get_json()["duplicate"])
        self.assertEqual(
            first.get_json()["route_group_id"],
            second.get_json()["route_group_id"],
        )
        self.assertEqual(len(self.planning_rows()), 1)
        self.assertEqual(self.planning_rows()[0]["source"], "route")
        proposal_payloads = [
            json.loads(row["payload_json"])
            for row in self.spreadsheet.worksheet(
                app_module.ROUTE_PROPOSALS_SHEET
            ).dict_rows()
        ]
        matching_groups = [
            payload
            for payload in proposal_payloads
            if payload.get("route_group_id")
            == first.get_json()["route_group_id"]
        ]
        self.assertEqual(len(matching_groups), 1)

    def test_legacy_daily_route_import_rejects_missing_required_visit(self):
        required = self.append_planning_row(
            planned_activity_id="required-missing-from-cache",
            customer_row=4,
            scheduled_at="2026-07-27T13:00:00+02:00",
        )
        saved = self.route_payload([self.route_stop(2, 1, 21)])
        saved["route_date"] = NOW.date().isoformat()
        app_module.save_route_proposal(
            self.spreadsheet,
            user_name="olle",
            user_display_name="Olle",
            route_date=NOW.date(),
            payload=saved,
        )

        response = self.client.post(
            "/planning/route-import",
            json={"client_request_id": "legacy-import-missing-required"},
        )

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "required_stops_missing_from_daily_route",
        )
        rows = self.planning_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["planned_activity_id"],
            required["planned_activity_id"],
        )
        self.assertFalse(any(row["source"] == "route" for row in rows))

    def test_legacy_daily_route_import_rejects_full_day_with_fixed_activity(self):
        fixed_phone = self.append_planning_row(
            planned_activity_id="fixed-phone-at-capacity",
            contact_type="phone",
            scheduled_at="2026-07-27T13:00:00+02:00",
        )
        saved = self.route_payload([self.route_stop(2, 1, 410)])
        saved["route_date"] = NOW.date().isoformat()
        saved["summary"]["total_minutes"] = 410
        app_module.save_route_proposal(
            self.spreadsheet,
            user_name="olle",
            user_display_name="Olle",
            route_date=NOW.date(),
            payload=saved,
        )

        response = self.client.post(
            "/planning/route-import",
            json={"client_request_id": "legacy-import-over-capacity"},
        )

        self.assertEqual(response.status_code, 422, response.get_json())
        self.assertEqual(
            response.get_json()["error"],
            "day_capacity_exhausted",
        )
        rows = self.planning_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["planned_activity_id"], fixed_phone["planned_activity_id"])
        self.assertFalse(any(row["source"] == "route" for row in rows))


if __name__ == "__main__":
    main()
