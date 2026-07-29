from __future__ import annotations

import importlib.util
from pathlib import Path
import re
import sys
from unittest import TestCase, main


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "planning_release_migration",
    ROOT / "scripts" / "planning_release_migration.py",
)
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)
PRODUCTION_SPEC = importlib.util.spec_from_file_location(
    "planning_production_release",
    ROOT / "scripts" / "planning_production_release.py",
)
production = importlib.util.module_from_spec(PRODUCTION_SPEC)
PRODUCTION_SPEC.loader.exec_module(production)


def column_number(label):
    value = 0
    for character in label:
        value = value * 26 + ord(character.upper()) - ord("A") + 1
    return value


class FakeWorksheet:
    def __init__(self, title, headers=(), rows=()):
        self.title = title
        self.values = [list(headers)] if headers else []
        self.values.extend([
            [row.get(header, "") for header in headers]
            if isinstance(row, dict)
            else list(row)
            for row in rows
        ])
        self.row_count = max(100, len(self.values) + 20)
        self.col_count = max(10, len(headers))
        self.batch_update_count = 0

    def get_all_values(self):
        return [list(row) for row in self.values]

    def row_values(self, row_number):
        if 0 < row_number <= len(self.values):
            return list(self.values[row_number - 1])
        return []

    def append_row(self, values, value_input_option=None):
        self.values.append(list(values))

    def batch_clear(self, ranges):
        for range_name in ranges:
            _start, end = range_name.split(":")
            match = re.fullmatch(r"([A-Z]+)(\d+)", end)
            max_column = column_number(match.group(1))
            max_row = int(match.group(2))
            for row_index in range(min(max_row, len(self.values))):
                self.values[row_index][:max_column] = [""] * min(
                    max_column,
                    len(self.values[row_index]),
                )

    def resize(self, rows=None, cols=None):
        if rows is not None:
            self.row_count = max(self.row_count, int(rows))
        if cols is not None:
            self.col_count = max(self.col_count, int(cols))

    def update(self, values, range_name=None, value_input_option=None):
        self._write_range(range_name, values)

    def batch_update(self, data, value_input_option=None):
        self.batch_update_count += 1
        for item in data:
            self._write_range(item["range"], item["values"])

    def _write_range(self, range_name, values):
        start = str(range_name).split(":", 1)[0]
        match = re.fullmatch(r"([A-Z]+)(\d+)", start)
        start_column = column_number(match.group(1))
        start_row = int(match.group(2))
        for row_offset, values_row in enumerate(values):
            row_number = start_row + row_offset
            while len(self.values) < row_number:
                self.values.append([])
            row = self.values[row_number - 1]
            for column_offset, value in enumerate(values_row):
                target_column = start_column + column_offset
                while len(row) < target_column:
                    row.append("")
                row[target_column - 1] = value


class FakeSpreadsheet:
    def __init__(self, worksheets):
        self.worksheets = {
            worksheet.title: worksheet
            for worksheet in worksheets
        }
        self.values_batch_update_count = 0

    def worksheet(self, title):
        if title not in self.worksheets:
            raise migration.WorksheetNotFound(title)
        return self.worksheets[title]

    def add_worksheet(self, title, rows, cols):
        worksheet = FakeWorksheet(title)
        worksheet.row_count = int(rows)
        worksheet.col_count = int(cols)
        self.worksheets[title] = worksheet
        return worksheet

    def values_batch_update(self, body):
        self.values_batch_update_count += 1
        for item in body["data"]:
            sheet_part, cell_range = item["range"].split("!", 1)
            title = sheet_part.strip("'").replace("''", "'")
            self.worksheet(title)._write_range(
                cell_range,
                item["values"],
            )


MASTER_HEADERS = [
    "customer",
    "customer_id",
    "customer_number",
    "Address",
    "City",
]
CUSTOMER_A_ID = "11111111-1111-4111-8111-111111111111"
CUSTOMER_B_ID = "22222222-2222-4222-8222-222222222222"


def migration_spreadsheet(*, unresolved=False, planned_headers=None):
    customers = FakeWorksheet(
        migration.CUSTOMER_SHEET,
        MASTER_HEADERS,
        [
            {
                "customer": "Butik A",
                "customer_id": CUSTOMER_A_ID,
                "customer_number": "100",
                "Address": "Storgatan 1",
                "City": "Göteborg",
            },
            {
                "customer": "Butik B",
                "customer_id": CUSTOMER_B_ID,
                "customer_number": "200",
                "Address": "Kungsgatan 2",
                "City": "Malmö",
            },
        ],
    )
    contacts = FakeWorksheet(
        "contacts",
        [
            "contact_id",
            "customer_id",
            "customer_number",
            "customer",
            "Address",
            "City",
        ],
        [{
            "contact_id": "contact-1",
            "customer_id": "",
            "customer_number": "" if unresolved else "100",
            "customer": "Okänd butik" if unresolved else "Butik A",
            "Address": "" if unresolved else "Storgatan 1",
            "City": "" if unresolved else "Göteborg",
        }],
    )
    email_messages = FakeWorksheet(
        "email_messages",
        [
            "email_id",
            "customer_id",
            "customer_number",
            "customer",
        ],
        [{
            "email_id": "email-1",
            "customer_id": "",
            "customer_number": "200",
            "customer": "Butik B",
        }],
    )
    email_recipients = FakeWorksheet(
        "email_recipients",
        ["email_id", "customer_id", "customer"],
        [{
            "email_id": "email-1",
            "customer_id": "",
            "customer": "Butik B",
        }],
    )
    worksheets = [
        customers,
        contacts,
        email_messages,
        email_recipients,
    ]
    if planned_headers is not None:
        worksheets.append(
            FakeWorksheet(
                migration.PLANNED_SHEET,
                planned_headers,
                [{
                    "planned_activity_id": "activity-1",
                    "customer_id": CUSTOMER_B_ID,
                    "customer": "Butik B",
                }],
            )
        )
    return FakeSpreadsheet(worksheets)


class PlanningReleaseMigrationTests(TestCase):
    def test_staging_key_is_mandatory_and_never_production(self):
        with self.assertRaises(RuntimeError):
            migration.staging_sheet_key({
                "APP_ENV": "production",
                "STAGING_SHEET_KEY": "staging",
            })
        with self.assertRaises(RuntimeError):
            migration.staging_sheet_key({
                "APP_ENV": "staging",
                "SHEET_KEY": "production",
            })
        with self.assertRaises(RuntimeError):
            migration.staging_sheet_key({
                "APP_ENV": "staging",
                "SHEET_KEY": "same",
                "STAGING_SHEET_KEY": "same",
            })

    def test_apply_backfills_identity_creates_schema_and_is_idempotent(self):
        spreadsheet = migration_spreadsheet()

        dry_run = migration.analyze_migration(spreadsheet)
        first_apply = migration.apply_migration(spreadsheet)
        second_apply = migration.apply_migration(spreadsheet)

        self.assertEqual(dry_run["blocking_errors"], [])
        self.assertTrue(dry_run["planned_activities_missing"])
        self.assertEqual(
            dry_run["worksheets"]["contacts"]["proposed_backfills"],
            1,
        )
        self.assertEqual(first_apply["blocking_errors"], [])
        self.assertEqual(first_apply["apply_write_batches"], 4)
        self.assertTrue(first_apply["idempotent"])
        self.assertEqual(second_apply["apply_write_batches"], 0)
        self.assertTrue(second_apply["idempotent"])
        contacts = spreadsheet.worksheet("contacts").get_all_values()
        customer_id_column = contacts[0].index("customer_id")
        self.assertEqual(contacts[1][customer_id_column], CUSTOMER_A_ID)
        self.assertEqual(
            spreadsheet.worksheet(
                migration.PLANNED_SHEET
            ).row_values(1),
            migration.PLANNED_ACTIVITY_COLUMNS,
        )
        recipient_values = spreadsheet.worksheet(
            "email_recipients"
        ).get_all_values()
        recipient_id_column = recipient_values[0].index("customer_id")
        self.assertEqual(
            recipient_values[1][recipient_id_column],
            CUSTOMER_B_ID,
        )

    def test_apply_reorders_safe_planned_schema_without_losing_values(self):
        spreadsheet = migration_spreadsheet(
            planned_headers=[
                "customer",
                "planned_activity_id",
                "customer_id",
            ]
        )

        result = migration.apply_migration(spreadsheet)
        values = spreadsheet.worksheet(
            migration.PLANNED_SHEET
        ).get_all_values()
        nonblank_rows = [
            row for row in values[1:]
            if any(str(value).strip() for value in row)
        ]

        self.assertEqual(result["blocking_errors"], [])
        self.assertEqual(values[0], migration.PLANNED_ACTIVITY_COLUMNS)
        self.assertEqual(len(nonblank_rows), 1)
        self.assertEqual(
            nonblank_rows[0][
                migration.PLANNED_ACTIVITY_COLUMNS.index("customer_id")
            ],
            CUSTOMER_B_ID,
        )

    def test_unresolved_identity_blocks_before_any_write(self):
        spreadsheet = migration_spreadsheet(unresolved=True)

        report = migration.analyze_migration(spreadsheet)
        with self.assertRaises(RuntimeError):
            migration.apply_migration(spreadsheet)

        self.assertIn(
            "unresolved_customer_identity",
            {
                item["code"]
                for item in report["blocking_errors"]
            },
        )
        self.assertNotIn(
            migration.PLANNED_SHEET,
            spreadsheet.worksheets,
        )
        self.assertEqual(
            spreadsheet.worksheet("contacts").batch_update_count,
            0,
        )


class PlanningProductionReleaseTests(TestCase):
    replacement_uuid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    def test_production_key_has_no_fallback(self):
        with self.assertRaises(RuntimeError):
            production.production_sheet_key({
                "APP_ENV": "staging",
                "PRODUCTION_SHEET_KEY": "production",
            })
        with self.assertRaises(RuntimeError):
            production.production_sheet_key({
                "APP_ENV": "production",
                "SHEET_KEY": "legacy-fallback",
            })
        self.assertEqual(
            production.production_sheet_key({
                "APP_ENV": "production",
                "PRODUCTION_SHEET_KEY": "production",
            }),
            "production",
        )

    def test_targeted_repair_and_safe_backfills_are_idempotent(self):
        spreadsheet = migration_spreadsheet(
            unresolved=True,
            planned_headers=migration.PLANNED_ACTIVITY_COLUMNS,
        )
        customer_sheet = spreadsheet.worksheet(
            migration.CUSTOMER_SHEET
        )
        customer_id_column = customer_sheet.row_values(1).index(
            "customer_id"
        )
        customer_sheet.values[1][customer_id_column] = "legacy-invalid-id"

        plan = production.build_production_plan(
            spreadsheet,
            repair_master_row=2,
            replacement_uuid=self.replacement_uuid,
        )
        applied = production.apply_production_plan(
            spreadsheet,
            plan,
        )
        second = production.build_production_plan(
            spreadsheet,
            repair_master_row=2,
            replacement_uuid=self.replacement_uuid,
        )

        self.assertEqual(plan["blocking_errors"], [])
        self.assertTrue(plan["safe_to_apply"])
        self.assertEqual(plan["new_uuid_count"], 1)
        self.assertEqual(
            plan["repair"]["current_customer_id"],
            "legacy-invalid-id",
        )
        self.assertEqual(
            plan["repair"]["replacement_customer_id"],
            self.replacement_uuid,
        )
        self.assertTrue(plan["legacy_warnings"])
        self.assertEqual(
            applied["written_cell_count"],
            plan["change_count"],
        )
        self.assertEqual(spreadsheet.values_batch_update_count, 1)
        self.assertEqual(second["blocking_errors"], [])
        self.assertEqual(second["change_count"], 0)
        self.assertEqual(second["new_uuid_count"], 0)
        self.assertTrue(second["idempotent"])
        self.assertEqual(
            customer_sheet.values[1][customer_id_column],
            self.replacement_uuid,
        )

    def test_nonempty_orphan_reference_still_blocks(self):
        spreadsheet = migration_spreadsheet(
            planned_headers=migration.PLANNED_ACTIVITY_COLUMNS
        )
        contacts = spreadsheet.worksheet("contacts")
        customer_id_column = contacts.row_values(1).index("customer_id")
        contacts.values[1][customer_id_column] = (
            "99999999-9999-4999-8999-999999999999"
        )

        plan = production.build_production_plan(spreadsheet)

        self.assertIn(
            "orphan_customer_id",
            {
                issue["code"]
                for issue in plan["blocking_errors"]
            },
        )
        with self.assertRaises(RuntimeError):
            production.apply_production_plan(spreadsheet, plan)
        self.assertEqual(spreadsheet.values_batch_update_count, 0)

    def test_unresolved_planned_identity_and_schema_are_blocking(self):
        spreadsheet = migration_spreadsheet(
            planned_headers=migration.PLANNED_ACTIVITY_COLUMNS
        )
        planned = spreadsheet.worksheet(migration.PLANNED_SHEET)
        customer_id_column = planned.row_values(1).index("customer_id")
        customer_column = planned.row_values(1).index("customer")
        planned.values[1][customer_id_column] = ""
        planned.values[1][customer_column] = "Okänd planerad kund"

        identity_plan = production.build_production_plan(spreadsheet)
        bad_schema = migration_spreadsheet(
            planned_headers=[
                "planned_activity_id",
                "customer_id",
                "customer",
            ]
        )
        schema_plan = production.build_production_plan(bad_schema)

        self.assertIn(
            "unresolved_customer_identity",
            {
                issue["code"]
                for issue in identity_plan["blocking_errors"]
            },
        )
        self.assertIn(
            "unexpected_planned_schema",
            {
                issue["code"]
                for issue in schema_plan["blocking_errors"]
            },
        )

    def test_apply_stops_if_a_target_cell_changed_after_dry_run(self):
        spreadsheet = migration_spreadsheet(
            planned_headers=migration.PLANNED_ACTIVITY_COLUMNS
        )
        plan = production.build_production_plan(spreadsheet)
        email_messages = spreadsheet.worksheet("email_messages")
        customer_id_column = email_messages.row_values(1).index(
            "customer_id"
        )
        email_messages.values[1][customer_id_column] = CUSTOMER_A_ID

        with self.assertRaises(RuntimeError):
            production.apply_production_plan(spreadsheet, plan)

        self.assertEqual(spreadsheet.values_batch_update_count, 0)


if __name__ == "__main__":
    main()
