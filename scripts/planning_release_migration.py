"""Dry-run and apply the Sales planning schema/identity migration on staging.

The command is deliberately staging-only and fail-closed:

    APP_ENV=staging
    STAGING_SHEET_KEY=<copy>
    PRODUCTION_SHEET_KEY=<production>
    python scripts/planning_release_migration.py
    python scripts/planning_release_migration.py --apply
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import sys
import unicodedata
import uuid

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from gspread.utils import rowcol_to_a1


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CUSTOMER_SHEET = "customers_enriched"
PLANNED_SHEET = "planned_activities"
PLANNED_ACTIVITY_COLUMNS = [
    "planned_activity_id",
    "user_name",
    "sales_person",
    "customer_id",
    "customer_key",
    "customer_row",
    "customer_number",
    "customer",
    "contact_type",
    "scheduled_at",
    "duration_minutes",
    "time_is_estimated",
    "note",
    "status",
    "source",
    "source_contact_id",
    "completed_contact_id",
    "route_group_id",
    "route_sequence",
    "client_request_id",
    "create_fingerprint",
    "last_mutation_request_id",
    "last_mutation_fingerprint",
    "revision",
    "created_at",
    "updated_at",
]
IDENTITY_SHEETS = (
    "contacts",
    "sales_activities",
    "email_messages",
    "email_recipients",
    "order_rows",
    PLANNED_SHEET,
)


def normalized(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join(
        "".join(
            char for char in text if not unicodedata.combining(char)
        ).strip().casefold().split()
    )


def valid_uuid(value):
    try:
        parsed = uuid.UUID(str(value or "").strip())
    except (ValueError, AttributeError, TypeError):
        return False
    return str(parsed) == str(value or "").strip().lower()


def staging_sheet_key(environ=None):
    environment = os.environ if environ is None else environ
    app_env = str(environment.get("APP_ENV") or "").strip().casefold()
    if app_env != "staging":
        raise RuntimeError(
            "Planning release migration requires APP_ENV=staging."
        )
    staging_key = str(
        environment.get("STAGING_SHEET_KEY") or ""
    ).strip()
    production_key = str(
        environment.get("PRODUCTION_SHEET_KEY")
        or environment.get("SHEET_KEY")
        or ""
    ).strip()
    if not staging_key:
        raise RuntimeError(
            "STAGING_SHEET_KEY is required; no production fallback is allowed."
        )
    if production_key and staging_key == production_key:
        raise RuntimeError(
            "STAGING_SHEET_KEY must not equal the production Sheet key."
        )
    return staging_key


def worksheet_values(spreadsheet, title):
    try:
        return spreadsheet.worksheet(title).get_all_values()
    except WorksheetNotFound:
        return None


def dict_rows(values):
    if not values:
        return [], []
    headers = [str(value or "").strip() for value in values[0]]
    rows = []
    for row_number, values_row in enumerate(values[1:], start=2):
        padded = list(values_row) + [""] * (
            len(headers) - len(values_row)
        )
        rows.append((
            row_number,
            {
                header: padded[index]
                for index, header in enumerate(headers)
                if header
            },
        ))
    return headers, rows


def customer_location(row):
    address = normalized(" ".join(filter(None, [
        str(
            row.get("address_google")
            or row.get("Address")
            or row.get("address")
            or ""
        ).strip(),
        str(
            row.get("address_number_google")
            or row.get("Number")
            or row.get("address_number")
            or ""
        ).strip(),
    ])))
    city = normalized(
        row.get("city_google")
        or row.get("City")
        or row.get("city")
    )
    return address, city


def build_customer_indexes(customer_rows):
    by_id = defaultdict(list)
    by_number = defaultdict(list)
    by_name = defaultdict(list)
    for row_number, row in customer_rows:
        customer = {
            **row,
            "_row_number": row_number,
        }
        customer_id = str(row.get("customer_id") or "").strip()
        if customer_id:
            by_id[customer_id].append(customer)
        customer_number = normalized(
            row.get("customer_number")
            or row.get("Customer number")
        )
        if customer_number:
            by_number[customer_number].append(customer)
        customer_name = normalized(
            row.get("customer")
            or row.get("Customer")
        )
        if customer_name:
            by_name[customer_name].append(customer)
    return by_id, by_number, by_name


def resolve_legacy_customer(row, by_number, by_name):
    number = normalized(
        row.get("customer_number")
        or row.get("Customer number")
    )
    if number:
        matches = by_number.get(number, [])
        return matches[0] if len(matches) == 1 else None

    name = normalized(
        row.get("customer")
        or row.get("Customer")
    )
    address, city = customer_location(row)
    if not name or (not address and not city):
        return None
    matches = []
    for customer in by_name.get(name, []):
        customer_address, customer_city = customer_location(customer)
        if (
            (not address or customer_address == address)
            and (not city or customer_city == city)
        ):
            matches.append(customer)
    return matches[0] if len(matches) == 1 else None


def analyze_migration(
    spreadsheet,
    *,
    legacy_unresolved_are_warnings=False,
):
    customer_values = worksheet_values(spreadsheet, CUSTOMER_SHEET)
    if not customer_values:
        raise RuntimeError("customers_enriched is missing or empty.")
    customer_headers, customer_rows = dict_rows(customer_values)
    if "customer_id" not in customer_headers:
        raise RuntimeError(
            "customers_enriched is missing the customer_id column."
        )
    by_id, by_number, by_name = build_customer_indexes(customer_rows)
    master_ids = set(by_id)
    blocking = []
    legacy_warnings = []
    empty_master_ids = []
    invalid_master_ids = []
    duplicate_master_ids = []
    for row_number, row in customer_rows:
        customer_id = str(row.get("customer_id") or "").strip()
        if not customer_id:
            empty_master_ids.append(row_number)
        elif not valid_uuid(customer_id):
            invalid_master_ids.append(row_number)
    for customer_id, matches in by_id.items():
        if len(matches) > 1:
            duplicate_master_ids.append({
                "customer_id": customer_id,
                "rows": [
                    customer["_row_number"] for customer in matches
                ],
            })
    if empty_master_ids:
        blocking.append({
            "code": "empty_master_customer_id",
            "rows": empty_master_ids,
        })
    if invalid_master_ids:
        blocking.append({
            "code": "invalid_master_customer_id",
            "rows": invalid_master_ids,
        })
    if duplicate_master_ids:
        blocking.append({
            "code": "duplicate_master_customer_id",
            "items": duplicate_master_ids,
        })

    worksheet_reports = {}
    updates = []
    identity_values = {
        title: worksheet_values(spreadsheet, title)
        for title in IDENTITY_SHEETS
    }
    planned_values = identity_values[PLANNED_SHEET]
    planned_missing = planned_values is None
    planned_schema_rewrite = False
    if planned_values is not None:
        planned_headers, planned_rows = dict_rows(planned_values)
        duplicate_headers = sorted({
            header for header in planned_headers
            if header and planned_headers.count(header) > 1
        })
        extra_headers = [
            header for header in planned_headers
            if header and header not in PLANNED_ACTIVITY_COLUMNS
        ]
        if duplicate_headers or extra_headers:
            blocking.append({
                "code": "unsafe_planned_schema",
                "duplicate_headers": duplicate_headers,
                "extra_headers": extra_headers,
            })
        planned_schema_rewrite = (
            planned_headers != PLANNED_ACTIVITY_COLUMNS
            and not duplicate_headers
            and not extra_headers
        )
    else:
        planned_headers, planned_rows = [], []

    email_customer_ids = defaultdict(set)
    email_message_values = identity_values.get("email_messages")
    if email_message_values:
        _email_headers, email_message_rows = dict_rows(
            email_message_values
        )
        for _row_number, row in email_message_rows:
            email_id = str(row.get("email_id") or "").strip()
            if not email_id:
                continue
            customer_id = str(row.get("customer_id") or "").strip()
            if customer_id in master_ids:
                email_customer_ids[email_id].add(customer_id)
                continue
            customer = resolve_legacy_customer(
                row,
                by_number,
                by_name,
            )
            if customer:
                email_customer_ids[email_id].add(
                    customer["customer_id"]
                )

    for title in IDENTITY_SHEETS:
        values = identity_values[title]
        if values is None:
            worksheet_reports[title] = {
                "exists": False,
                "row_count": 0,
                "blank_customer_id": 0,
                "orphan_customer_id": 0,
                "proposed_backfills": 0,
            }
            continue
        headers, rows = dict_rows(values)
        if "customer_id" not in headers:
            if rows:
                blocking.append({
                    "code": "missing_customer_id_column",
                    "worksheet": title,
                })
            worksheet_reports[title] = {
                "exists": True,
                "row_count": len(rows),
                "blank_customer_id": len(rows),
                "orphan_customer_id": 0,
                "proposed_backfills": 0,
            }
            continue
        blank_count = 0
        orphan_count = 0
        proposed = 0
        unresolved_rows = []
        invalid_rows = []
        orphan_rows = []
        for row_number, row in rows:
            customer_id = str(row.get("customer_id") or "").strip()
            customer_name = str(
                row.get("customer")
                or row.get("Customer")
                or ""
            ).strip()
            if customer_id:
                if not valid_uuid(customer_id):
                    invalid_rows.append(row_number)
                elif customer_id not in master_ids:
                    orphan_count += 1
                    orphan_rows.append(row_number)
                continue
            if title == "email_recipients":
                inherited_ids = email_customer_ids.get(
                    str(row.get("email_id") or "").strip(),
                    set(),
                )
                if len(inherited_ids) == 1:
                    blank_count += 1
                    proposed += 1
                    updates.append({
                        "worksheet": title,
                        "row": row_number,
                        "customer_id": next(iter(inherited_ids)),
                    })
                    continue
            if not customer_name:
                continue
            blank_count += 1
            customer = resolve_legacy_customer(
                row,
                by_number,
                by_name,
            )
            if customer:
                proposed += 1
                updates.append({
                    "worksheet": title,
                    "row": row_number,
                    "customer_id": customer["customer_id"],
                })
            else:
                unresolved_rows.append(row_number)
        if invalid_rows:
            blocking.append({
                "code": "invalid_customer_id",
                "worksheet": title,
                "rows": invalid_rows,
            })
        if orphan_rows:
            blocking.append({
                "code": "orphan_customer_id",
                "worksheet": title,
                "rows": orphan_rows,
            })
        if unresolved_rows:
            issue = {
                "code": "unresolved_customer_identity",
                "worksheet": title,
                "rows": unresolved_rows,
            }
            if (
                legacy_unresolved_are_warnings
                and title != PLANNED_SHEET
            ):
                legacy_warnings.append(issue)
            else:
                blocking.append(issue)
        worksheet_reports[title] = {
            "exists": True,
            "row_count": len(rows),
            "blank_customer_id": blank_count,
            "orphan_customer_id": orphan_count,
            "proposed_backfills": proposed,
        }

    duplicate_planned_activity_ids = []
    if planned_rows:
        activity_ids = defaultdict(list)
        for row_number, row in planned_rows:
            activity_id = str(
                row.get("planned_activity_id") or ""
            ).strip()
            if activity_id:
                activity_ids[activity_id].append(row_number)
        duplicate_planned_activity_ids = [
            {"planned_activity_id": value, "rows": rows}
            for value, rows in activity_ids.items()
            if len(rows) > 1
        ]
        if duplicate_planned_activity_ids:
            blocking.append({
                "code": "duplicate_planned_activity_id",
                "items": duplicate_planned_activity_ids,
            })

    return {
        "customer_count": len(customer_rows),
        "empty_master_customer_id": len(empty_master_ids),
        "invalid_master_customer_id": len(invalid_master_ids),
        "duplicate_master_customer_id": len(duplicate_master_ids),
        "unique_master_customer_id": len(master_ids),
        "planned_activities_missing": planned_missing,
        "planned_schema": planned_headers,
        "planned_schema_rewrite": planned_schema_rewrite,
        "duplicate_planned_activity_id": len(
            duplicate_planned_activity_ids
        ),
        "worksheets": worksheet_reports,
        "updates": updates,
        "blocking_errors": blocking,
        "legacy_warnings": legacy_warnings,
    }


def rewrite_planned_schema(sheet):
    values = sheet.get_all_values()
    old_headers, old_rows = dict_rows(values)
    rewritten = [PLANNED_ACTIVITY_COLUMNS]
    for _row_number, row in old_rows:
        rewritten.append([
            row.get(header, "")
            for header in PLANNED_ACTIVITY_COLUMNS
        ])
    if values:
        end = rowcol_to_a1(
            max(1, len(values)),
            max(1, len(old_headers)),
        )
        sheet.batch_clear([f"A1:{end}"])
    sheet.resize(
        rows=max(sheet.row_count, len(rewritten) + 20),
        cols=max(sheet.col_count, len(PLANNED_ACTIVITY_COLUMNS)),
    )
    sheet.update(
        rewritten,
        range_name=(
            f"A1:{rowcol_to_a1(len(rewritten), len(PLANNED_ACTIVITY_COLUMNS))}"
        ),
        value_input_option="RAW",
    )


def apply_migration(spreadsheet):
    report = analyze_migration(spreadsheet)
    if report["blocking_errors"]:
        raise RuntimeError(
            "Migration blocked: "
            + json.dumps(
                report["blocking_errors"],
                ensure_ascii=False,
            )
        )
    writes = 0
    if report["planned_activities_missing"]:
        sheet = spreadsheet.add_worksheet(
            title=PLANNED_SHEET,
            rows=2000,
            cols=len(PLANNED_ACTIVITY_COLUMNS),
        )
        sheet.append_row(
            PLANNED_ACTIVITY_COLUMNS,
            value_input_option="RAW",
        )
        writes += 1
    elif report["planned_schema_rewrite"]:
        rewrite_planned_schema(spreadsheet.worksheet(PLANNED_SHEET))
        writes += 1

    updates_by_sheet = defaultdict(list)
    for update in report["updates"]:
        updates_by_sheet[update["worksheet"]].append(update)
    for title, updates in updates_by_sheet.items():
        sheet = spreadsheet.worksheet(title)
        headers = [
            str(value or "").strip()
            for value in sheet.row_values(1)
        ]
        customer_id_column = headers.index("customer_id") + 1
        data = [
            {
                "range": (
                    f"{rowcol_to_a1(update['row'], customer_id_column)}:"
                    f"{rowcol_to_a1(update['row'], customer_id_column)}"
                ),
                "values": [[update["customer_id"]]],
            }
            for update in updates
        ]
        if data:
            sheet.batch_update(data, value_input_option="RAW")
            writes += 1

    result = analyze_migration(spreadsheet)
    result["apply_write_batches"] = writes
    result["idempotent"] = (
        not result["updates"]
        and not result["planned_activities_missing"]
        and not result["planned_schema_rewrite"]
    )
    return result


def open_staging_spreadsheet(environ=None):
    environment = os.environ if environ is None else environ
    sheet_key = staging_sheet_key(environment)
    credentials = Credentials.from_service_account_info(
        json.loads(environment["GOOGLE_CREDENTIALS"]),
        scopes=SCOPES,
    )
    return gspread.authorize(credentials).open_by_key(sheet_key), sheet_key


def masked_sheet_key(sheet_key):
    value = str(sheet_key or "")
    return f"…{value[-6:]}" if len(value) > 6 else "…"


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Validate or apply the Sales planning staging migration."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply safe backfills/schema creation to staging.",
    )
    args = parser.parse_args(argv)
    spreadsheet, sheet_key = open_staging_spreadsheet()
    report = (
        apply_migration(spreadsheet)
        if args.apply
        else analyze_migration(spreadsheet)
    )
    report["mode"] = "apply" if args.apply else "dry_run"
    report["environment"] = "staging"
    report["sheet_key"] = masked_sheet_key(sheet_key)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not report["blocking_errors"] else 2


if __name__ == "__main__":
    sys.exit(main())
