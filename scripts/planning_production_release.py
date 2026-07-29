"""Fail-closed production release command for Sales planning data.

Examples:

    APP_ENV=production
    PRODUCTION_SHEET_KEY=<production>
    python scripts/planning_production_release.py \
      --confirm-production \
      --backup-id <verified-backup-id> \
      --repair-master-row 2090 \
      --report outputs/planning-production-dry-run.json

    python scripts/planning_production_release.py \
      --confirm-production \
      --backup-id <verified-backup-id> \
      --repair-master-row 2090 \
      --replacement-uuid <uuid-from-dry-run> \
      --apply \
      --report outputs/planning-production-apply.json
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import sys
import uuid

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

from planning_release_migration import (
    CUSTOMER_SHEET,
    IDENTITY_SHEETS,
    PLANNED_ACTIVITY_COLUMNS,
    PLANNED_SHEET,
    SCOPES,
    analyze_migration,
    dict_rows,
    masked_sheet_key,
    valid_uuid,
    worksheet_values,
)


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def production_sheet_key(environ=None):
    environment = os.environ if environ is None else environ
    if str(environment.get("APP_ENV") or "").strip().casefold() != "production":
        raise RuntimeError(
            "Production release requires APP_ENV=production."
        )
    production_key = str(
        environment.get("PRODUCTION_SHEET_KEY") or ""
    ).strip()
    if not production_key:
        raise RuntimeError(
            "PRODUCTION_SHEET_KEY is required; no staging or SHEET_KEY "
            "fallback is allowed."
        )
    return production_key


def validated_uuid4(value):
    try:
        parsed = uuid.UUID(str(value or "").strip())
    except (ValueError, AttributeError, TypeError):
        return ""
    if parsed.version != 4:
        return ""
    return str(parsed)


def _customer_details(row_number, row):
    address = " ".join(filter(None, [
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
    ]))
    return {
        "row": row_number,
        "customer": str(
            row.get("customer")
            or row.get("Customer")
            or ""
        ).strip(),
        "customer_number": str(
            row.get("customer_number")
            or row.get("Customer number")
            or ""
        ).strip(),
        "address": address,
        "city": str(
            row.get("city_google")
            or row.get("City")
            or row.get("city")
            or ""
        ).strip(),
        "current_customer_id": str(
            row.get("customer_id") or ""
        ).strip(),
    }


def _without_repaired_reference_errors(
    blocking_errors,
    repaired_rows_by_sheet,
    repair_master_row,
):
    remaining = []
    for issue in blocking_errors:
        code = issue.get("code")
        if (
            code == "invalid_master_customer_id"
            and issue.get("rows") == [repair_master_row]
        ):
            continue
        if code == "invalid_customer_id":
            repaired_rows = repaired_rows_by_sheet.get(
                issue.get("worksheet"),
                set(),
            )
            unresolved_rows = [
                row for row in issue.get("rows", [])
                if row not in repaired_rows
            ]
            if unresolved_rows:
                remaining.append({
                    **issue,
                    "rows": unresolved_rows,
                })
            continue
        remaining.append(issue)
    return remaining


def build_production_plan(
    spreadsheet,
    *,
    repair_master_row=None,
    replacement_uuid=None,
):
    report = analyze_migration(
        spreadsheet,
        legacy_unresolved_are_warnings=True,
    )
    blocking_errors = list(report["blocking_errors"])
    if (
        report["planned_activities_missing"]
        or report["planned_schema"] != PLANNED_ACTIVITY_COLUMNS
        or report["planned_schema_rewrite"]
    ):
        blocking_errors.append({
            "code": "unexpected_planned_schema",
            "expected_columns": PLANNED_ACTIVITY_COLUMNS,
            "actual_columns": report["planned_schema"],
        })

    changes_by_row = {}
    for update in report["updates"]:
        changes_by_row[
            (update["worksheet"], int(update["row"]))
        ] = {
            **update,
            "expected_customer_id": "",
            "reason": "safe_legacy_backfill",
        }

    master_values = worksheet_values(spreadsheet, CUSTOMER_SHEET)
    _master_headers, master_rows = dict_rows(master_values)
    repair = None
    repaired_rows_by_sheet = defaultdict(set)
    new_uuid_count = 0

    if repair_master_row is not None:
        matches = [
            (row_number, row)
            for row_number, row in master_rows
            if row_number == int(repair_master_row)
        ]
        if len(matches) != 1:
            blocking_errors.append({
                "code": "repair_master_row_not_found",
                "row": repair_master_row,
            })
        else:
            row_number, master_row = matches[0]
            repair = _customer_details(row_number, master_row)
            old_id = repair["current_customer_id"]
            repair["exact_reference_occurrences"] = []
            if not old_id:
                blocking_errors.append({
                    "code": "repair_master_id_is_empty",
                    "row": row_number,
                })
            elif valid_uuid(old_id):
                repair["status"] = "already_valid"
                repair["replacement_customer_id"] = old_id
            else:
                requested_uuid = (
                    validated_uuid4(replacement_uuid)
                    if replacement_uuid else str(uuid.uuid4())
                )
                if replacement_uuid and not requested_uuid:
                    blocking_errors.append({
                        "code": "replacement_uuid_must_be_uuid4",
                    })
                existing_master_ids = {
                    str(row.get("customer_id") or "").strip()
                    for _index, row in master_rows
                }
                if requested_uuid in existing_master_ids:
                    blocking_errors.append({
                        "code": "replacement_uuid_collision",
                    })
                repair.update({
                    "status": "planned_repair",
                    "replacement_customer_id": requested_uuid,
                })
                new_uuid_count = 1

                for title in IDENTITY_SHEETS:
                    values = worksheet_values(spreadsheet, title)
                    if not values:
                        continue
                    _headers, rows = dict_rows(values)
                    for reference_row, row in rows:
                        if str(
                            row.get("customer_id") or ""
                        ).strip() != old_id:
                            continue
                        repair["exact_reference_occurrences"].append({
                            "worksheet": title,
                            "row": reference_row,
                        })
                        repaired_rows_by_sheet[title].add(reference_row)
                        changes_by_row[(title, reference_row)] = {
                            "worksheet": title,
                            "row": reference_row,
                            "customer_id": requested_uuid,
                            "expected_customer_id": old_id,
                            "reason": "exact_old_customer_id",
                        }

                for key, change in list(changes_by_row.items()):
                    if change["customer_id"] == old_id:
                        changes_by_row[key] = {
                            **change,
                            "customer_id": requested_uuid,
                            "reason": (
                                "safe_legacy_backfill_to_repaired_master"
                            ),
                        }
                changes_by_row[(CUSTOMER_SHEET, row_number)] = {
                    "worksheet": CUSTOMER_SHEET,
                    "row": row_number,
                    "customer_id": requested_uuid,
                    "expected_customer_id": old_id,
                    "reason": "repair_invalid_master_uuid",
                }
                blocking_errors = _without_repaired_reference_errors(
                    blocking_errors,
                    repaired_rows_by_sheet,
                    row_number,
                )

    changes = sorted(
        changes_by_row.values(),
        key=lambda item: (item["worksheet"], item["row"]),
    )
    return {
        "mode": "production",
        "dry_run": True,
        "master": {
            "customer_count": report["customer_count"],
            "empty_customer_id": report["empty_master_customer_id"],
            "invalid_customer_id": report["invalid_master_customer_id"],
            "duplicate_customer_id": report[
                "duplicate_master_customer_id"
            ],
        },
        "planned_activities": {
            "missing": report["planned_activities_missing"],
            "schema": report["planned_schema"],
            "schema_exact": (
                report["planned_schema"] == PLANNED_ACTIVITY_COLUMNS
            ),
            "duplicate_activity_id": report[
                "duplicate_planned_activity_id"
            ],
            "unresolved_identity": [
                issue
                for issue in blocking_errors
                if (
                    issue.get("code")
                    == "unresolved_customer_identity"
                    and issue.get("worksheet") == PLANNED_SHEET
                )
            ],
        },
        "repair": repair,
        "changes": changes,
        "change_count": len(changes),
        "new_uuid_count": new_uuid_count,
        "changes_by_worksheet": {
            worksheet: len([
                item for item in changes
                if item["worksheet"] == worksheet
            ])
            for worksheet in sorted({
                item["worksheet"] for item in changes
            })
        },
        "legacy_warnings": report["legacy_warnings"],
        "blocking_errors": blocking_errors,
        "safe_to_apply": not blocking_errors,
        "idempotent": not changes and not blocking_errors,
    }


def _quoted_sheet_name(title):
    return "'" + str(title).replace("'", "''") + "'"


def apply_production_plan(spreadsheet, plan):
    if plan["blocking_errors"]:
        raise RuntimeError(
            "Production release blocked: "
            + json.dumps(
                plan["blocking_errors"],
                ensure_ascii=False,
            )
        )
    if not plan["changes"]:
        return {
            **plan,
            "dry_run": False,
            "write_request_count": 0,
            "written_cell_count": 0,
            "idempotent": True,
        }

    values_by_sheet = {}
    headers_by_sheet = {}
    data = []
    for change in plan["changes"]:
        title = change["worksheet"]
        if title not in values_by_sheet:
            values = worksheet_values(spreadsheet, title)
            headers, _rows = dict_rows(values)
            if "customer_id" not in headers:
                raise RuntimeError(
                    f"{title} is missing customer_id before write."
                )
            values_by_sheet[title] = values
            headers_by_sheet[title] = headers
        values = values_by_sheet[title]
        headers = headers_by_sheet[title]
        row_number = int(change["row"])
        column_number = headers.index("customer_id") + 1
        current_value = ""
        if row_number <= len(values):
            row_values = values[row_number - 1]
            if column_number <= len(row_values):
                current_value = str(
                    row_values[column_number - 1] or ""
                ).strip()
        if current_value != change["expected_customer_id"]:
            raise RuntimeError(
                "Production changed after dry-run at "
                f"{title} row {row_number}; no writes performed."
            )
        cell = rowcol_to_a1(row_number, column_number)
        data.append({
            "range": f"{_quoted_sheet_name(title)}!{cell}",
            "values": [[change["customer_id"]]],
        })

    spreadsheet.values_batch_update({
        "valueInputOption": "RAW",
        "data": data,
    })
    return {
        **plan,
        "dry_run": False,
        "write_request_count": 1,
        "written_cell_count": len(data),
        "idempotent": False,
    }


def verify_backup(production, backup, production_key, backup_id):
    if str(production_key).strip() == str(backup_id).strip():
        raise RuntimeError(
            "Backup ID must not equal the production Sheet ID."
        )
    production_sheets = {
        sheet.title: (sheet.row_count, sheet.col_count)
        for sheet in production.worksheets()
    }
    backup_sheets = {
        sheet.title: (sheet.row_count, sheet.col_count)
        for sheet in backup.worksheets()
    }
    if production_sheets != backup_sheets:
        raise RuntimeError(
            "Backup worksheet structure does not match production."
        )
    return {
        "verified": True,
        "backup_title": backup.title,
        "backup_sheet_id": backup_id,
        "worksheet_count": len(backup_sheets),
        "worksheet_structure_matches": True,
    }


def open_production_and_backup(environ, backup_id):
    production_key = production_sheet_key(environ)
    credentials = Credentials.from_service_account_info(
        json.loads(environ["GOOGLE_CREDENTIALS"]),
        scopes=SCOPES,
    )
    client = gspread.authorize(credentials)
    production = client.open_by_key(production_key)
    backup = client.open_by_key(backup_id)
    verification = verify_backup(
        production,
        backup,
        production_key,
        backup_id,
    )
    return production, production_key, verification


def save_report(path, report):
    report_path = Path(path).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report_path


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Fail-closed Sales planning production release."
    )
    parser.add_argument("--confirm-production", action="store_true")
    parser.add_argument("--backup-id", required=True)
    parser.add_argument("--repair-master-row", type=int)
    parser.add_argument("--replacement-uuid")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", required=True)
    args = parser.parse_args(argv)

    if not args.confirm_production:
        raise RuntimeError(
            "--confirm-production is required for every production run."
        )
    production, production_key, backup = open_production_and_backup(
        os.environ,
        args.backup_id,
    )
    plan = build_production_plan(
        production,
        repair_master_row=args.repair_master_row,
        replacement_uuid=args.replacement_uuid,
    )
    if (
        args.apply
        and plan["new_uuid_count"]
        and not args.replacement_uuid
    ):
        raise RuntimeError(
            "Apply requires --replacement-uuid from the reviewed dry-run."
        )

    result = (
        apply_production_plan(production, plan)
        if args.apply else plan
    )
    if args.apply:
        verification_plan = build_production_plan(
            production,
            repair_master_row=args.repair_master_row,
            replacement_uuid=args.replacement_uuid,
        )
        result["post_apply"] = verification_plan
        result["idempotent"] = (
            not verification_plan["blocking_errors"]
            and verification_plan["change_count"] == 0
            and verification_plan["new_uuid_count"] == 0
        )
    result.update({
        "production_sheet_id": masked_sheet_key(production_key),
        "backup": backup,
        "report_version": 1,
    })
    report_path = save_report(args.report, result)
    print(json.dumps({
        "ok": not result["blocking_errors"],
        "mode": "apply" if args.apply else "dry_run",
        "production_sheet_id": result["production_sheet_id"],
        "backup_sheet_id": args.backup_id,
        "safe_to_apply": result.get("safe_to_apply", False),
        "change_count": result["change_count"],
        "changes_by_worksheet": result["changes_by_worksheet"],
        "new_uuid_count": result["new_uuid_count"],
        "idempotent": result["idempotent"],
        "blocking_errors": result["blocking_errors"],
        "legacy_warning_count": len(result["legacy_warnings"]),
        "report": str(report_path),
    }, ensure_ascii=False, indent=2))
    return 0 if not result["blocking_errors"] else 2


if __name__ == "__main__":
    sys.exit(main())
