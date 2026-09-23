"""One-time planned-activity reconciliation. Dry-run unless --apply is supplied.

The sheet key and credentials come from the selected env file. Apply requires
an explicit confirmation of the configured sheet key and is safe to rerun.
"""

import argparse
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-sheet-key", default="")
    args = parser.parse_args()

    if not args.env_file.is_file():
        parser.error(f"Environment file not found: {args.env_file}")
    load_dotenv(args.env_file, override=True)
    sys.path.insert(0, str(ROOT / "web-app"))
    import app

    if not app.SHEET_ID or not os.environ.get("GOOGLE_CREDENTIALS"):
        parser.error("Configured sheet key and Google credentials are required")
    if args.apply and args.confirm_sheet_key != app.SHEET_ID:
        parser.error("--apply requires --confirm-sheet-key matching the selected sheet")

    spreadsheet = app.get_spreadsheet_with_retry()
    contact_sheet = app.get_worksheet(spreadsheet, "sales_activities")
    _headers, contacts = app.worksheet_snapshot(
        contact_sheet,
        expected_columns=app.CONTACT_COLUMNS,
        required_columns=app.CONTACT_REQUIRED_COLUMNS,
    )
    contact_rows = [row for _index, row in contacts]

    with app.planning_write_lock():
        sheet, headers, activities = app.read_planned_activity_snapshot(spreadsheet)
        if sheet is None or "status" not in headers or "customer_id" not in headers:
            parser.error("planned_activities is missing required columns")
        candidates = app.superseded_planned_activity_candidates(
            activities, contact_rows
        )
        ids = [str(row.get("planned_activity_id") or "").strip()
               for _index, row in candidates]
        if not all(ids) or len(ids) != len(set(ids)):
            parser.error("Blank or duplicate candidate activity IDs require review")
        if args.apply:
            changed = app.reconcile_superseded_planned_activities(
                spreadsheet, contact_rows, apply=True
            )
            if changed != ids:
                raise RuntimeError("Planning rows changed during reconciliation")
            _sheet, _headers, after = app.read_planned_activity_snapshot(spreadsheet)
            statuses = {
                str(row.get("planned_activity_id") or "").strip():
                    str(row.get("status") or "").strip().casefold()
                for _index, row in after
            }
            if any(statuses.get(activity_id) != "superseded" for activity_id in ids):
                raise RuntimeError("Some updated activities were not read back as superseded")
            remaining = app.superseded_planned_activity_candidates(
                after, contact_rows
            )
            if remaining:
                raise RuntimeError("Reconciliation left eligible planned activities")

    print(json.dumps({
        "mode": "apply" if args.apply else "dry_run",
        "eligible_count": len(ids),
        "planned_activity_ids": ids,
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
