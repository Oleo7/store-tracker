"""Dry-run/apply deterministic customer IDs for historical CRM email rows."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1


ROOT = Path(__file__).resolve().parents[1]
WEB_APP = ROOT / "web-app"
sys.path.insert(0, str(WEB_APP))

from legacy_email_identity import plan_legacy_email_identity_backfill
from sheets_availability import read_with_retry


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEETS = (
    "customers_enriched",
    "email_messages",
    "email_recipients",
    "sales_activities",
)


def indexed_rows(values):
    if not values:
        return [], []
    headers = [str(value or "").strip() for value in values[0]]
    rows = []
    for row_index, values_row in enumerate(values[1:], start=2):
        padded = list(values_row) + [""] * (len(headers) - len(values_row))
        rows.append((row_index, dict(zip(headers, padded))))
    return headers, rows


def load_production_spreadsheet():
    load_dotenv(ROOT / ".env")
    if str(os.environ.get("APP_ENV") or "").strip().casefold() != "production":
        raise RuntimeError("APP_ENV=production is required for this backfill.")
    sheet_key = str(os.environ.get("PRODUCTION_SHEET_KEY") or "").strip()
    if not sheet_key:
        raise RuntimeError("PRODUCTION_SHEET_KEY is required.")
    credentials = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    client = gspread.authorize(
        Credentials.from_service_account_info(credentials, scopes=SCOPES)
    )
    return read_with_retry(lambda: client.open_by_key(sheet_key))


def read_sources(spreadsheet):
    worksheets = {}
    sources = {}
    for title in SHEETS:
        sheet = read_with_retry(lambda title=title: spreadsheet.worksheet(title))
        values = read_with_retry(sheet.get_all_values)
        headers, rows = indexed_rows(values)
        if "customer_id" not in headers:
            raise RuntimeError(f"{title} is missing customer_id.")
        worksheets[title] = sheet
        sources[title] = (headers, rows)
    return worksheets, sources


def public_report(plan):
    return {
        sheet: {
            key: value for key, value in result.items() if key != "updates"
        }
        for sheet, result in plan.items()
        if sheet != "totals"
    } | {"totals": plan["totals"]}


def apply_plan(worksheets, sources, plan):
    for plan_key, sheet_title in (
        ("email_messages", "email_messages"),
        ("email_recipients", "email_recipients"),
        ("sales_activities", "sales_activities"),
    ):
        headers = sources[sheet_title][0]
        column = headers.index("customer_id") + 1
        updates = [{
            "range": (
                f"{rowcol_to_a1(item['row_index'], column)}:"
                f"{rowcol_to_a1(item['row_index'], column)}"
            ),
            "values": [[item["customer_id"]]],
        } for item in plan[plan_key]["updates"]]
        if updates:
            # Deliberately no automatic retry: a partially accepted write must be
            # inspected by rerunning the idempotent dry-run.
            worksheets[sheet_title].batch_update(
                updates, value_input_option="RAW"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-production", action="store_true")
    args = parser.parse_args()
    if args.apply and not args.confirm_production:
        raise RuntimeError("--apply requires --confirm-production.")

    spreadsheet = load_production_spreadsheet()
    worksheets, sources = read_sources(spreadsheet)
    plan = plan_legacy_email_identity_backfill(
        customers=[row for _index, row in sources["customers_enriched"][1]],
        message_rows=sources["email_messages"][1],
        recipient_rows=sources["email_recipients"][1],
        activity_rows=sources["sales_activities"][1],
    )
    print(json.dumps(public_report(plan), ensure_ascii=False, sort_keys=True))
    if args.apply:
        apply_plan(worksheets, sources, plan)
        print(json.dumps({"applied": plan["totals"]["backfilled"]}))


if __name__ == "__main__":
    main()
