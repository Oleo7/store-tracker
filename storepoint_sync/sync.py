from __future__ import annotations

import argparse
import calendar
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SOURCE_SHEET_KEY = "1SL7mYtrgMmUdtvt6eykg4OOuefRtpRrUurvwfu_Jdck"
TARGET_SHEET_KEY = "1pl0h9oiKOn0kUvrCrPk4ftiBpKKNBFkl1rOtCUuc5m4"
SOURCE_WORKSHEET = "order_rows"
TARGET_WORKSHEET = "storepoint_template_49b0fd29731a"
TIMEZONE = "Europe/Stockholm"
SOURCE_COLUMNS = ["Delivery date", "Customer", "Address", "Number", "Postal code", "City"]
TARGET_COLUMNS = ["name", "address", "city", "postcode"]


@dataclass(frozen=True)
class StorepointConfig:
    source_sheet_key: str
    target_sheet_key: str
    google_credentials: dict[str, Any]
    source_worksheet: str = SOURCE_WORKSHEET
    target_worksheet: str = TARGET_WORKSHEET
    timezone_name: str = TIMEZONE

    @classmethod
    def from_env(cls, args: argparse.Namespace | None = None) -> "StorepointConfig":
        load_dotenv()
        args = args or argparse.Namespace()
        google_credentials_raw = os.environ.get("GOOGLE_CREDENTIALS", "").strip()
        source_sheet_key = (
            getattr(args, "source_sheet_key", None)
            or os.environ.get("STOREPOINT_SOURCE_SHEET_KEY", "").strip()
            or os.environ.get("SHEET_KEY", "").strip()
            or SOURCE_SHEET_KEY
        )
        target_sheet_key = (
            getattr(args, "target_sheet_key", None)
            or os.environ.get("STOREPOINT_TARGET_SHEET_KEY", "").strip()
            or TARGET_SHEET_KEY
        )
        source_worksheet = getattr(args, "source_worksheet", None) or SOURCE_WORKSHEET
        target_worksheet = getattr(args, "target_worksheet", None) or TARGET_WORKSHEET
        timezone_name = getattr(args, "timezone", None) or TIMEZONE

        missing = []
        if not google_credentials_raw:
            missing.append("GOOGLE_CREDENTIALS")
        if not source_sheet_key:
            missing.append("SHEET_KEY")
        if not target_sheet_key:
            missing.append("STOREPOINT_TARGET_SHEET_KEY")
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        try:
            google_credentials = json.loads(google_credentials_raw)
        except json.JSONDecodeError as exc:
            raise ValueError("GOOGLE_CREDENTIALS must be valid JSON") from exc

        return cls(
            source_sheet_key=source_sheet_key,
            target_sheet_key=target_sheet_key,
            google_credentials=google_credentials,
            source_worksheet=source_worksheet,
            target_worksheet=target_worksheet,
            timezone_name=timezone_name,
        )


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date


@dataclass(frozen=True)
class StorepointRow:
    name: str
    address: str
    city: str
    postcode: str

    def as_list(self) -> list[str]:
        return [self.name, self.address, self.city, self.postcode]


@dataclass(frozen=True)
class StorepointResult:
    dry_run: bool
    source_worksheet: str
    target_worksheet: str
    date_window: DateWindow
    source_rows: int
    filtered_rows: int
    unique_rows: int
    stale_rows: int


def open_client(config: StorepointConfig):
    credentials = Credentials.from_service_account_info(config.google_credentials, scopes=SCOPES)
    return gspread.authorize(credentials)


def sync_storepoint_customers(
    config: StorepointConfig,
    dry_run: bool = False,
    today: date | None = None,
    months_back: int = 3,
    include_future_days: int = 0,
    client=None,
) -> StorepointResult:
    client = client or open_client(config)
    source_spreadsheet = client.open_by_key(config.source_sheet_key)
    target_spreadsheet = client.open_by_key(config.target_sheet_key)
    source_sheet = source_spreadsheet.worksheet(config.source_worksheet)
    target_sheet = target_spreadsheet.worksheet(config.target_worksheet)

    today = today or today_in_timezone(config.timezone_name)
    window = DateWindow(start=subtract_months(today, months_back), end=today + timedelta(days=include_future_days))

    source_values = source_sheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    headers, source_rows = values_to_dicts(source_values)
    require_columns(headers, SOURCE_COLUMNS, config.source_worksheet)
    output_rows, filtered_rows = build_storepoint_rows(source_rows, window)

    target_values = target_sheet.get_all_values()
    target_headers = target_values[0] if target_values else []
    target_indexes = require_columns(target_headers, TARGET_COLUMNS, config.target_worksheet)
    stale_rows = count_stale_target_rows(target_values, target_indexes, len(output_rows))

    if not dry_run:
        write_target_rows(target_sheet, target_indexes, output_rows)
        verify_target_rows(target_sheet, target_indexes, output_rows)

    return StorepointResult(
        dry_run=dry_run,
        source_worksheet=config.source_worksheet,
        target_worksheet=config.target_worksheet,
        date_window=window,
        source_rows=len(source_rows),
        filtered_rows=filtered_rows,
        unique_rows=len(output_rows),
        stale_rows=stale_rows,
    )


def build_storepoint_rows(rows: Iterable[dict[str, Any]], window: DateWindow) -> tuple[list[StorepointRow], int]:
    output_rows: list[StorepointRow] = []
    seen: set[tuple[str, str, str, str]] = set()
    filtered_rows = 0

    for row in rows:
        delivery_date = parse_delivery_date(row.get("Delivery date"))
        if delivery_date is None or delivery_date < window.start or delivery_date > window.end:
            continue

        name = text(row.get("Customer"))
        if not name:
            continue

        filtered_rows += 1
        output = StorepointRow(
            name=name,
            address=join_address(row.get("Address"), row.get("Number")),
            city=text(row.get("City")),
            postcode=text(row.get("Postal code")),
        )
        key = normalized_tuple(output.as_list())
        if key in seen:
            continue
        seen.add(key)
        output_rows.append(output)

    return output_rows, filtered_rows


def write_target_rows(target_sheet, target_indexes: dict[str, int], output_rows: list[StorepointRow]) -> None:
    ensure_target_rows(target_sheet, len(output_rows) + 1)

    for start_index, end_index in contiguous_index_groups(target_indexes[column] for column in TARGET_COLUMNS):
        target_sheet.batch_clear([open_ended_range(start_index, end_index)])

    if not output_rows:
        return

    rows_by_target_column = {
        target_indexes["name"]: [[row.name] for row in output_rows],
        target_indexes["address"]: [[row.address] for row in output_rows],
        target_indexes["city"]: [[row.city] for row in output_rows],
        target_indexes["postcode"]: [[row.postcode] for row in output_rows],
    }
    for start_index, end_index in contiguous_index_groups(rows_by_target_column.keys()):
        values = []
        for row_number in range(len(output_rows)):
            values.append(
                [
                    rows_by_target_column[column_index][row_number][0]
                    for column_index in range(start_index, end_index + 1)
                ]
            )
        target_sheet.update(
            values=values,
            range_name=bounded_range(start_index, end_index, len(output_rows)),
            raw=True,
        )


def verify_target_rows(target_sheet, target_indexes: dict[str, int], expected_rows: list[StorepointRow]) -> None:
    values = target_sheet.get_all_values()
    actual_rows = extract_target_rows(values, target_indexes, len(expected_rows))
    expected_values = [row.as_list() for row in expected_rows]
    if actual_rows != expected_values:
        for index, (actual, expected) in enumerate(zip_longest(actual_rows, expected_values), start=2):
            if actual != expected:
                raise ValueError(f"Verification failed at target row {index}: expected {expected}, got {actual}")

    if count_stale_target_rows(values, target_indexes, len(expected_rows)):
        raise ValueError("Verification failed: stale target values remain below the expected output.")


def extract_target_rows(values: list[list[Any]], target_indexes: dict[str, int], row_count: int) -> list[list[str]]:
    rows = []
    for row in values[1 : row_count + 1]:
        rows.append([text(cell_at(row, target_indexes[column])) for column in TARGET_COLUMNS])
    while len(rows) < row_count:
        rows.append(["", "", "", ""])
    return rows


def count_stale_target_rows(values: list[list[Any]], target_indexes: dict[str, int], expected_rows: int) -> int:
    stale_rows = 0
    for row in values[expected_rows + 1 :]:
        if any(text(cell_at(row, target_indexes[column])) for column in TARGET_COLUMNS):
            stale_rows += 1
    return stale_rows


def values_to_dicts(values: list[list[Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    if not values:
        return [], []

    headers = [text(header) for header in values[0]]
    rows = []
    for value_row in values[1:]:
        row = {}
        for index, header in enumerate(headers):
            if header:
                row[header] = value_row[index] if index < len(value_row) else ""
        rows.append(row)
    return headers, rows


def require_columns(headers: list[Any], required_columns: list[str], sheet_name: str) -> dict[str, int]:
    normalized_headers = {normalize_header(header): index for index, header in enumerate(headers)}
    missing = [column for column in required_columns if normalize_header(column) not in normalized_headers]
    if missing:
        raise ValueError(f"Missing columns in {sheet_name}: {', '.join(missing)}")
    return {column: normalized_headers[normalize_header(column)] for column in required_columns}


def parse_delivery_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return google_serial_date(value)

    value_text = text(value)
    if not value_text:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", value_text):
        return google_serial_date(float(value_text))

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value_text[:19], fmt).date()
        except ValueError:
            continue

    normalized = value_text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def google_serial_date(value: float) -> date:
    return date(1899, 12, 30) + timedelta(days=int(value))


def today_in_timezone(timezone_name: str) -> date:
    try:
        return datetime.now(ZoneInfo(timezone_name)).date()
    except ZoneInfoNotFoundError:
        return datetime.now().date()


def subtract_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 - months
    year = month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def join_address(address: Any, number: Any) -> str:
    return " ".join(part for part in [text(address), text(number)] if part)


def normalize_header(value: Any) -> str:
    return " ".join(text(value).casefold().split())


def normalized_tuple(values: list[str]) -> tuple[str, str, str, str]:
    return tuple(" ".join(value.casefold().split()) for value in values)


def text(value: Any) -> str:
    return str(value or "").replace("\xa0", " ").strip()


def cell_at(row: list[Any], index: int) -> Any:
    return row[index] if index < len(row) else ""


def ensure_target_rows(target_sheet, required_rows: int) -> None:
    current_rows = getattr(target_sheet, "row_count", required_rows)
    if current_rows < required_rows:
        target_sheet.add_rows(required_rows - current_rows)


def contiguous_index_groups(indexes: Iterable[int]) -> list[tuple[int, int]]:
    groups: list[tuple[int, int]] = []
    for index in sorted(indexes):
        if not groups or index > groups[-1][1] + 1:
            groups.append((index, index))
        else:
            groups[-1] = (groups[-1][0], index)
    return groups


def open_ended_range(start_index: int, end_index: int) -> str:
    start = column_name(start_index)
    end = column_name(end_index)
    if start == end:
        return f"{start}2:{start}"
    return f"{start}2:{end}"


def bounded_range(start_index: int, end_index: int, row_count: int) -> str:
    start = column_name(start_index)
    end = column_name(end_index)
    last_row = row_count + 1
    if start == end:
        return f"{start}2:{start}{last_row}"
    return f"{start}2:{end}{last_row}"


def column_name(zero_based_index: int) -> str:
    number = zero_based_index + 1
    name = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def zip_longest(left: list[list[str]], right: list[list[str]]):
    max_length = max(len(left), len(right))
    for index in range(max_length):
        yield left[index] if index < len(left) else None, right[index] if index < len(right) else None


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync recent CRM_DATABASE order customers into the Storepoint template.")
    parser.add_argument("--dry-run", action="store_true", help="Build and verify the output shape, but do not write.")
    parser.add_argument("--today", help="Date to treat as today in YYYY-MM-DD format. Defaults to Europe/Stockholm today.")
    parser.add_argument("--months-back", type=int, default=3, help="Calendar months to include before today. Defaults to 3.")
    parser.add_argument("--include-future-days", type=int, default=0, help="Optional future delivery days to include. Defaults to 0.")
    parser.add_argument("--source-sheet-key", help="CRM_DATABASE spreadsheet id. Defaults to SHEET_KEY or the known CRM_DATABASE id.")
    parser.add_argument("--target-sheet-key", help="Storepoint spreadsheet id. Defaults to STOREPOINT_TARGET_SHEET_KEY or the known target id.")
    parser.add_argument("--source-worksheet", default=SOURCE_WORKSHEET)
    parser.add_argument("--target-worksheet", default=TARGET_WORKSHEET)
    parser.add_argument("--timezone", default=TIMEZONE)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        today = parse_delivery_date(args.today) if args.today else None
        if args.today and today is None:
            raise ValueError("--today must be a date in YYYY-MM-DD format")
        config = StorepointConfig.from_env(args)
        result = sync_storepoint_customers(
            config,
            dry_run=args.dry_run,
            today=today,
            months_back=args.months_back,
            include_future_days=args.include_future_days,
        )
    except Exception as exc:
        print(f"Storepoint sync failed: {exc}", file=sys.stderr)
        return 1

    action = "Dry run" if result.dry_run else "Sync"
    print(f"{action} completed.")
    print(f"Source worksheet: {result.source_worksheet}")
    print(f"Target worksheet: {result.target_worksheet}")
    print(f"Date window: {result.date_window.start.isoformat()} to {result.date_window.end.isoformat()}")
    print(f"Source rows: {result.source_rows}")
    print(f"Filtered rows: {result.filtered_rows}")
    print(f"Unique Storepoint rows: {result.unique_rows}")
    print(f"Stale target rows before sync: {result.stale_rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
