from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


ORDER_COLUMNS = [
    "Reference",
    "Order date",
    "Delivery date",
    "Customer",
    "placedBy",
    "buyerEmail",
    "placedAs",
    "Customer Reference",
    "Buyer number",
    "Customer number",
    "Logistics number",
    "Address",
    "Number",
    "Postal code",
    "City",
    "Country",
    "Phone number",
    "SKU",
    "Product",
    "Weight",
    "Quantity",
    "Total weight",
    "Unit",
    "Total (Pre-discount)",
    "Product Discount",
    "Total",
    "Currency",
    "Order Discount (Amount)",
    "Order Discount (%)",
    "Batch",
]

CUSTOMER_EMAIL_COLUMN = "email_last_order"
CUSTOMER_EMAIL_LEFT_COLUMN = "email"
CUSTOMER_EMAIL_RIGHT_COLUMN = "city_google"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
STATE_SHEET_NAME = "_stockfiller_sync_state"
PRODUCTION_BASE_URL = "https://api.stockfiller.com/v1"
SANDBOX_BASE_URL = "https://public-api.staging.stockfillertech.com/v1"
DEFAULT_LOOKBACK_HOURS = 48
DEFAULT_TIMEOUT_SECONDS = 30
BACKFILL_CHUNK_DAYS = 31
NUMERIC_COLUMNS = {
    "Weight",
    "Quantity",
    "Total weight",
    "Total (Pre-discount)",
    "Product Discount",
    "Total",
    "Order Discount (Amount)",
    "Order Discount (%)",
}


@dataclass(frozen=True)
class SyncConfig:
    base_url: str
    api_token: str
    supplier_identifier: str
    supplier_id: str
    sheet_key: str
    google_credentials: dict[str, Any]
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS

    @classmethod
    def from_env(cls) -> "SyncConfig":
        load_dotenv()
        api_token = os.environ.get("STOCKFILLER_API_TOKEN", "").strip()
        supplier_identifier = os.environ.get("STOCKFILLER_SUPPLIER_IDENTIFIER", "supplierGln").strip()
        supplier_id = os.environ.get("STOCKFILLER_SUPPLIER_ID", "").strip()
        sheet_key = os.environ.get("SHEET_KEY", "").strip()
        google_credentials_raw = os.environ.get("GOOGLE_CREDENTIALS", "").strip()

        missing = []
        if not api_token:
            missing.append("STOCKFILLER_API_TOKEN")
        if not supplier_id:
            missing.append("STOCKFILLER_SUPPLIER_ID")
        if not sheet_key:
            missing.append("SHEET_KEY")
        if not google_credentials_raw:
            missing.append("GOOGLE_CREDENTIALS")
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        valid_identifiers = {"supplierGln", "supplierExternalId", "supplierInternalId"}
        if supplier_identifier not in valid_identifiers:
            options = ", ".join(sorted(valid_identifiers))
            raise ValueError(f"STOCKFILLER_SUPPLIER_IDENTIFIER must be one of: {options}")

        try:
            google_credentials = json.loads(google_credentials_raw)
        except json.JSONDecodeError as exc:
            raise ValueError("GOOGLE_CREDENTIALS must be valid JSON") from exc

        timeout_seconds = int(os.environ.get("STOCKFILLER_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))

        return cls(
            base_url=resolve_stockfiller_base_url(),
            api_token=api_token,
            supplier_identifier=supplier_identifier,
            supplier_id=supplier_id,
            sheet_key=sheet_key,
            google_credentials=google_credentials,
            timeout_seconds=timeout_seconds,
        )


@dataclass(frozen=True)
class SyncWindow:
    mode: str
    params: dict[str, str]
    stop_value: str | None


@dataclass(frozen=True)
class SyncResult:
    dry_run: bool
    mode: str
    target_worksheet: str
    fetched_orders: int
    output_rows: int
    replace_references: int
    existing_rows: int
    kept_existing_rows: int
    final_rows: int
    stop_value: str | None


class StockfillerClient:
    def __init__(self, config: SyncConfig, session: requests.Session | None = None):
        self.config = config
        self.session = session or requests.Session()

    def iter_orders(self, params: dict[str, str]) -> Iterable[dict[str, Any]]:
        page = 1
        while True:
            request_params = {
                self.config.supplier_identifier: self.config.supplier_id,
                "page": str(page),
                **params,
            }
            response = self.session.get(
                f"{self.config.base_url}/supplier/order",
                headers={"Authorization": f"Bearer {self.config.api_token}"},
                params=request_params,
                timeout=self.config.timeout_seconds,
            )
            if response.status_code == 404:
                return
            response.raise_for_status()

            payload = response.json()
            data = payload.get("data") or []
            if not isinstance(data, list):
                raise ValueError("Unexpected Stockfiller response: 'data' is not a list")

            for order in data:
                yield order

            meta = payload.get("meta") or {}
            returned = int(meta.get("returned") or len(data))
            page_size = int(meta.get("pageSize") or 100)
            if not data or returned < page_size:
                return
            page += 1


def resolve_stockfiller_base_url() -> str:
    explicit_base_url = os.environ.get("STOCKFILLER_BASE_URL", "").strip()
    if explicit_base_url:
        return explicit_base_url.rstrip("/")

    environment = os.environ.get("STOCKFILLER_ENVIRONMENT", "production").strip().lower()
    if environment in {"production", "prod"}:
        return PRODUCTION_BASE_URL
    if environment in {"sandbox", "staging", "stage"}:
        return SANDBOX_BASE_URL
    raise ValueError("STOCKFILLER_ENVIRONMENT must be production or sandbox")


def build_sync_window(args: argparse.Namespace, state: dict[str, str], now: datetime | None = None) -> SyncWindow:
    now = now or datetime.now(timezone.utc)
    stop = parse_datetime_arg(args.stop) if args.stop else now

    if args.mode == "backfill":
        if not args.start:
            raise ValueError("--start is required in backfill mode")
        start = parse_datetime_arg(args.start)
        return SyncWindow(
            mode="backfill",
            params={
                "createdDateTimeStart": to_stockfiller_datetime(start),
                "createdDateTimeStop": to_stockfiller_datetime(stop),
            },
            stop_value=to_stockfiller_datetime(stop),
        )

    if args.start:
        start = parse_datetime_arg(args.start)
    elif state.get("last_successful_updated_stop"):
        start = parse_datetime_arg(state["last_successful_updated_stop"]) - timedelta(hours=args.overlap_hours)
    else:
        start = now - timedelta(hours=args.lookback_hours)

    return SyncWindow(
        mode="incremental",
        params={
            "updatedDateTimeStart": to_stockfiller_datetime(start),
            "updatedDateTimeStop": to_stockfiller_datetime(stop),
        },
        stop_value=to_stockfiller_datetime(stop),
    )


def sync_orders(
    config: SyncConfig,
    window: SyncWindow,
    dry_run: bool = False,
    spreadsheet=None,
    target_worksheet: str = "order_rows",
    update_state: bool = True,
) -> SyncResult:
    spreadsheet = spreadsheet or open_spreadsheet(config)
    order_sheet = get_or_create_worksheet(spreadsheet, target_worksheet, rows=1000, cols=len(ORDER_COLUMNS))
    existing_values = order_sheet.get_all_values(value_render_option="UNFORMATTED_VALUE")
    headers, existing_rows = values_to_dicts(existing_values)

    client = StockfillerClient(config)
    orders = dedupe_orders_by_reference(fetch_orders_for_window(client, window))
    replace_references = {text(order.get("reference")) for order in orders if text(order.get("reference"))}
    output_rows = flatten_orders(orders)
    apply_crm_customer_numbers(output_rows, load_crm_customer_numbers(spreadsheet))

    merged_headers = merge_headers(headers)
    kept_existing_rows = [row for row in existing_rows if text(row.get("Reference")) not in replace_references]
    final_rows = kept_existing_rows + output_rows

    if not dry_run:
        write_rows(order_sheet, merged_headers, final_rows)
        if target_worksheet == "order_rows":
            sync_latest_order_emails(spreadsheet, final_rows)
        if update_state:
            state = read_sync_state(spreadsheet)
            state.update(
                {
                    "last_successful_mode": window.mode,
                    "last_successful_run_at": to_stockfiller_datetime(datetime.now(timezone.utc)),
                }
            )
            if window.mode == "incremental":
                state["last_successful_updated_stop"] = window.stop_value or ""
            else:
                state["last_successful_backfill_created_stop"] = window.stop_value or ""
            write_sync_state(spreadsheet, state)

    return SyncResult(
        dry_run=dry_run,
        mode=window.mode,
        target_worksheet=target_worksheet,
        fetched_orders=len(orders),
        output_rows=len(output_rows),
        replace_references=len(replace_references),
        existing_rows=len(existing_rows),
        kept_existing_rows=len(kept_existing_rows),
        final_rows=len(final_rows),
        stop_value=window.stop_value,
    )


def fetch_orders_for_window(client: StockfillerClient, window: SyncWindow) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for params in order_params_for_window(window):
        orders.extend(client.iter_orders(params))
    return orders


def order_params_for_window(window: SyncWindow) -> list[dict[str, str]]:
    if (
        window.mode == "backfill"
        and "createdDateTimeStart" in window.params
        and "createdDateTimeStop" in window.params
    ):
        return chunk_datetime_params(
            window.params,
            start_key="createdDateTimeStart",
            stop_key="createdDateTimeStop",
            chunk_days=BACKFILL_CHUNK_DAYS,
        )
    return [window.params]


def chunk_datetime_params(params: dict[str, str], start_key: str, stop_key: str, chunk_days: int) -> list[dict[str, str]]:
    start = parse_datetime_arg(params[start_key])
    stop = parse_datetime_arg(params[stop_key])
    if start > stop:
        return [params]

    chunks = []
    current = start
    while current <= stop:
        chunk_stop = min(current + timedelta(days=chunk_days) - timedelta(seconds=1), stop)
        chunk = {
            **params,
            start_key: to_stockfiller_datetime(current),
            stop_key: to_stockfiller_datetime(chunk_stop),
        }
        chunks.append(chunk)
        current = chunk_stop + timedelta(seconds=1)
    return chunks


def dedupe_orders_by_reference(orders: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_reference: dict[str, dict[str, Any]] = {}
    without_reference: list[dict[str, Any]] = []
    for order in orders:
        reference = text(order.get("reference") or order.get("externalOrderReference"))
        if not reference:
            without_reference.append(order)
            continue

        existing = by_reference.get(reference)
        if existing is None or order_updated_at(order) >= order_updated_at(existing):
            by_reference[reference] = order
    return list(by_reference.values()) + without_reference


def order_updated_at(order: dict[str, Any]) -> datetime:
    value = text(order.get("updatedAtDateTime") or order.get("createdAtDateTime"))
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return parse_datetime_arg(value)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def open_spreadsheet(config: SyncConfig):
    credentials = Credentials.from_service_account_info(config.google_credentials, scopes=SCOPES)
    return gspread.authorize(credentials).open_by_key(config.sheet_key)


def get_or_create_worksheet(spreadsheet, title: str, rows: int, cols: int):
    try:
        return spreadsheet.worksheet(title)
    except WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def read_sync_state(spreadsheet) -> dict[str, str]:
    try:
        worksheet = spreadsheet.worksheet(STATE_SHEET_NAME)
    except WorksheetNotFound:
        return {}

    rows = worksheet.get_all_values()
    state = {}
    for row in rows[1:]:
        if len(row) >= 2 and row[0]:
            state[row[0]] = row[1]
    return state


def write_sync_state(spreadsheet, state: dict[str, str]) -> None:
    worksheet = get_or_create_worksheet(spreadsheet, STATE_SHEET_NAME, rows=10, cols=3)
    now = to_stockfiller_datetime(datetime.now(timezone.utc))
    values = [["key", "value", "updated_at"]]
    for key, value in sorted(state.items()):
        values.append([key, value, now])
    worksheet.clear()
    worksheet.update(values=values, range_name="A1")


def values_to_dicts(values: list[list[str]]) -> tuple[list[str], list[dict[str, str]]]:
    if not values:
        return ORDER_COLUMNS[:], []

    headers = [text(header) for header in values[0]]
    rows = []
    for value_row in values[1:]:
        row = {}
        for index, header in enumerate(headers):
            if header:
                row[header] = value_row[index] if index < len(value_row) else ""
        rows.append(row)
    return headers, rows


def merge_headers(existing_headers: list[str]) -> list[str]:
    extras = [header for header in existing_headers if header and header not in ORDER_COLUMNS]
    return ORDER_COLUMNS + extras


def write_rows(worksheet, headers: list[str], rows: list[dict[str, Any]]) -> None:
    values = [headers]
    values.extend([[format_cell(row.get(header, ""), header) for header in headers] for row in rows])
    worksheet.clear()
    worksheet.update(values=values, range_name="A1", raw=True)


def flatten_orders(orders: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    rows = []
    for order in orders:
        rows.extend(flatten_order(order))
    return rows


def load_crm_customer_numbers(spreadsheet) -> dict[str, str]:
    try:
        worksheet = spreadsheet.worksheet("customers_enriched")
    except WorksheetNotFound:
        return {}

    values = worksheet.get_all_values()
    if not values:
        return {}

    headers = [text(header) for header in values[0]]
    if "customer" not in headers or "customer_number" not in headers:
        return {}

    customer_index = headers.index("customer")
    number_index = headers.index("customer_number")
    customer_numbers = {}
    for row in values[1:]:
        customer = row[customer_index] if customer_index < len(row) else ""
        number = row[number_index] if number_index < len(row) else ""
        customer_key = normalize_key(customer)
        number_value = text(number)
        if customer_key and number_value:
            customer_numbers[customer_key] = number_value
    return customer_numbers


def apply_crm_customer_numbers(order_rows: list[dict[str, str]], customer_numbers_by_name: dict[str, str]) -> None:
    for row in order_rows:
        crm_customer_number = customer_numbers_by_name.get(normalize_key(row.get("Customer")))
        if crm_customer_number:
            row["Customer number"] = crm_customer_number


def latest_order_emails_by_customer(order_rows: Iterable[dict[str, Any]]) -> dict[str, str]:
    """Return buyerEmail from each customer's last physical row in order_rows."""
    latest_emails: dict[str, str] = {}
    for row in order_rows:
        customer_key = normalize_key(row.get("Customer"))
        if customer_key:
            latest_emails[customer_key] = text(row.get("buyerEmail"))
    return latest_emails


def sync_latest_order_emails(spreadsheet, order_rows: Iterable[dict[str, Any]]) -> None:
    worksheet = spreadsheet.worksheet("customers_enriched")
    values = worksheet.get_all_values()
    if not values:
        raise ValueError("customers_enriched is empty; cannot add email_last_order")

    headers = [text(header) for header in values[0]]
    ensure_customer_email_column(worksheet, headers)
    values = worksheet.get_all_values()
    headers = [text(header) for header in values[0]]
    customer_index = required_header_index(headers, "customer", "customers_enriched")
    email_index = required_header_index(headers, CUSTOMER_EMAIL_COLUMN, "customers_enriched")
    latest_emails = latest_order_emails_by_customer(order_rows)

    email_values = []
    for row in values[1:]:
        customer = row[customer_index] if customer_index < len(row) else ""
        email_values.append([latest_emails.get(normalize_key(customer), "")])

    if email_values:
        column = column_name(email_index)
        worksheet.update(
            values=email_values,
            range_name=f"{column}2:{column}{len(email_values) + 1}",
            raw=True,
        )


def ensure_customer_email_column(worksheet, headers: list[str]) -> list[str]:
    headers = list(headers)
    if CUSTOMER_EMAIL_COLUMN in headers:
        left_index = required_header_index(headers, CUSTOMER_EMAIL_LEFT_COLUMN, "customers_enriched")
        email_index = headers.index(CUSTOMER_EMAIL_COLUMN)
        right_index = required_header_index(headers, CUSTOMER_EMAIL_RIGHT_COLUMN, "customers_enriched")
        if email_index != left_index + 1 or right_index != email_index + 1:
            raise ValueError(
                "customers_enriched must order email, email_last_order, and city_google consecutively"
            )
        return headers

    left_index = required_header_index(headers, CUSTOMER_EMAIL_LEFT_COLUMN, "customers_enriched")
    right_index = required_header_index(headers, CUSTOMER_EMAIL_RIGHT_COLUMN, "customers_enriched")
    if right_index != left_index + 1:
        raise ValueError(
            "customers_enriched must have city_google immediately after email "
            "before email_last_order can be inserted"
        )

    insert_col = right_index + 1
    worksheet.insert_cols([[CUSTOMER_EMAIL_COLUMN]], col=insert_col)
    return headers[:right_index] + [CUSTOMER_EMAIL_COLUMN] + headers[right_index:]


def required_header_index(headers: list[str], header: str, sheet_name: str) -> int:
    try:
        return headers.index(header)
    except ValueError as exc:
        raise ValueError(f"{sheet_name} is missing required column: {header}") from exc


def column_name(zero_based_index: int) -> str:
    number = zero_based_index + 1
    name = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        name = chr(65 + remainder) + name
    return name


def flatten_order(order: dict[str, Any]) -> list[dict[str, str]]:
    reference = text(order.get("reference") or order.get("externalOrderReference"))
    if not reference:
        return []

    address, number = split_street_address(order.get("deliveryAddress"))
    currency = text(order.get("currency"))
    order_date = date_part(order.get("createdAtDateTime"))
    delivery_date = date_part(order.get("deliveryDate"))
    customer = first_text(order.get("buyerName"), order.get("buyerLegalName"), order.get("buyerExternalId"), order.get("buyerGln"))
    customer_number = first_text(order.get("buyerExternalId"), order.get("buyerGln"), order.get("buyerOrganisationNumber"))

    rows = []
    for order_row in order.get("orderRows") or []:
        if truthy(order_row.get("deposit")):
            continue

        quantity = parse_decimal(order_row.get("ordered"))
        delivered_quantity = first_decimal(order_row.get("delivered"), order_row.get("received"))
        financial_quantity = delivered_quantity if delivered_quantity is not None else quantity
        if quantity is None and financial_quantity is None:
            continue
        quantity = quantity or Decimal(0)
        financial_quantity = financial_quantity or Decimal(0)

        discounted_price = parse_decimal(first_not_none(order_row.get("priceDiscounted"), order_row.get("price")))
        base_price = parse_decimal(order_row.get("price"))
        total = money_from_minor(discounted_price * financial_quantity, currency) if discounted_price is not None else ""
        pre_discount_total = money_from_minor(base_price * financial_quantity, currency) if base_price is not None else ""
        discount = format_discount(order_row, currency, financial_quantity)

        rows.append(
            {
                "Reference": reference,
                "Order date": order_date,
                "Delivery date": delivery_date,
                "Customer": customer,
                "placedBy": text(order.get("placedBy")),
                "buyerEmail": text(order.get("buyerEmail")),
                "placedAs": text(order.get("placedAs")),
                "Customer Reference": text(order.get("customerReference")),
                "Buyer number": text(order.get("buyerGln")),
                "Customer number": customer_number,
                "Logistics number": text(order.get("buyerExternalLogisticsId")),
                "Address": address,
                "Number": number,
                "Postal code": text(order.get("deliveryZipCode")),
                "City": text(order.get("deliveryCity")),
                "Country": text(order.get("deliveryCountryCode")),
                "Phone number": "",
                "SKU": text(order_row.get("productSku")),
                "Product": text(order_row.get("productName")),
                "Weight": "",
                "Quantity": format_decimal(quantity),
                "Total weight": format_decimal(financial_quantity),
                "Unit": text(order_row.get("unit")),
                "Total (Pre-discount)": pre_discount_total,
                "Product Discount": discount,
                "Total": total,
                "Currency": currency,
                "Order Discount (Amount)": "",
                "Order Discount (%)": "",
                "Batch": text(order_row.get("note")),
            }
        )
    return rows


def format_discount(order_row: dict[str, Any], currency: str, quantity: Decimal) -> str:
    base_price = parse_decimal(order_row.get("price"))
    discounted_price = parse_decimal(order_row.get("priceDiscounted"))
    if base_price is not None and discounted_price is not None:
        return money_from_minor((base_price - discounted_price) * quantity, currency)

    discount_amount = parse_decimal(order_row.get("discountAmount"))
    if discount_amount is not None:
        return money_from_minor(discount_amount * quantity, currency)

    discount_percentage = parse_decimal(order_row.get("discountPercentage"))
    if discount_percentage is not None and base_price is not None:
        return money_from_minor(base_price * quantity * discount_percentage / Decimal(100), currency)

    return ""


def split_street_address(value: Any) -> tuple[str, str]:
    address = text(value)
    if not address:
        return "", ""

    match = re.match(r"^(?P<street>.*\D)\s+(?P<number>\d+\s*[A-Za-z]?(?:[-/]\d+\s*[A-Za-z]?)?)$", address)
    if not match:
        return address, ""
    return match.group("street").strip(), match.group("number").replace(" ", "")


def parse_datetime_arg(value: str) -> datetime:
    text_value = text(value)
    if not text_value:
        raise ValueError("Datetime value cannot be empty")

    normalized = text_value.replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        normalized = f"{normalized}T00:00:00+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def to_stockfiller_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def date_part(value: Any) -> str:
    value_text = text(value)
    if not value_text:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value_text):
        return value_text
    try:
        return parse_datetime_arg(value_text).date().isoformat()
    except ValueError:
        return value_text[:10]


def money_from_minor(value: Decimal, currency: str) -> str:
    units = minor_units(currency)
    amount = value / (Decimal(10) ** units)
    quantizer = Decimal(1) if units == 0 else Decimal("0.01")
    return format_decimal(amount.quantize(quantizer))


def minor_units(currency: str) -> int:
    if text(currency).upper() in {"BIF", "CLP", "DJF", "GNF", "JPY", "KMF", "KRW", "MGA", "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF"}:
        return 0
    return 2


def parse_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value).replace(",", "."))
    except (InvalidOperation, ValueError):
        return None


def first_decimal(*values: Any) -> Decimal | None:
    for value in values:
        parsed = parse_decimal(value)
        if parsed is not None:
            return parsed
    return None


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal(1)))
    return format(normalized, "f")


def format_cell(value: Any, header: str | None = None) -> Any:
    if header in NUMERIC_COLUMNS:
        parsed = parse_number(value)
        if parsed is not None:
            if parsed == parsed.to_integral():
                return int(parsed)
            return float(parsed)

    if isinstance(value, Decimal):
        return format_decimal(value)
    return text(value)


def text(value: Any) -> str:
    return str(value or "").replace("\xa0", " ").strip()


def parse_number(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))

    cleaned = "".join(ch for ch in text(value).replace("−", "-") if ch.isdigit() or ch in ",.-")
    if cleaned in {"", "-", ".", ","}:
        return None
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def normalize_key(value: Any) -> str:
    return " ".join(text(value).casefold().split())


def first_text(*values: Any) -> str:
    for value in values:
        candidate = text(value)
        if candidate:
            return candidate
    return ""


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Stockfiller supplier orders to CRM_DATABASE order_rows.")
    parser.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    parser.add_argument("--start", help="UTC start date/time. Required for backfill. Example: 2026-01-01 or 2026-01-01T00:00:00Z")
    parser.add_argument("--stop", help="UTC stop date/time. Defaults to now.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and merge in memory, but do not write to Google Sheets.")
    parser.add_argument("--target-worksheet", default="order_rows", help="Worksheet to update. Defaults to order_rows.")
    parser.add_argument("--lookback-hours", type=int, default=int(os.environ.get("STOCKFILLER_SYNC_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS)))
    parser.add_argument("--overlap-hours", type=int, default=int(os.environ.get("STOCKFILLER_SYNC_OVERLAP_HOURS", 2)))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        config = SyncConfig.from_env()
        spreadsheet = open_spreadsheet(config)
        state = read_sync_state(spreadsheet)
        window = build_sync_window(args, state)
        result = sync_orders(
            config,
            window,
            dry_run=args.dry_run,
            spreadsheet=spreadsheet,
            target_worksheet=args.target_worksheet,
            update_state=args.target_worksheet == "order_rows",
        )
    except Exception as exc:
        print(f"Stockfiller sync failed: {exc}", file=sys.stderr)
        return 1

    action = "Dry run" if result.dry_run else "Sync"
    print(f"{action} completed.")
    print(f"Mode: {result.mode}")
    print(f"Target worksheet: {result.target_worksheet}")
    print(f"Fetched orders: {result.fetched_orders}")
    print(f"Output rows: {result.output_rows}")
    print(f"References replaced: {result.replace_references}")
    print(f"Existing rows: {result.existing_rows}")
    print(f"Final rows: {result.final_rows}")
    if result.stop_value:
        print(f"Window stop: {result.stop_value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
