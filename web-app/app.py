from flask import Flask, Response, jsonify, send_file, request, send_from_directory, session, g
from flask_cors import CORS
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from urllib.parse import unquote
from datetime import datetime, date, timedelta
from collections import defaultdict
from io import BytesIO
from queue import Empty, Full, Queue
import os
import json
import math
import re
import requests
import threading
import time
import unicodedata
import uuid
from zipfile import ZIP_DEFLATED, ZipFile
from xml.sax.saxutils import escape as xml_escape
from dotenv import load_dotenv
from gspread.utils import rowcol_to_a1
from requests.exceptions import ConnectionError as RequestsConnectionError
from priority import (
    build_contact_features,
    build_order_features,
    build_priority_customers,
    normalize_customer_key,
)
from reminder_email import (
    EMAIL_PROPOSAL_PRODUCT_SETTINGS,
    EMAIL_PROPOSAL_TYPES,
    EMAIL_EVENTS_COLUMNS,
    EMAIL_MESSAGES_COLUMNS,
    EMAIL_RECIPIENTS_COLUMNS,
    SETTINGS_COLUMNS,
    USER_COLUMNS,
    brevo_event_time,
    build_email_proposal_copy,
    build_new_customer_order_rows,
    build_reactivation_order_rows,
    build_settings_product_catalog,
    canonicalize_proposal_order_rows,
    build_latest_order_context,
    classify_customer_relationship,
    classify_clicked_url,
    count_unique_order_customers,
    first_name,
    email_event_key,
    is_valid_email,
    is_yes,
    normalize_brevo_event,
    normalize_email,
    normalize_message_id,
    normalize_proposal_type,
    round_store_count_to_ten,
    render_email_proposal,
    safe_http_url,
    split_email_values,
    stockholm_now,
    stockholm_time_text,
    stockholm_today,
)

load_dotenv()

app = Flask(__name__)
app.config.update(
    SECRET_KEY=(os.environ.get("FLASK_SECRET_KEY") or "store-tracker-local-session"),
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("RENDER", "").strip().lower() == "true",
)
CORS(app, supports_credentials=True)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ.get("SHEET_KEY", "")
IMAGE_DIR = os.path.abspath(os.path.join(app.root_path, "..", "images"))

CUSTOMER_COLUMNS = ["customer", "cancelled_flag", "sales_person", "customer_segment",
                    "customer_reference", "customer_number", "name", "phone", "email",
                    "email_last_order", "comment"]

ORDER_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer", "placedBy", "buyerEmail",
                 "placedAs", "Customer Reference",
                 "Buyer number", "Customer number", "Logistics number", "Address", "Number",
                 "Postal code", "City", "Country", "Phone number", "SKU", "Product", "Weight",
                 "Quantity", "Total weight", "Unit", "Total (Pre-discount)", "Product Discount",
                 "Total", "Currency", "Order Discount (Amount)", "Order Discount (%)", "Batch"]
ORDER_REQUIRED_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer",
                          "Quantity", "Total", "Currency"]

FREEZER_COLUMNS = ["Franui", "Schufrulade", "Boujee", "polarbar", "none"]
FREEZER_SUMMARY_ROWS = [
    {"field": "Franui", "label": "Franui"},
    {"field": "Schufrulade", "label": "Schufrulade"},
    {"field": "Boujee", "label": "Boujee"},
    {"field": "polarbar", "label": "Polarbär"},
    {"field": "none", "label": "Ingen"},
]
CONTACT_LOG_FREEZER_LABELS = {
    "Franui": "Franui",
    "Schufrulade": "Schufrulade",
    "Boujee": "Boujee",
    "polarbar": "Polarbär",
    "none": "Ingen",
}
CONTACT_LOG_COLUMNS = [
    "Datum",
    "Ansvarig",
    "Kund",
    "Kanal",
    "Resultat",
    "Kommentar",
    "Nästa uppföljning",
    "I frysdisken",
]

CONTACT_COLUMNS = ["date_time", "sales_person", "customer", "contact_channel", "result",
                   "comment", "customer_contact_person", "follow_up_date",
                   *FREEZER_COLUMNS, "email_id"]
CONTACT_REQUIRED_COLUMNS = ["date_time", "sales_person", "customer", "contact_channel",
                            "result", "comment", "customer_contact_person", "follow_up_date"]

EMAIL_MESSAGES_SHEET = "email_messages"
EMAIL_RECIPIENTS_SHEET = "email_recipients"
EMAIL_EVENTS_SHEET = "email_events"
USERS_SHEET = "users"
SETTINGS_SHEET = "settings"
BREVO_SEND_URL = "https://api.brevo.com/v3/smtp/email"
BREVO_EVENTS_URL = "https://api.brevo.com/v3/smtp/statistics/events"
EMAIL_SEND_MODE = (os.environ.get("EMAIL_SEND_MODE") or "test").strip().casefold()
EMAIL_TEST_RECIPIENT = (os.environ.get("EMAIL_TEST_RECIPIENT") or "olle@eatpolarbar.com").strip()
BREVO_RECONCILE_INTERVAL_SECONDS = max(
    60, int(os.environ.get("BREVO_RECONCILE_INTERVAL_SECONDS") or 900)
)
BREVO_RECONCILE_DAYS = max(1, min(30, int(os.environ.get("BREVO_RECONCILE_DAYS") or 3)))
BREVO_RECONCILE_MAX_RECIPIENTS = max(
    1, min(500, int(os.environ.get("BREVO_RECONCILE_MAX_RECIPIENTS") or 100))
)
EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS = 60
EMAIL_PROPOSAL_GRACE_DAYS = 7
EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS = 7
EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS = 10
# Backward-compatible names used by older tests and integrations.
REMINDER_EMAIL_GRACE_DAYS = EMAIL_PROPOSAL_GRACE_DAYS
REMINDER_EMAIL_CONTACT_COOLDOWN_DAYS = EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS
REMINDER_EMAIL_SENT_COOLDOWN_DAYS = EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS
_active_send_ids = set()
_active_send_lock = threading.Lock()
_brevo_event_queue = Queue(maxsize=1000)
_brevo_worker_start_lock = threading.Lock()
_brevo_workers_started = False
_brevo_processing_lock = threading.Lock()
_brevo_reconcile_lock = threading.Lock()
_email_sheets_cache = None
_email_sheets_cache_lock = threading.Lock()


_spreadsheet_cache = None


def checkbox_to_sheet_value(value):
    return "1" if str(value).strip().lower() in {"1", "true", "yes", "on"} else ""


def is_checked_value(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def text_to_sheet_value(value, max_length=None):
    text = str(value or "").strip()
    return text[:max_length] if max_length is not None else text


def merge_worksheet_cell_value(column, current, candidate):
    if column in FREEZER_COLUMNS:
        return "1" if is_checked_value(current) or is_checked_value(candidate) else ""

    current_text = str(current or "").strip()
    return current if current_text else candidate


def ensure_customer_name_column(sheet, headers):
    if "name" in headers:
        return headers
    if "phone" not in headers:
        return headers

    original_headers = list(headers)
    insert_at = original_headers.index("phone") + 1
    sheet.insert_cols([["name"]], col=insert_at)
    return original_headers[:insert_at - 1] + ["name"] + original_headers[insert_at - 1:]


def ensure_worksheet_columns(sheet, headers, columns):
    normalized_headers = [str(header).strip() for header in headers]
    for column in columns:
        if column in normalized_headers:
            continue
        target_column = len(normalized_headers) + 1
        grid_columns = getattr(sheet, "col_count", 0) or 0
        if grid_columns and grid_columns < target_column:
            sheet.resize(cols=target_column)
        sheet.update_cell(1, target_column, column)
        normalized_headers.append(column)
    return normalized_headers


def ensure_unique_worksheet_columns(sheet, headers, columns):
    normalized_headers = [str(header).strip() for header in headers]
    duplicate_groups = {
        column: [idx for idx, header in enumerate(normalized_headers) if header == column]
        for column in columns
    }
    duplicate_groups = {column: indexes for column, indexes in duplicate_groups.items() if len(indexes) > 1}
    if not duplicate_groups:
        return normalized_headers

    try:
        rows = sheet.get_all_values()
        if rows:
            for column, indexes in duplicate_groups.items():
                primary_idx = indexes[0]
                merged_values = []
                primary_values = []
                for row in rows[1:]:
                    merged = ""
                    for idx in indexes:
                        value = row[idx] if idx < len(row) else ""
                        merged = merge_worksheet_cell_value(column, merged, value)
                    current = row[primary_idx] if primary_idx < len(row) else ""
                    merged_values.append([merged])
                    primary_values.append(current)

                if merged_values and any(value[0] != primary for value, primary in zip(merged_values, primary_values)):
                    range_name = f"{rowcol_to_a1(2, primary_idx + 1)}:{rowcol_to_a1(len(rows), primary_idx + 1)}"
                    sheet.update(merged_values, range_name=range_name)

        duplicate_indexes = sorted(
            {idx for indexes in duplicate_groups.values() for idx in indexes[1:]},
            reverse=True,
        )
        for idx in duplicate_indexes:
            sheet.delete_columns(idx + 1)
            del normalized_headers[idx]
    except Exception as exc:
        app.logger.warning("Could not deduplicate worksheet columns for %s: %s", sheet.title, exc)
        return normalized_headers

    return normalized_headers


def ensure_contact_worksheet_schema(sheet):
    headers = ensure_worksheet_columns(sheet, sheet.row_values(1), CONTACT_COLUMNS)
    return ensure_unique_worksheet_columns(sheet, headers, FREEZER_COLUMNS)


def build_worksheet_row(headers, row_data, single_value_columns=None):
    single_value_columns = set(single_value_columns or [])
    seen = set()
    row = []
    for header in headers:
        if header in single_value_columns and header in seen:
            row.append("")
            continue
        row.append(row_data.get(header, ""))
        if header:
            seen.add(header)
    return row


def get_spreadsheet(force_reconnect=False):
    global _spreadsheet_cache
    if _spreadsheet_cache is None or force_reconnect:
        creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
        _spreadsheet_cache = gspread.authorize(creds).open_by_key(SHEET_ID)
    return _spreadsheet_cache


def get_spreadsheet_with_retry():
    """Return spreadsheet, reconnecting once on stale-connection errors."""
    try:
        return get_spreadsheet()
    except RequestsConnectionError:
        return get_spreadsheet(force_reconnect=True)


def worksheet_to_dicts(worksheet, expected_columns=None, required_columns=None):
    rows = worksheet.get_all_values()
    if not rows:
        return []

    headers = [str(header).strip() for header in rows[0]]
    required_columns = required_columns or []
    missing_columns = [col for col in required_columns if col not in headers]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Worksheet '{worksheet.title}' saknar obligatoriska kolumner: {missing}")

    expected_columns = expected_columns or headers
    result = []
    for row in rows[1:]:
        item = {col: "" for col in expected_columns}
        for idx, header in enumerate(headers):
            if not header:
                continue
            value = row[idx] if idx < len(row) else ""
            if header in item:
                item[header] = merge_worksheet_cell_value(header, item[header], value)
            else:
                item[header] = value
        result.append(item)
    return result


def get_or_create_worksheet(spreadsheet, title, columns, rows=1000):
    try:
        sheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=max(len(columns), 10))
        sheet.append_row(columns)
        return sheet

    headers = [str(header).strip() for header in sheet.row_values(1)]
    if not headers:
        sheet.append_row(columns)
    else:
        ensure_worksheet_columns(sheet, headers, columns)
    return sheet


def ensure_email_worksheets(spreadsheet):
    global _email_sheets_cache
    spreadsheet_identity = id(spreadsheet)
    with _email_sheets_cache_lock:
        if _email_sheets_cache and _email_sheets_cache[0] == spreadsheet_identity:
            return _email_sheets_cache[1]

        contact_sheet = spreadsheet.worksheet("sales_activities")
        ensure_contact_worksheet_schema(contact_sheet)
        sheets = {
            EMAIL_MESSAGES_SHEET: get_or_create_worksheet(spreadsheet, EMAIL_MESSAGES_SHEET, EMAIL_MESSAGES_COLUMNS),
            EMAIL_RECIPIENTS_SHEET: get_or_create_worksheet(spreadsheet, EMAIL_RECIPIENTS_SHEET, EMAIL_RECIPIENTS_COLUMNS),
            EMAIL_EVENTS_SHEET: get_or_create_worksheet(spreadsheet, EMAIL_EVENTS_SHEET, EMAIL_EVENTS_COLUMNS),
        }
        _email_sheets_cache = (spreadsheet_identity, sheets)
        return sheets


def get_user_rows(spreadsheet):
    return worksheet_to_dicts(
        spreadsheet.worksheet(USERS_SHEET),
        expected_columns=USER_COLUMNS,
        required_columns=USER_COLUMNS,
    )


def public_user(user):
    return {key: str(user.get(key, "")).strip() for key in ("user_name", "name", "role", "email", "phone")}


def find_active_user(spreadsheet, user_name):
    requested = str(user_name or "").strip().casefold()
    for user in get_user_rows(spreadsheet):
        if str(user.get("user_name", "")).strip().casefold() == requested and is_yes(user.get("active")):
            return user
    return None


def get_settings(spreadsheet):
    rows = worksheet_to_dicts(
        spreadsheet.worksheet(SETTINGS_SHEET),
        expected_columns=SETTINGS_COLUMNS,
        required_columns=["key", "value"],
    )
    return {
        str(row.get("key", "")).strip(): str(row.get("value", "")).strip()
        for row in rows if str(row.get("key", "")).strip()
    }


def get_email_rows(spreadsheet):
    sheets = ensure_email_worksheets(spreadsheet)
    return (
        worksheet_to_dicts(sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS),
        worksheet_to_dicts(sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS),
        worksheet_to_dicts(sheets[EMAIL_EVENTS_SHEET], expected_columns=EMAIL_EVENTS_COLUMNS),
    )


def append_dict_row(sheet, columns, values):
    headers = ensure_worksheet_columns(sheet, sheet.row_values(1), columns)
    sheet.append_row(build_worksheet_row(headers, values), value_input_option="RAW")


def find_sheet_row(sheet, column, value, normalizer=lambda item: str(item or "").strip()):
    headers = [str(header).strip() for header in sheet.row_values(1)]
    if column not in headers:
        return None, headers, {}
    target = normalizer(value)
    for row_index, row in enumerate(sheet.get_all_values()[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        item = dict(zip(headers, padded))
        if normalizer(item.get(column)) == target:
            return row_index, headers, item
    return None, headers, {}


def update_sheet_row(sheet, row_index, headers, updates):
    for key, value in updates.items():
        if key not in headers:
            continue
        sheet.update_cell(row_index, headers.index(key) + 1, value)


def worksheet_snapshot(sheet, expected_columns=None):
    values = sheet.get_all_values()
    if not values:
        return list(expected_columns or []), []
    headers = [str(header).strip() for header in values[0]]
    rows = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        item = dict(zip(headers, padded))
        if expected_columns:
            item = {column: item.get(column, "") for column in expected_columns}
        rows.append((row_index, item))
    return headers, rows


def batch_update_sheet_rows(sheet, headers, row_updates):
    data = []
    for row_index, row in row_updates:
        values = [row.get(header, "") for header in headers]
        data.append({
            "range": f"A{row_index}:{rowcol_to_a1(row_index, len(headers))}",
            "values": [values],
        })
    if data:
        sheet.batch_update(data, value_input_option="RAW")


def run_with_retry(operation, *, attempts=5, base_delay=0.5, label="Google Sheets"):
    last_error = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            delay = base_delay * (2 ** attempt)
            app.logger.warning("%s failed (attempt %s/%s): %s", label, attempt + 1, attempts, exc)
            time.sleep(delay)
    raise last_error


def current_user():
    return dict(session.get("user") or {})


def get_order_rows(spreadsheet):
    return worksheet_to_dicts(
        spreadsheet.worksheet("order_rows"),
        expected_columns=ORDER_COLUMNS,
        required_columns=ORDER_REQUIRED_COLUMNS,
    )


def get_contact_rows(spreadsheet):
    return worksheet_to_dicts(
        spreadsheet.worksheet("sales_activities"),
        expected_columns=CONTACT_COLUMNS,
        required_columns=CONTACT_REQUIRED_COLUMNS,
    )


def get_customer_rows(spreadsheet):
    customer_values = spreadsheet.worksheet("customers_enriched").get_all_values()
    customer_headers = customer_values[0] if customer_values else []
    customers = []
    for i, row in enumerate(customer_values[1:], start=2):
        padded = row + [""] * (len(customer_headers) - len(row))
        d = dict(zip(customer_headers, padded))
        name = d.get("customer", "").strip()
        if not name:
            continue
        customers.append({
            "row": i,
            "customer": name,
            "cancelled_flag": d.get("cancelled_flag", "").strip(),
            "sales_person": d.get("sales_person", "").strip(),
            "customer_segment": d.get("customer_segment", "").strip(),
            "customer_number": d.get("customer_number", "").strip(),
            "phone": d.get("phone", "").strip(),
            "email": d.get("email", "").strip(),
            "email_last_order": d.get("email_last_order", "").strip(),
            "city_google": d.get("city_google", "").strip(),
            "region_google": d.get("region_google", "").strip(),
            "latitude_google": d.get("latitude_google", "").strip(),
            "longitude_google": d.get("longitude_google", "").strip(),
            "comment": d.get("comment", "").strip(),
        })
    return customers


def normalize_key(value):
    return normalize_customer_key(value)


def parse_date_value(value):
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt).date()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def parse_datetime_value(value):
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            parsed = datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt)
            return parsed
        except ValueError:
            pass

    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        return None


def format_date_value(value, fallback=""):
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        parsed = parse_date_value(value)
    return parsed.isoformat() if parsed else fallback


def build_latest_contact_followups(contact_rows):
    latest_by_customer = {}
    for idx, contact in enumerate(contact_rows):
        customer_key = normalize_key(contact.get("customer"))
        if not customer_key:
            continue

        registered_at = parse_datetime_value(contact.get("date_time")) or datetime.min
        sort_key = (registered_at, idx)
        if customer_key not in latest_by_customer or sort_key > latest_by_customer[customer_key][0]:
            latest_by_customer[customer_key] = (sort_key, contact)

    return {
        customer_key: parse_date_value(contact.get("follow_up_date"))
        for customer_key, (_, contact) in latest_by_customer.items()
    }


def parse_number_value(value, default=0.0):
    text = str(value or "").strip()
    if not text:
        return default

    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ",.-")
    if cleaned in {"", "-", ".", ","}:
        return default

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return default


def parse_coordinate_value(value, kind):
    text = str(value or "").strip()
    if not text:
        return None

    limits = {
        "latitude": (55.0, 70.0),
        "longitude": (10.0, 25.0),
    }
    lower, upper = limits[kind]

    def in_range(number):
        return math.isfinite(number) and lower <= number <= upper

    normalized = text.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        parsed = float(normalized)
        if in_range(parsed):
            return parsed
    except ValueError:
        pass

    # Some Google Sheet writes have been interpreted as thousands-grouped
    # numbers, e.g. 57,8934438 -> 578934438. Recover by restoring the decimal.
    sign = -1 if normalized.startswith("-") else 1
    integer_part = normalized.lstrip("+-").split(".", 1)[0]
    digits = "".join(ch for ch in integer_part if ch.isdigit())
    if not digits:
        return None

    raw_number = sign * int(digits)
    for decimals in range(1, 13):
        candidate = raw_number / (10 ** decimals)
        if in_range(candidate):
            return candidate

    return None


def week_start(day):
    return day - timedelta(days=day.weekday())


def week_key(day):
    iso = day.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def month_key(day):
    return f"{day.year}-{day.month:02d}"


def week_label(day):
    iso = day.isocalendar()
    return f"Vecka {iso.week} ({iso.year})"


def build_contact_log_rows(contact_rows):
    rows = []
    for idx, row in enumerate(contact_rows):
        parsed_datetime = parse_datetime_value(row.get("date_time"))
        parsed_date = parsed_datetime.date() if parsed_datetime else parse_date_value(row.get("date_time"))
        freezer_labels = [
            label for field, label in CONTACT_LOG_FREEZER_LABELS.items()
            if is_checked_value(row.get(field))
        ]
        log_row = {
            "Datum": format_date_value(parsed_date),
            "Ansvarig": text_to_sheet_value(row.get("sales_person")),
            "Kund": text_to_sheet_value(row.get("customer")),
            "Kanal": text_to_sheet_value(row.get("contact_channel")),
            "Resultat": text_to_sheet_value(row.get("result")),
            "Kommentar": text_to_sheet_value(row.get("comment")),
            "Nästa uppföljning": format_date_value(row.get("follow_up_date")),
            "I frysdisken": ", ".join(freezer_labels),
            "_month": month_key(parsed_date) if parsed_date else "",
            "_week": week_key(parsed_date) if parsed_date else "",
            "_week_label": week_label(parsed_date) if parsed_date else "",
            "_sort_value": parsed_datetime or (datetime.combine(parsed_date, datetime.min.time()) if parsed_date else datetime.min),
            "_source_index": idx,
        }
        rows.append(log_row)

    rows.sort(key=lambda item: (item["_sort_value"], item["_source_index"]), reverse=True)
    return rows


def get_contact_log_filter_values(args):
    filters = {}
    for key in ("responsible", "month", "week", "result"):
        values = []
        for value in args.getlist(key):
            values.extend(part.strip() for part in str(value).split(","))
        filters[key] = {value for value in values if value}
    for key in ("customer", "comment"):
        value = " ".join(str(value).strip() for value in args.getlist(key) if str(value).strip())
        if value:
            filters[key] = value
    return filters


def normalize_contact_log_search_text(value):
    text = unicodedata.normalize("NFD", str(value or "").casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def contact_log_is_subsequence(needle, haystack):
    needle_index = 0
    for char in haystack:
        if needle_index < len(needle) and needle[needle_index] == char:
            needle_index += 1
    return needle_index == len(needle)


def contact_log_text_matches(value, query):
    terms = normalize_contact_log_search_text(query).split()
    if not terms:
        return True

    normalized_value = normalize_contact_log_search_text(value)
    words = normalized_value.split()
    compact_value = normalized_value.replace(" ", "")

    for term in terms:
        if term in normalized_value or term in compact_value:
            continue
        if len(term) >= 4 and any(contact_log_is_subsequence(term, word) for word in words):
            continue
        return False
    return True


def filter_contact_log_rows(rows, filters):
    filtered = list(rows)
    if filters.get("responsible"):
        filtered = [row for row in filtered if row["Ansvarig"] in filters["responsible"]]
    if filters.get("month"):
        filtered = [row for row in filtered if row["_month"] in filters["month"]]
    if filters.get("week"):
        filtered = [row for row in filtered if row["_week"] in filters["week"]]
    if filters.get("result"):
        filtered = [row for row in filtered if row["Resultat"] in filters["result"]]
    if filters.get("customer"):
        filtered = [row for row in filtered if contact_log_text_matches(row["Kund"], filters["customer"])]
    if filters.get("comment"):
        filtered = [row for row in filtered if contact_log_text_matches(row["Kommentar"], filters["comment"])]
    return filtered


def build_contact_log_options(rows):
    def unique_display_values(key):
        return sorted({row[key] for row in rows if row.get(key)}, key=str.casefold)

    month_values = sorted({row["_month"] for row in rows if row["_month"]}, reverse=True)
    week_values = sorted(
        {row["_week"] for row in rows if row["_week"]},
        key=lambda value: tuple(int(part) for part in value.replace("W", "").split("-")),
        reverse=True,
    )
    week_labels = {row["_week"]: row["_week_label"] for row in rows if row["_week"]}

    return {
        "responsible": [{"value": value, "label": value} for value in unique_display_values("Ansvarig")],
        "month": [{"value": value, "label": value} for value in month_values],
        "week": [{"value": value, "label": week_labels.get(value, value)} for value in week_values],
        "result": [{"value": value, "label": value} for value in unique_display_values("Resultat")],
    }


def public_contact_log_row(row):
    return {column: row.get(column, "") for column in CONTACT_LOG_COLUMNS}


def build_contact_log_payload(contact_rows, filters=None):
    all_rows = build_contact_log_rows(contact_rows)
    selected_filters = filters or {}
    filtered_rows = filter_contact_log_rows(all_rows, selected_filters)
    return {
        "columns": CONTACT_LOG_COLUMNS,
        "rows": [public_contact_log_row(row) for row in filtered_rows],
        "filters": build_contact_log_options(all_rows),
        "total_count": len(all_rows),
        "filtered_count": len(filtered_rows),
    }


def xlsx_column_name(index):
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def xml_text(value):
    text = str(value or "")
    text = "".join(ch for ch in text if ch in "\n\r\t" or ord(ch) >= 32)
    return xml_escape(text)


def build_xlsx(columns, rows, sheet_name="Kontaktlogg"):
    output = BytesIO()
    sheet_name = xml_escape(sheet_name[:31] or "Kontaktlogg")
    table = [columns] + [[row.get(column, "") for column in columns] for row in rows]
    last_column = xlsx_column_name(len(columns))
    last_row = max(1, len(table))

    def cell_xml(row_idx, col_idx, value, style=""):
        cell_ref = f"{xlsx_column_name(col_idx)}{row_idx}"
        style_attr = f' s="{style}"' if style else ""
        return f'<c r="{cell_ref}" t="inlineStr"{style_attr}><is><t>{xml_text(value)}</t></is></c>'

    row_xml = []
    for row_idx, values in enumerate(table, start=1):
        style = "1" if row_idx == 1 else ""
        cells = "".join(cell_xml(row_idx, col_idx, value, style) for col_idx, value in enumerate(values, start=1))
        row_xml.append(f'<row r="{row_idx}">{cells}</row>')

    column_widths = [12, 16, 30, 14, 16, 52, 18, 26]
    cols_xml = "".join(
        f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
        for idx, width in enumerate(column_widths, start=1)
    )

    worksheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="A1:{last_column}{last_row}"/>
  <sheetViews><sheetView tabSelected="1" workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>
  <cols>{cols_xml}</cols>
  <sheetData>{"".join(row_xml)}</sheetData>
  <autoFilter ref="A1:{last_column}{last_row}"/>
</worksheet>'''

    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>''')
        archive.writestr("_rels/.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>''')
        archive.writestr("xl/workbook.xml", f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="{sheet_name}" sheetId="1" r:id="rId1"/></sheets>
</workbook>''')
        archive.writestr("xl/_rels/workbook.xml.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>''')
        archive.writestr("xl/styles.xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>
</styleSheet>''')
        archive.writestr("xl/worksheets/sheet1.xml", worksheet_xml)

    output.seek(0)
    return output.getvalue()


def build_recent_weeks(today, count=5):
    current_start = week_start(today)
    weeks = []
    for offset in range(count - 1, -1, -1):
        start = current_start - timedelta(weeks=offset)
        iso = start.isocalendar()
        weeks.append({
            "key": f"{iso.year}-W{iso.week:02d}",
            "label": f"Vecka {iso.week}",
            "start_date": start.isoformat(),
            "end_date": (start + timedelta(days=6)).isoformat(),
        })
    return weeks


SWEDISH_MONTH_LABELS = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "maj",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "okt",
    11: "nov",
    12: "dec",
}


def format_week_date_range(start, end):
    start_month = SWEDISH_MONTH_LABELS[start.month]
    end_month = SWEDISH_MONTH_LABELS[end.month]
    if start.month == end.month:
        return f"{start.day}-{end.day} {end_month}"
    return f"{start.day} {start_month}-{end.day} {end_month}"


def build_dfp_top_weeks(order_rows, year=2026, limit=5):
    totals_by_week = defaultdict(float)
    week_dates = {}

    for order in order_rows:
        order_date = parse_date_value(order["Order date"])
        if not order_date or order_date.year != year:
            continue

        total_weight = parse_number_value(order["Total weight"], default=0.0)
        if total_weight <= 0:
            continue

        start = week_start(order_date)
        end = start + timedelta(days=6)
        key = week_key(order_date)
        totals_by_week[key] += total_weight
        week_dates[key] = (start, end, order_date.isocalendar().week)

    top_weeks = sorted(
        totals_by_week.items(),
        key=lambda item: (-item[1], week_dates[item[0]][0]),
    )[:limit]
    top_total = top_weeks[0][1] if top_weeks else 0

    return [
        {
            "rank": idx + 1,
            "week_key": key,
            "label": f"Vecka {week_number}",
            "short_label": f"V{week_number}",
            "date_range": format_week_date_range(start, end),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "dfp_count": format_dfp_count(total),
            "share_of_top": round((total / top_total) * 100) if top_total else 0,
        }
        for idx, (key, total) in enumerate(top_weeks)
        for start, end, week_number in [week_dates[key]]
    ]


def build_freezer_summary(contact_rows):
    latest_contact_by_customer = {}
    for idx, contact in enumerate(contact_rows):
        customer_key = normalize_key(contact.get("customer"))
        if not customer_key:
            continue

        registered_at = parse_datetime_value(contact.get("date_time")) or datetime.min
        sort_key = (registered_at, idx)
        if customer_key not in latest_contact_by_customer or sort_key > latest_contact_by_customer[customer_key][0]:
            latest_contact_by_customer[customer_key] = (sort_key, contact)

    product_customer_sets = {item["field"]: set() for item in FREEZER_SUMMARY_ROWS}
    seller_customer_sets = {
        item["field"]: defaultdict(set)
        for item in FREEZER_SUMMARY_ROWS
    }
    seller_labels = {}

    for customer_key, (_, contact) in latest_contact_by_customer.items():
        checked_fields = [
            field for field in FREEZER_COLUMNS
            if is_checked_value(contact.get(field))
        ]
        if not checked_fields:
            continue

        seller_label = text_to_sheet_value(contact.get("sales_person")) or "Ej angiven"
        seller_key = seller_label.casefold()
        seller_labels.setdefault(seller_key, seller_label)

        for field in checked_fields:
            product_customer_sets[field].add(customer_key)
            seller_customer_sets[field][seller_key].add(customer_key)

    sales_people = [
        {"key": key, "label": seller_labels[key]}
        for key in sorted(seller_labels, key=lambda value: seller_labels[value].casefold())
    ]

    rows = []
    for item in FREEZER_SUMMARY_ROWS:
        field = item["field"]
        rows.append({
            "field": field,
            "label": item["label"],
            "total": len(product_customer_sets[field]),
            "counts": {
                person["key"]: len(seller_customer_sets[field].get(person["key"], set()))
                for person in sales_people
            },
        })

    sum_counts = {
        person["key"]: sum(row["counts"].get(person["key"], 0) for row in rows)
        for person in sales_people
    }
    total_sum = sum(row["total"] for row in rows)
    polarbar_row = next((row for row in rows if row["field"] == "polarbar"), None)

    def share_percent(value, total):
        return round((value / total) * 100) if total else 0

    return {
        "sales_people": sales_people,
        "rows": rows,
        "sum_row": {
            "label": "Summa",
            "total": total_sum,
            "counts": sum_counts,
        },
        "polarbar_share_row": {
            "label": "Polarbär andel",
            "total": share_percent(polarbar_row["total"] if polarbar_row else 0, total_sum),
            "counts": {
                person["key"]: share_percent(
                    (polarbar_row["counts"].get(person["key"], 0) if polarbar_row else 0),
                    sum_counts.get(person["key"], 0),
                )
                for person in sales_people
            },
        },
    }


def format_dfp_count(count):
    return int(count) if float(count).is_integer() else round(count, 1)


def calculate_customer_risk(order_count, latest_order, latest_delivery, today):
    most_recent = max(latest_order, latest_delivery) if latest_order and latest_delivery else (latest_order or latest_delivery)
    if order_count == 0 or not most_recent:
        return ""

    days_since = (today - most_recent).days
    if days_since >= 75:
        return "Återaktivering krävs"
    if days_since >= 60:
        return "Hög risk"
    if days_since >= 45:
        return "Risk"
    if days_since >= 30:
        return "Bevaka"
    return "Aktiv"


def is_positive_contact(result):
    text = str(result or "").strip().lower()
    positive_results = ("positiv", "positivt", "order lagd!", "order lagd")
    return any(phrase in text for phrase in positive_results)


def segment_sort_key(segment):
    text = str(segment or "").strip()
    if not text:
        return (99, "")
    first = text[:1].upper()
    if first in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        return (ord(first) - ord("A"), text.casefold())
    return (50, text.casefold())


def now_text():
    return stockholm_time_text()


def format_datetime_value(value, fallback=""):
    parsed = value if isinstance(value, datetime) else parse_datetime_value(value)
    return parsed.strftime("%Y-%m-%d %H:%M:%S") if parsed else fallback


def get_customer_by_row(spreadsheet, row_number):
    sheet = spreadsheet.worksheet("customers_enriched")
    rows = sheet.get_all_values()
    if not rows or row_number < 2 or row_number > len(rows):
        return None
    headers = [str(header).strip() for header in rows[0]]
    row = rows[row_number - 1]
    padded = row + [""] * (len(headers) - len(row))
    customer = dict(zip(headers, padded))
    customer["row"] = row_number
    return customer if str(customer.get("customer", "")).strip() else None


def customer_is_cancelled(customer):
    value = str(customer.get("cancelled_flag", "") or "").strip().casefold()
    return value in {"1", "y", "yes", "ja", "true", "cancelled", "canceled", "avslutad"}


def blocked_recipient_reasons(recipient_rows):
    reasons = {}
    for row in recipient_rows:
        email = normalize_email(row.get("intended_email"))
        if not email:
            continue
        bounce_type = str(row.get("bounce_type", "")).strip().casefold()
        if str(row.get("unsubscribed_at", "")).strip():
            reasons[email] = "Avregistrerad i Brevo"
        elif str(row.get("blocked_at", "")).strip():
            reasons[email] = "Blockerad i Brevo"
        elif bounce_type in {"hardbounce", "hard bounce", "invalid", "spam"}:
            reasons[email] = "Permanent studs eller ogiltig adress"
    return reasons


def build_recipient_options(customer, latest_order, recipient_rows):
    order_emails = split_email_values(customer.get("email_last_order"))
    customer_emails = split_email_values(customer.get("email"))
    combined = split_email_values(customer.get("email_last_order"), customer.get("email"))
    order_keys = {normalize_email(item["email"]) for item in order_emails}
    customer_keys = {normalize_email(item["email"]) for item in customer_emails}
    blocked = blocked_recipient_reasons(recipient_rows)
    latest_buyer_key = normalize_email(latest_order.get("buyer_email"))

    recipients = []
    for item in combined:
        if not item["valid"]:
            continue
        email = item["email"]
        key = normalize_email(email)
        source = "email_last_order" if key in order_keys else "email"
        if key == latest_buyer_key or source == "email_last_order":
            greeting = first_name(latest_order.get("placed_by"))
        elif key in customer_keys:
            greeting = first_name(customer.get("name"))
        else:
            greeting = ""
        blocked_reason = blocked.get(key, "")
        recipients.append({
            "email": email,
            "source": source,
            "valid": bool(item["valid"]),
            "selected": bool(item["valid"] and not blocked_reason),
            "greeting_name": greeting,
            "blocked_reason": blocked_reason,
        })
    return recipients


def build_email_proposal_warnings(customer_name, latest_order, contact_rows, message_rows, created_at=None):
    warnings = []
    customer_key = normalize_key(customer_name)
    today = stockholm_today()

    recent_contacts = []
    for row in contact_rows:
        if normalize_key(row.get("customer")) != customer_key:
            continue
        if str(row.get("email_id", "")).strip():
            continue
        registered = parse_datetime_value(row.get("date_time"))
        if registered and (today - registered.date()).days <= 7:
            recent_contacts.append(registered)
    if recent_contacts:
        latest = max(recent_contacts)
        warnings.append({
            "code": "recent_contact",
            "message": f"En säljkontakt registrerades {latest.strftime('%Y-%m-%d')}.",
        })

    recent_messages = []
    for row in message_rows:
        if normalize_key(row.get("customer")) != customer_key or is_yes(row.get("is_test")):
            continue
        if str(row.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue
        sent_at = parse_datetime_value(row.get("sent_at"))
        if sent_at and (today - sent_at.date()).days <= 10:
            recent_messages.append(sent_at)
    if recent_messages:
        latest = max(recent_messages)
        warnings.append({
            "code": "recent_reminder",
            "message": f"Ett mejlförslag skickades {latest.strftime('%Y-%m-%d')}.",
        })

    created = parse_datetime_value(created_at)
    latest_order_date = parse_date_value(latest_order.get("order_date"))
    if created and latest_order_date and latest_order_date > created.date():
        warnings.append({
            "code": "new_order",
            "message": f"En ny order registrerades {latest_order_date.isoformat()} efter att utkastet skapades.",
        })
    return warnings


def build_reminder_warnings(customer_name, latest_order, contact_rows, message_rows, created_at=None):
    """Backward-compatible alias for the generic proposal warnings."""
    return build_email_proposal_warnings(
        customer_name, latest_order, contact_rows, message_rows, created_at=created_at
    )


def latest_live_email_proposals_by_customer(message_rows):
    latest = {}
    for row in message_rows:
        if is_yes(row.get("is_test")):
            continue
        if str(row.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue
        customer_key = normalize_key(row.get("customer"))
        sent_at = parse_datetime_value(row.get("sent_at"))
        if not customer_key or not sent_at:
            continue
        if customer_key not in latest or sent_at > latest[customer_key]:
            latest[customer_key] = sent_at
    return latest


def latest_live_reminders_by_customer(message_rows):
    """Backward-compatible alias; all V1 email types share the same cooldown."""
    return latest_live_email_proposals_by_customer(message_rows)


def build_email_proposal_status(customer, priority, relationship, latest_live_proposals,
                                blocked_recipients, today):
    relationship = relationship or {}
    proposal_type = normalize_proposal_type(relationship.get("email_type"))
    blockers = []
    if customer_is_cancelled(customer):
        blockers.append("customer_cancelled")

    recipient_candidates = split_email_values(customer.get("email_last_order"), customer.get("email"))
    usable_recipients = {
        normalize_email(item.get("email"))
        for item in recipient_candidates
        if item.get("valid") and normalize_email(item.get("email")) not in blocked_recipients
    }
    if not usable_recipients:
        blockers.append("no_usable_recipient")

    order_count = int(parse_number_value(priority.get("order_count"), 0) or 0)
    has_prior_order = bool(relationship.get("has_prior_order", order_count > 0))
    action_type = str((priority.get("next_action") or {}).get("action_type", "")).strip()
    if action_type in {"negative_reactivation", "follow_up", "scheduled_followup"}:
        blockers.append("other_followup_takes_precedence")

    reason = ""
    if proposal_type == "reminder":
        if not has_prior_order:
            blockers.append("no_prior_order")
        days_since_delivery = parse_number_value(
            relationship.get("days_since_delivery", priority.get("days_since_delivery")), None
        )
        overdue_days = parse_number_value(priority.get("overdue_days"), None)
        expected_cycle = parse_number_value(priority.get("expected_cycle_days"), None)
        if days_since_delivery is not None and expected_cycle is not None:
            due_after_days = min(
                int(expected_cycle) + EMAIL_PROPOSAL_GRACE_DAYS,
                EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS,
            )
            if days_since_delivery < due_after_days:
                blockers.append("not_due_yet")
            else:
                reason = f"{int(days_since_delivery)} dagar sedan senaste leverans"
        elif overdue_days is None or overdue_days < EMAIL_PROPOSAL_GRACE_DAYS:
            blockers.append("not_due_yet")
        else:
            reason = f"{int(overdue_days)} dagar efter förväntat återköpsdatum"
    elif proposal_type == "reactivation":
        if not has_prior_order:
            blockers.append("no_prior_order")
        days_since_delivery = relationship.get("days_since_delivery")
        reason = (
            f"{int(days_since_delivery)} dagar sedan senaste leverans"
            if days_since_delivery is not None and days_since_delivery >= 0
            else "Tidigare kund utan leverans de senaste 60 dagarna"
        )
    else:
        if has_prior_order:
            blockers.append("has_prior_order")
        reason = "Ingen tidigare order"

    latest_contact_date = parse_date_value(priority.get("latest_contact_date"))
    if latest_contact_date:
        days_since_contact = (today - latest_contact_date).days
        if 0 <= days_since_contact <= EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS:
            blockers.append("recent_sales_contact")

    latest_sent = latest_live_proposals.get(normalize_key(customer.get("customer")))
    if latest_sent:
        days_since_sent = (today - latest_sent.date()).days
        if 0 <= days_since_sent <= EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS:
            blockers.append("recent_email_proposal")

    return {
        "due": not blockers,
        "email_type": proposal_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[proposal_type],
        "reason": reason if not blockers else "",
        "blockers": blockers,
        "eligible_recipient_count": len(usable_recipients),
        "latest_sent_at": format_datetime_value(latest_sent) if latest_sent else "",
    }


def build_reminder_email_status(customer, priority, latest_live_reminders, blocked_recipients, today):
    relationship = {
        "email_type": "reminder",
        "has_prior_order": int(parse_number_value(priority.get("order_count"), 0) or 0) > 0,
        "days_since_delivery": priority.get("days_since_delivery"),
    }
    status = build_email_proposal_status(
        customer, priority, relationship, latest_live_reminders, blocked_recipients, today
    )
    if "recent_email_proposal" in status["blockers"]:
        status["blockers"] = [
            "recent_reminder_email" if item == "recent_email_proposal" else item
            for item in status["blockers"]
        ]
    return status


def build_email_proposal_draft(spreadsheet, row_number, draft_id=None, created_at=None):
    customer = get_customer_by_row(spreadsheet, row_number)
    if not customer:
        return None
    order_rows = get_order_rows(spreadsheet)
    contact_rows = get_contact_rows(spreadsheet)
    message_rows, recipient_rows, _ = get_email_rows(spreadsheet)
    latest_order = build_latest_order_context(order_rows, customer.get("customer"))
    relationship = classify_customer_relationship(
        order_rows,
        customer.get("customer"),
        today=stockholm_today(),
        recent_days=EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS,
    )
    proposal_type = relationship["email_type"]
    settings = get_settings(spreadsheet)
    product_catalog = build_settings_product_catalog(settings)
    suggested_rows = canonicalize_proposal_order_rows(
        latest_order.get("order_rows", []), product_catalog
    )
    if proposal_type == "reactivation":
        suggested_rows = build_reactivation_order_rows(product_catalog)
    elif proposal_type == "new_customer":
        suggested_rows = build_new_customer_order_rows(product_catalog)

    unique_store_count = count_unique_order_customers(order_rows)
    copy = build_email_proposal_copy(
        proposal_type,
        customer.get("customer"),
        latest_delivery_date=latest_order.get("delivery_date"),
        has_order_rows=bool(suggested_rows),
        unique_store_count=unique_store_count,
    )
    created_at = created_at or now_text()
    product_setting = EMAIL_PROPOSAL_PRODUCT_SETTINGS[proposal_type]
    product_sheet_url = safe_http_url(settings.get(product_setting))
    fallback_product_url = safe_http_url(settings.get("reminder_product_sheet_url"))
    used_product_fallback = bool(not product_sheet_url and fallback_product_url)
    if used_product_fallback:
        product_sheet_url = fallback_product_url
    stockfiller_url = safe_http_url(
        settings.get("email_proposal_stockfiller_url") or settings.get("reminder_stockfiller_url")
    )
    notices = []
    if not product_sheet_url:
        notices.append(
            f"Produktbladslänken för {EMAIL_PROPOSAL_TYPES[proposal_type].lower()} saknas eller är ogiltig "
            "och utelämnas från mejlet."
        )
    elif used_product_fallback and proposal_type != "reminder":
        notices.append(
            f"Inställningen {product_setting} saknas. Det vanliga produktbladet används tills vidare."
        )
    if not stockfiller_url:
        notices.append("Stockfiller-länken saknas eller är ogiltig och utelämnas från mejlet.")
    if not product_catalog:
        notices.append("Inga sku_-artiklar finns i settings. Produktrader behöver fyllas i manuellt.")
    elif len(suggested_rows) < 4 and proposal_type in {"reactivation", "new_customer"}:
        notices.append("En eller flera av standardartiklarna saknas i settings.")

    user = current_user()
    return {
        "draft_id": draft_id or str(uuid.uuid4()),
        "created_at": created_at,
        "send_mode": EMAIL_SEND_MODE,
        "test_recipient": EMAIL_TEST_RECIPIENT if EMAIL_SEND_MODE != "live" else "",
        "customer": {
            "row": row_number,
            "customer": str(customer.get("customer", "")).strip(),
            "customer_number": str(customer.get("customer_number", "")).strip(),
            "cancelled": customer_is_cancelled(customer),
        },
        "email_type": proposal_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[proposal_type],
        "relationship": relationship,
        "latest_order_reference": latest_order.get("reference", ""),
        "latest_delivery_date": latest_order.get("delivery_date", ""),
        "recipients": build_recipient_options(customer, latest_order, recipient_rows),
        "subject": copy["subject"],
        "intro_text": copy["intro_text"],
        "closing_text": copy["closing_text"],
        "order_rows": suggested_rows,
        "product_catalog": product_catalog,
        "links": {
            "product_sheet_url": product_sheet_url,
            "stockfiller_url": stockfiller_url,
        },
        "cta_labels": {
            "product_sheet": copy["product_sheet_label"],
            "stockfiller": copy["stockfiller_label"],
        },
        "stats": {
            "unique_order_customers": unique_store_count,
            "rounded_unique_order_customers": round_store_count_to_ten(unique_store_count),
        },
        "signature": public_user(user),
        "warnings": build_email_proposal_warnings(
            customer.get("customer"), latest_order, contact_rows, message_rows
        ),
        "notices": notices,
    }


def build_reminder_draft(spreadsheet, row_number, draft_id=None, created_at=None):
    """Backward-compatible alias returning the customer's current proposal type."""
    return build_email_proposal_draft(
        spreadsheet, row_number, draft_id=draft_id, created_at=created_at
    )


def send_brevo_transactional_email(*, sender, recipient_email, recipient_name, reply_to,
                                    subject, html_body, text_body, email_id,
                                    email_type="reminder"):
    api_key = str(os.environ.get("BREVO_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("BREVO_API_KEY saknas")
    response = requests.post(
        BREVO_SEND_URL,
        headers={"api-key": api_key, "accept": "application/json", "content-type": "application/json"},
        json={
            "sender": sender,
            "to": [{"email": recipient_email, "name": recipient_name or recipient_email}],
            "replyTo": reply_to,
            "subject": subject,
            "htmlContent": html_body,
            "textContent": text_body,
            "tags": ["store-tracker", f"proposal-{normalize_proposal_type(email_type)}", f"email-{email_id}"],
        },
        timeout=20,
    )
    if response.status_code >= 400:
        detail = response.text[:500] if response.text else f"HTTP {response.status_code}"
        raise RuntimeError(detail)
    try:
        payload = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        payload = {}
    message_id = normalize_message_id(payload.get("messageId"))
    if not message_id:
        raise RuntimeError("Brevo returnerade inget Message ID")
    return message_id


def build_sales_activity_for_email(spreadsheet, *, email_id, email_type, customer_name, user,
                                   recipients, partial):
    sheet = spreadsheet.worksheet("sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    type_label = EMAIL_PROPOSAL_TYPES[normalize_proposal_type(email_type)]
    result = f"Mejlförslag delvis skickat – {type_label}" if partial else f"Mejlförslag skickat – {type_label}"
    row_data = {
        "date_time": now_text(),
        "sales_person": user.get("name") or user.get("user_name", ""),
        "customer": customer_name,
        "contact_channel": "Mejl",
        "result": result,
        "comment": f"Mottagare: {', '.join(recipients)}",
        "customer_contact_person": "",
        "follow_up_date": "",
        "email_id": email_id,
    }
    sheet.append_row(build_worksheet_row(headers, row_data, single_value_columns=FREEZER_COLUMNS), value_input_option="USER_ENTERED")


@app.before_request
def require_authenticated_session():
    public_endpoints = {
        "index", "images", "login", "get_session", "brevo_webhook", "brevo_reconcile"
    }
    if request.method == "OPTIONS" or request.endpoint in public_endpoints:
        return None
    user = current_user()
    if not user.get("user_name"):
        return jsonify({"ok": False, "error": "authentication_required"}), 401
    g.current_user = user
    return None


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/images/<path:filename>")
def images(filename):
    return send_from_directory(IMAGE_DIR, filename)


@app.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    try:
        spreadsheet = get_spreadsheet_with_retry()
        user = find_active_user(spreadsheet, data.get("user_name"))
    except Exception:
        app.logger.exception("Could not read users worksheet during login")
        return jsonify({"ok": False, "error": "user_store_unavailable"}), 503
    if not user or str(data.get("password") or "") != str(user.get("password") or ""):
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    profile = public_user(user)
    session.clear()
    session.permanent = True
    session["user"] = profile
    return jsonify({"ok": True, "user": profile})


@app.route("/session", methods=["GET"])
def get_session():
    profile = current_user()
    if not profile.get("user_name"):
        return jsonify({"ok": False, "authenticated": False}), 401
    spreadsheet = get_spreadsheet_with_retry()
    active_user = find_active_user(spreadsheet, profile.get("user_name"))
    if not active_user:
        session.clear()
        return jsonify({"ok": False, "authenticated": False}), 401
    profile = public_user(active_user)
    session["user"] = profile
    session.permanent = True
    return jsonify({"ok": True, "authenticated": True, "user": profile})


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/customers/<int:row>/email-proposal-draft", methods=["GET"])
@app.route("/customers/<int:row>/reminder-email-draft", methods=["GET"])
def get_email_proposal_draft(row):
    spreadsheet = get_spreadsheet_with_retry()
    draft = build_email_proposal_draft(spreadsheet, row)
    if not draft:
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    if draft["customer"]["cancelled"]:
        draft["notices"].insert(0, "Kunden är markerad som avslutad och mejlet kan inte skickas.")
    return jsonify({"ok": True, "draft": draft})


def sanitize_order_rows(rows):
    result = []
    for row in (rows or [])[:50]:
        product = str(row.get("product", "")).strip()[:250]
        quantity = str(row.get("quantity", "")).strip()[:30]
        unit = str(row.get("unit", "DFP")).strip()[:20] or "DFP"
        if product:
            result.append({
                "product": product,
                "quantity": quantity,
                "unit": unit,
                "new_for_customer": bool(row.get("new_for_customer")),
            })
    return result


@app.route("/customers/<int:row>/email-proposal/send", methods=["POST"])
@app.route("/customers/<int:row>/reminder-email/send", methods=["POST"])
def send_email_proposal(row):
    data = request.get_json(silent=True) or {}
    draft_id = str(data.get("draft_id", "")).strip()
    if not draft_id or len(draft_id) > 80:
        return jsonify({"ok": False, "error": "invalid_draft_id"}), 400

    with _active_send_lock:
        if draft_id in _active_send_ids:
            return jsonify({"ok": False, "error": "send_in_progress"}), 409
        _active_send_ids.add(draft_id)

    try:
        spreadsheet = get_spreadsheet_with_retry()
        sheets = ensure_email_worksheets(spreadsheet)
        existing_row, _, existing = find_sheet_row(sheets[EMAIL_MESSAGES_SHEET], "email_id", draft_id)
        if existing_row:
            return jsonify({
                "ok": False,
                "error": "duplicate_send",
                "status": existing.get("status", ""),
            }), 409

        draft_created_at = str(data.get("created_at", "")).strip()
        current_draft = build_email_proposal_draft(
            spreadsheet,
            row,
            draft_id=draft_id,
            created_at=draft_created_at or now_text(),
        )
        if not current_draft:
            return jsonify({"ok": False, "error": "customer_not_found"}), 404
        if current_draft["customer"]["cancelled"]:
            return jsonify({"ok": False, "error": "customer_cancelled"}), 409

        requested_email_type = str(data.get("email_type", "")).strip()
        if requested_email_type and normalize_proposal_type(requested_email_type) != current_draft["email_type"]:
            return jsonify({
                "ok": False,
                "error": "email_type_changed",
                "email_type": current_draft["email_type"],
                "email_type_label": current_draft["email_type_label"],
            }), 409

        allowed = {normalize_email(item["email"]): item for item in current_draft["recipients"]}
        selected = []
        for item in data.get("recipients") or []:
            if not item.get("selected"):
                continue
            key = normalize_email(item.get("email"))
            allowed_item = allowed.get(key)
            if not allowed_item or not allowed_item.get("valid"):
                continue
            if allowed_item.get("blocked_reason"):
                return jsonify({
                    "ok": False,
                    "error": "recipient_blocked",
                    "email": allowed_item.get("email"),
                    "reason": allowed_item.get("blocked_reason"),
                }), 409
            selected.append({
                "email": allowed_item["email"],
                "greeting_name": first_name(item.get("greeting_name")),
            })
        if not selected:
            return jsonify({"ok": False, "error": "no_valid_recipients"}), 400

        warnings = list(current_draft.get("warnings") or [])
        if str(data.get("latest_order_reference", "")).strip() != str(current_draft.get("latest_order_reference", "")).strip():
            warnings.append({
                "code": "new_order",
                "message": "Kundens senaste order har ändrats sedan utkastet öppnades.",
            })
        unique_warnings = {item["code"]: item for item in warnings}
        warnings = list(unique_warnings.values())
        if warnings and not data.get("confirm_warnings"):
            return jsonify({
                "ok": False,
                "error": "warning_confirmation_required",
                "warnings": warnings,
            }), 409

        subject = str(data.get("subject", "")).strip()[:250]
        intro_text = str(data.get("intro_text", "")).strip()[:5000]
        closing_text = str(data.get("closing_text", "")).strip()[:5000]
        order_rows = sanitize_order_rows(data.get("order_rows"))
        if not subject or not intro_text:
            return jsonify({"ok": False, "error": "missing_email_content"}), 400

        product_sheet_url = safe_http_url(current_draft["links"].get("product_sheet_url"))
        stockfiller_url = safe_http_url(current_draft["links"].get("stockfiller_url"))
        product_sheet_label = current_draft["cta_labels"].get("product_sheet") or "Se Produktblad"
        stockfiller_label = current_draft["cta_labels"].get("stockfiller") or "Beställ direkt via Stockfiller"
        email_type = current_draft["email_type"]
        user = current_user()
        if not is_valid_email(user.get("email")):
            return jsonify({"ok": False, "error": "invalid_sender_email"}), 400
        sender_name = str(user.get("name") or user.get("user_name") or "Polarbär").strip()
        sender = {"name": f"{sender_name} på Polarbär", "email": user["email"]}
        reply_to = {"name": user.get("name") or sender["name"], "email": user["email"]}
        is_test = EMAIL_SEND_MODE != "live"
        if is_test and not is_valid_email(EMAIL_TEST_RECIPIENT):
            return jsonify({"ok": False, "error": "invalid_test_recipient"}), 500

        first_rendered = render_email_proposal(
            greeting_name=selected[0].get("greeting_name"),
            subject=subject,
            intro_text=intro_text,
            closing_text=closing_text,
            order_rows=order_rows,
            product_sheet_url=product_sheet_url,
            stockfiller_url=stockfiller_url,
            sender=user,
            product_sheet_label=product_sheet_label,
            stockfiller_label=stockfiller_label,
        )
        created_at = draft_created_at or now_text()
        append_dict_row(sheets[EMAIL_MESSAGES_SHEET], EMAIL_MESSAGES_COLUMNS, {
            "email_id": draft_id,
            "customer": current_draft["customer"]["customer"],
            "customer_number": current_draft["customer"].get("customer_number", ""),
            "email_type": email_type,
            "sender_user_name": user.get("user_name", ""),
            "sender_name": user.get("name", ""),
            "sender_email": user.get("email", ""),
            "subject": subject,
            "body_text": first_rendered["text"],
            "body_html": first_rendered["html"],
            "latest_order_reference": current_draft.get("latest_order_reference", ""),
            "latest_delivery_date": current_draft.get("latest_delivery_date", ""),
            "product_sheet_url": product_sheet_url,
            "stockfiller_url": stockfiller_url,
            "is_test": "Y" if is_test else "N",
            "recipient_count": len(selected),
            "status": "pending",
            "created_at": created_at,
            "sent_at": "",
        })

        successes = []
        failures = []
        for recipient in selected:
            intended_email = recipient["email"]
            actual_email = EMAIL_TEST_RECIPIENT if is_test else intended_email
            rendered = render_email_proposal(
                greeting_name=recipient.get("greeting_name"),
                subject=subject,
                intro_text=intro_text,
                closing_text=closing_text,
                order_rows=order_rows,
                product_sheet_url=product_sheet_url,
                stockfiller_url=stockfiller_url,
                sender=user,
                product_sheet_label=product_sheet_label,
                stockfiller_label=stockfiller_label,
            )
            outgoing_subject = rendered["subject"]
            if is_test:
                outgoing_subject = f"[TEST – avsett för {intended_email}] {outgoing_subject}"
            sent_at = now_text()
            message_id = ""
            error_text = ""
            try:
                message_id = send_brevo_transactional_email(
                    sender=sender,
                    recipient_email=actual_email,
                    recipient_name=recipient.get("greeting_name") or actual_email,
                    reply_to=reply_to,
                    subject=outgoing_subject,
                    html_body=rendered["html"],
                    text_body=rendered["text"],
                    email_id=draft_id,
                    email_type=email_type,
                )
                successes.append(intended_email)
            except Exception as exc:
                error_text = str(exc)[:500]
                failures.append({"email": intended_email, "error": error_text})
            append_dict_row(sheets[EMAIL_RECIPIENTS_SHEET], EMAIL_RECIPIENTS_COLUMNS, {
                "email_id": draft_id,
                "customer": current_draft["customer"]["customer"],
                "intended_email": intended_email,
                "actual_email": actual_email,
                "greeting_name": recipient.get("greeting_name", ""),
                "brevo_message_id": message_id,
                "send_status": "sent" if message_id else "failed",
                "send_error": error_text,
                "sent_at": sent_at if message_id else "",
            })

        status = "failed" if not successes else ("partial" if failures else "sent")
        sent_at = now_text() if successes else ""
        message_row, message_headers, _ = find_sheet_row(sheets[EMAIL_MESSAGES_SHEET], "email_id", draft_id)
        if message_row:
            update_sheet_row(sheets[EMAIL_MESSAGES_SHEET], message_row, message_headers, {
                "status": status,
                "sent_at": sent_at,
            })
        if successes and not is_test:
            build_sales_activity_for_email(
                spreadsheet,
                email_id=draft_id,
                email_type=email_type,
                customer_name=current_draft["customer"]["customer"],
                user=user,
                recipients=successes,
                partial=bool(failures),
            )
        response_payload = {
            "ok": bool(successes),
            "email_id": draft_id,
            "email_type": email_type,
            "email_type_label": current_draft["email_type_label"],
            "status": status,
            "sent": successes,
            "failed": failures,
            "is_test": is_test,
            "test_recipient": EMAIL_TEST_RECIPIENT if is_test else "",
        }
        return jsonify(response_payload), (200 if successes else 502)
    finally:
        with _active_send_lock:
            _active_send_ids.discard(draft_id)


def _event_semantic_key(event):
    return email_event_key(
        event.get("brevo_message_id"),
        event.get("event_type"),
        event.get("event_time"),
        event.get("url"),
        event.get("actual_email"),
    )


def _recipient_summary(recipient, event_rows):
    """Derive summary fields from the append-only raw log, making retries idempotent."""
    ordered = sorted(event_rows, key=lambda row: str(row.get("event_time") or ""))
    times_by_type = defaultdict(list)
    for event in ordered:
        event_type = str(event.get("event_type") or "").strip().casefold()
        event_time = str(event.get("event_time") or "").strip()
        if event_time:
            times_by_type[event_type].append(event_time)

    updates = {
        "delivered_at": (times_by_type["delivered"] or [""])[0],
        "first_opened_at": (times_by_type["opened"] or [""])[0],
        "last_opened_at": (times_by_type["opened"] or [""])[-1],
        "open_count": len(times_by_type["opened"]),
        "product_sheet_first_clicked_at": (times_by_type["product_sheet_clicked"] or [""])[0],
        "product_sheet_last_clicked_at": (times_by_type["product_sheet_clicked"] or [""])[-1],
        "product_sheet_click_count": len(times_by_type["product_sheet_clicked"]),
        "stockfiller_first_clicked_at": (times_by_type["stockfiller_clicked"] or [""])[0],
        "stockfiller_last_clicked_at": (times_by_type["stockfiller_clicked"] or [""])[-1],
        "stockfiller_click_count": len(times_by_type["stockfiller_clicked"]),
        "bounce_type": "",
        "blocked_at": "",
        "unsubscribed_at": (times_by_type["unsubscribed"] or [""])[-1],
        "last_event_at": str(ordered[-1].get("event_time") or "") if ordered else "",
    }
    for event in ordered:
        event_type = str(event.get("event_type") or "").strip().casefold()
        if event_type in {"hardbounce", "invalid", "blocked", "spam"}:
            updates["bounce_type"] = event_type
        if event_type in {"blocked", "spam"}:
            updates["blocked_at"] = str(event.get("event_time") or "")
    return {**recipient, **updates}


def process_brevo_events(spreadsheet, sheets, payloads):
    """Persist a batch with one read/append/update cycle and semantic deduplication."""
    message_headers, message_rows = worksheet_snapshot(
        sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS
    )
    recipient_headers, recipient_rows = worksheet_snapshot(
        sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
    )
    _, stored_event_rows = worksheet_snapshot(
        sheets[EMAIL_EVENTS_SHEET], expected_columns=EMAIL_EVENTS_COLUMNS
    )
    messages_by_email_id = {row.get("email_id", ""): row for _, row in message_rows}
    recipients_by_message_id = {
        normalize_message_id(row.get("brevo_message_id")): (row_index, row)
        for row_index, row in recipient_rows
        if normalize_message_id(row.get("brevo_message_id"))
    }
    all_events = [row for _, row in stored_event_rows]
    existing_keys = {_event_semantic_key(row) for row in all_events}
    new_events = []
    affected_message_ids = set()

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        message_id = normalize_message_id(
            payload.get("message-id") or payload.get("messageId") or payload.get("message_id")
        )
        recipient_info = recipients_by_message_id.get(message_id)
        recipient = recipient_info[1] if recipient_info else {}
        email_id = recipient.get("email_id", "")
        message = messages_by_email_id.get(email_id, {})
        event_type = normalize_brevo_event(payload)
        url = str(payload.get("link") or payload.get("url") or "").strip()
        if event_type == "clicked":
            event_type = classify_clicked_url(
                url,
                message.get("product_sheet_url", ""),
                message.get("stockfiller_url", ""),
            )
        actual_email = recipient.get("actual_email") or payload.get("email", "")
        event = {
            "received_at": now_text(),
            "event_time": brevo_event_time(payload),
            "email_id": email_id,
            "brevo_message_id": message_id,
            "intended_email": recipient.get("intended_email", ""),
            "actual_email": actual_email,
            "event_type": event_type,
            "url": url,
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True)[:45000],
        }
        event["event_key"] = _event_semantic_key(event)
        if message_id:
            affected_message_ids.add(message_id)
        if event["event_key"] in existing_keys:
            continue
        existing_keys.add(event["event_key"])
        new_events.append(event)
        all_events.append(event)

    if new_events:
        event_sheet = sheets[EMAIL_EVENTS_SHEET]
        event_sheet.append_rows(
            [build_worksheet_row(EMAIL_EVENTS_COLUMNS, event) for event in new_events],
            value_input_option="RAW",
        )

    events_by_message_id = defaultdict(list)
    for event in all_events:
        message_id = normalize_message_id(event.get("brevo_message_id"))
        if message_id in affected_message_ids:
            events_by_message_id[message_id].append(event)

    recipient_updates = []
    for message_id in affected_message_ids:
        recipient_info = recipients_by_message_id.get(message_id)
        if not recipient_info:
            continue
        row_index, recipient = recipient_info
        recipient_updates.append((row_index, _recipient_summary(recipient, events_by_message_id[message_id])))
    batch_update_sheet_rows(sheets[EMAIL_RECIPIENTS_SHEET], recipient_headers, recipient_updates)
    return len(new_events)


def process_brevo_event(spreadsheet, sheets, payload):
    return bool(process_brevo_events(spreadsheet, sheets, [payload]))


def _process_brevo_batch_with_retry(payloads):
    def operation():
        spreadsheet = get_spreadsheet_with_retry()
        sheets = ensure_email_worksheets(spreadsheet)
        with _brevo_processing_lock:
            return process_brevo_events(spreadsheet, sheets, payloads)

    return run_with_retry(operation, label="Brevo event batch")


def _brevo_event_worker():
    while True:
        first = _brevo_event_queue.get()
        batch = [first]
        try:
            # Small coalescing window greatly reduces Sheets calls during webhook bursts.
            time.sleep(0.05)
            while len(batch) < 100:
                try:
                    batch.append(_brevo_event_queue.get_nowait())
                except Empty:
                    break
            _process_brevo_batch_with_retry(batch)
        except Exception:
            app.logger.exception("Brevo event batch failed after retries")
        finally:
            for _ in batch:
                _brevo_event_queue.task_done()


def fetch_brevo_events(message_id):
    api_key = str(os.environ.get("BREVO_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("BREVO_API_KEY is missing")
    normalized_id = normalize_message_id(message_id)

    def operation():
        response = requests.get(
            BREVO_EVENTS_URL,
            headers={"api-key": api_key, "accept": "application/json"},
            params={"messageId": f"<{normalized_id}>", "limit": 500, "sort": "asc"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            event.setdefault("messageId", normalized_id)
        return events

    return run_with_retry(operation, attempts=4, base_delay=1, label=f"Brevo API {normalized_id}")


def reconcile_recent_brevo_events(*, days=None, max_recipients=None):
    if not _brevo_reconcile_lock.acquire(blocking=False):
        return {"ok": True, "status": "already_running"}
    try:
        days = int(days or BREVO_RECONCILE_DAYS)
        max_recipients = int(max_recipients or BREVO_RECONCILE_MAX_RECIPIENTS)

        def load_recent_recipients():
            spreadsheet = get_spreadsheet_with_retry()
            sheets = ensure_email_worksheets(spreadsheet)
            return worksheet_snapshot(
                sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
            )[1]

        recipient_rows = run_with_retry(
            load_recent_recipients, label="Brevo reconciliation Sheets read"
        )
        cutoff = stockholm_today() - timedelta(days=max(1, min(30, days)))
        candidates = []
        for _, recipient in recipient_rows:
            message_id = normalize_message_id(recipient.get("brevo_message_id"))
            sent_at = parse_datetime_value(recipient.get("sent_at"))
            if (
                message_id
                and str(recipient.get("send_status") or "").casefold() == "sent"
                and sent_at
                and sent_at.date() >= cutoff
            ):
                candidates.append((sent_at, message_id))
        candidates.sort(reverse=True)

        fetched = []
        failures = []
        seen_message_ids = set()
        for _, message_id in candidates:
            if message_id in seen_message_ids or len(seen_message_ids) >= max_recipients:
                continue
            seen_message_ids.add(message_id)
            try:
                fetched.extend(fetch_brevo_events(message_id))
            except Exception as exc:
                failures.append({"message_id": message_id, "error": str(exc)[:250]})
                app.logger.warning("Could not reconcile Brevo message %s: %s", message_id, exc)

        inserted = _process_brevo_batch_with_retry(fetched) if fetched else 0
        return {
            "ok": True,
            "status": "completed",
            "checked_recipients": len(seen_message_ids),
            "fetched_events": len(fetched),
            "inserted_events": inserted,
            "failures": failures,
        }
    finally:
        _brevo_reconcile_lock.release()


def _brevo_reconcile_worker():
    while True:
        time.sleep(BREVO_RECONCILE_INTERVAL_SECONDS)
        try:
            reconcile_recent_brevo_events()
        except Exception:
            app.logger.exception("Scheduled Brevo reconciliation failed")


def start_brevo_background_workers():
    global _brevo_workers_started
    with _brevo_worker_start_lock:
        if _brevo_workers_started:
            return
        threading.Thread(target=_brevo_event_worker, name="brevo-events", daemon=True).start()
        threading.Thread(target=_brevo_reconcile_worker, name="brevo-reconcile", daemon=True).start()
        _brevo_workers_started = True


@app.route("/api/brevo/webhook/<secret>", methods=["POST"])
def brevo_webhook(secret):
    expected = str(os.environ.get("BREVO_WEBHOOK_SECRET", "")).strip()
    if not expected:
        return jsonify({"ok": False, "error": "webhook_not_configured"}), 503
    if secret != expected:
        return jsonify({"ok": False, "error": "not_found"}), 404
    payload = request.get_json(silent=True)
    events = payload if isinstance(payload, list) else [payload or {}]
    start_brevo_background_workers()
    queued = 0
    try:
        for event in events:
            _brevo_event_queue.put_nowait(event)
            queued += 1
    except Full:
        return jsonify({"ok": False, "error": "event_queue_full", "queued": queued}), 503
    return jsonify({"ok": True, "queued": queued}), 202


@app.route("/api/brevo/reconcile/<secret>", methods=["POST"])
def brevo_reconcile(secret):
    expected = str(os.environ.get("BREVO_WEBHOOK_SECRET", "")).strip()
    if not expected:
        return jsonify({"ok": False, "error": "webhook_not_configured"}), 503
    if secret != expected:
        return jsonify({"ok": False, "error": "not_found"}), 404
    result = reconcile_recent_brevo_events(
        days=request.args.get("days", type=int),
        max_recipients=request.args.get("max_recipients", type=int),
    )
    return jsonify(result), (202 if result.get("status") == "already_running" else 200)


@app.route("/customers", methods=["GET"])
def get_customers():
    spreadsheet = get_spreadsheet_with_retry()
    sheet = spreadsheet.worksheet("customers_enriched")
    all_rows = sheet.get_all_values()
    headers = all_rows[0]

    # Build latest contact/follow_up_date per customer from sales_activities
    contact_rows = get_contact_rows(spreadsheet)
    latest_contact = {}
    latest_contact_followup = build_latest_contact_followups(contact_rows)
    for c in contact_rows:
        name = c["customer"].strip().lower()
        dt = parse_date_value(c["date_time"])
        if dt and (name not in latest_contact or dt > latest_contact[name]):
            latest_contact[name] = dt

    customers = []
    for i, row in enumerate(all_rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        customer = {col: d.get(col, "") for col in CUSTOMER_COLUMNS}
        customer["latitude"]  = parse_coordinate_value(d.get("latitude_google") or d.get("latitude",  ""), "latitude")
        customer["longitude"] = parse_coordinate_value(d.get("longitude_google") or d.get("longitude", ""), "longitude")
        addr = d.get("address_google", "").strip()
        num  = d.get("address_number_google", "").strip()
        customer["address_google"] = addr
        customer["address_number_google"] = num
        customer["city_google"] = d.get("city_google", "").strip()
        customer["postal_code_google"] = d.get("postal_code_google", "").strip()
        customer["region_google"] = d.get("region_google", "").strip()
        customer["address"] = f"{addr} {num}".strip()
        customer["city"] = customer["city_google"] or d.get("city", "")
        customer_key = customer["customer"].strip().lower()
        customer["latest_contact_date"] = format_date_value(latest_contact.get(customer_key))
        customer["follow_up_date"] = format_date_value(latest_contact_followup.get(normalize_key(customer_key)))
        customers.append({"row": i, **customer})
    return jsonify(customers)


@app.route("/contact-log", methods=["GET"])
def get_contact_log():
    spreadsheet = get_spreadsheet_with_retry()
    contact_rows = get_contact_rows(spreadsheet)
    filters = get_contact_log_filter_values(request.args)
    return jsonify(build_contact_log_payload(contact_rows, filters))


@app.route("/contact-log/export", methods=["GET"])
def export_contact_log():
    spreadsheet = get_spreadsheet_with_retry()
    contact_rows = get_contact_rows(spreadsheet)
    filters = get_contact_log_filter_values(request.args)
    payload = build_contact_log_payload(contact_rows, filters)
    workbook = build_xlsx(payload["columns"], payload["rows"])
    filename = f"kontaktlogg_{stockholm_today().isoformat()}.xlsx"
    return Response(
        workbook,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _timeline_sort_value(item):
    return parse_datetime_value(item.get("date_time")) or datetime.min


def _timeline_contact_item(contact):
    result = str(contact.get("result", "")).strip()
    channel = str(contact.get("contact_channel", "")).strip()
    details = []
    if contact.get("follow_up_date"):
        details.append({"label": "Nästa uppföljning", "value": format_date_value(contact["follow_up_date"])})
    return {
        "date_time": str(contact.get("date_time", "")).strip(),
        "event_type": "contact",
        "title": result or channel or "Kundkontakt",
        "sales_person": str(contact.get("sales_person", "")).strip(),
        "channel": channel,
        "result": result,
        "recipient": str(contact.get("customer_contact_person", "")).strip(),
        "comment": str(contact.get("comment", "")).strip(),
        "details": details,
    }


def build_customer_timeline(customer_name, order_rows, contact_rows, sheets):
    """Build the customer-specific, user-facing activity stream.

    Raw delivery, bounce and other technical Brevo events remain in email_events;
    this projection intentionally exposes only the V1 events approved for the UI.
    """
    customer_key = normalize_key(customer_name)
    timeline = []

    for contact in contact_rows:
        if normalize_key(contact.get("customer")) != customer_key:
            continue
        # Email proposal sends have their own richer timeline item below.
        if str(contact.get("email_id", "")).strip():
            continue
        timeline.append(_timeline_contact_item(contact))

    message_rows = worksheet_to_dicts(
        sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS
    )
    recipient_rows = worksheet_to_dicts(
        sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
    )
    messages = []
    recipients_by_email_id = {}
    for recipient in recipient_rows:
        recipients_by_email_id.setdefault(str(recipient.get("email_id", "")).strip(), []).append(recipient)

    for message in message_rows:
        if normalize_key(message.get("customer")) != customer_key or is_yes(message.get("is_test")):
            continue
        if str(message.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue
        sent_at = parse_datetime_value(message.get("sent_at"))
        if not sent_at:
            continue
        messages.append((sent_at, message))
        email_id = str(message.get("email_id", "")).strip()
        sent_recipients = [
            row for row in recipients_by_email_id.get(email_id, [])
            if str(row.get("send_status", "")).strip().casefold() == "sent"
        ]
        recipient_label = ", ".join(row.get("intended_email", "") for row in sent_recipients if row.get("intended_email"))
        partial = str(message.get("status", "")).strip().casefold() == "partial"
        email_type = normalize_proposal_type(message.get("email_type"))
        type_label = EMAIL_PROPOSAL_TYPES[email_type]
        timeline.append({
            "date_time": message.get("sent_at", ""),
            "event_type": "email_proposal_sent",
            "title": f"{type_label} skickad" + (" delvis" if partial else ""),
            "sales_person": message.get("sender_name", ""),
            "channel": "Mejl",
            "result": f"Mejlförslag skickat – {type_label}" + (" delvis" if partial else ""),
            "recipient": recipient_label,
            "comment": message.get("subject", ""),
            "email_id": email_id,
            "details": [
                {"label": "Ämne", "value": message.get("subject", "") or "—"},
            ],
        })

        for recipient in sent_recipients:
            intended_email = str(recipient.get("intended_email", "")).strip()
            event_specs = (
                ("open_count", "last_opened_at", "email_proposal_opened", "Öppnat"),
                ("product_sheet_click_count", "product_sheet_last_clicked_at", "product_sheet_clicked", "Produktblad klickat"),
                ("stockfiller_click_count", "stockfiller_last_clicked_at", "stockfiller_clicked", "Stockfiller klickat"),
            )
            for count_field, time_field, event_type, label in event_specs:
                count = int(parse_number_value(recipient.get(count_field), 0))
                event_time = str(recipient.get(time_field, "")).strip()
                if count < 1 or not event_time:
                    continue
                title = f"{label} {count} gånger" if count > 1 else label
                timeline.append({
                    "date_time": event_time,
                    "event_type": event_type,
                    "title": title,
                    "sales_person": message.get("sender_name", ""),
                    "channel": "Mejl",
                    "result": title,
                    "recipient": intended_email,
                    "comment": "",
                    "email_id": email_id,
                    "details": [
                        {"label": "Antal", "value": str(count)},
                    ],
                })

    # Attribute each order to the latest live email proposal sent 0–10 days earlier.
    grouped_orders = {}
    for index, order in enumerate(order_rows):
        if normalize_key(order.get("Customer")) != customer_key:
            continue
        order_date = parse_date_value(order.get("Order date"))
        if not order_date:
            continue
        reference = str(order.get("Reference", "")).strip()
        group_key = reference or f"{order_date.isoformat()}:{index}"
        group = grouped_orders.setdefault(group_key, {
            "reference": reference,
            "date": order_date,
            "total": 0.0,
            "currency": "",
            "dfp": 0.0,
        })
        group["total"] += parse_number_value(order.get("Total"), 0)
        group["currency"] = group["currency"] or str(order.get("Currency", "")).strip()
        if str(order.get("Unit", "")).strip().casefold() == "dfp":
            group["dfp"] += parse_number_value(order.get("Quantity"), 0)

    for order in grouped_orders.values():
        eligible = [
            (sent_at, message) for sent_at, message in messages
            if 0 <= (order["date"] - sent_at.date()).days <= 10
        ]
        if not eligible:
            continue
        _, attributed_message = max(eligible, key=lambda item: item[0])
        value = round(order["total"], 2)
        value_text = f"{value:,.2f}".replace(",", " ").replace(".", ",")
        if order["currency"]:
            value_text += f" {order['currency']}"
        details = [
            {"label": "Orderreferens", "value": order["reference"] or "—"},
            {"label": "Ordervärde", "value": value_text},
        ]
        if order["dfp"]:
            details.append({"label": "Antal DFP", "value": str(int(order["dfp"]) if order["dfp"].is_integer() else order["dfp"])})
        timeline.append({
            "date_time": f"{order['date'].isoformat()} 12:00:00",
            "event_type": "subsequent_order",
            "title": "Efterföljande order",
            "sales_person": attributed_message.get("sender_name", ""),
            "channel": "Order",
            "result": "Order inom 10 dagar",
            "recipient": "",
            "comment": f"Order {order['reference']}" if order["reference"] else "Ny order",
            "email_id": attributed_message.get("email_id", ""),
            "details": details,
        })

    timeline.sort(key=_timeline_sort_value, reverse=True)
    return timeline


@app.route("/customers/<customer_name>/stats", methods=["GET"])
def get_customer_stats(customer_name):
    customer_name = unquote(customer_name).strip().lower()
    spreadsheet = get_spreadsheet_with_retry()

    # Orders
    order_rows = get_order_rows(spreadsheet)
    total_sales = 0.0
    latest_order_date = None
    currency = ""

    unique_references = set()
    for o in order_rows:
        if o["Customer"].strip().lower() != customer_name:
            continue
        try:
            cleaned = "".join(c for c in o["Total"] if c.isdigit() or c in ".,").replace(",", ".")
            if cleaned:
                total_sales += float(cleaned)
        except ValueError:
            pass
        if not currency and o["Currency"].strip():
            currency = o["Currency"].strip()
        d = parse_date_value(o["Order date"])
        if d and (latest_order_date is None or d > latest_order_date):
            latest_order_date = d
        if o["Reference"].strip():
            unique_references.add(o["Reference"].strip())

    # Contacts
    contact_rows = get_contact_rows(spreadsheet)
    contacts = []
    for c in contact_rows:
        if c["customer"].strip().lower() != customer_name:
            continue
        contact = {k: c[k] for k in ("customer", "date_time", "sales_person", "contact_channel", "result", "comment", "customer_contact_person", "follow_up_date",
                                     *FREEZER_COLUMNS)}
        contact["_sort_date"] = parse_date_value(c["date_time"]) or date.min
        contact["date_time"] = format_date_value(c["date_time"])
        contact["follow_up_date"] = format_date_value(c["follow_up_date"])
        contacts.append(contact)
    contacts.sort(key=lambda x: x["_sort_date"], reverse=True)
    for contact in contacts:
        contact.pop("_sort_date", None)

    sheets = ensure_email_worksheets(spreadsheet)
    timeline = build_customer_timeline(customer_name, order_rows, contact_rows, sheets)

    return jsonify({
        "total_sales": round(total_sales, 2),
        "latest_order_date": format_date_value(latest_order_date, fallback="—"),
        "currency": currency,
        "order_count": len(unique_references),
        "contacts": contacts,
        "timeline": timeline,
    })


@app.route("/customer-insights", methods=["GET"])
def get_customer_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = stockholm_today()
    customers = get_customer_rows(spreadsheet)

    contact_rows = get_contact_rows(spreadsheet)
    latest_contact_followup = build_latest_contact_followups(contact_rows)
    message_rows, recipient_rows, _ = get_email_rows(spreadsheet)
    latest_live_proposals = latest_live_email_proposals_by_customer(message_rows)
    blocked_recipients = blocked_recipient_reasons(recipient_rows)

    # Latest order date and order count per customer
    order_rows = get_order_rows(spreadsheet)
    latest_order = {}
    latest_delivery = {}
    order_references = defaultdict(set)
    for o in order_rows:
        name = normalize_key(o["Customer"])
        is_ordered = (
            parse_number_value(o.get("Quantity"), 0) > 0
            or parse_number_value(o.get("Total"), 0) > 0
        )
        if not name or not is_ordered:
            continue
        d = parse_date_value(o["Order date"])
        dd = parse_date_value(o["Delivery date"])
        ref = o["Reference"].strip()
        if d and (name not in latest_order or d > latest_order[name]):
            latest_order[name] = d
        if dd and (name not in latest_delivery or dd > latest_delivery[name]):
            latest_delivery[name] = dd
        if ref:
            order_references[name].add(ref)

    order_features = build_order_features(order_rows)
    contact_features = build_contact_features(contact_rows, order_features)
    priority_customers = build_priority_customers(
        customers,
        order_features,
        contact_features,
        None,
        today,
        limit=len(customers),
    )
    priority_by_name = {
        normalize_key(customer["customer"]): customer
        for customer in priority_customers
    }
    customers_by_name = {
        normalize_key(customer["customer"]): customer
        for customer in customers
    }

    # Compute insights for all customers
    all_names = (
        set(latest_contact_followup.keys())
        | set(latest_order.keys())
        | set(order_references.keys())
        | set(latest_delivery.keys())
        | {normalize_key(c["customer"]) for c in customers if normalize_key(c.get("customer"))}
    )
    insights = {}
    for name in all_names:
        # missad_uppfoljning
        nf = latest_contact_followup.get(normalize_key(name))
        missad = bool(nf and nf < today)

        # customer_risk — based on most recent of order date or delivery date
        lo = latest_order.get(name)
        ld_check = latest_delivery.get(name)
        count = len(order_references.get(name, set()))
        risk = calculate_customer_risk(count, lo, ld_check, today)

        ld = latest_delivery.get(name)
        latest_delivery_date = format_date_value(ld)
        priority = priority_by_name.get(normalize_key(name), {})
        customer = customers_by_name.get(normalize_key(name), {"customer": name})
        has_prior_order = bool(priority.get("order_count", 0) or name in order_references)
        days_since_delivery = (today - ld).days if ld else None
        email_type = (
            "new_customer"
            if not has_prior_order
            else "reminder"
            if ld and days_since_delivery <= EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS
            else "reactivation"
        )
        relationship = {
            "email_type": email_type,
            "email_type_label": EMAIL_PROPOSAL_TYPES[email_type],
            "latest_delivery_date": latest_delivery_date,
            "days_since_delivery": days_since_delivery,
            "has_prior_order": has_prior_order,
        }
        email_proposal = build_email_proposal_status(
            customer,
            priority,
            relationship,
            latest_live_proposals,
            blocked_recipients,
            today,
        )
        insights[name] = {
            "missad_uppfoljning": missad,
            "customer_risk": risk,
            "priority_level": priority.get("priority_level", ""),
            "priority_score": priority.get("priority_score"),
            "priority_type": priority.get("priority_type", ""),
            "recommended_action": priority.get("recommended_action", ""),
            "reasons": priority.get("reasons", []),
            "next_action": priority.get("next_action", {}),
            "order_count": priority.get("order_count", 0),
            "total_dfp": priority.get("total_dfp"),
            "expected_order_dfp": priority.get("expected_order_dfp"),
            "latest_order_date": priority.get("latest_order_date", ""),
            "latest_delivery_date": latest_delivery_date,
            "latest_delivery_month": latest_delivery_date[:7] if latest_delivery_date else "",  # "YYYY-MM"
            "expected_cycle_days": priority.get("expected_cycle_days"),
            "expected_cycle_source": priority.get("expected_cycle_source", ""),
            "expected_next_order_date": priority.get("expected_next_order_date", ""),
            "overdue_days": priority.get("overdue_days"),
            "days_since_delivery": priority.get("days_since_delivery"),
            "latest_contact_date": priority.get("latest_contact_date", ""),
            "latest_contact_result": priority.get("latest_contact_result", ""),
            "latest_contact_class": priority.get("latest_contact_class", ""),
            "latest_contact_channel": priority.get("latest_contact_channel", ""),
            "follow_up_due": priority.get("follow_up_due", False),
            "has_order_after_latest_contact": priority.get("has_order_after_latest_contact", False),
            "email_proposal_due": email_proposal["due"],
            "email_proposal_type": email_proposal["email_type"],
            "email_proposal_type_label": email_proposal["email_type_label"],
            "email_proposal_reason": email_proposal["reason"],
            "email_proposal_blockers": email_proposal["blockers"],
            "email_proposal_recipient_count": email_proposal["eligible_recipient_count"],
            "email_proposal_latest_sent_at": email_proposal["latest_sent_at"],
            # Compatibility for clients deployed before the broader proposal flow.
            "reminder_email_due": email_proposal["due"] and email_type == "reminder",
            "reminder_email_reason": email_proposal["reason"] if email_type == "reminder" else "",
            "reminder_email_blockers": email_proposal["blockers"],
            "reminder_email_recipient_count": email_proposal["eligible_recipient_count"],
            "reminder_email_latest_sent_at": email_proposal["latest_sent_at"],
        }

    return jsonify(insights)


@app.route("/followup-insights", methods=["GET"])
def get_followup_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = stockholm_today()
    weeks = build_recent_weeks(today)
    week_keys = {w["key"] for w in weeks}
    current_week_key = weeks[-1]["key"]
    previous_week_key = weeks[-2]["key"] if len(weeks) > 1 else ""
    selected_responsible = request.args.get("responsible", "").strip()

    customers_by_name = {}
    customers = get_customer_rows(spreadsheet)
    for customer in customers:
        customers_by_name[normalize_key(customer["customer"])] = customer

    contact_rows = get_contact_rows(spreadsheet)
    order_rows = get_order_rows(spreadsheet)
    dfp_top_weeks_2026 = build_dfp_top_weeks(order_rows, year=2026, limit=5)

    responsible_options = sorted({
        c["sales_person"] for c in customers_by_name.values() if c["sales_person"]
    })

    def customer_belongs_to_selected(customer_name):
        customer = customers_by_name.get(normalize_key(customer_name))
        if not customer or not customer["sales_person"]:
            return False
        if not selected_responsible:
            return True
        return customer["sales_person"] == selected_responsible

    def contact_belongs_to_selected(contact):
        return customer_belongs_to_selected(contact["customer"])

    # DFP leaderboard is intentionally global and ignores the selected responsible filter.
    # It sums Total weight for every order row by the customer's responsible salesperson.
    dfp_counts = {w["key"]: defaultdict(float) for w in weeks}
    dfp_team_totals = {w["key"]: 0.0 for w in weeks}
    for order in order_rows:
        order_date = parse_date_value(order["Order date"])
        if not order_date:
            continue
        key = week_key(order_date)
        if key not in week_keys:
            continue

        total_weight = parse_number_value(order["Total weight"], default=0.0)
        if total_weight <= 0:
            continue

        dfp_team_totals[key] += total_weight

        customer = customers_by_name.get(normalize_key(order["Customer"]))
        if not customer or not customer["sales_person"]:
            continue
        responsible = customer["sales_person"]
        dfp_counts[key][responsible] += total_weight

    dfp_leaderboard = []
    for w in weeks:
        leaders = sorted(dfp_counts[w["key"]].items(), key=lambda item: (-item[1], item[0]))[:3]
        dfp_leaderboard.append({
            "week_key": w["key"],
            "label": w["label"],
            "team_total_dfp": format_dfp_count(dfp_team_totals[w["key"]]),
            "leaders": [
                {
                    "rank": idx + 1,
                    "sales_person": name,
                    "dfp_count": format_dfp_count(count),
                }
                for idx, (name, count) in enumerate(leaders)
            ],
        })

    contact_count_by_week = {w["key"]: 0 for w in weeks}
    positive_count_by_week = {w["key"]: 0 for w in weeks}
    contact_dates_by_customer = defaultdict(list)

    for contact in contact_rows:
        contact_date = parse_date_value(contact["date_time"])
        if not contact_date:
            continue

        customer_key = normalize_key(contact["customer"])
        if contact_belongs_to_selected(contact):
            contact_dates_by_customer[customer_key].append(contact_date)
            key = week_key(contact_date)
            if key in week_keys:
                contact_count_by_week[key] += 1
                if is_positive_contact(contact["result"]):
                    positive_count_by_week[key] += 1

    for dates in contact_dates_by_customer.values():
        dates.sort()

    current_contacts = contact_count_by_week.get(current_week_key, 0)
    previous_contacts = contact_count_by_week.get(previous_week_key, 0)
    if previous_contacts == 0:
        contact_delta_percent = 100 if current_contacts > 0 else 0
    else:
        contact_delta_percent = round(((current_contacts - previous_contacts) / previous_contacts) * 100)

    latest_order = {}
    latest_delivery = {}
    order_count_by_customer = defaultdict(int)
    orders_after_contact_by_week = {w["key"]: set() for w in weeks}

    for idx, order in enumerate(order_rows):
        customer_key = normalize_key(order["Customer"])
        order_date = parse_date_value(order["Order date"])
        delivery_date = parse_date_value(order["Delivery date"])
        ref = order["Reference"].strip() or f"row-{idx}"

        if order_date:
            if customer_key not in latest_order or order_date > latest_order[customer_key]:
                latest_order[customer_key] = order_date
        if delivery_date:
            if customer_key not in latest_delivery or delivery_date > latest_delivery[customer_key]:
                latest_delivery[customer_key] = delivery_date
        if ref:
            order_count_by_customer[customer_key] += 1

        if not order_date or not customer_belongs_to_selected(order["Customer"]):
            continue

        key = week_key(order_date)
        if key not in week_keys:
            continue

        prior_contacts = [d for d in contact_dates_by_customer.get(customer_key, []) if d <= order_date]
        if not prior_contacts:
            continue
        latest_prior_contact = prior_contacts[-1]
        if 0 <= (order_date - latest_prior_contact).days <= 10:
            orders_after_contact_by_week[key].add(ref)

    order_features = build_order_features(order_rows)
    contact_features = build_contact_features(contact_rows, order_features)
    priority_customers = build_priority_customers(
        customers,
        order_features,
        contact_features,
        selected_responsible or None,
        today,
        limit=30,
    )

    return jsonify({
        "generated_at": stockholm_now().isoformat(timespec="minutes"),
        "selected_responsible": selected_responsible,
        "responsible_options": responsible_options,
        "weeks": weeks,
        "dfp_leaderboard": dfp_leaderboard,
        "dfp_top_weeks_2026": dfp_top_weeks_2026,
        "freezer_summary": build_freezer_summary(contact_rows),
        "contacts": {
            "current_week_count": current_contacts,
            "previous_week_count": previous_contacts,
            "delta_percent": contact_delta_percent,
            "delta_is_positive": current_contacts >= previous_contacts,
            "positive_by_week": [
                {"week_key": w["key"], "label": w["label"], "count": positive_count_by_week[w["key"]]}
                for w in weeks
            ],
            "orders_after_contact_by_week": [
                {"week_key": w["key"], "label": w["label"], "count": len(orders_after_contact_by_week[w["key"]])}
                for w in weeks
            ],
        },
        "priority_customers": priority_customers,
    })


@app.route("/customers/<int:row>/contact", methods=["PATCH"])
def update_customer_contact(row):
    data = request.get_json() or {}
    sheet = get_spreadsheet_with_retry().worksheet("customers_enriched")
    headers = sheet.row_values(1)
    if "name" in data:
        headers = ensure_customer_name_column(sheet, headers)

    missing_columns = []
    fields = [
        ("name",                 "name"),
        ("phone",                "phone"),
        ("email",                "email"),
        ("address_google",       "address_google"),
        ("address_number_google","address_number_google"),
        ("city_google",          "city_google"),
        ("postal_code_google",   "postal_code_google"),
        ("region_google",        "region_google"),
        ("comment",              "comment"),
    ]
    address_fields = {"address_google", "address_number_google", "city_google", "postal_code_google", "region_google"}
    address_changed = any(f in data for f in address_fields)

    for field, col_name in fields:
        if field in data and col_name not in headers:
            missing_columns.append(col_name)

    if missing_columns:
        return jsonify({"ok": False, "missing_columns": missing_columns}), 400

    for field, col_name in fields:
        if field in data:
            col_idx = headers.index(col_name) + 1
            if col_name == "comment":
                value = text_to_sheet_value(data[field], max_length=50)
            else:
                value = data[field]
            sheet.update_cell(row, col_idx, value)

    if address_changed:
        # Clear coordinates first
        for coord_col in ("latitude_google", "longitude_google"):
            if coord_col in headers:
                sheet.update_cell(row, headers.index(coord_col) + 1, "")

        # Build full address from updated values + existing sheet values
        existing = dict(zip(headers, sheet.row_values(row)))
        def val(field):
            return data.get(field, existing.get(field, "")).strip()

        address_str = f"{val('address_google')} {val('address_number_google')}, {val('postal_code_google')} {val('city_google')}, Sweden".strip(", ")

        new_lat = new_lng = None
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
        if api_key and address_str:
            try:
                resp = requests.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"address": address_str, "key": api_key, "language": "sv"},
                    timeout=10,
                )
                geo = resp.json()
                if geo.get("results"):
                    loc = geo["results"][0]["geometry"]["location"]
                    new_lat = loc["lat"]
                    new_lng = loc["lng"]
                    lat_value = round(float(new_lat), 7)
                    lng_value = round(float(new_lng), 7)
                    if "latitude_google" in headers:
                        sheet.update_cell(row, headers.index("latitude_google") + 1, lat_value)
                    if "longitude_google" in headers:
                        sheet.update_cell(row, headers.index("longitude_google") + 1, lng_value)
            except Exception:
                pass

    if "modified" in headers:
        sheet.update_cell(row, headers.index("modified") + 1, True)

    result = {"ok": True}
    if address_changed:
        result["latitude"]  = new_lat
        result["longitude"] = new_lng
    return jsonify(result)


@app.route("/customers/<customer_name>/contacts", methods=["POST"])
def add_contact(customer_name):
    customer_name = unquote(customer_name)
    data = request.get_json()
    sheet = get_spreadsheet_with_retry().worksheet("sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    freezer_values = {field: checkbox_to_sheet_value(data.get(field, "")) for field in FREEZER_COLUMNS}
    if not any(freezer_values.values()):
        return jsonify({"ok": False, "error": "freezer_selection_required"}), 400

    row_data = {
        "date_time": data.get("date_time", stockholm_now().strftime("%Y-%m-%d %H:%M")),
        "sales_person": data.get("sales_person", ""),
        "customer": customer_name,
        "contact_channel": data.get("contact_channel", ""),
        "result": data.get("result", ""),
        "comment": data.get("comment", ""),
        "customer_contact_person": data.get("customer_contact_person", ""),
        "follow_up_date": data.get("follow_up_date", ""),
        **freezer_values,
    }
    row = build_worksheet_row(headers, row_data, single_value_columns=FREEZER_COLUMNS)
    sheet.append_row(row)
    return jsonify({"ok": True})


@app.route("/config")
def config():
    return jsonify({"mapsApiKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")})


if __name__ == "__main__":
    start_brevo_background_workers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT") or 5000), debug=False)
