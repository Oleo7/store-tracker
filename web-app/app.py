from flask import Flask, Response, jsonify, send_file, request, send_from_directory
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import unquote
from datetime import datetime, date, timedelta
from collections import defaultdict
from io import BytesIO
import os
import json
import math
import re
import requests
import unicodedata
from zipfile import ZIP_DEFLATED, ZipFile
from xml.sax.saxutils import escape as xml_escape
from dotenv import load_dotenv
from requests.exceptions import ConnectionError as RequestsConnectionError
from priority import (
    build_contact_features,
    build_order_features,
    build_priority_customers,
    normalize_customer_key,
)

load_dotenv()

app = Flask(__name__)
CORS(app)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ.get("SHEET_KEY", "")
IMAGE_DIR = os.path.abspath(os.path.join(app.root_path, "..", "images"))

CUSTOMER_COLUMNS = ["customer", "cancelled_flag", "sales_person", "customer_segment",
                    "customer_reference", "customer_number", "name", "phone", "email",
                    "comment"]

ORDER_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer", "Customer Reference",
                 "Buyer number", "Customer number", "Logistics number", "Address", "Number",
                 "Postal code", "City", "Country", "Phone number", "SKU", "Product", "Weight",
                 "Quantity", "Total weight", "Unit", "Total (Pre-discount)", "Product Discount",
                 "Total", "Currency", "Order Discount (Amount)", "Order Discount (%)", "Batch"]
ORDER_REQUIRED_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer",
                          "Quantity", "Total", "Currency"]

FREEZER_COLUMNS = ["Franui", "Schufrulade", "Boujee", "polarbar", "none"]
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
                   *FREEZER_COLUMNS]
CONTACT_REQUIRED_COLUMNS = ["date_time", "sales_person", "customer", "contact_channel",
                            "result", "comment", "customer_contact_person", "follow_up_date"]


_spreadsheet_cache = None


def checkbox_to_sheet_value(value):
    return "1" if str(value).strip().lower() in {"1", "true", "yes", "on"} else ""


def is_checked_value(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def text_to_sheet_value(value, max_length=None):
    text = str(value or "").strip()
    return text[:max_length] if max_length is not None else text


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
        sheet.insert_cols([[column]], col=len(normalized_headers) + 1)
        normalized_headers.append(column)
    return normalized_headers


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
            item[header] = row[idx] if idx < len(row) else ""
        result.append(item)
    return result


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


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/images/<path:filename>")
def images(filename):
    return send_from_directory(IMAGE_DIR, filename)


@app.route("/login", methods=["POST"])
def login():
    data = request.get_json()
    if data.get("password") == os.environ.get("APP_PASSWORD"):
        return jsonify({"ok": True})
    return jsonify({"ok": False}), 401


@app.route("/customers", methods=["GET"])
def get_customers():
    spreadsheet = get_spreadsheet_with_retry()
    sheet = spreadsheet.worksheet("customers_enriched")
    all_rows = sheet.get_all_values()
    headers = all_rows[0]

    # Build latest contact/follow_up_date per customer from sales_activities
    contact_rows = get_contact_rows(spreadsheet)
    latest_contact = {}
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        dt = parse_date_value(c["date_time"])
        nf = parse_date_value(c["follow_up_date"])
        if dt and (name not in latest_contact or dt > latest_contact[name]):
            latest_contact[name] = dt
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

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
        customer["follow_up_date"] = format_date_value(latest_followup.get(customer_key))
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
    filename = f"kontaktlogg_{date.today().isoformat()}.xlsx"
    return Response(
        workbook,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


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

    return jsonify({
        "total_sales": round(total_sales, 2),
        "latest_order_date": format_date_value(latest_order_date, fallback="—"),
        "currency": currency,
        "order_count": len(unique_references),
        "contacts": contacts,
    })


@app.route("/customer-insights", methods=["GET"])
def get_customer_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = date.today()
    customers = get_customer_rows(spreadsheet)

    # Latest follow_up_date per customer
    contact_rows = get_contact_rows(spreadsheet)
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        nf = parse_date_value(c["follow_up_date"])
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

    # Latest order date and order count per customer
    order_rows = get_order_rows(spreadsheet)
    latest_order = {}
    latest_delivery = {}
    order_count = {}
    for o in order_rows:
        name = o["Customer"].strip().lower()
        d = parse_date_value(o["Order date"])
        dd = parse_date_value(o["Delivery date"])
        ref = o["Reference"].strip()
        if d and (name not in latest_order or d > latest_order[name]):
            latest_order[name] = d
        if dd and (name not in latest_delivery or dd > latest_delivery[name]):
            latest_delivery[name] = dd
        if ref:
            order_count[name] = order_count.get(name, 0) + 1

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

    # Compute insights for all customers
    all_names = (
        set(latest_followup.keys())
        | set(latest_order.keys())
        | set(order_count.keys())
        | set(latest_delivery.keys())
        | {c["customer"].strip().lower() for c in customers if c.get("customer")}
    )
    insights = {}
    for name in all_names:
        # missad_uppfoljning
        nf = latest_followup.get(name)
        missad = bool(nf and nf < today)

        # customer_risk — based on most recent of order date or delivery date
        lo = latest_order.get(name)
        ld_check = latest_delivery.get(name)
        count = order_count.get(name, 0)
        risk = calculate_customer_risk(count, lo, ld_check, today)

        ld = latest_delivery.get(name)
        latest_delivery_date = format_date_value(ld)
        priority = priority_by_name.get(normalize_key(name), {})
        insights[name] = {
            "missad_uppfoljning": missad,
            "customer_risk": risk,
            "priority_level": priority.get("priority_level", ""),
            "priority_score": priority.get("priority_score"),
            "priority_type": priority.get("priority_type", ""),
            "latest_delivery_date": latest_delivery_date,
            "latest_delivery_month": latest_delivery_date[:7] if latest_delivery_date else "",  # "YYYY-MM"
        }

    return jsonify(insights)


@app.route("/followup-insights", methods=["GET"])
def get_followup_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = date.today()
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
        "generated_at": datetime.now().isoformat(timespec="minutes"),
        "selected_responsible": selected_responsible,
        "responsible_options": responsible_options,
        "weeks": weeks,
        "dfp_leaderboard": dfp_leaderboard,
        "dfp_top_weeks_2026": dfp_top_weeks_2026,
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
    headers = ensure_worksheet_columns(sheet, sheet.row_values(1), FREEZER_COLUMNS)
    freezer_values = {field: checkbox_to_sheet_value(data.get(field, "")) for field in FREEZER_COLUMNS}
    if not any(freezer_values.values()):
        return jsonify({"ok": False, "error": "freezer_selection_required"}), 400

    row_data = {
        "date_time": data.get("date_time", datetime.now().strftime("%Y-%m-%d %H:%M")),
        "sales_person": data.get("sales_person", ""),
        "customer": customer_name,
        "contact_channel": data.get("contact_channel", ""),
        "result": data.get("result", ""),
        "comment": data.get("comment", ""),
        "customer_contact_person": data.get("customer_contact_person", ""),
        "follow_up_date": data.get("follow_up_date", ""),
        **freezer_values,
    }
    row = [row_data.get(header, "") for header in headers]
    sheet.append_row(row)
    return jsonify({"ok": True})


@app.route("/config")
def config():
    return jsonify({"mapsApiKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
