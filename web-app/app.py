from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import unquote
from datetime import datetime, date, timedelta
from collections import defaultdict
import os
import json
import requests
from dotenv import load_dotenv
from requests.exceptions import ConnectionError as RequestsConnectionError

load_dotenv()

app = Flask(__name__)
CORS(app)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = os.environ.get("SHEET_KEY", "")

CUSTOMER_COLUMNS = ["customer", "cancelled_flag", "sales_person", "customer_segment",
                    "customer_reference", "customer_number", "phone", "email",
                    "comment"]

ORDER_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer", "Customer Reference",
                 "Buyer number", "Customer number", "Logistics number", "Address", "Number",
                 "Postal code", "City", "Country", "Phone number", "SKU", "Product", "Weight",
                 "Quantity", "Total weight", "Unit", "Total (Pre-discount)", "Product Discount",
                 "Total", "Currency", "Order Discount (Amount)", "Order Discount (%)", "Batch"]

CONTACT_COLUMNS = ["date_time", "sales_person", "customer", "contact_channel", "result",
                   "comment", "customer_contact_person", "follow_up_date",
                   "Franui", "Schufrulade", "Boujee", "polarbar"]


_spreadsheet_cache = None


def checkbox_to_sheet_value(value):
    return "1" if str(value).strip().lower() in {"1", "true", "yes", "on"} else ""


def text_to_sheet_value(value, max_length=None):
    text = str(value or "").strip()
    return text[:max_length] if max_length is not None else text

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


def rows_to_dicts(rows, columns):
    result = []
    for row in rows:
        padded = row + [""] * (len(columns) - len(row))
        result.append(dict(zip(columns, padded[:len(columns)])))
    return result


def normalize_key(value):
    return str(value or "").strip().lower()


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


def week_start(day):
    return day - timedelta(days=day.weekday())


def week_key(day):
    iso = day.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


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
    contact_rows = rows_to_dicts(spreadsheet.worksheet("sales_activities").get_all_values()[1:], CONTACT_COLUMNS)
    latest_contact = {}
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        dt = c["date_time"].strip()
        nf = c["follow_up_date"].strip()
        if dt and (name not in latest_contact or dt > latest_contact[name]):
            latest_contact[name] = dt
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

    def parse_coord(val):
        try:
            return float(val.replace(",", ".")) if val else None
        except ValueError:
            return None

    customers = []
    for i, row in enumerate(all_rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        customer = {col: d.get(col, "") for col in CUSTOMER_COLUMNS}
        customer["latitude"]  = parse_coord(d.get("latitude_google") or d.get("latitude",  ""))
        customer["longitude"] = parse_coord(d.get("longitude_google") or d.get("longitude", ""))
        addr = d.get("address_google", "").strip()
        num  = d.get("address_number_google", "").strip()
        customer["address_google"] = addr
        customer["address_number_google"] = num
        customer["city_google"] = d.get("city_google", "").strip()
        customer["postal_code_google"] = d.get("postal_code_google", "").strip()
        customer["region_google"] = d.get("region_google", "").strip()
        customer["address"] = f"{addr} {num}".strip()
        customer["city"] = customer["city_google"] or d.get("city", "")
        customer["latest_contact_date"] = latest_contact.get(customer["customer"].strip().lower(), "")[:10]
        customer["follow_up_date"] = latest_followup.get(customer["customer"].strip().lower(), "")
        customers.append({"row": i, **customer})
    return jsonify(customers)


@app.route("/customers/<customer_name>/stats", methods=["GET"])
def get_customer_stats(customer_name):
    customer_name = unquote(customer_name).strip().lower()
    spreadsheet = get_spreadsheet_with_retry()

    # Orders
    order_rows = rows_to_dicts(spreadsheet.worksheet("order_rows").get_all_values()[1:], ORDER_COLUMNS)
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
        d = o["Order date"].strip()
        if d and (latest_order_date is None or d > latest_order_date):
            latest_order_date = d
        if o["Reference"].strip():
            unique_references.add(o["Reference"].strip())

    # Contacts
    contact_rows = rows_to_dicts(spreadsheet.worksheet("sales_activities").get_all_values()[1:], CONTACT_COLUMNS)
    contacts = [
        {k: c[k] for k in ("customer", "date_time", "sales_person", "contact_channel", "result", "comment", "customer_contact_person", "follow_up_date",
                           "Franui", "Schufrulade", "Boujee", "polarbar")}
        for c in contact_rows
        if c["customer"].strip().lower() == customer_name
    ]
    contacts.sort(key=lambda x: x["date_time"], reverse=True)

    return jsonify({
        "total_sales": round(total_sales, 2),
        "latest_order_date": latest_order_date or "—",
        "currency": currency,
        "order_count": len(unique_references),
        "contacts": contacts,
    })


@app.route("/customer-insights", methods=["GET"])
def get_customer_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = date.today()

    # Latest follow_up_date per customer
    contact_rows = rows_to_dicts(spreadsheet.worksheet("sales_activities").get_all_values()[1:], CONTACT_COLUMNS)
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        nf = c["follow_up_date"].strip()
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

    # Latest order date and order count per customer
    order_rows = rows_to_dicts(spreadsheet.worksheet("order_rows").get_all_values()[1:], ORDER_COLUMNS)
    latest_order = {}
    latest_delivery = {}
    order_count = {}
    for o in order_rows:
        name = o["Customer"].strip().lower()
        d = o["Order date"].strip()
        dd = o["Delivery date"].strip()
        ref = o["Reference"].strip()
        if d and (name not in latest_order or d > latest_order[name]):
            latest_order[name] = d
        if dd and (name not in latest_delivery or dd > latest_delivery[name]):
            latest_delivery[name] = dd
        if ref:
            order_count[name] = order_count.get(name, 0) + 1

    # Compute insights for all customers
    all_names = set(latest_followup.keys()) | set(latest_order.keys()) | set(order_count.keys()) | set(latest_delivery.keys())
    insights = {}
    for name in all_names:
        # missad_uppfoljning
        nf = latest_followup.get(name, "")
        missad = bool(nf and nf < today.isoformat())

        # customer_risk — based on most recent of order date or delivery date
        lo = latest_order.get(name, "")
        ld_check = latest_delivery.get(name, "")
        most_recent = max(lo, ld_check) if lo and ld_check else (lo or ld_check)
        count = order_count.get(name, 0)
        if count == 0 or not most_recent:
            risk = ""
        else:
            try:
                recent_date = date.fromisoformat(most_recent[:10])
                days = (today - recent_date).days
                if days >= 75:
                    risk = "Återaktivering krävs"
                elif days >= 60:
                    risk = "Hög risk"
                elif days >= 45:
                    risk = "Risk"
                elif days >= 30:
                    risk = "Bevaka"
                else:
                    risk = "Aktiv"
            except ValueError:
                risk = ""

        ld = latest_delivery.get(name, "")
        insights[name] = {
            "missad_uppfoljning": missad,
            "customer_risk": risk,
            "latest_delivery_date": ld,
            "latest_delivery_month": ld[:7] if ld else "",  # "YYYY-MM"
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

    customer_values = spreadsheet.worksheet("customers_enriched").get_all_values()
    customer_headers = customer_values[0] if customer_values else []
    customers_by_name = {}
    for i, row in enumerate(customer_values[1:], start=2):
        padded = row + [""] * (len(customer_headers) - len(row))
        d = dict(zip(customer_headers, padded))
        name = d.get("customer", "").strip()
        if not name:
            continue
        customers_by_name[normalize_key(name)] = {
            "row": i,
            "customer": name,
            "sales_person": d.get("sales_person", "").strip(),
            "customer_segment": d.get("customer_segment", "").strip(),
        }

    contact_rows = rows_to_dicts(spreadsheet.worksheet("sales_activities").get_all_values()[1:], CONTACT_COLUMNS)
    order_rows = rows_to_dicts(spreadsheet.worksheet("order_rows").get_all_values()[1:], ORDER_COLUMNS)

    responsible_options = sorted({
        c["sales_person"] for c in customers_by_name.values() if c["sales_person"]
    })

    def customer_belongs_to_selected(customer_name):
        if not selected_responsible:
            return True
        customer = customers_by_name.get(normalize_key(customer_name))
        return bool(customer and customer["sales_person"] == selected_responsible)

    def contact_belongs_to_selected(contact):
        return not selected_responsible or contact["sales_person"].strip() == selected_responsible

    # DFP leaderboard is intentionally global and ignores the selected responsible filter.
    # It sums Quantity for every order row by the customer's responsible salesperson.
    dfp_counts = {w["key"]: defaultdict(float) for w in weeks}
    for order in order_rows:
        order_date = parse_date_value(order["Order date"])
        if not order_date:
            continue
        key = week_key(order_date)
        if key not in week_keys:
            continue

        customer = customers_by_name.get(normalize_key(order["Customer"]))
        responsible = customer["sales_person"] if customer and customer["sales_person"] else "Okänd"
        quantity = parse_number_value(order["Quantity"], default=0.0)
        dfp_counts[key][responsible] += quantity

    dfp_leaderboard = []
    for w in weeks:
        leaders = sorted(dfp_counts[w["key"]].items(), key=lambda item: (-item[1], item[0]))[:3]
        dfp_leaderboard.append({
            "week_key": w["key"],
            "label": w["label"],
            "leaders": [
                {
                    "rank": idx + 1,
                    "sales_person": name,
                    "dfp_count": int(count) if float(count).is_integer() else round(count, 1),
                }
                for idx, (name, count) in enumerate(leaders)
            ],
        })

    contact_count_by_week = {w["key"]: 0 for w in weeks}
    positive_count_by_week = {w["key"]: 0 for w in weeks}
    contact_dates_by_customer = defaultdict(list)
    latest_contact_for_risk = {}

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

        if (not selected_responsible or contact_belongs_to_selected(contact)) and (
            customer_key not in latest_contact_for_risk or contact_date > latest_contact_for_risk[customer_key]
        ):
            latest_contact_for_risk[customer_key] = contact_date

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

    risk_customers = []
    for customer_key, customer in customers_by_name.items():
        if selected_responsible and customer["sales_person"] != selected_responsible:
            continue

        lo = latest_order.get(customer_key)
        ld = latest_delivery.get(customer_key)
        risk = calculate_customer_risk(order_count_by_customer.get(customer_key, 0), lo, ld, today)
        if risk not in {"Bevaka", "Risk", "Hög risk", "Återaktivering krävs"}:
            continue

        latest_contact = latest_contact_for_risk.get(customer_key)
        if latest_contact and lo and latest_contact >= lo:
            continue

        risk_customers.append({
            "row": customer["row"],
            "customer": customer["customer"],
            "sales_person": customer["sales_person"],
            "segment": customer["customer_segment"],
            "risk_status": risk,
            "latest_order_date": lo.isoformat() if lo else "",
            "latest_contact_date": latest_contact.isoformat() if latest_contact else "",
        })

    risk_priority = {"Återaktivering krävs": 0, "Hög risk": 1, "Risk": 2, "Bevaka": 3}
    risk_customers.sort(key=lambda c: (
        segment_sort_key(c["segment"]),
        risk_priority.get(c["risk_status"], 9),
        c["latest_order_date"] or "9999-12-31",
        c["customer"].casefold(),
    ))

    return jsonify({
        "generated_at": datetime.now().isoformat(timespec="minutes"),
        "selected_responsible": selected_responsible,
        "responsible_options": responsible_options,
        "weeks": weeks,
        "dfp_leaderboard": dfp_leaderboard,
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
        "risk_customers": risk_customers,
    })


@app.route("/customers/<int:row>/contact", methods=["PATCH"])
def update_customer_contact(row):
    data = request.get_json()
    sheet = get_spreadsheet_with_retry().worksheet("customers_enriched")
    headers = sheet.row_values(1)
    missing_columns = []
    fields = [
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
                    lat_str = f"{new_lat:.7f}".replace(".", ",")
                    lng_str = f"{new_lng:.7f}".replace(".", ",")
                    if "latitude_google" in headers:
                        sheet.update_cell(row, headers.index("latitude_google") + 1, lat_str)
                    if "longitude_google" in headers:
                        sheet.update_cell(row, headers.index("longitude_google") + 1, lng_str)
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
    headers = sheet.row_values(1)
    row_data = {
        "date_time": data.get("date_time", datetime.now().strftime("%Y-%m-%d %H:%M")),
        "sales_person": data.get("sales_person", ""),
        "customer": customer_name,
        "contact_channel": data.get("contact_channel", ""),
        "result": data.get("result", ""),
        "comment": data.get("comment", ""),
        "customer_contact_person": data.get("customer_contact_person", ""),
        "follow_up_date": data.get("follow_up_date", ""),
        "Franui": checkbox_to_sheet_value(data.get("Franui", "")),
        "Schufrulade": checkbox_to_sheet_value(data.get("Schufrulade", "")),
        "Boujee": checkbox_to_sheet_value(data.get("Boujee", "")),
        "polarbar": checkbox_to_sheet_value(data.get("polarbar", "")),
    }
    row = [row_data.get(header, "") for header in headers]
    sheet.append_row(row)
    return jsonify({"ok": True})


@app.route("/config")
def config():
    return jsonify({"mapsApiKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
