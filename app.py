from flask import Flask, jsonify, send_file, request
from flask_cors import CORS
import gspread
from google.oauth2.service_account import Credentials
from urllib.parse import unquote
from datetime import datetime, date
import os
import json

app = Flask(__name__)
CORS(app)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1uWhuacNGdhfIfA9mQjI04fR23ZCa6rGeXrlyNLbxEwc"
CREDS_FILE = "credentials.json"

CUSTOMER_COLUMNS = ["customer", "city", "region", "customerResponsible", "customerSegment",
                    "customerReference", "address", "phoneNumber", "email", "contactPerson"]

ORDER_COLUMNS = ["reference", "orderDate", "deliveryDate", "customer", "customerReference",
                 "buyerNumber", "customerNumber", "logisticsNumber", "address", "number",
                 "postalCode", "city", "country", "phoneNumber", "sku", "product", "weight",
                 "quantity", "totalWeight", "unit", "totalPreDiscount", "productDiscount",
                 "total", "currency", "orderDiscountAmount", "orderDiscountPercentage",
                 "batch", "column1"]

CONTACT_COLUMNS = ["date", "seller", "customer", "channel", "result", "comment",
                   "contactPerson", "nextFollowUp", "orderInStockfiller"]


def get_spreadsheet():
    env_creds = os.environ.get("GOOGLE_CREDENTIALS")
    if env_creds:
        creds = Credentials.from_service_account_info(json.loads(env_creds), scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds).open_by_key(SHEET_ID)


def rows_to_dicts(rows, columns):
    result = []
    for row in rows:
        padded = row + [""] * (len(columns) - len(row))
        result.append(dict(zip(columns, padded[:len(columns)])))
    return result


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/customers", methods=["GET"])
def get_customers():
    spreadsheet = get_spreadsheet()
    sheet = spreadsheet.worksheet("kundtabell")
    all_rows = sheet.get_all_values()
    headers = all_rows[0]

    # Build latest nextFollowUp per customer from kundkontakter
    contact_rows = rows_to_dicts(spreadsheet.worksheet("kundkontakter").get_all_values()[1:], CONTACT_COLUMNS)
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        nf = c["nextFollowUp"].strip()
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

    customers = []
    for i, row in enumerate(all_rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        customer = {col: d.get(col, "") for col in CUSTOMER_COLUMNS}
        def parse_coord(val):
            try:
                return float(val.replace(",", ".")) if val else None
            except ValueError:
                return None
        customer["latitude"]  = parse_coord(d.get("latitude",  ""))
        customer["longitude"] = parse_coord(d.get("longitude", ""))
        customer["nextFollowUp"] = latest_followup.get(customer["customer"].strip().lower(), "")
        customers.append({"row": i, **customer})
    return jsonify(customers)


@app.route("/customers/<customer_name>/stats", methods=["GET"])
def get_customer_stats(customer_name):
    customer_name = unquote(customer_name).strip().lower()
    spreadsheet = get_spreadsheet()

    # Orders
    order_rows = rows_to_dicts(spreadsheet.worksheet("ordertabell").get_all_values()[1:], ORDER_COLUMNS)
    total_sales = 0.0
    latest_order_date = None
    currency = ""

    unique_references = set()
    for o in order_rows:
        if o["customer"].strip().lower() != customer_name:
            continue
        try:
            cleaned = "".join(c for c in o["total"] if c.isdigit() or c in ".,").replace(",", ".")
            if cleaned:
                total_sales += float(cleaned)
        except ValueError:
            pass
        if not currency and o["currency"].strip():
            currency = o["currency"].strip()
        d = o["orderDate"].strip()
        if d and (latest_order_date is None or d > latest_order_date):
            latest_order_date = d
        if o["reference"].strip():
            unique_references.add(o["reference"].strip())

    # Contacts
    contact_rows = rows_to_dicts(spreadsheet.worksheet("kundkontakter").get_all_values()[1:], CONTACT_COLUMNS)
    contacts = [
        {k: c[k] for k in ("customer", "date", "seller", "channel", "result", "comment", "contactPerson", "nextFollowUp", "orderInStockfiller")}
        for c in contact_rows
        if c["customer"].strip().lower() == customer_name
    ]
    contacts.sort(key=lambda x: x["date"], reverse=True)

    return jsonify({
        "total_sales": round(total_sales, 2),
        "latest_order_date": latest_order_date or "—",
        "currency": currency,
        "order_count": len(unique_references),
        "contacts": contacts,
    })


@app.route("/customer-insights", methods=["GET"])
def get_customer_insights():
    spreadsheet = get_spreadsheet()
    today = date.today()

    # Latest nextFollowUp per customer
    contact_rows = rows_to_dicts(spreadsheet.worksheet("kundkontakter").get_all_values()[1:], CONTACT_COLUMNS)
    latest_followup = {}
    for c in contact_rows:
        name = c["customer"].strip().lower()
        nf = c["nextFollowUp"].strip()
        if nf and (name not in latest_followup or nf > latest_followup[name]):
            latest_followup[name] = nf

    # Latest order date and order count per customer
    order_rows = rows_to_dicts(spreadsheet.worksheet("ordertabell").get_all_values()[1:], ORDER_COLUMNS)
    latest_order = {}
    latest_delivery = {}
    order_count = {}
    for o in order_rows:
        name = o["customer"].strip().lower()
        d = o["orderDate"].strip()
        dd = o["deliveryDate"].strip()
        ref = o["reference"].strip()
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

        # customer_risk
        lo = latest_order.get(name, "")
        count = order_count.get(name, 0)
        if count == 0 or not lo:
            risk = ""
        else:
            try:
                lo_date = date.fromisoformat(lo[:10])
                days = (today - lo_date).days
                if days > 60:
                    risk = "FÖRLORAD?"
                elif days > 40:
                    risk = "HÖG RISK"
                elif days > 20:
                    risk = "RISK"
                else:
                    risk = "OK"
            except ValueError:
                risk = ""

        insights[name] = {
            "missad_uppfoljning": missad,
            "customer_risk": risk,
            "latest_delivery_date": latest_delivery.get(name, ""),
        }

    return jsonify(insights)


@app.route("/customers/<customer_name>/contacts", methods=["POST"])
def add_contact(customer_name):
    customer_name = unquote(customer_name)
    data = request.get_json()
    sheet = get_spreadsheet().worksheet("kundkontakter")
    row = [
        data.get("date", datetime.now().strftime("%Y-%m-%d %H:%M")),
        data.get("seller", ""),
        customer_name,
        data.get("channel", ""),
        data.get("result", ""),
        data.get("comment", ""),
        data.get("contactPerson", ""),
        data.get("nextFollowUp", ""),
        data.get("orderInStockfiller", ""),
    ]
    sheet.append_row(row)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
