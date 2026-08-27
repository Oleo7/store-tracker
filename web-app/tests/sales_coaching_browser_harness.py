"""Isolated browser harness for sales-coaching desktop/mobile smoke tests."""

from pathlib import Path
from datetime import datetime, timedelta
import os
import sys

WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from test_planning import default_spreadsheet  # noqa: E402


if __name__ == "__main__":
    spreadsheet = default_spreadsheet()
    activity_sheet = spreadsheet.worksheet("sales_activities")
    headers = activity_sheet.values[0]
    now = datetime.now().replace(hour=10, minute=0, second=0, microsecond=0)
    customer_sheet = spreadsheet.worksheet("customers_enriched")
    customer_headers = customer_sheet.values[0]
    olle_customers = []
    for index in range(35):
        customer = {
            "customer": f"Smoke-butik {index + 1}",
            "customer_id": f"90000000-0000-4000-8000-{index + 1:012d}",
            "sales_person": "Olle",
            "customer_segment": "A",
            "customer_number": f"SMOKE-{index + 1}",
        }
        olle_customers.append(customer)
        customer_sheet.append_row([
            customer.get(header, "") for header in customer_headers
        ])

    for index, customer in enumerate(olle_customers):
        row = {
            "date_time": (now - timedelta(days=1, minutes=index)).isoformat(
                timespec="minutes"
            ),
            "sales_person": "olle",
            "sales_user_name": "olle",
            "customer": customer["customer"],
            "customer_id": customer["customer_id"],
            "contact_channel": "Telefon",
            "result": "Positiv" if index < 28 else "Neutral",
            "activity_source": "manual",
            "contact_id": f"smoke-olle-{index}",
        }
        activity_sheet.append_row([row.get(header, "") for header in headers])

    seller_customers = {
        "sofia": ("Butik B", "22222222-2222-4222-8222-222222222222"),
        "viewer": ("Butik C", "33333333-3333-4333-8333-333333333333"),
    }
    for seller, positive_count in (("sofia", 6), ("viewer", 8)):
        customer, customer_id = seller_customers[seller]
        for index in range(10):
            row = {
                "date_time": (now - timedelta(days=index + 1)).isoformat(
                    timespec="minutes"
                ),
                "sales_person": seller,
                "sales_user_name": seller,
                "customer": customer,
                "customer_id": customer_id,
                "contact_channel": "Telefon",
                "result": "Positiv" if index < positive_count else "Neutral",
                "activity_source": "manual",
                "contact_id": f"smoke-{seller}-{index}",
            }
            activity_sheet.append_row([row.get(header, "") for header in headers])

    order_sheet = spreadsheet.worksheet("order_rows")
    order_headers = order_sheet.values[0]
    for index, customer in enumerate(olle_customers[:7], start=1):
        early_order = {
            "Reference": f"EARLY-ORDER-{index}",
            "Order date": now.date().isoformat(),
            "Customer": customer["customer"],
            "Customer number": customer["customer_number"],
            "Quantity": "1",
            "Unit": "DFP",
            "Total": "100",
            "Currency": "SEK",
            "customer_id": customer["customer_id"],
        }
        order_sheet.append_row([
            early_order.get(header, "") for header in order_headers
        ])

    app_module.app.config.update(
        SECRET_KEY="sales-coaching-browser-harness",
        TESTING=False,
    )
    app_module.get_spreadsheet_with_retry = lambda: spreadsheet
    app_module.app.run(
        host="127.0.0.1",
        port=int(os.environ.get("SALES_COACHING_BROWSER_PORT", "5065")),
        debug=False,
        use_reloader=False,
        threaded=True,
    )
