"""Local browser harness for the sales-planning end-to-end checks.

The harness uses the same in-memory Google Sheets doubles as the API tests, so
browser verification can exercise create/update/contact/route flows without
touching production CRM data.
"""

from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import sys

from flask import Response, redirect, request, session

WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from test_planning import ConstantRoadProvider, default_spreadsheet


def append_dict(sheet, columns, values):
    sheet.append_row([values.get(column, "") for column in columns])


def build_browser_spreadsheet():
    spreadsheet = default_spreadsheet()
    today = app_module.stockholm_today()

    orders = spreadsheet.worksheet("order_rows")
    for customer, customer_number, reference, days_ago in (
        ("Butik A", "C-1", "BROWSER-A", 95),
        ("Butik C", "C-3", "BROWSER-C", 80),
    ):
        append_dict(
            orders,
            app_module.ORDER_COLUMNS,
            {
                "Customer": customer,
                "Customer number": customer_number,
                "Order date": (today - timedelta(days=days_ago)).isoformat(),
                "Delivery date": (
                    today - timedelta(days=days_ago - 2)
                ).isoformat(),
                "Reference": reference,
                "Quantity": "24",
                "Unit": "DFP",
                "Total": "2400",
                "Total weight": "120",
                "Currency": "SEK",
            },
        )

    contacts = spreadsheet.worksheet("sales_activities")
    append_dict(
        contacts,
        app_module.CONTACT_COLUMNS,
        {
            "date_time": f"{(today - timedelta(days=1)).isoformat()} 08:15",
            "sales_person": "Olle",
            "customer": "Butik C",
            "customer_id": "33333333-3333-4333-8333-333333333333",
            "contact_channel": "Telefon",
            "result": "Neutral",
            "comment": "Ring igen efter lunchleveransen",
            "customer_contact_person": "Kim",
            "follow_up_date": (today + timedelta(days=3)).isoformat(),
            "contact_id": "browser-legacy-contact",
        },
    )
    append_dict(
        contacts,
        app_module.CONTACT_COLUMNS,
        {
            "date_time": f"{(today - timedelta(days=14)).isoformat()} 09:00",
            "sales_person": "Olle",
            "customer": "Butik A",
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "contact_channel": "Telefon",
            "result": "Neutral",
            "comment": "Försenad uppföljning för mobil QA",
            "follow_up_date": (today - timedelta(days=5)).isoformat(),
            "contact_id": "browser-overdue-contact",
        },
    )

    planning = spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
    olle = {"user_name": "olle", "name": "Olle"}
    for row in (
        app_module.build_planned_activity_row(
            activity_id="browser-phone-today",
            owner=olle,
            customer=app_module.get_customer_by_row(spreadsheet, 2),
            contact_type="phone",
            scheduled_at=f"{today.isoformat()}T15:00:00+02:00",
            note="Stäm av höstens kampanj",
            source="manual",
            client_request_id="browser-seed-phone",
        ),
        app_module.build_planned_activity_row(
            activity_id="browser-visit-tomorrow",
            owner=olle,
            customer=app_module.get_customer_by_row(spreadsheet, 4),
            contact_type="visit",
            scheduled_at=(
                f"{(today + timedelta(days=1)).isoformat()}T11:00:00+02:00"
            ),
            note="Bokat sortimentsmöte",
            source="manual",
            client_request_id="browser-seed-visit",
        ),
    ):
        append_dict(planning, app_module.PLANNED_ACTIVITY_COLUMNS, row)

    return spreadsheet


if __name__ == "__main__":
    browser_spreadsheet = build_browser_spreadsheet()
    road_provider = ConstantRoadProvider(seconds=6 * 60)

    @app_module.app.post("/__harness__/fail-planning-write-once")
    def fail_planning_write_once():
        """Inject one local-only write failure for partial-save retry QA."""

        browser_spreadsheet.worksheet(
            app_module.PLANNED_ACTIVITIES_SHEET
        ).fail_next_batch_update = RuntimeError(
            "Browser harness: injected planning write failure"
        )
        return {"ok": True}

    @app_module.app.get("/__harness__/role/<user_name>")
    def select_harness_role(user_name):
        """Switch the isolated browser session without exposing test passwords."""

        user = app_module.find_active_user(browser_spreadsheet, user_name)
        if not user:
            return {"ok": False, "error": "unknown_harness_user"}, 404
        session["user"] = app_module.public_user(user)
        session.permanent = True
        return redirect("/?__harness_geo=1")

    @app_module.app.before_request
    def serve_deterministic_geolocation_harness():
        """Serve the real UI with deterministic local GPS only when requested."""

        if request.path != "/" or request.args.get("__harness_geo") != "1":
            return None
        html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        geolocation_script = """
<script>
Object.defineProperty(navigator, "geolocation", {
  configurable: true,
  value: {
    getCurrentPosition(success) {
      success({
        coords: {
          latitude: 57.7089,
          longitude: 11.9746,
          accuracy: 5
        }
      });
    }
  }
});
</script>
"""
        return Response(
            html.replace("</body>", f"{geolocation_script}</body>"),
            mimetype="text/html",
        )

    app_module.app.config.update(
        SECRET_KEY="planning-browser-harness-secret",
        TESTING=False,
        PLANNING_SUGGESTIONS_STUB=True,
    )
    app_module.get_spreadsheet_with_retry = lambda: browser_spreadsheet
    app_module.get_route_travel_time_provider = lambda: road_provider
    app_module.app.run(
        host="127.0.0.1",
        port=int(os.environ.get("BROWSER_HARNESS_PORT", "5055")),
        debug=False,
        use_reloader=False,
        threaded=True,
    )
