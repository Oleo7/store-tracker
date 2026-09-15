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
        mature = index < 10
        row = {
            "date_time": (
                now - timedelta(days=12 if mature else 1, minutes=index)
            ).isoformat(
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
    for seller, positive_count in (("sofia", 10), ("viewer", 10)):
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

    maturity_cutoff = now.date() - timedelta(days=10)
    latest_complete_sunday = maturity_cutoff - timedelta(
        days=(maturity_cutoff.weekday() - 6) % 7
    )
    newer_trend_monday = latest_complete_sunday - timedelta(weeks=5, days=6)
    trend_orders = []
    trend_profiles = [
        {
            "week_start": newer_trend_monday - timedelta(weeks=1),
            "positive_counts": {"olle": 6, "sofia": 7, "viewer": 6},
            "converted_indexes": {
                "olle": {0, 1, 6},
                "sofia": {0, 1, 2, 3, 4, 5},
                "viewer": {0, 1},
            },
        },
        {
            "week_start": newer_trend_monday,
            "positive_counts": {"olle": 6, "sofia": 6, "viewer": 6},
            "converted_indexes": {
                "olle": {0, 1, 2, 6},
                "sofia": {0, 1},
                "viewer": {0, 1, 2, 6},
            },
        },
    ]
    denominators = {"olle": 10, "sofia": 10, "viewer": 8}
    trend_customer_sequence = 1000
    for week_index, profile in enumerate(trend_profiles, start=1):
        for seller, denominator in denominators.items():
            for index in range(denominator):
                trend_customer_sequence += 1
                customer = {
                    "customer": f"Trend-butik {week_index}-{seller}-{index + 1}",
                    "customer_id": f"trend-{week_index}-{seller}-{index + 1}",
                    "sales_person": seller,
                    "customer_segment": "A",
                    "customer_number": f"TREND-{trend_customer_sequence}",
                }
                customer_sheet.append_row([
                    customer.get(header, "") for header in customer_headers
                ])
                contact_at = datetime.combine(
                    profile["week_start"] + timedelta(days=index % 6),
                    now.time(),
                ) + timedelta(minutes=index)
                contact_id = f"smoke-trend-{week_index}-{seller}-{index}"
                activity_row = {
                    "date_time": contact_at.isoformat(timespec="minutes"),
                    "sales_person": seller,
                    "sales_user_name": seller,
                    "customer": customer["customer"],
                    "customer_id": customer["customer_id"],
                    "contact_channel": "Mejl" if seller == "olle" and index == 6 else "Telefon",
                    "result": (
                        "Positiv"
                        if index < profile["positive_counts"][seller]
                        else "Neutral"
                    ),
                    "activity_source": "manual",
                    "contact_id": contact_id,
                }
                activity_sheet.append_row([
                    activity_row.get(header, "") for header in headers
                ])
                if index in profile["converted_indexes"][seller]:
                    trend_orders.append({
                        "Reference": f"TREND-ORDER-{week_index}-{seller}-{index + 1}",
                        "Order date": (contact_at.date() + timedelta(days=2)).isoformat(),
                        "Customer": customer["customer"],
                        "Customer number": customer["customer_number"],
                        "placedBy": seller,
                        "placedAs": "supplier",
                        "SKU": "10001",
                        "Quantity": "1",
                        "Total weight": "1",
                        "Unit": "DFP",
                        "Total": "500",
                        "Currency": "SEK",
                        "customer_id": customer["customer_id"],
                    })

    order_sheet = spreadsheet.worksheet("order_rows")
    order_headers = order_sheet.values[0]
    converted_indexes = (0, 1, 2, 3, 10, 11, 12)
    for sequence, index in enumerate(converted_indexes, start=1):
        customer = olle_customers[index]
        contact_date = now - timedelta(days=12 if index < 10 else 1)
        early_order = {
            "Reference": f"SMOKE-ORDER-{sequence}",
            "Order date": (
                contact_date + timedelta(days=2 if index < 10 else 1)
            ).date().isoformat(),
            "Customer": customer["customer"],
            "Customer number": customer["customer_number"],
            "placedBy": "olle",
            "placedAs": "supplier",
            "SKU": "10001",
            "Quantity": "1",
            "Total weight": "1",
            "Unit": "DFP",
            "Total": "500",
            "Currency": "SEK",
            "customer_id": customer["customer_id"],
        }
        order_sheet.append_row([
            early_order.get(header, "") for header in order_headers
        ])
    for trend_order in trend_orders:
        order_sheet.append_row([
            trend_order.get(header, "") for header in order_headers
        ])

    # One own supplier order without an eligible preceding contact makes the
    # second component of Säljkopplat resultat visible in the browser fixture.
    own_customer = {
        "customer": "Smoke egen order",
        "customer_id": "90000000-0000-4000-8000-999999999999",
        "sales_person": "olle",
        "customer_segment": "A",
        "customer_number": "SMOKE-OWN-1",
    }
    customer_sheet.append_row([
        own_customer.get(header, "") for header in customer_headers
    ])
    own_order = {
        "Reference": "SMOKE-OWN-ORDER-1",
        "Order date": (now.date() - timedelta(days=20)).isoformat(),
        "Customer": own_customer["customer"],
        "Customer number": own_customer["customer_number"],
        "placedBy": "olle",
        "placedAs": "supplier",
        "SKU": "10002",
        "Quantity": "1",
        "Total weight": "2",
        "Unit": "DFP",
        "Total": "900",
        "Currency": "SEK",
        "customer_id": own_customer["customer_id"],
    }
    order_sheet.append_row([
        own_order.get(header, "") for header in order_headers
    ])

    app_module.app.config.update(
        SECRET_KEY="sales-coaching-browser-harness",
        TESTING=False,
    )
    app_module.get_spreadsheet_with_retry = lambda: spreadsheet
    initial_activity_values = [list(row) for row in activity_sheet.values]

    # Test-only authenticated, section-focused pages let the local browser QA
    # create deterministic screenshots without adding production-only switches.
    auth_functions = app_module.app.before_request_funcs[None]
    auth_index = next(
        index for index, function in enumerate(auth_functions)
        if function.__name__ == "require_authenticated_session"
    )
    original_auth = auth_functions[auth_index]

    def harness_auth():
        if app_module.request.endpoint in {
            "browser_harness_login", "browser_harness_shot",
        }:
            return None
        return original_auth()

    auth_functions[auth_index] = harness_auth

    @app_module.app.get("/__test__/login")
    def browser_harness_login():
        user = app_module.find_active_user(spreadsheet, "admin")
        app_module.session.clear()
        app_module.session.permanent = True
        app_module.session["user"] = app_module.public_user(user)
        next_url = str(app_module.request.args.get("next") or "/")
        if not next_url.startswith("/") or next_url.startswith("//"):
            next_url = "/"
        response = app_module.Response(status=302)
        response.headers["Location"] = next_url
        return response

    @app_module.app.get("/__test__/shot")
    def browser_harness_shot():
        html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        html = html.replace("</head>", """
<style>
body.qa-screenshot #session-control,
body.qa-screenshot #view-insights > .header,
body.qa-screenshot #insights-view-toggle,
body.qa-screenshot #sc-filter-form { display: none !important; }
body.qa-screenshot #view-insights { padding-top: 16px !important; }
body.qa-screenshot #sales-coaching-dashboard { max-width: none !important; }
body.qa-screenshot #sc-dashboard-content > .sc-section { margin: 0 !important; }
body.qa-screenshot.qa-mobile,
body.qa-screenshot.qa-mobile #view-insights { width: 390px !important; max-width: 390px !important; }
</style>
</head>""")
        html = html.replace("</body>", """
<script>
(() => {
  document.body.classList.add('qa-screenshot');
  if (new URLSearchParams(location.search).get('mobile') === '1') {
    document.body.classList.add('qa-mobile');
  }
  const keys = {
    team: '[aria-labelledby="sc-team-title"]',
    sales: '.sc-team-10d-trend-section',
    activity: '.sc-human-activity-trend-section'
  };
  const selected = keys[new URLSearchParams(location.search).get('section')] || keys.team;
  const timer = setInterval(() => {
    const target = document.querySelector(selected);
    if (!target) return;
    document.querySelectorAll('#sc-dashboard-content > .sc-section').forEach(section => {
      section.hidden = section !== target;
    });
    document.documentElement.dataset.qaReady = 'true';
    clearInterval(timer);
  }, 25);
})();
</script>
</body>""")
        return app_module.Response(html, content_type="text/html; charset=utf-8")

    # Test-only fixture switch: existing smoke starts with insufficient history.
    # Mutate only historical snapshot fields after its other assertions finish.
    @app_module.app.post("/__test__/priority-profile")
    def priority_profile_fixture():
        from sales_coaching import ANALYTICS_SNAPSHOT_VERSION, PRIORITY_PERCENTILE_BASIS
        if app_module.request.args.get("reset") == "1":
            activity_sheet.values = [list(row) for row in initial_activity_values]
            app_module._sheet_read_cache.clear()
            return {"ok": True}
        profiles = {
            "olle": [80] * 21 + [50] * 7 + [10] * 7,
            "viewer": [80] * 4 + [50] * 3 + [10] * 3,
            "sofia": [80] * 2 + [50] * 5 + [10] * 3,
        }
        for values in activity_sheet.values[1:]:
            row = dict(zip(headers, values))
            for seller, percentiles in profiles.items():
                prefix = f"smoke-{seller}-"
                if not row.get("contact_id", "").startswith(prefix):
                    continue
                index = int(row["contact_id"][len(prefix):])
                snapshot = {
                    "analytics_snapshot_version": ANALYTICS_SNAPSHOT_VERSION,
                    "priority_snapshot_quality": "exact",
                    "priority_percentile_basis_at_contact": PRIORITY_PERCENTILE_BASIS,
                    "priority_percentile_at_contact": str(percentiles[index]),
                    "customer_segment_at_contact": "A",
                }
                for key, value in snapshot.items():
                    values[headers.index(key)] = value
        app_module._sheet_read_cache.clear()
        return {"ok": True}

    app_module.app.run(
        host="127.0.0.1",
        port=int(os.environ.get("SALES_COACHING_BROWSER_PORT", "5065")),
        debug=False,
        use_reloader=False,
        threaded=True,
    )
