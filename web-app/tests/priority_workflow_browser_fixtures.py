"""Synthetic fixtures for the isolated harness; never reads an external CRM."""

from copy import deepcopy
from datetime import timedelta


def install_priority_workflow_fixtures(app_module, spreadsheet):
    baseline = {name: deepcopy(sheet.values) for name, sheet in spreadsheet.sheets.items()}

    def restore():
        for name in list(spreadsheet.sheets):
            if name not in baseline:
                del spreadsheet.sheets[name]
            else:
                spreadsheet.sheets[name].values = deepcopy(baseline[name])
        app_module._sheet_read_cache.clear()
        app_module._email_sheets_cache = None

    @app_module.app.post("/__test__/priority-workflow")
    def priority_workflow_fixture():
        restore()
        if app_module.request.args.get("reset") == "1":
            return {"ok": True}
        now = app_module.stockholm_now()
        today = now.date()
        stores = {}
        customer_sheet = spreadsheet.worksheet("customers_enriched")
        contacts = spreadsheet.worksheet("sales_activities")
        orders = spreadsheet.worksheet("order_rows")
        plans = spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        owner = app_module.find_active_user(spreadsheet, "olle")

        def append(sheet, data):
            sheet.append_row([data.get(key, "") for key in sheet.values[0]])

        names = ["Stort A", "Bokad", "Löfte", "Kalender igår", "Dagens aktivitet",
                 "Granska", "Första order", "Kontaktbesked", "Orderpaus"]
        for index, label in enumerate(names, 1):
            stores[label] = {
                "customer_id": f"a3300000-0000-4000-8000-{index:012d}",
                "customer": f"QA {label}", "customer_number": f"QA-{index}",
                "sales_person": "Olle", "customer_segment": "A",
                "phone": "0100000000", "email": "fixture@example.invalid",
                "latitude_google": "57.70", "longitude_google": "11.90",
            }
            append(customer_sheet, stores[label])

        def contact(label, age, result="Positiv", due=None, suffix=""):
            store = stores[label]
            identifier = f"qa-contact-{names.index(label) + 1}{suffix}"
            append(contacts, {
                "contact_id": identifier, "customer_id": store["customer_id"],
                "customer": store["customer"], "sales_person": "Olle", "sales_user_name": "olle",
                "date_time": (now - timedelta(days=age)).isoformat(),
                "contact_channel": "Telefon", "result": result,
                "comment": f"Syntetisk uppföljning {label}{suffix}",
                "follow_up_date": due.isoformat() if due else "",
                "follow_up_purpose": "customer_followup" if due else "",
                "follow_up_agreement_confirmed": "Y" if due else "",
            })
            return identifier

        def order(label, age, volume):
            store = stores[label]
            append(orders, {
                "Order number": f"QA-{label}-{age}", "Customer": store["customer"],
                "Order date": (today - timedelta(days=age + 2)).isoformat(),
                "Delivery date": (today - timedelta(days=age)).isoformat(),
                "customer_id": store["customer_id"], "Sales person": "Olle",
                "Total weight": str(volume), "Quantity": "1", "Unit": "Dfp",
                "Total": "1000", "Currency": "SEK", "Article": "Polarbar",
            })

        def plan(label, days, channel="phone", source_contact_id="", confirmed=False, picking=False):
            identifier = f"qa-plan-{names.index(label) + 1}"
            scheduled = (now + timedelta(days=days)).replace(hour=10 if days else 1, minute=0, second=0, microsecond=0)
            row = app_module.build_planned_activity_row(
                activity_id=identifier, owner=owner, customer=stores[label], contact_type=channel,
                scheduled_at=scheduled, note=f"Syntetisk planering {label}",
                source="follow_up" if source_contact_id else "manual",
                source_contact_id=source_contact_id, appointment_confirmed=confirmed, picking_help=picking,
                client_request_id=f"qa-seed-{identifier}",
            )
            append(plans, row)
            return identifier

        contact("Löfte", 12, due=today - timedelta(days=1))
        contact("Granska", 240, due=today - timedelta(days=200))
        contact("Bokad", 240, due=today - timedelta(days=200), suffix="-older-promise")
        plan("Kalender igår", -1, "visit", confirmed=True, picking=True)
        plan("Dagens aktivitet", 0, "email")
        for age in [35, 65, 95]:
            order("Bokad", age, 240)
        plan("Bokad", 1, confirmed=True)
        order("Första order", 8, 40)
        order("Orderpaus", 2, 120)
        if app_module.request.args.get("pagination") == "1":
            for index in range(61):
                contact("Löfte", 13 + index, due=today - timedelta(days=1), suffix=f"-page-{index}")
        app_module._sheet_read_cache.clear()
        return {"ok": True, "stores": stores, "today": today.isoformat()}

    @app_module.app.get("/__test__/priority-workflow-state")
    def priority_workflow_state():
        # Limited fixture evidence for save/retry assertions; no production route.
        return {"ok": True,
                "contacts": [row for row in spreadsheet.worksheet("sales_activities").dict_rows()
                             if str(row.get("customer_id", "")).startswith("a3300000-")],
                "activities": [row for row in spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows()
                               if str(row.get("customer_id", "")).startswith("a3300000-")]}
