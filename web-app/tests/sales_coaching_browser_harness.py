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
    for seller, positive_count in (("olle", 2), ("sofia", 6), ("viewer", 8)):
        for index in range(10):
            row = {
                "date_time": (now - timedelta(days=index + 1)).isoformat(
                    timespec="minutes"
                ),
                "sales_person": seller,
                "sales_user_name": seller,
                "customer": "Butik A",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "contact_channel": "Telefon",
                "result": "Positiv" if index < positive_count else "Neutral",
                "activity_source": "manual",
                "contact_id": f"smoke-{seller}-{index}",
            }
            activity_sheet.append_row([row.get(header, "") for header in headers])

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
