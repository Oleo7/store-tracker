"""Isolated browser harness for sales-coaching desktop/mobile smoke tests."""

from pathlib import Path
import os
import sys

WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from test_planning import default_spreadsheet  # noqa: E402


if __name__ == "__main__":
    spreadsheet = default_spreadsheet()

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
