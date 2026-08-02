from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module


class Performance1ATests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")

    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="performance-1a-test")
        current_user_patcher = patch.object(
            app_module,
            "current_user",
            return_value={"user_name": "performance-test", "role": "Säljare"},
        )
        current_user_patcher.start()
        self.addCleanup(current_user_patcher.stop)

    def test_customer_list_renders_before_slow_or_failed_insights(self):
        start = self.html.index("function loadCustomers()")
        end = self.html.index("// ── Multi-select filter state", start)
        body = self.html[start:end]

        customers_request = body.index("fetchJsonShared(`${API}/customers`)")
        insights_request = body.index("fetchJsonShared(`${API}/customer-insights`)")
        await_customers = body.index("await customersRequest")
        first_render = body.index("renderList()")
        await_insights = body.index("await loadInsights")

        self.assertLess(customers_request, await_customers)
        self.assertLess(insights_request, await_customers)
        self.assertLess(first_render, await_insights)
        self.assertIn("if (!customerListReady", body)
        self.assertIn("parentLoadSerial", body)
        self.assertIn("Prioriteringen är klar – uppdaterar sortering", self.html)
        self.assertIn("Kunde inte ladda kundprioritering", self.html)
        self.assertIn("requestAnimationFrame(() => window.scrollTo", self.html)

    def test_get_email_rows_without_events_never_accesses_event_worksheet(self):
        accessed_worksheets = []

        class Worksheet:
            def __init__(self, title, headers):
                self.title = title
                self.headers = list(headers)

            def row_values(self, row):
                self.assert_header_row(row)
                return self.headers

            def get_all_values(self):
                return [self.headers]

            def assert_header_row(self, row):
                if row != 1:
                    raise AssertionError("Only the header row should be requested")

        class Spreadsheet:
            def worksheet(self, title):
                accessed_worksheets.append(title)
                if title == app_module.EMAIL_EVENTS_SHEET:
                    raise AssertionError("email_events must not be accessed")
                headers = {
                    app_module.EMAIL_MESSAGES_SHEET: app_module.EMAIL_MESSAGES_COLUMNS,
                    app_module.EMAIL_RECIPIENTS_SHEET: app_module.EMAIL_RECIPIENTS_COLUMNS,
                    "sales_activities": app_module.CONTACT_COLUMNS,
                }[title]
                return Worksheet(title, headers)

        app_module._email_sheets_cache = None
        self.addCleanup(setattr, app_module, "_email_sheets_cache", None)
        with patch.object(app_module, "ensure_contact_worksheet_schema"):
            rows = app_module.get_email_rows(
                Spreadsheet(), include_events=False
            )

        self.assertEqual(rows, ([], [], []))
        self.assertNotIn(app_module.EMAIL_EVENTS_SHEET, accessed_worksheets)


if __name__ == "__main__":
    import unittest

    unittest.main()
