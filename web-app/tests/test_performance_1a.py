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

    def test_customer_insights_does_not_request_email_events(self):
        spreadsheet = object()
        with (
            patch.object(app_module, "get_spreadsheet_with_retry", return_value=spreadsheet),
            patch.object(app_module, "get_customer_rows", return_value=[]),
            patch.object(app_module, "get_contact_rows", return_value=[]),
            patch.object(app_module, "get_order_rows", return_value=[]),
            patch.object(app_module, "get_email_rows", return_value=([], [], [])) as email_rows,
        ):
            response = app_module.app.test_client().get("/customer-insights")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {})
        email_rows.assert_called_once_with(spreadsheet, include_events=False)


if __name__ == "__main__":
    import unittest

    unittest.main()
