"""Integration at the actual auth/snapshot/customer-insights boundary.

The opt-in v3.1 response is ID-keyed; the old name-keyed response remains
unchanged for planning and historical clients.
"""
from datetime import datetime, timedelta
from unittest.mock import patch
import app as app_module
from test_planning import PlanningApiTestCase, NOW


class ValueContactApiStage1Tests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.invalidate_priority_snapshot()

    def tearDown(self):
        app_module.invalidate_priority_snapshot()
        super().tearDown()

    def test_legacy_response_untouched_and_opt_in_scoring_by_id(self):
        legacy = self.client.get("/customer-insights")
        self.assertEqual(legacy.status_code, 200)
        self.assertNotIn("__by_customer_id", legacy.get_json())

        response = self.client.get("/customer-insights", headers={"X-PB-Value-Model": "v3.1"})
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        by_id = payload["__by_customer_id"]
        self.assertIn("11111111-1111-4111-8111-111111111111", by_id)
        self.assertIn("33333333-3333-4333-8333-333333333333", by_id)
        self.assertNotIn("22222222-2222-4222-8222-222222222222", by_id)
        # Archived stores can remain visible but must not be recommended.
        archived = by_id["44444444-4444-4444-8444-444444444444"]
        self.assertIsNone(archived["contact_priority_raw"])
        active = [item for item in by_id.values()
                  if item.get("value_model_version") == "v3.3-preview"]
        self.assertTrue(active, "Actual v3.3 payload must be checked; no vacuous version-filter loop")
        for item in active:
            self.assertIsNotNone(item["business_value_raw"])
            self.assertIsNotNone(item["contact_priority_raw"])
            self.assertIn(item["list_guidance"]["status_key"],
                          ("act_now", "planned", "wait", "idle", "overdue_followup", "data_missing"))
            self.assertEqual(item["value_model_version"], "v3.3-preview")
            self.assertIn("priority_score", item)  # Legacy routing/scoring preserved.
            self.assertIn("work_priority_raw", item)
            self.assertEqual(item["customer_guidance"], item["list_guidance"])
            if not item["list_guidance"]["can_start_unscheduled_contact"]:
                self.assertEqual(item["contact_priority_raw"], 0)

    def test_two_stores_same_display_name_are_not_cross_associated(self):
        sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = sheet.values[0]
        other = {
            "customer": "Butik A", "customer_id": "99999999-9999-4999-8999-999999999999",
            "sales_person": "Olle", "customer_segment": "C", "customer_number": "C-99",
        }
        sheet.append_row([other.get(h, "") for h in headers])
        order = {
            "Reference": "value-cid-99", "Customer": "Butik A",
            "Customer number": "C-99", "customer_id": other["customer_id"],
            "Order date": (NOW.date() - timedelta(days=65)).isoformat(),
            "Delivery date": (NOW.date() - timedelta(days=65)).isoformat(),
            "Total weight": "60", "Total": "1000",
        }
        order_sheet = self.spreadsheet.worksheet("order_rows")
        order_sheet.append_row([order.get(h, "") for h in order_sheet.values[0]])
        app_module.invalidate_priority_snapshot()
        payload = self.client.get("/customer-insights", headers={"X-PB-Value-Model": "v3.1"}).get_json()
        indexed = payload["__by_customer_id"]
        first = indexed["11111111-1111-4111-8111-111111111111"]
        second = indexed[other["customer_id"]]
        self.assertEqual(first["business_deliveries_used"], 0)
        self.assertEqual(second["business_deliveries_used"], 1)
        self.assertNotEqual(first["business_value_raw"], second["business_value_raw"])
