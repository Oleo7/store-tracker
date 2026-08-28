from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import Mock, patch
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from tests.test_planning import NOW, default_spreadsheet  # noqa: E402


class SalesCoachingAccessTests(TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="sales-coaching-test")
        app_module._sheet_read_cache.clear()
        self.spreadsheet = default_spreadsheet()
        self.spreadsheet.worksheet("sales_activities").append_row([
            {
                "date_time": "2026-07-27 09:00",
                "sales_person": "Olle",
                "sales_user_name": "olle",
                "customer": "Butik A",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "customer_number": "C-1",
                "contact_channel": "Telefon",
                "contact_type_key": "phone",
                "result": "Positiv",
                "result_class": "positive",
                "activity_source": "manual",
                "contact_id": "contact-1",
            }.get(column, "")
            for column in app_module.CONTACT_COLUMNS
        ])
        self.spreadsheet.worksheet("order_rows").append_row([
            {
                "Reference": "ORDER-1",
                "Order date": "2026-07-28",
                "Customer": "Butik A",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "Customer number": "C-1",
                "Quantity": "2",
                "Unit": "DFP",
                "Total": "200",
                "Currency": "SEK",
            }.get(column, "")
            for column in app_module.ORDER_COLUMNS
        ])
        self.spreadsheet_patch = patch.object(
            app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet
        )
        self.now_patch = patch.object(app_module, "stockholm_now", return_value=NOW)
        self.today_patch = patch.object(app_module, "stockholm_today", return_value=NOW.date())
        self.spreadsheet_mock = self.spreadsheet_patch.start()
        self.now_patch.start()
        self.today_patch.start()
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.today_patch.stop()
        self.now_patch.stop()
        self.spreadsheet_patch.stop()
        app_module._sheet_read_cache.clear()

    def login(self, admin):
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = {
                "user_name": "admin" if admin else "olle",
                "name": "Admin" if admin else "Olle",
                "role": "Administratör" if admin else "Säljare",
                "admin": bool(admin),
            }

    def test_non_admin_gets_403_on_both_endpoints_before_sheet_read(self):
        self.login(False)

        summary = self.client.get("/sales-coaching-insights")
        drilldown = self.client.get(
            "/sales-coaching-insights/drilldown?metric=human_activities"
        )

        self.assertEqual(summary.status_code, 403)
        self.assertEqual(drilldown.status_code, 403)
        self.assertEqual(summary.get_json()["error"], "admin_required")
        self.spreadsheet_mock.assert_not_called()

    def test_admin_contract_has_data_quality_and_get_does_not_write(self):
        self.login(True)
        before = {
            title: deepcopy(sheet.values)
            for title, sheet in self.spreadsheet.sheets.items()
        }
        added_before = list(self.spreadsheet.added_sheets)

        response = self.client.get(
            "/sales-coaching-insights?start=2026-07-01&end=2026-07-31&seller=olle"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        payload = response.get_json()
        self.assertEqual(
            set(payload),
            {
                "meta", "options", "data_quality", "metric_definitions", "kpis",
                "seller_comparison", "team_comparison", "coaching_matrix",
                "team_order_10d_trend", "coaching_matrices", "funnel", "outcome_10d",
                "weekly_trend", "visit_efficiency", "channel_effectiveness",
                "priority_allocation", "follow_up_discipline", "coaching_cards",
            },
        )
        self.assertIn("secure_customer_identity", payload["data_quality"])
        self.assertIn("order_attribution_identity_coverage", payload["data_quality"])
        self.assertIn("flagged_activity_rows", payload["data_quality"])
        self.assertIn("quality_issue_count", payload["data_quality"])
        self.assertIn("core_analytics", payload["data_quality"])
        self.assertIn("historical_priority", payload["data_quality"])
        self.assertIn("positive_dialogue", payload["metric_definitions"])
        self.assertNotIn("admin", payload["options"]["sellers"])
        self.assertEqual(
            payload["team_comparison"]["sellers"],
            payload["seller_comparison"],
        )
        self.assertEqual(self.spreadsheet.added_sheets, added_before)
        self.assertEqual(
            {title: sheet.values for title, sheet in self.spreadsheet.sheets.items()},
            before,
        )

    def test_invalid_seller_filter_and_drilldown_limits_are_stable(self):
        self.login(True)

        seller = self.client.get("/sales-coaching-insights?seller=does-not-exist")
        metric = self.client.get(
            "/sales-coaching-insights/drilldown?metric=private_sheet_column"
        )
        limit = self.client.get(
            "/sales-coaching-insights/drilldown?metric=human_activities&limit=201"
        )

        self.assertEqual(seller.status_code, 400)
        self.assertEqual(seller.get_json()["error"], "invalid_seller")
        self.assertEqual(metric.get_json()["error"], "invalid_metric")
        self.assertEqual(limit.get_json()["error"], "invalid_limit")

    def test_drilldown_is_whitelisted_and_capped_contract_is_safe(self):
        self.login(True)

        response = self.client.get(
            "/sales-coaching-insights/drilldown"
            "?metric=human_activities&start=2026-07-01&end=2026-07-31&limit=1"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        payload = response.get_json()
        self.assertEqual(payload["limit"], 1)
        self.assertLessEqual(len(payload["rows"]), 1)
        self.assertNotIn("comment", payload["rows"][0])

    def test_each_present_analytics_sheet_is_read_at_most_once_per_request(self):
        self.login(True)
        app_module._sheet_read_cache.clear()
        readers = {}
        for title, sheet in self.spreadsheet.sheets.items():
            readers[title] = Mock(wraps=sheet.get_all_values)
            sheet.get_all_values = readers[title]

        response = self.client.get(
            "/sales-coaching-insights?start=2026-07-01&end=2026-07-31"
        )

        self.assertEqual(response.status_code, 200, response.get_json())
        for title in (
            "customers_enriched", "sales_activities", "order_rows",
            app_module.USERS_SHEET, app_module.PLANNED_ACTIVITIES_SHEET,
        ):
            self.assertLessEqual(
                readers[title].call_count,
                1,
                f"{title} read {readers[title].call_count} times",
            )


class ContactAnalyticsWriteTests(TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="contact-analytics-test")
        app_module._sheet_read_cache.clear()
        self.spreadsheet = default_spreadsheet()
        self.patches = [
            patch.object(app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet),
            patch.object(app_module, "stockholm_now", return_value=NOW),
            patch.object(app_module, "stockholm_today", return_value=NOW.date()),
        ]
        for item in self.patches:
            item.start()
        self.client = app_module.app.test_client()
        user = self.spreadsheet.worksheet(app_module.USERS_SHEET).dict_rows()[0]
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(user)

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        app_module._sheet_read_cache.clear()

    def post(self, **overrides):
        payload = {
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "date_time": "2026-07-27 10:00",
            "contact_channel": "Besök",
            "result": "Ej anträffbar",
            "comment": "Ingen på plats",
            **overrides,
        }
        return self.client.post("/customers/Butik%20A/contacts", json=payload)

    def test_unreachable_visit_allows_unobserved_freezer_but_other_visit_does_not(self):
        with patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value={"priorities": []},
        ):
            unreachable = self.post(client_request_id="unreachable-visit-1")
        positive = self.post(
            client_request_id="positive-visit-1",
            result="Positiv",
        )

        self.assertEqual(unreachable.status_code, 200, unreachable.get_json())
        self.assertEqual(positive.status_code, 400)
        self.assertEqual(positive.get_json()["error"], "freezer_selection_required")
        saved = self.spreadsheet.worksheet("sales_activities").dict_rows()[0]
        self.assertTrue(all(not saved[column] for column in app_module.FREEZER_COLUMNS))
        self.assertEqual(saved["result_class"], "unreachable")
        self.assertEqual(saved["analytics_snapshot_version"], "sales_coaching_v2")

    def test_inaccessible_strong_id_is_rejected_before_malformed_payload(self):
        response = self.client.post(
            "/customers/Butik%20B/contacts",
            json={"customer_id": "22222222-2222-4222-8222-222222222222"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "customer_not_found")

    def test_accessible_strong_id_reaches_payload_validation(self):
        response = self.client.post(
            "/customers/Butik%20A/contacts",
            json={"customer_id": "11111111-1111-4111-8111-111111111111"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "freezer_selection_required")

    def test_strong_id_and_route_name_mismatch_is_hidden(self):
        response = self.client.post(
            "/customers/Butik%20A/contacts",
            json={"customer_id": "33333333-3333-4333-8333-333333333333"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "customer_not_found")

    def test_legacy_name_only_request_keeps_payload_validation_order(self):
        response = self.client.post("/customers/Butik%20B/contacts", json={})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "freezer_selection_required")

    def test_pre_contact_snapshot_is_written_once_and_replay_preserves_it(self):
        priorities = [{
            "customer_id": "11111111-1111-4111-8111-111111111111",
            "customer_number": "C-1",
            "customer": "Butik A",
            "sales_person": "Olle",
            "priority_score": 88,
            "score_version": "v2.1",
            "intent_timing": 95,
            "value_index": 70,
            "strategic_index": 100,
            "expected_order_dfp": 20,
            "lifecycle": "prospect",
            "segment": "A",
            "recommendation_eligible": True,
            "recommendation_suppression_reason": "",
        }]
        with patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value={"priorities": priorities},
        ) as snapshot_mock:
            first = self.post(client_request_id="snapshot-replay-1")
            first_row = deepcopy(
                self.spreadsheet.worksheet("sales_activities").dict_rows()[0]
            )
            replay = self.post(client_request_id="snapshot-replay-1")

        rows = self.spreadsheet.worksheet("sales_activities").dict_rows()
        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(replay.status_code, 200, replay.get_json())
        self.assertTrue(replay.get_json()["duplicate"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["priority_snapshot_quality"], "exact")
        self.assertEqual(rows[0]["analytics_snapshot_version"], "sales_coaching_v2")
        self.assertEqual(rows[0]["priority_score_at_contact"], 88)
        self.assertEqual(
            rows[0]["priority_percentile_basis_at_contact"],
            "owner_active_scored_portfolio_midrank_v2",
        )
        self.assertTrue(rows[0]["snapshot_created_at"])
        self.assertIsNotNone(rows[0]["snapshot_lag_hours"])
        self.assertIs(rows[0]["recommendation_eligible_at_contact"], True)
        self.assertEqual(rows[0]["suppression_reason_at_contact"], "")
        self.assertEqual(rows[0], first_row)
        snapshot_mock.assert_called_once()

    def test_priority_percentile_basis_is_last_analytics_column(self):
        self.assertEqual(
            app_module.CONTACT_ANALYTICS_COLUMNS[-1],
            "priority_percentile_basis_at_contact",
        )


if __name__ == "__main__":
    main()
