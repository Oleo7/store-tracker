from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
import sys
import threading
import time
from unittest import TestCase
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from legacy_email_identity import plan_legacy_email_identity_backfill
from priority import build_contact_features, build_order_features, build_priority_customers
from sheets_availability import SheetReadCache, read_with_retry
from tests.test_planning import PlanningApiTestCase, default_spreadsheet


class FakeHttpError(Exception):
    def __init__(self, status, retry_after=None):
        self.response = type("Response", (), {
            "status_code": status,
            "headers": ({"Retry-After": retry_after} if retry_after else {}),
        })()


class SheetAvailabilityUnitTests(TestCase):
    def test_concurrent_identical_reads_are_single_flight_and_defensive(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        calls = 0
        lock = threading.Lock()

        def load():
            nonlocal calls
            with lock:
                calls += 1
            time.sleep(0.03)
            return [["header"], ["value"]]

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(
                lambda _item: cache.values(
                    spreadsheet, "customers_enriched", loader=load
                )[0],
                range(8),
            ))
        results[0][1][0] = "mutated"
        warm, hit = cache.values(
            spreadsheet, "customers_enriched", loader=load
        )
        self.assertEqual(calls, 1)
        self.assertTrue(hit)
        self.assertEqual(warm[1][0], "value")

    def test_first_429_retries_then_succeeds_and_honors_retry_after(self):
        calls = 0
        sleeps = []

        def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise FakeHttpError(429, "1.5")
            return "ok"

        result = read_with_retry(
            operation, sleep=sleeps.append, random_value=lambda: 0
        )
        self.assertEqual(result, "ok")
        self.assertEqual(calls, 2)
        self.assertEqual(sleeps, [1.5])

    def test_permanent_403_is_not_retried(self):
        calls = 0

        def operation():
            nonlocal calls
            calls += 1
            raise FakeHttpError(403)

        with self.assertRaises(FakeHttpError):
            read_with_retry(operation, sleep=lambda _delay: None)
        self.assertEqual(calls, 1)

    def test_worksheet_handles_are_cached_and_reconnect_clear_invalidates(self):
        cache = SheetReadCache()
        spreadsheet = object()
        calls = 0

        def load():
            nonlocal calls
            calls += 1
            return object()

        first = cache.worksheet(spreadsheet, "users", loader=load)
        second = cache.worksheet(spreadsheet, "users", loader=load)
        cache.invalidate(spreadsheet, "users", worksheets=True)
        third = cache.worksheet(spreadsheet, "users", loader=load)
        self.assertIs(first, second)
        self.assertIsNot(first, third)
        self.assertEqual(calls, 2)


class AppAvailabilityTests(TestCase):
    def setUp(self):
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()
        self.spreadsheet = default_spreadsheet()
        self.spreadsheet._store_tracker_enable_read_cache = True

    def tearDown(self):
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()

    def test_write_invalidates_cached_dataset(self):
        first = app_module.get_customer_rows(self.spreadsheet)
        self.assertEqual(first[0]["customer"], "Butik A")
        sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = sheet.row_values(1)
        app_module.update_sheet_row(
            sheet, 2, headers, {"customer": "Butik A uppdaterad"}
        )
        second = app_module.get_customer_rows(self.spreadsheet)
        self.assertEqual(second[0]["customer"], "Butik A uppdaterad")

    def test_append_write_is_never_blindly_retried(self):
        sheet = self.spreadsheet.worksheet("sales_activities")
        sheet.fail_next_batch_update = FakeHttpError(429)
        before = sheet.batch_update_count
        with self.assertRaises(FakeHttpError):
            app_module.append_dict_row(
                sheet,
                app_module.CONTACT_COLUMNS,
                {"date_time": "2026-08-08", "customer": "Butik A"},
            )
        self.assertEqual(sheet.batch_update_count - before, 1)

    def test_global_snapshot_is_reused_then_invalidated(self):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        with patch.object(
            app_module, "build_current_priority_snapshot",
            wraps=app_module.build_current_priority_snapshot,
        ) as build:
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            self.assertEqual(build.call_count, 1)
            app_module.invalidate_sheet_cache(
                self.spreadsheet, "sales_activities"
            )
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            self.assertEqual(build.call_count, 2)

    def test_email_sales_activity_persists_canonical_customer_id(self):
        app_module.build_sales_activity_for_email(
            self.spreadsheet,
            email_id="email-identity-1",
            email_type="reminder",
            customer_name="Butik A",
            customer_id="11111111-1111-4111-8111-111111111111",
            user={"user_name": "olle", "name": "Olle"},
            recipients=["buyer@example.com"],
            partial=False,
        )
        row = self.spreadsheet.worksheet("sales_activities").dict_rows()[-1]
        self.assertEqual(
            row["customer_id"], "11111111-1111-4111-8111-111111111111"
        )
        self.assertEqual(row["email_id"], "email-identity-1")


class ConcurrentEndpointAvailabilityTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        self.spreadsheet._store_tracker_enable_read_cache = True
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()

    def tearDown(self):
        app_module._sheet_read_cache.clear()
        app_module.invalidate_priority_snapshot()
        super().tearDown()

    def _client(self, user_name="olle"):
        client = app_module.app.test_client()
        user = next(
            row for row in self.spreadsheet.worksheet(
                app_module.USERS_SHEET
            ).dict_rows()
            if row["user_name"] == user_name
        )
        with client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(user)
        return client

    def test_concurrent_hot_endpoints_have_bounded_reads_and_owner_isolation(self):
        counts = {}
        count_lock = threading.Lock()
        budget_titles = {
            "customers_enriched", "order_rows", "sales_activities", "users",
            "email_messages", "email_recipients", "planned_activities",
        }
        for title in budget_titles:
            sheet = self.spreadsheet.worksheet(title)
            original = sheet.get_all_values

            def counted(original=original, title=title):
                with count_lock:
                    counts[title] = counts.get(title, 0) + 1
                time.sleep(0.01)
                return original()

            sheet.get_all_values = counted

        requests = (
            ("/customer-insights", "olle"),
            ("/planning/activities?user_name=olle&start=2026-07-27&end=2026-08-02", "olle"),
            ("/planning/suggestions?user_name=olle", "olle"),
        )

        def request_endpoint(item):
            path, user_name = item
            return self._client(user_name).get(path).status_code

        with ThreadPoolExecutor(max_workers=3) as pool:
            statuses = list(pool.map(request_endpoint, requests))
        self.assertEqual(statuses, [200, 200, 200])
        self.assertLessEqual(sum(counts.values()), len(budget_titles), counts)
        self.assertTrue(
            all(counts.get(title, 0) <= 1 for title in budget_titles),
            counts,
        )
        counts.clear()
        with ThreadPoolExecutor(max_workers=3) as pool:
            warm_statuses = list(pool.map(request_endpoint, requests))
        self.assertEqual(warm_statuses, [200, 200, 200])
        self.assertEqual(sum(counts.values()), 0, counts)

        activity_rows = []
        olle = {"user_name": "olle", "name": "Olle"}
        sofia = {"user_name": "sofia", "name": "Sofia"}
        olle_candidates = app_module.planning_suggestion_candidates(
            self.spreadsheet, olle, activity_rows
        )
        sofia_candidates = app_module.planning_suggestion_candidates(
            self.spreadsheet, sofia, activity_rows
        )
        self.assertTrue(olle_candidates)
        self.assertTrue(sofia_candidates)
        self.assertTrue(
            {item["customer_id"] for item in olle_candidates}.isdisjoint(
                {item["customer_id"] for item in sofia_candidates}
            )
        )


class LegacyEmailIdentityTests(TestCase):
    def test_ornaset_email_resolves_old_followup_without_hiding_legitimate_reorder(self):
        customer_id = "f43f7cee-1e62-4b70-9735-0000000002e8"
        customers = [{
            "row": 2, "customer_id": customer_id,
            "customer_number": "100", "customer": "Stora COOP Örnäset",
            "sales_person": "Olle", "customer_segment": "A",
        }]
        orders = [{
            "Reference": reference, "Order date": delivered,
            "Delivery date": delivered, "customer_id": customer_id,
            "Customer number": "100", "Customer": "Stora COOP Örnäset",
            "Quantity": "20", "Total weight": "20", "Total": "400",
        } for reference, delivered in (
            ("ORDER-1", "2026-05-01"), ("ORDER-2", "2026-06-01"),
        )]
        contacts = [
            {
                "contact_id": "human-1", "customer_id": customer_id,
                "customer": "Stora COOP Örnäset",
                "date_time": "2026-06-25 10:00:00", "sales_person": "Olle",
                "contact_channel": "Telefon", "result": "Neutral",
                "follow_up_date": "2026-07-01",
            },
            {
                "contact_id": "", "email_id": "email-ornaset-1",
                "customer_id": customer_id, "customer": "Stora COOP Örnäset",
                "date_time": "2026-08-05 10:00:00", "sales_person": "Olle",
                "contact_channel": "Mejl", "result": "Mejlförslag skickat",
                "follow_up_date": "",
            },
        ]
        order_features = build_order_features(orders)
        contact_features = build_contact_features(contacts, order_features)
        feature = next(iter(contact_features.values()))
        self.assertTrue(feature["follow_up_resolved"])
        item = build_priority_customers(
            customers, order_features, contact_features, "Olle",
            date(2026, 8, 8), limit=1,
        )[0]
        self.assertNotIn("legacy_missed_followup", item["covered_trigger_keys"])
        self.assertEqual(item["primary_trigger_type"], "established_reorder_due")

    def test_backfill_prefers_unique_number_then_exact_unique_name(self):
        plan = plan_legacy_email_identity_backfill(
            customers=[
                {"customer": "Stora COOP Örnäset", "customer_number": "100", "customer_id": "f43f7cee-1e62-4b70-9735-0000000002e8"},
                {"customer": "Dublett", "customer_number": "200", "customer_id": "id-a"},
                {"customer": "Dublett", "customer_number": "201", "customer_id": "id-b"},
            ],
            message_rows=[
                (2, {"email_id": "mail-1", "customer": "Fel namn", "customer_number": "100", "customer_id": ""}),
                (3, {"email_id": "mail-2", "customer": "Stora COOP Örnäset", "customer_number": "", "customer_id": ""}),
                (4, {"email_id": "mail-3", "customer": "Dublett", "customer_number": "", "customer_id": ""}),
                (5, {"email_id": "mail-4", "customer": "Saknas", "customer_number": "", "customer_id": ""}),
            ],
            recipient_rows=[
                (2, {"email_id": "mail-1", "customer_id": ""}),
                (3, {"email_id": "unknown", "customer_id": ""}),
            ],
            activity_rows=[(2, {"email_id": "mail-2", "customer_id": ""})],
        )
        expected = "f43f7cee-1e62-4b70-9735-0000000002e8"
        self.assertEqual(plan["email_messages"]["backfilled"], 2)
        self.assertEqual(plan["email_messages"]["ambiguous"], 1)
        self.assertEqual(plan["email_messages"]["unresolved"], 1)
        self.assertTrue(all(
            item["customer_id"] == expected
            for item in plan["email_messages"]["updates"]
        ))
        self.assertEqual(
            plan["email_recipients"]["updates"][0]["customer_id"], expected
        )
        self.assertEqual(
            plan["sales_activities"]["updates"][0]["customer_id"], expected
        )

    def test_backfill_is_idempotent_for_already_repaired_rows(self):
        customer_id = "f43f7cee-1e62-4b70-9735-0000000002e8"
        plan = plan_legacy_email_identity_backfill(
            customers=[{"customer": "Stora COOP Örnäset", "customer_number": "100", "customer_id": customer_id}],
            message_rows=[(2, {"email_id": "mail-1", "customer_id": customer_id})],
            recipient_rows=[(2, {"email_id": "mail-1", "customer_id": customer_id})],
            activity_rows=[(2, {"email_id": "mail-1", "customer_id": customer_id})],
        )
        self.assertEqual(plan["totals"], {
            "examined": 0, "backfilled": 0, "ambiguous": 0, "unresolved": 0,
        })
