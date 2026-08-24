import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
from unittest import TestCase
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from test_planning import default_spreadsheet


class Performance1ATests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")

    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="performance-1a-test")
        app_module._sheet_read_cache.clear()
        self.addCleanup(app_module._sheet_read_cache.clear)
        current_user_patcher = patch.object(
            app_module,
            "current_user",
            return_value={"user_name": "performance-test", "role": "Säljare"},
        )
        current_user_patcher.start()
        self.addCleanup(current_user_patcher.stop)

    def _run_customer_cache_contention(self, *, invalidate_during_load=False):
        spreadsheet = default_spreadsheet()
        spreadsheet._store_tracker_enable_read_cache = True
        # Keep the request focused on the contended customers dataset.
        app_module.get_contact_rows(spreadsheet)
        sheet = spreadsheet.worksheet("customers_enriched")
        original_loader = sheet.get_all_values
        loader_started = threading.Event()
        release_loader = threading.Event()
        waiter_waiting = threading.Event()
        calls = 0
        clock_counts = {}
        clock_values = {}
        clock_lock = threading.Lock()
        original_clock = app_module._sheet_read_cache.monotonic

        def monitored_clock():
            name = threading.current_thread().name
            with clock_lock:
                count = clock_counts.get(name, 0) + 1
                clock_counts[name] = count
                value = max(time.monotonic(), clock_values.get(name, 0.0))
                if name == "cache-waiter" and count == 2:
                    waiter_waiting.set()
                elif name == "cache-waiter" and count == 3:
                    value = max(value, clock_values[name] + 0.125)
                clock_values[name] = value
                return value

        def load():
            nonlocal calls
            calls += 1
            if calls == 1:
                loader_started.set()
                self.assertTrue(release_loader.wait(2))
            return original_loader()

        responses = {}
        errors = []

        def request_customers(name):
            try:
                responses[name] = app_module.app.test_client().get("/customers")
            except Exception as error:
                errors.append(error)

        sheet.get_all_values = load
        app_module._sheet_read_cache.monotonic = monitored_clock
        try:
            with patch.dict(
                os.environ,
                {"PERFORMANCE_LOGGING_ENABLED": "true"},
                clear=False,
            ), patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=spreadsheet,
            ), self.assertLogs(
                app_module.PERFORMANCE_LOGGER_NAME,
                level=logging.INFO,
            ) as captured:
                loader_thread = threading.Thread(
                    target=request_customers,
                    args=("loader",),
                    name="cache-loader",
                )
                waiter_thread = threading.Thread(
                    target=request_customers,
                    args=("waiter",),
                    name="cache-waiter",
                )
                loader_thread.start()
                self.assertTrue(loader_started.wait(2))
                waiter_thread.start()
                self.assertTrue(waiter_waiting.wait(2))
                if invalidate_during_load:
                    app_module._sheet_read_cache.invalidate(
                        spreadsheet, "customers_enriched"
                    )
                release_loader.set()
                loader_thread.join(2)
                waiter_thread.join(2)
                self.assertFalse(loader_thread.is_alive())
                self.assertFalse(waiter_thread.is_alive())
                responses["immediate"] = app_module.app.test_client().get(
                    "/customers"
                )
        finally:
            sheet.get_all_values = original_loader
            app_module._sheet_read_cache.monotonic = original_clock

        self.assertFalse(errors)
        self.assertEqual(
            {response.status_code for response in responses.values()}, {200}
        )
        records = [json.loads(record.getMessage()) for record in captured.records]
        return records, calls, captured.output

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

    def test_planning_activities_emits_structured_performance_records_at_info(self):
        spreadsheet = default_spreadsheet()
        with patch.dict(
            os.environ,
            {"PERFORMANCE_LOGGING_ENABLED": "true"},
            clear=False,
        ), patch.object(
            app_module,
            "get_spreadsheet_with_retry",
            return_value=spreadsheet,
        ), patch.object(
            app_module,
            "current_user",
            return_value={"user_name": "olle", "role": "Säljare"},
        ), self.assertLogs(
            app_module.PERFORMANCE_LOGGER_NAME,
            level=logging.INFO,
        ) as captured:
            response = app_module.app.test_client().get(
                "/planning/activities?start=2026-07-27&end=2026-08-02"
            )

        self.assertEqual(response.status_code, 200, response.get_json())
        records = [json.loads(record.getMessage()) for record in captured.records]
        self.assertTrue(records)
        self.assertTrue(all(
            record.levelno == logging.INFO for record in captured.records
        ))
        self.assertEqual({entry["event"] for entry in records}, {"performance"})
        self.assertEqual(
            {entry["endpoint"] for entry in records},
            {"/planning/activities"},
        )
        self.assertEqual(len({entry["request_id"] for entry in records}), 1)
        self.assertTrue(next(iter({entry["request_id"] for entry in records})))
        self.assertIn("total", {entry["step"] for entry in records})
        for entry in records:
            with self.subTest(step=entry["step"]):
                self.assertIn("total_ms", entry)
                self.assertIn("duration_ms", entry)
                self.assertIn("row_count", entry)
                self.assertIn("google_sheets_read_count", entry)
                self.assertNotIn("customer", entry)
        self.assertNotIn("Butik A", " ".join(captured.output))

    def test_performance_logger_has_dedicated_info_stdout_handler(self):
        handlers = [
            handler for handler in app_module.performance_logger.handlers
            if getattr(handler, "store_tracker_performance_handler", False)
        ]

        self.assertEqual(len(handlers), 1)
        self.assertEqual(app_module.performance_logger.level, logging.INFO)
        self.assertFalse(app_module.performance_logger.propagate)
        self.assertEqual(handlers[0].level, logging.INFO)
        self.assertIs(handlers[0].stream, sys.stdout)

    def test_waited_hit_emits_wait_without_counting_physical_read(self):
        records, calls, output = self._run_customer_cache_contention()
        wait_records = [
            record for record in records
            if record["step"] == "sheets.cache.wait.customers_enriched"
        ]
        self.assertEqual(calls, 1)
        self.assertEqual(len(wait_records), 1)
        self.assertGreater(wait_records[0]["duration_ms"], 0)
        waiter_request_id = wait_records[0]["request_id"]
        waiter_records = [
            record for record in records
            if record["request_id"] == waiter_request_id
        ]
        self.assertEqual(
            {record["google_sheets_read_count"] for record in waiter_records},
            {0},
        )
        self.assertIn(
            "sheets.cache.hit",
            {record["step"] for record in waiter_records},
        )
        request_ids_with_wait = {
            record["request_id"] for record in records
            if record["step"].startswith("sheets.cache.wait.")
        }
        immediate_ids = {
            record["request_id"] for record in records
            if record["step"] == "total"
            and record["google_sheets_read_count"] == 0
            and record["request_id"] not in request_ids_with_wait
        }
        self.assertEqual(len(immediate_ids), 1)
        self.assertNotIn("Butik A", " ".join(output))

    def test_wait_then_loader_and_store_skipped_telemetry_are_exact(self):
        records, calls, output = self._run_customer_cache_contention(
            invalidate_during_load=True
        )
        self.assertEqual(calls, 2)
        wait_records = [
            record for record in records
            if record["step"] == "sheets.cache.wait.customers_enriched"
        ]
        skipped_records = [
            record for record in records
            if record["step"]
            == "sheets.cache.store_skipped.customers_enriched"
        ]
        self.assertEqual(len(wait_records), 1)
        self.assertGreater(wait_records[0]["duration_ms"], 0)
        self.assertEqual(len(skipped_records), 1)
        self.assertEqual(skipped_records[0]["duration_ms"], 0)
        waiter_request_id = wait_records[0]["request_id"]
        skipped_request_id = skipped_records[0]["request_id"]
        for request_id in (waiter_request_id, skipped_request_id):
            request_records = [
                record for record in records
                if record["request_id"] == request_id
            ]
            self.assertEqual(
                {
                    record["google_sheets_read_count"]
                    for record in request_records
                },
                {1},
            )
        serialized = " ".join(output)
        for forbidden in (
            "Butik A",
            "11111111-1111-4111-8111-111111111111",
            "performance-test",
        ):
            self.assertNotIn(forbidden, serialized)


if __name__ == "__main__":
    import unittest

    unittest.main()
