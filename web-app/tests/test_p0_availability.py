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

    def test_unrelated_invalidation_preserves_in_flight_store(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0

        def load():
            nonlocal calls
            calls += 1
            loader_started.set()
            self.assertTrue(release_loader.wait(2))
            return [["header"], ["sales"]]

        before = cache.generation_signature(
            spreadsheet, ["sales_activities"]
        )
        with ThreadPoolExecutor(max_workers=1) as pool:
            first_future = pool.submit(
                cache.values_with_info,
                spreadsheet,
                "sales_activities",
                loader=load,
            )
            self.assertTrue(loader_started.wait(2))
            cache.invalidate(spreadsheet, "planned_activities")
            after = cache.generation_signature(
                spreadsheet, ["sales_activities"]
            )
            release_loader.set()
            first_rows, first_info = first_future.result(timeout=2)

        second_rows, second_info = cache.values_with_info(
            spreadsheet, "sales_activities", loader=load
        )
        self.assertEqual(before, after)
        self.assertEqual(calls, 1)
        self.assertEqual(first_rows, [["header"], ["sales"]])
        self.assertTrue(first_info.stored)
        self.assertTrue(second_info.cache_hit)
        self.assertEqual(second_rows, first_rows)

    def test_same_dataset_invalidation_rejects_in_flight_store(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0

        def load():
            nonlocal calls
            calls += 1
            if calls == 1:
                loader_started.set()
                self.assertTrue(release_loader.wait(2))
                return [["header"], ["old"]]
            return [["header"], ["new"]]

        with ThreadPoolExecutor(max_workers=1) as pool:
            first_future = pool.submit(
                cache.values_with_info,
                spreadsheet,
                "sales_activities",
                loader=load,
            )
            self.assertTrue(loader_started.wait(2))
            cache.invalidate(spreadsheet, "sales_activities")
            release_loader.set()
            first_rows, first_info = first_future.result(timeout=2)

        second_rows, second_info = cache.values_with_info(
            spreadsheet, "sales_activities", loader=load
        )
        self.assertEqual(first_rows[1][0], "old")
        self.assertTrue(first_info.invalidated_during_load)
        self.assertFalse(first_info.stored)
        self.assertEqual(second_rows[1][0], "new")
        self.assertTrue(second_info.performed_load)
        self.assertTrue(second_info.stored)
        self.assertEqual(calls, 2)

    def test_multiple_waiters_survive_unrelated_invalidation_single_flight(self):
        wait_events = {
            "waiter-b": threading.Event(),
            "waiter-c": threading.Event(),
        }
        clock_counts = {}
        clock_lock = threading.Lock()

        def monitored_clock():
            name = threading.current_thread().name
            with clock_lock:
                clock_counts[name] = clock_counts.get(name, 0) + 1
                if name in wait_events and clock_counts[name] == 2:
                    wait_events[name].set()
            return time.monotonic()

        cache = SheetReadCache(ttl_seconds=12, monotonic=monitored_clock)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0
        results = {}
        errors = []

        def load():
            nonlocal calls
            calls += 1
            loader_started.set()
            self.assertTrue(release_loader.wait(2))
            return [["header"], ["current"]]

        def read(name):
            try:
                results[name] = cache.values_with_info(
                    spreadsheet, "sales_activities", loader=load
                )
            except Exception as error:
                errors.append(error)

        threads = [
            threading.Thread(target=read, args=(name,), name=name)
            for name in ("loader", "waiter-b", "waiter-c")
        ]
        threads[0].start()
        self.assertTrue(loader_started.wait(2))
        threads[1].start()
        threads[2].start()
        self.assertTrue(wait_events["waiter-b"].wait(2))
        self.assertTrue(wait_events["waiter-c"].wait(2))
        cache.invalidate(spreadsheet, "planned_activities")
        release_loader.set()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.assertFalse(errors)
        self.assertEqual(calls, 1)
        self.assertEqual(
            {rows[1][0] for rows, _info in results.values()},
            {"current"},
        )
        for name in ("waiter-b", "waiter-c"):
            self.assertTrue(results[name][1].cache_hit)
            self.assertGreater(results[name][1].waited_seconds, 0)

    def test_same_dataset_invalidation_elects_one_second_loader(self):
        wait_events = {
            "waiter-b": threading.Event(),
            "waiter-c": threading.Event(),
        }
        clock_counts = {}
        clock_lock = threading.Lock()

        def monitored_clock():
            name = threading.current_thread().name
            with clock_lock:
                clock_counts[name] = clock_counts.get(name, 0) + 1
                if name in wait_events and clock_counts[name] == 2:
                    wait_events[name].set()
            return time.monotonic()

        cache = SheetReadCache(ttl_seconds=12, monotonic=monitored_clock)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0
        active_loaders = 0
        max_active_loaders = 0
        load_lock = threading.Lock()
        results = {}
        errors = []

        def load():
            nonlocal calls, active_loaders, max_active_loaders
            with load_lock:
                calls += 1
                call_number = calls
                active_loaders += 1
                max_active_loaders = max(max_active_loaders, active_loaders)
            try:
                if call_number == 1:
                    loader_started.set()
                    self.assertTrue(release_loader.wait(2))
                    return [["header"], ["old"]]
                return [["header"], ["new"]]
            finally:
                with load_lock:
                    active_loaders -= 1

        def read(name):
            try:
                results[name] = cache.values_with_info(
                    spreadsheet, "sales_activities", loader=load
                )
            except Exception as error:
                errors.append(error)

        threads = [
            threading.Thread(target=read, args=(name,), name=name)
            for name in ("loader", "waiter-b", "waiter-c")
        ]
        threads[0].start()
        self.assertTrue(loader_started.wait(2))
        threads[1].start()
        threads[2].start()
        self.assertTrue(wait_events["waiter-b"].wait(2))
        self.assertTrue(wait_events["waiter-c"].wait(2))
        cache.invalidate(spreadsheet, "sales_activities")
        release_loader.set()
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())

        self.assertFalse(errors)
        self.assertEqual(calls, 2)
        self.assertEqual(max_active_loaders, 1)
        self.assertEqual(results["loader"][0][1][0], "old")
        self.assertTrue(results["loader"][1].invalidated_during_load)
        waiter_results = [results["waiter-b"], results["waiter-c"]]
        self.assertEqual(
            {rows[1][0] for rows, _info in waiter_results}, {"new"}
        )
        loaders = [info for _rows, info in waiter_results if info.performed_load]
        self.assertEqual(len(loaders), 1)
        self.assertGreater(loaders[0].waited_seconds, 0)

    def test_spreadsheet_and_global_invalidation_are_aba_safe(self):
        for scope in ("spreadsheet", "global"):
            with self.subTest(scope=scope):
                cache = SheetReadCache(ttl_seconds=12)
                spreadsheet = object()
                loader_started = threading.Event()
                release_loader = threading.Event()
                calls = 0

                def load():
                    nonlocal calls
                    calls += 1
                    if calls == 1:
                        loader_started.set()
                        self.assertTrue(release_loader.wait(2))
                        return [["header"], ["old"]]
                    return [["header"], ["new"]]

                old_token = cache.generation_signature(
                    spreadsheet, ["sales_activities"]
                )
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(
                        cache.values_with_info,
                        spreadsheet,
                        "sales_activities",
                        loader=load,
                    )
                    self.assertTrue(loader_started.wait(2))
                    if scope == "spreadsheet":
                        cache.invalidate(spreadsheet)
                    else:
                        cache.clear()
                    current_token = cache.generation_signature(
                        spreadsheet, ["sales_activities"]
                    )
                    release_loader.set()
                    _old_rows, old_info = future.result(timeout=2)

                current_rows, current_info = cache.values_with_info(
                    spreadsheet, "sales_activities", loader=load
                )
                self.assertNotEqual(old_token, current_token)
                self.assertTrue(old_info.invalidated_during_load)
                self.assertEqual(current_rows[1][0], "new")
                self.assertTrue(current_info.performed_load)
                self.assertEqual(calls, 2)

    def test_generation_signature_is_immutable_deterministic_and_scoped(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        titles = ["sales_activities", "order_rows"]
        initial = cache.generation_signature(spreadsheet, titles)
        reordered = cache.generation_signature(
            spreadsheet, reversed(titles)
        )
        self.assertEqual(initial, reordered)
        self.assertIsInstance(initial, tuple)
        with self.assertRaises(TypeError):
            initial[0] = ("changed", (0, 0, 0))

        cache.invalidate(spreadsheet, "score_events")
        self.assertEqual(
            initial, cache.generation_signature(spreadsheet, titles)
        )
        cache.invalidate(spreadsheet, "sales_activities")
        relevant = cache.generation_signature(spreadsheet, titles)
        self.assertNotEqual(initial, relevant)
        cache.invalidate(spreadsheet)
        broad = cache.generation_signature(spreadsheet, titles)
        self.assertNotEqual(relevant, broad)
        cache.clear()
        self.assertNotEqual(
            broad, cache.generation_signature(spreadsheet, titles)
        )

    def test_multiple_titles_and_cross_spreadsheet_title_invalidation_are_exact(self):
        cache = SheetReadCache(ttl_seconds=12)
        first_spreadsheet = object()
        second_spreadsheet = object()
        all_titles = ["sales_activities", "planned_activities", "order_rows"]
        first_before = dict(cache.generation_signature(
            first_spreadsheet, all_titles
        ))
        second_before = dict(cache.generation_signature(
            second_spreadsheet, all_titles
        ))
        cache.values(
            first_spreadsheet,
            "sales_activities",
            loader=lambda: [["first"]],
        )
        cache.values(
            second_spreadsheet,
            "sales_activities",
            loader=lambda: [["second"]],
        )

        cache.invalidate(
            first_spreadsheet,
            "sales_activities",
            "planned_activities",
        )
        first_after = dict(cache.generation_signature(
            first_spreadsheet, all_titles
        ))
        second_after = dict(cache.generation_signature(
            second_spreadsheet, all_titles
        ))
        self.assertNotEqual(
            first_before["sales_activities"],
            first_after["sales_activities"],
        )
        self.assertNotEqual(
            first_before["planned_activities"],
            first_after["planned_activities"],
        )
        self.assertEqual(
            first_before["order_rows"], first_after["order_rows"]
        )
        self.assertEqual(second_before, second_after)

        cache.invalidate(None, "sales_activities")
        self.assertNotEqual(
            first_after["sales_activities"],
            dict(cache.generation_signature(
                first_spreadsheet, all_titles
            ))["sales_activities"],
        )
        self.assertNotEqual(
            second_after["sales_activities"],
            dict(cache.generation_signature(
                second_spreadsheet, all_titles
            ))["sales_activities"],
        )

    def test_title_only_invalidation_rejects_known_in_flight_load(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0

        def load():
            nonlocal calls
            calls += 1
            if calls == 1:
                loader_started.set()
                self.assertTrue(release_loader.wait(2))
                return [["old"]]
            return [["new"]]

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                cache.values_with_info,
                spreadsheet,
                "sales_activities",
                loader=load,
            )
            self.assertTrue(loader_started.wait(2))
            cache.invalidate(None, "sales_activities")
            release_loader.set()
            _rows, old_info = future.result(timeout=2)

        current_rows, current_info = cache.values_with_info(
            spreadsheet, "sales_activities", loader=load
        )
        self.assertTrue(old_info.invalidated_during_load)
        self.assertEqual(current_rows, [["new"]])
        self.assertTrue(current_info.performed_load)
        self.assertEqual(calls, 2)

    def test_loader_exception_releases_single_flight_waiters(self):
        waiter_waiting = threading.Event()
        clock_counts = {}
        clock_lock = threading.Lock()

        def monitored_clock():
            name = threading.current_thread().name
            with clock_lock:
                clock_counts[name] = clock_counts.get(name, 0) + 1
                if name == "exception-waiter" and clock_counts[name] == 2:
                    waiter_waiting.set()
            return time.monotonic()

        cache = SheetReadCache(ttl_seconds=12, monotonic=monitored_clock)
        spreadsheet = object()
        loader_started = threading.Event()
        release_loader = threading.Event()
        calls = 0
        waiter_result = []

        def load():
            nonlocal calls
            calls += 1
            if calls == 1:
                loader_started.set()
                self.assertTrue(release_loader.wait(2))
                raise RuntimeError("expected read failure")
            return [["current"]]

        with ThreadPoolExecutor(max_workers=1) as pool:
            failed_future = pool.submit(
                cache.values_with_info,
                spreadsheet,
                "sales_activities",
                loader=load,
            )
            self.assertTrue(loader_started.wait(2))
            waiter = threading.Thread(
                target=lambda: waiter_result.append(
                    cache.values_with_info(
                        spreadsheet, "sales_activities", loader=load
                    )
                ),
                name="exception-waiter",
            )
            waiter.start()
            self.assertTrue(waiter_waiting.wait(2))
            release_loader.set()
            with self.assertRaises(RuntimeError):
                failed_future.result(timeout=2)
            waiter.join(2)
            self.assertFalse(waiter.is_alive())

        self.assertEqual(waiter_result[0][0], [["current"]])
        self.assertTrue(waiter_result[0][1].performed_load)
        self.assertEqual(calls, 2)
        self.assertFalse(cache._loading)

    def test_read_metadata_and_defensive_copy_contract(self):
        cache = SheetReadCache(ttl_seconds=12)
        spreadsheet = object()
        calls = 0

        def load():
            nonlocal calls
            calls += 1
            return [["header"], ["value"]]

        first_rows, first_info = cache.values_with_info(
            spreadsheet, "customers_enriched", loader=load
        )
        self.assertFalse(first_info.cache_hit)
        self.assertEqual(first_info.waited_seconds, 0)
        self.assertTrue(first_info.performed_load)
        self.assertTrue(first_info.stored)
        self.assertFalse(first_info.invalidated_during_load)
        first_rows[1][0] = "mutated"

        second_rows, second_info = cache.values_with_info(
            spreadsheet, "customers_enriched", loader=load
        )
        self.assertTrue(second_info.cache_hit)
        self.assertEqual(second_info.waited_seconds, 0)
        self.assertFalse(second_info.performed_load)
        self.assertFalse(second_info.stored)
        self.assertFalse(second_info.invalidated_during_load)
        self.assertEqual(second_rows[1][0], "value")
        self.assertEqual(calls, 1)

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

    def test_priority_snapshot_ignores_unrelated_dataset_invalidations(self):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        with patch.object(
            app_module,
            "build_current_priority_snapshot",
            wraps=app_module.build_current_priority_snapshot,
        ) as build:
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            for title in ("score_events", "planning_suggestions"):
                app_module.invalidate_sheet_cache(self.spreadsheet, title)
                app_module.get_authoritative_priority_snapshot(
                    self.spreadsheet, today=date(2026, 8, 8)
                )

        self.assertEqual(build.call_count, 1)

    def test_priority_snapshot_rebuilds_for_each_relevant_dependency(self):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        with patch.object(
            app_module,
            "build_current_priority_snapshot",
            wraps=app_module.build_current_priority_snapshot,
        ) as build:
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            for expected_count, title in enumerate(
                ("sales_activities", "order_rows"), start=2
            ):
                app_module.invalidate_sheet_cache(self.spreadsheet, title)
                app_module.get_authoritative_priority_snapshot(
                    self.spreadsheet, today=date(2026, 8, 8)
                )
                self.assertEqual(build.call_count, expected_count)

    def test_relevant_invalidation_during_snapshot_build_prevents_reuse(self):
        self._assert_snapshot_build_race("sales_activities", expected_builds=2)

    def test_unrelated_invalidation_during_snapshot_build_allows_reuse(self):
        self._assert_snapshot_build_race("score_events", expected_builds=1)

    def _assert_snapshot_build_race(self, invalidated_title, *, expected_builds):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        original = app_module.build_current_priority_snapshot
        build_started = threading.Event()
        release_build = threading.Event()
        build_count = 0
        first_result = []
        errors = []

        def blocked_build(*args, **kwargs):
            nonlocal build_count
            build_count += 1
            if build_count == 1:
                build_started.set()
                self.assertTrue(release_build.wait(2))
            return original(*args, **kwargs)

        def build_snapshot():
            try:
                first_result.append(
                    app_module.get_authoritative_priority_snapshot(
                        self.spreadsheet, today=date(2026, 8, 8)
                    )
                )
            except Exception as error:
                errors.append(error)

        with patch.object(
            app_module,
            "build_current_priority_snapshot",
            side_effect=blocked_build,
        ):
            thread = threading.Thread(target=build_snapshot)
            thread.start()
            self.assertTrue(build_started.wait(2))
            app_module.invalidate_sheet_cache(
                self.spreadsheet, invalidated_title
            )
            release_build.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertFalse(errors)
            self.assertEqual(len(first_result), 1)
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )

        self.assertEqual(build_count, expected_builds)

    def test_explicit_planning_rows_still_use_planned_dataset_generation(self):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        planned_rows = [{
            "planned_activity_id": "explicit-plan",
            "status": "planned",
            "scheduled_at": "2026-08-10T10:00:00+02:00",
        }]
        with patch.object(
            app_module,
            "build_current_priority_snapshot",
            wraps=app_module.build_current_priority_snapshot,
        ) as build:
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet,
                today=date(2026, 8, 8),
                planned_activity_rows=planned_rows,
            )
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet,
                today=date(2026, 8, 8),
                planned_activity_rows=planned_rows,
            )
            self.assertEqual(build.call_count, 1)
            app_module.invalidate_sheet_cache(
                self.spreadsheet, "planned_activities"
            )
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet,
                today=date(2026, 8, 8),
                planned_activity_rows=planned_rows,
            )

        self.assertEqual(build.call_count, 2)

    def test_broad_and_global_cache_invalidation_rebuild_priority_snapshot(self):
        app_module.ensure_email_worksheets(
            self.spreadsheet, include_events=False
        )
        with patch.object(
            app_module,
            "build_current_priority_snapshot",
            wraps=app_module.build_current_priority_snapshot,
        ) as build:
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            self.assertEqual(build.call_count, 1)
            app_module._sheet_read_cache.invalidate(self.spreadsheet)
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )
            self.assertEqual(build.call_count, 2)
            app_module._sheet_read_cache.clear()
            app_module.get_authoritative_priority_snapshot(
                self.spreadsheet, today=date(2026, 8, 8)
            )

        self.assertEqual(build.call_count, 3)

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
