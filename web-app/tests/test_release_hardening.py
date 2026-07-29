from __future__ import annotations

import os
from pathlib import Path
import sys
from unittest import TestCase, main
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module


class PlanningLockReleaseTests(TestCase):
    def test_single_worker_is_safe_in_development(self):
        state = app_module.planning_lock_health({
            "APP_ENV": "development",
            "WEB_CONCURRENCY": "1",
        })

        self.assertTrue(state["safe"])
        self.assertEqual(state["mode"], "process_local")
        self.assertEqual(state["worker_count"], 1)
        self.assertEqual(state["reason"], "")

    def test_multiple_workers_are_unsafe(self):
        state = app_module.planning_lock_health({
            "APP_ENV": "development",
            "WEB_CONCURRENCY": "2",
        })

        self.assertFalse(state["safe"])
        self.assertEqual(state["worker_count"], 2)
        self.assertEqual(
            state["reason"],
            "multiple_workers_without_distributed_lock",
        )

    def test_missing_worker_count_is_deterministic(self):
        development = app_module.planning_lock_health({
            "APP_ENV": "development",
        })
        production = app_module.planning_lock_health({
            "APP_ENV": "production",
            "APP_INSTANCE_COUNT": "1",
        })

        self.assertTrue(development["safe"])
        self.assertEqual(development["worker_count"], 1)
        self.assertFalse(production["safe"])
        self.assertIsNone(production["worker_count"])
        self.assertEqual(production["reason"], "worker_count_unknown")

    def test_distributed_lock_url_never_simulates_a_lock(self):
        state = app_module.planning_lock_health({
            "APP_ENV": "development",
            "WEB_CONCURRENCY": "1",
            "PLANNING_DISTRIBUTED_LOCK_URL": "redis://example",
        })

        self.assertFalse(state["safe"])
        self.assertEqual(state["mode"], "process_local")
        self.assertEqual(
            state["reason"],
            "distributed_lock_not_implemented",
        )

    def test_pilot_startup_requires_one_worker_and_instance(self):
        valid = {
            "APP_ENV": "production",
            "WEB_CONCURRENCY": "1",
            "APP_INSTANCE_COUNT": "1",
        }
        state = app_module.validate_pilot_startup(valid)
        self.assertTrue(state["safe"])

        for invalid in (
            {**valid, "WEB_CONCURRENCY": "2"},
            {**valid, "APP_INSTANCE_COUNT": "2"},
            {
                "APP_ENV": "production",
                "APP_INSTANCE_COUNT": "1",
            },
            {
                "APP_ENV": "production",
                "WEB_CONCURRENCY": "1",
            },
        ):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                app_module.validate_pilot_startup(invalid)

    def test_gunicorn_worker_setting_must_agree(self):
        conflict = app_module.planning_lock_health({
            "APP_ENV": "production",
            "WEB_CONCURRENCY": "1",
            "GUNICORN_CMD_ARGS": "--workers 2 --threads 4",
            "APP_INSTANCE_COUNT": "1",
        })

        self.assertFalse(conflict["safe"])
        self.assertEqual(
            conflict["reason"],
            "conflicting_worker_configuration",
        )

    def test_process_start_command_cannot_override_single_worker_claim(self):
        with patch.object(
            sys,
            "argv",
            ["gunicorn", "--workers", "2", "app:app"],
        ):
            conflict = app_module.planning_lock_health({
                "APP_ENV": "production",
                "WEB_CONCURRENCY": "1",
                "APP_INSTANCE_COUNT": "1",
            })

        self.assertFalse(conflict["safe"])
        self.assertEqual(conflict["worker_count"], 2)
        self.assertEqual(
            conflict["reason"],
            "conflicting_worker_configuration",
        )

    def test_health_is_public_and_fail_closed(self):
        client = app_module.app.test_client()
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "development",
                "WEB_CONCURRENCY": "1",
            },
            clear=True,
        ):
            safe = client.get("/health")
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "development",
                "WEB_CONCURRENCY": "2",
            },
            clear=True,
        ):
            unsafe = client.get("/health")

        self.assertEqual(safe.status_code, 200, safe.get_json())
        self.assertTrue(safe.get_json()["ok"])
        self.assertTrue(
            safe.get_json()["planning_write_lock"]["safe"]
        )
        self.assertEqual(unsafe.status_code, 503, unsafe.get_json())
        self.assertFalse(unsafe.get_json()["ok"])
        self.assertFalse(
            unsafe.get_json()["planning_write_lock"]["safe"]
        )


class SheetEnvironmentReleaseTests(TestCase):
    def test_production_never_falls_back_to_legacy_sheet_key(self):
        with self.assertRaises(RuntimeError):
            app_module.resolve_sheet_id({
                "APP_ENV": "production",
                "SHEET_KEY": "legacy-production",
            })

        self.assertEqual(
            app_module.resolve_sheet_id({
                "APP_ENV": "production",
                "SHEET_KEY": "legacy-production",
                "PRODUCTION_SHEET_KEY": "explicit-production",
            }),
            "explicit-production",
        )

    def test_staging_never_falls_back_to_production(self):
        with self.assertRaises(RuntimeError):
            app_module.resolve_sheet_id({
                "APP_ENV": "staging",
                "SHEET_KEY": "production",
            })
        with self.assertRaises(RuntimeError):
            app_module.resolve_sheet_id({
                "APP_ENV": "staging",
                "SHEET_KEY": "same",
                "STAGING_SHEET_KEY": "same",
            })

        self.assertEqual(
            app_module.resolve_sheet_id({
                "APP_ENV": "staging",
                "SHEET_KEY": "production",
                "STAGING_SHEET_KEY": "staging",
            }),
            "staging",
        )

    def test_pilot_routes_never_falls_back_to_browser_key(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "GOOGLE_MAPS_API_KEY": "browser-only",
            },
            clear=True,
        ), self.assertRaises(app_module.TravelTimeConfigurationError):
            app_module.get_route_travel_time_provider()


if __name__ == "__main__":
    main()
