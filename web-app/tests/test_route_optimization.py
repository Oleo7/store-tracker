from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys
from unittest import TestCase, main
from unittest.mock import patch
from zoneinfo import ZoneInfo


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
from route_optimization import (
    MAX_VISITS,
    PRIORITY_PENALTY_MULTIPLIER,
    ROUTE_MAX_SECONDS,
    RouteOptimizationError,
    RouteOptimizationProvider,
    SERVICE_SECONDS,
    TrustedCoordinate,
    build_optimize_tours_request,
    build_request_fingerprint,
    coordinate_quality,
    load_service_account_credentials,
    parse_optimize_tours_response,
    priority_penalty,
)
import scripts.route_optimization_smoke as smoke_module
from tests.test_planning import FakeWorksheet, default_spreadsheet


STOCKHOLM = ZoneInfo("Europe/Stockholm")
NOW = datetime(2026, 8, 10, 8, 0, tzinfo=STOCKHOLM)
START = TrustedCoordinate(57.70887, 11.97456)


def shipment(index=1, *, required=False, score=50, fixed_at=None):
    return {
        "customer_id": f"00000000-0000-4000-8000-{index:012d}",
        "coordinate": TrustedCoordinate(57.0 + index / 100000, 11.0 + index / 100000),
        "priority_score": score,
        "required": required,
        "fixed_at": fixed_at,
        "activity_id": f"activity-{index}" if required else "",
        "revision": 2 if required else 0,
    }


def successful_response(shipments, *, selected=(0,), start=NOW, duration_minutes=60, breaks=()):
    visits = []
    for offset, index in enumerate(selected, start=1):
        visits.append({
            "shipmentIndex": index,
            "shipmentLabel": f"customer:{shipments[index]['customer_id']}",
            "isPickup": True,
            "startTime": (start + timedelta(minutes=offset * 10)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        })
    selected_set = set(selected)
    return {
        "routes": [{
            "vehicleLabel": "owner:olle",
            "vehicleStartTime": start.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "vehicleEndTime": (start + timedelta(minutes=duration_minutes)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "visits": visits,
            "transitions": [{} for _ in range(len(visits) + 1)],
            "breaks": list(breaks),
            "metrics": {
                "travelDuration": "1200s",
                "waitDuration": "0s",
                "breakDuration": "0s",
                "visitDuration": f"{len(visits) * SERVICE_SECONDS}s",
            },
        }],
        "skippedShipments": [
            {"index": index}
            for index in range(len(shipments)) if index not in selected_set
        ],
    }


class FakeCredentials:
    def __init__(self):
        self.token = "old-token"
        self.expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        self.refresh_count = 0

    def refresh(self, _request):
        self.refresh_count += 1
        self.token = f"token-{self.refresh_count}"
        self.expiry = datetime.now(timezone.utc) + timedelta(hours=1)


class FakeResponse:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.responses.pop(0)


class RouteOptimizationModelTests(TestCase):
    def test_t1_penalty_proof_and_clamping(self):
        self.assertGreater(PRIORITY_PENALTY_MULTIPLIER, ROUTE_MAX_SECONDS / 3600)
        self.assertEqual(priority_penalty(0), 10.0)
        self.assertEqual(priority_penalty(60.4), 600.0)
        self.assertEqual(priority_penalty(999), 1000.0)

    def test_t2_exact_request_schema_optional_and_mandatory(self):
        fixed = NOW + timedelta(hours=2)
        items = [shipment(1, score=74), shipment(2, required=True, fixed_at=fixed)]
        body = build_optimize_tours_request(
            run_id="run-1", owner_user_name="Olle", route_start=NOW,
            start=START, shipments=items, timeout_seconds=90,
        )
        model = body["model"]
        self.assertEqual(body["searchMode"], "CONSUME_ALL_AVAILABLE_TIME")
        self.assertIs(body["considerRoadTraffic"], True)
        self.assertEqual(body["label"], "store-tracker:run-1")
        self.assertNotIn("considerRoadTraffic", model)
        self.assertNotIn("label", model)
        self.assertEqual(model["shipments"][0]["penaltyCost"], 740.0)
        self.assertNotIn("penaltyCost", model["shipments"][1])
        self.assertEqual(model["shipments"][0]["loadDemands"]["visit_slots"]["amount"], "1")
        vehicle = model["vehicles"][0]
        self.assertEqual(vehicle["travelMode"], "DRIVING")
        self.assertEqual(vehicle["loadLimits"]["visit_slots"]["maxLoad"], "15")
        self.assertEqual(vehicle["loadLimits"]["visit_slots"]["startLoadInterval"], {"min": "0", "max": "0"})
        self.assertEqual(vehicle["startLocation"], vehicle["endLocation"])
        rendered = str(body)
        for forbidden in ("deliveries", "costPerKilometer", "costPerTraveledHour", "fixedCost"):
            self.assertNotIn(forbidden, rendered)
        for private_value in ("Butik A", "customer@example.com", "+46700000000", "ring kunden"):
            self.assertNotIn(private_value, rendered)

        at_start = build_optimize_tours_request(
            run_id="route-start-window", owner_user_name="Olle", route_start=NOW,
            start=START, shipments=[shipment(3, required=True, fixed_at=NOW)],
        )
        start_window = at_start["model"]["shipments"][0]["pickups"][0]["timeWindows"][0]
        self.assertEqual(start_window["startTime"], at_start["model"]["globalStartTime"])
        self.assertLess(start_window["startTime"], start_window["endTime"])

    def test_t3_full_universe_577_has_no_shortlist(self):
        items = [shipment(index) for index in range(1, 578)]
        body = build_optimize_tours_request(
            run_id="run-577", owner_user_name="Olle", route_start=NOW,
            start=START, shipments=items,
        )
        self.assertEqual(len(body["model"]["shipments"]), 577)
        validate_body = smoke_module.synthetic_request(solving_mode="VALIDATE_ONLY")
        solve_body = smoke_module.synthetic_request(solving_mode="DEFAULT_SOLVE")
        self.assertEqual(validate_body["solvingMode"], "VALIDATE_ONLY")
        self.assertEqual(solve_body["solvingMode"], "DEFAULT_SOLVE")
        self.assertEqual(len(validate_body["model"]["shipments"]), 20)
        self.assertEqual(
            validate_body["model"]["shipments"],
            solve_body["model"]["shipments"],
        )
        with patch.dict(os.environ, {
            "ROUTE_OPTIMIZATION_PROJECT": "synthetic-project",
            "ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS": "synthetic-credentials",
        }, clear=False), patch.object(
            smoke_module, "load_service_account_credentials", return_value=object()
        ), patch.object(
            smoke_module, "RouteOptimizationProvider"
        ) as provider_class, patch.object(
            sys, "argv", ["route_optimization_smoke.py", "--validate-only"]
        ), patch("builtins.print"):
            provider_class.return_value.optimize.return_value = ({"routes": []}, 200)
            self.assertEqual(smoke_module.main(), 0)
        validate_call = provider_class.return_value.optimize.call_args.kwargs
        self.assertEqual(validate_call["body"]["solvingMode"], "VALIDATE_ONLY")
        self.assertEqual(len(validate_call["body"]["model"]["shipments"]), 20)

        events = []
        paid_items = smoke_module.synthetic_shipments()

        def reject_saved_response(*args, **kwargs):
            events.append("parse")
            raise RouteOptimizationError("route_response_invalid", "invalid", 502)

        with patch.dict(os.environ, {
            "ROUTE_OPTIMIZATION_PROJECT": "synthetic-project",
            "ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS": "synthetic-credentials",
        }, clear=False), patch.object(
            smoke_module, "load_service_account_credentials", return_value=object()
        ), patch.object(
            smoke_module, "RouteOptimizationProvider"
        ) as paid_provider, patch.object(
            smoke_module, "persist_paid_response",
            side_effect=lambda response: events.append("persist") or Path("synthetic-response.json"),
        ), patch.object(
            smoke_module, "parse_optimize_tours_response",
            side_effect=reject_saved_response,
        ), patch.object(
            sys, "argv", ["route_optimization_smoke.py", "--paid-synthetic-solve"]
        ), patch("builtins.print") as print_mock:
            paid_provider.return_value.optimize.return_value = (
                successful_response(
                    paid_items,
                    selected=tuple(range(15)),
                    start=smoke_module.SYNTHETIC_ROUTE_START,
                ),
                200,
            )
            self.assertEqual(smoke_module.main(), 2)
        self.assertEqual(events, ["persist", "parse"])
        report = json.loads(print_mock.call_args.args[0])
        self.assertEqual(report["raw_response_path"], "synthetic-response.json")
        self.assertFalse(report["parser_accepted"])

    def test_t4_coordinate_quality_bounds_and_generic_fallback_detection(self):
        customers = [{
            "customer_id": f"c-{index}",
            "latitude_google": "57.7",
            "longitude_google": "11.9",
            "city_google": f"City {index % 3}",
            "postal_code_google": f"40{index:03d}",
        } for index in range(5)]
        customers.append({"customer_id": "bad", "latitude_google": "91", "longitude_google": "11"})
        quality = coordinate_quality(customers)
        self.assertTrue(all(not quality[f"c-{index}"]["trusted"] for index in range(5)))
        self.assertEqual(quality["c-0"]["reason"], "suspicious_shared")
        self.assertEqual(quality["bad"]["reason"], "invalid")

    def test_t5_fixed_break_and_vehicle_end_window(self):
        break_start = NOW + timedelta(hours=1)
        body = build_optimize_tours_request(
            run_id="fixed", owner_user_name="Olle", route_start=NOW,
            start=START, shipments=[shipment(1)],
            fixed_breaks=[{"scheduled_at": break_start, "duration_seconds": 600}],
            timeout_seconds=180,
        )
        vehicle = body["model"]["vehicles"][0]
        request = vehicle["breakRule"]["breakRequests"][0]
        self.assertEqual(request["earliestStartTime"], request["latestStartTime"])
        self.assertEqual(request["minDuration"], "600s")
        self.assertGreaterEqual(vehicle["endTimeWindows"][0]["startTime"], request["earliestStartTime"])

    def test_t6_fingerprint_ignores_names_but_tracks_business_inputs(self):
        base = [shipment(1, score=70)]
        first = build_request_fingerprint(
            owner_user_name="Olle", route_date="2026-08-10", route_start=NOW,
            route_mode="automatic", start=START, shipments=base, fixed_activities=[],
        )
        copied = [{**base[0], "customer": "A name never fingerprinted"}]
        self.assertEqual(first, build_request_fingerprint(
            owner_user_name="Olle", route_date="2026-08-10", route_start=NOW,
            route_mode="automatic", start=START, shipments=copied, fixed_activities=[],
        ))
        changed = [{**base[0], "priority_score": 71}]
        self.assertNotEqual(first, build_request_fingerprint(
            owner_user_name="Olle", route_date="2026-08-10", route_start=NOW,
            route_mode="automatic", start=START, shipments=changed, fixed_activities=[],
        ))

    def test_t7_response_parser_accepts_known_pickups_and_totals(self):
        items = [shipment(1), shipment(2)]
        parsed = parse_optimize_tours_response(
            successful_response(items, selected=(1,)), shipments=items,
            owner_user_name="Olle", route_start=NOW,
        )
        self.assertEqual([stop["customer_id"] for stop in parsed["stops"]], [items[1]["customer_id"]])
        self.assertEqual(parsed["performed_count"], 1)
        self.assertEqual(parsed["skipped_count"], 1)

    def test_t7_protojson_shipment_identity_fallback_and_validation(self):
        items = [shipment(1), shipment(2)]

        skipped_zero = successful_response(items, selected=(1,))
        skipped_zero["skippedShipments"] = [{
            "label": f"customer:{items[0]['customer_id']}"
        }]
        parsed = parse_optimize_tours_response(
            skipped_zero,
            shipments=items,
            owner_user_name="Olle",
            route_start=NOW,
        )
        self.assertEqual(parsed["skipped_count"], 1)

        performed_zero = successful_response(items, selected=(0,))
        del performed_zero["routes"][0]["visits"][0]["shipmentIndex"]
        parsed = parse_optimize_tours_response(
            performed_zero,
            shipments=items,
            owner_user_name="Olle",
            route_start=NOW,
        )
        self.assertEqual(parsed["stops"][0]["customer_id"], items[0]["customer_id"])

        mismatched = successful_response(items, selected=(0,))
        mismatched["routes"][0]["visits"][0]["shipmentLabel"] = (
            f"customer:{items[1]['customer_id']}"
        )
        unknown = successful_response(items, selected=(1,))
        unknown["skippedShipments"] = [{"label": "customer:unknown"}]
        for response in (mismatched, unknown):
            with self.subTest(response=response):
                with self.assertRaises(RouteOptimizationError):
                    parse_optimize_tours_response(
                        response,
                        shipments=items,
                        owner_user_name="Olle",
                        route_start=NOW,
                    )

    def test_t7a_response_parser_reports_shipment_identity_invalid(self):
        items = [shipment(1), shipment(2)]
        response = successful_response(items, selected=(0,))
        response["routes"][0]["visits"][0]["shipmentLabel"] = f"customer:{items[1]['customer_id']}"
        with self.assertRaises(RouteOptimizationError) as exc:
            parse_optimize_tours_response(
                response,
                shipments=items,
                owner_user_name="Olle",
                route_start=NOW,
            )
        self.assertEqual(exc.exception.code, "route_response_invalid")
        self.assertEqual(
            exc.exception.details.get("diagnostic_reason"),
            "shipment_identity_invalid",
        )

    def test_t7b_response_parser_reports_vehicle_label_mismatch(self):
        items = [shipment(1)]
        response = successful_response(items, selected=(0,))
        response["routes"][0]["vehicleLabel"] = "owner:someone_else"
        with self.assertRaises(RouteOptimizationError) as exc:
            parse_optimize_tours_response(
                response,
                shipments=items,
                owner_user_name="Olle",
                route_start=NOW,
            )
        self.assertEqual(exc.exception.code, "route_response_invalid")
        self.assertEqual(
            exc.exception.details.get("diagnostic_reason"),
            "vehicle_label_mismatch",
        )

    def test_t7c_response_parser_reports_traffic_infeasibility(self):
        items = [shipment(1)]
        response = successful_response(items, selected=(0,))
        response["routes"][0]["hasTrafficInfeasibilities"] = True
        with self.assertRaises(RouteOptimizationError) as exc:
            parse_optimize_tours_response(
                response,
                shipments=items,
                owner_user_name="Olle",
                route_start=NOW,
            )
        self.assertEqual(exc.exception.code, "route_response_invalid")
        self.assertEqual(
            exc.exception.details.get("diagnostic_reason"),
            "traffic_infeasibility",
        )

    def test_t8_response_parser_rejects_missing_mandatory_duplicate_and_seven_hours(self):
        mandatory = [shipment(1, required=True)]
        cases = [
            successful_response(mandatory, selected=()),
            successful_response([shipment(1), shipment(2)], selected=(0, 0)),
            successful_response([shipment(1)], selected=(0,), duration_minutes=420),
            successful_response(
                [shipment(index) for index in range(1, 17)],
                selected=tuple(range(16)),
            ),
            {"validationErrors": [{"code": "INVALID"}], "routes": []},
        ]
        arguments = [
            mandatory,
            [shipment(1), shipment(2)],
            [shipment(1)],
            [shipment(index) for index in range(1, 17)],
            [shipment(1)],
        ]
        for response, items in zip(cases, arguments):
            with self.subTest(response=response):
                with self.assertRaises(RouteOptimizationError):
                    parse_optimize_tours_response(
                        response, shipments=items, owner_user_name="Olle", route_start=NOW,
                    )

    def test_t9_response_parser_validates_exact_break(self):
        fixed = [{"scheduled_at": NOW + timedelta(hours=1), "duration_seconds": 600}]
        returned = [{
            "startTime": fixed[0]["scheduled_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "duration": "600s",
        }]
        items = [shipment(1)]
        parsed = parse_optimize_tours_response(
            successful_response(items, breaks=returned), shipments=items,
            owner_user_name="Olle", route_start=NOW, fixed_breaks=fixed,
        )
        self.assertEqual(parsed["performed_count"], 1)

    def test_t10_provider_refreshes_and_retries_exactly_once_on_401(self):
        credentials = FakeCredentials()
        session = FakeSession([FakeResponse(401), FakeResponse(200, {"routes": []})])
        provider = RouteOptimizationProvider(credentials=credentials, session=session)
        payload, status = provider.optimize(project="p", body={}, timeout_seconds=90)
        self.assertEqual(status, 200)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(credentials.refresh_count, 1)
        self.assertNotIn("key=", session.calls[0][0][0])
        self.assertEqual(session.calls[0][1]["timeout"], (10, 120))

    def test_t11_provider_never_blind_retries_429_or_5xx(self):
        for status in (408, 429, 500):
            with self.subTest(status=status):
                session = FakeSession([FakeResponse(status)])
                provider = RouteOptimizationProvider(credentials=FakeCredentials(), session=session)
                with self.assertRaises(RouteOptimizationError) as raised:
                    provider.optimize(project="p", body={}, timeout_seconds=90)
                self.assertEqual(len(session.calls), 1)
                self.assertEqual(raised.exception.counted_attempt, status == 408 or status >= 500)


class RouteOptimizationIntegrationTests(TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="route-optimization-test")
        self.spreadsheet = default_spreadsheet()
        self.client = app_module.app.test_client()
        user = self.spreadsheet.worksheet(app_module.USERS_SHEET).dict_rows()[0]
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(user)
        self.environment = patch.dict(os.environ, {
            "ROUTE_ENGINE": "route_optimization",
            "ROUTE_OPTIMIZATION_PROJECT": "test-project",
            "ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS": "test-placeholder",
            "ROUTE_OPTIMIZATION_WEEKLY_OWNER_LIMIT": "2",
            "ROUTE_OPTIMIZATION_WEEKLY_TEAM_LIMIT": "6",
        }, clear=False)
        self.environment.start()
        self.spreadsheet_patch = patch.object(app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet)
        self.spreadsheet_patch.start()
        self.now_patch = patch.object(app_module, "stockholm_now", return_value=NOW)
        self.today_patch = patch.object(app_module, "stockholm_today", return_value=NOW.date())
        self.now_patch.start()
        self.today_patch.start()

    def tearDown(self):
        self.today_patch.stop()
        self.now_patch.stop()
        self.spreadsheet_patch.stop()
        self.environment.stop()
        app_module._route_optimization_provider = None
        app_module._route_optimization_provider_config = None

    def priority_snapshot(self, customers=None):
        customers = customers or app_module.get_customer_rows(self.spreadsheet)
        return {
            "customers": customers,
            "priorities": [{
                "customer_id": customer["customer_id"],
                "priority_score": 80 - (index % 20),
            } for index, customer in enumerate(customers) if not app_module.customer_is_cancelled(customer)],
            "contact_rows": [],
        }

    def test_t12_input_builder_uses_all_577_owner_customers(self):
        customers = [{
            "row": index + 2,
            "customer": f"Butik {index}",
            "customer_id": f"10000000-0000-4000-8000-{index:012d}",
            "sales_person": "Olle",
            "cancelled_flag": "",
            "latitude_google": 56.0 + index / 100000,
            "longitude_google": 12.0 + index / 100000,
            "city_google": f"Ort {index}",
            "postal_code_google": f"{40000 + index}",
        } for index in range(577)]
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(
                self.spreadsheet.worksheet(app_module.USERS_SHEET).dict_rows()[0]
            )
        with self.client.application.test_request_context():
            with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=self.priority_snapshot(customers)):
                inputs, error = app_module.build_route_optimization_inputs(
                    spreadsheet=self.spreadsheet,
                    owner={"user_name": "olle", "name": "Olle"},
                    route_date=NOW.date(),
                    start=app_module.Coordinate(57.7, 11.9),
                )
        self.assertIsNone(error)
        self.assertEqual(len(inputs["shipments"]), 577)

    def test_t13_required_visits_and_fixed_breaks_survive_preprocessing(self):
        planned = self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        rows = [
            {
                "planned_activity_id": "mandatory",
                "user_name": "olle", "sales_person": "Olle",
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "customer_row": 2, "customer": "Butik A", "contact_type": "visit",
                "scheduled_at": "2026-08-10T10:00:00+02:00", "duration_minutes": 20,
                "time_is_estimated": "", "status": "planned", "source": "system_suggestion", "revision": 3,
            },
            {
                "planned_activity_id": "break",
                "user_name": "olle", "sales_person": "Olle",
                "customer_id": "33333333-3333-4333-8333-333333333333",
                "customer_row": 4, "customer": "Butik C", "contact_type": "phone",
                "scheduled_at": "2026-08-10T11:00:00+02:00", "duration_minutes": 10,
                "status": "planned", "source": "manual", "revision": 2,
            },
        ]
        planned.values.extend([[row.get(header, "") for header in app_module.PLANNED_ACTIVITY_COLUMNS] for row in rows])
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=self.priority_snapshot()):
            inputs, error = app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet, owner={"user_name": "olle", "name": "Olle"},
                route_date=NOW.date(), start=app_module.Coordinate(57.7, 11.9),
            )
        self.assertIsNone(error)
        required = [item for item in inputs["shipments"] if item["required"]]
        self.assertEqual([item["activity_id"] for item in required], ["mandatory"])
        self.assertEqual(inputs["fixed_breaks"][0]["activity_id"], "break")
        bad_quality = {
            customer_id: {"trusted": False, "reason": "suspicious_shared"}
            for customer_id in ("11111111-1111-4111-8111-111111111111",)
        }
        with app_module.app.app_context(), patch.object(
            app_module, "get_authoritative_priority_snapshot", return_value=self.priority_snapshot()
        ), patch.object(app_module, "route_coordinate_quality", return_value=bad_quality):
            _bad_inputs, bad_error = app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet, owner={"user_name": "olle", "name": "Olle"},
                route_date=NOW.date(), start=app_module.Coordinate(57.7, 11.9),
            )
        self.assertEqual(bad_error[1], 422)
        self.assertEqual(bad_error[0].get_json()["code"], "route_required_coordinate_untrusted")

    def test_t14_preview_is_ledger_only_and_candidate_rows_do_not_restrict(self):
        snapshot = self.priority_snapshot()
        captured = {}

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                captured["body"] = body
                items = [shipment(1), shipment(2)]
                # Labels/indices must match the real request; select its first shipment.
                customer_ids = [item["label"].split(":", 1)[1] for item in body["model"]["shipments"]]
                real = [{"customer_id": value} for value in customer_ids]
                response = successful_response(real, selected=(0,), start=app_module.route_start_datetime(NOW.date()))
                return response, 200

        with patch.dict(os.environ, {"PERFORMANCE_LOGGING_ENABLED": "true"}, clear=False), patch.object(
            app_module, "get_authoritative_priority_snapshot", return_value=snapshot
        ), patch.object(
            app_module, "route_optimization_provider", return_value=Provider()
        ), patch.object(app_module.app.logger, "info") as performance_logs:
            response = self.client.post("/planning/route-preview", json={
                    "route_date": NOW.date().isoformat(),
                    "route_mode": "automatic",
                    "candidate_rows": [2],
                    "client_request_id": "preview-universe-1",
                    "start": {"latitude": 57.7, "longitude": 11.9},
                })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(captured["body"]["model"]["shipments"]), 2)
        self.assertEqual(self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows(), [])
        self.assertNotIn(app_module.ROUTE_PROPOSALS_SHEET, self.spreadsheet.added_sheets)
        ledger = self.spreadsheet.worksheet(app_module.ROUTE_OPTIMIZATION_RUNS_SHEET).dict_rows()
        self.assertEqual(len(ledger), 1)
        self.assertEqual(ledger[0]["status"], "completed")
        performance_steps = {
            json.loads(call.args[0]).get("step")
            for call in performance_logs.call_args_list
            if call.args and str(call.args[0]).startswith("{")
        }
        self.assertTrue({
            "route_optimization.input_build",
            "route_optimization.quota_reservation",
            "route_optimization.google_solve",
            "route_optimization.response_validation",
        }.issubset(performance_steps))

    def test_t15_completed_fingerprint_reuses_result_without_call_or_quota(self):
        snapshot = self.priority_snapshot()
        calls = []

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                calls.append(body)
                ids = [item["label"].split(":", 1)[1] for item in body["model"]["shipments"]]
                return successful_response([{"customer_id": value} for value in ids], selected=(0,), start=app_module.route_start_datetime(NOW.date())), 200

        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(app_module, "route_optimization_provider", return_value=Provider()):
            for request_id in ("cache-a", "cache-b"):
                response = self.client.post("/planning/route-preview", json={
                    "route_date": NOW.date().isoformat(), "route_mode": "automatic",
                    "client_request_id": request_id,
                    "start": {"latitude": 57.7, "longitude": 11.9},
                })
                self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(self.spreadsheet.worksheet(app_module.ROUTE_OPTIMIZATION_RUNS_SHEET).dict_rows()), 1)

    def test_completed_request_replay_reuses_exact_run_before_quota(self):
        snapshot = self.priority_snapshot()
        calls = []

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                calls.append(body)
                ids = [
                    item["label"].split(":", 1)[1]
                    for item in body["model"]["shipments"]
                ]
                return successful_response(
                    [{"customer_id": value} for value in ids],
                    selected=(0,),
                    start=app_module.route_start_datetime(NOW.date()),
                ), 200

        payload = {
            "route_date": NOW.date().isoformat(),
            "route_mode": "automatic",
            "client_request_id": "exact-recovery-replay",
            "start": {"latitude": 57.7, "longitude": 11.9, "accuracy": 12},
        }
        with patch.dict(os.environ, {
            "ROUTE_OPTIMIZATION_WEEKLY_OWNER_LIMIT": "1",
            "ROUTE_OPTIMIZATION_WEEKLY_TEAM_LIMIT": "1",
        }, clear=False), patch.object(
            app_module, "get_authoritative_priority_snapshot", return_value=snapshot
        ), patch.object(
            app_module, "route_optimization_provider", return_value=Provider()
        ):
            first = self.client.post("/planning/route-preview", json=payload)
            replay = self.client.post("/planning/route-preview", json=payload)

        self.assertEqual(first.status_code, 200, first.get_json())
        self.assertEqual(replay.status_code, 200, replay.get_json())
        self.assertEqual(len(calls), 1)
        rows = self.spreadsheet.worksheet(
            app_module.ROUTE_OPTIMIZATION_RUNS_SHEET
        ).dict_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["client_request_id"], payload["client_request_id"])
        self.assertEqual(sum(app_module.is_yes(row["counted_attempt"]) for row in rows), 1)

    def test_recovery_status_is_read_only_actor_scoped_and_maps_run_states(self):
        provider_patch = patch.object(app_module, "route_optimization_provider")
        provider = provider_patch.start()
        self.addCleanup(provider_patch.stop)
        missing = self.client.get(
            "/planning/route-preview-status?client_request_id=missing"
        )
        self.assertEqual(missing.get_json(), {"ok": True, "state": "not_found"})
        self.assertNotIn(
            app_module.ROUTE_OPTIMIZATION_RUNS_SHEET,
            self.spreadsheet.added_sheets,
        )

        sheet = app_module.route_optimization_run_sheet(self.spreadsheet)
        rows = [
            {
                "run_id": "running", "actor_user_name": "olle",
                "client_request_id": "running-id", "status": "running",
                "started_at": NOW.isoformat(), "timeout_seconds": 90,
            },
            {
                "run_id": "completed", "actor_user_name": "olle",
                "client_request_id": "completed-id", "status": "completed",
            },
            {
                "run_id": "failed", "actor_user_name": "olle",
                "client_request_id": "failed-id", "status": "failed",
                "error_code": "route_provider_timeout",
            },
            {
                "run_id": "stale", "actor_user_name": "olle",
                "client_request_id": "stale-id", "status": "running",
                "started_at": (NOW - timedelta(seconds=200)).isoformat(),
                "timeout_seconds": 90,
            },
            {
                "run_id": "unknown", "actor_user_name": "olle",
                "client_request_id": "unknown-id", "status": "indeterminate",
                "error_code": "route_unknown_outcome",
            },
            {
                "run_id": "private", "actor_user_name": "sofia",
                "client_request_id": "private-id", "status": "completed",
            },
        ]
        for row in rows:
            app_module.append_dict_row(
                sheet, app_module.ROUTE_OPTIMIZATION_RUN_COLUMNS, row
            )
        before = sheet.dict_rows()
        cases = {
            "running-id": {"ok": True, "state": "running"},
            "completed-id": {"ok": True, "state": "completed"},
            "failed-id": {
                "ok": True, "state": "failed",
                "error_code": "route_provider_timeout",
            },
            "stale-id": {
                "ok": True, "state": "indeterminate",
                "error_code": "route_optimization_stale_running",
            },
            "unknown-id": {
                "ok": True, "state": "indeterminate",
                "error_code": "route_unknown_outcome",
            },
            "private-id": {"ok": True, "state": "not_found"},
        }
        for request_id, expected in cases.items():
            with self.subTest(request_id=request_id):
                response = self.client.get(
                    "/planning/route-preview-status",
                    query_string={"client_request_id": request_id},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json(), expected)
        self.assertEqual(sheet.dict_rows(), before)
        provider.assert_not_called()

    def test_t15a_http_200_parse_failure_records_diagnostic_json(self):
        snapshot = self.priority_snapshot()

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                ids = [item["label"].split(":", 1)[1] for item in body["model"]["shipments"]]
                response, status = successful_response(
                    [{"customer_id": value} for value in ids], selected=(0,), start=app_module.route_start_datetime(NOW.date())
                ), 200
                response["routes"][0]["vehicleLabel"] = "owner:someone_else"
                return response, status

        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(app_module, "route_optimization_provider", return_value=Provider()):            response = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(),
                "route_mode": "automatic",
                "client_request_id": "parse-failure-200",
                "start": {"latitude": 57.7, "longitude": 11.9},
            })
        self.assertEqual(response.status_code, 502)
        rows = self.spreadsheet.worksheet(app_module.ROUTE_OPTIMIZATION_RUNS_SHEET).dict_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["http_status"]), "200")
        payload = json.loads(str(row["result_payload_json"] or "{}"))
        self.assertEqual(payload["diagnostic_reason"], "vehicle_label_mismatch")
        self.assertEqual(payload["error_code"], "route_response_invalid")
        self.assertEqual(payload["route_count"], 1)
        self.assertEqual(payload["visit_count"], 1)
        self.assertGreaterEqual(payload["skipped_count"], 0)
        self.assertGreaterEqual(payload["solve_duration_ms"], 0)

    def test_t16_client_request_conflict_and_running_fingerprint_are_409(self):
        snapshot = self.priority_snapshot()
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot):
            inputs, _error = app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet, owner={"user_name": "olle", "name": "Olle"},
                route_date=NOW.date(), start=app_module.Coordinate(57.7, 11.9),
            )
        sheet = app_module.route_optimization_run_sheet(self.spreadsheet)
        app_module.append_dict_row(sheet, app_module.ROUTE_OPTIMIZATION_RUN_COLUMNS, {
            "run_id": "running", "actor_user_name": "olle", "user_name": "olle",
            "usage_iso_week": app_module.route_optimization_usage_week(NOW.date()),
            "route_date": NOW.date().isoformat(), "client_request_id": "same-id",
            "request_fingerprint": inputs["fingerprint"], "engine_version": "ro-v1",
            "status": "running", "counted_attempt": "Y", "started_at": NOW.isoformat(),
        })
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot):
            same = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(), "client_request_id": "other-id",
                "start": {"latitude": 57.7, "longitude": 11.9},
            })
            changed = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(), "client_request_id": "same-id",
                "start": {"latitude": 57.8, "longitude": 11.9},
            })
        self.assertEqual(same.status_code, 409)
        self.assertEqual(same.get_json()["code"], "route_optimization_in_progress")
        self.assertEqual(changed.status_code, 409)
        self.assertEqual(changed.get_json()["code"], "route_request_id_conflict")
        app_module.append_dict_row(sheet, app_module.ROUTE_OPTIMIZATION_RUN_COLUMNS, {
            "run_id": "counted-two", "actor_user_name": "olle", "user_name": "olle",
            "usage_iso_week": app_module.route_optimization_usage_week(NOW.date()),
            "route_date": NOW.date().isoformat(), "client_request_id": "old-attempt",
            "request_fingerprint": "another-fingerprint", "engine_version": "ro-v1",
            "status": "failed", "counted_attempt": "Y", "started_at": NOW.isoformat(),
        })
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot):
            quota = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(), "client_request_id": "quota-new-id",
                "start": {"latitude": 57.8, "longitude": 11.9},
            })
        self.assertEqual(quota.status_code, 429)
        self.assertEqual(quota.get_json()["code"], "route_optimization_quota_exceeded")
        self.assertIn("reset_at", quota.get_json())

    def test_t17_apply_revalidates_fingerprint_and_never_calls_google(self):
        snapshot = self.priority_snapshot()
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot):
            inputs, input_error = app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet, owner={"user_name": "olle", "name": "Olle"},
                route_date=NOW.date(), start=app_module.Coordinate(57.7, 11.9),
            )
        self.assertIsNone(input_error)
        valid_preview = {
            "route_date": NOW.date().isoformat(), "user_name": "olle",
            "route_engine_version": "ro-v1",
            "route_optimization_fingerprint": inputs["fingerprint"],
            "route_start_at": inputs["route_start_at"].isoformat(timespec="minutes"),
            "plan_fingerprint": app_module.planning_state_fingerprint(inputs["date_rows"]),
            "start": {"latitude": 57.7, "longitude": 11.9},
            "stops": [{
                "customer_id": "11111111-1111-4111-8111-111111111111",
                "customer_row": 2, "row": 2, "sequence": 1,
                "scheduled_at": "2026-08-10T09:00:00+02:00",
                "estimated_at": "2026-08-10T09:00:00+02:00",
                "duration_minutes": 20, "required": False,
            }],
            "summary": {"route_minutes": 60, "route_end_at": "2026-08-10T10:00:00+02:00"},
            "timeline": {"route_end_at": "2026-08-10T10:00:00+02:00"},
            "route_payload": {"engine": "route_optimization", "engine_version": "ro-v1"},
        }
        valid_token = app_module.planning_preview_serializer().dumps(valid_preview)
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(app_module, "route_optimization_provider") as provider:
            applied = self.client.post("/planning/route-apply", json={
                "preview_token": valid_token, "client_request_id": "apply-valid-1", "user_name": "olle",
            })
            duplicate = self.client.post("/planning/route-apply", json={
                "preview_token": valid_token, "client_request_id": "apply-valid-1", "user_name": "olle",
            })
        self.assertEqual(applied.status_code, 200, applied.get_json())
        self.assertEqual(duplicate.status_code, 200, duplicate.get_json())
        self.assertTrue(duplicate.get_json()["duplicate"])
        provider.assert_not_called()
        self.assertEqual(len(self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows()), 1)
        self.assertEqual(len(self.spreadsheet.worksheet(app_module.ROUTE_PROPOSALS_SHEET).dict_rows()), 1)

        preview = {
            "route_date": NOW.date().isoformat(), "user_name": "olle",
            "route_engine_version": "ro-v1", "route_optimization_fingerprint": "stale",
            "start": {"latitude": 57.7, "longitude": 11.9},
            "stops": [{"customer_id": "11111111-1111-4111-8111-111111111111"}],
        }
        token = app_module.planning_preview_serializer().dumps(preview)
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot), patch.object(app_module, "route_optimization_provider") as stale_provider:
            response = self.client.post("/planning/route-apply", json={
                "preview_token": token, "client_request_id": "apply-stale-1", "user_name": "olle",
            })
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "planning_changed")
        stale_provider.assert_not_called()
        self.assertEqual(len(self.spreadsheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET).dict_rows()), 1)

    def test_t18_feature_flag_keeps_legacy_path_default(self):
        with patch.dict(os.environ, {"ROUTE_ENGINE": "legacy"}, clear=False), patch.object(
            app_module, "build_planning_route_preview", return_value=({"ok": True}, None)
        ) as legacy, patch.object(app_module, "build_route_optimization_preview") as optimized:
            response = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(),
                "candidate_rows": [2],
                "start": {"latitude": 57.7, "longitude": 11.9},
            })
        self.assertEqual(response.status_code, 200)
        legacy.assert_called_once()
        optimized.assert_not_called()
        self.assertTrue(app_module.route_optimization_configuration_health({"ROUTE_ENGINE": "legacy"})["safe"])
        self.assertFalse(app_module.route_optimization_configuration_health({"ROUTE_ENGINE": "unknown"})["safe"])
        self.assertFalse(app_module.route_optimization_configuration_health({"ROUTE_ENGINE": "route_optimization"})["safe"])
        self.assertFalse(app_module.route_optimization_configuration_health({
            "ROUTE_ENGINE": "route_optimization",
            "ROUTE_OPTIMIZATION_PROJECT": "project",
            "GOOGLE_CREDENTIALS": "sheets-only",
        })["safe"])
        with self.assertRaises(RouteOptimizationError):
            load_service_account_credentials({"GOOGLE_CREDENTIALS": "{}"})
        frontend = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn('client_request_id: planningRoutePreviewRequestId', frontend)
        self.assertIn('route_mode: "automatic"', frontend)
        self.assertIn("Optimerar rutten – det kan ta upp till 3 minuter.", frontend)


if __name__ == "__main__":
    main()
