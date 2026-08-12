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
import route_optimization as route_optimization_module
from route_optimization import (
    _diagnostic_duration_seconds,
    MAX_VISITS,
    PRIORITY_PENALTY_MULTIPLIER,
    QUADRATIC_SOFT_DURATION_BUFFER_SECONDS,
    QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR,
    ROUTE_ENGINE_VERSION,
    ROUTE_MAX_SECONDS,
    RouteOptimizationError,
    RouteOptimizationProvider,
    SERVICE_SECONDS,
    TrustedCoordinate,
    build_input_fingerprint,
    build_legacy_ro_v1_request_fingerprint,
    build_optimize_tours_request,
    build_request_fingerprint,
    coordinate_quality,
    load_service_account_credentials,
    parse_optimize_tours_response,
    priority_penalty,
    quadratic_soft_duration_diagnostics,
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
    def test_signed_diagnostic_duration_parser_is_exact_and_shape_safe(self):
        self.assertEqual(_diagnostic_duration_seconds("46s"), 46)
        self.assertEqual(_diagnostic_duration_seconds("-46s"), -46)
        self.assertEqual(_diagnostic_duration_seconds("0s"), 0)
        self.assertEqual(_diagnostic_duration_seconds("1.000000001s"), 1.000000001)
        self.assertEqual(_diagnostic_duration_seconds("-0.250s"), -0.25)
        for value in (None, "", "46", "1.1234567890s", {}, 46, True):
            with self.subTest(value=value):
                self.assertIsNone(_diagnostic_duration_seconds(value))

    def test_t1_penalty_proof_and_clamping(self):
        self.assertEqual(ROUTE_ENGINE_VERSION, "ro-v2")
        self.assertEqual(QUADRATIC_SOFT_DURATION_BUFFER_SECONDS, 300)
        self.assertEqual(
            QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR, 28800
        )
        self.assertGreater(PRIORITY_PENALTY_MULTIPLIER, ROUTE_MAX_SECONDS / 3600)
        self.assertEqual(priority_penalty(0), 10.0)
        self.assertEqual(priority_penalty(20), 200.0)
        self.assertEqual(priority_penalty(40), 400.0)
        self.assertEqual(priority_penalty(60.4), 600.0)
        self.assertEqual(priority_penalty(80), 800.0)
        self.assertEqual(priority_penalty(100), 1000.0)
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
        duration_limit = vehicle["routeDurationLimit"]
        self.assertEqual(duration_limit, {
            "maxDuration": "25199s",
            "quadraticSoftMaxDuration": "24899s",
            "costPerSquareHourAfterQuadraticSoftMax": 28800,
        })
        rendered = str(body)
        for forbidden in (
            "deliveries", "costPerKilometer", "costPerTraveledHour",
            "fixedCost", "softMaxDuration", "costPerHourAfterSoftMax",
            "travelDurationMultiple",
        ):
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

    def test_t5a_quadratic_soft_duration_is_dynamic_and_safely_optional(self):
        reduced = build_optimize_tours_request(
            run_id="pre-route-fixed",
            owner_user_name="Olle",
            route_start=NOW,
            start=START,
            shipments=[shipment(1)],
            pre_route_fixed_seconds=600,
        )
        reduced_limit = reduced["model"]["vehicles"][0]["routeDurationLimit"]
        self.assertEqual(reduced_limit["maxDuration"], "24599s")
        self.assertEqual(reduced_limit["quadraticSoftMaxDuration"], "24299s")
        self.assertEqual(
            reduced_limit["costPerSquareHourAfterQuadraticSoftMax"], 28800
        )

        small_capacity = build_optimize_tours_request(
            run_id="small-capacity",
            owner_user_name="Olle",
            route_start=NOW,
            start=START,
            shipments=[shipment(1)],
            pre_route_fixed_seconds=ROUTE_MAX_SECONDS - 300,
        )
        self.assertEqual(
            small_capacity["model"]["vehicles"][0]["routeDurationLimit"],
            {"maxDuration": "300s"},
        )

    def test_t5b_quadratic_soft_duration_diagnostics_use_stable_decimal_cost(self):
        at_hard_max = quadratic_soft_duration_diagnostics(
            model_route_max_seconds=25199,
            route_duration_seconds=25199,
        )
        self.assertEqual(at_hard_max, {
            "quadratic_soft_duration_enabled": True,
            "quadratic_soft_buffer_seconds": 300,
            "quadratic_soft_max_seconds": 24899,
            "quadratic_soft_cost_per_square_hour": 28800,
            "quadratic_soft_exceedance_seconds": 300,
            "quadratic_soft_duration_cost": 200,
        })
        sofia_completed = quadratic_soft_duration_diagnostics(
            model_route_max_seconds=25199,
            route_duration_seconds=25050,
        )
        self.assertEqual(
            sofia_completed["quadratic_soft_exceedance_seconds"], 151
        )
        self.assertEqual(sofia_completed["quadratic_soft_duration_cost"], 50.7)
        disabled = quadratic_soft_duration_diagnostics(
            model_route_max_seconds=300,
            route_duration_seconds=300,
        )
        self.assertFalse(disabled["quadratic_soft_duration_enabled"])
        self.assertIsNone(disabled["quadratic_soft_max_seconds"])
        self.assertIsNone(disabled["quadratic_soft_duration_cost"])

    def test_t6_fingerprint_ignores_names_but_tracks_business_inputs(self):
        base = [shipment(1, score=70)]
        fingerprint_kwargs = dict(
            owner_user_name="Olle", route_date="2026-08-10", route_start=NOW,
            route_mode="automatic", start=START, shipments=base, fixed_activities=[],
        )
        first = build_request_fingerprint(**fingerprint_kwargs)
        input_fingerprint = build_input_fingerprint(**fingerprint_kwargs)
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
        with patch.object(
            route_optimization_module, "ROUTE_ENGINE_VERSION", "ro-v1"
        ):
            ro_v1 = build_request_fingerprint(**fingerprint_kwargs)
            self.assertEqual(
                input_fingerprint,
                build_input_fingerprint(**fingerprint_kwargs),
            )
        self.assertNotEqual(first, ro_v1)
        self.assertNotEqual(
            first,
            build_legacy_ro_v1_request_fingerprint(**fingerprint_kwargs),
        )
        self.assertEqual(
            build_legacy_ro_v1_request_fingerprint(**fingerprint_kwargs),
            "25cddb37f7a5570658ce7c9908ded3ddc65e0ed1f6bd69fbcbe4d66bb027d00a",
        )
        with patch.object(
            route_optimization_module,
            "QUADRATIC_SOFT_DURATION_BUFFER_SECONDS",
            301,
        ):
            changed_policy = build_request_fingerprint(
                owner_user_name="Olle", route_date="2026-08-10",
                route_start=NOW, route_mode="automatic", start=START,
                shipments=base, fixed_activities=[],
            )
        self.assertNotEqual(first, changed_policy)
        with patch.object(
            route_optimization_module,
            "QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR",
            28799,
        ):
            changed_cost_policy = build_request_fingerprint(
                owner_user_name="Olle", route_date="2026-08-10",
                route_start=NOW, route_mode="automatic", start=START,
                shipments=base, fixed_activities=[],
            )
        self.assertNotEqual(first, changed_cost_policy)

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
        items = [
            shipment(1, required=True, fixed_at=NOW + timedelta(minutes=10)),
            shipment(2),
        ]
        response = successful_response(items, selected=(0,))
        response["routes"][0]["hasTrafficInfeasibilities"] = True
        response["routes"][0]["transitions"][0]["trafficInfoUnavailable"] = True
        response["skippedShipments"][0]["reasons"] = [
            {"code": "CANNOT_BE_PERFORMED_WITHIN_VEHICLE_DURATION_LIMIT"},
            {"code": "CANNOT_BE_PERFORMED_WITHIN_VEHICLE_TIME_WINDOWS"},
        ]
        fixed_breaks = [{
            "scheduled_at": NOW + timedelta(minutes=30),
            "duration_seconds": 600,
        }]
        with self.assertRaises(RouteOptimizationError) as exc:
            parse_optimize_tours_response(
                response,
                shipments=items,
                owner_user_name="Olle",
                route_start=NOW,
                pre_route_fixed_seconds=600,
                fixed_breaks=fixed_breaks,
                timeout_seconds=180,
            )
        error = exc.exception
        self.assertEqual(error.code, "route_traffic_infeasible")
        self.assertEqual(error.http_status, 422)
        self.assertEqual(
            error.public_message,
            (
                "Trafiken gör att rutten inte ryms inom dagens fasta tider "
                "och sjutimmarsgräns. Justera planeringen och försök igen."
            ),
        )
        self.assertEqual(error.details["diagnostic_reason"], "traffic_infeasibility")
        self.assertEqual(error.details["transition_count"], 2)
        self.assertEqual(error.details["expected_transition_count"], 2)
        self.assertTrue(error.details["transition_count_matches_expected"])
        self.assertEqual(error.details["traffic_info_unavailable_count"], 1)
        self.assertEqual(error.details["route_metrics"]["travelDuration"], "1200s")
        self.assertEqual(error.details["skip_reason_counts"], {
            "CANNOT_BE_PERFORMED_WITHIN_VEHICLE_DURATION_LIMIT": 1,
            "CANNOT_BE_PERFORMED_WITHIN_VEHICLE_TIME_WINDOWS": 1,
        })
        self.assertEqual(error.details["absolute_route_max_seconds"], 25199)
        self.assertEqual(error.details["model_route_max_seconds"], 24599)
        self.assertTrue(error.details["quadratic_soft_duration_enabled"])
        self.assertEqual(error.details["quadratic_soft_buffer_seconds"], 300)
        self.assertEqual(error.details["quadratic_soft_max_seconds"], 24299)
        self.assertEqual(
            error.details["quadratic_soft_cost_per_square_hour"], 28800
        )
        self.assertEqual(error.details["quadratic_soft_exceedance_seconds"], 0)
        self.assertEqual(error.details["quadratic_soft_duration_cost"], 0)
        self.assertEqual(error.details["required_count"], 1)
        self.assertTrue(error.details["has_required_visits"])
        self.assertEqual(error.details["fixed_visit_count"], 1)
        self.assertTrue(error.details["has_fixed_visits"])
        self.assertEqual(error.details["fixed_break_count"], 1)
        self.assertTrue(error.details["has_fixed_breaks"])
        self.assertEqual(error.details["pre_route_fixed_seconds"], 600)
        self.assertEqual(error.details["timeout_seconds"], 180)
        self.assertIs(error.details["traffic_deficit_calculated"], False)

    def test_t7c_transition_diagnostics_default_only_omitted_delay_and_break(self):
        items = [shipment(1)]

        def diagnostic_for(transition):
            response = successful_response(items)
            route = response["routes"][0]
            route["hasTrafficInfeasibilities"] = True
            route["transitions"][0] = transition
            with self.assertRaises(RouteOptimizationError) as exc:
                parse_optimize_tours_response(
                    response,
                    shipments=items,
                    owner_user_name="Olle",
                    route_start=NOW,
                )
            self.assertEqual(exc.exception.code, "route_traffic_infeasible")
            return exc.exception.details["transition_diagnostics"][0]

        omitted = diagnostic_for({
            "travelDuration": "891s",
            "totalDuration": "775s",
            "waitDuration": "-116s",
        })
        self.assertEqual(omitted["delay_duration_seconds"], 0)
        self.assertEqual(omitted["break_duration_seconds"], 0)
        self.assertEqual(omitted["transition_residual_seconds"], -116)

        explicit_zero = diagnostic_for({
            "travelDuration": "891s",
            "totalDuration": "775s",
            "waitDuration": "-116s",
            "delayDuration": "0s",
            "breakDuration": "0s",
        })
        self.assertEqual(explicit_zero["transition_residual_seconds"], -116)

        malformed_delay = diagnostic_for({
            "travelDuration": "891s",
            "totalDuration": "775s",
            "delayDuration": "invalid",
        })
        self.assertIsNone(malformed_delay["transition_residual_seconds"])

        malformed_break = diagnostic_for({
            "travelDuration": "891s",
            "totalDuration": "775s",
            "breakDuration": {"seconds": 0},
        })
        self.assertIsNone(malformed_break["transition_residual_seconds"])

        missing_travel = diagnostic_for({"totalDuration": "775s"})
        self.assertIsNone(missing_travel["transition_residual_seconds"])

        missing_total = diagnostic_for({"travelDuration": "891s"})
        self.assertIsNone(missing_total["transition_residual_seconds"])

        fractional = diagnostic_for({
            "travelDuration": "891.25s",
            "totalDuration": "775.125s",
            "delayDuration": "-0.125s",
            "breakDuration": "0s",
        })
        self.assertEqual(fractional["transition_residual_seconds"], -116)

    def test_t7d_structural_error_precedes_traffic_infeasibility(self):
        items = [shipment(1), shipment(2)]
        duplicate = successful_response(items, selected=(0, 0))
        incomplete = successful_response(items, selected=(0,))
        incomplete["routes"][0]["transitions"].pop()
        wrong_vehicle = successful_response(items, selected=(0,))
        wrong_vehicle["routes"][0]["vehicleIndex"] = 1
        malformed_start = successful_response(items, selected=(0,))
        malformed_start["routes"][0]["vehicleStartTime"] = 123
        malformed_breaks = successful_response(items, selected=(0,))
        malformed_breaks["routes"][0]["breaks"] = 123
        cases = (
            (duplicate, "shipment_identity_invalid"),
            (incomplete, "transition_count_invalid"),
            (wrong_vehicle, "vehicle_index_mismatch"),
            (malformed_start, "route_structure_invalid"),
            (malformed_breaks, "route_structure_invalid"),
        )
        for response, reason in cases:
            response["routes"][0]["hasTrafficInfeasibilities"] = True
            with self.subTest(reason=reason):
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
                    reason,
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

    def legacy_ro_v1_fingerprint(self, inputs):
        return build_legacy_ro_v1_request_fingerprint(
            owner_user_name="olle",
            route_date=NOW.date().isoformat(),
            route_start=inputs["route_start_at"],
            route_mode="automatic",
            start=inputs["start"],
            shipments=inputs["shipments"],
            fixed_activities=inputs["fixed_activities"],
        )

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
        rows = self.spreadsheet.worksheet(
            app_module.ROUTE_OPTIMIZATION_RUNS_SHEET
        ).dict_rows()
        self.assertEqual(len(rows), 1)
        persisted = json.loads(str(rows[0]["result_payload_json"]))
        self.assertRegex(rows[0]["input_fingerprint"], r"^[0-9a-f]{64}$")
        self.assertEqual(persisted["model_diagnostics"], {
            "route_engine_version": "ro-v2",
            "model_route_max_seconds": 25199,
            "quadratic_soft_duration_enabled": True,
            "quadratic_soft_buffer_seconds": 300,
            "quadratic_soft_max_seconds": 24899,
            "quadratic_soft_cost_per_square_hour": 28800,
            "quadratic_soft_exceedance_seconds": 0,
            "quadratic_soft_duration_cost": 0,
        })
        self.assertEqual(persisted["summary"]["stop_count"], 1)
        self.assertEqual(persisted["summary"]["total_priority_score"], 80)
        self.assertEqual(persisted["summary"]["route_seconds"], 3600)
        self.assertGreaterEqual(persisted["solve_duration_ms"], 0)

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

    def test_completed_ro_v1_request_replays_across_policy_deploy_only_for_same_input(self):
        snapshot = self.priority_snapshot()
        with patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value=snapshot,
        ):
            inputs, input_error = app_module.build_route_optimization_inputs(
                spreadsheet=self.spreadsheet,
                owner={"user_name": "olle", "name": "Olle"},
                route_date=NOW.date(),
                start=app_module.Coordinate(57.7, 11.9),
            )
        self.assertIsNone(input_error)
        stored_result = {
            "stops": [],
            "summary": {
                "stop_count": 0,
                "total_priority_score": 0,
                "route_seconds": 0,
                "route_minutes": 0,
                "route_end_at": inputs["route_start_at"].isoformat(
                    timespec="minutes"
                ),
            },
            "performed_count": 0,
            "skipped_count": len(inputs["shipments"]),
            "solve_duration_ms": 1234,
        }
        sheet = app_module.route_optimization_run_sheet(self.spreadsheet)
        app_module.append_dict_row(
            sheet,
            app_module.ROUTE_OPTIMIZATION_RUN_COLUMNS,
            {
                "run_id": "completed-before-ro-v2",
                "actor_user_name": "olle",
                "user_name": "olle",
                "usage_iso_week": app_module.route_optimization_usage_week(
                    NOW.date()
                ),
                "route_date": NOW.date().isoformat(),
                "client_request_id": "completed-before-ro-v2",
                "request_fingerprint": self.legacy_ro_v1_fingerprint(inputs),
                "engine_version": "ro-v1",
                "status": "completed",
                "counted_attempt": "Y",
                "started_at": NOW.isoformat(),
                "completed_at": NOW.isoformat(),
                "result_payload_json": json.dumps(stored_result),
            },
        )
        original_payload = {
            "route_date": NOW.date().isoformat(),
            "route_mode": "automatic",
            "client_request_id": "completed-before-ro-v2",
            "start": {"latitude": 57.7, "longitude": 11.9},
        }
        with patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value=snapshot,
        ), patch.object(app_module, "route_optimization_provider") as provider:
            status = self.client.get(
                "/planning/route-preview-status",
                query_string={
                    "client_request_id": "completed-before-ro-v2"
                },
            )
            recovered = self.client.post(
                "/planning/route-preview", json=original_payload
            )
            changed = self.client.post(
                "/planning/route-preview",
                json={
                    **original_payload,
                    "start": {"latitude": 57.8, "longitude": 11.9},
                },
            )

        self.assertEqual(status.get_json(), {"ok": True, "state": "completed"})
        self.assertEqual(recovered.status_code, 200, recovered.get_json())
        self.assertEqual(
            recovered.get_json()["route_optimization_run_id"],
            "completed-before-ro-v2",
        )
        self.assertEqual(changed.status_code, 409, changed.get_json())
        self.assertEqual(changed.get_json()["code"], "route_request_id_conflict")
        provider.assert_not_called()

    def test_recovery_status_is_read_only_actor_scoped_and_maps_run_states(self):
        provider_patch = patch.object(app_module, "route_optimization_provider")
        provider = provider_patch.start()
        self.addCleanup(provider_patch.stop)
        invalid = self.client.get("/planning/route-preview-status")
        self.assertEqual(invalid.status_code, 400)
        self.assertIn("krävs för statuskontrollen", invalid.get_json()["message"])
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
                "run_id": "traffic-failed", "actor_user_name": "olle",
                "client_request_id": "traffic-failed-id", "status": "failed",
                "error_code": "route_traffic_infeasible",
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
            "traffic-failed-id": {
                "ok": True, "state": "failed",
                "error_code": "route_traffic_infeasible",
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

    def test_t15b_http_200_traffic_failure_records_compact_diagnostics(self):
        customers = [{
            "row": index + 2,
            "customer": f"Butik {index}",
            "customer_id": f"20000000-0000-4000-8000-{index:012d}",
            "sales_person": "Olle",
            "cancelled_flag": "",
            "latitude_google": 56.0 + index / 100000,
            "longitude_google": 12.0 + index / 100000,
            "city_google": f"Ort {index}",
            "postal_code_google": f"{41000 + index}",
        } for index in range(12)]
        snapshot = self.priority_snapshot(customers)
        calls = []

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                calls.append(body)
                ids = [
                    item["label"].split(":", 1)[1]
                    for item in body["model"]["shipments"]
                ]
                route_start = app_module.route_start_datetime(NOW.date())
                response = successful_response(
                    [{"customer_id": value} for value in ids],
                    selected=tuple(range(12)),
                    start=route_start,
                )
                route = response["routes"][0]
                route["hasTrafficInfeasibilities"] = True
                route["vehicleEndTime"] = (
                    route_start + timedelta(seconds=25199)
                ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                route["metrics"] = {
                    "travelDuration": "10915s",
                    "visitDuration": "14400s",
                    "waitDuration": "-116s",
                    "delayDuration": "0s",
                    "breakDuration": "0s",
                    "totalDuration": "25199s",
                }
                travel_seconds = [835] * 8 + [836] * 4 + [891]
                route["transitions"] = [{
                    "startTime": (
                        route_start + timedelta(minutes=30 * index)
                    ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "travelDuration": f"{travel}s",
                    "totalDuration": f"{775 if index == 12 else travel}s",
                    "waitDuration": "-116s" if index == 12 else "0s",
                    "trafficInfoUnavailable": False,
                } for index, travel in enumerate(travel_seconds)]
                return response, 200

        with patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value=snapshot,
        ), patch.object(
            app_module,
            "route_optimization_provider",
            return_value=Provider(),
        ):
            response = self.client.post("/planning/route-preview", json={
                "route_date": NOW.date().isoformat(),
                "route_mode": "automatic",
                "client_request_id": "traffic-failure-200",
                "start": {"latitude": 57.7, "longitude": 11.9},
            })

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.get_json()["code"], "route_traffic_infeasible")
        self.assertIn("Trafiken gör att rutten inte ryms", response.get_json()["message"])
        self.assertEqual(len(calls), 1)
        rows = self.spreadsheet.worksheet(
            app_module.ROUTE_OPTIMIZATION_RUNS_SHEET
        ).dict_rows()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["status"], "failed")
        self.assertTrue(app_module.is_yes(row["counted_attempt"]))
        self.assertEqual(str(row["http_status"]), "200")
        self.assertEqual(row["error_code"], "route_traffic_infeasible")
        payload = json.loads(str(row["result_payload_json"] or "{}"))
        self.assertEqual(payload["diagnostic_reason"], "traffic_infeasibility")
        self.assertEqual(payload["route_count"], 1)
        self.assertEqual(payload["visit_count"], 12)
        self.assertEqual(payload["stop_count"], 12)
        self.assertEqual(payload["shipment_count"], 12)
        self.assertEqual(payload["transition_count"], 13)
        self.assertEqual(payload["expected_transition_count"], 13)
        self.assertEqual(payload["skipped_count"], 0)
        self.assertEqual(payload["break_count"], 0)
        self.assertTrue(payload["hasTrafficInfeasibilities"])
        self.assertTrue(payload["vehicle_label_matches"])
        self.assertEqual(payload["traffic_info_unavailable_count"], 0)
        self.assertEqual(payload["route_metrics"]["travelDuration"], "10915s")
        self.assertEqual(payload["route_total_duration_seconds"], 25199)
        self.assertEqual(payload["route_travel_duration_seconds"], 10915)
        self.assertEqual(payload["route_visit_duration_seconds"], 14400)
        self.assertEqual(payload["route_wait_duration_seconds"], -116)
        self.assertEqual(payload["route_delay_duration_seconds"], 0)
        self.assertEqual(payload["route_break_duration_seconds"], 0)
        self.assertEqual(payload["aggregate_timeline_residual_seconds"], -116)
        self.assertEqual(payload["absolute_route_max_seconds"], 25199)
        self.assertEqual(payload["model_route_max_seconds"], 25199)
        self.assertTrue(payload["quadratic_soft_duration_enabled"])
        self.assertEqual(payload["quadratic_soft_buffer_seconds"], 300)
        self.assertEqual(payload["quadratic_soft_max_seconds"], 24899)
        self.assertEqual(
            payload["quadratic_soft_cost_per_square_hour"], 28800
        )
        self.assertEqual(payload["quadratic_soft_exceedance_seconds"], 300)
        self.assertEqual(payload["quadratic_soft_duration_cost"], 200)
        self.assertEqual(payload["route_elapsed_seconds"], 25199)
        self.assertTrue(payload["route_elapsed_matches_total_duration"])
        self.assertEqual(payload["negative_wait_transition_count"], 1)
        self.assertEqual(payload["negative_residual_transition_count"], 1)
        self.assertEqual(payload["most_negative_transition_index"], 12)
        self.assertEqual(
            payload["most_negative_transition_residual_seconds"], -116
        )
        self.assertEqual(payload["transition_residual_min_seconds"], -116)
        self.assertEqual(payload["transition_residual_max_seconds"], 0)
        self.assertEqual(len(payload["transition_diagnostics"]), 13)
        self.assertEqual(
            payload["transition_diagnostics"][12]["wait_duration_seconds"],
            -116,
        )
        self.assertEqual(
            payload["transition_diagnostics"][12]["transition_residual_seconds"],
            -116,
        )
        self.assertEqual(payload["required_count"], 0)
        self.assertFalse(payload["has_required_visits"])
        self.assertEqual(payload["fixed_visit_count"], 0)
        self.assertFalse(payload["has_fixed_visits"])
        self.assertEqual(payload["fixed_break_count"], 0)
        self.assertFalse(payload["has_fixed_breaks"])
        self.assertEqual(payload["pre_route_fixed_seconds"], 0)
        self.assertTrue(payload["consider_road_traffic"])
        self.assertEqual(payload["search_mode"], "CONSUME_ALL_AVAILABLE_TIME")
        self.assertEqual(payload["solving_mode"], "DEFAULT_SOLVE")
        self.assertEqual(payload["timeout_seconds"], 90)
        self.assertEqual(payload["max_visits"], 15)
        self.assertEqual(payload["service_duration_seconds"], 1200)
        self.assertIs(payload["traffic_deficit_calculated"], False)
        self.assertGreaterEqual(payload["solve_duration_ms"], 0)

        request = calls[0]
        vehicle = request["model"]["vehicles"][0]
        self.assertEqual(vehicle["routeDurationLimit"], {
            "maxDuration": "25199s",
            "quadraticSoftMaxDuration": "24899s",
            "costPerSquareHourAfterQuadraticSoftMax": 28800,
        })
        self.assertEqual(request["searchMode"], "CONSUME_ALL_AVAILABLE_TIME")
        self.assertEqual(request["solvingMode"], "DEFAULT_SOLVE")
        self.assertTrue(request["considerRoadTraffic"])
        self.assertNotIn("softMaxDuration", json.dumps(request))
        self.assertNotIn("costPerHourAfterSoftMax", json.dumps(request))
        self.assertNotIn("travelDurationMultiple", json.dumps(request))

    def test_t15c_malformed_http_200_response_cannot_break_diagnostics(self):
        snapshot = self.priority_snapshot()
        calls = []

        class Provider:
            def optimize(_self, *, project, body, timeout_seconds):
                calls.append(body)
                if len(calls) == 1:
                    return {"routes": 123}, 200
                ids = [
                    item["label"].split(":", 1)[1]
                    for item in body["model"]["shipments"]
                ]
                malformed = successful_response(
                    [{"customer_id": value} for value in ids],
                    selected=(0,),
                    start=app_module.route_start_datetime(NOW.date()),
                )
                route = malformed["routes"][0]
                if len(calls) == 2:
                    route["visits"] = 123
                elif len(calls) == 3:
                    route["transitions"] = 123
                else:
                    route["hasTrafficInfeasibilities"] = True
                    route["metrics"]["travelDuration"] = {"malformed": True}
                    route["transitions"][0]["travelDuration"] = "not-a-duration"
                    route["transitions"][0]["totalDuration"] = "also-invalid"
                return malformed, 200

        with patch.dict(os.environ, {
            "ROUTE_OPTIMIZATION_WEEKLY_OWNER_LIMIT": "10",
            "ROUTE_OPTIMIZATION_WEEKLY_TEAM_LIMIT": "10",
        }, clear=False), patch.object(
            app_module,
            "get_authoritative_priority_snapshot",
            return_value=snapshot,
        ), patch.object(
            app_module,
            "route_optimization_provider",
            return_value=Provider(),
        ):
            responses = [
                self.client.post("/planning/route-preview", json={
                    "route_date": NOW.date().isoformat(),
                    "route_mode": "automatic",
                    "client_request_id": request_id,
                    "start": {"latitude": 57.7, "longitude": 11.9},
                })
                for request_id in (
                    "malformed-routes-200",
                    "malformed-visits-200",
                    "malformed-transitions-200",
                    "malformed-traffic-metrics-200",
                )
            ]

        self.assertEqual(len(calls), 4)
        for response in responses[:3]:
            self.assertEqual(response.status_code, 502)
            self.assertEqual(response.get_json()["code"], "route_response_invalid")
        self.assertEqual(responses[3].status_code, 422)
        self.assertEqual(
            responses[3].get_json()["code"], "route_traffic_infeasible"
        )
        rows = self.spreadsheet.worksheet(
            app_module.ROUTE_OPTIMIZATION_RUNS_SHEET
        ).dict_rows()
        self.assertEqual(len(rows), 4)
        for row in rows[:3]:
            self.assertEqual(row["status"], "failed")
            self.assertEqual(str(row["http_status"]), "200")
            self.assertEqual(row["error_code"], "route_response_invalid")
            diagnostic = json.loads(str(row["result_payload_json"] or "{}"))
            self.assertEqual(diagnostic["error_code"], "route_response_invalid")
            self.assertEqual(
                diagnostic["diagnostic_reason"],
                "route_structure_invalid",
            )
        diagnostics = [
            json.loads(str(row["result_payload_json"] or "{}"))
            for row in rows
        ]
        self.assertEqual(diagnostics[0]["route_count"], 0)
        self.assertEqual(diagnostics[1]["route_count"], 1)
        self.assertEqual(diagnostics[1]["visit_count"], 0)
        self.assertEqual(diagnostics[2]["route_count"], 1)
        self.assertEqual(diagnostics[2]["visit_count"], 1)
        traffic_row = rows[3]
        self.assertEqual(traffic_row["status"], "failed")
        self.assertEqual(str(traffic_row["http_status"]), "200")
        self.assertEqual(traffic_row["error_code"], "route_traffic_infeasible")
        self.assertEqual(diagnostics[3]["error_code"], "route_traffic_infeasible")
        self.assertIsNone(diagnostics[3]["route_travel_duration_seconds"])
        self.assertIsNone(
            diagnostics[3]["transition_diagnostics"][0][
                "travel_duration_seconds"
            ]
        )

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
            "route_engine_version": ROUTE_ENGINE_VERSION,
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
            "route_payload": {
                "engine": "route_optimization",
                "engine_version": ROUTE_ENGINE_VERSION,
            },
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
