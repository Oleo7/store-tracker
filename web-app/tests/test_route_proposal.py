from __future__ import annotations

from itertools import permutations
from pathlib import Path
import random
import sys
from unittest import TestCase, main
from unittest.mock import patch

import requests


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module
import route_proposal as route_proposal_module
from route_proposal import (
    Coordinate,
    GoogleRoutesTravelTimeProvider,
    RequiredStopsNotFeasible,
    RouteCandidate,
    RouteVerificationError,
    TravelTimeResult,
    TravelTimeTimeout,
    TravelTimeUnavailable,
    _duration_to_ceiling_seconds,
    calculate_route_proposal,
    shortlist_candidates,
    solve_route,
    verify_route,
)


START = Coordinate(57.7089, 11.9746)


def candidate(
    row,
    score,
    latitude=None,
    longitude=None,
    name=None,
    required=False,
):
    return RouteCandidate(
        row=row,
        customer=name or f"Store {row}",
        coordinate=Coordinate(
            latitude if latitude is not None else 57.7 + row / 1000,
            longitude if longitude is not None else 11.9 + row / 1000,
        ),
        priority_score=score,
        required=required,
    )


def complete_matrix(start_legs, between):
    count = len(start_legs)
    rows = [list(start_legs)]
    for origin in range(count):
        rows.append([
            0 if origin == destination else between[origin][destination]
            for destination in range(count)
        ])
    return rows


class SolverTests(TestCase):
    def test_provider_duration_is_rounded_up_to_integer_seconds(self):
        self.assertEqual(_duration_to_ceiling_seconds("3s"), 3)
        self.assertEqual(_duration_to_ceiling_seconds("3.0001s"), 4)
        self.assertIsNone(_duration_to_ceiling_seconds("-1s"))
        self.assertIsNone(_duration_to_ceiling_seconds("bad"))

    def test_exact_solver_includes_return_leg(self):
        candidates = [candidate(2, 50)]
        matrix = [[60], [999999]]
        solution = solve_route(candidates, matrix, return_seconds=[120])
        route = verify_route(
            candidates=candidates,
            drive_seconds=matrix,
            return_seconds=[120],
            route_indices=solution.route_indices,
        )

        self.assertEqual(solution.route_indices, (0,))
        self.assertTrue(solution.optimality_proven)
        self.assertEqual(route.drive_seconds, 180)
        self.assertEqual(route.return_drive_seconds, 120)
        self.assertEqual(route.total_seconds, 1380)

    def test_exact_solver_rejects_exactly_seven_hours(self):
        candidates = [candidate(2, 75)]
        matrix = [[24000], [0]]
        solution = solve_route(candidates, matrix)
        self.assertEqual(solution.route_indices, ())

    def test_exact_solver_accepts_one_second_under_seven_hours(self):
        candidates = [candidate(2, 75)]
        matrix = [[23999], [0]]
        solution = solve_route(candidates, matrix)
        route = verify_route(
            candidates=candidates,
            drive_seconds=matrix,
            route_indices=solution.route_indices,
        )

        self.assertEqual(route.total_seconds, 25199)

    def test_exact_solver_rejects_route_one_second_over_budget(self):
        candidates = [candidate(2, 75)]
        matrix = [[24001], [0]]
        solution = solve_route(candidates, matrix)
        self.assertEqual(solution.route_indices, ())

    def test_score_wins_before_drive_time(self):
        candidates = [candidate(2, 40), candidate(3, 70)]
        matrix = complete_matrix(
            [60, 600],
            [
                [0, 40000],
                [40000, 0],
            ],
        )
        solution = solve_route(
            candidates,
            matrix,
            max_total_seconds=2000,
            service_seconds_per_stop=1200,
        )
        self.assertEqual(solution.route_indices, (1,))

    def test_required_zero_score_stop_wins_over_optional_high_score(self):
        candidates = [
            candidate(2, 0, required=True),
            candidate(3, 100),
        ]
        matrix = complete_matrix(
            [60, 60],
            [
                [0, 40000],
                [40000, 0],
            ],
        )

        solution = solve_route(
            candidates,
            matrix,
            max_total_seconds=2000,
            service_seconds_per_stop=1200,
        )
        route = verify_route(
            candidates=candidates,
            drive_seconds=matrix,
            route_indices=solution.route_indices,
            max_total_seconds=2000,
            service_seconds_per_stop=1200,
        )

        self.assertEqual(solution.route_indices, (0,))
        self.assertEqual(route.total_priority_score, 0)
        self.assertTrue(route.stops[0].candidate.required)

    def test_required_stop_over_strict_budget_has_stable_422_error(self):
        candidates = [candidate(2, 0, required=True)]
        with self.assertRaises(RequiredStopsNotFeasible) as raised:
            solve_route(
                candidates,
                [[800], [0]],
                max_total_seconds=2000,
                service_seconds_per_stop=1200,
            )

        self.assertEqual(raised.exception.code, "required_stops_not_feasible")
        self.assertEqual(raised.exception.http_status, 422)

    def test_more_than_maximum_required_stops_has_stable_422_error(self):
        candidates = [
            candidate(index + 2, 0, required=True)
            for index in range(16)
        ]
        matrix = [[0] * len(candidates) for _ in range(len(candidates) + 1)]

        with self.assertRaises(RequiredStopsNotFeasible) as raised:
            solve_route(candidates, matrix)

        self.assertEqual(raised.exception.code, "required_stops_not_feasible")
        self.assertEqual(raised.exception.http_status, 422)

    def test_lower_drive_time_breaks_equal_score_tie(self):
        candidates = [candidate(2, 50), candidate(3, 50)]
        matrix = complete_matrix(
            [300, 60],
            [
                [0, 40000],
                [40000, 0],
            ],
        )
        solution = solve_route(
            candidates,
            matrix,
            max_total_seconds=1800,
            service_seconds_per_stop=1200,
        )
        self.assertEqual(solution.route_indices, (1,))

    def test_full_tie_is_stable_by_row_sequence(self):
        candidates = [candidate(9, 50), candidate(4, 50)]
        matrix = complete_matrix(
            [60, 60],
            [
                [0, 60],
                [60, 0],
            ],
        )
        ordered = sorted(candidates, key=lambda item: item.row)
        solution = solve_route(ordered, matrix)
        rows = tuple(ordered[index].row for index in solution.route_indices)
        self.assertEqual(rows, (4, 9))

    def test_equal_drive_state_keeps_full_lexicographically_smallest_path(self):
        candidates = [candidate(2, 10), candidate(3, 10), candidate(4, 10)]
        matrix = [[0, 0, 0] for _ in range(4)]
        solution = solve_route(candidates, matrix)
        rows = tuple(candidates[index].row for index in solution.route_indices)
        self.assertEqual(rows, (2, 3, 4))

    def test_exact_solver_matches_independent_brute_force(self):
        rng = random.Random(20260725)
        for fixture_index in range(12):
            candidates = [
                candidate(index + 2, rng.randint(5, 100))
                for index in range(6)
            ]
            start_legs = [rng.randint(60, 900) for _ in candidates]
            between = [
                [
                    0 if left == right else rng.randint(60, 1200)
                    for right in range(6)
                ]
                for left in range(6)
            ]
            matrix = complete_matrix(start_legs, between)
            max_seconds = 4200
            service_seconds = 600

            solution = solve_route(
                candidates,
                matrix,
                max_total_seconds=max_seconds,
                service_seconds_per_stop=service_seconds,
            )
            expected = brute_force_route(
                candidates, matrix, max_seconds, service_seconds
            )
            with self.subTest(fixture=fixture_index):
                self.assertEqual(solution.route_indices, expected)

    def test_beam_solver_is_feasible_and_repeatable(self):
        candidates = [candidate(index + 2, 100 - index) for index in range(16)]
        matrix = complete_matrix(
            [60] * 16,
            [
                [0 if left == right else 60 for right in range(16)]
                for left in range(16)
            ],
        )
        kwargs = {
            "max_total_seconds": 28800,
            "service_seconds_per_stop": 1200,
            "exact_solver_limit": 15,
            "beam_width": 150,
            "beam_time_limit_seconds": 1,
        }
        first = solve_route(candidates, matrix, **kwargs)
        second = solve_route(candidates, matrix, **kwargs)
        verified = verify_route(
            candidates=candidates,
            drive_seconds=matrix,
            route_indices=first.route_indices,
        )

        self.assertEqual(first.route_indices, second.route_indices)
        self.assertFalse(first.optimality_proven)
        self.assertLessEqual(verified.total_seconds, 28800)
        self.assertEqual(len(first.route_indices), 15)

    def test_beam_solver_keeps_required_stop(self):
        candidates = [
            *[
                candidate(index + 2, 100 - index)
                for index in range(15)
            ],
            candidate(100, 0, required=True),
        ]
        matrix = complete_matrix(
            [60] * len(candidates),
            [
                [
                    0 if left == right else 60
                    for right in range(len(candidates))
                ]
                for left in range(len(candidates))
            ],
        )

        solution = solve_route(
            candidates,
            matrix,
            max_total_seconds=28800,
            service_seconds_per_stop=1200,
            exact_solver_limit=15,
            beam_width=150,
            beam_time_limit_seconds=1,
        )

        self.assertIn(15, solution.route_indices)
        self.assertEqual(len(solution.route_indices), 15)

    def test_post_verifier_rejects_an_overlong_solver_result(self):
        candidates = [candidate(2, 100)]
        with self.assertRaises(RouteVerificationError):
            verify_route(
                candidates=candidates,
                drive_seconds=[[24000], [0]],
                route_indices=[0],
            )

    def test_post_verifier_rejects_boolean_index_and_road_time(self):
        candidates = [candidate(2, 100)]
        with self.assertRaises(RouteVerificationError):
            verify_route(
                candidates=candidates,
                drive_seconds=[[60], [0]],
                route_indices=[True],
            )
        with self.assertRaises(RouteVerificationError):
            verify_route(
                candidates=candidates,
                drive_seconds=[[True], [0]],
                route_indices=[0],
            )

    def test_post_verifier_rejects_route_missing_required_stop(self):
        candidates = [
            candidate(2, 0, required=True),
            candidate(3, 100),
        ]
        matrix = complete_matrix(
            [60, 60],
            [
                [0, 60],
                [60, 0],
            ],
        )

        with self.assertRaises(RequiredStopsNotFeasible) as raised:
            verify_route(
                candidates=candidates,
                drive_seconds=matrix,
                route_indices=[1],
            )

        self.assertEqual(raised.exception.code, "required_stops_not_feasible")

    def test_shortlist_is_bounded_and_deterministic(self):
        candidates = [candidate(index + 2, 100 - index) for index in range(40)]
        direct = {item.row: 60 + item.row for item in candidates}
        first = shortlist_candidates(
            start=START,
            candidates=candidates,
            direct_seconds=direct,
        )
        second = shortlist_candidates(
            start=START,
            candidates=list(reversed(candidates)),
            direct_seconds=direct,
        )

        self.assertEqual(len(first), 24)
        self.assertEqual(
            [item.row for item in first],
            [item.row for item in second],
        )
        self.assertIn(2, {item.row for item in first})

    def test_shortlist_never_drops_required_stops(self):
        candidates = [
            candidate(
                index + 2,
                0 if index >= 14 else 100 - index,
                required=index >= 14,
            )
            for index in range(20)
        ]
        required_rows = {
            item.row for item in candidates if item.required
        }
        direct = {item.row: 60 for item in candidates}

        shortlisted = shortlist_candidates(
            start=START,
            candidates=candidates,
            direct_seconds=direct,
            limit=5,
        )

        self.assertEqual(len(shortlisted), len(required_rows))
        self.assertEqual({item.row for item in shortlisted}, required_rows)

    def test_large_shortlist_has_bounded_geographic_work(self):
        candidates = [
            candidate(
                index + 2,
                1 + (index * 37) % 100,
                latitude=55.1 + (index % 80) * 0.18,
                longitude=10.1 + (index // 80) * 0.18,
            )
            for index in range(2400)
        ]
        direct = {
            item.row: 60 + (index % 400) * 15
            for index, item in enumerate(candidates)
        }
        original_haversine = route_proposal_module._haversine_km
        haversine_calls = 0

        def counted_haversine(left, right):
            nonlocal haversine_calls
            haversine_calls += 1
            return original_haversine(left, right)

        with patch.object(
            route_proposal_module,
            "_haversine_km",
            new=counted_haversine,
        ):
            first = shortlist_candidates(
                start=START,
                candidates=candidates,
                direct_seconds=direct,
            )

        second = shortlist_candidates(
            start=START,
            candidates=list(reversed(candidates)),
            direct_seconds=direct,
        )

        self.assertEqual(len(first), 24)
        self.assertEqual(
            [item.row for item in first],
            [item.row for item in second],
        )
        # The approximate route evaluates at most 4 * shortlist_limit prefixes.
        # This catches either former O(n^2) geographic pass without relying on
        # machine-specific wall-clock timing.
        self.assertLessEqual(haversine_calls, len(candidates) * 24 * 4)

    def test_two_stage_calculation_caps_matrix_and_adds_return_elements(self):
        candidates = [candidate(index + 2, 100 - index) for index in range(30)]
        provider = FormulaProvider()
        proposal = calculate_route_proposal(
            start=START,
            candidates=candidates,
            provider=provider,
            exact_solver_limit=1,
            beam_width=100,
            beam_time_limit_seconds=0.2,
        )

        self.assertEqual(provider.call_shapes[0], (1, 30))
        self.assertEqual(provider.call_shapes[1], (25, 24))
        self.assertEqual(provider.call_shapes[2], (24, 1))
        self.assertLessEqual(provider.call_shapes[1][0] * provider.call_shapes[1][1], 600)
        self.assertTrue(proposal.shortlisted)
        self.assertLess(proposal.route.total_seconds, 25200)
        self.assertLessEqual(len(proposal.route.stops), 15)

    def test_shortlisting_prevents_global_optimality_claim(self):
        candidates = [candidate(index + 2, 100 - index) for index in range(20)]
        proposal = calculate_route_proposal(
            start=START,
            candidates=candidates,
            provider=FormulaProvider(),
            shortlist_limit=10,
            exact_solver_limit=15,
        )
        self.assertTrue(proposal.shortlisted)
        self.assertFalse(proposal.solution.optimality_proven)
        self.assertTrue(proposal.solution.solver_status.startswith("shortlisted_"))

    def test_calculation_respects_lower_non_required_time_budget(self):
        proposal = calculate_route_proposal(
            start=START,
            candidates=[candidate(2, 100), candidate(3, 90)],
            provider=FormulaProvider(),
            max_total_seconds=1380,
            service_seconds_per_stop=600,
        )

        self.assertEqual(len(proposal.route.stops), 1)
        self.assertLess(proposal.route.total_seconds, 1380)

    def test_required_stop_without_direct_road_has_stable_422_error(self):
        with self.assertRaises(RequiredStopsNotFeasible) as raised:
            calculate_route_proposal(
                start=START,
                candidates=[candidate(2, 0, required=True)],
                provider=SequenceProvider([[(None,)]]),
            )

        self.assertEqual(raised.exception.code, "required_stops_not_feasible")
        self.assertEqual(raised.exception.http_status, 422)


class ProviderTests(TestCase):
    def test_routes_provider_defaults_to_traffic_unaware(self):
        http = FormulaHttp()
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=http,
            cache_ttl_seconds=0,
        )
        result = provider.get_matrix_seconds(
            [START],
            [Coordinate(57.71, 11.98)],
            ephemeral_origin_indexes=frozenset({0}),
        )

        self.assertEqual(result.routing_preference, "TRAFFIC_UNAWARE")
        self.assertEqual(
            http.calls[0]["json"]["routingPreference"],
            "TRAFFIC_UNAWARE",
        )

    def test_routes_provider_reads_element_status_and_ceil_duration(self):
        http = FormulaHttp()
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=http,
            cache_ttl_seconds=0,
        )
        result = provider.get_matrix_seconds(
            [START],
            [Coordinate(57.71, 11.98), Coordinate(57.72, 11.99)],
            ephemeral_origin_indexes=frozenset({0}),
        )

        self.assertEqual(result.seconds[0], (61, 62))
        self.assertEqual(result.request_count, 1)
        self.assertNotIn("secret", str(http.calls[0]["json"]))
        self.assertEqual(
            http.calls[0]["headers"]["X-Goog-FieldMask"],
            "originIndex,destinationIndex,duration,status,condition",
        )

    def test_store_pairs_are_cached_but_ephemeral_start_is_not(self):
        http = FormulaHttp()
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=http,
            cache_ttl_seconds=600,
        )
        origins = [START, Coordinate(57.71, 11.98)]
        destinations = [Coordinate(57.72, 11.99), Coordinate(57.73, 12.0)]

        provider.get_matrix_seconds(
            origins,
            destinations,
            ephemeral_origin_indexes=frozenset({0}),
        )
        calls_after_first = len(http.calls)
        second = provider.get_matrix_seconds(
            origins,
            destinations,
            ephemeral_origin_indexes=frozenset({0}),
        )

        self.assertEqual(calls_after_first, 2)
        self.assertEqual(len(http.calls), 3)
        self.assertEqual(second.cache_hits, 2)

    def test_timeout_becomes_controlled_provider_error(self):
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=TimeoutHttp(),
        )
        with self.assertRaises(TravelTimeTimeout):
            provider.get_matrix_seconds(
                [START],
                [Coordinate(57.71, 11.98)],
                ephemeral_origin_indexes=frozenset({0}),
            )

    def test_missing_route_is_returned_as_unavailable_not_approximated(self):
        http = StaticHttp([
            {
                "originIndex": 0,
                "destinationIndex": 0,
                "condition": "ROUTE_NOT_FOUND",
                "status": {},
            }
        ])
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=http,
        )
        result = provider.get_matrix_seconds(
            [START],
            [Coordinate(57.71, 11.98)],
            ephemeral_origin_indexes=frozenset({0}),
        )
        self.assertEqual(result.seconds, ((None,),))

    def test_missing_matrix_element_retries_then_fails_whole_block(self):
        http = StaticHttp([])
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=2,
            http_session=http,
            sleeper=lambda _seconds: None,
        )
        with self.assertRaises(TravelTimeUnavailable):
            provider.get_matrix_seconds(
                [START],
                [Coordinate(57.71, 11.98)],
                ephemeral_origin_indexes=frozenset({0}),
            )
        self.assertEqual(http.call_count, 2)

    def test_traffic_aware_matrix_requests_never_exceed_625_elements(self):
        http = FormulaHttp()
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            max_attempts=1,
            http_session=http,
            cache_ttl_seconds=0,
        )
        origins = [Coordinate(57 + index / 1000, 11.0) for index in range(26)]
        destinations = [Coordinate(58 + index / 1000, 12.0) for index in range(26)]
        provider.get_matrix_seconds(origins, destinations)

        self.assertTrue(http.calls)
        self.assertTrue(all(
            len(call["json"]["origins"]) * len(call["json"]["destinations"]) <= 625
            for call in http.calls
        ))

    def test_traffic_aware_optimal_requests_never_exceed_100_elements(self):
        http = FormulaHttp()
        provider = GoogleRoutesTravelTimeProvider(
            "secret",
            routing_preference="TRAFFIC_AWARE_OPTIMAL",
            max_attempts=1,
            http_session=http,
            cache_ttl_seconds=0,
        )
        origins = [Coordinate(57 + index / 1000, 11.0) for index in range(11)]
        destinations = [
            Coordinate(58 + index / 1000, 12.0)
            for index in range(11)
        ]
        provider.get_matrix_seconds(origins, destinations)

        self.assertTrue(http.calls)
        self.assertTrue(all(
            len(call["json"]["origins"]) * len(call["json"]["destinations"]) <= 100
            for call in http.calls
        ))


class RouteEndpointTests(TestCase):
    def setUp(self):
        app_module.app.config.update(TESTING=True, SECRET_KEY="route-test-secret")
        self.client = app_module.app.test_client()
        self.customers = [
            {
                "row": 2,
                "customer": "Authoritative Store",
                "cancelled_flag": "",
                "sales_person": "Route User",
                "latitude_google": "57.7000",
                "longitude_google": "11.9000",
            },
            {
                "row": 3,
                "customer": "Other Store",
                "cancelled_flag": "",
                "sales_person": "Other Seller",
                "latitude_google": "57.7100",
                "longitude_google": "11.9100",
            },
        ]
        self.priorities = [
            {"row": 2, "customer": "Authoritative Store", "priority_score": 88},
            {"row": 3, "customer": "Other Store", "priority_score": 42},
        ]
        self.provider = FormulaProvider()
        self.patchers = [
            patch.object(app_module, "get_spreadsheet_with_retry", return_value=object()),
            patch.object(app_module, "get_customer_rows", return_value=self.customers),
            patch.object(app_module, "get_contact_rows", return_value=[]),
            patch.object(app_module, "get_order_rows", return_value=[]),
            patch.object(app_module, "get_email_rows", return_value=([], [], [])),
            patch.object(
                app_module,
                "build_current_priority_snapshot",
                return_value=(self.priorities, {}),
            ),
            patch.object(
                app_module,
                "get_route_travel_time_provider",
                return_value=self.provider,
            ),
            patch.object(
                app_module,
                "get_saved_route_proposal",
                return_value=None,
            ),
            patch.object(app_module, "save_route_proposal"),
        ]
        self.mocks = []
        for patcher in self.patchers:
            self.mocks.append(patcher.start())
        self.saved_route_mock = self.mocks[-2]
        self.save_route_mock = self.mocks[-1]

    def tearDown(self):
        for patcher in reversed(self.patchers):
            patcher.stop()

    def login(self, *, role="Säljare", name="Route User"):
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = {
                "user_name": "route-user",
                "name": name,
                "role": role,
            }

    def test_authentication_is_required(self):
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7, "longitude": 11.9},
            "candidate_rows": [2],
        })
        self.assertEqual(response.status_code, 401)

    def test_success_uses_authoritative_row_score_name_and_coordinates(self):
        self.login()
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7089, "longitude": 11.9746},
            "candidate_rows": [2],
            "priority_scores": {"2": 9999},
            "max_total_minutes": 1,
            "service_minutes_per_stop": 0,
        })
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["stops"][0]["row"], 2)
        self.assertEqual(payload["stops"][0]["customer"], "Authoritative Store")
        self.assertEqual(payload["stops"][0]["priority_score"], 88)
        self.assertEqual(payload["stops"][0]["latitude"], 57.7)
        self.assertEqual(payload["stops"][0]["longitude"], 11.9)
        self.assertEqual(payload["meta"]["max_total_minutes"], 420)
        self.assertEqual(payload["meta"]["max_route_stops"], 15)
        self.assertEqual(payload["meta"]["service_minutes_per_stop"], 20)
        self.assertTrue(payload["meta"]["includes_return_to_start"])
        self.assertEqual(payload["route_owner"], "Route User")
        self.assertIn("route_date", payload)
        self.save_route_mock.assert_called_once()

    def test_endpoint_caps_candidates_before_first_provider_call(self):
        self.login()
        self.customers[:] = [
            {
                "row": row,
                "customer": f"Store {row}",
                "cancelled_flag": "",
                "sales_person": "Route User",
                "latitude_google": str(57.0 + row / 1000),
                "longitude_google": str(11.0 + row / 1000),
            }
            for row in range(2, 77)
        ]
        self.priorities[:] = [
            {
                "row": row,
                "customer": f"Store {row}",
                "priority_score": 1000 - row,
            }
            for row in range(2, 77)
        ]

        with patch.object(
            app_module,
            "route_matrix_candidate_limit",
            return_value=20,
        ):
            response = self.client.post("/route-proposal", json={
                "start": {"latitude": 57.7, "longitude": 11.9},
                "candidate_rows": [2],
            })

        payload = response.get_json()
        self.assertEqual(response.status_code, 200, payload)
        self.assertEqual(
            payload["meta"]["candidate_count_before_preselection"],
            75,
        )
        self.assertEqual(
            payload["meta"]["candidate_count_after_preselection"],
            20,
        )
        self.assertEqual(payload["meta"]["matrix_candidate_limit"], 20)
        self.assertEqual(self.provider.call_shapes[0], (1, 20))

    def test_phone_recommendation_does_not_exclude_high_score_route_candidate(self):
        self.login()
        self.priorities[0]["recommended_channel"] = "telefon"

        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7089, "longitude": 11.9746},
            "candidate_rows": [2],
        })
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([stop["row"] for stop in payload["stops"]], [2])
        self.assertEqual(payload["stops"][0]["priority_score"], 88)

    def test_only_requested_rows_can_be_selected(self):
        self.login(role="Administratör")
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7089, "longitude": 11.9746},
            "candidate_rows": [3],
        })
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual([stop["row"] for stop in payload["stops"]], [3])
        self.assertEqual(payload["stops"][0]["priority_score"], 42)

    def test_seller_scope_ignores_client_rows_and_uses_only_owned_customers(self):
        self.login()
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7089, "longitude": 11.9746},
            "candidate_rows": [3],
        })
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([stop["row"] for stop in payload["stops"]], [2])
        self.assertEqual(payload["meta"]["requested_candidate_count"], 1)

    def test_saved_daily_route_is_returned_without_new_provider_calls(self):
        self.login()
        saved = {}

        def load_saved(_spreadsheet, _user_name, _route_date):
            if not saved:
                return None
            return {
                **saved,
                "cached": True,
                "meta": {**saved["meta"], "daily_cache_hit": True},
            }

        def persist_saved(_spreadsheet, **kwargs):
            saved.update(kwargs["payload"])

        self.saved_route_mock.side_effect = load_saved
        self.save_route_mock.side_effect = persist_saved
        first = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7089, "longitude": 11.9746},
            "candidate_rows": [2],
        })
        calls_after_first = len(self.provider.call_shapes)
        second = self.client.post("/route-proposal", json={
            "start": {"latitude": 58.0, "longitude": 12.0},
            "candidate_rows": [3],
        })

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertTrue(second.get_json()["cached"])
        self.assertEqual(
            second.get_json()["start"],
            first.get_json()["start"],
        )
        self.assertEqual(len(self.provider.call_shapes), calls_after_first)
        self.assertEqual(self.save_route_mock.call_count, 1)

    def test_invalid_start_is_rejected(self):
        self.login()
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 999, "longitude": 11.9},
            "candidate_rows": [2],
        })
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "invalid_start")

    def test_candidate_without_server_score_is_excluded(self):
        self.login()
        self.priorities.clear()
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7, "longitude": 11.9},
            "candidate_rows": [2],
        })
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.get_json()["code"], "no_eligible_candidates")

    def test_cancelled_candidate_is_excluded_server_side(self):
        self.login()
        self.customers[0]["cancelled_flag"] = "ja"
        response = self.client.post("/route-proposal", json={
            "start": {"latitude": 57.7, "longitude": 11.9},
            "candidate_rows": [2],
        })
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.get_json()["code"], "no_eligible_candidates")

    def test_provider_timeout_has_stable_error_contract(self):
        self.login()
        with patch.object(
            app_module,
            "get_route_travel_time_provider",
            return_value=ErrorProvider(TravelTimeTimeout()),
        ):
            response = self.client.post("/route-proposal", json={
                "start": {"latitude": 57.7, "longitude": 11.9},
                "candidate_rows": [2],
            })
        payload = response.get_json()
        self.assertEqual(response.status_code, 504)
        self.assertEqual(payload["error"], "travel_time_timeout")
        self.assertEqual(payload["code"], "travel_time_timeout")
        self.assertTrue(payload["message"])


class RouteProposalStorageTests(TestCase):
    def test_saved_route_round_trips_through_daily_sheet(self):
        sheet = RouteStorageWorksheet()
        spreadsheet = RouteStorageSpreadsheet(sheet)
        payload = {
            "ok": True,
            "cached": False,
            "generated_at": "2026-07-26T09:00:00+02:00",
            "route_date": "2026-07-26",
            "route_owner": "Sofia",
            "start": {"latitude": 57.7, "longitude": 11.9},
            "stops": [{"row": 2, "sequence": 1}],
            "summary": {"stop_count": 1},
            "meta": {"daily_cache_hit": False},
        }

        app_module.save_route_proposal(
            spreadsheet,
            user_name="sofia",
            user_display_name="Sofia",
            route_date=app_module.date(2026, 7, 26),
            payload=payload,
        )
        loaded = app_module.get_saved_route_proposal(
            spreadsheet,
            "SOFIA",
            app_module.date(2026, 7, 26),
        )

        self.assertTrue(loaded["cached"])
        self.assertTrue(loaded["meta"]["daily_cache_hit"])
        self.assertEqual(loaded["route_owner"], "Sofia")
        self.assertEqual(loaded["start"], payload["start"])
        self.assertEqual(len(sheet.values), 2)


class FrontendRouteProposalFlowTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")

    def test_route_proposal_seeds_the_editable_map_route(self):
        self.assertIn("function seedRouteProposalMapSelection()", self.html)
        self.assertIn(
            "if (routeProposal) seedRouteProposalMapSelection();",
            self.html,
        )
        self.assertIn("return routeInMapStops;", self.html)
        self.assertIn(
            "if (!routeProposal) saveCurrentMapRouteDraft();",
            self.html,
        )

    def test_route_proposal_uses_the_standard_map_controls(self):
        self.assertIn(
            'document.getElementById("map-clear-route-btn").addEventListener("click", clearMapRoute);',
            self.html,
        )
        self.assertIn('googleMapsButton.textContent = "🌎 Öppna i Google Maps";', self.html)
        self.assertIn('clearButton.textContent = "Rensa rutt";', self.html)

    def test_staged_google_maps_export_is_removed(self):
        self.assertNotIn("getRouteProposalExportStages", self.html)
        self.assertNotIn("Google Maps-etapper", self.html)
        self.assertNotIn("route-proposal-stage-link", self.html)
        self.assertNotIn("Google Maps delar rutten", self.html)

    def test_daily_route_is_checked_before_geolocation(self):
        get_index = self.html.index('fetch(`${API}/route-proposal`, {\n        method: "GET"')
        position_index = self.html.index(
            "const currentPosition = await getCurrentPositionForRoute();"
        )
        self.assertLess(get_index, position_index)

    def test_seller_route_context_uses_owned_customers(self):
        self.assertIn("function currentUserIsSeller()", self.html)
        self.assertIn("function getRouteProposalBaseCustomers()", self.html)
        self.assertIn(
            "normalizeRouteIdentity(customer?.sales_person) === sellerName",
            self.html,
        )

    def test_route_ui_shows_owner_date_and_return_to_start(self):
        self.assertIn(
            "Ruttförslag för ${owner} (${routeDate})",
            self.html,
        )
        self.assertIn("Retur till start", self.html)
        self.assertIn("const ROUTE_MAX_STOPS = 15;", self.html)
        self.assertIn("const ROUTE_MAX_TOTAL_MINUTES = 420;", self.html)

    def test_google_maps_export_returns_to_route_start(self):
        self.assertIn(
            'const useRouteProposal = mapContext !== "planning" && routeProposal;',
            self.html,
        )
        self.assertIn(
            "const returnPoint = useRouteProposal ? getRouteCoordinatePair(routeProposal.start) : null;",
            self.html,
        )
        self.assertIn(
            "const waypoints = useRouteProposal ? stops : stops.slice(0, -1);",
            self.html,
        )

    def test_priority_sorted_list_replaces_daily_focus(self):
        self.assertNotIn('id="chip-daily-focus"', self.html)
        self.assertNotIn("dailyFocusActive", self.html)
        self.assertNotIn("DAILY_FOCUS_LIMIT", self.html)
        self.assertIn("const LIST_BATCH_SIZE = 20;", self.html)
        self.assertIn(
            "return getFilteredCustomers().slice().sort(prioritySort);",
            self.html,
        )

    def test_customer_cards_use_one_priority_guidance_layout(self):
        self.assertIn("getPrioritySummaryHtml(c)", self.html)
        self.assertIn("getNextActionHtml(c)", self.html)
        self.assertIn("getPotentialHtml(c)", self.html)
        self.assertIn("getCardDatesHtml(c)", self.html)
        self.assertIn("getCustomerCardAriaLabel(c, routeStop)", self.html)
        self.assertIn('"besök": "Besök"', self.html)
        self.assertIn('"telefon": "Telefon"', self.html)
        self.assertIn("Potential ej beräknad", self.html)
        self.assertNotIn("Missad uppföljning</span>", self.html)

    def test_route_candidates_ignore_recommended_channel(self):
        start = self.html.index("function getRouteProposalCandidates(")
        end = self.html.index("function getStockholmRouteDate()", start)
        candidate_source = self.html[start:end]
        self.assertIn("score > 0", candidate_source)
        self.assertNotIn("recommended_channel", candidate_source)

    def test_route_card_does_not_repeat_priority_score(self):
        start = self.html.index('const routeSummary = routeStop ? `')
        end = self.html.index("` : \"\";", start)
        route_summary_source = self.html[start:end]
        self.assertNotIn("routeStop.priority_score", route_summary_source)

    def test_watchlist_panel_is_removed_from_insights(self):
        self.assertNotIn("Butiker värda att bevaka", self.html)
        self.assertNotIn("priority-customer-list", self.html)
        self.assertNotIn("renderPriorityCustomers", self.html)

    def test_initial_list_waits_for_insights_and_has_error_fallback(self):
        self.assertIn("await loadInsights({ render: false });", self.html)
        self.assertIn("applyDefaultResponsibleFilter();", self.html)
        self.assertIn("Kundprioriteringen kunde inte laddas.", self.html)
        self.assertIn("priority-neutral", self.html)


class FormulaProvider:
    def __init__(self):
        self.call_shapes = []

    def get_matrix_seconds(
        self, origins, destinations, *, ephemeral_origin_indexes=frozenset()
    ):
        self.call_shapes.append((len(origins), len(destinations)))
        seconds = []
        for origin in origins:
            row = []
            for destination in destinations:
                if origin == destination:
                    row.append(0)
                else:
                    row.append(60)
            seconds.append(tuple(row))
        return TravelTimeResult(
            seconds=tuple(seconds),
            pair_count=len(origins) * len(destinations),
            request_count=1,
            routing_preference="TRAFFIC_UNAWARE",
        )


class SequenceProvider:
    def __init__(self, matrices):
        self.matrices = list(matrices)

    def get_matrix_seconds(
        self, origins, destinations, *, ephemeral_origin_indexes=frozenset()
    ):
        matrix = self.matrices.pop(0)
        return TravelTimeResult(
            seconds=tuple(tuple(row) for row in matrix),
            pair_count=len(origins) * len(destinations),
            request_count=1,
            routing_preference="TRAFFIC_UNAWARE",
        )


class FormulaResponse:
    status_code = 200

    def __init__(self, elements):
        self.elements = elements

    def json(self):
        return self.elements


class FormulaHttp:
    def __init__(self):
        self.calls = []

    def post(self, url, *, json, headers, timeout):
        self.calls.append({
            "url": url,
            "json": json,
            "headers": headers,
            "timeout": timeout,
        })
        elements = []
        for origin_index, _origin in enumerate(json["origins"]):
            for destination_index, _destination in enumerate(json["destinations"]):
                elements.append({
                    "originIndex": origin_index,
                    "destinationIndex": destination_index,
                    "condition": "ROUTE_EXISTS",
                    "status": {},
                    "duration": f"{60 + origin_index + destination_index + 0.1}s",
                })
        return FormulaResponse(elements)


class StaticHttp:
    def __init__(self, elements):
        self.elements = elements
        self.call_count = 0

    def post(self, url, *, json, headers, timeout):
        self.call_count += 1
        return FormulaResponse(self.elements)


class TimeoutHttp:
    def post(self, url, *, json, headers, timeout):
        raise requests.Timeout("route timeout")


class ErrorProvider:
    def __init__(self, error):
        self.error = error

    def get_matrix_seconds(
        self, origins, destinations, *, ephemeral_origin_indexes=frozenset()
    ):
        raise self.error


class RouteStorageWorksheet:
    title = app_module.ROUTE_PROPOSALS_SHEET
    col_count = 10
    row_count = 500

    def __init__(self):
        self.values = [list(app_module.ROUTE_PROPOSAL_COLUMNS)]

    def get_all_values(self):
        return [list(row) for row in self.values]

    def row_values(self, row):
        index = row - 1
        return list(self.values[index]) if index < len(self.values) else []

    def update_cell(self, row, column, value):
        while len(self.values) < row:
            self.values.append([])
        while len(self.values[row - 1]) < column:
            self.values[row - 1].append("")
        self.values[row - 1][column - 1] = value

    def resize(self, **_kwargs):
        return None

    def batch_update(self, updates, value_input_option="RAW"):
        del value_input_option
        for update in updates:
            self.values.extend([list(row) for row in update["values"]])


class RouteStorageSpreadsheet:
    def __init__(self, sheet):
        self.sheet = sheet

    def worksheet(self, title):
        if title != app_module.ROUTE_PROPOSALS_SHEET:
            raise AssertionError(title)
        return self.sheet


def brute_force_route(candidates, matrix, max_seconds, service_seconds):
    best_path = ()
    best_key = (0, 0, 0, ())
    for length in range(1, len(candidates) + 1):
        for path in permutations(range(len(candidates)), length):
            drive = 0
            origin = 0
            valid = True
            for index in path:
                leg = matrix[origin][index]
                if leg is None:
                    valid = False
                    break
                drive += leg
                origin = index + 1
            total = drive + length * service_seconds
            if not valid or total > max_seconds:
                continue
            score = sum(candidates[index].priority_score for index in path)
            rows = tuple(candidates[index].row for index in path)
            key = (-score, drive, total, rows)
            if key < best_key:
                best_key = key
                best_path = path
    return best_path


if __name__ == "__main__":
    main()
