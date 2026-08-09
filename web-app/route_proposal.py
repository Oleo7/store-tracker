from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import math
import threading
import time
from typing import Callable, Protocol, Sequence

import requests


MAX_TOTAL_SECONDS = 7 * 60 * 60
SERVICE_SECONDS_PER_STOP = 20 * 60
SHORTLIST_LIMIT = 24
EXACT_SOLVER_LIMIT = 15
MAX_ROUTE_STOPS = 15
LOCAL_CLUSTER_RADIUS_KM = 40.0
LOCAL_CLUSTER_CELL_KM = 20.0
LOCAL_CLUSTER_CELL_REACH = 3
APPROXIMATE_ROUTE_PREFIX_FACTOR = 4
ROUTES_MATRIX_URL = "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix"
SUPPORTED_ROUTING_PREFERENCES = {
    "TRAFFIC_AWARE",
    "TRAFFIC_AWARE_OPTIMAL",
    "TRAFFIC_UNAWARE",
}


class RouteProposalError(RuntimeError):
    code = "route_proposal_error"
    message = "Kunde inte skapa ett ruttförslag."
    http_status = 500

    def __init__(self, message: str | None = None):
        super().__init__(message or self.message)
        self.public_message = message or self.message


class TravelTimeConfigurationError(RouteProposalError):
    code = "travel_time_not_configured"
    message = "Körtidstjänsten är inte konfigurerad."
    http_status = 503


class TravelTimeTimeout(RouteProposalError):
    code = "travel_time_timeout"
    message = "Kunde inte beräkna körtider. Försök igen."
    http_status = 504


class TravelTimeUnavailable(RouteProposalError):
    code = "travel_time_unavailable"
    message = "Kunde inte beräkna körtider. Försök igen."
    http_status = 502


class NoFeasibleRoute(RouteProposalError):
    code = "no_feasible_route"
    message = "Ingen genomförbar rutt hittades under sju timmar inklusive retur till start."
    http_status = 422


class RequiredStopsNotFeasible(RouteProposalError):
    code = "required_stops_not_feasible"
    message = "Alla obligatoriska stopp ryms inte inom ruttens tids- och stoppgränser."
    http_status = 422


class RouteVerificationError(RouteProposalError):
    code = "route_verification_failed"
    message = "Den beräknade rutten kunde inte verifieras."
    http_status = 500


@dataclass(frozen=True)
class Coordinate:
    latitude: float
    longitude: float


@dataclass(frozen=True)
class RouteCandidate:
    row: int
    customer: str
    coordinate: Coordinate
    priority_score: int
    required: bool = False


def _haversine_km(origin: Coordinate, destination: Coordinate) -> float:
    radius_km = 6371.0088
    origin_lat = math.radians(origin.latitude)
    destination_lat = math.radians(destination.latitude)
    delta_lat = destination_lat - origin_lat
    delta_lon = math.radians(destination.longitude - origin.longitude)
    value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(origin_lat)
        * math.cos(destination_lat)
        * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.asin(min(1.0, math.sqrt(value)))


def anchor_aware_preselect_candidates(
    *, start: Coordinate, candidates: Sequence[RouteCandidate],
    anchor_rows: Sequence[int], limit: int = 35,
) -> tuple[RouteCandidate, ...]:
    """Select a deterministic business/geographic pool around fixed anchors."""
    effective_limit = max(1, int(limit))
    by_row = {candidate.row: candidate for candidate in candidates}
    anchors = [
        by_row[row] for row in anchor_rows
        if row in by_row and by_row[row].required
    ]
    remaining_required = sorted(
        (
            candidate for candidate in candidates
            if candidate.required and candidate.row not in set(anchor_rows)
        ),
        key=lambda candidate: candidate.row,
    )
    anchors.extend(remaining_required)
    if len(anchors) > effective_limit:
        raise RequiredStopsNotFeasible()

    optional = sorted(
        (candidate for candidate in candidates if not candidate.required),
        key=lambda candidate: candidate.row,
    )
    if len(anchors) + len(optional) <= effective_limit:
        return tuple([*anchors, *optional])

    anchor_coordinates = [candidate.coordinate for candidate in anchors]
    segments = list(zip(
        [start, *anchor_coordinates],
        [*anchor_coordinates, start],
    ))

    def stable(candidate):
        return candidate.row

    def anchor_distance(candidate):
        return min(
            _haversine_km(candidate.coordinate, anchor)
            for anchor in anchor_coordinates
        )

    def detour(candidate):
        return min(
            _haversine_km(origin, candidate.coordinate)
            + _haversine_km(candidate.coordinate, destination)
            - _haversine_km(origin, destination)
            for origin, destination in segments
        )

    detours = {candidate.row: max(0.0, detour(candidate)) for candidate in optional}
    rankings = (
        sorted(optional, key=lambda item: (-item.priority_score, stable(item))),
        sorted(optional, key=lambda item: (
            anchor_distance(item), -item.priority_score, stable(item)
        )),
        sorted(optional, key=lambda item: (
            detours[item.row], -item.priority_score, stable(item)
        )),
        sorted(optional, key=lambda item: (
            -(item.priority_score / (1.0 + detours[item.row])),
            -item.priority_score,
            stable(item),
        )),
    )
    positions = [0] * len(rankings)
    selected = list(anchors)
    selected_rows = {candidate.row for candidate in selected}
    while len(selected) < effective_limit:
        added = False
        for ranking_index, ranking in enumerate(rankings):
            while (
                positions[ranking_index] < len(ranking)
                and ranking[positions[ranking_index]].row in selected_rows
            ):
                positions[ranking_index] += 1
            if positions[ranking_index] >= len(ranking):
                continue
            candidate = ranking[positions[ranking_index]]
            positions[ranking_index] += 1
            selected.append(candidate)
            selected_rows.add(candidate.row)
            added = True
            if len(selected) >= effective_limit:
                break
        if not added:
            break
    return tuple(selected)


@dataclass(frozen=True)
class TravelTimeResult:
    seconds: tuple[tuple[int | None, ...], ...]
    cache_hits: int = 0
    pair_count: int = 0
    request_count: int = 0
    routing_preference: str = "TRAFFIC_UNAWARE"


@dataclass(frozen=True)
class RouteSolution:
    route_indices: tuple[int, ...]
    solver_status: str
    optimality_proven: bool
    algorithm: str
    calculation_duration_ms: int


@dataclass(frozen=True)
class VerifiedStop:
    candidate: RouteCandidate
    sequence: int
    leg_drive_seconds: int
    cumulative_drive_seconds: int
    cumulative_total_seconds: int


@dataclass(frozen=True)
class VerifiedRoute:
    stops: tuple[VerifiedStop, ...]
    total_priority_score: int
    drive_seconds: int
    return_drive_seconds: int
    service_seconds: int
    total_seconds: int


@dataclass(frozen=True)
class RouteProposalResult:
    route: VerifiedRoute
    solution: RouteSolution
    input_candidate_count: int
    road_reachable_candidate_count: int
    matrix_candidate_count: int
    shortlisted: bool
    excluded_missing_road_time: int
    excluded_over_budget: int
    provider_request_count: int
    provider_cache_hits: int
    provider_pair_count: int
    routing_preference: str
    calculation_duration_ms: int


class TravelTimeProvider(Protocol):
    def get_matrix_seconds(
        self,
        origins: Sequence[Coordinate],
        destinations: Sequence[Coordinate],
        *,
        ephemeral_origin_indexes: frozenset[int] = frozenset(),
    ) -> TravelTimeResult:
        """Return a directed road-time matrix in integer seconds."""


class GoogleRoutesTravelTimeProvider:
    """Google Routes v2 matrix provider with a short, process-local pair cache."""

    def __init__(
        self,
        api_key: str,
        *,
        routing_preference: str = "TRAFFIC_UNAWARE",
        timeout_seconds: float = 15.0,
        cache_ttl_seconds: float = 600.0,
        max_attempts: int = 2,
        http_session=None,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        key = str(api_key or "").strip()
        if not key:
            raise TravelTimeConfigurationError()

        preference = str(routing_preference or "TRAFFIC_UNAWARE").strip().upper()
        if preference not in SUPPORTED_ROUTING_PREFERENCES:
            preference = "TRAFFIC_UNAWARE"

        self.api_key = key
        self.routing_preference = preference
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self.cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self.max_attempts = max(1, int(max_attempts))
        self.http = http_session or requests.Session()
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._cache: dict[tuple, tuple[float, int | None]] = {}
        self._cache_lock = threading.RLock()
        # Avoid duplicate matrix spend when two requests arrive together.
        self._request_lock = threading.RLock()

    def get_matrix_seconds(
        self,
        origins: Sequence[Coordinate],
        destinations: Sequence[Coordinate],
        *,
        ephemeral_origin_indexes: frozenset[int] = frozenset(),
    ) -> TravelTimeResult:
        origins = tuple(origins)
        destinations = tuple(destinations)
        if not origins or not destinations:
            return TravelTimeResult(
                seconds=tuple(tuple() for _ in origins),
                routing_preference=self.routing_preference,
            )

        ephemeral = frozenset(
            index for index in ephemeral_origin_indexes if 0 <= index < len(origins)
        )
        matrix: list[list[int | None]] = [
            [None for _ in destinations] for _ in origins
        ]
        known: list[list[bool]] = [
            [False for _ in destinations] for _ in origins
        ]
        cache_hits = 0
        pair_count = len(origins) * len(destinations)
        request_count = 0

        with self._request_lock:
            now = self._monotonic()
            self._purge_expired(now)

            for origin_index, origin in enumerate(origins):
                if origin_index in ephemeral:
                    continue
                for destination_index, destination in enumerate(destinations):
                    cached = self._cache_get(origin, destination, now)
                    if cached is not _CACHE_MISS:
                        matrix[origin_index][destination_index] = cached
                        known[origin_index][destination_index] = True
                        cache_hits += 1

            origin_groups = []
            if ephemeral:
                origin_groups.append(tuple(sorted(ephemeral)))
            shared_origins = tuple(
                index for index in range(len(origins)) if index not in ephemeral
            )
            if shared_origins:
                origin_groups.append(shared_origins)

            max_elements = (
                100 if self.routing_preference == "TRAFFIC_AWARE_OPTIMAL" else 625
            )
            for origin_group in origin_groups:
                for destination_start in range(0, len(destinations), max_elements):
                    destination_indexes = tuple(
                        range(
                            destination_start,
                            min(destination_start + max_elements, len(destinations)),
                        )
                    )
                    origin_block_size = max(
                        1, max_elements // max(1, len(destination_indexes))
                    )
                    for origin_start in range(0, len(origin_group), origin_block_size):
                        origin_indexes = origin_group[
                            origin_start : origin_start + origin_block_size
                        ]
                        if self._block_is_complete(
                            known, origin_indexes, destination_indexes
                        ):
                            continue

                        block = self._request_matrix(
                            [origins[index] for index in origin_indexes],
                            [destinations[index] for index in destination_indexes],
                        )
                        request_count += 1
                        for local_origin, origin_index in enumerate(origin_indexes):
                            for local_destination, destination_index in enumerate(
                                destination_indexes
                            ):
                                seconds = block[local_origin][local_destination]
                                matrix[origin_index][destination_index] = seconds
                                known[origin_index][destination_index] = True
                                if origin_index not in ephemeral:
                                    self._cache_put(
                                        origins[origin_index],
                                        destinations[destination_index],
                                        seconds,
                                        now,
                                    )

        return TravelTimeResult(
            seconds=tuple(tuple(row) for row in matrix),
            cache_hits=cache_hits,
            pair_count=pair_count,
            request_count=request_count,
            routing_preference=self.routing_preference,
        )

    def _request_matrix(
        self,
        origins: Sequence[Coordinate],
        destinations: Sequence[Coordinate],
    ) -> list[list[int | None]]:
        body = {
            "origins": [_route_matrix_origin(point) for point in origins],
            "destinations": [_route_matrix_destination(point) for point in destinations],
            "travelMode": "DRIVE",
            "routingPreference": self.routing_preference,
            "languageCode": "sv-SE",
            "regionCode": "se",
            "units": "METRIC",
        }
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": (
                "originIndex,destinationIndex,duration,status,condition"
            ),
        }

        for attempt in range(self.max_attempts):
            try:
                response = self.http.post(
                    ROUTES_MATRIX_URL,
                    json=body,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
            except requests.Timeout as exc:
                if attempt + 1 >= self.max_attempts:
                    raise TravelTimeTimeout() from exc
                self._sleeper(0.25 * (2**attempt))
                continue
            except requests.RequestException as exc:
                if attempt + 1 >= self.max_attempts:
                    raise TravelTimeUnavailable() from exc
                self._sleeper(0.25 * (2**attempt))
                continue

            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 >= self.max_attempts:
                    raise TravelTimeUnavailable()
                self._sleeper(0.25 * (2**attempt))
                continue
            if response.status_code >= 400:
                raise TravelTimeUnavailable()
            try:
                return self._parse_matrix_response(
                    response.json(),
                    origin_count=len(origins),
                    destination_count=len(destinations),
                )
            except (TypeError, ValueError, TravelTimeUnavailable) as exc:
                if attempt + 1 >= self.max_attempts:
                    if isinstance(exc, TravelTimeUnavailable):
                        raise
                    raise TravelTimeUnavailable() from exc
                self._sleeper(0.25 * (2**attempt))

        raise TravelTimeUnavailable()

    @staticmethod
    def _parse_matrix_response(
        elements,
        *,
        origin_count: int,
        destination_count: int,
    ) -> list[list[int | None]]:
        if not isinstance(elements, list):
            raise TravelTimeUnavailable()

        matrix: list[list[int | None]] = [
            [None for _ in range(destination_count)]
            for _ in range(origin_count)
        ]
        seen: set[tuple[int, int]] = set()
        for element in elements:
            if not isinstance(element, dict):
                raise TravelTimeUnavailable()
            origin_index = element.get("originIndex")
            destination_index = element.get("destinationIndex")
            if (
                isinstance(origin_index, bool)
                or isinstance(destination_index, bool)
                or not isinstance(origin_index, int)
                or not isinstance(destination_index, int)
                or not 0 <= origin_index < origin_count
                or not 0 <= destination_index < destination_count
            ):
                raise TravelTimeUnavailable()
            key = (origin_index, destination_index)
            if key in seen:
                raise TravelTimeUnavailable()

            status = element.get("status") or {}
            if not isinstance(status, dict):
                raise TravelTimeUnavailable()
            try:
                status_code = int(status.get("code") or 0)
            except (TypeError, ValueError) as exc:
                raise TravelTimeUnavailable() from exc
            if status_code != 0:
                raise TravelTimeUnavailable()

            condition = element.get("condition")
            if condition == "ROUTE_EXISTS":
                seconds = _duration_to_ceiling_seconds(element.get("duration"))
                if seconds is None:
                    raise TravelTimeUnavailable()
                matrix[origin_index][destination_index] = seconds
            elif condition == "ROUTE_NOT_FOUND":
                matrix[origin_index][destination_index] = None
            else:
                raise TravelTimeUnavailable()
            seen.add(key)

        if len(seen) != origin_count * destination_count:
            raise TravelTimeUnavailable()
        return matrix

    def _cache_key(self, origin: Coordinate, destination: Coordinate) -> tuple:
        return (
            self.routing_preference,
            round(origin.latitude, 5),
            round(origin.longitude, 5),
            round(destination.latitude, 5),
            round(destination.longitude, 5),
        )

    def _cache_get(
        self, origin: Coordinate, destination: Coordinate, now: float
    ) -> int | None | object:
        key = self._cache_key(origin, destination)
        with self._cache_lock:
            cached = self._cache.get(key)
            if not cached:
                return _CACHE_MISS
            expires_at, seconds = cached
            if expires_at <= now:
                self._cache.pop(key, None)
                return _CACHE_MISS
            return seconds

    def _cache_put(
        self,
        origin: Coordinate,
        destination: Coordinate,
        seconds: int | None,
        now: float,
    ) -> None:
        if self.cache_ttl_seconds <= 0:
            return
        key = self._cache_key(origin, destination)
        with self._cache_lock:
            self._cache[key] = (now + self.cache_ttl_seconds, seconds)

    def _purge_expired(self, now: float) -> None:
        with self._cache_lock:
            expired = [
                key for key, (expires_at, _) in self._cache.items()
                if expires_at <= now
            ]
            for key in expired:
                self._cache.pop(key, None)

    @staticmethod
    def _block_is_complete(
        known: list[list[bool]],
        origin_indexes: Sequence[int],
        destination_indexes: Sequence[int],
    ) -> bool:
        return all(
            known[origin][destination]
            for origin in origin_indexes
            for destination in destination_indexes
        )


_CACHE_MISS = object()


def calculate_route_proposal(
    *,
    start: Coordinate,
    candidates: Sequence[RouteCandidate],
    provider: TravelTimeProvider,
    max_total_seconds: int = MAX_TOTAL_SECONDS,
    service_seconds_per_stop: int = SERVICE_SECONDS_PER_STOP,
    shortlist_limit: int = SHORTLIST_LIMIT,
    exact_solver_limit: int = EXACT_SOLVER_LIMIT,
    max_route_stops: int = MAX_ROUTE_STOPS,
    beam_width: int = 512,
    beam_max_expansions: int = 150_000,
    beam_time_limit_seconds: float = 2.0,
    monotonic: Callable[[], float] = time.monotonic,
) -> RouteProposalResult:
    started = monotonic()
    ordered_candidates = tuple(sorted(candidates, key=lambda item: item.row))
    if not ordered_candidates:
        raise NoFeasibleRoute()
    effective_max_route_stops = max(
        1,
        min(int(max_route_stops), MAX_ROUTE_STOPS),
    )
    required_candidates = tuple(
        candidate for candidate in ordered_candidates if candidate.required
    )
    if len(required_candidates) > effective_max_route_stops:
        raise RequiredStopsNotFeasible()

    direct_result = provider.get_matrix_seconds(
        [start],
        [candidate.coordinate for candidate in ordered_candidates],
        ephemeral_origin_indexes=frozenset({0}),
    )
    _validate_provider_matrix(
        direct_result.seconds,
        expected_rows=1,
        expected_columns=len(ordered_candidates),
    )
    direct_seconds = {
        candidate.row: _valid_road_seconds(direct_result.seconds[0][index])
        for index, candidate in enumerate(ordered_candidates)
    }
    road_reachable = tuple(
        candidate
        for candidate in ordered_candidates
        if direct_seconds[candidate.row] is not None
        and direct_seconds[candidate.row] + service_seconds_per_stop
        < max_total_seconds
    )
    road_reachable_rows = {candidate.row for candidate in road_reachable}
    if any(
        candidate.row not in road_reachable_rows
        for candidate in required_candidates
    ):
        raise RequiredStopsNotFeasible()
    if not road_reachable:
        raise NoFeasibleRoute()

    shortlist = shortlist_candidates(
        start=start,
        candidates=road_reachable,
        direct_seconds=direct_seconds,
        limit=min(shortlist_limit, SHORTLIST_LIMIT),
        service_seconds_per_stop=service_seconds_per_stop,
    )
    shortlist_rows = {candidate.row for candidate in shortlist}
    if any(
        candidate.row not in shortlist_rows
        for candidate in required_candidates
    ):
        raise RequiredStopsNotFeasible()

    matrix_result = provider.get_matrix_seconds(
        [start, *(candidate.coordinate for candidate in shortlist)],
        [candidate.coordinate for candidate in shortlist],
        ephemeral_origin_indexes=frozenset({0}),
    )
    _validate_provider_matrix(
        matrix_result.seconds,
        expected_rows=len(shortlist) + 1,
        expected_columns=len(shortlist),
    )
    return_result = provider.get_matrix_seconds(
        [candidate.coordinate for candidate in shortlist],
        [start],
        ephemeral_origin_indexes=frozenset(range(len(shortlist))),
    )
    _validate_provider_matrix(
        return_result.seconds,
        expected_rows=len(shortlist),
        expected_columns=1,
    )
    return_seconds = tuple(row[0] for row in return_result.seconds)
    solution = solve_route(
        shortlist,
        matrix_result.seconds,
        return_seconds=return_seconds,
        max_total_seconds=max_total_seconds,
        service_seconds_per_stop=service_seconds_per_stop,
        exact_solver_limit=exact_solver_limit,
        max_route_stops=max_route_stops,
        beam_width=beam_width,
        beam_max_expansions=beam_max_expansions,
        beam_time_limit_seconds=beam_time_limit_seconds,
        monotonic=monotonic,
    )
    if len(shortlist) < len(road_reachable):
        solution = replace(
            solution,
            solver_status=f"shortlisted_{solution.solver_status}",
            optimality_proven=False,
        )
    verified = verify_route(
        candidates=shortlist,
        drive_seconds=matrix_result.seconds,
        return_seconds=return_seconds,
        route_indices=solution.route_indices,
        max_total_seconds=max_total_seconds,
        service_seconds_per_stop=service_seconds_per_stop,
        max_route_stops=max_route_stops,
    )
    if not verified.stops:
        raise NoFeasibleRoute()

    provider_hits = (
        direct_result.cache_hits
        + matrix_result.cache_hits
        + return_result.cache_hits
    )
    provider_pairs = (
        direct_result.pair_count
        + matrix_result.pair_count
        + return_result.pair_count
    )
    elapsed_ms = max(0, int(round((monotonic() - started) * 1000)))
    return RouteProposalResult(
        route=verified,
        solution=solution,
        input_candidate_count=len(ordered_candidates),
        road_reachable_candidate_count=len(road_reachable),
        matrix_candidate_count=len(shortlist),
        shortlisted=len(shortlist) < len(road_reachable),
        excluded_missing_road_time=sum(
            direct_seconds[candidate.row] is None
            for candidate in ordered_candidates
        ),
        excluded_over_budget=sum(
            direct_seconds[candidate.row] is not None
            and direct_seconds[candidate.row] + service_seconds_per_stop
            >= max_total_seconds
            for candidate in ordered_candidates
        ),
        provider_request_count=(
            direct_result.request_count
            + matrix_result.request_count
            + return_result.request_count
        ),
        provider_cache_hits=provider_hits,
        provider_pair_count=provider_pairs,
        routing_preference=matrix_result.routing_preference,
        calculation_duration_ms=elapsed_ms,
    )


def shortlist_candidates(
    *,
    start: Coordinate,
    candidates: Sequence[RouteCandidate],
    direct_seconds: dict[int, int | None],
    limit: int = SHORTLIST_LIMIT,
    service_seconds_per_stop: int = SERVICE_SECONDS_PER_STOP,
) -> tuple[RouteCandidate, ...]:
    ordered = tuple(sorted(candidates, key=lambda item: item.row))
    required = tuple(candidate for candidate in ordered if candidate.required)
    if len(required) > MAX_ROUTE_STOPS:
        raise RequiredStopsNotFeasible()
    limit = max(
        len(required),
        max(1, min(int(limit), SHORTLIST_LIMIT)),
    )
    if len(ordered) <= limit:
        return ordered

    by_score = sorted(
        ordered,
        key=lambda item: (
            -item.priority_score,
            _direct_or_default(direct_seconds, item.row),
            item.row,
        ),
    )
    by_efficiency = sorted(
        ordered,
        key=lambda item: (
            -(
                item.priority_score
                / (
                    service_seconds_per_stop
                    + max(1, _direct_or_default(direct_seconds, item.row))
                )
            ),
            -item.priority_score,
            item.row,
        ),
    )
    cluster_values = _local_cluster_values(ordered)
    by_cluster_value = sorted(
        ordered,
        key=lambda item: (
            -cluster_values[item.row],
            -item.priority_score,
            _direct_or_default(direct_seconds, item.row),
            item.row,
        ),
    )
    approximate_route = _approximate_route_ranking(
        start,
        ordered,
        direct_seconds,
        service_seconds_per_stop,
        max_candidates=min(
            len(ordered),
            limit * APPROXIMATE_ROUTE_PREFIX_FACTOR,
        ),
    )

    rankings = (by_score, by_efficiency, by_cluster_value, approximate_route)
    selected: list[RouteCandidate] = list(required)
    selected_rows: set[int] = {candidate.row for candidate in required}
    cursors = [0 for _ in rankings]
    while len(selected) < limit:
        added = False
        for ranking_index, ranking in enumerate(rankings):
            while (
                cursors[ranking_index] < len(ranking)
                and ranking[cursors[ranking_index]].row in selected_rows
            ):
                cursors[ranking_index] += 1
            if cursors[ranking_index] >= len(ranking):
                continue
            candidate = ranking[cursors[ranking_index]]
            cursors[ranking_index] += 1
            selected.append(candidate)
            selected_rows.add(candidate.row)
            added = True
            if len(selected) >= limit:
                break
        if not added:
            break

    if len(selected) < limit:
        for candidate in by_score:
            if candidate.row in selected_rows:
                continue
            selected.append(candidate)
            selected_rows.add(candidate.row)
            if len(selected) >= limit:
                break

    # Stable solver node order makes complete ties reproducible.
    return tuple(sorted(selected, key=lambda item: item.row))


def solve_route(
    candidates: Sequence[RouteCandidate],
    drive_seconds: Sequence[Sequence[int | None]],
    *,
    return_seconds: Sequence[int | None] | None = None,
    max_total_seconds: int = MAX_TOTAL_SECONDS,
    service_seconds_per_stop: int = SERVICE_SECONDS_PER_STOP,
    exact_solver_limit: int = EXACT_SOLVER_LIMIT,
    max_route_stops: int = MAX_ROUTE_STOPS,
    beam_width: int = 512,
    beam_max_expansions: int = 150_000,
    beam_time_limit_seconds: float = 2.0,
    monotonic: Callable[[], float] = time.monotonic,
) -> RouteSolution:
    candidates = tuple(candidates)
    _validate_matrix_shape(candidates, drive_seconds)
    return_seconds = _normalize_return_seconds(candidates, return_seconds)
    max_route_stops = max(1, min(int(max_route_stops), MAX_ROUTE_STOPS))
    required_indexes = frozenset(
        index for index, candidate in enumerate(candidates) if candidate.required
    )
    if len(required_indexes) > max_route_stops:
        raise RequiredStopsNotFeasible()
    if len(candidates) <= exact_solver_limit:
        solution = _solve_exact_dp(
            candidates,
            drive_seconds,
            return_seconds=return_seconds,
            max_total_seconds=max_total_seconds,
            service_seconds_per_stop=service_seconds_per_stop,
            max_route_stops=max_route_stops,
            monotonic=monotonic,
        )
    else:
        solution = _solve_beam(
            candidates,
            drive_seconds,
            return_seconds=return_seconds,
            max_total_seconds=max_total_seconds,
            service_seconds_per_stop=service_seconds_per_stop,
            max_route_stops=max_route_stops,
            beam_width=beam_width,
            max_expansions=beam_max_expansions,
            time_limit_seconds=beam_time_limit_seconds,
            monotonic=monotonic,
        )
    if not required_indexes.issubset(solution.route_indices):
        raise RequiredStopsNotFeasible()
    return solution


def verify_route(
    *,
    candidates: Sequence[RouteCandidate],
    drive_seconds: Sequence[Sequence[int | None]],
    route_indices: Sequence[int],
    return_seconds: Sequence[int | None] | None = None,
    max_total_seconds: int = MAX_TOTAL_SECONDS,
    service_seconds_per_stop: int = SERVICE_SECONDS_PER_STOP,
    max_route_stops: int = MAX_ROUTE_STOPS,
) -> VerifiedRoute:
    candidates = tuple(candidates)
    _validate_matrix_shape(candidates, drive_seconds)
    return_seconds = _normalize_return_seconds(candidates, return_seconds)
    max_route_stops = max(1, min(int(max_route_stops), MAX_ROUTE_STOPS))
    required_indexes = {
        index for index, candidate in enumerate(candidates) if candidate.required
    }
    if len(required_indexes) > max_route_stops:
        raise RequiredStopsNotFeasible()
    if len(route_indices) > max_route_stops:
        raise RouteVerificationError()
    seen: set[int] = set()
    stops: list[VerifiedStop] = []
    cumulative_drive = 0
    total_score = 0
    origin_matrix_index = 0

    for sequence, candidate_index in enumerate(route_indices, start=1):
        if (
            isinstance(candidate_index, bool)
            or not isinstance(candidate_index, int)
            or not 0 <= candidate_index < len(candidates)
            or candidate_index in seen
        ):
            raise RouteVerificationError()
        seen.add(candidate_index)
        leg = drive_seconds[origin_matrix_index][candidate_index]
        valid_leg = _valid_road_seconds(leg)
        if valid_leg is None:
            raise RouteVerificationError()
        leg = valid_leg

        cumulative_drive += leg
        cumulative_total = (
            cumulative_drive + sequence * service_seconds_per_stop
        )
        if cumulative_total >= max_total_seconds:
            raise RouteVerificationError()

        candidate = candidates[candidate_index]
        total_score += candidate.priority_score
        stops.append(
            VerifiedStop(
                candidate=candidate,
                sequence=sequence,
                leg_drive_seconds=leg,
                cumulative_drive_seconds=cumulative_drive,
                cumulative_total_seconds=cumulative_total,
            )
        )
        origin_matrix_index = candidate_index + 1

    if not required_indexes.issubset(seen):
        raise RequiredStopsNotFeasible()

    return_drive_seconds = 0
    if stops:
        return_drive_seconds = _valid_road_seconds(
            return_seconds[route_indices[-1]]
        )
        if return_drive_seconds is None:
            raise RouteVerificationError()
    service_seconds = len(stops) * service_seconds_per_stop
    drive_seconds_total = cumulative_drive + return_drive_seconds
    total_seconds = drive_seconds_total + service_seconds
    if total_seconds >= max_total_seconds:
        raise RouteVerificationError()
    return VerifiedRoute(
        stops=tuple(stops),
        total_priority_score=total_score,
        drive_seconds=drive_seconds_total,
        return_drive_seconds=return_drive_seconds,
        service_seconds=service_seconds,
        total_seconds=total_seconds,
    )


def _solve_exact_dp(
    candidates: tuple[RouteCandidate, ...],
    drive_seconds: Sequence[Sequence[int | None]],
    *,
    return_seconds: tuple[int | None, ...],
    max_total_seconds: int,
    service_seconds_per_stop: int,
    max_route_stops: int,
    monotonic: Callable[[], float],
) -> RouteSolution:
    started = monotonic()
    count = len(candidates)
    if count == 0:
        return RouteSolution((), "optimal", True, "exact-dp-v1", 0)
    required_mask = sum(
        1 << index
        for index, candidate in enumerate(candidates)
        if candidate.required
    )

    state_count = 1 << count
    subset_scores = [0] * state_count
    for mask in range(1, state_count):
        lowest = mask & -mask
        index = lowest.bit_length() - 1
        subset_scores[mask] = (
            subset_scores[mask ^ lowest] + candidates[index].priority_score
        )

    costs: list[dict[int, int]] = [dict() for _ in range(state_count)]
    path_codes: list[dict[int, int]] = [dict() for _ in range(state_count)]
    predecessors: dict[tuple[int, int], tuple[int, int]] = {}
    stable_rank = {
        index: rank
        for rank, index in enumerate(
            sorted(range(count), key=lambda item: (candidates[item].row, item))
        )
    }
    path_code_base = count + 1
    for index in range(count):
        leg = drive_seconds[0][index]
        if leg is None or leg + service_seconds_per_stop >= max_total_seconds:
            continue
        mask = 1 << index
        costs[mask][index] = leg
        path_codes[mask][index] = stable_rank[index] + 1
        predecessors[(mask, index)] = (0, -1)

    best_indices: tuple[int, ...] = ()
    best_key = None if required_mask else (0, 0, 0, ())
    for mask in range(1, state_count):
        if not costs[mask]:
            continue
        stop_count = mask.bit_count()
        if stop_count > max_route_stops:
            continue
        service_seconds = stop_count * service_seconds_per_stop
        for last in sorted(costs[mask]):
            drive = costs[mask][last]
            return_leg = return_seconds[last]
            if return_leg is not None and mask & required_mask == required_mask:
                round_trip_drive = drive + return_leg
                total = round_trip_drive + service_seconds
                if total < max_total_seconds:
                    route_indices = _reconstruct_path(
                        mask,
                        last,
                        predecessors,
                    )
                    row_sequence = tuple(
                        candidates[index].row for index in route_indices
                    )
                    key = (
                        -subset_scores[mask],
                        round_trip_drive,
                        total,
                        row_sequence,
                    )
                    if best_key is None or key < best_key:
                        best_key = key
                        best_indices = route_indices

            if stop_count >= max_route_stops:
                continue
            remaining = ((1 << count) - 1) ^ mask
            next_index = 0
            while remaining:
                if remaining & 1:
                    leg = drive_seconds[last + 1][next_index]
                    if leg is not None:
                        new_drive = drive + leg
                        new_mask = mask | (1 << next_index)
                        new_total = (
                            new_drive
                            + new_mask.bit_count() * service_seconds_per_stop
                        )
                        if new_total < max_total_seconds:
                            current = costs[new_mask].get(next_index)
                            new_path_code = (
                                path_codes[mask][last] * path_code_base
                                + stable_rank[next_index]
                                + 1
                            )
                            current_path_code = path_codes[new_mask].get(next_index)
                            if (
                                current is None
                                or new_drive < current
                                or (
                                    new_drive == current
                                    and (
                                        current_path_code is None
                                        or new_path_code < current_path_code
                                    )
                                )
                            ):
                                costs[new_mask][next_index] = new_drive
                                path_codes[new_mask][next_index] = new_path_code
                                predecessors[(new_mask, next_index)] = (mask, last)
                remaining >>= 1
                next_index += 1

    elapsed_ms = max(0, int(round((monotonic() - started) * 1000)))
    return RouteSolution(
        route_indices=best_indices,
        solver_status="optimal",
        optimality_proven=True,
        algorithm="exact-dp-v1",
        calculation_duration_ms=elapsed_ms,
    )


def _solve_beam(
    candidates: tuple[RouteCandidate, ...],
    drive_seconds: Sequence[Sequence[int | None]],
    *,
    return_seconds: tuple[int | None, ...],
    max_total_seconds: int,
    service_seconds_per_stop: int,
    max_route_stops: int,
    beam_width: int,
    max_expansions: int,
    time_limit_seconds: float,
    monotonic: Callable[[], float],
) -> RouteSolution:
    started = monotonic()
    deadline = started + max(0.05, float(time_limit_seconds))
    beam_width = max(50, int(beam_width))
    max_expansions = max(1, int(max_expansions))
    count = len(candidates)
    all_mask = (1 << count) - 1
    required_mask = sum(
        1 << index
        for index, candidate in enumerate(candidates)
        if candidate.required
    )
    ordered_scores = sorted(
        ((candidate.priority_score, index) for index, candidate in enumerate(candidates)),
        reverse=True,
    )

    # mask, last candidate index, drive seconds, score, path
    states: list[tuple[int, int, int, int, tuple[int, ...]]] = [
        (0, -1, 0, 0, ())
    ]
    best_state = states[0] if not required_mask else None
    status = "beam_complete"
    expansion_count = 0

    for _depth in range(min(count, max_route_stops)):
        depth_expansions = sum(
            count - mask.bit_count()
            for mask, _last, _drive, _score, _path in states
        )
        if expansion_count + depth_expansions > max_expansions:
            status = "expansion_limit_feasible"
            break
        # Wall time is only a fail-safe between complete deterministic depths.
        if monotonic() >= deadline:
            status = "time_limit_feasible"
            break
        next_by_state: dict[
            tuple[int, int], tuple[int, int, int, int, tuple[int, ...]]
        ] = {}
        for mask, last, drive, score, path in states:
            remaining = all_mask ^ mask
            next_index = 0
            while remaining:
                if remaining & 1:
                    expansion_count += 1
                    origin_index = 0 if last < 0 else last + 1
                    leg = drive_seconds[origin_index][next_index]
                    return_leg = return_seconds[next_index]
                    if leg is not None:
                        new_drive = drive + leg
                        new_mask = mask | (1 << next_index)
                        new_stop_count = len(path) + 1
                        new_total = (
                            new_drive
                            + new_stop_count * service_seconds_per_stop
                        )
                        if new_total < max_total_seconds:
                            new_state = (
                                new_mask,
                                next_index,
                                new_drive,
                                score + candidates[next_index].priority_score,
                                (*path, next_index),
                            )
                            state_key = (new_mask, next_index)
                            current = next_by_state.get(state_key)
                            if current is None or _beam_exact_key(
                                new_state,
                                candidates,
                                service_seconds_per_stop,
                                return_seconds,
                            ) < _beam_exact_key(
                                current,
                                candidates,
                                service_seconds_per_stop,
                                return_seconds,
                            ):
                                next_by_state[state_key] = new_state
                            if (
                                return_leg is not None
                                and new_total + return_leg < max_total_seconds
                                and new_mask & required_mask == required_mask
                                and (
                                    best_state is None
                                    or _beam_exact_key(
                                        new_state,
                                        candidates,
                                        service_seconds_per_stop,
                                        return_seconds,
                                    ) < _beam_exact_key(
                                        best_state,
                                        candidates,
                                        service_seconds_per_stop,
                                        return_seconds,
                                    )
                                )
                            ):
                                best_state = new_state
                remaining >>= 1
                next_index += 1
        if not next_by_state:
            break
        states = sorted(
            next_by_state.values(),
            key=lambda state: _beam_rank_key(
                state,
                candidates,
                ordered_scores,
                max_total_seconds,
                service_seconds_per_stop,
                return_seconds,
                max_route_stops,
                required_mask,
            ),
        )[:beam_width]

    elapsed_ms = max(0, int(round((monotonic() - started) * 1000)))
    return RouteSolution(
        route_indices=best_state[4] if best_state is not None else (),
        solver_status=status,
        optimality_proven=False,
        algorithm="deterministic-bounded-beam-v1",
        calculation_duration_ms=elapsed_ms,
    )


def _beam_exact_key(
    state: tuple[int, int, int, int, tuple[int, ...]],
    candidates: Sequence[RouteCandidate],
    service_seconds_per_stop: int,
    return_seconds: Sequence[int | None],
) -> tuple:
    _mask, last, drive, score, path = state
    return_drive = (
        return_seconds[last]
        if last >= 0 and return_seconds[last] is not None
        else (0 if last < 0 else MAX_TOTAL_SECONDS)
    )
    round_trip_drive = drive + return_drive
    total = round_trip_drive + len(path) * service_seconds_per_stop
    rows = tuple(candidates[index].row for index in path)
    return (-score, round_trip_drive, total, rows)


def _beam_rank_key(
    state: tuple[int, int, int, int, tuple[int, ...]],
    candidates: Sequence[RouteCandidate],
    ordered_scores: Sequence[tuple[int, int]],
    max_total_seconds: int,
    service_seconds_per_stop: int,
    return_seconds: Sequence[int | None],
    max_route_stops: int,
    required_mask: int,
) -> tuple:
    mask, last, drive, score, path = state
    return_drive = (
        return_seconds[last]
        if last >= 0 and return_seconds[last] is not None
        else 0
    )
    remaining_stop_capacity = max(
        0,
        min(
            max_route_stops - len(path),
            (
                (max_total_seconds - drive - return_drive)
                // service_seconds_per_stop
                - len(path)
            ),
        ),
    )
    remaining_scores = [
        candidate_score
        for candidate_score, index in ordered_scores
        if not mask & (1 << index)
    ]
    optimistic_score = score + sum(remaining_scores[:remaining_stop_capacity])
    missing_required_count = (required_mask & ~mask).bit_count()
    return (
        missing_required_count,
        -optimistic_score,
        -score,
        drive,
        tuple(candidates[index].row for index in path),
    )


def _reconstruct_path(
    mask: int,
    last: int,
    predecessors: dict[tuple[int, int], tuple[int, int]],
) -> tuple[int, ...]:
    reversed_path = []
    while mask and last >= 0:
        reversed_path.append(last)
        mask, last = predecessors[(mask, last)]
    reversed_path.reverse()
    return tuple(reversed_path)


def _validate_matrix_shape(
    candidates: Sequence[RouteCandidate],
    drive_seconds: Sequence[Sequence[int | None]],
) -> None:
    expected_rows = len(candidates) + 1
    expected_columns = len(candidates)
    if len(drive_seconds) != expected_rows:
        raise RouteVerificationError()
    if any(len(row) != expected_columns for row in drive_seconds):
        raise RouteVerificationError()


def _normalize_return_seconds(
    candidates: Sequence[RouteCandidate],
    return_seconds: Sequence[int | None] | None,
) -> tuple[int | None, ...]:
    if return_seconds is None:
        return tuple(0 for _candidate in candidates)
    if len(return_seconds) != len(candidates):
        raise RouteVerificationError()
    normalized = tuple(return_seconds)
    if any(
        value is not None and _valid_road_seconds(value) is None
        for value in normalized
    ):
        raise RouteVerificationError()
    return normalized


def _validate_provider_matrix(
    matrix: Sequence[Sequence[int | None]],
    *,
    expected_rows: int,
    expected_columns: int,
) -> None:
    if (
        len(matrix) != expected_rows
        or any(len(row) != expected_columns for row in matrix)
        or any(
            value is not None and _valid_road_seconds(value) is None
            for row in matrix
            for value in row
        )
    ):
        raise TravelTimeUnavailable()


def _valid_road_seconds(value) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _approximate_route_ranking(
    start: Coordinate,
    candidates: Sequence[RouteCandidate],
    direct_seconds: dict[int, int | None],
    service_seconds_per_stop: int,
    *,
    max_candidates: int,
) -> list[RouteCandidate]:
    remaining = list(candidates)
    route = []
    current = start
    step_limit = max(0, min(len(remaining), int(max_candidates)))
    for _ in range(step_limit):
        candidate_index = min(
            range(len(remaining)),
            key=lambda index: (
                -(
                    remaining[index].priority_score
                    / (
                        1.0
                        + _haversine_km(current, remaining[index].coordinate)
                        + (
                            (
                                _direct_or_default(
                                    direct_seconds,
                                    remaining[index].row,
                                )
                            )
                            / max(1, service_seconds_per_stop)
                        )
                        * 0.05
                    )
                ),
                _direct_or_default(direct_seconds, remaining[index].row),
                remaining[index].row,
            ),
        )
        candidate = remaining.pop(candidate_index)
        route.append(candidate)
        current = candidate.coordinate
    return route


def _direct_or_default(
    direct_seconds: dict[int, int | None], row: int
) -> int:
    value = direct_seconds.get(row)
    return value if value is not None else MAX_TOTAL_SECONDS


def _local_cluster_values(
    candidates: Sequence[RouteCandidate],
) -> dict[int, float]:
    """Approximate 40 km score density with fixed-size spatial aggregates.

    Each candidate inspects a constant 7x7 set of 20 km cells rather than all
    other candidates. Score-weighted cell centroids preserve a useful distance
    decay while bounding dense-city and duplicate-coordinate workloads.
    """
    candidates = tuple(candidates)
    if not candidates:
        return {}

    reference_latitude = sum(
        candidate.coordinate.latitude for candidate in candidates
    ) / len(candidates)
    reference_latitude = max(-85.0, min(85.0, reference_latitude))
    longitude_km_per_degree = (
        111.320
        * max(0.05, math.cos(math.radians(reference_latitude)))
    )
    latitude_km_per_degree = 110.574

    projected: dict[int, tuple[float, float, tuple[int, int]]] = {}
    # total score, score-weighted x, score-weighted y
    cells: dict[tuple[int, int], list[float]] = {}
    for candidate in candidates:
        x = candidate.coordinate.longitude * longitude_km_per_degree
        y = candidate.coordinate.latitude * latitude_km_per_degree
        cell = (
            math.floor(x / LOCAL_CLUSTER_CELL_KM),
            math.floor(y / LOCAL_CLUSTER_CELL_KM),
        )
        projected[candidate.row] = (x, y, cell)
        score = float(candidate.priority_score)
        aggregate = cells.setdefault(cell, [0.0, 0.0, 0.0])
        aggregate[0] += score
        aggregate[1] += score * x
        aggregate[2] += score * y

    values: dict[int, float] = {}
    for candidate in candidates:
        x, y, own_cell = projected[candidate.row]
        own_score = float(candidate.priority_score)
        value = own_score
        for delta_x in range(
            -LOCAL_CLUSTER_CELL_REACH,
            LOCAL_CLUSTER_CELL_REACH + 1,
        ):
            for delta_y in range(
                -LOCAL_CLUSTER_CELL_REACH,
                LOCAL_CLUSTER_CELL_REACH + 1,
            ):
                cell = (own_cell[0] + delta_x, own_cell[1] + delta_y)
                aggregate = cells.get(cell)
                if aggregate is None:
                    continue

                score = aggregate[0]
                weighted_x = aggregate[1]
                weighted_y = aggregate[2]
                if cell == own_cell:
                    score -= own_score
                    weighted_x -= own_score * x
                    weighted_y -= own_score * y
                if score <= 0:
                    continue

                centroid_x = weighted_x / score
                centroid_y = weighted_y / score
                distance = math.hypot(x - centroid_x, y - centroid_y)
                if distance < LOCAL_CLUSTER_RADIUS_KM:
                    value += score * (
                        1.0 - distance / LOCAL_CLUSTER_RADIUS_KM
                    )
        values[candidate.row] = value
    return values


def _haversine_km(left: Coordinate, right: Coordinate) -> float:
    lat1 = math.radians(left.latitude)
    lat2 = math.radians(right.latitude)
    delta_lat = lat2 - lat1
    delta_lng = math.radians(right.longitude - left.longitude)
    value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lng / 2) ** 2
    )
    return 6371.0088 * 2 * math.asin(min(1.0, math.sqrt(value)))


def _route_matrix_origin(point: Coordinate) -> dict:
    return {
        "waypoint": {
            "location": {
                "latLng": {
                    "latitude": point.latitude,
                    "longitude": point.longitude,
                }
            }
        }
    }


def _route_matrix_destination(point: Coordinate) -> dict:
    return _route_matrix_origin(point)


def _duration_to_ceiling_seconds(value) -> int | None:
    text = str(value or "").strip()
    if not text.endswith("s"):
        return None
    try:
        seconds = Decimal(text[:-1])
    except (InvalidOperation, ValueError):
        return None
    if not seconds.is_finite() or seconds < 0:
        return None
    return int(seconds.to_integral_value(rounding=ROUND_CEILING))


def seconds_to_minutes(seconds: int) -> int | float:
    minutes = round(int(seconds) / 60, 1)
    return int(minutes) if float(minutes).is_integer() else minutes
