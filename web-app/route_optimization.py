"""Google Route Optimization v1 request, provider and response validation.

This module is deliberately independent from Flask and Google Sheets.  The web
application owns access control, snapshots, quota accounting and persistence;
this module owns the deterministic optimization contract only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import math
import os
import re
import threading
import time
from typing import Any, Iterable, Mapping

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.service_account import Credentials


ROUTE_ENGINE_VERSION = "ro-v2"
ROUTE_COST_PER_HOUR = 1.0
PRIORITY_PENALTY_MULTIPLIER = 10.0
ROUTE_MAX_SECONDS = 25199
SERVICE_SECONDS = 1200
MAX_VISITS = 15
QUADRATIC_SOFT_DURATION_BUFFER_SECONDS = 300
QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR = 28800
GOOGLE_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
GOOGLE_OPTIMIZE_TOURS_URL = (
    "https://routeoptimization.googleapis.com/v1/projects/{project}:optimizeTours"
)
SWEDEN_LATITUDE_RANGE = (55.0, 70.0)
SWEDEN_LONGITUDE_RANGE = (10.0, 25.0)


class RouteOptimizationError(Exception):
    """A safe, classified Route Optimization failure."""

    def __init__(
        self,
        code: str,
        public_message: str,
        http_status: int = 422,
        *,
        provider_status: int | None = None,
        counted_attempt: bool = False,
        details: Mapping[str, Any] | None = None,
    ):
        super().__init__(code)
        self.code = code
        self.public_message = public_message
        self.http_status = http_status
        self.provider_status = provider_status
        self.counted_attempt = counted_attempt
        self.details = dict(details or {})


@dataclass(frozen=True)
class TrustedCoordinate:
    latitude: float
    longitude: float


def _normalized_location(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def parse_trusted_coordinate(latitude: Any, longitude: Any) -> TrustedCoordinate | None:
    try:
        lat = float(str(latitude).strip().replace(",", "."))
        lon = float(str(longitude).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(lat) or not math.isfinite(lon):
        return None
    if not (SWEDEN_LATITUDE_RANGE[0] <= lat <= SWEDEN_LATITUDE_RANGE[1]):
        return None
    if not (SWEDEN_LONGITUDE_RANGE[0] <= lon <= SWEDEN_LONGITUDE_RANGE[1]):
        return None
    return TrustedCoordinate(round(lat, 5), round(lon, 5))


def coordinate_quality(customers: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Classify coordinates, including generic shared fallback coordinates."""
    parsed: dict[str, TrustedCoordinate | None] = {}
    coordinate_locations: dict[tuple[float, float], set[str]] = {}
    coordinate_counts: dict[tuple[float, float], int] = {}
    for customer in customers:
        customer_id = str(customer.get("customer_id") or "").strip()
        if not customer_id:
            continue
        coordinate = parse_trusted_coordinate(
            customer.get("latitude_google"), customer.get("longitude_google")
        )
        parsed[customer_id] = coordinate
        if coordinate is None:
            continue
        key = (coordinate.latitude, coordinate.longitude)
        coordinate_counts[key] = coordinate_counts.get(key, 0) + 1
        city = _normalized_location(customer.get("city_google"))
        postal = _normalized_location(customer.get("postal_code_google"))
        region = _normalized_location(customer.get("region_google"))
        if city or postal:
            location_key = f"{city}|{postal}"
        else:
            location_key = region
        if location_key:
            coordinate_locations.setdefault(key, set()).add(location_key)
    suspicious = {
        key
        for key, count in coordinate_counts.items()
        if count >= 5 and len(coordinate_locations.get(key, set())) >= 3
    }
    result = {}
    for customer_id, coordinate in parsed.items():
        if coordinate is None:
            result[customer_id] = {"trusted": False, "reason": "invalid"}
        elif (coordinate.latitude, coordinate.longitude) in suspicious:
            result[customer_id] = {"trusted": False, "reason": "suspicious_shared"}
        else:
            result[customer_id] = {
                "trusted": True,
                "coordinate": coordinate,
                "reason": "",
            }
    return result


def clamp_priority_score(value: Any) -> int:
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError, OverflowError):
        score = 1
    return max(1, min(100, score))


def priority_penalty(value: Any) -> float:
    return clamp_priority_score(value) * PRIORITY_PENALTY_MULTIPLIER


def quadratic_soft_duration_diagnostics(
    *,
    model_route_max_seconds: int,
    route_duration_seconds: int | float | Decimal,
) -> dict[str, Any]:
    """Return the configured policy and Store Tracker-derived duration cost.

    Cost is calculated with Decimal and rounded half-up to one decimal for
    stable diagnostic JSON.  It is derived from the configured model formula,
    not copied from a Google response cost field.
    """
    enabled = model_route_max_seconds > QUADRATIC_SOFT_DURATION_BUFFER_SECONDS
    result = {
        "quadratic_soft_duration_enabled": enabled,
        "quadratic_soft_buffer_seconds": (
            QUADRATIC_SOFT_DURATION_BUFFER_SECONDS
        ),
        "quadratic_soft_max_seconds": (
            model_route_max_seconds - QUADRATIC_SOFT_DURATION_BUFFER_SECONDS
            if enabled else None
        ),
        "quadratic_soft_cost_per_square_hour": (
            QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR
        ),
        "quadratic_soft_exceedance_seconds": None,
        "quadratic_soft_duration_cost": None,
    }
    if not enabled:
        return result
    try:
        duration = Decimal(str(route_duration_seconds))
    except InvalidOperation:
        return result
    if not duration.is_finite():
        return result
    threshold = Decimal(result["quadratic_soft_max_seconds"])
    exceedance = max(Decimal(0), duration - threshold)
    cost = (
        Decimal(QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR)
        * (exceedance / Decimal(3600)) ** 2
    ).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    result["quadratic_soft_exceedance_seconds"] = _diagnostic_decimal_number(
        exceedance
    )
    result["quadratic_soft_duration_cost"] = _diagnostic_decimal_number(cost)
    return result


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Route timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _duration_seconds(value: Any) -> int:
    text = str(value or "0s").strip()
    if not text.endswith("s"):
        raise ValueError("Invalid duration")
    return max(0, int(round(float(text[:-1] or 0))))


_PROTOBUF_DURATION_PATTERN = re.compile(
    r"^-?(?:0|[1-9]\d*)(?:\.\d{1,9})?s$"
)
_PROTOBUF_DURATION_MAX_SECONDS = Decimal("315576000000")


def _diagnostic_duration_decimal(value: Any) -> Decimal | None:
    """Parse generated ProtoJSON Duration text exactly for diagnostics only."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not _PROTOBUF_DURATION_PATTERN.fullmatch(text):
        return None
    try:
        seconds = Decimal(text[:-1])
    except InvalidOperation:
        return None
    if not seconds.is_finite() or abs(seconds) > _PROTOBUF_DURATION_MAX_SECONDS:
        return None
    return seconds


def _diagnostic_decimal_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _diagnostic_duration_seconds(value: Any) -> int | float | None:
    """Return signed seconds without coercing malformed values or truncating fractions."""
    return _diagnostic_decimal_number(_diagnostic_duration_decimal(value))


def _response_shipment_index(
    item: Mapping[str, Any],
    *,
    index_key: str,
    label_key: str,
    label_indexes: Mapping[str, int],
    shipment_count: int,
) -> int:
    """Resolve a ProtoJSON shipment reference by source index and/or label."""
    explicit_index = None
    if index_key in item:
        value = item[index_key]
        if isinstance(value, bool):
            raise ValueError("Invalid shipment index")
        if isinstance(value, int):
            explicit_index = value
        elif isinstance(value, str) and value.strip().lstrip("-").isdigit():
            explicit_index = int(value.strip())
        else:
            raise ValueError("Invalid shipment index")
        if explicit_index < 0 or explicit_index >= shipment_count:
            raise ValueError("Shipment index out of range")

    label = str(item.get(label_key) or "").strip()
    label_index = None
    if label:
        if label not in label_indexes:
            raise ValueError("Unknown shipment label")
        label_index = label_indexes[label]

    if explicit_index is None and label_index is None:
        raise ValueError("Missing shipment identity")
    if explicit_index is not None and label_index is not None and explicit_index != label_index:
        raise ValueError("Shipment index and label disagree")
    return explicit_index if explicit_index is not None else label_index


def _visit_request(
    shipment: Mapping[str, Any],
    *,
    global_start: datetime,
    global_end: datetime,
) -> dict[str, Any]:
    customer_id = str(shipment["customer_id"])
    coordinate = shipment["coordinate"]
    request: dict[str, Any] = {
        "label": f"visit:{customer_id}",
        "arrivalLocation": {
            "latitude": coordinate.latitude,
            "longitude": coordinate.longitude,
        },
        "duration": f"{SERVICE_SECONDS}s",
    }
    fixed_at = shipment.get("fixed_at")
    if fixed_at:
        request["timeWindows"] = [{
            "startTime": _utc_text(max(global_start, fixed_at - timedelta(minutes=15))),
            "endTime": _utc_text(min(global_end, fixed_at + timedelta(minutes=15))),
        }]
    return request


def build_optimize_tours_request(
    *,
    run_id: str,
    owner_user_name: str,
    route_start: datetime,
    start: TrustedCoordinate,
    shipments: Iterable[Mapping[str, Any]],
    fixed_breaks: Iterable[Mapping[str, Any]] = (),
    pre_route_fixed_seconds: int = 0,
    timeout_seconds: int = 90,
    solving_mode: str = "DEFAULT_SOLVE",
) -> dict[str, Any]:
    shipment_list = list(shipments)
    available_seconds = ROUTE_MAX_SECONDS - max(0, int(pre_route_fixed_seconds))
    if available_seconds <= 0:
        raise RouteOptimizationError(
            "route_day_capacity_exhausted",
            "Dagens fasta aktiviteter lämnar inte plats för en körbar rutt.",
        )
    global_end = route_start + timedelta(seconds=available_seconds)
    breaks = sorted(fixed_breaks, key=lambda item: item["scheduled_at"])
    vehicle: dict[str, Any] = {
        "label": f"owner:{str(owner_user_name).strip().casefold()}",
        "travelMode": "DRIVING",
        "startLocation": {"latitude": start.latitude, "longitude": start.longitude},
        "endLocation": {"latitude": start.latitude, "longitude": start.longitude},
        "startTimeWindows": [{
            "startTime": _utc_text(route_start),
            "endTime": _utc_text(route_start + timedelta(seconds=1)),
        }],
        "loadLimits": {
            "visit_slots": {
                "maxLoad": str(MAX_VISITS),
                "startLoadInterval": {"min": "0", "max": "0"},
            }
        },
        "routeDurationLimit": {"maxDuration": f"{available_seconds}s"},
        "costPerHour": ROUTE_COST_PER_HOUR,
    }
    if available_seconds > QUADRATIC_SOFT_DURATION_BUFFER_SECONDS:
        vehicle["routeDurationLimit"].update({
            "quadraticSoftMaxDuration": (
                f"{available_seconds - QUADRATIC_SOFT_DURATION_BUFFER_SECONDS}s"
            ),
            "costPerSquareHourAfterQuadraticSoftMax": (
                QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR
            ),
        })
    if breaks:
        latest_break_end = max(
            item["scheduled_at"] + timedelta(seconds=int(item["duration_seconds"]))
            for item in breaks
        )
        vehicle["endTimeWindows"] = [{
            "startTime": _utc_text(latest_break_end),
            "endTime": _utc_text(global_end),
        }]
        vehicle["breakRule"] = {"breakRequests": [{
            "earliestStartTime": _utc_text(item["scheduled_at"]),
            "latestStartTime": _utc_text(item["scheduled_at"]),
            "minDuration": f"{int(item['duration_seconds'])}s",
        } for item in breaks]}

    rendered_shipments = []
    for shipment in shipment_list:
        item: dict[str, Any] = {
            "label": f"customer:{shipment['customer_id']}",
            "pickups": [_visit_request(
                shipment,
                global_start=route_start,
                global_end=global_end,
            )],
            "loadDemands": {"visit_slots": {"amount": "1"}},
        }
        if not shipment.get("required"):
            item["penaltyCost"] = priority_penalty(shipment.get("priority_score"))
        rendered_shipments.append(item)

    return {
        "timeout": f"{int(timeout_seconds)}s",
        "solvingMode": solving_mode,
        "searchMode": "CONSUME_ALL_AVAILABLE_TIME",
        "label": f"store-tracker:{run_id}",
        "considerRoadTraffic": True,
        "model": {
            "globalStartTime": _utc_text(route_start),
            "globalEndTime": _utc_text(global_end),
            "shipments": rendered_shipments,
            "vehicles": [vehicle],
        },
        "populatePolylines": False,
        "populateTransitionPolylines": False,
        "allowLargeDeadlineDespiteInterruptionRisk": False,
        "useGeodesicDistances": False,
        "maxValidationErrors": 20,
    }


def request_fingerprint(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _request_input_fingerprint_payload(
    *,
    owner_user_name: str,
    route_date: str,
    route_start: datetime,
    route_mode: str,
    start: TrustedCoordinate,
    shipments: Iterable[Mapping[str, Any]],
    fixed_activities: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    shipment_values = []
    for item in shipments:
        coordinate = item["coordinate"]
        shipment_values.append({
            "customer_id": str(item["customer_id"]),
            "priority_score": clamp_priority_score(item.get("priority_score")),
            "latitude": round(coordinate.latitude, 5),
            "longitude": round(coordinate.longitude, 5),
            "required": bool(item.get("required")),
            "fixed_at": _utc_text(item["fixed_at"]) if item.get("fixed_at") else "",
            "activity_id": str(item.get("activity_id") or ""),
            "revision": int(item.get("revision") or 0),
        })
    fixed_values = [{
        "activity_id": str(item.get("activity_id") or ""),
        "revision": int(item.get("revision") or 0),
        "contact_type": str(item.get("contact_type") or ""),
        "scheduled_at": _utc_text(item["scheduled_at"]),
        "duration_seconds": int(item["duration_seconds"]),
        "status": str(item.get("status") or ""),
    } for item in fixed_activities]
    return {
        "owner_user_name": str(owner_user_name).strip().casefold(),
        "route_date": route_date,
        "route_start": _utc_text(route_start),
        "route_mode": route_mode,
        "start": {
            "latitude": round(start.latitude, 4),
            "longitude": round(start.longitude, 4),
        },
        "shipments": sorted(shipment_values, key=lambda item: item["customer_id"]),
        "fixed_activities": sorted(fixed_values, key=lambda item: item["activity_id"]),
    }


def build_input_fingerprint(**kwargs: Any) -> str:
    """Fingerprint user/business input independently of solver policy."""
    return request_fingerprint(_request_input_fingerprint_payload(**kwargs))


def _request_fingerprint_for_policy(
    *,
    engine_version: str,
    include_quadratic_policy: bool,
    **kwargs: Any,
) -> str:
    constants = {
        "route_cost_per_hour": ROUTE_COST_PER_HOUR,
        "priority_penalty_multiplier": PRIORITY_PENALTY_MULTIPLIER,
        "route_max_seconds": ROUTE_MAX_SECONDS,
        "service_seconds": SERVICE_SECONDS,
        "max_visits": MAX_VISITS,
    }
    if include_quadratic_policy:
        constants.update({
            "quadratic_soft_duration_buffer_seconds": (
                QUADRATIC_SOFT_DURATION_BUFFER_SECONDS
            ),
            "quadratic_soft_duration_cost_per_square_hour": (
                QUADRATIC_SOFT_DURATION_COST_PER_SQUARE_HOUR
            ),
        })
    input_payload = _request_input_fingerprint_payload(**kwargs)
    return request_fingerprint({
        "engine_version": engine_version,
        **input_payload,
        "constants": constants,
    })


def build_request_fingerprint(**kwargs: Any) -> str:
    """Fingerprint input plus the active solver policy for cache isolation."""
    return _request_fingerprint_for_policy(
        engine_version=ROUTE_ENGINE_VERSION,
        include_quadratic_policy=True,
        **kwargs,
    )


def build_legacy_ro_v1_request_fingerprint(**kwargs: Any) -> str:
    """Recreate the deployed ro-v1 fingerprint for exact recovery only."""
    input_payload = _request_input_fingerprint_payload(**kwargs)
    return request_fingerprint({
        "engine_version": "ro-v1",
        **input_payload,
        "constants": {
            "route_cost_per_hour": 1.0,
            "priority_penalty_multiplier": 10.0,
            "route_max_seconds": 25199,
            "service_seconds": 1200,
            "max_visits": 15,
        },
    })


def _parse_time(value: Any) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        raise ValueError("Timestamp lacks timezone")
    return result.astimezone(timezone.utc)


_TRAFFIC_DIAGNOSTIC_METRIC_KEYS = (
    "travelDuration",
    "waitDuration",
    "delayDuration",
    "breakDuration",
    "visitDuration",
    "totalDuration",
    "travelDistanceMeters",
)


def _diagnostic_transition_duration(
    transition: Mapping[str, Any],
    field: str,
    *,
    omitted_is_zero: bool = False,
) -> Decimal | None:
    """Parse one transition duration, optionally defaulting omission to zero.

    The default applies only when the field is absent.  An explicitly present
    malformed value remains invalid so diagnostics never turn bad provider data
    into a false zero.
    """
    if omitted_is_zero and field not in transition:
        return Decimal(0)
    return _diagnostic_duration_decimal(transition.get(field))


def _traffic_infeasibility_diagnostics(
    response: Mapping[str, Any],
    route: Mapping[str, Any],
    *,
    visits: list[Mapping[str, Any]],
    transitions: list[Mapping[str, Any]],
    shipments: list[Mapping[str, Any]],
    vehicle_start: datetime,
    vehicle_end: datetime,
    pre_route_fixed_seconds: Any = 0,
    fixed_breaks: list[Mapping[str, Any]] | None = None,
    timeout_seconds: Any = None,
    solving_mode: str = "DEFAULT_SOLVE",
) -> dict[str, Any]:
    """Return compact response facts without identities or local retiming.

    ``aggregate_timeline_residual_seconds`` is
    total - travel - visit - delay - break.  Per-transition residual is
    total - travel - delay - break.  Neither residual is interpreted as a
    traffic deficit; wait duration remains a separate observed value.
    """
    try:
        fixed_break_list = fixed_breaks if isinstance(fixed_breaks, list) else []
        route_metrics = route.get("metrics")
        route_metrics = route_metrics if isinstance(route_metrics, Mapping) else {}
        compact_metrics: dict[str, Any] = {}
        for key in _TRAFFIC_DIAGNOSTIC_METRIC_KEYS:
            value = route_metrics.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                if math.isfinite(float(value)):
                    compact_metrics[key] = value
            elif isinstance(value, str) and len(value) <= 80:
                compact_metrics[key] = value

        metric_keys = {
            "total": "totalDuration",
            "travel": "travelDuration",
            "visit": "visitDuration",
            "wait": "waitDuration",
            "delay": "delayDuration",
            "break": "breakDuration",
        }
        metric_values = {
            name: _diagnostic_duration_decimal(route_metrics.get(api_key))
            for name, api_key in metric_keys.items()
        }
        aggregate_parts = (
            metric_values["total"],
            metric_values["travel"],
            metric_values["visit"],
            metric_values["delay"],
            metric_values["break"],
        )
        aggregate_residual = None
        if all(value is not None for value in aggregate_parts):
            aggregate_residual = (
                metric_values["total"]
                - metric_values["travel"]
                - metric_values["visit"]
                - metric_values["delay"]
                - metric_values["break"]
            )

        transition_diagnostics = []
        residual_values: list[tuple[int, Decimal]] = []
        negative_wait_count = 0
        for index, transition in enumerate(transitions):
            if not isinstance(transition, Mapping):
                continue
            values = {
                "travel": _diagnostic_transition_duration(
                    transition, "travelDuration"
                ),
                "total": _diagnostic_transition_duration(
                    transition, "totalDuration"
                ),
                "wait": _diagnostic_transition_duration(
                    transition, "waitDuration"
                ),
                "delay": _diagnostic_transition_duration(
                    transition, "delayDuration", omitted_is_zero=True
                ),
                "break": _diagnostic_transition_duration(
                    transition, "breakDuration", omitted_is_zero=True
                ),
            }
            residual = None
            residual_parts = (
                values["total"],
                values["travel"],
                values["delay"],
                values["break"],
            )
            if all(value is not None for value in residual_parts):
                residual = (
                    values["total"]
                    - values["travel"]
                    - values["delay"]
                    - values["break"]
                )
                residual_values.append((index, residual))
            if values["wait"] is not None and values["wait"] < 0:
                negative_wait_count += 1
            start_at = None
            try:
                if isinstance(transition.get("startTime"), str):
                    start_at = _utc_text(_parse_time(transition.get("startTime")))
            except (TypeError, ValueError):
                start_at = None
            traffic_unavailable = transition.get("trafficInfoUnavailable")
            if not isinstance(traffic_unavailable, bool):
                traffic_unavailable = None
            transition_diagnostics.append({
                "transition_index": index,
                "travel_duration_seconds": _diagnostic_decimal_number(
                    values["travel"]
                ),
                "total_duration_seconds": _diagnostic_decimal_number(
                    values["total"]
                ),
                "wait_duration_seconds": _diagnostic_decimal_number(
                    values["wait"]
                ),
                "delay_duration_seconds": _diagnostic_decimal_number(
                    values["delay"]
                ),
                "break_duration_seconds": _diagnostic_decimal_number(
                    values["break"]
                ),
                "transition_residual_seconds": _diagnostic_decimal_number(
                    residual
                ),
                "traffic_info_unavailable": traffic_unavailable,
                "transition_start_at": start_at,
            })

        negative_residuals = [
            (index, value) for index, value in residual_values if value < 0
        ]
        most_negative = min(negative_residuals, key=lambda item: item[1]) if negative_residuals else None
        residual_only = [value for _index, value in residual_values]

        skip_reason_counts: dict[str, int] = {}
        skipped_items = response.get("skippedShipments")
        if isinstance(skipped_items, list):
            for skipped in skipped_items:
                if not isinstance(skipped, Mapping):
                    continue
                reasons = skipped.get("reasons")
                if not isinstance(reasons, list):
                    continue
                for reason in reasons:
                    if not isinstance(reason, Mapping):
                        continue
                    code_value = reason.get("code")
                    if not isinstance(code_value, str):
                        continue
                    code = code_value.strip()
                    if not code or len(code) > 80:
                        continue
                    if code not in skip_reason_counts and len(skip_reason_counts) >= 32:
                        continue
                    skip_reason_counts[code] = skip_reason_counts.get(code, 0) + 1

        normalized_pre_route_seconds = None
        if not isinstance(pre_route_fixed_seconds, bool):
            try:
                normalized_pre_route_seconds = max(0, int(pre_route_fixed_seconds))
            except (TypeError, ValueError, OverflowError):
                normalized_pre_route_seconds = None
        model_route_max_seconds = (
            ROUTE_MAX_SECONDS - normalized_pre_route_seconds
            if normalized_pre_route_seconds is not None
            and normalized_pre_route_seconds <= ROUTE_MAX_SECONDS
            else None
        )
        normalized_timeout = None
        if not isinstance(timeout_seconds, bool):
            try:
                normalized_timeout = int(timeout_seconds)
            except (TypeError, ValueError, OverflowError):
                normalized_timeout = None

        delta = vehicle_end - vehicle_start
        elapsed = (
            Decimal(delta.days * 86400 + delta.seconds)
            + Decimal(delta.microseconds) / Decimal(1_000_000)
        )
        quadratic_diagnostics = (
            quadratic_soft_duration_diagnostics(
                model_route_max_seconds=model_route_max_seconds,
                route_duration_seconds=elapsed,
            )
            if model_route_max_seconds is not None
            else {}
        )
        required_count = sum(
            item.get("required") is True
            for item in shipments
            if isinstance(item, Mapping)
        )
        fixed_visit_count = sum(
            item.get("fixed_at") is not None
            for item in shipments
            if isinstance(item, Mapping)
        )
        expected_transition_count = len(visits) + 1
        return {
            "diagnostic_reason": "traffic_infeasibility",
            "route_engine_version": ROUTE_ENGINE_VERSION,
            "shipment_count": len(shipments),
            "stop_count": len(visits),
            "transition_count": len(transitions),
            "expected_transition_count": expected_transition_count,
            "transition_count_matches_expected": (
                len(transitions) == expected_transition_count
            ),
            "traffic_info_unavailable_count": sum(
                transition.get("trafficInfoUnavailable") is True
                for transition in transitions
                if isinstance(transition, Mapping)
            ),
            "route_metrics": compact_metrics,
            "route_total_duration_seconds": _diagnostic_decimal_number(
                metric_values["total"]
            ),
            "route_travel_duration_seconds": _diagnostic_decimal_number(
                metric_values["travel"]
            ),
            "route_visit_duration_seconds": _diagnostic_decimal_number(
                metric_values["visit"]
            ),
            "route_wait_duration_seconds": _diagnostic_decimal_number(
                metric_values["wait"]
            ),
            "route_delay_duration_seconds": _diagnostic_decimal_number(
                metric_values["delay"]
            ),
            "route_break_duration_seconds": _diagnostic_decimal_number(
                metric_values["break"]
            ),
            "aggregate_timeline_residual_seconds": _diagnostic_decimal_number(
                aggregate_residual
            ),
            "transition_diagnostics": transition_diagnostics,
            "negative_wait_transition_count": negative_wait_count,
            "negative_residual_transition_count": len(negative_residuals),
            "most_negative_transition_index": (
                most_negative[0] if most_negative else None
            ),
            "most_negative_transition_residual_seconds": (
                _diagnostic_decimal_number(most_negative[1])
                if most_negative else None
            ),
            "transition_residual_min_seconds": (
                _diagnostic_decimal_number(min(residual_only))
                if residual_only else None
            ),
            "transition_residual_max_seconds": (
                _diagnostic_decimal_number(max(residual_only))
                if residual_only else None
            ),
            "route_start_at": _utc_text(vehicle_start),
            "route_end_at": _utc_text(vehicle_end),
            "route_elapsed_seconds": _diagnostic_decimal_number(elapsed),
            "route_elapsed_matches_total_duration": (
                elapsed == metric_values["total"]
                if metric_values["total"] is not None else None
            ),
            "required_count": required_count,
            "has_required_visits": required_count > 0,
            "fixed_visit_count": fixed_visit_count,
            "has_fixed_visits": fixed_visit_count > 0,
            "fixed_break_count": len(fixed_break_list),
            "has_fixed_breaks": bool(fixed_break_list),
            "pre_route_fixed_seconds": normalized_pre_route_seconds,
            "consider_road_traffic": True,
            "search_mode": "CONSUME_ALL_AVAILABLE_TIME",
            "solving_mode": str(solving_mode or "DEFAULT_SOLVE"),
            "timeout_seconds": normalized_timeout,
            "absolute_route_max_seconds": ROUTE_MAX_SECONDS,
            "model_route_max_seconds": model_route_max_seconds,
            **quadratic_diagnostics,
            "max_visits": MAX_VISITS,
            "service_duration_seconds": SERVICE_SECONDS,
            "skip_reason_counts": skip_reason_counts,
            "traffic_deficit_calculated": False,
        }
    except Exception:
        # Diagnostics must never replace the classified provider error.
        expected_transition_count = len(visits) + 1
        return {
            "diagnostic_reason": "traffic_infeasibility",
            "transition_count": len(transitions),
            "expected_transition_count": expected_transition_count,
            "transition_count_matches_expected": (
                len(transitions) == expected_transition_count
            ),
            "absolute_route_max_seconds": ROUTE_MAX_SECONDS,
            "traffic_deficit_calculated": False,
        }


def parse_optimize_tours_response(
    response: Mapping[str, Any],
    *,
    shipments: Iterable[Mapping[str, Any]],
    owner_user_name: str,
    route_start: datetime,
    pre_route_fixed_seconds: int = 0,
    fixed_breaks: Iterable[Mapping[str, Any]] = (),
    timeout_seconds: int | None = None,
    solving_mode: str = "DEFAULT_SOLVE",
) -> dict[str, Any]:
    shipment_list = list(shipments)
    fixed_break_list = list(fixed_breaks)
    shipment_labels = [f"customer:{str(item.get('customer_id') or '').strip()}" for item in shipment_list]
    if any(label == "customer:" for label in shipment_labels) or len(set(shipment_labels)) != len(shipment_labels):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    label_indexes = {label: index for index, label in enumerate(shipment_labels)}
    validation_errors_value = response.get("validationErrors")
    if (
        validation_errors_value is not None
        and not isinstance(validation_errors_value, list)
    ):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade ett ogiltigt svar.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    validation_errors = list(validation_errors_value or [])
    if validation_errors:
        raise RouteOptimizationError(
            "route_request_model_invalid",
            "Ruttmodellen kunde inte valideras.",
            502,
            counted_attempt=True,
        )
    routes_value = response.get("routes")
    if routes_value is not None and not isinstance(routes_value, list):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    routes = list(routes_value or [])
    mandatory_indexes = {index for index, item in enumerate(shipment_list) if item.get("required")}
    if len(routes) != 1:
        code = "route_no_feasible_solution" if not routes and not mandatory_indexes else "route_response_invalid"
        raise RouteOptimizationError(code, "Google kunde inte skapa en giltig rutt.", 422, counted_attempt=True)
    route = routes[0]
    if not isinstance(route, Mapping):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    expected_vehicle = f"owner:{str(owner_user_name).strip().casefold()}"
    if route.get("vehicleLabel") != expected_vehicle:
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "vehicle_label_mismatch"},
        )
    vehicle_index = route.get("vehicleIndex")
    if (
        isinstance(vehicle_index, bool)
        or vehicle_index not in (None, 0, "0")
    ):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "vehicle_index_mismatch"},
        )
    traffic_infeasible = route.get("hasTrafficInfeasibilities") is True
    visits_value = route.get("visits")
    if visits_value is not None and not isinstance(visits_value, list):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    visits = list(visits_value or [])
    if not visits and not mandatory_indexes:
        raise RouteOptimizationError(
            "route_no_feasible_solution",
            "Google hittade ingen genomförbar rutt.",
            422,
            counted_attempt=True,
        )
    if len(visits) > MAX_VISITS:
        raise RouteOptimizationError("route_response_invalid", "Google returnerade för många stopp.", 502, counted_attempt=True)
    seen_indexes: set[int] = set()
    stops = []
    for sequence, visit in enumerate(visits, start=1):
        if not isinstance(visit, Mapping):
            raise RouteOptimizationError(
                "route_response_invalid",
                "Google returnerade ett okänt stopp.",
                502,
                counted_attempt=True,
                details={"diagnostic_reason": "shipment_identity_invalid"},
            )
        try:
            index = _response_shipment_index(
                visit,
                index_key="shipmentIndex",
                label_key="shipmentLabel",
                label_indexes=label_indexes,
                shipment_count=len(shipment_list),
            )
        except (TypeError, ValueError):
            raise RouteOptimizationError(
                "route_response_invalid",
                "Google returnerade ett okänt stopp.",
                502,
                counted_attempt=True,
                details={"diagnostic_reason": "shipment_identity_invalid"},
            )
        if (
            index in seen_indexes
            or visit.get("isPickup") is not True
            or visit.get("visitRequestIndex") not in (None, 0)
        ):
            raise RouteOptimizationError(
                "route_response_invalid",
                "Google returnerade ett okänt eller duplicerat stopp.",
                502,
                counted_attempt=True,
                details={"diagnostic_reason": "shipment_identity_invalid"},
            )
        shipment = shipment_list[index]
        seen_indexes.add(index)
        if shipment.get("fixed_at"):
            try:
                actual_start = _parse_time(visit.get("startTime"))
            except (TypeError, ValueError):
                raise RouteOptimizationError("route_response_invalid", "Google returnerade en ogiltig besökstid.", 502, counted_attempt=True)
            delta = abs((actual_start - shipment["fixed_at"].astimezone(timezone.utc)).total_seconds())
            if delta > 15 * 60:
                raise RouteOptimizationError("route_required_visit_time_changed", "Google flyttade ett fast besök utanför dess tidsfönster.", 422, counted_attempt=True)
        stops.append({
            "sequence": sequence,
            "customer_id": str(shipment["customer_id"]),
            "required": bool(shipment.get("required")),
            "planned_activity_id": str(shipment.get("activity_id") or ""),
            "required_activity_ids": [str(shipment.get("activity_id"))] if shipment.get("activity_id") else [],
            "scheduled_at": _utc_text(shipment["fixed_at"]) if shipment.get("fixed_at") else str(visit.get("startTime") or ""),
            "estimated_at": str(visit.get("startTime") or ""),
            "duration_minutes": SERVICE_SECONDS // 60,
            "priority_score": clamp_priority_score(shipment.get("priority_score")),
        })
    skipped_value = response.get("skippedShipments")
    if skipped_value is not None and not isinstance(skipped_value, list):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig skipplista.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    skipped_items = list(skipped_value or [])
    if any(not isinstance(item, Mapping) for item in skipped_items):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig skipplista.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    try:
        skipped_indexes = {
            _response_shipment_index(
                item,
                index_key="index",
                label_key="label",
                label_indexes=label_indexes,
                shipment_count=len(shipment_list),
            )
            for item in skipped_items
        }
    except (TypeError, ValueError):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig skipplista.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    if len(skipped_indexes) != len(skipped_items):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig skipplista.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    if mandatory_indexes - seen_indexes or mandatory_indexes & skipped_indexes:
        raise RouteOptimizationError("route_required_visit_missing", "Ett obligatoriskt besök saknas i rutten.", 422, counted_attempt=True)
    if seen_indexes & skipped_indexes or any(index >= len(shipment_list) for index in skipped_indexes):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig skipplista.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )
    if seen_indexes | skipped_indexes != set(range(len(shipment_list))):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade inte utfall för alla butiker.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "shipment_identity_invalid"},
        )

    transitions_value = route.get("transitions")
    if transitions_value is not None and not isinstance(transitions_value, list):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ofullständig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    transitions = list(transitions_value or [])
    if any(not isinstance(transition, Mapping) for transition in transitions):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ofullständig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    if transitions and len(transitions) != len(visits) + 1:
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ofullständig rutt.",
            502,
            counted_attempt=True,
            details={
                "diagnostic_reason": "transition_count_invalid",
                "transition_count": len(transitions),
                "expected_transition_count": len(visits) + 1,
                "transition_count_matches_expected": False,
            },
        )
    if traffic_infeasible and len(transitions) != len(visits) + 1:
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ofullständig rutt.",
            502,
            counted_attempt=True,
            details={
                "diagnostic_reason": "transition_count_invalid",
                "transition_count": len(transitions),
                "expected_transition_count": len(visits) + 1,
                "transition_count_matches_expected": False,
            },
        )
    try:
        vehicle_start = _parse_time(route["vehicleStartTime"])
        vehicle_end = _parse_time(route["vehicleEndTime"])
    except (KeyError, TypeError, ValueError):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade ogiltiga rutttider.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    returned_breaks_value = route.get("breaks")
    if (
        returned_breaks_value is not None
        and not isinstance(returned_breaks_value, list)
    ):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig fast aktivitet.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    returned_breaks = list(returned_breaks_value or [])
    if any(not isinstance(item, Mapping) for item in returned_breaks):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig fast aktivitet.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    metrics_value = route.get("metrics")
    if metrics_value is not None and not isinstance(metrics_value, Mapping):
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade ogiltiga ruttmått.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "route_structure_invalid"},
        )
    if traffic_infeasible:
        raise RouteOptimizationError(
            "route_traffic_infeasible",
            (
                "Trafiken gör att rutten inte ryms inom dagens fasta tider "
                "och sjutimmarsgräns. Justera planeringen och försök igen."
            ),
            422,
            counted_attempt=True,
            details=_traffic_infeasibility_diagnostics(
                response,
                route,
                visits=visits,
                transitions=transitions,
                shipments=shipment_list,
                vehicle_start=vehicle_start,
                vehicle_end=vehicle_end,
                pre_route_fixed_seconds=pre_route_fixed_seconds,
                fixed_breaks=fixed_break_list,
                timeout_seconds=timeout_seconds,
                solving_mode=solving_mode,
            ),
        )

    available_seconds = ROUTE_MAX_SECONDS - max(0, int(pre_route_fixed_seconds))
    route_seconds = int((vehicle_end - vehicle_start).total_seconds())
    expected_start = route_start.astimezone(timezone.utc)
    if (
        vehicle_start < expected_start
        or vehicle_start > expected_start + timedelta(seconds=1)
        or vehicle_end > expected_start + timedelta(seconds=available_seconds)
        or route_seconds < 0
        or route_seconds > available_seconds
        or route_seconds + pre_route_fixed_seconds >= 25200
    ):
        raise RouteOptimizationError("route_response_invalid", "Rutten överskrider sjutimmarsgränsen.", 502, counted_attempt=True)
    expected_breaks = sorted(fixed_break_list, key=lambda item: item["scheduled_at"])
    if len(returned_breaks) != len(expected_breaks):
        raise RouteOptimizationError("route_response_invalid", "En fast aktivitet saknas i rutten.", 502, counted_attempt=True)
    for expected, actual in zip(expected_breaks, returned_breaks):
        try:
            actual_start = _parse_time(actual["startTime"])
            actual_duration = _duration_seconds(actual.get("duration"))
        except (KeyError, TypeError, ValueError):
            raise RouteOptimizationError("route_response_invalid", "Google returnerade en ogiltig fast aktivitet.", 502, counted_attempt=True)
        if actual_start != expected["scheduled_at"].astimezone(timezone.utc) or actual_duration < int(expected["duration_seconds"]):
            raise RouteOptimizationError("route_response_invalid", "Google flyttade en fast aktivitet.", 502, counted_attempt=True)
    metrics = dict(metrics_value or {})
    travel_seconds = _duration_seconds(metrics.get("travelDuration"))
    wait_seconds = _duration_seconds(metrics.get("waitDuration"))
    break_seconds = _duration_seconds(metrics.get("breakDuration"))
    visit_seconds = _duration_seconds(metrics.get("visitDuration"))
    return_seconds = (
        _duration_seconds(transitions[-1].get("travelDuration"))
        if transitions else 0
    )
    return {
        "stops": stops,
        "summary": {
            "stop_count": len(stops),
            "total_priority_score": sum(item["priority_score"] for item in stops),
            "route_seconds": route_seconds,
            "route_minutes": round(route_seconds / 60, 1),
            "total_minutes": round(route_seconds / 60, 1),
            "travel_seconds": travel_seconds,
            "travel_minutes": round(travel_seconds / 60, 1),
            "drive_minutes": round(travel_seconds / 60, 1),
            "return_drive_minutes": round(return_seconds / 60, 1),
            "wait_seconds": wait_seconds,
            "wait_minutes": round(wait_seconds / 60, 1),
            "break_seconds": break_seconds,
            "break_minutes": round(break_seconds / 60, 1),
            "visit_seconds": visit_seconds,
            "service_minutes": round(visit_seconds / 60, 1),
            "route_end_at": vehicle_end.isoformat(timespec="minutes"),
        },
        "performed_count": len(stops),
        "skipped_count": len(shipment_list) - len(stops),
    }


def load_service_account_credentials(environ: Mapping[str, str] | None = None) -> Credentials:
    environment = os.environ if environ is None else environ
    raw = environment.get("ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS")
    if not raw:
        raise RouteOptimizationError("route_optimization_not_configured", "Ruttoptimeringen är inte konfigurerad.", 503)
    try:
        info = json.loads(raw)
        return Credentials.from_service_account_info(info, scopes=[GOOGLE_OAUTH_SCOPE])
    except (TypeError, ValueError, KeyError):
        raise RouteOptimizationError("route_optimization_not_configured", "Ruttoptimeringens autentisering är ogiltig.", 503)


class RouteOptimizationProvider:
    """One-call provider with token caching and only the prescribed 401 retry."""

    def __init__(self, *, credentials=None, session=None, clock=time.time):
        self._credentials = credentials or load_service_account_credentials()
        self._session = session or requests.Session()
        self._clock = clock
        self._lock = threading.Lock()

    def _token(self, *, force_refresh=False) -> str:
        with self._lock:
            expiry = getattr(self._credentials, "expiry", None)
            if expiry is not None and expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            near_expiry = expiry is None or expiry.timestamp() - self._clock() < 120
            if force_refresh or not getattr(self._credentials, "token", None) or near_expiry:
                try:
                    self._credentials.refresh(GoogleAuthRequest())
                except Exception as exc:
                    raise RouteOptimizationError(
                        "route_optimization_auth_failed",
                        "Ruttoptimeringen kunde inte autentiseras.",
                        503,
                        counted_attempt=False,
                    ) from exc
            return str(self._credentials.token)

    def optimize(self, *, project: str, body: Mapping[str, Any], timeout_seconds: int) -> tuple[dict[str, Any], int]:
        url = GOOGLE_OPTIMIZE_TOURS_URL.format(project=project)
        for attempt in range(2):
            token = self._token(force_refresh=attempt == 1)
            try:
                response = self._session.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                        "X-Server-Timeout": str(int(timeout_seconds) + 15),
                    },
                    json=dict(body),
                    timeout=(10, int(timeout_seconds) + 30),
                )
            except requests.Timeout as exc:
                raise RouteOptimizationError("route_optimization_timeout", "Ruttoptimeringen tog för lång tid.", 504, counted_attempt=True) from exc
            except requests.ConnectionError as exc:
                raise RouteOptimizationError("route_optimization_connection_failed", "Ruttoptimeringen kunde inte nås.", 503, counted_attempt=True) from exc
            except requests.RequestException as exc:
                raise RouteOptimizationError("route_optimization_connection_failed", "Ruttoptimeringen kunde inte nås.", 503, counted_attempt=True) from exc
            if response.status_code == 401 and attempt == 0:
                continue
            if response.status_code == 429:
                raise RouteOptimizationError("route_optimization_provider_quota", "Google Route Optimization är tillfälligt begränsad.", 503, provider_status=429, counted_attempt=False)
            if response.status_code == 408:
                raise RouteOptimizationError("route_optimization_timeout", "Ruttoptimeringen tog för lång tid.", 504, provider_status=408, counted_attempt=True)
            if response.status_code in {400, 401, 403}:
                code = "route_optimization_auth_failed" if response.status_code in {401, 403} else "route_optimization_request_rejected"
                raise RouteOptimizationError(code, "Ruttoptimeringen kunde inte genomföras.", 503, provider_status=response.status_code, counted_attempt=False)
            if response.status_code >= 500:
                raise RouteOptimizationError("route_optimization_provider_failed", "Google Route Optimization är tillfälligt otillgänglig.", 503, provider_status=response.status_code, counted_attempt=True)
            if not 200 <= response.status_code < 300:
                raise RouteOptimizationError("route_optimization_provider_failed", "Ruttoptimeringen kunde inte genomföras.", 503, provider_status=response.status_code, counted_attempt=False)
            try:
                payload = response.json()
            except ValueError as exc:
                raise RouteOptimizationError("route_response_invalid", "Google returnerade ett ogiltigt svar.", 502, provider_status=response.status_code, counted_attempt=True) from exc
            if not isinstance(payload, dict):
                raise RouteOptimizationError("route_response_invalid", "Google returnerade ett ogiltigt svar.", 502, provider_status=response.status_code, counted_attempt=True)
            return payload, response.status_code
        raise RouteOptimizationError("route_optimization_auth_failed", "Ruttoptimeringen kunde inte autentiseras.", 503, provider_status=401, counted_attempt=False)
