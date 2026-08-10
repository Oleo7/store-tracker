"""Google Route Optimization v1 request, provider and response validation.

This module is deliberately independent from Flask and Google Sheets.  The web
application owns access control, snapshots, quota accounting and persistence;
this module owns the deterministic optimization contract only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
import threading
import time
from typing import Any, Iterable, Mapping

import requests
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.service_account import Credentials


ROUTE_ENGINE_VERSION = "ro-v1"
ROUTE_COST_PER_HOUR = 1.0
PRIORITY_PENALTY_MULTIPLIER = 10.0
ROUTE_MAX_SECONDS = 25199
SERVICE_SECONDS = 1200
MAX_VISITS = 15
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


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Route timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _duration_seconds(value: Any) -> int:
    text = str(value or "0s").strip()
    if not text.endswith("s"):
        raise ValueError("Invalid duration")
    return max(0, int(round(float(text[:-1] or 0))))


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


def build_request_fingerprint(
    *,
    owner_user_name: str,
    route_date: str,
    route_start: datetime,
    route_mode: str,
    start: TrustedCoordinate,
    shipments: Iterable[Mapping[str, Any]],
    fixed_activities: Iterable[Mapping[str, Any]],
) -> str:
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
    return request_fingerprint({
        "engine_version": ROUTE_ENGINE_VERSION,
        "owner_user_name": str(owner_user_name).strip().casefold(),
        "route_date": route_date,
        "route_start": _utc_text(route_start),
        "route_mode": route_mode,
        "start": {
            "latitude": round(start.latitude, 4),
            "longitude": round(start.longitude, 4),
        },
        "constants": {
            "route_cost_per_hour": ROUTE_COST_PER_HOUR,
            "priority_penalty_multiplier": PRIORITY_PENALTY_MULTIPLIER,
            "route_max_seconds": ROUTE_MAX_SECONDS,
            "service_seconds": SERVICE_SECONDS,
            "max_visits": MAX_VISITS,
        },
        "shipments": sorted(shipment_values, key=lambda item: item["customer_id"]),
        "fixed_activities": sorted(fixed_values, key=lambda item: item["activity_id"]),
    })


def _parse_time(value: Any) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    result = datetime.fromisoformat(text)
    if result.tzinfo is None:
        raise ValueError("Timestamp lacks timezone")
    return result.astimezone(timezone.utc)


def parse_optimize_tours_response(
    response: Mapping[str, Any],
    *,
    shipments: Iterable[Mapping[str, Any]],
    owner_user_name: str,
    route_start: datetime,
    pre_route_fixed_seconds: int = 0,
    fixed_breaks: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    shipment_list = list(shipments)
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
    validation_errors = list(response.get("validationErrors") or [])
    if validation_errors:
        raise RouteOptimizationError(
            "route_request_model_invalid",
            "Ruttmodellen kunde inte valideras.",
            502,
            counted_attempt=True,
        )
    routes = list(response.get("routes") or [])
    mandatory_indexes = {index for index, item in enumerate(shipment_list) if item.get("required")}
    if len(routes) != 1:
        code = "route_no_feasible_solution" if not routes and not mandatory_indexes else "route_response_invalid"
        raise RouteOptimizationError(code, "Google kunde inte skapa en giltig rutt.", 422, counted_attempt=True)
    route = routes[0]
    expected_vehicle = f"owner:{str(owner_user_name).strip().casefold()}"
    if route.get("vehicleLabel") != expected_vehicle:
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "vehicle_label_mismatch"},
        )
    if route.get("hasTrafficInfeasibilities") is True:
        raise RouteOptimizationError(
            "route_response_invalid",
            "Google returnerade en ogiltig rutt.",
            502,
            counted_attempt=True,
            details={"diagnostic_reason": "traffic_infeasibility"},
        )
    visits = list(route.get("visits") or [])
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
    skipped_items = list(response.get("skippedShipments") or [])
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

    try:
        vehicle_start = _parse_time(route["vehicleStartTime"])
        vehicle_end = _parse_time(route["vehicleEndTime"])
    except (KeyError, TypeError, ValueError):
        raise RouteOptimizationError("route_response_invalid", "Google returnerade ogiltiga rutttider.", 502, counted_attempt=True)
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
    transitions = list(route.get("transitions") or [])
    if transitions and len(transitions) != len(visits) + 1:
        raise RouteOptimizationError("route_response_invalid", "Google returnerade en ofullständig rutt.", 502, counted_attempt=True)
    returned_breaks = list(route.get("breaks") or [])
    expected_breaks = sorted(fixed_breaks, key=lambda item: item["scheduled_at"])
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
    metrics = dict(route.get("metrics") or {})
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
