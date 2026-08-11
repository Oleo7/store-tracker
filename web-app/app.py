from flask import (
    Flask,
    Response,
    g,
    has_request_context,
    jsonify,
    request,
    send_file,
    send_from_directory,
    session,
)
from flask_cors import CORS
from contextlib import contextmanager
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from urllib.parse import unquote
from datetime import datetime, date, time as datetime_time, timedelta, timezone
from collections import defaultdict
import copy
from io import BytesIO
from queue import Empty, Full, Queue
import os
import json
import hashlib
import math
import re
import requests
import shlex
import sys
import threading
import time
import unicodedata
import uuid
from zoneinfo import ZoneInfo
from zipfile import ZIP_DEFLATED, ZipFile
from xml.sax.saxutils import escape as xml_escape
from dotenv import load_dotenv
from gspread.utils import rowcol_to_a1
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from requests.exceptions import ConnectionError as RequestsConnectionError
from priority import (
    SCORE_VERSION,
    apply_workflow_suppressions,
    build_contact_features,
    build_order_features,
    build_priority_customers,
    normalize_customer_key,
)
from contact_channel import recommend_contact_channel
from route_proposal import (
    Coordinate,
    GoogleRoutesTravelTimeProvider,
    MAX_ROUTE_STOPS,
    MAX_TOTAL_SECONDS,
    RouteCandidate,
    RouteProposalError,
    SERVICE_SECONDS_PER_STOP,
    TravelTimeConfigurationError,
    anchor_aware_preselect_candidates,
    calculate_route_proposal,
    seconds_to_minutes,
)
from route_optimization import (
    MAX_VISITS as ROUTE_OPTIMIZATION_MAX_VISITS,
    ROUTE_ENGINE_VERSION,
    ROUTE_MAX_SECONDS as ROUTE_OPTIMIZATION_MAX_SECONDS,
    SERVICE_SECONDS as ROUTE_OPTIMIZATION_SERVICE_SECONDS,
    RouteOptimizationError,
    RouteOptimizationProvider,
    TrustedCoordinate,
    build_optimize_tours_request,
    build_request_fingerprint as build_route_optimization_fingerprint,
    coordinate_quality as route_coordinate_quality,
    parse_optimize_tours_response,
)
from reminder_email import (
    EMAIL_PROPOSAL_PRODUCT_SETTINGS,
    EMAIL_PROPOSAL_TEMPLATE_FIELDS,
    EMAIL_PROPOSAL_TYPES,
    EMAIL_EVENTS_COLUMNS,
    EMAIL_MESSAGES_COLUMNS,
    EMAIL_RECIPIENTS_COLUMNS,
    SETTINGS_COLUMNS,
    USER_COLUMNS,
    brevo_event_time,
    build_email_proposal_copy,
    build_email_proposal_template_defaults,
    build_new_customer_order_rows,
    build_reactivation_order_rows,
    build_settings_product_catalog,
    canonicalize_proposal_order_rows,
    build_latest_order_context,
    classify_customer_relationship,
    classify_clicked_url,
    count_unique_order_customers,
    first_name,
    email_event_key,
    is_valid_email,
    is_yes,
    normalize_brevo_event,
    normalize_email,
    normalize_message_id,
    normalize_proposal_type,
    round_store_count_to_ten,
    render_email_proposal,
    recipient_greeting_name,
    safe_http_url,
    split_email_values,
    stockholm_now,
    stockholm_time_text,
    stockholm_today,
)
from planning_suggestions import (
    PlanningSuggestionService,
    SCORE_EVENT_COLUMNS,
    SCORE_EVENTS_SHEET,
    SuggestionError,
    build_phase1_stub_candidates,
    decision_context_hash,
    deterministic_suggestion_id,
    mutation_fingerprint as suggestion_mutation_fingerprint,
    public_suggestion,
)
from sheets_availability import SheetReadCache, read_with_retry

load_dotenv()


LOCAL_SESSION_SECRET = "store-tracker-local-session"
PILOT_ENVIRONMENTS = {"pilot", "prod", "production", "staging"}
PILOT_LOCK_ERROR = (
    "Planning uses a process-local write lock. Production pilot requires "
    "exactly one worker and one application instance."
)


def application_environment(environ=None):
    environment = os.environ if environ is None else environ
    return str(
        environment.get("APP_ENV")
        or environment.get("FLASK_ENV")
        or environment.get("ENVIRONMENT")
        or "development"
    ).strip().casefold()


def _positive_int(value):
    try:
        parsed = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 1 else None


def _gunicorn_worker_count(value):
    try:
        parts = shlex.split(str(value or ""))
    except ValueError:
        return None
    for index, part in enumerate(parts):
        if part in {"--workers", "-w"} and index + 1 < len(parts):
            return _positive_int(parts[index + 1])
        if part.startswith("--workers="):
            return _positive_int(part.split("=", 1)[1])
    return None


def planning_lock_health(environ=None):
    environment = os.environ if environ is None else environ
    app_env = application_environment(environment)
    configured_worker_count = _positive_int(
        environment.get("WEB_CONCURRENCY")
    )
    gunicorn_worker_count = _gunicorn_worker_count(
        environment.get("GUNICORN_CMD_ARGS")
    )
    process_worker_count = _gunicorn_worker_count(
        shlex.join(sys.argv[1:])
    )
    worker_values = {
        value
        for value in (
            configured_worker_count,
            gunicorn_worker_count,
            process_worker_count,
        )
        if value is not None
    }
    distributed_lock_configured = bool(
        str(
            environment.get("PLANNING_DISTRIBUTED_LOCK_URL") or ""
        ).strip()
    )
    instance_count = _positive_int(
        environment.get("APP_INSTANCE_COUNT")
        or environment.get("REPLICA_COUNT")
    )

    if len(worker_values) > 1:
        worker_count = max(worker_values)
        reason = "conflicting_worker_configuration"
    elif worker_values:
        worker_count = next(iter(worker_values))
        reason = ""
    elif app_env in PILOT_ENVIRONMENTS:
        worker_count = None
        reason = "worker_count_unknown"
    else:
        worker_count = 1
        reason = ""

    if distributed_lock_configured:
        reason = "distributed_lock_not_implemented"
    elif reason == "conflicting_worker_configuration":
        pass
    elif worker_count is not None and worker_count > 1:
        reason = "multiple_workers_without_distributed_lock"
    elif app_env in PILOT_ENVIRONMENTS and instance_count is None:
        reason = "instance_count_unknown"
    elif instance_count is not None and instance_count > 1:
        reason = "multiple_instances_without_distributed_lock"

    safe = not reason
    return {
        "mode": "process_local",
        "worker_count": worker_count,
        "instance_count": instance_count,
        "safe": safe,
        "reason": reason,
        "distributed_lock_configured": distributed_lock_configured,
    }


def validate_pilot_startup(environ=None):
    environment = os.environ if environ is None else environ
    if application_environment(environment) not in PILOT_ENVIRONMENTS:
        return planning_lock_health(environment)
    health_state = planning_lock_health(environment)
    if not health_state["safe"]:
        raise RuntimeError(
            f"{PILOT_LOCK_ERROR} ({health_state['reason']})"
        )
    return health_state


def resolve_sheet_id(environ=None):
    environment = os.environ if environ is None else environ
    app_env = application_environment(environment)
    production_key = str(
        environment.get("PRODUCTION_SHEET_KEY") or ""
    ).strip()
    development_key = str(
        environment.get("SHEET_KEY") or ""
    ).strip()
    staging_key = str(
        environment.get("STAGING_SHEET_KEY") or ""
    ).strip()
    if app_env == "staging":
        if not staging_key:
            raise RuntimeError(
                "STAGING_SHEET_KEY must be configured in staging; "
                "staging never falls back to the production Sheet."
            )
        production_comparison_key = (
            production_key or development_key
        )
        if (
            production_comparison_key
            and staging_key == production_comparison_key
        ):
            raise RuntimeError(
                "STAGING_SHEET_KEY must not equal the production Sheet key."
            )
        return staging_key
    if app_env in {"pilot", "prod", "production"}:
        if not production_key:
            raise RuntimeError(
                "PRODUCTION_SHEET_KEY must be configured in production; "
                "production never falls back to SHEET_KEY."
            )
        return production_key
    return production_key or development_key


def resolve_flask_secret_key(environ=None):
    environment = os.environ if environ is None else environ
    configured = str(environment.get("FLASK_SECRET_KEY") or "").strip()
    render_deployment = str(
        environment.get("RENDER") or ""
    ).strip().casefold() in {"1", "true", "yes", "on"}
    environment_name = application_environment(environment)
    if not configured and (
        render_deployment or environment_name in PILOT_ENVIRONMENTS
    ):
        raise RuntimeError(
            "FLASK_SECRET_KEY must be configured for production deployments."
        )
    return configured or LOCAL_SESSION_SECRET


app = Flask(__name__)
validate_pilot_startup()
app.config.update(
    SECRET_KEY=resolve_flask_secret_key(),
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=(
        os.environ.get("RENDER", "").strip().lower() == "true"
        or application_environment() in PILOT_ENVIRONMENTS
    ),
)
CORS(app, supports_credentials=True)


PERFORMANCE_ENDPOINTS = {
    "/session",
    "/customers",
    "/customer-insights",
    "/followup-insights",
    "/planning/activities",
    "/planning/suggestions",
    "/planning/route-preview",
    "/customers/<customer_name>/stats",
}
PERFORMANCE_SHEETS = {
    "customers_enriched",
    "sales_activities",
    "order_rows",
    "email_messages",
    "email_recipients",
    "email_events",
    "planned_activities",
    "planning_suggestions",
    "score_events",
    "users",
}


def performance_logging_enabled(environ=None):
    environment = os.environ if environ is None else environ
    return str(environment.get("PERFORMANCE_LOGGING_ENABLED") or "").strip().casefold() in {
        "1", "true", "yes", "y", "on",
    }


def _performance_endpoint():
    rule = str(request.url_rule) if request.url_rule is not None else request.path
    return rule if rule in PERFORMANCE_ENDPOINTS else ""


def _performance_sheet_step(sheet):
    title = str(getattr(sheet, "title", "") or "").strip()
    safe_title = title if title in PERFORMANCE_SHEETS else "other"
    return f"sheets.read.{safe_title}"


def record_performance_step(step, started_at, row_count=None):
    if not performance_logging_enabled() or not has_request_context():
        return
    if not getattr(g, "performance_request_id", ""):
        return
    g.performance_steps.append({
        "step": str(step),
        "duration_ms": round((time.perf_counter() - started_at) * 1000, 1),
        "row_count": row_count,
    })


def record_google_sheet_read(cache_hit):
    if not cache_hit and has_request_context() and getattr(
        g, "performance_request_id", ""
    ):
        g.google_sheets_read_count += 1


@contextmanager
def performance_step(step):
    started_at = time.perf_counter()
    measurement = {"row_count": None}
    try:
        yield measurement
    finally:
        record_performance_step(step, started_at, measurement["row_count"])


@app.before_request
def start_performance_measurement():
    if not performance_logging_enabled() or not _performance_endpoint():
        return None
    g.performance_request_id = uuid.uuid4().hex
    g.performance_started_at = time.perf_counter()
    g.performance_steps = []
    g.google_sheets_read_count = 0
    return None


@app.after_request
def finalize_response(response):
    if request.endpoint == "images":
        response.headers["Cache-Control"] = "public, max-age=3600"
    elif request.endpoint == "index":
        response.headers["Cache-Control"] = "no-cache"
    elif request.endpoint == "get_session" or response.is_json:
        response.headers["Cache-Control"] = "no-store"

    request_id = getattr(g, "performance_request_id", "")
    if not request_id:
        return response

    endpoint = _performance_endpoint()
    total_ms = round(
        (time.perf_counter() - g.performance_started_at) * 1000,
        1,
    )
    response_size = response.calculate_content_length()
    common = {
        "event": "performance",
        "request_id": request_id,
        "endpoint": endpoint,
        "status_code": response.status_code,
        "total_ms": total_ms,
        "response_size_bytes": response_size,
        "google_sheets_read_count": getattr(
            g, "google_sheets_read_count", 0
        ),
    }
    entries = [{**common, "step": "total", "duration_ms": total_ms, "row_count": None}]
    entries.extend({**common, **step} for step in g.performance_steps)
    for entry in entries:
        app.logger.info(json.dumps(entry, separators=(",", ":"), sort_keys=True))
    return response


@app.route("/health", methods=["GET"])
def health():
    lock_health = planning_lock_health()
    route_health = route_optimization_configuration_health()
    safe = bool(lock_health["safe"] and route_health["safe"])
    status = 200 if safe else 503
    return jsonify({
        "ok": safe,
        "mode": lock_health["mode"],
        "worker_count": lock_health["worker_count"],
        "safe": safe,
        "reason": lock_health["reason"] or route_health["reason"],
        "planning_write_lock": lock_health,
        "route_optimization": route_health,
    }), status

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = resolve_sheet_id()
IMAGE_DIR = os.path.abspath(os.path.join(app.root_path, "..", "images"))

CUSTOMER_COLUMNS = ["customer", "cancelled_flag", "sales_person", "customer_segment",
                    "customer_reference", "customer_id", "customer_number", "name", "phone", "email",
                    "email_last_order", "comment"]

ORDER_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer", "placedBy", "buyerEmail",
                 "placedAs", "Customer Reference",
                 "Buyer number", "Customer number", "Logistics number", "Address", "Number",
                 "Postal code", "City", "Country", "Phone number", "SKU", "Product", "Weight",
                 "Quantity", "Total weight", "Unit", "Total (Pre-discount)", "Product Discount",
                 "Total", "Currency", "Order Discount (Amount)", "Order Discount (%)", "Batch",
                 "customer_id"]
ORDER_REQUIRED_COLUMNS = ["Reference", "Order date", "Delivery date", "Customer",
                          "Quantity", "Total", "Currency"]

FREEZER_COLUMNS = ["Franui", "Schufrulade", "Boujee", "polarbar", "none"]
FREEZER_SUMMARY_ROWS = [
    {"field": "Franui", "label": "Franui"},
    {"field": "Schufrulade", "label": "Schufrulade"},
    {"field": "Boujee", "label": "Boujee"},
    {"field": "polarbar", "label": "Polarbär"},
    {"field": "none", "label": "Ingen"},
]
CONTACT_LOG_FREEZER_LABELS = {
    "Franui": "Franui",
    "Schufrulade": "Schufrulade",
    "Boujee": "Boujee",
    "polarbar": "Polarbär",
    "none": "Ingen",
}
CONTACT_LOG_COLUMNS = [
    "Datum",
    "Ansvarig",
    "Kund",
    "Kanal",
    "Resultat",
    "Kommentar",
    "Nästa uppföljning",
    "I frysdisken",
]

CONTACT_COLUMNS = ["date_time", "sales_person", "customer", "customer_id", "contact_channel", "result",
                   "comment", "customer_contact_person", "follow_up_date",
                   *FREEZER_COLUMNS, "email_id", "contact_id", "planned_activity_id"]
CONTACT_REQUIRED_COLUMNS = ["date_time", "sales_person", "customer", "contact_channel",
                            "result", "comment", "customer_contact_person", "follow_up_date"]

EMAIL_MESSAGES_SHEET = "email_messages"
EMAIL_RECIPIENTS_SHEET = "email_recipients"
EMAIL_EVENTS_SHEET = "email_events"
USERS_SHEET = "users"
SETTINGS_SHEET = "settings"
# LEGACY/ROLLBACK COMPATIBILITY: the list-view proposal flow is retired.
# Keep this storage and its endpoints until a separate removal migration.
ROUTE_PROPOSALS_SHEET = "route_proposals"
ROUTE_PROPOSAL_COLUMNS = [
    "route_date",
    "user_name",
    "user_display_name",
    "generated_at",
    "payload_json",
]
ROUTE_OPTIMIZATION_RUNS_SHEET = "route_optimization_runs"
ROUTE_OPTIMIZATION_RUN_COLUMNS = [
    "run_id",
    "actor_user_name",
    "user_name",
    "usage_iso_week",
    "route_date",
    "client_request_id",
    "request_fingerprint",
    "engine_version",
    "status",
    "counted_attempt",
    "started_at",
    "completed_at",
    "timeout_seconds",
    "shipment_count",
    "required_count",
    "performed_count",
    "skipped_count",
    "excluded_untrusted_coordinates",
    "google_request_label",
    "http_status",
    "error_code",
    "result_payload_json",
]
PLANNED_ACTIVITIES_SHEET = "planned_activities"
PLANNED_ACTIVITY_COLUMNS = [
    "planned_activity_id",
    "user_name",
    "sales_person",
    "customer_id",
    "customer_key",
    "customer_row",
    "customer_number",
    "customer",
    "contact_type",
    "scheduled_at",
    "duration_minutes",
    "time_is_estimated",
    "note",
    "status",
    "source",
    "source_contact_id",
    "completed_contact_id",
    "route_group_id",
    "route_sequence",
    "client_request_id",
    "create_fingerprint",
    "last_mutation_request_id",
    "last_mutation_fingerprint",
    "revision",
    "created_at",
    "updated_at",
    "source_suggestion_id",
    "source_trigger_key",
    "recommended_contact_type",
]
PLANNING_CONTACT_TYPES = {"visit", "phone", "email"}
PLANNING_CONTACT_TYPE_LABELS = {
    "visit": "Besök",
    "phone": "Telefon",
    "email": "Mejl",
}
PLANNING_CONTACT_DURATIONS = {
    "visit": 20,
    "phone": 10,
    "email": 10,
}
PLANNING_STATUSES = {"planned", "completed", "skipped", "cancelled"}
PLANNING_SOURCES = {"manual", "follow_up", "route", "system_suggestion"}
PLANNING_PREVIEW_MAX_AGE_SECONDS = 30 * 60
PLANNING_ROUTE_START_HOUR = 9
PLANNING_ROUTE_CONFLICT_MINUTES = 15
STOCKHOLM_ZONE = ZoneInfo("Europe/Stockholm")
BREVO_SEND_URL = "https://api.brevo.com/v3/smtp/email"
BREVO_EVENTS_URL = "https://api.brevo.com/v3/smtp/statistics/events"
EMAIL_SEND_MODE = (os.environ.get("EMAIL_SEND_MODE") or "test").strip().casefold()
EMAIL_TEST_RECIPIENT = (os.environ.get("EMAIL_TEST_RECIPIENT") or "olle@eatpolarbar.com").strip()
BREVO_RECONCILE_INTERVAL_SECONDS = max(
    60, int(os.environ.get("BREVO_RECONCILE_INTERVAL_SECONDS") or 900)
)
BREVO_RECONCILE_DAYS = max(1, min(30, int(os.environ.get("BREVO_RECONCILE_DAYS") or 3)))
BREVO_RECONCILE_MAX_RECIPIENTS = max(
    1, min(500, int(os.environ.get("BREVO_RECONCILE_MAX_RECIPIENTS") or 100))
)
EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS = 60
EMAIL_PROPOSAL_GRACE_DAYS = 7
EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS = 7
EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS = 10
EMAIL_ORDER_ATTRIBUTION_DAYS = 10
EMAIL_CLICK_FOLLOWUP_WAIT_DAYS = 3
EMAIL_OPEN_FOLLOWUP_WAIT_DAYS = 10
PLANNING_SUGGESTION_PREVIEW_DEFAULT = 10
ANCHOR_ROUTE_CANDIDATE_LIMIT = 35
# Backward-compatible names used by older tests and integrations.
REMINDER_EMAIL_GRACE_DAYS = EMAIL_PROPOSAL_GRACE_DAYS
REMINDER_EMAIL_CONTACT_COOLDOWN_DAYS = EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS
REMINDER_EMAIL_SENT_COOLDOWN_DAYS = EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS
_active_send_ids = set()
_active_send_lock = threading.Lock()
_brevo_event_queue = Queue(maxsize=1000)
_brevo_worker_start_lock = threading.Lock()
_brevo_workers_started = False
_brevo_processing_lock = threading.Lock()
_brevo_reconcile_lock = threading.Lock()
_email_sheets_cache = None
_email_sheets_cache_lock = threading.Lock()
_settings_write_lock = threading.Lock()
_worksheet_append_lock = threading.RLock()
_route_provider_lock = threading.Lock()
_route_provider = None
_route_provider_config = None
_route_proposal_daily_lock = threading.RLock()
_route_optimization_run_lock = threading.RLock()
_route_optimization_provider_lock = threading.Lock()
_route_optimization_provider = None
_route_optimization_provider_config = None
_planning_write_lock = threading.RLock()


READ_CACHE_TITLES = {
    "customers_enriched",
    "order_rows",
    "sales_activities",
    "users",
    "email_messages",
    "email_recipients",
    "email_events",
    "planned_activities",
    "planning_suggestions",
    "score_events",
    ROUTE_OPTIMIZATION_RUNS_SHEET,
}


def route_engine_name(environ=None):
    environment = os.environ if environ is None else environ
    return str(environment.get("ROUTE_ENGINE") or "legacy").strip().casefold()


def route_optimization_configuration_health(environ=None):
    environment = os.environ if environ is None else environ
    engine = route_engine_name(environment)
    if engine not in {"legacy", "route_optimization"}:
        return {
            "safe": False,
            "engine": engine,
            "reason": "invalid_route_engine",
        }
    if engine == "legacy":
        return {"safe": True, "engine": engine, "reason": ""}
    missing = []
    if not str(environment.get("ROUTE_OPTIMIZATION_PROJECT") or "").strip():
        missing.append("ROUTE_OPTIMIZATION_PROJECT")
    if not str(environment.get("ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS") or "").strip():
        missing.append("ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS")
    return {
        "safe": not missing,
        "engine": engine,
        "reason": "" if not missing else "route_optimization_not_configured",
        "missing": missing,
    }


def route_optimization_int_setting(name, default, minimum=1, maximum=1000):
    try:
        return max(minimum, min(maximum, int(os.environ.get(name) or default)))
    except (TypeError, ValueError):
        return default


def route_optimization_provider():
    global _route_optimization_provider, _route_optimization_provider_config
    config = (
        str(os.environ.get("ROUTE_OPTIMIZATION_GOOGLE_CREDENTIALS") or ""),
    )
    with _route_optimization_provider_lock:
        if _route_optimization_provider is None or _route_optimization_provider_config != config:
            _route_optimization_provider = RouteOptimizationProvider()
            _route_optimization_provider_config = config
        return _route_optimization_provider


def sheets_read_cache_ttl(environ=None):
    environment = os.environ if environ is None else environ
    try:
        return max(1.0, min(60.0, float(
            environment.get("SHEETS_READ_CACHE_TTL_SECONDS") or 12
        )))
    except (TypeError, ValueError):
        return 12.0


_sheet_read_cache = SheetReadCache(ttl_seconds=sheets_read_cache_ttl())
_priority_snapshot_condition = threading.Condition(threading.RLock())
_priority_snapshot_entry = None
_priority_snapshot_loading = set()


_spreadsheet_cache = None


def checkbox_to_sheet_value(value):
    return "1" if str(value).strip().lower() in {"1", "true", "yes", "on"} else ""


def is_checked_value(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def text_to_sheet_value(value, max_length=None):
    text = str(value or "").strip()
    return text[:max_length] if max_length is not None else text


def merge_worksheet_cell_value(column, current, candidate):
    if column in FREEZER_COLUMNS:
        return "1" if is_checked_value(current) or is_checked_value(candidate) else ""

    current_text = str(current or "").strip()
    return current if current_text else candidate


def ensure_customer_name_column(sheet, headers):
    if "name" in headers:
        return headers
    if "phone" not in headers:
        return headers

    original_headers = list(headers)
    insert_at = original_headers.index("phone") + 1
    sheet.insert_cols([["name"]], col=insert_at)
    invalidate_sheet_for_write(sheet)
    return original_headers[:insert_at - 1] + ["name"] + original_headers[insert_at - 1:]


def ensure_worksheet_columns(sheet, headers, columns):
    normalized_headers = [str(header).strip() for header in headers]
    for column in columns:
        if column in normalized_headers:
            continue
        target_column = len(normalized_headers) + 1
        grid_columns = getattr(sheet, "col_count", 0) or 0
        if grid_columns and grid_columns < target_column:
            sheet.resize(cols=target_column)
        sheet.update_cell(1, target_column, column)
        invalidate_sheet_for_write(sheet)
        normalized_headers.append(column)
    return normalized_headers


def ensure_unique_worksheet_columns(sheet, headers, columns):
    normalized_headers = [str(header).strip() for header in headers]
    duplicate_groups = {
        column: [idx for idx, header in enumerate(normalized_headers) if header == column]
        for column in columns
    }
    duplicate_groups = {column: indexes for column, indexes in duplicate_groups.items() if len(indexes) > 1}
    if not duplicate_groups:
        return normalized_headers

    try:
        rows = read_with_retry(
            sheet.get_all_values,
            on_retry=_log_sheet_read_retry(f"schema.{sheet.title}"),
        )
        if rows:
            for column, indexes in duplicate_groups.items():
                primary_idx = indexes[0]
                merged_values = []
                primary_values = []
                for row in rows[1:]:
                    merged = ""
                    for idx in indexes:
                        value = row[idx] if idx < len(row) else ""
                        merged = merge_worksheet_cell_value(column, merged, value)
                    current = row[primary_idx] if primary_idx < len(row) else ""
                    merged_values.append([merged])
                    primary_values.append(current)

                if merged_values and any(value[0] != primary for value, primary in zip(merged_values, primary_values)):
                    range_name = f"{rowcol_to_a1(2, primary_idx + 1)}:{rowcol_to_a1(len(rows), primary_idx + 1)}"
                    sheet.update(merged_values, range_name=range_name)

        duplicate_indexes = sorted(
            {idx for indexes in duplicate_groups.values() for idx in indexes[1:]},
            reverse=True,
        )
        for idx in duplicate_indexes:
            sheet.delete_columns(idx + 1)
            invalidate_sheet_for_write(sheet)
            del normalized_headers[idx]
    except Exception as exc:
        app.logger.warning("Could not deduplicate worksheet columns for %s: %s", sheet.title, exc)
        return normalized_headers

    return normalized_headers


def ensure_contact_worksheet_schema(sheet):
    with _planning_write_lock:
        headers = ensure_worksheet_columns(
            sheet,
            read_with_retry(
                lambda: sheet.row_values(1),
                on_retry=_log_sheet_read_retry("schema.sales_activities"),
            ),
            CONTACT_COLUMNS,
        )
        return ensure_unique_worksheet_columns(
            sheet,
            headers,
            FREEZER_COLUMNS,
        )


def build_worksheet_row(headers, row_data, single_value_columns=None):
    single_value_columns = set(single_value_columns or [])
    seen = set()
    row = []
    for header in headers:
        if header in single_value_columns and header in seen:
            row.append("")
            continue
        row.append(row_data.get(header, ""))
        if header:
            seen.add(header)
    return row


def get_spreadsheet(force_reconnect=False):
    global _spreadsheet_cache, _email_sheets_cache
    if _spreadsheet_cache is None or force_reconnect:
        if force_reconnect:
            _sheet_read_cache.clear()
            invalidate_priority_snapshot()
            _email_sheets_cache = None
        creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
        _spreadsheet_cache = gspread.authorize(creds).open_by_key(SHEET_ID)
    return _spreadsheet_cache


def get_spreadsheet_with_retry():
    """Return spreadsheet, reconnecting once on stale-connection errors."""
    with performance_step("sheets.open"):
        try:
            return read_with_retry(
                get_spreadsheet,
                on_retry=_log_sheet_read_retry("spreadsheet"),
            )
        except RequestsConnectionError:
            return read_with_retry(
                lambda: get_spreadsheet(force_reconnect=True),
                on_retry=_log_sheet_read_retry("spreadsheet_reconnect"),
            )


def _log_sheet_read_retry(label):
    def log_retry(error, attempt, attempts, delay):
        app.logger.warning(
            "Sheets read retry for %s (%s/%s, %.2fs): %s",
            label, attempt, attempts, delay, error,
        )
    return log_retry


def get_worksheet(spreadsheet, title):
    sheet = _sheet_read_cache.worksheet(
        spreadsheet,
        title,
        loader=lambda: spreadsheet.worksheet(title),
    )
    try:
        sheet._store_tracker_spreadsheet = spreadsheet
    except Exception:
        pass
    return sheet


def sheet_cache_enabled(spreadsheet):
    return bool(
        spreadsheet
        and (
            spreadsheet.__class__.__module__.startswith("gspread")
            or getattr(
                spreadsheet, "_store_tracker_enable_read_cache", False
            )
        )
    )


def cached_worksheet_values(sheet, spreadsheet=None):
    spreadsheet = (
        spreadsheet
        or getattr(sheet, "_store_tracker_spreadsheet", None)
        or _spreadsheet_cache
    )
    title = str(getattr(sheet, "title", "") or "").strip()
    if (
        title not in READ_CACHE_TITLES
        or not sheet_cache_enabled(spreadsheet)
    ):
        return read_with_retry(
            sheet.get_all_values,
            on_retry=_log_sheet_read_retry(f"values.{title or 'other'}"),
        ), False
    values, cache_hit = _sheet_read_cache.values(
        spreadsheet,
        title,
        loader=sheet.get_all_values,
    )
    return values, cache_hit


def invalidate_priority_snapshot():
    global _priority_snapshot_entry
    with _priority_snapshot_condition:
        _priority_snapshot_entry = None
        _priority_snapshot_condition.notify_all()


def invalidate_sheet_cache(spreadsheet, *titles, worksheets=False):
    _sheet_read_cache.invalidate(spreadsheet, *titles, worksheets=worksheets)
    if set(titles) & READ_CACHE_TITLES:
        invalidate_priority_snapshot()


def invalidate_sheet_for_write(sheet):
    title = str(getattr(sheet, "title", "") or "").strip()
    spreadsheet = getattr(sheet, "spreadsheet", None)
    if spreadsheet is None:
        spreadsheet = _spreadsheet_cache
    if title:
        invalidate_sheet_cache(spreadsheet, title)


def get_route_travel_time_provider():
    """Return the shared backend-only Routes provider for route proposals."""
    global _route_provider, _route_provider_config

    routes_key = str(os.environ.get("GOOGLE_ROUTES_API_KEY") or "").strip()
    maps_fallback_key = str(os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()
    if application_environment() in PILOT_ENVIRONMENTS and not routes_key:
        raise TravelTimeConfigurationError()
    api_key = routes_key or maps_fallback_key
    if not api_key:
        raise TravelTimeConfigurationError()

    routing_preference = str(
        os.environ.get("ROUTE_ROUTING_PREFERENCE") or "TRAFFIC_UNAWARE"
    ).strip().upper()
    try:
        timeout_seconds = float(
            os.environ.get("ROUTE_MATRIX_TIMEOUT_SECONDS") or 15
        )
    except (TypeError, ValueError):
        timeout_seconds = 15.0
    try:
        cache_ttl_seconds = float(
            os.environ.get("ROUTE_MATRIX_CACHE_TTL_SECONDS") or 600
        )
    except (TypeError, ValueError):
        cache_ttl_seconds = 600.0

    config = (
        api_key,
        routing_preference,
        timeout_seconds,
        cache_ttl_seconds,
    )
    with _route_provider_lock:
        if _route_provider is None or _route_provider_config != config:
            _route_provider = GoogleRoutesTravelTimeProvider(
                api_key,
                routing_preference=routing_preference,
                timeout_seconds=timeout_seconds,
                cache_ttl_seconds=cache_ttl_seconds,
            )
            _route_provider_config = config
    return _route_provider


def route_matrix_candidate_limit(environ=None):
    environment = os.environ if environ is None else environ
    try:
        configured = int(
            environment.get("ROUTE_MATRIX_CANDIDATE_LIMIT") or 60
        )
    except (TypeError, ValueError):
        configured = 60
    return max(MAX_ROUTE_STOPS, min(configured, 200))


def worksheet_to_dicts(worksheet, expected_columns=None, required_columns=None):
    with performance_step(_performance_sheet_step(worksheet)) as measurement:
        rows, cache_hit = cached_worksheet_values(worksheet)
        measurement["row_count"] = max(0, len(rows) - 1)
    record_performance_step(
        f"sheets.cache.{'hit' if cache_hit else 'miss'}",
        time.perf_counter(),
        max(0, len(rows) - 1),
    )
    if not rows:
        return []

    headers = [str(header).strip() for header in rows[0]]
    required_columns = required_columns or []
    missing_columns = [col for col in required_columns if col not in headers]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Worksheet '{worksheet.title}' saknar obligatoriska kolumner: {missing}")

    expected_columns = expected_columns or headers
    result = []
    for row in rows[1:]:
        item = {col: "" for col in expected_columns}
        for idx, header in enumerate(headers):
            if not header:
                continue
            value = row[idx] if idx < len(row) else ""
            if header in item:
                item[header] = merge_worksheet_cell_value(header, item[header], value)
            else:
                item[header] = value
        result.append(item)
    return result


def get_or_create_worksheet(spreadsheet, title, columns, rows=1000):
    try:
        sheet = get_worksheet(spreadsheet, title)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=title, rows=rows, cols=max(len(columns), 10))
        try:
            sheet._store_tracker_spreadsheet = spreadsheet
        except Exception:
            pass
        sheet.append_row(columns)
        invalidate_sheet_cache(spreadsheet, title, worksheets=True)
        return sheet

    values, _cache_hit = cached_worksheet_values(sheet, spreadsheet)
    headers = [str(header).strip() for header in (values[0] if values else [])]
    if not headers:
        sheet.append_row(columns)
        invalidate_sheet_cache(spreadsheet, title)
    else:
        ensure_worksheet_columns(sheet, headers, columns)
    return sheet


def ensure_planned_activities_worksheet(spreadsheet):
    with _planning_write_lock:
        return get_or_create_worksheet(
            spreadsheet,
            PLANNED_ACTIVITIES_SHEET,
            PLANNED_ACTIVITY_COLUMNS,
            rows=2000,
        )


def get_planned_activity_snapshot(spreadsheet):
    with _planning_write_lock:
        sheet = ensure_planned_activities_worksheet(spreadsheet)
        headers, rows = worksheet_snapshot(
            sheet,
            expected_columns=PLANNED_ACTIVITY_COLUMNS,
        )
        return sheet, headers, rows


def get_saved_route_proposal(spreadsheet, user_name, route_date):
    sheet = get_or_create_worksheet(
        spreadsheet,
        ROUTE_PROPOSALS_SHEET,
        ROUTE_PROPOSAL_COLUMNS,
        rows=500,
    )
    requested_user = normalize_key(user_name)
    requested_date = (
        route_date.isoformat()
        if isinstance(route_date, date)
        else str(route_date or "").strip()
    )
    rows = worksheet_to_dicts(
        sheet,
        expected_columns=ROUTE_PROPOSAL_COLUMNS,
        required_columns=ROUTE_PROPOSAL_COLUMNS,
    )
    for row in reversed(rows):
        if (
            normalize_key(row.get("user_name")) != requested_user
            or str(row.get("route_date") or "").strip() != requested_date
        ):
            continue
        try:
            payload = json.loads(str(row.get("payload_json") or ""))
        except (TypeError, ValueError):
            app.logger.warning(
                "Ignoring invalid saved route proposal for %s on %s",
                user_name,
                requested_date,
            )
            continue
        if not isinstance(payload, dict) or payload.get("ok") is not True:
            continue
        payload = dict(payload)
        payload["cached"] = True
        payload["route_date"] = requested_date
        payload["route_owner"] = (
            str(row.get("user_display_name") or "").strip()
            or payload.get("route_owner")
            or str(user_name or "").strip()
        )
        payload["meta"] = {
            **dict(payload.get("meta") or {}),
            "daily_cache_hit": True,
        }
        return payload
    return None


def save_route_proposal(
    spreadsheet,
    *,
    user_name,
    user_display_name,
    route_date,
    payload,
):
    sheet = get_or_create_worksheet(
        spreadsheet,
        ROUTE_PROPOSALS_SHEET,
        ROUTE_PROPOSAL_COLUMNS,
        rows=500,
    )
    route_date_text = (
        route_date.isoformat()
        if isinstance(route_date, date)
        else str(route_date or "").strip()
    )
    append_dict_row(
        sheet,
        ROUTE_PROPOSAL_COLUMNS,
        {
            "route_date": route_date_text,
            "user_name": str(user_name or "").strip(),
            "user_display_name": str(user_display_name or "").strip(),
            "generated_at": str(payload.get("generated_at") or "").strip(),
            "payload_json": json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        },
    )


def ensure_email_worksheets(spreadsheet, *, include_events=True):
    global _email_sheets_cache
    spreadsheet_identity = id(spreadsheet)
    with _email_sheets_cache_lock:
        if _email_sheets_cache and _email_sheets_cache[0] == spreadsheet_identity:
            sheets = _email_sheets_cache[1]
        else:
            contact_sheet = get_worksheet(spreadsheet, "sales_activities")
            ensure_contact_worksheet_schema(contact_sheet)
            sheets = {
                EMAIL_MESSAGES_SHEET: get_or_create_worksheet(
                    spreadsheet, EMAIL_MESSAGES_SHEET, EMAIL_MESSAGES_COLUMNS
                ),
                EMAIL_RECIPIENTS_SHEET: get_or_create_worksheet(
                    spreadsheet, EMAIL_RECIPIENTS_SHEET, EMAIL_RECIPIENTS_COLUMNS
                ),
            }
            _email_sheets_cache = (spreadsheet_identity, sheets)

        if include_events and EMAIL_EVENTS_SHEET not in sheets:
            sheets[EMAIL_EVENTS_SHEET] = get_or_create_worksheet(
                spreadsheet, EMAIL_EVENTS_SHEET, EMAIL_EVENTS_COLUMNS
            )
        return sheets


def get_user_rows(spreadsheet):
    return worksheet_to_dicts(
        get_worksheet(spreadsheet, USERS_SHEET),
        expected_columns=USER_COLUMNS,
        required_columns=USER_COLUMNS,
    )


def public_user(user):
    profile = {
        key: str(user.get(key, "")).strip()
        for key in ("user_name", "name", "role", "email", "phone")
    }
    profile["admin"] = admin_flag_is_enabled(user.get("admin"))
    return profile


def find_active_user(spreadsheet, user_name):
    requested = str(user_name or "").strip().casefold()
    for user in get_user_rows(spreadsheet):
        if str(user.get("user_name", "")).strip().casefold() == requested and is_yes(user.get("active")):
            return user
    return None


def get_settings(spreadsheet):
    rows = worksheet_to_dicts(
        get_worksheet(spreadsheet, SETTINGS_SHEET),
        expected_columns=SETTINGS_COLUMNS,
        required_columns=["key", "value"],
    )
    return {
        str(row.get("key", "")).strip(): str(row.get("value", "")).strip()
        for row in rows if str(row.get("key", "")).strip()
    }


def email_proposal_template_setting_key(proposal_type, field):
    proposal_type = normalize_proposal_type(proposal_type)
    return f"email_proposal_{proposal_type}_{field}"


def sanitize_template_order_rows(rows, product_catalog):
    sanitized = []
    for row in (rows or [])[:20]:
        product = str(row.get("product", "")).strip()[:250]
        quantity = str(row.get("quantity", "")).strip()[:30]
        unit = str(row.get("unit", "DFP")).strip()[:20] or "DFP"
        if not product:
            continue
        sanitized.append({"product": product, "quantity": quantity, "unit": unit})
    return canonicalize_proposal_order_rows(sanitized, product_catalog)


def get_email_proposal_template_config(settings, proposal_type, product_catalog):
    proposal_type = normalize_proposal_type(proposal_type)
    defaults = build_email_proposal_template_defaults(proposal_type)
    result = {
        "email_type": proposal_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[proposal_type],
        **defaults,
    }
    customized_fields = []
    for field in EMAIL_PROPOSAL_TEMPLATE_FIELDS:
        key = email_proposal_template_setting_key(proposal_type, field)
        if key in settings:
            result[field] = str(settings.get(key, "")).strip()
            customized_fields.append(field)

    order_key = email_proposal_template_setting_key(proposal_type, "order_config")
    order_config = None
    if settings.get(order_key):
        try:
            parsed = json.loads(settings[order_key])
            if isinstance(parsed, dict):
                order_config = parsed
        except (TypeError, ValueError):
            order_config = None

    if proposal_type == "reminder":
        default_mode = "latest_order"
        default_rows = []
    elif proposal_type == "reactivation":
        default_mode = "fixed"
        default_rows = build_reactivation_order_rows(product_catalog)
    else:
        default_mode = "fixed"
        default_rows = build_new_customer_order_rows(product_catalog)

    mode = str((order_config or {}).get("mode", default_mode)).strip().casefold()
    if proposal_type != "reminder":
        mode = "fixed"
    elif mode not in {"latest_order", "fixed"}:
        mode = default_mode
    rows = sanitize_template_order_rows(
        (order_config or {}).get("rows", default_rows),
        product_catalog,
    )
    if mode == "latest_order":
        rows = []
    result.update({
        "order_mode": mode,
        "order_rows": rows,
        "customized": bool(customized_fields or order_config),
    })
    return result


def save_email_proposal_template_config(spreadsheet, proposal_type, config):
    sheet = get_worksheet(spreadsheet, SETTINGS_SHEET)
    descriptions = {
        "subject": "Standardämne för mejlförslag",
        "intro_text": "Standardbrödtext för mejlförslag",
        "closing_text": "Standardavslutning för mejlförslag",
        "stockfiller_label": "Knapptext för Stockfiller",
        "product_sheet_label": "Knapptext för produktblad",
        "order_config": "Standardprodukter och antal för mejlförslag",
    }
    values = {
        email_proposal_template_setting_key(proposal_type, field): (
            config[field],
            f"{descriptions[field]} – {EMAIL_PROPOSAL_TYPES[proposal_type]}",
        )
        for field in EMAIL_PROPOSAL_TEMPLATE_FIELDS
    }
    values[email_proposal_template_setting_key(proposal_type, "order_config")] = (
        json.dumps({
            "mode": config["order_mode"],
            "rows": config["order_rows"],
        }, ensure_ascii=False, separators=(",", ":")),
        f"{descriptions['order_config']} – {EMAIL_PROPOSAL_TYPES[proposal_type]}",
    )

    with _settings_write_lock:
        headers = ensure_worksheet_columns(
            sheet, read_with_retry(lambda: sheet.row_values(1)), SETTINGS_COLUMNS
        )
        key_column = headers.index("key")
        rows = read_with_retry(sheet.get_all_values)
        row_by_key = {}
        for row_index, row in enumerate(rows[1:], start=2):
            key = str(row[key_column] if key_column < len(row) else "").strip()
            if key and key not in row_by_key:
                row_by_key[key] = row_index
        for key, (value, description) in values.items():
            row_index = row_by_key.get(key)
            if row_index:
                sheet.update_cell(row_index, headers.index("value") + 1, value)
                sheet.update_cell(row_index, headers.index("description") + 1, description)
            else:
                append_dict_row(sheet, SETTINGS_COLUMNS, {
                    "key": key,
                    "value": value,
                    "description": description,
                })
                row_by_key[key] = len(read_with_retry(sheet.get_all_values))
        invalidate_sheet_for_write(sheet)


def get_email_rows(spreadsheet, *, include_events=True):
    sheets = ensure_email_worksheets(
        spreadsheet, include_events=include_events
    )
    message_rows = worksheet_to_dicts(
        sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS
    )
    recipient_rows = worksheet_to_dicts(
        sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
    )
    event_rows = (
        worksheet_to_dicts(
            sheets[EMAIL_EVENTS_SHEET],
            expected_columns=EMAIL_EVENTS_COLUMNS,
        )
        if include_events
        else []
    )
    return message_rows, recipient_rows, event_rows


def append_dict_row(sheet, columns, values, value_input_option="RAW", single_value_columns=None):
    return append_dict_rows(
        sheet,
        columns,
        [values],
        value_input_option=value_input_option,
        single_value_columns=single_value_columns,
    )[0]


def append_dict_rows(sheet, columns, values, value_input_option="RAW", single_value_columns=None):
    """Append rows to an explicit A-based range instead of relying on Sheets table detection.

    Google Sheets' append endpoint searches for a "logical table". A blank row or an
    orphaned value in a later column can make that table start somewhere other than
    column A, which shifts the entire appended row. Resolving the next row while
    holding a process-wide lock and updating an explicit A1 range keeps every value
    aligned with the worksheet headers.
    """
    if not values:
        return []

    with _worksheet_append_lock:
        headers = ensure_worksheet_columns(
            sheet,
            read_with_retry(
                lambda: sheet.row_values(1),
                on_retry=_log_sheet_read_retry(f"append.{sheet.title}.headers"),
            ),
            columns,
        )
        rendered_rows = [
            build_worksheet_row(
                headers,
                row,
                single_value_columns=single_value_columns,
            )
            for row in values
        ]
        existing_rows = read_with_retry(
            sheet.get_all_values,
            on_retry=_log_sheet_read_retry(f"append.{sheet.title}.rows"),
        )
        first_row = max(2, len(existing_rows) + 1)
        last_row = first_row + len(rendered_rows) - 1

        grid_rows = getattr(sheet, "row_count", 0) or 0
        if grid_rows and last_row > grid_rows:
            sheet.resize(rows=max(last_row, grid_rows + 100))

        end_cell = rowcol_to_a1(last_row, len(headers))
        sheet.batch_update(
            [{
                "range": f"A{first_row}:{end_cell}",
                "values": rendered_rows,
            }],
            value_input_option=value_input_option,
        )
        invalidate_sheet_for_write(sheet)
        return list(range(first_row, last_row + 1))


def find_sheet_row(sheet, column, value, normalizer=lambda item: str(item or "").strip()):
    headers = [str(header).strip() for header in read_with_retry(
        lambda: sheet.row_values(1),
        on_retry=_log_sheet_read_retry(f"find.{sheet.title}.headers"),
    )]
    if column not in headers:
        return None, headers, {}
    target = normalizer(value)
    values = read_with_retry(
        sheet.get_all_values,
        on_retry=_log_sheet_read_retry(f"find.{sheet.title}.rows"),
    )
    for row_index, row in enumerate(values[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        item = dict(zip(headers, padded))
        if normalizer(item.get(column)) == target:
            return row_index, headers, item
    return None, headers, {}


def update_sheet_row(sheet, row_index, headers, updates):
    data = []
    for key, value in updates.items():
        if key not in headers:
            continue
        cell = rowcol_to_a1(row_index, headers.index(key) + 1)
        data.append({"range": f"{cell}:{cell}", "values": [[value]]})
    if data:
        sheet.batch_update(data, value_input_option="RAW")
        invalidate_sheet_for_write(sheet)


def worksheet_snapshot(sheet, expected_columns=None):
    with performance_step(_performance_sheet_step(sheet)) as measurement:
        values, cache_hit = cached_worksheet_values(sheet)
        measurement["row_count"] = max(0, len(values) - 1)
    record_performance_step(
        f"sheets.cache.{'hit' if cache_hit else 'miss'}",
        time.perf_counter(),
        max(0, len(values) - 1),
    )
    if not values:
        return list(expected_columns or []), []
    headers = [str(header).strip() for header in values[0]]
    rows = []
    for row_index, row in enumerate(values[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        item = dict(zip(headers, padded))
        if expected_columns:
            item = {column: item.get(column, "") for column in expected_columns}
        rows.append((row_index, item))
    return headers, rows


def batch_update_sheet_rows(sheet, headers, row_updates):
    data = []
    for row_index, row in row_updates:
        values = [row.get(header, "") for header in headers]
        data.append({
            "range": f"A{row_index}:{rowcol_to_a1(row_index, len(headers))}",
            "values": [values],
        })
    if data:
        sheet.batch_update(data, value_input_option="RAW")
        invalidate_sheet_for_write(sheet)


def batch_update_sheet_changes(sheet, headers, row_changes, new_rows=()):
    """Commit sparse mutations and contiguous appended rows in one API call."""
    data = []
    for row_index, changes in row_changes:
        for key, value in changes.items():
            if key not in headers:
                continue
            cell = rowcol_to_a1(row_index, headers.index(key) + 1)
            data.append({"range": f"{cell}:{cell}", "values": [[value]]})
    appended_indexes = []
    if new_rows:
        existing = read_with_retry(
            sheet.get_all_values,
            on_retry=_log_sheet_read_retry(f"batch.{sheet.title}.rows"),
        )
        first_row = max(2, len(existing) + 1)
        last_row = first_row + len(new_rows) - 1
        grid_rows = getattr(sheet, "row_count", 0) or 0
        if grid_rows and last_row > grid_rows:
            sheet.resize(rows=max(last_row, grid_rows + 100))
        data.append({
            "range": (
                f"A{first_row}:"
                f"{rowcol_to_a1(last_row, len(headers))}"
            ),
            "values": [
                build_worksheet_row(headers, row)
                for row in new_rows
            ],
        })
        appended_indexes = list(range(first_row, last_row + 1))
    if data:
        sheet.batch_update(data, value_input_option="RAW")
        invalidate_sheet_for_write(sheet)
    return appended_indexes


def run_with_retry(operation, *, attempts=5, base_delay=0.5, label="Google Sheets"):
    last_error = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            delay = base_delay * (2 ** attempt)
            app.logger.warning("%s failed (attempt %s/%s): %s", label, attempt + 1, attempts, exc)
            time.sleep(delay)
    raise last_error


def current_user():
    return dict(session.get("user") or {})


def get_order_rows(spreadsheet):
    return worksheet_to_dicts(
        get_worksheet(spreadsheet, "order_rows"),
        expected_columns=ORDER_COLUMNS,
        required_columns=ORDER_REQUIRED_COLUMNS,
    )


def get_contact_rows(spreadsheet):
    return worksheet_to_dicts(
        get_worksheet(spreadsheet, "sales_activities"),
        expected_columns=CONTACT_COLUMNS,
        required_columns=CONTACT_REQUIRED_COLUMNS,
    )


def get_customer_rows(spreadsheet):
    sheet = get_worksheet(spreadsheet, "customers_enriched")
    with performance_step(_performance_sheet_step(sheet)) as measurement:
        customer_values, cache_hit = cached_worksheet_values(sheet, spreadsheet)
        measurement["row_count"] = max(0, len(customer_values) - 1)
    record_performance_step(
        f"sheets.cache.{'hit' if cache_hit else 'miss'}",
        time.perf_counter(),
        max(0, len(customer_values) - 1),
    )
    customer_headers = customer_values[0] if customer_values else []
    customers = []
    for i, row in enumerate(customer_values[1:], start=2):
        padded = row + [""] * (len(customer_headers) - len(row))
        d = dict(zip(customer_headers, padded))
        name = d.get("customer", "").strip()
        if not name:
            continue
        customers.append({
            "row": i,
            "customer": name,
            "customer_id": d.get("customer_id", "").strip(),
            "cancelled_flag": d.get("cancelled_flag", "").strip(),
            "sales_person": d.get("sales_person", "").strip(),
            "customer_segment": d.get("customer_segment", "").strip(),
            "customer_number": d.get("customer_number", "").strip(),
            "phone": d.get("phone", "").strip(),
            "email": d.get("email", "").strip(),
            "email_last_order": d.get("email_last_order", "").strip(),
            "city_google": d.get("city_google", "").strip(),
            "address_google": d.get("address_google", "").strip(),
            "address_number_google": d.get("address_number_google", "").strip(),
            "postal_code_google": d.get("postal_code_google", "").strip(),
            "region_google": d.get("region_google", "").strip(),
            "latitude_google": d.get("latitude_google", "").strip(),
            "longitude_google": d.get("longitude_google", "").strip(),
            "comment": d.get("comment", "").strip(),
        })
    return customers


def normalize_key(value):
    return normalize_customer_key(value)


def normalize_role(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.replace("\xa0", " ").strip().casefold().split())


def user_is_seller(user):
    return normalize_role((user or {}).get("role")) in {
        "saljare",
        "account manager",
        "accountmanager",
    }


def admin_flag_is_enabled(value):
    """Return True only for the explicitly configured users.admin value Y."""
    return str(value or "").strip().casefold() == "y"


def user_is_admin(user):
    value = (user or {}).get("admin")
    return value is True or admin_flag_is_enabled(value)


def user_route_display_name(user):
    return (
        str((user or {}).get("name") or "").strip()
        or str((user or {}).get("user_name") or "").strip()
    )


def user_route_identity_keys(user):
    return {
        value
        for value in (
            normalize_key((user or {}).get("user_name")),
            normalize_key((user or {}).get("name")),
        )
        if value
    }


def customer_owned_by_user(customer, user):
    return (
        normalize_key((customer or {}).get("sales_person"))
        in user_route_identity_keys(user)
    )


def user_can_be_sales_owner(user, customers):
    """Keep authentication role separate from operational CRM ownership."""
    if not user or not is_yes(user.get("active")):
        return False
    if user_is_seller(user):
        return True
    return bool(
        user_is_admin(user)
        and any(customer_owned_by_user(customer, user) for customer in customers)
    )


def canonical_owner_for_customer(spreadsheet, customer, *, users=None):
    """Resolve the current customer owner to the canonical users.user_name."""
    owner_key = normalize_key((customer or {}).get("sales_person"))
    if not owner_key:
        return None
    users = users if users is not None else get_user_rows(spreadsheet)
    matches = [
        user for user in users
        if is_yes(user.get("active")) and owner_key in user_route_identity_keys(user)
    ]
    return matches[0] if len(matches) == 1 else None


def contact_currently_owned_by(
    contact, owner, customers, *, customer_lookup=None
):
    """Apply current master ownership while preserving contact actor history."""
    lookup = customer_lookup or CustomerLookup(customers)
    customer = related_row_customer(
        contact, customers, customer_lookup=lookup
    )
    return bool(customer and customer_owned_by_user(customer, owner))


def customer_access_allowed(customer, user):
    return bool(customer) and (
        user_is_admin(user) or customer_owned_by_user(customer, user)
    )


class CustomerResolutionError(ValueError):
    def __init__(self, code, message, status=409):
        super().__init__(message)
        self.code = code
        self.status = status


class CustomerLookup:
    """Request-local indexes over one already loaded canonical customer list."""

    def __init__(self, customers):
        self.by_id = {}
        self.by_number = {}
        self.by_name = {}
        self.by_row = {}
        self.ambiguous_ids = set()
        self.ambiguous_numbers = set()
        self.ambiguous_names = set()
        self.ambiguous_rows = set()
        for customer in customers:
            self._add(
                self.by_id,
                self.ambiguous_ids,
                str(customer.get("customer_id") or "").strip(),
                customer,
            )
            self._add(
                self.by_number,
                self.ambiguous_numbers,
                normalize_key(customer.get("customer_number")),
                customer,
            )
            self._add(
                self.by_name,
                self.ambiguous_names,
                normalize_key(customer.get("customer")),
                customer,
            )
            self._add(
                self.by_row,
                self.ambiguous_rows,
                customer.get("row"),
                customer,
                allow_empty=True,
            )

    @staticmethod
    def _add(mapping, ambiguous, key, customer, *, allow_empty=False):
        if (not allow_empty and not key) or (allow_empty and key is None):
            return
        if key in ambiguous:
            return
        if key in mapping:
            mapping.pop(key, None)
            ambiguous.add(key)
            return
        mapping[key] = customer

    def resolve(
        self,
        *,
        customer_id="",
        customer_number="",
        customer_name="",
        row=None,
    ):
        requested_id = str(customer_id or "").strip()
        if requested_id:
            if requested_id in self.ambiguous_ids:
                raise CustomerResolutionError(
                    "customer_identity_conflict",
                    "Kund-ID:t finns på flera kundrader och måste rättas.",
                )
            return self.by_id.get(requested_id)

        requested_number = normalize_key(customer_number)
        if requested_number:
            if requested_number in self.ambiguous_numbers:
                raise CustomerResolutionError(
                    "ambiguous_customer",
                    "Kundnumret matchar flera butiker.",
                )
            return self.by_number.get(requested_number)

        requested_name = normalize_key(customer_name)
        if requested_name:
            if requested_name in self.ambiguous_names:
                raise CustomerResolutionError(
                    "ambiguous_customer",
                    "Kundnamnet matchar flera butiker.",
                )
            return self.by_name.get(requested_name)

        if row not in (None, ""):
            try:
                requested_row = int(row)
            except (TypeError, ValueError, OverflowError):
                return None
            if requested_row in self.ambiguous_rows:
                return None
            return self.by_row.get(requested_row)
        return None


def resolve_customer(
    customers,
    *,
    customer_id="",
    customer_number="",
    customer_name="",
    row=None,
    customer_lookup=None,
):
    """Resolve one canonical customer without allowing weaker identifiers to bypass stronger ones."""
    lookup = customer_lookup or CustomerLookup(customers)
    return lookup.resolve(
        customer_id=customer_id,
        customer_number=customer_number,
        customer_name=customer_name,
        row=row,
    )


def resolve_customer_from_data(customers, data, *, customer_lookup=None):
    data = data or {}
    return resolve_customer(
        customers,
        customer_id=data.get("customer_id"),
        customer_number=data.get("customer_number"),
        customer_name=data.get("customer") or data.get("customer_name"),
        row=data.get("customer_row") or data.get("row"),
        customer_lookup=customer_lookup,
    )


def resolve_accessible_customer(
    customers,
    user,
    *,
    customer_lookup=None,
    **identifiers,
):
    try:
        customer = resolve_customer(
            customers,
            customer_lookup=customer_lookup,
            **identifiers,
        )
    except CustomerResolutionError:
        return None
    return customer if customer_access_allowed(customer, user) else None


def filter_accessible_customers(customers, user):
    if user_is_admin(user):
        return list(customers)
    return [
        customer for customer in customers
        if customer_owned_by_user(customer, user)
    ]


def related_row_customer(
    row,
    customers,
    *,
    name_key="customer",
    number_key="customer_number",
    customer_lookup=None,
):
    try:
        return resolve_customer(
            customers,
            customer_id=row.get("customer_id"),
            customer_number=row.get(number_key),
            customer_name=row.get(name_key),
            row=row.get("customer_row") or row.get("row"),
            customer_lookup=customer_lookup,
        )
    except CustomerResolutionError:
        return None


def related_rows_for_customer(
    rows,
    customers,
    customer,
    *,
    name_key="customer",
    number_key="customer_number",
    customer_lookup=None,
):
    lookup = customer_lookup or CustomerLookup(customers)
    return [
        row for row in rows
        if related_row_customer(
            row,
            customers,
            name_key=name_key,
            number_key=number_key,
            customer_lookup=lookup,
        ) is customer
    ]


def accessible_contact_rows(
    contact_rows,
    customers,
    user,
    *,
    customer_lookup=None,
):
    if user_is_admin(user):
        return list(contact_rows)
    lookup = customer_lookup or CustomerLookup(customers)
    result = []
    for contact in contact_rows:
        customer = related_row_customer(
            contact,
            customers,
            customer_lookup=lookup,
        )
        if not customer_access_allowed(customer, user):
            continue
        result.append({
            **contact,
            "customer": str(customer.get("customer") or "").strip(),
            "customer_id": str(customer.get("customer_id") or "").strip(),
        })
    return result


def accessible_related_rows(
    rows,
    customers,
    user,
    *,
    name_key="customer",
    number_key="customer_number",
    customer_lookup=None,
):
    if user_is_admin(user):
        return list(rows)
    lookup = customer_lookup or CustomerLookup(customers)
    result = []
    for row in rows:
        customer = related_row_customer(
            row,
            customers,
            name_key=name_key,
            number_key=number_key,
            customer_lookup=lookup,
        )
        if not customer_access_allowed(customer, user):
            continue
        normalized_row = dict(row)
        normalized_row[name_key] = str(customer.get("customer") or "").strip()
        if number_key:
            normalized_row[number_key] = str(
                customer.get("customer_number") or ""
            ).strip()
        normalized_row["customer_id"] = str(
            customer.get("customer_id") or ""
        ).strip()
        result.append(normalized_row)
    return result


def planning_error(code, message, status=400, *, field=None, **extra):
    payload = {
        "ok": False,
        "error": code,
        "code": code,
        "message": message,
    }
    if field:
        payload["field"] = field
    payload.update(extra)
    return jsonify(payload), status


def normalize_client_request_id(value):
    text = str(value or "").strip()
    if not text or len(text) > 120 or any(ord(char) < 32 for char in text):
        return ""
    return text


def stable_planning_uuid(kind, *parts):
    identity = ":".join(
        [str(kind or "").strip().casefold()]
        + [str(part or "").strip() for part in parts]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"polarbar-planning:{identity}"))


def planning_request_scope(actor, operation, resource, request_id):
    return stable_planning_uuid(
        "request-scope",
        normalize_key((actor or {}).get("user_name")),
        str(operation or "").strip().casefold(),
        str(resource or "").strip(),
        str(request_id or "").strip(),
    )


def canonical_payload_fingerprint(payload):
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def planning_revision(row):
    try:
        return max(1, int(float(row.get("revision") or 1)))
    except (TypeError, ValueError):
        return 1


def planning_create_fingerprint(*, actor, owner, customer_id, contact_type,
                                scheduled_at, duration_minutes, note, source,
                                source_contact_id):
    return canonical_payload_fingerprint({
        "operation": "planned_activity.create.v1",
        "actor": normalize_key((actor or {}).get("user_name")),
        "owner": normalize_key((owner or {}).get("user_name")),
        "customer_id": str(customer_id or "").strip(),
        "contact_type": normalize_planning_contact_type(contact_type),
        "scheduled_at": planning_datetime_text(scheduled_at),
        "duration_minutes": int(duration_minutes or 0),
        "note": str(note or "").strip(),
        "source": str(source or "").strip().casefold(),
        "source_contact_id": str(source_contact_id or "").strip(),
    })


def planning_update_fingerprint(*, actor, activity_id, expected_revision, changes):
    return canonical_payload_fingerprint({
        "operation": "planned_activity.update.v1",
        "actor": normalize_key((actor or {}).get("user_name")),
        "planned_activity_id": str(activity_id or "").strip(),
        "expected_revision": int(expected_revision),
        "changes": changes,
    })


def normalize_planning_contact_type(value):
    normalized = normalize_role(value).replace("_", " ").replace("-", " ")
    normalized = " ".join(normalized.split())
    aliases = {
        "visit": "visit",
        "besok": "visit",
        "mote": "visit",
        "phone": "phone",
        "telefon": "phone",
        "call": "phone",
        "email": "email",
        "e mail": "email",
        "e post": "email",
        "mejl": "email",
    }
    return aliases.get(normalized, "")


def planning_contact_label(value):
    return PLANNING_CONTACT_TYPE_LABELS.get(
        normalize_planning_contact_type(value),
        str(value or "").strip(),
    )


def parse_planning_date(value):
    text = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_planning_datetime(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    if parsed.tzinfo is None:
        local = parsed.replace(tzinfo=STOCKHOLM_ZONE)
        # ZoneInfo accepts nonexistent DST wall times. A UTC round-trip does not.
        round_trip = local.astimezone(timezone.utc).astimezone(STOCKHOLM_ZONE)
        if round_trip.replace(tzinfo=None) != parsed:
            return None
        parsed = local
    else:
        supplied_offset = parsed.utcoffset()
        if supplied_offset is None:
            return None
        stockholm_value = parsed.astimezone(STOCKHOLM_ZONE)
        # UTC input is an absolute timestamp and may be normalized. Other
        # explicit offsets represent a Stockholm wall time and must match both
        # the actual local offset and wall clock, including DST folds/gaps.
        if supplied_offset != timedelta(0) and (
            stockholm_value.replace(tzinfo=None)
            != parsed.replace(tzinfo=None)
            or stockholm_value.utcoffset() != supplied_offset
        ):
            return None
        parsed = stockholm_value
    return parsed.replace(second=0, microsecond=0)


def planning_datetime_text(value):
    parsed = parse_planning_datetime(value)
    return parsed.isoformat(timespec="minutes") if parsed else ""


def planning_timestamp(value=None):
    parsed = value if isinstance(value, datetime) else None
    if parsed is None:
        parsed = stockholm_now()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=STOCKHOLM_ZONE)
    return parsed.astimezone(STOCKHOLM_ZONE).isoformat(timespec="seconds")


def parse_planning_instant(value):
    try:
        parsed = datetime.fromisoformat(
            str(value or "").strip().replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=STOCKHOLM_ZONE)
    return parsed.astimezone(STOCKHOLM_ZONE)


def next_planning_updated_at(previous_value):
    candidate = stockholm_now()
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=STOCKHOLM_ZONE)
    candidate = candidate.astimezone(STOCKHOLM_ZONE)
    try:
        previous = datetime.fromisoformat(
            str(previous_value or "").strip().replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        previous = None
    if previous is not None:
        if previous.tzinfo is None:
            previous = previous.replace(tzinfo=STOCKHOLM_ZONE)
        previous = previous.astimezone(STOCKHOLM_ZONE)
        if candidate <= previous:
            candidate = previous + timedelta(microseconds=1)
    return candidate.isoformat(timespec="microseconds")


def route_start_datetime(route_date, now=None):
    now = now or stockholm_now()
    if now.tzinfo is None:
        now = now.replace(tzinfo=STOCKHOLM_ZONE)
    now = now.astimezone(STOCKHOLM_ZONE)
    if route_date == now.date():
        minute_value = now.minute + (
            1 if now.second or now.microsecond else 0
        )
        minute = ((minute_value + 4) // 5) * 5
        rounded = now.replace(second=0, microsecond=0)
        if minute >= 60:
            rounded = rounded.replace(minute=0) + timedelta(hours=1)
        else:
            rounded = rounded.replace(minute=minute)
        return rounded
    return datetime.combine(
        route_date,
        datetime_time(hour=PLANNING_ROUTE_START_HOUR),
        tzinfo=STOCKHOLM_ZONE,
    )


def resolve_planning_owner(
    spreadsheet,
    requested_user_name=None,
    *,
    default_admin_to_first_seller=False,
    customers=None,
    users=None,
):
    caller = current_user()
    caller_name = str(caller.get("user_name") or "").strip()
    explicitly_requested = str(requested_user_name or "").strip()
    requested = explicitly_requested or caller_name
    if normalize_key(requested) != normalize_key(caller_name) and not user_is_admin(caller):
        return None, planning_error(
            "planning_owner_forbidden",
            "Du får bara hantera din egen planering.",
            403,
        )

    try:
        customers = (
            customers if customers is not None
            else get_customer_rows(spreadsheet)
        )
        users = users if users is not None else get_user_rows(spreadsheet)
        if (
            user_is_admin(caller)
            and default_admin_to_first_seller
            and (
                not explicitly_requested
                or normalize_key(requested) == normalize_key(caller_name)
            )
        ):
            active_sellers = sorted(
                (
                    user
                    for user in users
                    if user_can_be_sales_owner(user, customers)
                ),
                key=lambda user: normalize_key(user.get("user_name")),
            )
            caller_owner = next((
                user for user in active_sellers
                if normalize_key(user.get("user_name"))
                == normalize_key(caller_name)
            ), None)
            owner = caller_owner or (
                active_sellers[0] if active_sellers else None
            )
        else:
            requested_key = normalize_key(requested)
            owner = next((
                user for user in users
                if is_yes(user.get("active")) and requested_key in {
                    normalize_key(user.get("user_name")),
                    normalize_key(user.get("name")),
                }
            ), None)
    except Exception:
        app.logger.exception("Could not resolve planning owner")
        return None, planning_error(
            "user_store_unavailable",
            "Användaren kunde inte verifieras. Försök igen.",
            503,
        )
    if not owner:
        return None, planning_error(
            "planning_owner_not_found",
            "Den valda säljaren är inte aktiv.",
            404,
        )
    if not user_can_be_sales_owner(owner, customers) and not user_is_admin(caller):
        return None, planning_error(
            "planning_access_forbidden",
            "Ditt konto saknar behörighet till Planering.",
            403,
        )
    if not user_can_be_sales_owner(owner, customers):
        return None, planning_error(
            "planning_owner_not_sales_user",
            "Den valda användaren kan inte ha en säljplanering.",
            422,
        )
    return public_user(owner), None


def planning_owner_matches(row, owner):
    return normalize_key(row.get("user_name")) == normalize_key(
        (owner or {}).get("user_name")
    )


def public_planned_activity(row, *, now=None):
    now = now or stockholm_now()
    scheduled_at = parse_planning_datetime(row.get("scheduled_at"))
    status = str(row.get("status") or "planned").strip().casefold()
    if status not in PLANNING_STATUSES:
        status = "planned"
    overdue = bool(
        status == "planned"
        and scheduled_at
        and scheduled_at < now.astimezone(STOCKHOLM_ZONE)
    )
    try:
        duration_minutes = int(float(row.get("duration_minutes") or 0))
    except (TypeError, ValueError):
        duration_minutes = 0
    try:
        customer_row = int(float(row.get("customer_row") or 0)) or None
    except (TypeError, ValueError):
        customer_row = None
    try:
        route_sequence = int(float(row.get("route_sequence") or 0)) or None
    except (TypeError, ValueError):
        route_sequence = None

    return {
        "planned_activity_id": str(row.get("planned_activity_id") or "").strip(),
        "user_name": str(row.get("user_name") or "").strip(),
        "sales_person": str(row.get("sales_person") or "").strip(),
        "customer_id": str(row.get("customer_id") or "").strip(),
        "customer_key": str(row.get("customer_key") or "").strip(),
        "customer_row": customer_row,
        "customer_number": str(row.get("customer_number") or "").strip(),
        "customer": str(row.get("customer") or "").strip(),
        "contact_type": normalize_planning_contact_type(row.get("contact_type")),
        "contact_type_label": planning_contact_label(row.get("contact_type")),
        "scheduled_at": (
            scheduled_at.isoformat(timespec="minutes") if scheduled_at else ""
        ),
        "duration_minutes": duration_minutes,
        "time_is_estimated": is_yes(row.get("time_is_estimated")),
        "note": str(row.get("note") or "").strip(),
        "status": status,
        "display_status": "overdue" if overdue else status,
        "overdue": overdue,
        "source": str(row.get("source") or "").strip().casefold(),
        "source_contact_id": str(row.get("source_contact_id") or "").strip(),
        "completed_contact_id": str(row.get("completed_contact_id") or "").strip(),
        "route_group_id": str(row.get("route_group_id") or "").strip(),
        "route_sequence": route_sequence,
        "client_request_id": str(row.get("client_request_id") or "").strip(),
        "revision": planning_revision(row),
        "created_at": str(row.get("created_at") or "").strip(),
        "updated_at": str(row.get("updated_at") or "").strip(),
        "source_suggestion_id": str(
            row.get("source_suggestion_id") or ""
        ).strip(),
        "source_trigger_key": str(
            row.get("source_trigger_key") or ""
        ).strip(),
        "recommended_contact_type": normalize_planning_contact_type(
            row.get("recommended_contact_type")
        ),
    }


def find_planned_activity(spreadsheet, activity_id):
    sheet, headers, rows = get_planned_activity_snapshot(spreadsheet)
    requested = str(activity_id or "").strip()
    for row_index, row in rows:
        if str(row.get("planned_activity_id") or "").strip() == requested:
            return sheet, headers, row_index, row
    return sheet, headers, None, {}


def resolve_planning_customer(spreadsheet, data):
    """Compatibility wrapper around the shared canonical customer resolver."""
    customer = resolve_customer_from_data(get_customer_rows(spreadsheet), data)
    if customer is None and str((data or {}).get("customer_id") or "").strip():
        raise CustomerResolutionError(
            "customer_id_not_found",
            "Butikens kund-ID kunde inte hittas. Ladda om kundlistan och försök igen.",
            404,
        )
    return customer


def customer_identity_matches(
    left_name,
    left_number,
    right_name,
    right_number,
    left_customer_id="",
    right_customer_id="",
):
    left_id_key = normalize_key(left_customer_id)
    right_id_key = normalize_key(right_customer_id)
    if left_id_key and right_id_key:
        return left_id_key == right_id_key
    left_number_key = normalize_key(left_number)
    right_number_key = normalize_key(right_number)
    if left_number_key and right_number_key:
        return left_number_key == right_number_key
    return normalize_key(left_name) == normalize_key(right_name)


def parse_date_value(value):
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt).date()
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        return None


def parse_datetime_value(value):
    text = str(value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y/%m/%d",
                "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
                "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            parsed = datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt)
            return parsed
        except ValueError:
            pass

    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    except ValueError:
        return None


def format_date_value(value, fallback=""):
    if isinstance(value, datetime):
        parsed = value.date()
    elif isinstance(value, date):
        parsed = value
    else:
        parsed = parse_date_value(value)
    return parsed.isoformat() if parsed else fallback


def build_latest_contact_followups(contact_rows):
    latest_by_customer = {}
    for idx, contact in enumerate(contact_rows):
        customer_key = normalize_key(contact.get("customer"))
        if not customer_key:
            continue

        registered_at = parse_datetime_value(contact.get("date_time")) or datetime.min
        sort_key = (registered_at, idx)
        if customer_key not in latest_by_customer or sort_key > latest_by_customer[customer_key][0]:
            latest_by_customer[customer_key] = (sort_key, contact)

    return {
        customer_key: parse_date_value(contact.get("follow_up_date"))
        for customer_key, (_, contact) in latest_by_customer.items()
    }


def parse_number_value(value, default=0.0):
    text = str(value or "").strip()
    if not text:
        return default

    cleaned = "".join(ch for ch in text if ch.isdigit() or ch in ",.-")
    if cleaned in {"", "-", ".", ","}:
        return default

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return default


def parse_coordinate_value(value, kind):
    text = str(value or "").strip()
    if not text:
        return None

    limits = {
        "latitude": (55.0, 70.0),
        "longitude": (10.0, 25.0),
    }
    lower, upper = limits[kind]

    def in_range(number):
        return math.isfinite(number) and lower <= number <= upper

    normalized = text.replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        parsed = float(normalized)
        if in_range(parsed):
            return parsed
    except ValueError:
        pass

    # Some Google Sheet writes have been interpreted as thousands-grouped
    # numbers, e.g. 57,8934438 -> 578934438. Recover by restoring the decimal.
    sign = -1 if normalized.startswith("-") else 1
    integer_part = normalized.lstrip("+-").split(".", 1)[0]
    digits = "".join(ch for ch in integer_part if ch.isdigit())
    if not digits:
        return None

    raw_number = sign * int(digits)
    for decimals in range(1, 13):
        candidate = raw_number / (10 ** decimals)
        if in_range(candidate):
            return candidate

    return None


def week_start(day):
    return day - timedelta(days=day.weekday())


def week_key(day):
    iso = day.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def month_key(day):
    return f"{day.year}-{day.month:02d}"


def week_label(day):
    iso = day.isocalendar()
    return f"Vecka {iso.week} ({iso.year})"


def build_contact_log_rows(contact_rows):
    rows = []
    for idx, row in enumerate(contact_rows):
        parsed_datetime = parse_datetime_value(row.get("date_time"))
        parsed_date = parsed_datetime.date() if parsed_datetime else parse_date_value(row.get("date_time"))
        freezer_labels = [
            label for field, label in CONTACT_LOG_FREEZER_LABELS.items()
            if is_checked_value(row.get(field))
        ]
        log_row = {
            "Datum": format_date_value(parsed_date),
            "Ansvarig": text_to_sheet_value(row.get("sales_person")),
            "Kund": text_to_sheet_value(row.get("customer")),
            "Kanal": text_to_sheet_value(row.get("contact_channel")),
            "Resultat": text_to_sheet_value(row.get("result")),
            "Kommentar": text_to_sheet_value(row.get("comment")),
            "Nästa uppföljning": format_date_value(row.get("follow_up_date")),
            "I frysdisken": ", ".join(freezer_labels),
            "_month": month_key(parsed_date) if parsed_date else "",
            "_week": week_key(parsed_date) if parsed_date else "",
            "_week_label": week_label(parsed_date) if parsed_date else "",
            "_sort_value": parsed_datetime or (datetime.combine(parsed_date, datetime.min.time()) if parsed_date else datetime.min),
            "_source_index": idx,
        }
        rows.append(log_row)

    rows.sort(key=lambda item: (item["_sort_value"], item["_source_index"]), reverse=True)
    return rows


def get_contact_log_filter_values(args):
    filters = {}
    for key in ("responsible", "month", "week", "result"):
        values = []
        for value in args.getlist(key):
            values.extend(part.strip() for part in str(value).split(","))
        filters[key] = {value for value in values if value}
    for key in ("customer", "comment"):
        value = " ".join(str(value).strip() for value in args.getlist(key) if str(value).strip())
        if value:
            filters[key] = value
    return filters


def normalize_contact_log_search_text(value):
    text = unicodedata.normalize("NFD", str(value or "").casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def contact_log_is_subsequence(needle, haystack):
    needle_index = 0
    for char in haystack:
        if needle_index < len(needle) and needle[needle_index] == char:
            needle_index += 1
    return needle_index == len(needle)


def contact_log_text_matches(value, query):
    terms = normalize_contact_log_search_text(query).split()
    if not terms:
        return True

    normalized_value = normalize_contact_log_search_text(value)
    words = normalized_value.split()
    compact_value = normalized_value.replace(" ", "")

    for term in terms:
        if term in normalized_value or term in compact_value:
            continue
        if len(term) >= 4 and any(contact_log_is_subsequence(term, word) for word in words):
            continue
        return False
    return True


def filter_contact_log_rows(rows, filters):
    filtered = list(rows)
    if filters.get("responsible"):
        filtered = [row for row in filtered if row["Ansvarig"] in filters["responsible"]]
    if filters.get("month"):
        filtered = [row for row in filtered if row["_month"] in filters["month"]]
    if filters.get("week"):
        filtered = [row for row in filtered if row["_week"] in filters["week"]]
    if filters.get("result"):
        filtered = [row for row in filtered if row["Resultat"] in filters["result"]]
    if filters.get("customer"):
        filtered = [row for row in filtered if contact_log_text_matches(row["Kund"], filters["customer"])]
    if filters.get("comment"):
        filtered = [row for row in filtered if contact_log_text_matches(row["Kommentar"], filters["comment"])]
    return filtered


def build_contact_log_options(rows):
    def unique_display_values(key):
        return sorted({row[key] for row in rows if row.get(key)}, key=str.casefold)

    month_values = sorted({row["_month"] for row in rows if row["_month"]}, reverse=True)
    week_values = sorted(
        {row["_week"] for row in rows if row["_week"]},
        key=lambda value: tuple(int(part) for part in value.replace("W", "").split("-")),
        reverse=True,
    )
    week_labels = {row["_week"]: row["_week_label"] for row in rows if row["_week"]}

    return {
        "responsible": [{"value": value, "label": value} for value in unique_display_values("Ansvarig")],
        "month": [{"value": value, "label": value} for value in month_values],
        "week": [{"value": value, "label": week_labels.get(value, value)} for value in week_values],
        "result": [{"value": value, "label": value} for value in unique_display_values("Resultat")],
    }


def public_contact_log_row(row):
    return {column: row.get(column, "") for column in CONTACT_LOG_COLUMNS}


def build_contact_log_payload(contact_rows, filters=None):
    all_rows = build_contact_log_rows(contact_rows)
    selected_filters = filters or {}
    filtered_rows = filter_contact_log_rows(all_rows, selected_filters)
    return {
        "columns": CONTACT_LOG_COLUMNS,
        "rows": [public_contact_log_row(row) for row in filtered_rows],
        "filters": build_contact_log_options(all_rows),
        "total_count": len(all_rows),
        "filtered_count": len(filtered_rows),
    }


def xlsx_column_name(index):
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def xml_text(value):
    text = str(value or "")
    text = "".join(ch for ch in text if ch in "\n\r\t" or ord(ch) >= 32)
    return xml_escape(text)


def build_xlsx(columns, rows, sheet_name="Kontaktlogg"):
    output = BytesIO()
    sheet_name = xml_escape(sheet_name[:31] or "Kontaktlogg")
    table = [columns] + [[row.get(column, "") for column in columns] for row in rows]
    last_column = xlsx_column_name(len(columns))
    last_row = max(1, len(table))

    def cell_xml(row_idx, col_idx, value, style=""):
        cell_ref = f"{xlsx_column_name(col_idx)}{row_idx}"
        style_attr = f' s="{style}"' if style else ""
        return f'<c r="{cell_ref}" t="inlineStr"{style_attr}><is><t>{xml_text(value)}</t></is></c>'

    row_xml = []
    for row_idx, values in enumerate(table, start=1):
        style = "1" if row_idx == 1 else ""
        cells = "".join(cell_xml(row_idx, col_idx, value, style) for col_idx, value in enumerate(values, start=1))
        row_xml.append(f'<row r="{row_idx}">{cells}</row>')

    column_widths = [12, 16, 30, 14, 16, 52, 18, 26]
    cols_xml = "".join(
        f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
        for idx, width in enumerate(column_widths, start=1)
    )

    worksheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <dimension ref="A1:{last_column}{last_row}"/>
  <sheetViews><sheetView tabSelected="1" workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>
  <cols>{cols_xml}</cols>
  <sheetData>{"".join(row_xml)}</sheetData>
  <autoFilter ref="A1:{last_column}{last_row}"/>
</worksheet>'''

    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>''')
        archive.writestr("_rels/.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>''')
        archive.writestr("xl/workbook.xml", f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="{sheet_name}" sheetId="1" r:id="rId1"/></sheets>
</workbook>''')
        archive.writestr("xl/_rels/workbook.xml.rels", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>''')
        archive.writestr("xl/styles.xml", '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>
</styleSheet>''')
        archive.writestr("xl/worksheets/sheet1.xml", worksheet_xml)

    output.seek(0)
    return output.getvalue()


def build_recent_weeks(today, count=5):
    current_start = week_start(today)
    weeks = []
    for offset in range(count - 1, -1, -1):
        start = current_start - timedelta(weeks=offset)
        iso = start.isocalendar()
        weeks.append({
            "key": f"{iso.year}-W{iso.week:02d}",
            "label": f"Vecka {iso.week}",
            "start_date": start.isoformat(),
            "end_date": (start + timedelta(days=6)).isoformat(),
        })
    return weeks


SWEDISH_MONTH_LABELS = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "maj",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "okt",
    11: "nov",
    12: "dec",
}


def format_week_date_range(start, end):
    start_month = SWEDISH_MONTH_LABELS[start.month]
    end_month = SWEDISH_MONTH_LABELS[end.month]
    if start.month == end.month:
        return f"{start.day}-{end.day} {end_month}"
    return f"{start.day} {start_month}-{end.day} {end_month}"


def build_dfp_top_weeks(order_rows, year=2026, limit=5):
    totals_by_week = defaultdict(float)
    week_dates = {}

    for order in order_rows:
        order_date = parse_date_value(order["Order date"])
        if not order_date or order_date.year != year:
            continue

        total_weight = parse_number_value(order["Total weight"], default=0.0)
        if total_weight <= 0:
            continue

        start = week_start(order_date)
        end = start + timedelta(days=6)
        key = week_key(order_date)
        totals_by_week[key] += total_weight
        week_dates[key] = (start, end, order_date.isocalendar().week)

    top_weeks = sorted(
        totals_by_week.items(),
        key=lambda item: (-item[1], week_dates[item[0]][0]),
    )[:limit]
    top_total = top_weeks[0][1] if top_weeks else 0

    return [
        {
            "rank": idx + 1,
            "week_key": key,
            "label": f"Vecka {week_number}",
            "short_label": f"V{week_number}",
            "date_range": format_week_date_range(start, end),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "dfp_count": format_dfp_count(total),
            "share_of_top": round((total / top_total) * 100) if top_total else 0,
        }
        for idx, (key, total) in enumerate(top_weeks)
        for start, end, week_number in [week_dates[key]]
    ]


def build_freezer_summary(contact_rows):
    latest_contact_by_customer = {}
    for idx, contact in enumerate(contact_rows):
        customer_key = normalize_key(contact.get("customer"))
        if not customer_key:
            continue

        registered_at = parse_datetime_value(contact.get("date_time")) or datetime.min
        sort_key = (registered_at, idx)
        if customer_key not in latest_contact_by_customer or sort_key > latest_contact_by_customer[customer_key][0]:
            latest_contact_by_customer[customer_key] = (sort_key, contact)

    product_customer_sets = {item["field"]: set() for item in FREEZER_SUMMARY_ROWS}
    seller_customer_sets = {
        item["field"]: defaultdict(set)
        for item in FREEZER_SUMMARY_ROWS
    }
    seller_labels = {}

    for customer_key, (_, contact) in latest_contact_by_customer.items():
        checked_fields = [
            field for field in FREEZER_COLUMNS
            if is_checked_value(contact.get(field))
        ]
        if not checked_fields:
            continue

        seller_label = text_to_sheet_value(contact.get("sales_person")) or "Ej angiven"
        seller_key = seller_label.casefold()
        seller_labels.setdefault(seller_key, seller_label)

        for field in checked_fields:
            product_customer_sets[field].add(customer_key)
            seller_customer_sets[field][seller_key].add(customer_key)

    sales_people = [
        {"key": key, "label": seller_labels[key]}
        for key in sorted(seller_labels, key=lambda value: seller_labels[value].casefold())
    ]

    rows = []
    for item in FREEZER_SUMMARY_ROWS:
        field = item["field"]
        rows.append({
            "field": field,
            "label": item["label"],
            "total": len(product_customer_sets[field]),
            "counts": {
                person["key"]: len(seller_customer_sets[field].get(person["key"], set()))
                for person in sales_people
            },
        })

    sum_counts = {
        person["key"]: sum(row["counts"].get(person["key"], 0) for row in rows)
        for person in sales_people
    }
    total_sum = sum(row["total"] for row in rows)
    polarbar_row = next((row for row in rows if row["field"] == "polarbar"), None)

    def share_percent(value, total):
        return round((value / total) * 100) if total else 0

    return {
        "sales_people": sales_people,
        "rows": rows,
        "sum_row": {
            "label": "Summa",
            "total": total_sum,
            "counts": sum_counts,
        },
        "polarbar_share_row": {
            "label": "Polarbär andel",
            "total": share_percent(polarbar_row["total"] if polarbar_row else 0, total_sum),
            "counts": {
                person["key"]: share_percent(
                    (polarbar_row["counts"].get(person["key"], 0) if polarbar_row else 0),
                    sum_counts.get(person["key"], 0),
                )
                for person in sales_people
            },
        },
    }


def format_dfp_count(count):
    return int(count) if float(count).is_integer() else round(count, 1)


def calculate_customer_risk(order_count, latest_order, latest_delivery, today):
    most_recent = max(latest_order, latest_delivery) if latest_order and latest_delivery else (latest_order or latest_delivery)
    if order_count == 0 or not most_recent:
        return ""

    days_since = (today - most_recent).days
    if days_since >= 75:
        return "Återaktivering krävs"
    if days_since >= 60:
        return "Hög risk"
    if days_since >= 45:
        return "Risk"
    if days_since >= 30:
        return "Bevaka"
    return "Aktiv"


def is_positive_contact(result):
    text = str(result or "").strip().lower()
    positive_results = ("positiv", "positivt", "order lagd!", "order lagd")
    return any(phrase in text for phrase in positive_results)


def segment_sort_key(segment):
    text = str(segment or "").strip()
    if not text:
        return (99, "")
    first = text[:1].upper()
    if first in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        return (ord(first) - ord("A"), text.casefold())
    return (50, text.casefold())


def now_text():
    return stockholm_time_text()


def format_datetime_value(value, fallback=""):
    parsed = value if isinstance(value, datetime) else parse_datetime_value(value)
    return parsed.strftime("%Y-%m-%d %H:%M:%S") if parsed else fallback


def get_customer_by_row(spreadsheet, row_number):
    sheet = get_worksheet(spreadsheet, "customers_enriched")
    rows, _cache_hit = cached_worksheet_values(sheet, spreadsheet)
    if not rows or row_number < 2 or row_number > len(rows):
        return None
    headers = [str(header).strip() for header in rows[0]]
    row = rows[row_number - 1]
    padded = row + [""] * (len(headers) - len(row))
    customer = dict(zip(headers, padded))
    customer["row"] = row_number
    return customer if str(customer.get("customer", "")).strip() else None


def planning_suggestion_stub_enabled():
    configured = app.config.get("PLANNING_SUGGESTIONS_STUB")
    if configured is None:
        configured = os.environ.get("PLANNING_SUGGESTIONS_STUB")
    enabled = str(configured or "").strip().casefold() in {
        "1", "true", "yes", "on"
    }
    return enabled and application_environment() not in PILOT_ENVIRONMENTS


def planning_suggestion_service(spreadsheet):
    return PlanningSuggestionService(
        spreadsheet,
        lock=_planning_write_lock,
        now=stockholm_now(),
        zone=STOCKHOLM_ZONE,
        worksheet_getter=get_worksheet,
        values_reader=lambda sheet: cached_worksheet_values(
            sheet, spreadsheet
        )[0],
        invalidator=invalidate_sheet_for_write,
    )


def planning_suggestion_sort_key(item):
    return (
        -float(item.get("priority_score") or 0),
        -float(item.get("expected_order_dfp") or 0),
        int(item.get("trigger_precedence") or 99),
        str(item.get("customer_row") or item.get("customer_id") or ""),
    )


def planning_suggestion_candidates(spreadsheet, owner, activity_rows=()):
    if planning_suggestion_stub_enabled():
        customers = get_customer_rows(spreadsheet)
        contacts = get_contact_rows(spreadsheet)
        orders = get_order_rows(spreadsheet)
        candidates = build_phase1_stub_candidates(
            owner, customers, contacts=contacts, orders=orders
        )
    else:
        snapshot = get_authoritative_priority_snapshot(
            spreadsheet,
            today=stockholm_today(),
            planned_activity_rows=activity_rows,
        )
        customers = snapshot["customers"]
        with performance_step("suggestions.scoring") as measurement:
            priorities = snapshot["priorities"]
            measurement["row_count"] = len(priorities)
        owner_keys = user_route_identity_keys(owner)
        priorities = [
            priority for priority in priorities
            if normalize_key(priority.get("sales_person")) in owner_keys
        ]
        priorities = apply_workflow_suppressions(
            priorities,
            priority_workflow_suppressions(spreadsheet, priorities),
        )
        candidates = []
        for priority in priorities:
            if not str(priority.get("customer_id") or "").strip():
                continue
            suppression = str(
                priority.get("recommendation_suppression_reason") or ""
            ).strip()
            primary_trigger = str(
                priority.get("primary_trigger_type") or ""
            ).strip()
            recommendation_visible = bool(
                primary_trigger and priority.get("recommendation_eligible")
            )
            context_hash = decision_context_hash(
                owner=owner.get("user_name"),
                customer_id=priority.get("customer_id"),
                lifecycle=(
                    priority.get("decision_context_lifecycle")
                    or priority.get("lifecycle")
                ),
                order_count=priority.get("order_count"),
                latest_order_reference=priority.get("latest_order_reference"),
                latest_order_date=(
                    priority.get("latest_delivery_date")
                    or priority.get("latest_order_date")
                ),
                latest_contact_id=priority.get("latest_human_contact_id"),
                latest_contact_result=priority.get("latest_contact_result"),
                latest_contact_date=priority.get("latest_human_contact_date"),
                active_email_intent_event=priority.get(
                    "active_email_intent_event"
                ),
            )
            candidates.append({
                "decision_context_hash": context_hash,
                "customer_id": priority.get("customer_id"),
                "customer_key": (
                    priority.get("customer_number")
                    or normalize_key(priority.get("customer"))
                ),
                "customer_row": priority.get("row"),
                "customer": priority.get("customer"),
                "priority_score": priority.get("priority_score"),
                "expected_order_dfp": priority.get("expected_order_dfp"),
                "lifecycle": priority.get("lifecycle"),
                "segment": priority.get("segment"),
                "decision_context_lifecycle": priority.get(
                    "decision_context_lifecycle"
                ),
                "score_version": priority.get("score_version", SCORE_VERSION),
                "intent_timing": priority.get("intent_timing"),
                "value_index": priority.get("value_index"),
                "strategic_index": priority.get("strategic_index"),
                "recommendation_eligible": priority.get("recommendation_eligible"),
                "recommendation_suppression_reason": suppression,
                "reason_code": priority.get("primary_reason_code"),
                "reason_text": priority.get("primary_reason_text"),
                "primary_trigger_type": primary_trigger or "scoring_context",
                "primary_trigger_key": primary_trigger or "scoring_context",
                "covered_trigger_keys": priority.get("covered_trigger_keys") or [],
                "trigger_precedence": {
                    "stockfiller_click_followup": 1,
                    "product_sheet_click_followup": 2,
                    "email_open_followup": 3,
                    "established_reorder_due": 4,
                    "first_order_onboarding": 5,
                    "first_order_reorder": 6,
                    "positive_dialogue_followup": 7,
                    "strategic_contact_due": 8,
                    "legacy_missed_followup": 9,
                }.get(primary_trigger, 99),
                "externally_suppressed": not recommendation_visible,
                "overdue_days": priority.get("overdue_days"),
                "latest_human_contact_id": priority.get(
                    "latest_human_contact_id"
                ),
                "latest_human_contact_date": priority.get(
                    "latest_human_contact_date"
                ),
            })
    now = stockholm_now().astimezone(STOCKHOLM_ZONE)
    externally_planned_customer_ids = {
        str(row.get("customer_id") or "").strip()
        for row in activity_rows
        if str(row.get("status") or "planned").strip().casefold() == "planned"
        and not str(row.get("source_suggestion_id") or "").strip()
        and (
            parse_planning_datetime(row.get("scheduled_at")) is not None
            and parse_planning_datetime(row.get("scheduled_at")) >= now
        )
    }
    candidates = [
        {
            **candidate,
            "externally_suppressed": (
                bool(candidate.get("externally_suppressed"))
                or str(candidate.get("customer_id") or "").strip()
                in externally_planned_customer_ids
            ),
        }
        for candidate in candidates
    ]
    customers_by_id = {
        str(customer.get("customer_id") or "").strip(): customer
        for customer in customers
        if str(customer.get("customer_id") or "").strip()
    }
    enriched = []
    for candidate in candidates:
        customer = customers_by_id.get(
            str(candidate.get("customer_id") or "").strip(), {}
        )
        email_available = any(
            is_valid_email(address)
            for address in split_email_values(
                customer.get("email"), customer.get("email_last_order")
            )
        )
        channel = recommend_contact_channel(
            lifecycle=candidate.get("lifecycle"),
            overdue_days=candidate.get("overdue_days"),
            trigger_key=candidate.get("primary_trigger_key"),
            segment=candidate.get("segment"),
            has_human_contact=bool(
                candidate.get("latest_human_contact_id")
                or candidate.get("latest_human_contact_date")
            ),
            phone=customer.get("phone"),
            email_available=email_available,
            visible=not candidate.get("externally_suppressed"),
        )
        enriched.append({**candidate, **(channel or {})})
    candidates = enriched
    return sorted(candidates, key=planning_suggestion_sort_key)


def sync_suggestion_from_activity(spreadsheet, activity, *, request_id):
    suggestion_id = str(activity.get("source_suggestion_id") or "").strip()
    if not suggestion_id:
        return
    service = planning_suggestion_service(spreadsheet)
    try:
        _sheet, _events, _headers, _row_index, suggestion = service.find(
            suggestion_id
        )
        status = str(activity.get("status") or "planned").strip().casefold()
        suggestion_status = str(
            suggestion.get("status") or "pending"
        ).strip().casefold()
        if status == "completed" and suggestion_status in {
            "pending", "snoozed", "planned"
        }:
            action = "resolve"
        elif status in {"cancelled", "skipped"} and suggestion_status == "planned":
            action = "reopen"
        else:
            return
        fingerprint = suggestion_mutation_fingerprint(
            action,
            suggestion_id,
            request_id,
            {"planned_activity_id": activity.get("planned_activity_id")},
        )
        service.transition(
            suggestion_id,
            owner_name=suggestion.get("user_name"),
            action=action,
            expected_revision=int(float(suggestion.get("revision") or 1)),
            request_id=request_id,
            fingerprint=fingerprint,
            resolved_by_type="activity" if action == "resolve" else "",
            resolved_by_id=(
                activity.get("planned_activity_id") if action == "resolve" else ""
            ),
        )
    except SuggestionError as exc:
        if exc.code not in {"suggestion_not_pending", "suggestion_not_found"}:
            raise


def resolve_suggestions_for_customer_event(
    spreadsheet, *, customer_id, resolved_by_type, resolved_by_id, request_id
):
    """Resolve against the current canonical owner, never the event actor."""
    canonical_id = str(customer_id or "").strip()
    event_id = str(resolved_by_id or "").strip()
    if not canonical_id or not event_id:
        return []
    customers = get_customer_rows(spreadsheet)
    customer = resolve_customer(customers, customer_id=canonical_id)
    if not customer:
        return []
    try:
        owner = canonical_owner_for_customer(spreadsheet, customer)
    except Exception:
        owner = None
    if not owner:
        # Compatibility for a temporarily unavailable users worksheet: an
        # already materialized row can safely supply user_name only when its
        # stored owner identity still matches the current customer owner.
        try:
            _sheet, _events, _headers, stored = (
                planning_suggestion_service(spreadsheet).snapshot()
            )
            current_owner_key = normalize_key(customer.get("sales_person"))
            owner_names = {
                str(row.get("user_name") or "").strip()
                for _index, row in stored
                if str(row.get("customer_id") or "").strip() == canonical_id
                and current_owner_key in {
                    normalize_key(row.get("user_name")),
                    normalize_key(row.get("sales_person")),
                }
            }
            if len(owner_names) == 1:
                owner = {"user_name": owner_names.pop()}
        except Exception:
            owner = None
    if not owner:
        return []
    return planning_suggestion_service(spreadsheet).resolve_customer(
        owner_name=owner.get("user_name"),
        customer_id=canonical_id,
        resolved_by_type=resolved_by_type,
        resolved_by_id=event_id,
        request_id=request_id,
    )


def resolve_suggestions_for_contact(
    spreadsheet, *, owner=None, customer_id, contact_id, request_id
):
    return resolve_suggestions_for_customer_event(
        spreadsheet,
        customer_id=customer_id,
        resolved_by_type="contact",
        resolved_by_id=contact_id,
        request_id=request_id,
    )


def resolve_suggestions_for_email(
    spreadsheet, *, owner=None, customer_id, email_id
):
    return resolve_suggestions_for_customer_event(
        spreadsheet,
        customer_id=customer_id,
        resolved_by_type="email",
        resolved_by_id=email_id,
        request_id=f"email:{email_id}",
    )


def customer_is_cancelled(customer):
    value = str(customer.get("cancelled_flag", "") or "").strip().casefold()
    return value in {"1", "y", "yes", "ja", "true", "cancelled", "canceled", "avslutad"}


def build_planned_activity_row(
    *,
    activity_id,
    owner,
    customer,
    contact_type,
    scheduled_at,
    note="",
    status="planned",
    source="manual",
    source_contact_id="",
    completed_contact_id="",
    route_group_id="",
    route_sequence="",
    client_request_id="",
    time_is_estimated=False,
    created_at=None,
    updated_at=None,
    create_fingerprint="",
    last_mutation_request_id="",
    last_mutation_fingerprint="",
    revision=1,
    source_suggestion_id="",
    source_trigger_key="",
    recommended_contact_type="",
):
    contact_type = normalize_planning_contact_type(contact_type)
    source = str(source or "manual").strip().casefold()
    status = str(status or "planned").strip().casefold()
    scheduled = parse_planning_datetime(scheduled_at)
    now_value = planning_timestamp()
    customer_number = str(customer.get("customer_number") or "").strip()
    customer_name = str(customer.get("customer") or "").strip()
    return {
        "planned_activity_id": str(activity_id or "").strip(),
        "user_name": str(owner.get("user_name") or "").strip(),
        "sales_person": user_route_display_name(owner),
        "customer_id": str(customer.get("customer_id") or "").strip(),
        "customer_key": normalize_key(customer_number) or normalize_key(customer_name),
        "customer_row": customer.get("row") or "",
        "customer_number": customer_number,
        "customer": customer_name,
        "contact_type": contact_type,
        "scheduled_at": (
            scheduled.isoformat(timespec="minutes") if scheduled else ""
        ),
        "duration_minutes": PLANNING_CONTACT_DURATIONS.get(contact_type, 0),
        "time_is_estimated": "Y" if time_is_estimated else "N",
        "note": text_to_sheet_value(note, max_length=300),
        "status": status,
        "source": source,
        "source_contact_id": str(source_contact_id or "").strip(),
        "completed_contact_id": str(completed_contact_id or "").strip(),
        "route_group_id": str(route_group_id or "").strip(),
        "route_sequence": route_sequence if route_sequence not in (None, "") else "",
        "client_request_id": str(client_request_id or "").strip(),
        "create_fingerprint": str(create_fingerprint or "").strip(),
        "last_mutation_request_id": str(last_mutation_request_id or "").strip(),
        "last_mutation_fingerprint": str(last_mutation_fingerprint or "").strip(),
        "revision": max(1, int(revision or 1)),
        "created_at": str(created_at or now_value).strip(),
        "updated_at": str(updated_at or now_value).strip(),
        "source_suggestion_id": str(source_suggestion_id or "").strip(),
        "source_trigger_key": str(source_trigger_key or "").strip(),
        "recommended_contact_type": normalize_planning_contact_type(
            recommended_contact_type
        ),
    }


def owner_contact_matches(contact, owner):
    responsible = normalize_key(contact.get("sales_person"))
    return responsible in {
        normalize_key((owner or {}).get("name")),
        normalize_key((owner or {}).get("user_name")),
    }


def planned_contact_id_for_payload(
    *,
    owner,
    planned_activity_id,
    customer_name,
    customer_key,
    contact_channel,
    data,
    freezer_values,
    follow_up_enabled,
    follow_up_type,
    follow_up_at,
    follow_up_note,
    mirrored_follow_up_date,
):
    canonical = json.dumps(
        {
            "owner": normalize_key((owner or {}).get("user_name")),
            "planned_activity_id": str(
                planned_activity_id or ""
            ).strip(),
            "customer": (
                normalize_key(customer_key)
                or normalize_key(customer_name)
            ),
            "contact_channel": (
                normalize_planning_contact_type(contact_channel)
                or normalize_role(contact_channel)
            ),
            "date_time": str(data.get("date_time") or "").strip(),
            "result": str(data.get("result") or "").strip(),
            "comment": str(data.get("comment") or "").strip(),
            "customer_contact_person": str(
                data.get("customer_contact_person") or ""
            ).strip(),
            "freezers": {
                field: bool(is_checked_value(freezer_values.get(field)))
                for field in FREEZER_COLUMNS
            },
            "follow_up": {
                "enabled": bool(follow_up_enabled),
                "contact_type": str(follow_up_type or "").strip(),
                "scheduled_at": (
                    follow_up_at.isoformat(timespec="minutes")
                    if follow_up_at else ""
                ),
                "note": str(follow_up_note or "").strip(),
                "mirrored_date": str(
                    mirrored_follow_up_date or ""
                ).strip(),
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    payload_fingerprint = stable_planning_uuid(
        "planned-contact-payload",
        canonical,
    )
    return stable_planning_uuid(
        "planned-contact",
        (owner or {}).get("user_name"),
        planned_activity_id,
        payload_fingerprint,
    )


def ensure_followup_source_contact_id(
    spreadsheet,
    *,
    customer_name,
    customer_id="",
    customer_number="",
    source_contact_id="",
    owner=None,
):
    sheet = get_worksheet(spreadsheet, "sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    requested = str(source_contact_id or "").strip()
    customers = get_customer_rows(spreadsheet)
    customer_lookup = CustomerLookup(customers)
    target_customer = resolve_customer(
        customers,
        customer_id=customer_id,
        customer_number=customer_number,
        customer_name=customer_name,
        customer_lookup=customer_lookup,
    )
    candidate = None
    for row_index, row in worksheet_snapshot(
        sheet,
        expected_columns=CONTACT_COLUMNS,
    )[1]:
        if target_customer is None or related_row_customer(
            row, customers, customer_lookup=customer_lookup
        ) is not target_customer:
            continue
        if owner and not customer_owned_by_user(target_customer, owner):
            continue
        contact_id = str(row.get("contact_id") or "").strip()
        if requested and contact_id == requested:
            candidate = (row_index, row)
            break
        if not requested and str(row.get("follow_up_date") or "").strip():
            if candidate is None:
                candidate = (row_index, row)
                continue
            current_dt = parse_datetime_value(row.get("date_time")) or datetime.min
            previous_dt = parse_datetime_value(candidate[1].get("date_time")) or datetime.min
            if (current_dt, row_index) > (previous_dt, candidate[0]):
                candidate = (row_index, row)

    if candidate is None:
        return ""
    row_index, row = candidate
    contact_id = str(row.get("contact_id") or "").strip()
    if not contact_id:
        contact_id = stable_planning_uuid(
            "legacy-contact",
            row_index,
            str((target_customer or {}).get("customer_id") or "")
            or normalize_key(customer_name),
            row.get("date_time"),
        )
        update_sheet_row(
            sheet,
            row_index,
            headers,
            {"contact_id": contact_id},
        )
    return contact_id


def sync_followup_date_to_source_contact(
    spreadsheet,
    source_contact_id,
    follow_up_date,
):
    requested = str(source_contact_id or "").strip()
    if not requested:
        return False
    sheet = get_worksheet(spreadsheet, "sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    row_index, headers, _ = find_sheet_row(
        sheet,
        "contact_id",
        requested,
    )
    if not row_index:
        return False
    update_sheet_row(
        sheet,
        row_index,
        headers,
        {"follow_up_date": str(follow_up_date or "").strip()},
    )
    return True


def sync_planned_followup_mirror(spreadsheet, activity):
    if str(activity.get("source") or "").strip().casefold() != "follow_up":
        return True
    source_contact_id = str(
        activity.get("source_contact_id") or ""
    ).strip()
    if not source_contact_id:
        return False
    follow_up_date = ""
    if str(activity.get("status") or "planned").strip().casefold() == "planned":
        scheduled = parse_planning_datetime(activity.get("scheduled_at"))
        if scheduled is None:
            return False
        follow_up_date = scheduled.date().isoformat()
    return sync_followup_date_to_source_contact(
        spreadsheet,
        source_contact_id,
        follow_up_date,
    )


def planning_day_summaries(
    start_date,
    end_date,
    activities,
    unplanned_contacts=(),
):
    summaries = []
    current = start_date
    while current <= end_date:
        day_items = [
            item
            for item in activities
            if parse_planning_datetime(item.get("scheduled_at"))
            and parse_planning_datetime(item.get("scheduled_at")).date() == current
        ]
        active = [item for item in day_items if item.get("status") != "cancelled"]
        day_unplanned = [
            item
            for item in unplanned_contacts
            if (
                parse_date_value(
                    item.get("latest_contact_date")
                    or item.get("date_time")
                )
                == current
            )
        ]
        display_count = len(active) + len(day_unplanned)
        summary = {
            "date": current.isoformat(),
            "activity_count": display_count,
            "display_count": display_count,
            "planned_activity_count": len(active),
            "unplanned_count": len(day_unplanned),
            "total": display_count,
            "visit": sum(item.get("contact_type") == "visit" for item in active),
            "phone": sum(item.get("contact_type") == "phone" for item in active),
            "email": sum(item.get("contact_type") == "email" for item in active),
            "planned": sum(item.get("status") == "planned" for item in active),
            "completed": sum(item.get("status") == "completed" for item in active),
            "skipped": sum(item.get("status") == "skipped" for item in active),
            "cancelled": sum(item.get("status") == "cancelled" for item in day_items),
            "overdue": sum(bool(item.get("overdue")) for item in active),
        }
        summaries.append(summary)
        current += timedelta(days=1)
    return summaries


def public_unplanned_contact(row, row_index):
    parsed = parse_planning_datetime(row.get("date_time"))
    if parsed is None:
        parsed_naive = parse_datetime_value(row.get("date_time"))
        if parsed_naive:
            parsed = parsed_naive.replace(tzinfo=STOCKHOLM_ZONE)
    contact_id = str(row.get("contact_id") or "").strip()
    return {
        "contact_id": contact_id or f"legacy-row-{row_index}",
        "contact_row": row_index,
        "planned_activity_id": "",
        "customer": str(row.get("customer") or "").strip(),
        "customer_id": str(row.get("customer_id") or "").strip(),
        "customer_key": normalize_key(row.get("customer")),
        "sales_person": str(row.get("sales_person") or "").strip(),
        "contact_type": normalize_planning_contact_type(row.get("contact_channel")),
        "contact_type_label": planning_contact_label(row.get("contact_channel")),
        "contact_channel": str(row.get("contact_channel") or "").strip(),
        "date_time": (
            parsed.isoformat(timespec="minutes")
            if parsed else str(row.get("date_time") or "").strip()
        ),
        "result": str(row.get("result") or "").strip(),
        "comment": str(row.get("comment") or "").strip(),
        "customer_contact_person": str(
            row.get("customer_contact_person") or ""
        ).strip(),
        "unplanned": True,
    }


def legacy_followup_identity(row):
    return "|".join([
        str(row.get("customer_id") or "").strip(),
        normalize_key(row.get("customer")),
        str(parse_date_value(row.get("follow_up_date")) or ""),
        normalize_key(row.get("comment")),
    ])


def build_unscheduled_followup_groups(
    *,
    indexed_contacts,
    activities,
    customers,
    selected_start,
    selected_end,
    today,
):
    """Return the owner's unresolved follow-ups independently of selected week."""
    customer_lookup = CustomerLookup(customers)

    def customer_identity(row):
        customer = related_row_customer(
            row, customers, customer_lookup=customer_lookup
        )
        if not customer:
            return None
        customer_id = str(customer.get("customer_id") or "").strip()
        return ("id", customer_id) if customer_id else None

    latest_contact_by_customer = {}
    for row_index, row in indexed_contacts:
        identity = customer_identity(row)
        contact_date = parse_date_value(row.get("date_time"))
        if identity is None or contact_date is None:
            continue
        marker = (contact_date, row_index)
        if marker > latest_contact_by_customer.get(identity, (date.min, 0)):
            latest_contact_by_customer[identity] = marker

    future_planned_customers = set()
    for activity in activities:
        if str(activity.get("status") or "planned").strip().casefold() != "planned":
            continue
        scheduled = parse_planning_datetime(activity.get("scheduled_at"))
        identity = customer_identity(activity)
        if identity is not None and scheduled and scheduled.date() >= today:
            future_planned_customers.add(identity)

    booked_source_ids = set()
    booked_legacy = set()
    for activity in activities:
        if str(activity.get("status") or "planned").strip().casefold() not in {
            "planned", "completed"
        }:
            continue
        source_id = str(activity.get("source_contact_id") or "").strip()
        if source_id:
            booked_source_ids.add(source_id)
        else:
            booked_legacy.add("|".join([
                str(activity.get("customer_id") or "").strip(),
                normalize_key(activity.get("customer")),
                str(
                    (
                        parse_planning_datetime(activity.get("scheduled_at"))
                        or datetime.min.replace(tzinfo=STOCKHOLM_ZONE)
                    ).date()
                ),
                normalize_key(activity.get("note")),
            ]))

    overdue = []
    upcoming = []
    upcoming_limit = today + timedelta(days=30)
    for row_index, row in indexed_contacts:
        follow_up_date = parse_date_value(row.get("follow_up_date"))
        if not follow_up_date:
            continue
        identity = customer_identity(row)
        latest_contact = latest_contact_by_customer.get(identity)
        if (
            latest_contact
            and latest_contact[0] >= follow_up_date
            and latest_contact[1] != row_index
        ):
            continue
        if identity in future_planned_customers:
            continue
        source_contact_id = str(row.get("contact_id") or "").strip()
        if source_contact_id and source_contact_id in booked_source_ids:
            continue
        if not source_contact_id and legacy_followup_identity(row) in booked_legacy:
            continue
        in_upcoming_window = (
            today <= follow_up_date <= upcoming_limit
            or selected_start <= follow_up_date <= selected_end
        )
        if follow_up_date >= today and not in_upcoming_window:
            continue

        customer_id = str(row.get("customer_id") or "").strip()
        customer = related_row_customer(
            row, customers, customer_lookup=customer_lookup
        )
        item = {
            "customer_id": (
                str(customer.get("customer_id") or "").strip()
                if customer else customer_id
            ),
            "customer": str(row.get("customer") or "").strip(),
            "customer_key": (
                str(customer.get("customer_id") or "").strip()
                if customer else normalize_key(row.get("customer"))
            ),
            "customer_row": customer.get("row") if customer else None,
            "customer_number": (
                str(customer.get("customer_number") or "").strip()
                if customer else ""
            ),
            "follow_up_date": follow_up_date.isoformat(),
            "source_contact_id": source_contact_id,
            "source_contact_row": row_index,
            "contact_type": (
                normalize_planning_contact_type(row.get("contact_channel"))
                or "visit"
            ),
            "note": str(row.get("comment") or "").strip(),
            "days_overdue": max(0, (today - follow_up_date).days),
            "priority_score": {
                "A": 3,
                "B": 2,
                "C": 1,
            }.get(
                str((customer or {}).get("customer_segment") or "")
                .strip()
                .upper()[:1],
                0,
            ),
        }
        (overdue if follow_up_date < today else upcoming).append(item)

    overdue.sort(key=lambda item: (
        item["follow_up_date"],
        -float(item.get("priority_score") or 0),
        normalize_key(item.get("customer")),
    ))
    upcoming.sort(key=lambda item: (
        item["follow_up_date"],
        -float(item.get("priority_score") or 0),
        normalize_key(item.get("customer")),
    ))
    return overdue, upcoming


def planning_preview_serializer():
    secret = str(app.config.get("SECRET_KEY") or "").strip()
    return URLSafeTimedSerializer(secret, salt="polarbar-planning-route-preview-v1")


def blocked_recipient_reasons(recipient_rows):
    reasons = {}
    for row in recipient_rows:
        email = normalize_email(row.get("intended_email"))
        if not email:
            continue
        bounce_type = str(row.get("bounce_type", "")).strip().casefold()
        if str(row.get("unsubscribed_at", "")).strip():
            reasons[email] = "Avregistrerad i Brevo"
        elif str(row.get("blocked_at", "")).strip():
            reasons[email] = "Blockerad i Brevo"
        elif bounce_type in {"hardbounce", "hard bounce", "invalid", "spam"}:
            reasons[email] = "Permanent studs eller ogiltig adress"
    return reasons


def build_recipient_options(customer, latest_order, recipient_rows):
    order_emails = split_email_values(customer.get("email_last_order"))
    combined = split_email_values(customer.get("email_last_order"), customer.get("email"))
    order_keys = {normalize_email(item["email"]) for item in order_emails}
    blocked = blocked_recipient_reasons(recipient_rows)
    recipients = []
    for item in combined:
        if not item["valid"]:
            continue
        email = item["email"]
        key = normalize_email(email)
        source = "email_last_order" if key in order_keys else "email"
        greeting = recipient_greeting_name(
            email,
            customer.get("name", ""),
        )
        blocked_reason = blocked.get(key, "")
        recipients.append({
            "email": email,
            "source": source,
            "valid": bool(item["valid"]),
            "selected": bool(item["valid"] and not blocked_reason),
            "greeting_name": greeting,
            "blocked_reason": blocked_reason,
        })
    return recipients


def build_email_proposal_warnings(customer_name, latest_order, contact_rows, message_rows, created_at=None):
    warnings = []
    customer_key = normalize_key(customer_name)
    today = stockholm_today()

    recent_contacts = []
    for row in contact_rows:
        if normalize_key(row.get("customer")) != customer_key:
            continue
        if str(row.get("email_id", "")).strip():
            continue
        registered = parse_datetime_value(row.get("date_time"))
        if registered and (today - registered.date()).days <= 7:
            recent_contacts.append(registered)
    if recent_contacts:
        latest = max(recent_contacts)
        warnings.append({
            "code": "recent_contact",
            "message": f"En säljkontakt registrerades {latest.strftime('%Y-%m-%d')}.",
        })

    recent_messages = []
    for row in message_rows:
        if normalize_key(row.get("customer")) != customer_key or is_yes(row.get("is_test")):
            continue
        if str(row.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue
        sent_at = parse_datetime_value(row.get("sent_at"))
        if sent_at and (today - sent_at.date()).days <= 10:
            recent_messages.append(sent_at)
    if recent_messages:
        latest = max(recent_messages)
        warnings.append({
            "code": "recent_reminder",
            "message": f"Ett mejlförslag skickades {latest.strftime('%Y-%m-%d')}.",
        })

    created = parse_datetime_value(created_at)
    latest_order_date = parse_date_value(latest_order.get("order_date"))
    if created and latest_order_date and latest_order_date > created.date():
        warnings.append({
            "code": "new_order",
            "message": f"En ny order registrerades {latest_order_date.isoformat()} efter att utkastet skapades.",
        })
    return warnings


def build_reminder_warnings(customer_name, latest_order, contact_rows, message_rows, created_at=None):
    """Backward-compatible alias for the generic proposal warnings."""
    return build_email_proposal_warnings(
        customer_name, latest_order, contact_rows, message_rows, created_at=created_at
    )


def latest_live_email_proposals_by_customer(message_rows):
    latest = {}
    for row in message_rows:
        if is_yes(row.get("is_test")):
            continue
        if str(row.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue
        customer_key = normalize_key(row.get("customer"))
        sent_at = parse_datetime_value(row.get("sent_at"))
        if not customer_key or not sent_at:
            continue
        if customer_key not in latest or sent_at > latest[customer_key]:
            latest[customer_key] = sent_at
    return latest


def latest_live_reminders_by_customer(message_rows):
    """Backward-compatible alias; all V1 email types share the same cooldown."""
    return latest_live_email_proposals_by_customer(message_rows)


def build_email_proposal_status(customer, priority, relationship, latest_live_proposals,
                                blocked_recipients, today):
    relationship = relationship or {}
    proposal_type = normalize_proposal_type(relationship.get("email_type"))
    blockers = []
    if customer_is_cancelled(customer):
        blockers.append("customer_cancelled")

    recipient_candidates = split_email_values(customer.get("email_last_order"), customer.get("email"))
    usable_recipients = {
        normalize_email(item.get("email"))
        for item in recipient_candidates
        if item.get("valid") and normalize_email(item.get("email")) not in blocked_recipients
    }
    if not usable_recipients:
        blockers.append("no_usable_recipient")

    order_count = int(parse_number_value(priority.get("order_count"), 0) or 0)
    has_prior_order = bool(relationship.get("has_prior_order", order_count > 0))
    action_type = str((priority.get("next_action") or {}).get("action_type", "")).strip()
    if action_type in {"negative_reactivation", "follow_up", "scheduled_followup"}:
        blockers.append("other_followup_takes_precedence")

    reason = ""
    if proposal_type == "reminder":
        if not has_prior_order:
            blockers.append("no_prior_order")
        days_since_delivery = parse_number_value(
            relationship.get("days_since_delivery", priority.get("days_since_delivery")), None
        )
        overdue_days = parse_number_value(priority.get("overdue_days"), None)
        expected_cycle = parse_number_value(priority.get("expected_cycle_days"), None)
        if days_since_delivery is not None and expected_cycle is not None:
            due_after_days = min(
                int(expected_cycle) + EMAIL_PROPOSAL_GRACE_DAYS,
                EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS,
            )
            if days_since_delivery < due_after_days:
                blockers.append("not_due_yet")
            else:
                reason = f"{int(days_since_delivery)} dagar sedan senaste leverans"
        elif overdue_days is None or overdue_days < EMAIL_PROPOSAL_GRACE_DAYS:
            blockers.append("not_due_yet")
        else:
            reason = f"{int(overdue_days)} dagar efter förväntat återköpsdatum"
    elif proposal_type == "reactivation":
        if not has_prior_order:
            blockers.append("no_prior_order")
        days_since_delivery = relationship.get("days_since_delivery")
        reason = (
            f"{int(days_since_delivery)} dagar sedan senaste leverans"
            if days_since_delivery is not None and days_since_delivery >= 0
            else "Tidigare kund utan leverans de senaste 60 dagarna"
        )
    else:
        if has_prior_order:
            blockers.append("has_prior_order")
        reason = "Ingen tidigare order"

    latest_contact_date = parse_date_value(priority.get("latest_contact_date"))
    if latest_contact_date:
        days_since_contact = (today - latest_contact_date).days
        if 0 <= days_since_contact <= EMAIL_PROPOSAL_CONTACT_COOLDOWN_DAYS:
            blockers.append("recent_sales_contact")

    latest_sent = latest_live_proposals.get(normalize_key(customer.get("customer")))
    if latest_sent:
        days_since_sent = (today - latest_sent.date()).days
        if 0 <= days_since_sent <= EMAIL_PROPOSAL_SENT_COOLDOWN_DAYS:
            blockers.append("recent_email_proposal")

    return {
        "due": not blockers,
        "email_type": proposal_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[proposal_type],
        "reason": reason if not blockers else "",
        "blockers": blockers,
        "eligible_recipient_count": len(usable_recipients),
        "latest_sent_at": format_datetime_value(latest_sent) if latest_sent else "",
    }


def build_reminder_email_status(customer, priority, latest_live_reminders, blocked_recipients, today):
    relationship = {
        "email_type": "reminder",
        "has_prior_order": int(parse_number_value(priority.get("order_count"), 0) or 0) > 0,
        "days_since_delivery": priority.get("days_since_delivery"),
    }
    status = build_email_proposal_status(
        customer, priority, relationship, latest_live_reminders, blocked_recipients, today
    )
    if "recent_email_proposal" in status["blockers"]:
        status["blockers"] = [
            "recent_reminder_email" if item == "recent_email_proposal" else item
            for item in status["blockers"]
        ]
    return status


def build_email_proposal_draft(
    spreadsheet,
    row_number,
    draft_id=None,
    created_at=None,
    *,
    customer=None,
    customers=None,
):
    customer = customer or get_customer_by_row(spreadsheet, row_number)
    if not customer:
        return None
    order_rows = get_order_rows(spreadsheet)
    contact_rows = get_contact_rows(spreadsheet)
    message_rows, recipient_rows, _ = get_email_rows(
        spreadsheet, include_events=False
    )
    if customers is not None:
        customer_lookup = CustomerLookup(customers)
        order_rows = related_rows_for_customer(
            order_rows,
            customers,
            customer,
            name_key="Customer",
            number_key="Customer number",
            customer_lookup=customer_lookup,
        )
        contact_rows = related_rows_for_customer(
            contact_rows,
            customers,
            customer,
            customer_lookup=customer_lookup,
        )
        message_rows = related_rows_for_customer(
            message_rows,
            customers,
            customer,
            customer_lookup=customer_lookup,
        )
        visible_email_ids = {
            str(message.get("email_id") or "").strip()
            for message in message_rows
        }
        recipient_rows = [
            recipient for recipient in recipient_rows
            if str(recipient.get("email_id") or "").strip()
            in visible_email_ids
        ]
    latest_order = build_latest_order_context(order_rows, customer.get("customer"))
    relationship = classify_customer_relationship(
        order_rows,
        customer.get("customer"),
        today=stockholm_today(),
        recent_days=EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS,
    )
    proposal_type = relationship["email_type"]
    settings = get_settings(spreadsheet)
    product_catalog = build_settings_product_catalog(settings)
    template = get_email_proposal_template_config(settings, proposal_type, product_catalog)
    if template["order_mode"] == "fixed":
        suggested_rows = template["order_rows"]
    else:
        suggested_rows = canonicalize_proposal_order_rows(
            latest_order.get("order_rows", []), product_catalog
        )

    unique_store_count = count_unique_order_customers(order_rows)
    copy = build_email_proposal_copy(
        proposal_type,
        customer.get("customer"),
        latest_delivery_date=latest_order.get("delivery_date"),
        has_order_rows=bool(suggested_rows),
        unique_store_count=unique_store_count,
        template=template,
    )
    created_at = created_at or now_text()
    product_setting = EMAIL_PROPOSAL_PRODUCT_SETTINGS[proposal_type]
    product_sheet_url = safe_http_url(settings.get(product_setting))
    fallback_product_url = safe_http_url(settings.get("reminder_product_sheet_url"))
    used_product_fallback = bool(not product_sheet_url and fallback_product_url)
    if used_product_fallback:
        product_sheet_url = fallback_product_url
    stockfiller_url = safe_http_url(
        settings.get("email_proposal_stockfiller_url") or settings.get("reminder_stockfiller_url")
    )
    notices = []
    if not product_sheet_url:
        notices.append(
            f"Produktbladslänken för {EMAIL_PROPOSAL_TYPES[proposal_type].lower()} saknas eller är ogiltig "
            "och utelämnas från mejlet."
        )
    elif used_product_fallback and proposal_type != "reminder":
        notices.append(
            f"Inställningen {product_setting} saknas. Det vanliga produktbladet används tills vidare."
        )
    if not stockfiller_url:
        notices.append("Stockfiller-länken saknas eller är ogiltig och utelämnas från mejlet.")
    if not product_catalog:
        notices.append("Inga sku_-artiklar finns i settings. Produktrader behöver fyllas i manuellt.")
    elif len(suggested_rows) < 4 and proposal_type in {"reactivation", "new_customer"}:
        notices.append("En eller flera av standardartiklarna saknas i settings.")

    user = current_user()
    return {
        "draft_id": draft_id or str(uuid.uuid4()),
        "created_at": created_at,
        "send_mode": EMAIL_SEND_MODE,
        "test_recipient": EMAIL_TEST_RECIPIENT if EMAIL_SEND_MODE != "live" else "",
        "customer": {
            "row": row_number,
            "customer": str(customer.get("customer", "")).strip(),
            "customer_id": str(customer.get("customer_id", "")).strip(),
            "customer_number": str(customer.get("customer_number", "")).strip(),
            "cancelled": customer_is_cancelled(customer),
        },
        "email_type": proposal_type,
        "email_type_label": EMAIL_PROPOSAL_TYPES[proposal_type],
        "relationship": relationship,
        "latest_order_reference": latest_order.get("reference", ""),
        "latest_delivery_date": latest_order.get("delivery_date", ""),
        "recipients": build_recipient_options(customer, latest_order, recipient_rows),
        "subject": copy["subject"],
        "intro_text": copy["intro_text"],
        "closing_text": copy["closing_text"],
        "order_rows": suggested_rows,
        "product_catalog": product_catalog,
        "links": {
            "product_sheet_url": product_sheet_url,
            "stockfiller_url": stockfiller_url,
        },
        "cta_labels": {
            "product_sheet": copy["product_sheet_label"],
            "stockfiller": copy["stockfiller_label"],
        },
        "stats": {
            "unique_order_customers": unique_store_count,
            "rounded_unique_order_customers": round_store_count_to_ten(unique_store_count),
        },
        "signature": public_user(user),
        "warnings": build_email_proposal_warnings(
            customer.get("customer"), latest_order, contact_rows, message_rows
        ),
        "notices": notices,
    }


def build_reminder_draft(spreadsheet, row_number, draft_id=None, created_at=None):
    """Backward-compatible alias returning the customer's current proposal type."""
    return build_email_proposal_draft(
        spreadsheet, row_number, draft_id=draft_id, created_at=created_at
    )


def send_brevo_transactional_email(*, sender, recipient_email, recipient_name, reply_to,
                                    subject, html_body, text_body, email_id,
                                    email_type="reminder"):
    api_key = str(os.environ.get("BREVO_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("BREVO_API_KEY saknas")
    response = requests.post(
        BREVO_SEND_URL,
        headers={"api-key": api_key, "accept": "application/json", "content-type": "application/json"},
        json={
            "sender": sender,
            "to": [{"email": recipient_email, "name": recipient_name or recipient_email}],
            "replyTo": reply_to,
            "subject": subject,
            "htmlContent": html_body,
            "textContent": text_body,
            "tags": ["store-tracker", f"proposal-{normalize_proposal_type(email_type)}", f"email-{email_id}"],
        },
        timeout=20,
    )
    if response.status_code >= 400:
        detail = response.text[:500] if response.text else f"HTTP {response.status_code}"
        raise RuntimeError(detail)
    try:
        payload = response.json()
    except (ValueError, requests.exceptions.JSONDecodeError):
        payload = {}
    message_id = normalize_message_id(payload.get("messageId"))
    if not message_id:
        raise RuntimeError("Brevo returnerade inget Message ID")
    return message_id


def build_sales_activity_for_email(spreadsheet, *, email_id, email_type,
                                   customer_name, customer_id="", user,
                                   recipients, partial):
    sheet = get_worksheet(spreadsheet, "sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    type_label = EMAIL_PROPOSAL_TYPES[normalize_proposal_type(email_type)]
    result = f"Mejlförslag delvis skickat – {type_label}" if partial else f"Mejlförslag skickat – {type_label}"
    row_data = {
        "date_time": now_text(),
        "sales_person": user.get("name") or user.get("user_name", ""),
        "customer": customer_name,
        "customer_id": str(customer_id or "").strip(),
        "contact_channel": "Mejl",
        "result": result,
        "comment": f"Mottagare: {', '.join(recipients)}",
        "customer_contact_person": "",
        "follow_up_date": "",
        "email_id": email_id,
    }
    append_dict_row(
        sheet,
        headers,
        row_data,
        value_input_option="USER_ENTERED",
        single_value_columns=FREEZER_COLUMNS,
    )


@app.before_request
def require_authenticated_session():
    public_endpoints = {
        "index", "images", "login", "get_session", "health",
        "brevo_webhook", "brevo_reconcile"
    }
    if request.method == "OPTIONS" or request.endpoint in public_endpoints:
        return None
    user = current_user()
    if not user.get("user_name"):
        return jsonify({"ok": False, "error": "authentication_required"}), 401
    g.current_user = user
    return None


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/images/<path:filename>")
def images(filename):
    return send_from_directory(IMAGE_DIR, filename)


@app.route("/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}
    try:
        spreadsheet = get_spreadsheet_with_retry()
        user = find_active_user(spreadsheet, data.get("user_name"))
    except Exception:
        app.logger.exception("Could not read users worksheet during login")
        return jsonify({"ok": False, "error": "user_store_unavailable"}), 503
    if not user or str(data.get("password") or "") != str(user.get("password") or ""):
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    profile = public_user(user)
    session.clear()
    session.permanent = True
    session["user"] = profile
    return jsonify({"ok": True, "user": profile})


@app.route("/session", methods=["GET"])
def get_session():
    profile = current_user()
    if not profile.get("user_name"):
        return jsonify({"ok": False, "authenticated": False}), 401
    spreadsheet = get_spreadsheet_with_retry()
    active_user = find_active_user(spreadsheet, profile.get("user_name"))
    if not active_user:
        session.clear()
        return jsonify({"ok": False, "authenticated": False}), 401
    profile = public_user(active_user)
    session["user"] = profile
    session.permanent = True
    return jsonify({"ok": True, "authenticated": True, "user": profile})


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"ok": True})


@app.route("/email-proposal-settings", methods=["GET"])
def get_email_proposal_settings():
    if not user_is_admin(current_user()):
        return jsonify({"ok": False, "error": "admin_required"}), 403
    spreadsheet = get_spreadsheet_with_retry()
    settings = get_settings(spreadsheet)
    product_catalog = build_settings_product_catalog(settings)
    templates = {
        proposal_type: get_email_proposal_template_config(
            settings, proposal_type, product_catalog
        )
        for proposal_type in EMAIL_PROPOSAL_TYPES
    }
    return jsonify({
        "ok": True,
        "templates": templates,
        "product_catalog": product_catalog,
        "placeholders": {
            "{{dagar}}": "Dagar sedan senaste leverans",
            "{{veckor}}": "Veckor sedan senaste leverans",
            "{{antal_butiker}}": "Avrundat antal butiker med order",
            "{{butiksnamn}}": "Butikens namn",
            "(namn)": "Mottagarens valda hälsningsnamn",
        },
    })


@app.route("/email-proposal-settings/<proposal_type>", methods=["PUT"])
def update_email_proposal_settings(proposal_type):
    if not user_is_admin(current_user()):
        return jsonify({"ok": False, "error": "admin_required"}), 403
    proposal_type = str(proposal_type or "").strip().casefold()
    if proposal_type not in EMAIL_PROPOSAL_TYPES:
        return jsonify({"ok": False, "error": "invalid_email_type"}), 404

    data = request.get_json(silent=True) or {}
    values = {
        field: str(data.get(field) or "").strip()
        for field in EMAIL_PROPOSAL_TEMPLATE_FIELDS
    }
    if not values["subject"] or not values["intro_text"]:
        return jsonify({"ok": False, "error": "missing_email_content"}), 400
    if not values["stockfiller_label"] or not values["product_sheet_label"]:
        return jsonify({"ok": False, "error": "missing_cta_label"}), 400
    if len(values["subject"]) > 250:
        return jsonify({"ok": False, "error": "subject_too_long"}), 400
    if len(values["intro_text"]) > 5000 or len(values["closing_text"]) > 5000:
        return jsonify({"ok": False, "error": "body_too_long"}), 400
    if len(values["stockfiller_label"]) > 80 or len(values["product_sheet_label"]) > 80:
        return jsonify({"ok": False, "error": "cta_label_too_long"}), 400

    spreadsheet = get_spreadsheet_with_retry()
    settings = get_settings(spreadsheet)
    product_catalog = build_settings_product_catalog(settings)
    order_mode = str(data.get("order_mode") or "fixed").strip().casefold()
    if proposal_type != "reminder":
        order_mode = "fixed"
    elif order_mode not in {"latest_order", "fixed"}:
        return jsonify({"ok": False, "error": "invalid_order_mode"}), 400
    order_rows = sanitize_template_order_rows(data.get("order_rows"), product_catalog)
    if order_mode == "fixed" and (
        not order_rows or any(not str(row.get("quantity", "")).strip() for row in order_rows)
    ):
        return jsonify({"ok": False, "error": "missing_order_rows"}), 400
    if order_mode == "latest_order":
        order_rows = []

    config = {
        **values,
        "order_mode": order_mode,
        "order_rows": order_rows,
    }
    save_email_proposal_template_config(spreadsheet, proposal_type, config)
    updated_settings = get_settings(spreadsheet)
    updated = get_email_proposal_template_config(
        updated_settings, proposal_type, product_catalog
    )
    return jsonify({"ok": True, "template": updated})


@app.route("/customers/<int:row>/email-proposal-draft", methods=["GET"])
@app.route("/customers/<int:row>/reminder-email-draft", methods=["GET"])
def get_email_proposal_draft(row):
    spreadsheet = get_spreadsheet_with_retry()
    customers = get_customer_rows(spreadsheet)
    customer = resolve_accessible_customer(
        customers, current_user(), row=row
    )
    if customer is None:
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    draft = build_email_proposal_draft(
        spreadsheet,
        row,
        customer=customer,
        customers=customers,
    )
    if not draft:
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    if draft["customer"]["cancelled"]:
        draft["notices"].insert(0, "Kunden är markerad som avslutad och mejlet kan inte skickas.")
    return jsonify({"ok": True, "draft": draft})


def sanitize_order_rows(rows):
    result = []
    for row in (rows or [])[:50]:
        product = str(row.get("product", "")).strip()[:250]
        quantity = str(row.get("quantity", "")).strip()[:30]
        unit = str(row.get("unit", "DFP")).strip()[:20] or "DFP"
        if product:
            result.append({
                "product": product,
                "quantity": quantity,
                "unit": unit,
                "new_for_customer": bool(row.get("new_for_customer")),
            })
    return result


@app.route("/customers/<int:row>/email-proposal/send", methods=["POST"])
@app.route("/customers/<int:row>/reminder-email/send", methods=["POST"])
def send_email_proposal(row):
    data = request.get_json(silent=True) or {}
    draft_id = str(data.get("draft_id", "")).strip()
    if not draft_id or len(draft_id) > 80:
        return jsonify({"ok": False, "error": "invalid_draft_id"}), 400

    with _active_send_lock:
        if draft_id in _active_send_ids:
            return jsonify({"ok": False, "error": "send_in_progress"}), 409
        _active_send_ids.add(draft_id)

    try:
        spreadsheet = get_spreadsheet_with_retry()
        customers = get_customer_rows(spreadsheet)
        customer = resolve_accessible_customer(
            customers, current_user(), row=row
        )
        if customer is None:
            return jsonify({"ok": False, "error": "customer_not_found"}), 404
        sheets = ensure_email_worksheets(spreadsheet)
        existing_row, _, existing = find_sheet_row(sheets[EMAIL_MESSAGES_SHEET], "email_id", draft_id)
        if existing_row:
            return jsonify({
                "ok": False,
                "error": "duplicate_send",
                "status": existing.get("status", ""),
            }), 409

        draft_created_at = str(data.get("created_at", "")).strip()
        current_draft = build_email_proposal_draft(
            spreadsheet,
            row,
            draft_id=draft_id,
            created_at=draft_created_at or now_text(),
            customer=customer,
            customers=customers,
        )
        if not current_draft:
            return jsonify({"ok": False, "error": "customer_not_found"}), 404
        if current_draft["customer"]["cancelled"]:
            return jsonify({"ok": False, "error": "customer_cancelled"}), 409

        requested_email_type = str(data.get("email_type", "")).strip()
        if requested_email_type and normalize_proposal_type(requested_email_type) != current_draft["email_type"]:
            return jsonify({
                "ok": False,
                "error": "email_type_changed",
                "email_type": current_draft["email_type"],
                "email_type_label": current_draft["email_type_label"],
            }), 409

        allowed = {normalize_email(item["email"]): item for item in current_draft["recipients"]}
        selected = []
        for item in data.get("recipients") or []:
            if not item.get("selected"):
                continue
            key = normalize_email(item.get("email"))
            allowed_item = allowed.get(key)
            if not allowed_item or not allowed_item.get("valid"):
                continue
            if allowed_item.get("blocked_reason"):
                return jsonify({
                    "ok": False,
                    "error": "recipient_blocked",
                    "email": allowed_item.get("email"),
                    "reason": allowed_item.get("blocked_reason"),
                }), 409
            selected.append({
                "email": allowed_item["email"],
                "greeting_name": first_name(item.get("greeting_name")),
            })
        if not selected:
            return jsonify({"ok": False, "error": "no_valid_recipients"}), 400

        warnings = list(current_draft.get("warnings") or [])
        if str(data.get("latest_order_reference", "")).strip() != str(current_draft.get("latest_order_reference", "")).strip():
            warnings.append({
                "code": "new_order",
                "message": "Kundens senaste order har ändrats sedan utkastet öppnades.",
            })
        unique_warnings = {item["code"]: item for item in warnings}
        warnings = list(unique_warnings.values())
        if warnings and not data.get("confirm_warnings"):
            return jsonify({
                "ok": False,
                "error": "warning_confirmation_required",
                "warnings": warnings,
            }), 409

        subject = str(data.get("subject", "")).strip()[:250]
        intro_text = str(data.get("intro_text", "")).strip()[:5000]
        closing_text = str(data.get("closing_text", "")).strip()[:5000]
        order_rows = sanitize_order_rows(data.get("order_rows"))
        if not subject or not intro_text:
            return jsonify({"ok": False, "error": "missing_email_content"}), 400

        product_sheet_url = safe_http_url(current_draft["links"].get("product_sheet_url"))
        stockfiller_url = safe_http_url(current_draft["links"].get("stockfiller_url"))
        product_sheet_label = (
            current_draft["cta_labels"].get("product_sheet") or "Se sortiment och priser"
        )
        stockfiller_label = (
            current_draft["cta_labels"].get("stockfiller") or "Beställ i Stockfiller"
        )
        email_type = current_draft["email_type"]
        user = current_user()
        if not is_valid_email(user.get("email")):
            return jsonify({"ok": False, "error": "invalid_sender_email"}), 400
        sender_name = str(user.get("name") or user.get("user_name") or "Polarbär").strip()
        sender = {"name": f"{sender_name} på Polarbär", "email": user["email"]}
        reply_to = {"name": user.get("name") or sender["name"], "email": user["email"]}
        is_test = EMAIL_SEND_MODE != "live"
        if is_test and not is_valid_email(EMAIL_TEST_RECIPIENT):
            return jsonify({"ok": False, "error": "invalid_test_recipient"}), 500

        first_rendered = render_email_proposal(
            greeting_name=selected[0].get("greeting_name"),
            subject=subject,
            intro_text=intro_text,
            closing_text=closing_text,
            order_rows=order_rows,
            product_sheet_url=product_sheet_url,
            stockfiller_url=stockfiller_url,
            sender=user,
            product_sheet_label=product_sheet_label,
            stockfiller_label=stockfiller_label,
        )
        created_at = draft_created_at or now_text()
        append_dict_row(sheets[EMAIL_MESSAGES_SHEET], EMAIL_MESSAGES_COLUMNS, {
            "email_id": draft_id,
            "customer": current_draft["customer"]["customer"],
            "customer_number": current_draft["customer"].get("customer_number", ""),
            "customer_id": current_draft["customer"].get("customer_id", ""),
            "email_type": email_type,
            "sender_user_name": user.get("user_name", ""),
            "sender_name": user.get("name", ""),
            "sender_email": user.get("email", ""),
            "subject": subject,
            "body_text": first_rendered["text"],
            "body_html": first_rendered["html"],
            "latest_order_reference": current_draft.get("latest_order_reference", ""),
            "latest_delivery_date": current_draft.get("latest_delivery_date", ""),
            "product_sheet_url": product_sheet_url,
            "stockfiller_url": stockfiller_url,
            "is_test": "Y" if is_test else "N",
            "recipient_count": len(selected),
            "status": "pending",
            "created_at": created_at,
            "sent_at": "",
        })

        successes = []
        failures = []
        for recipient in selected:
            intended_email = recipient["email"]
            actual_email = EMAIL_TEST_RECIPIENT if is_test else intended_email
            rendered = render_email_proposal(
                greeting_name=recipient.get("greeting_name"),
                subject=subject,
                intro_text=intro_text,
                closing_text=closing_text,
                order_rows=order_rows,
                product_sheet_url=product_sheet_url,
                stockfiller_url=stockfiller_url,
                sender=user,
                product_sheet_label=product_sheet_label,
                stockfiller_label=stockfiller_label,
            )
            outgoing_subject = rendered["subject"]
            if is_test:
                outgoing_subject = f"[TEST – avsett för {intended_email}] {outgoing_subject}"
            sent_at = now_text()
            message_id = ""
            error_text = ""
            try:
                message_id = send_brevo_transactional_email(
                    sender=sender,
                    recipient_email=actual_email,
                    recipient_name=recipient.get("greeting_name") or actual_email,
                    reply_to=reply_to,
                    subject=outgoing_subject,
                    html_body=rendered["html"],
                    text_body=rendered["text"],
                    email_id=draft_id,
                    email_type=email_type,
                )
                successes.append(intended_email)
            except Exception as exc:
                error_text = str(exc)[:500]
                failures.append({"email": intended_email, "error": error_text})
            append_dict_row(sheets[EMAIL_RECIPIENTS_SHEET], EMAIL_RECIPIENTS_COLUMNS, {
                "email_id": draft_id,
                "customer": current_draft["customer"]["customer"],
                "customer_id": current_draft["customer"].get("customer_id", ""),
                "intended_email": intended_email,
                "actual_email": actual_email,
                "greeting_name": recipient.get("greeting_name", ""),
                "brevo_message_id": message_id,
                "send_status": "sent" if message_id else "failed",
                "send_error": error_text,
                "sent_at": sent_at if message_id else "",
            })

        status = "failed" if not successes else ("partial" if failures else "sent")
        sent_at = now_text() if successes else ""
        message_row, message_headers, _ = find_sheet_row(sheets[EMAIL_MESSAGES_SHEET], "email_id", draft_id)
        if message_row:
            update_sheet_row(sheets[EMAIL_MESSAGES_SHEET], message_row, message_headers, {
                "status": status,
                "sent_at": sent_at,
            })
        if successes and not is_test:
            build_sales_activity_for_email(
                spreadsheet,
                email_id=draft_id,
                email_type=email_type,
                customer_name=current_draft["customer"]["customer"],
                customer_id=current_draft["customer"].get("customer_id", ""),
                user=user,
                recipients=successes,
                partial=bool(failures),
            )
            try:
                resolve_suggestions_for_email(
                    spreadsheet,
                    owner=user,
                    customer_id=current_draft["customer"].get("customer_id", ""),
                    email_id=draft_id,
                )
            except Exception:
                app.logger.exception(
                    "Could not resolve planning suggestion after sent email %s",
                    draft_id,
                )
        response_payload = {
            "ok": bool(successes),
            "email_id": draft_id,
            "email_type": email_type,
            "email_type_label": current_draft["email_type_label"],
            "status": status,
            "sent": successes,
            "failed": failures,
            "is_test": is_test,
            "test_recipient": EMAIL_TEST_RECIPIENT if is_test else "",
        }
        return jsonify(response_payload), (200 if successes else 502)
    finally:
        with _active_send_lock:
            _active_send_ids.discard(draft_id)


def _event_semantic_key(event):
    return email_event_key(
        event.get("brevo_message_id"),
        event.get("event_type"),
        event.get("event_time"),
        event.get("url"),
        event.get("actual_email"),
    )


def _recipient_summary(recipient, event_rows):
    """Derive summary fields from the append-only raw log, making retries idempotent."""
    ordered = sorted(event_rows, key=lambda row: str(row.get("event_time") or ""))
    times_by_type = defaultdict(list)
    for event in ordered:
        event_type = str(event.get("event_type") or "").strip().casefold()
        if (
            event_type == "opened"
            and "transac-phishing-consumer" in str(event.get("payload_json") or "").casefold()
        ):
            # Brevo's own security scanner loads the tracking pixel immediately.
            # Keep the raw event for audit, but do not count it as a customer open.
            continue
        event_time = str(event.get("event_time") or "").strip()
        if event_time:
            times_by_type[event_type].append(event_time)

    updates = {
        "delivered_at": (times_by_type["delivered"] or [""])[0],
        "first_opened_at": (times_by_type["opened"] or [""])[0],
        "last_opened_at": (times_by_type["opened"] or [""])[-1],
        "open_count": len(times_by_type["opened"]),
        "product_sheet_first_clicked_at": (times_by_type["product_sheet_clicked"] or [""])[0],
        "product_sheet_last_clicked_at": (times_by_type["product_sheet_clicked"] or [""])[-1],
        "product_sheet_click_count": len(times_by_type["product_sheet_clicked"]),
        "stockfiller_first_clicked_at": (times_by_type["stockfiller_clicked"] or [""])[0],
        "stockfiller_last_clicked_at": (times_by_type["stockfiller_clicked"] or [""])[-1],
        "stockfiller_click_count": len(times_by_type["stockfiller_clicked"]),
        "bounce_type": "",
        "blocked_at": "",
        "unsubscribed_at": (times_by_type["unsubscribed"] or [""])[-1],
        "last_event_at": str(ordered[-1].get("event_time") or "") if ordered else "",
    }
    for event in ordered:
        event_type = str(event.get("event_type") or "").strip().casefold()
        if event_type in {"hardbounce", "invalid", "blocked", "spam"}:
            updates["bounce_type"] = event_type
        if event_type in {"blocked", "spam"}:
            updates["blocked_at"] = str(event.get("event_time") or "")
    return {**recipient, **updates}


def process_brevo_events(spreadsheet, sheets, payloads):
    """Persist a batch with one read/append/update cycle and semantic deduplication."""
    message_headers, message_rows = worksheet_snapshot(
        sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS
    )
    recipient_headers, recipient_rows = worksheet_snapshot(
        sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
    )
    _, stored_event_rows = worksheet_snapshot(
        sheets[EMAIL_EVENTS_SHEET], expected_columns=EMAIL_EVENTS_COLUMNS
    )
    messages_by_email_id = {row.get("email_id", ""): row for _, row in message_rows}
    recipients_by_message_id = {
        normalize_message_id(row.get("brevo_message_id")): (row_index, row)
        for row_index, row in recipient_rows
        if normalize_message_id(row.get("brevo_message_id"))
    }
    all_events = [row for _, row in stored_event_rows]
    existing_keys = {_event_semantic_key(row) for row in all_events}
    new_events = []
    affected_message_ids = set()

    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        message_id = normalize_message_id(
            payload.get("message-id") or payload.get("messageId") or payload.get("message_id")
        )
        recipient_info = recipients_by_message_id.get(message_id)
        recipient = recipient_info[1] if recipient_info else {}
        email_id = recipient.get("email_id", "")
        message = messages_by_email_id.get(email_id, {})
        event_type = normalize_brevo_event(payload)
        url = str(payload.get("link") or payload.get("url") or "").strip()
        if event_type == "clicked":
            event_type = classify_clicked_url(
                url,
                message.get("product_sheet_url", ""),
                message.get("stockfiller_url", ""),
            )
        actual_email = recipient.get("actual_email") or payload.get("email", "")
        event = {
            "received_at": now_text(),
            "event_time": brevo_event_time(payload),
            "email_id": email_id,
            "brevo_message_id": message_id,
            "intended_email": recipient.get("intended_email", ""),
            "actual_email": actual_email,
            "event_type": event_type,
            "url": url,
            "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True)[:45000],
        }
        event["event_key"] = _event_semantic_key(event)
        if message_id:
            affected_message_ids.add(message_id)
        if event["event_key"] in existing_keys:
            continue
        existing_keys.add(event["event_key"])
        new_events.append(event)
        all_events.append(event)

    if new_events:
        event_sheet = sheets[EMAIL_EVENTS_SHEET]
        append_dict_rows(
            event_sheet,
            EMAIL_EVENTS_COLUMNS,
            new_events,
            value_input_option="RAW",
        )

    events_by_message_id = defaultdict(list)
    for event in all_events:
        message_id = normalize_message_id(event.get("brevo_message_id"))
        if message_id in affected_message_ids:
            events_by_message_id[message_id].append(event)

    recipient_updates = []
    for message_id in affected_message_ids:
        recipient_info = recipients_by_message_id.get(message_id)
        if not recipient_info:
            continue
        row_index, recipient = recipient_info
        recipient_updates.append((row_index, _recipient_summary(recipient, events_by_message_id[message_id])))
    batch_update_sheet_rows(sheets[EMAIL_RECIPIENTS_SHEET], recipient_headers, recipient_updates)
    return len(new_events)


def process_brevo_event(spreadsheet, sheets, payload):
    return bool(process_brevo_events(spreadsheet, sheets, [payload]))


def _process_brevo_batch_with_retry(payloads):
    def operation():
        spreadsheet = get_spreadsheet_with_retry()
        sheets = ensure_email_worksheets(spreadsheet)
        with _brevo_processing_lock:
            return process_brevo_events(spreadsheet, sheets, payloads)

    return run_with_retry(operation, label="Brevo event batch")


def _brevo_event_worker():
    while True:
        first = _brevo_event_queue.get()
        batch = [first]
        try:
            # Small coalescing window greatly reduces Sheets calls during webhook bursts.
            time.sleep(0.05)
            while len(batch) < 100:
                try:
                    batch.append(_brevo_event_queue.get_nowait())
                except Empty:
                    break
            _process_brevo_batch_with_retry(batch)
        except Exception:
            app.logger.exception("Brevo event batch failed after retries")
        finally:
            for _ in batch:
                _brevo_event_queue.task_done()


def fetch_brevo_events(message_id):
    api_key = str(os.environ.get("BREVO_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("BREVO_API_KEY is missing")
    normalized_id = normalize_message_id(message_id)

    def operation():
        response = requests.get(
            BREVO_EVENTS_URL,
            headers={"api-key": api_key, "accept": "application/json"},
            params={"messageId": f"<{normalized_id}>", "limit": 500, "sort": "asc"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        events = payload.get("events", []) if isinstance(payload, dict) else []
        for event in events:
            event.setdefault("messageId", normalized_id)
        return events

    return run_with_retry(operation, attempts=4, base_delay=1, label=f"Brevo API {normalized_id}")


def reconcile_recent_brevo_events(*, days=None, max_recipients=None):
    if not _brevo_reconcile_lock.acquire(blocking=False):
        return {"ok": True, "status": "already_running"}
    try:
        days = int(days or BREVO_RECONCILE_DAYS)
        max_recipients = int(max_recipients or BREVO_RECONCILE_MAX_RECIPIENTS)

        def load_recent_recipients():
            spreadsheet = get_spreadsheet_with_retry()
            sheets = ensure_email_worksheets(spreadsheet)
            return worksheet_snapshot(
                sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
            )[1]

        recipient_rows = run_with_retry(
            load_recent_recipients, label="Brevo reconciliation Sheets read"
        )
        cutoff = stockholm_today() - timedelta(days=max(1, min(30, days)))
        candidates = []
        for _, recipient in recipient_rows:
            message_id = normalize_message_id(recipient.get("brevo_message_id"))
            sent_at = parse_datetime_value(recipient.get("sent_at"))
            if (
                message_id
                and str(recipient.get("send_status") or "").casefold() == "sent"
                and sent_at
                and sent_at.date() >= cutoff
            ):
                candidates.append((sent_at, message_id))
        candidates.sort(reverse=True)

        fetched = []
        failures = []
        seen_message_ids = set()
        for _, message_id in candidates:
            if message_id in seen_message_ids or len(seen_message_ids) >= max_recipients:
                continue
            seen_message_ids.add(message_id)
            try:
                fetched.extend(fetch_brevo_events(message_id))
            except Exception as exc:
                failures.append({"message_id": message_id, "error": str(exc)[:250]})
                app.logger.warning("Could not reconcile Brevo message %s: %s", message_id, exc)

        inserted = _process_brevo_batch_with_retry(fetched) if fetched else 0
        return {
            "ok": True,
            "status": "completed",
            "checked_recipients": len(seen_message_ids),
            "fetched_events": len(fetched),
            "inserted_events": inserted,
            "failures": failures,
        }
    finally:
        _brevo_reconcile_lock.release()


def _brevo_reconcile_worker():
    while True:
        time.sleep(BREVO_RECONCILE_INTERVAL_SECONDS)
        try:
            reconcile_recent_brevo_events()
        except Exception:
            app.logger.exception("Scheduled Brevo reconciliation failed")


def start_brevo_background_workers():
    global _brevo_workers_started
    with _brevo_worker_start_lock:
        if _brevo_workers_started:
            return
        threading.Thread(target=_brevo_event_worker, name="brevo-events", daemon=True).start()
        threading.Thread(target=_brevo_reconcile_worker, name="brevo-reconcile", daemon=True).start()
        _brevo_workers_started = True


@app.route("/api/brevo/webhook/<secret>", methods=["POST"])
def brevo_webhook(secret):
    expected = str(os.environ.get("BREVO_WEBHOOK_SECRET", "")).strip()
    if not expected:
        return jsonify({"ok": False, "error": "webhook_not_configured"}), 503
    if secret != expected:
        return jsonify({"ok": False, "error": "not_found"}), 404
    payload = request.get_json(silent=True)
    events = payload if isinstance(payload, list) else [payload or {}]
    start_brevo_background_workers()
    queued = 0
    try:
        for event in events:
            _brevo_event_queue.put_nowait(event)
            queued += 1
    except Full:
        return jsonify({"ok": False, "error": "event_queue_full", "queued": queued}), 503
    return jsonify({"ok": True, "queued": queued}), 202


@app.route("/api/brevo/reconcile/<secret>", methods=["POST"])
def brevo_reconcile(secret):
    expected = str(os.environ.get("BREVO_WEBHOOK_SECRET", "")).strip()
    if not expected:
        return jsonify({"ok": False, "error": "webhook_not_configured"}), 503
    if secret != expected:
        return jsonify({"ok": False, "error": "not_found"}), 404
    result = reconcile_recent_brevo_events(
        days=request.args.get("days", type=int),
        max_recipients=request.args.get("max_recipients", type=int),
    )
    return jsonify(result), (202 if result.get("status") == "already_running" else 200)


@app.route("/customers", methods=["GET"])
def get_customers():
    spreadsheet = get_spreadsheet_with_retry()
    sheet = get_worksheet(spreadsheet, "customers_enriched")
    with performance_step(_performance_sheet_step(sheet)) as measurement:
        all_rows, cache_hit = cached_worksheet_values(sheet, spreadsheet)
        measurement["row_count"] = max(0, len(all_rows) - 1)
    record_google_sheet_read(cache_hit)
    headers = all_rows[0]

    customers = []
    for i, row in enumerate(all_rows[1:], start=2):
        padded = row + [""] * (len(headers) - len(row))
        d = dict(zip(headers, padded))
        customer = {col: d.get(col, "") for col in CUSTOMER_COLUMNS}
        customer["latitude"]  = parse_coordinate_value(d.get("latitude_google") or d.get("latitude",  ""), "latitude")
        customer["longitude"] = parse_coordinate_value(d.get("longitude_google") or d.get("longitude", ""), "longitude")
        addr = d.get("address_google", "").strip()
        num  = d.get("address_number_google", "").strip()
        customer["address_google"] = addr
        customer["address_number_google"] = num
        customer["city_google"] = d.get("city_google", "").strip()
        customer["postal_code_google"] = d.get("postal_code_google", "").strip()
        customer["region_google"] = d.get("region_google", "").strip()
        customer["address"] = f"{addr} {num}".strip()
        customer["city"] = customer["city_google"] or d.get("city", "")
        customers.append({"row": i, **customer})

    user = current_user()
    customer_lookup = (
        None if user_is_admin(user) else CustomerLookup(customers)
    )
    contact_rows = accessible_contact_rows(
        get_contact_rows(spreadsheet),
        customers,
        user,
        customer_lookup=customer_lookup,
    )
    latest_contact = {}
    latest_contact_followup = build_latest_contact_followups(contact_rows)
    for contact in contact_rows:
        name = normalize_key(contact.get("customer"))
        contact_date = parse_date_value(contact.get("date_time"))
        if contact_date and (
            name not in latest_contact or contact_date > latest_contact[name]
        ):
            latest_contact[name] = contact_date

    visible_customers = filter_accessible_customers(customers, user)
    for customer in visible_customers:
        customer_key = normalize_key(customer.get("customer"))
        customer["latest_contact_date"] = format_date_value(
            latest_contact.get(customer_key)
        )
        customer["follow_up_date"] = format_date_value(
            latest_contact_followup.get(customer_key)
        )
    return jsonify(visible_customers)


@app.route("/contact-log", methods=["GET"])
def get_contact_log():
    spreadsheet = get_spreadsheet_with_retry()
    customers = get_customer_rows(spreadsheet)
    user = current_user()
    customer_lookup = (
        None if user_is_admin(user) else CustomerLookup(customers)
    )
    contact_rows = accessible_contact_rows(
        get_contact_rows(spreadsheet),
        customers,
        user,
        customer_lookup=customer_lookup,
    )
    filters = get_contact_log_filter_values(request.args)
    return jsonify(build_contact_log_payload(contact_rows, filters))


@app.route("/contact-log/export", methods=["GET"])
def export_contact_log():
    spreadsheet = get_spreadsheet_with_retry()
    customers = get_customer_rows(spreadsheet)
    user = current_user()
    customer_lookup = (
        None if user_is_admin(user) else CustomerLookup(customers)
    )
    contact_rows = accessible_contact_rows(
        get_contact_rows(spreadsheet),
        customers,
        user,
        customer_lookup=customer_lookup,
    )
    filters = get_contact_log_filter_values(request.args)
    payload = build_contact_log_payload(contact_rows, filters)
    workbook = build_xlsx(payload["columns"], payload["rows"])
    filename = f"kontaktlogg_{stockholm_today().isoformat()}.xlsx"
    return Response(
        workbook,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _timeline_sort_value(item):
    return parse_datetime_value(item.get("date_time")) or datetime.min


def _timeline_contact_item(contact):
    result = str(contact.get("result", "")).strip()
    channel = str(contact.get("contact_channel", "")).strip()
    details = []
    if contact.get("follow_up_date"):
        details.append({"label": "Nästa uppföljning", "value": format_date_value(contact["follow_up_date"])})
    return {
        "date_time": str(contact.get("date_time", "")).strip(),
        "event_type": "contact",
        "title": result or channel or "Kundkontakt",
        "sales_person": str(contact.get("sales_person", "")).strip(),
        "channel": channel,
        "result": result,
        "recipient": str(contact.get("customer_contact_person", "")).strip(),
        "comment": str(contact.get("comment", "")).strip(),
        "details": details,
    }


def _email_recipient_identity(recipient):
    return normalize_email(
        recipient.get("intended_email") or recipient.get("actual_email")
    )


def _email_event_datetime(value):
    return parse_datetime_value(value)


def _latest_email_recipient_event(recipient_rows, field):
    values = [
        _email_event_datetime(recipient.get(field))
        for recipient in recipient_rows
    ]
    values = [value for value in values if value]
    return (
        max(values).isoformat(sep=" ", timespec="seconds")
        if values else ""
    )


def build_live_email_records(message_rows, recipient_rows):
    """Return one normalized record per live logical email proposal.

    Test messages and failed recipients are intentionally excluded. Recipient and
    event sets are unique, so repeated Brevo opens/clicks never inflate the
    business-level metrics.
    """
    recipients_by_email_id = defaultdict(list)
    for recipient in recipient_rows:
        email_id = str(recipient.get("email_id", "")).strip()
        if email_id:
            recipients_by_email_id[email_id].append(recipient)

    records = []
    for message in message_rows:
        if is_yes(message.get("is_test")):
            continue
        if str(message.get("status", "")).strip().casefold() not in {"sent", "partial"}:
            continue

        email_id = str(message.get("email_id", "")).strip()
        customer = str(message.get("customer", "")).strip()
        customer_id = str(message.get("customer_id", "")).strip()
        customer_number = str(message.get("customer_number", "")).strip()
        customer_key = normalize_key(customer)
        sent_at = parse_datetime_value(message.get("sent_at"))
        if not email_id or not customer_key or not sent_at:
            continue

        sent_recipients = [
            row for row in recipients_by_email_id.get(email_id, [])
            if str(row.get("send_status", "")).strip().casefold() == "sent"
        ]
        recipient_labels = {}
        delivered_recipients = set()
        opened_recipients = set()
        product_clicked_recipients = set()
        stockfiller_clicked_recipients = set()
        delivered_times = []
        opened_times = []
        first_opened_times = []
        product_click_times = []
        stockfiller_click_times = []
        product_first_click_times = []
        stockfiller_first_click_times = []

        for recipient in sent_recipients:
            identity = _email_recipient_identity(recipient)
            if not identity:
                continue
            recipient_labels.setdefault(
                identity,
                str(recipient.get("intended_email") or recipient.get("actual_email") or "").strip(),
            )

            delivered_at = _email_event_datetime(recipient.get("delivered_at"))
            first_opened_at = _email_event_datetime(
                recipient.get("first_opened_at")
            )
            opened_at = (
                _email_event_datetime(recipient.get("last_opened_at"))
                or first_opened_at
            )
            first_opened_at = first_opened_at or opened_at
            product_clicked_at = _email_event_datetime(recipient.get("product_sheet_last_clicked_at"))
            stockfiller_clicked_at = _email_event_datetime(recipient.get("stockfiller_last_clicked_at"))
            product_first_clicked_at = _email_event_datetime(
                recipient.get("product_sheet_first_clicked_at")
            ) or product_clicked_at
            stockfiller_first_clicked_at = _email_event_datetime(
                recipient.get("stockfiller_first_clicked_at")
            ) or stockfiller_clicked_at

            opened = parse_number_value(recipient.get("open_count"), 0) > 0 and bool(opened_at)
            product_clicked = (
                parse_number_value(recipient.get("product_sheet_click_count"), 0) > 0
                and bool(product_clicked_at)
            )
            stockfiller_clicked = (
                parse_number_value(recipient.get("stockfiller_click_count"), 0) > 0
                and bool(stockfiller_clicked_at)
            )

            # A human open or click is also conclusive evidence that the message
            # reached the recipient, even if the delivered webhook arrived late.
            if delivered_at or opened or product_clicked or stockfiller_clicked:
                delivered_recipients.add(identity)
            if delivered_at:
                delivered_times.append(delivered_at)
            if opened:
                opened_recipients.add(identity)
                if opened_at:
                    opened_times.append(opened_at)
                if first_opened_at:
                    first_opened_times.append(first_opened_at)
            if product_clicked:
                product_clicked_recipients.add(identity)
                product_click_times.append(product_clicked_at)
                product_first_click_times.append(product_first_clicked_at)
            if stockfiller_clicked:
                stockfiller_clicked_recipients.add(identity)
                stockfiller_click_times.append(stockfiller_clicked_at)
                stockfiller_first_click_times.append(stockfiller_first_clicked_at)

        if not recipient_labels:
            continue

        records.append({
            "email_id": email_id,
            "customer": customer,
            "customer_key": customer_key,
            "customer_id": customer_id,
            "customer_number": customer_number,
            "proposal_type": normalize_proposal_type(message.get("email_type")),
            "sent_at": sent_at,
            "message": message,
            "recipients": sent_recipients,
            "recipient_labels": recipient_labels,
            "sent_recipients": set(recipient_labels),
            "delivered_recipients": delivered_recipients,
            "opened_recipients": opened_recipients,
            "product_clicked_recipients": product_clicked_recipients,
            "stockfiller_clicked_recipients": stockfiller_clicked_recipients,
            "delivered_at": max(delivered_times) if delivered_times else None,
            "opened_at": max(opened_times) if opened_times else None,
            "first_opened_at": min(first_opened_times)
            if first_opened_times else None,
            "product_clicked_at": max(product_click_times) if product_click_times else None,
            "stockfiller_clicked_at": max(stockfiller_click_times) if stockfiller_click_times else None,
            "product_first_clicked_at": min(product_first_click_times)
            if product_first_click_times else None,
            "stockfiller_first_clicked_at": min(stockfiller_first_click_times)
            if stockfiller_first_click_times else None,
        })

    return sorted(records, key=lambda record: record["sent_at"])


def group_customer_orders(order_rows):
    """Collapse product rows into logical orders before attribution."""
    grouped = {}
    for index, order in enumerate(order_rows):
        customer = str(order.get("Customer", "")).strip()
        customer_key = normalize_key(customer)
        customer_id = str(order.get("customer_id", "")).strip()
        customer_number = str(order.get("Customer number", "")).strip()
        identity_key = (
            f"id:{normalize_key(customer_id)}"
            if normalize_key(customer_id)
            else (
                f"number:{normalize_key(customer_number)}"
                if normalize_key(customer_number)
                else f"name:{customer_key}"
            )
        )
        order_date = parse_date_value(order.get("Order date"))
        is_ordered = (
            parse_number_value(order.get("Quantity"), 0) > 0
            or parse_number_value(order.get("Total"), 0) > 0
        )
        if not customer_key or not order_date or not is_ordered:
            continue

        reference = str(order.get("Reference", "")).strip()
        currency = str(order.get("Currency", "")).strip().upper()
        if reference:
            group_key = (
                identity_key, "reference", reference, order_date.isoformat(), currency
            )
        else:
            # An order without Reference normally consists of several product
            # rows. Group same-day rows instead of counting every SKU as an order.
            group_key = (identity_key, "fallback", order_date.isoformat(), currency)

        group = grouped.setdefault(group_key, {
            "customer": customer,
            "customer_key": customer_key,
            "customer_id": customer_id,
            "customer_number": customer_number,
            "identity_key": identity_key,
            "reference": reference,
            "date": order_date,
            "total": 0.0,
            "currency": currency,
            "dfp": 0.0,
            "source_row": index,
        })
        group["total"] += parse_number_value(order.get("Total"), 0)
        group["currency"] = group["currency"] or currency
        if str(order.get("Unit", "")).strip().casefold() == "dfp":
            group["dfp"] += parse_number_value(order.get("Quantity"), 0)

    return sorted(
        grouped.values(),
        key=lambda order: (order["date"], order["customer_key"], order["reference"], order["source_row"]),
    )


def attribute_orders_to_live_emails(live_records, grouped_orders, window_days=EMAIL_ORDER_ATTRIBUTION_DAYS):
    """Attribute each order once to the latest eligible live proposal."""
    attributed = defaultdict(list)
    for order in grouped_orders:
        eligible = [
            record for record in live_records
            if customer_identity_matches(
                record.get("customer"),
                record.get("customer_number"),
                order.get("customer"),
                order.get("customer_number"),
                record.get("customer_id"),
                order.get("customer_id"),
            )
            and 0 <= (order["date"] - record["sent_at"].date()).days <= window_days
        ]
        if not eligible:
            continue
        latest = max(eligible, key=lambda record: record["sent_at"])
        attributed[latest["email_id"]].append(order)
    return attributed


def build_email_engagement_snapshot(
    message_rows, recipient_rows, order_rows, today=None, customers=()
):
    """Return the latest live email outcome for every customer.

    Rank 1 is the most actionable. The click filter is deliberately delayed for
    three full calendar days so sellers do not follow up immediately after a
    recipient has shown intent.
    """
    today = today or stockholm_today()
    records = build_live_email_records(message_rows, recipient_rows)
    if customers:
        by_id = {
            normalize_key(row.get("customer_id")): row
            for row in customers if normalize_key(row.get("customer_id"))
        }
        by_number = {
            normalize_key(row.get("customer_number")): row
            for row in customers if normalize_key(row.get("customer_number"))
        }
        name_matches = defaultdict(list)
        for row in customers:
            if normalize_key(row.get("customer")):
                name_matches[normalize_key(row.get("customer"))].append(row)
        canonical_records = []
        for record in records:
            master = None
            record_id = normalize_key(record.get("customer_id"))
            record_number = normalize_key(record.get("customer_number"))
            if record_id:
                master = by_id.get(record_id)
                if master is None:
                    continue
            if master is None and record_number:
                master = by_number.get(record_number)
            if master is None:
                matches = name_matches.get(record.get("customer_key"), [])
                master = (
                    matches[0]
                    if not record_id and not record_number and len(matches) == 1
                    else None
                )
            if master is None:
                continue
            canonical_records.append({
                **record,
                "customer_id": str(master.get("customer_id") or "").strip(),
                "customer_number": str(master.get("customer_number") or "").strip(),
                "customer_key": normalize_key(master.get("customer")),
            })
        records = canonical_records
    attributed = attribute_orders_to_live_emails(records, group_customer_orders(order_rows))
    latest_by_customer = {}
    for record in records:
        identity_key = (
            f"id:{normalize_key(record.get('customer_id'))}"
            if normalize_key(record.get("customer_id"))
            else (
                f"number:{normalize_key(record.get('customer_number'))}"
                if normalize_key(record.get("customer_number"))
            else record["customer_key"]
            )
        )
        current = latest_by_customer.get(identity_key)
        if current is None or record["sent_at"] > current["sent_at"]:
            latest_by_customer[identity_key] = record

    snapshots = {}
    for identity_key, record in latest_by_customer.items():
        status = ""
        label = ""
        priority = None
        event_at = record["sent_at"]
        has_order = bool(attributed.get(record["email_id"]))

        if has_order:
            status = "ordered_within_10_days"
            label = "Order inom 10 dagar"
            priority = 0
            event_at = max(
                datetime.combine(order["date"], datetime.min.time())
                for order in attributed[record["email_id"]]
            )
        elif record["stockfiller_clicked_recipients"]:
            status = "stockfiller_clicked_no_order"
            label = "Stockfiller-klick utan order"
            priority = 1
            event_at = record["stockfiller_clicked_at"] or record["sent_at"]
        elif record["product_clicked_recipients"]:
            status = "product_sheet_clicked_no_order"
            label = "Produktbladsklick utan order"
            priority = 2
            event_at = record["product_clicked_at"] or record["sent_at"]
        elif record["opened_recipients"]:
            status = "opened_no_click"
            label = "Öppnat men inte klickat"
            priority = 3
            event_at = record["opened_at"] or record["sent_at"]
        elif record["delivered_recipients"]:
            status = "delivered_no_activity"
            label = "Levererat men ingen aktivitet"
            priority = 4
            event_at = record["delivered_at"] or record["sent_at"]

        clicked_without_order = status in {
            "stockfiller_clicked_no_order", "product_sheet_clicked_no_order"
        }
        wait_days_remaining = 0
        if clicked_without_order or status == "opened_no_click":
            if status == "stockfiller_clicked_no_order":
                first_event_at = record["stockfiller_first_clicked_at"] or event_at
                wait_days = EMAIL_CLICK_FOLLOWUP_WAIT_DAYS
            elif status == "product_sheet_clicked_no_order":
                first_event_at = record["product_first_clicked_at"] or event_at
                wait_days = EMAIL_CLICK_FOLLOWUP_WAIT_DAYS
            else:
                first_event_at = record["first_opened_at"] or event_at
                wait_days = EMAIL_OPEN_FOLLOWUP_WAIT_DAYS
            days_since_event = max(0, (today - first_event_at.date()).days)
            wait_days_remaining = max(0, wait_days - days_since_event)
            if clicked_without_order:
                clicked_without_order = wait_days_remaining == 0

        proposal_type = normalize_proposal_type(record.get("proposal_type"))

        snapshots[identity_key] = {
            "customer_id": record.get("customer_id", ""),
            "customer_number": record.get("customer_number", ""),
            "customer_key": record.get("customer_key", ""),
            "email_followup_status": status,
            "email_followup_label": label,
            "email_followup_priority": priority,
            "email_followup_sent_at": record["sent_at"].isoformat(
                sep=" ", timespec="seconds"
            ),
            "email_followup_last_event_at": event_at.isoformat(sep=" ", timespec="seconds") if event_at else "",
            "email_click_without_order": clicked_without_order,
            "email_clicked_without_order": clicked_without_order,
            "email_followup_wait_days_remaining": wait_days_remaining,
            "email_order_within_10_days": has_order,
            "email_followup_email_id": record["email_id"],
            "email_followup_proposal_type": proposal_type,
            "email_followup_proposal_label": EMAIL_PROPOSAL_TYPES[proposal_type],
            "email_first_opened_at": (
                record["first_opened_at"].isoformat(sep=" ", timespec="seconds")
                if record.get("first_opened_at") else ""
            ),
            "email_stockfiller_first_clicked_at": (
                record["stockfiller_first_clicked_at"].isoformat(sep=" ", timespec="seconds")
                if record.get("stockfiller_first_clicked_at") else ""
            ),
            "email_product_sheet_first_clicked_at": (
                record["product_first_clicked_at"].isoformat(sep=" ", timespec="seconds")
                if record.get("product_first_clicked_at") else ""
            ),
        }

    return snapshots


def email_engagement_for_customer(snapshots, customer):
    customer_id = normalize_key(customer.get("customer_id"))
    customer_number = normalize_key(customer.get("customer_number"))
    customer_name = normalize_key(customer.get("customer"))
    values = list((snapshots or {}).values())
    if customer_id:
        match = next((row for row in values if normalize_key(row.get("customer_id")) == customer_id), None)
        if match:
            return match
    if customer_number:
        match = next((row for row in values if normalize_key(row.get("customer_number")) == customer_number), None)
        if match:
            return match
    name_matches = [row for row in values if normalize_key(row.get("customer_key")) == customer_name]
    return name_matches[0] if customer_name and len(name_matches) == 1 else {}


def _email_rate(numerator, denominator):
    return round((numerator / denominator) * 100, 1) if denominator else 0.0


def build_email_performance(message_rows, recipient_rows, order_rows, included_customer_keys=None, today=None):
    """Build cumulative, live-only email KPIs using stores as primary grain."""
    records = build_live_email_records(message_rows, recipient_rows)
    if included_customer_keys is not None:
        included = {normalize_key(key) for key in included_customer_keys}
        records = [record for record in records if record["customer_key"] in included]

    grouped_orders = group_customer_orders(order_rows)
    attributed = attribute_orders_to_live_emails(records, grouped_orders)
    snapshots = build_email_engagement_snapshot(
        [record["message"] for record in records], recipient_rows, order_rows, today=today
    )

    sent_stores = {record["customer_key"] for record in records}
    delivered_stores = {
        record["customer_key"] for record in records if record["delivered_recipients"]
    }
    opened_stores = {
        record["customer_key"] for record in records if record["opened_recipients"]
    }
    product_clicked_stores = {
        record["customer_key"] for record in records if record["product_clicked_recipients"]
    }
    stockfiller_clicked_stores = {
        record["customer_key"] for record in records if record["stockfiller_clicked_recipients"]
    }

    delivered_email_keys = set()
    sent_email_keys = set()
    opened_recipient_keys = set()
    product_recipient_keys = set()
    stockfiller_recipient_keys = set()
    record_by_email_id = {record["email_id"]: record for record in records}
    for record in records:
        for recipient in record["sent_recipients"]:
            sent_email_keys.add((record["email_id"], recipient))
        for recipient in record["delivered_recipients"]:
            delivered_email_keys.add((record["email_id"], recipient))
        for recipient in record["opened_recipients"]:
            opened_recipient_keys.add((record["customer_key"], recipient))
        for recipient in record["product_clicked_recipients"]:
            product_recipient_keys.add((record["customer_key"], recipient))
        for recipient in record["stockfiller_clicked_recipients"]:
            stockfiller_recipient_keys.add((record["customer_key"], recipient))

    ordered_email_ids = {email_id for email_id, orders in attributed.items() if orders}
    ordered_stores = {
        record_by_email_id[email_id]["customer_key"]
        for email_id in ordered_email_ids if email_id in record_by_email_id
    }
    attributed_orders = [order for orders in attributed.values() for order in orders]
    order_value_by_currency = defaultdict(float)
    attributed_dfp = 0.0
    for order in attributed_orders:
        if order["currency"]:
            order_value_by_currency[order["currency"]] += order["total"]
        attributed_dfp += order["dfp"]

    followup_counts = defaultdict(int)
    for customer_key in sent_stores:
        status = snapshots.get(customer_key, {}).get("email_followup_status", "")
        if status:
            followup_counts[status] += 1

    store_count = len(sent_stores)
    return {
        "period_label": "Alla liveutskick",
        "attribution_days": EMAIL_ORDER_ATTRIBUTION_DAYS,
        "click_followup_wait_days": EMAIL_CLICK_FOLLOWUP_WAIT_DAYS,
        "live_email_count": len(records),
        "sent_email_count": len(sent_email_keys),
        "delivered_email_count": len(delivered_email_keys),
        "unique_store_count": store_count,
        "delivered_store_count": len(delivered_stores),
        "opened_store_count": len(opened_stores),
        "opened_store_rate": _email_rate(len(opened_stores), store_count),
        "product_clicked_store_count": len(product_clicked_stores),
        "product_clicked_store_rate": _email_rate(len(product_clicked_stores), store_count),
        "stockfiller_clicked_store_count": len(stockfiller_clicked_stores),
        "stockfiller_clicked_store_rate": _email_rate(len(stockfiller_clicked_stores), store_count),
        "opened_recipient_count": len(opened_recipient_keys),
        "product_clicked_recipient_count": len(product_recipient_keys),
        "stockfiller_clicked_recipient_count": len(stockfiller_recipient_keys),
        "ordered_store_count": len(ordered_stores),
        "ordered_store_rate": _email_rate(len(ordered_stores), store_count),
        "attributed_order_count": len(attributed_orders),
        "attributed_order_value_by_currency": {
            currency: round(value, 2)
            for currency, value in sorted(order_value_by_currency.items())
        },
        "attributed_dfp": round(attributed_dfp, 2),
        "followup_counts": dict(followup_counts),
    }


def build_customer_timeline(
    customer_name,
    order_rows,
    contact_rows,
    sheets,
    customer_number="",
    *,
    customer_record=None,
    customers=None,
    customer_lookup=None,
):
    """Build the customer-specific, user-facing activity stream.

    Raw delivery, bounce and other technical Brevo events remain in email_events;
    this projection intentionally exposes only the V1 events approved for the UI.
    """
    customer_key = normalize_key(customer_name)
    timeline = []

    canonical_rows = customer_record is not None and customers is not None
    for contact in contact_rows:
        if not canonical_rows and normalize_key(contact.get("customer")) != customer_key:
            continue
        # Email proposal sends have their own richer timeline item below.
        if str(contact.get("email_id", "")).strip():
            continue
        timeline.append(_timeline_contact_item(contact))

    message_rows = worksheet_to_dicts(
        sheets[EMAIL_MESSAGES_SHEET], expected_columns=EMAIL_MESSAGES_COLUMNS
    )
    recipient_rows = worksheet_to_dicts(
        sheets[EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
    )
    if customer_record is not None and customers is not None:
        message_rows = related_rows_for_customer(
            message_rows,
            customers,
            customer_record,
            customer_lookup=customer_lookup,
        )
        visible_email_ids = {
            str(message.get("email_id") or "").strip()
            for message in message_rows
        }
        recipient_rows = [
            recipient for recipient in recipient_rows
            if str(recipient.get("email_id") or "").strip()
            in visible_email_ids
        ]
    live_records = build_live_email_records(message_rows, recipient_rows)
    if not canonical_rows:
        live_records = [
            record for record in live_records
            if customer_identity_matches(
                customer_name,
                customer_number,
                record.get("customer"),
                record.get("customer_number"),
            )
        ]
    grouped_orders = group_customer_orders(order_rows)
    if not canonical_rows:
        grouped_orders = [
            order for order in grouped_orders
            if customer_identity_matches(
                customer_name,
                customer_number,
                order.get("customer"),
                order.get("customer_number"),
            )
        ]
    attributed_orders = attribute_orders_to_live_emails(
        live_records,
        grouped_orders,
    )

    event_specs = (
        ("opened_recipients", "opened_at", "email_proposal_opened", "Öppnat", "secondary"),
        ("product_clicked_recipients", "product_clicked_at", "product_sheet_clicked", "Produktblad klickat", "primary"),
        ("stockfiller_clicked_recipients", "stockfiller_clicked_at", "stockfiller_clicked", "Stockfiller klickat", "primary"),
    )
    for record in live_records:
        message = record["message"]
        partial = str(message.get("status", "")).strip().casefold() == "partial"
        email_type = normalize_proposal_type(message.get("email_type"))
        type_label = EMAIL_PROPOSAL_TYPES[email_type]
        recipient_label = ", ".join(
            record["recipient_labels"].get(identity, identity)
            for identity in sorted(record["sent_recipients"])
        )
        recipient_tracking = []
        for identity in sorted(record["sent_recipients"]):
            identity_rows = [
                recipient
                for recipient in record["recipients"]
                if _email_recipient_identity(recipient) == identity
            ]
            recipient_tracking.append({
                "email": record["recipient_labels"].get(identity, identity),
                "last_opened_at": _latest_email_recipient_event(
                    identity_rows, "last_opened_at"
                ),
                "product_sheet_last_clicked_at": _latest_email_recipient_event(
                    identity_rows,
                    "product_sheet_last_clicked_at"
                ),
                "stockfiller_last_clicked_at": _latest_email_recipient_event(
                    identity_rows,
                    "stockfiller_last_clicked_at"
                ),
            })
        timeline.append({
            "date_time": message.get("sent_at", ""),
            "event_type": "email_proposal_sent",
            "importance": "primary",
            "title": f"{type_label} skickad" + (" delvis" if partial else ""),
            "sales_person": message.get("sender_name", ""),
            "channel": "Mejl",
            "result": f"Mejlförslag skickat – {type_label}" + (" delvis" if partial else ""),
            "recipient": recipient_label,
            "comment": message.get("subject", ""),
            "email_id": record["email_id"],
            "recipient_tracking": recipient_tracking,
            "details": [
                {"label": "Ämne", "value": message.get("subject", "") or "—"},
            ],
        })

        for recipients_field, time_field, event_type, label, importance in event_specs:
            identities = record[recipients_field]
            event_time = record[time_field]
            if not identities or not event_time:
                continue
            event_recipients = ", ".join(
                record["recipient_labels"].get(identity, identity)
                for identity in sorted(identities)
            )
            timeline.append({
                "date_time": event_time.isoformat(sep=" ", timespec="seconds"),
                "event_type": event_type,
                "importance": importance,
                "title": label,
                "sales_person": message.get("sender_name", ""),
                "channel": "Mejl",
                "result": label,
                "recipient": event_recipients,
                "comment": "",
                "email_id": record["email_id"],
                "details": [],
            })

    for record in live_records:
        attributed_message = record["message"]
        for order in attributed_orders.get(record["email_id"], []):
            value = round(order["total"], 2)
            value_text = f"{value:,.2f}".replace(",", " ").replace(".", ",")
            if order["currency"]:
                value_text += f" {order['currency']}"
            details = [
                {"label": "Orderreferens", "value": order["reference"] or "—"},
                {"label": "Ordervärde", "value": value_text},
            ]
            if order["dfp"]:
                details.append({
                    "label": "Antal DFP",
                    "value": str(int(order["dfp"]) if order["dfp"].is_integer() else order["dfp"]),
                })
            timeline.append({
                "date_time": f"{order['date'].isoformat()} 12:00:00",
                "event_type": "subsequent_order",
                "importance": "primary",
                "title": "Efterföljande order",
                "sales_person": attributed_message.get("sender_name", ""),
                "channel": "Order",
                "result": "Order inom 10 dagar",
                "recipient": "",
                "comment": f"Order {order['reference']}" if order["reference"] else "Ny order",
                "email_id": record["email_id"],
                "details": details,
            })

    timeline.sort(key=_timeline_sort_value, reverse=True)
    return timeline


@app.route("/customers/<customer_name>/stats", methods=["GET"])
def get_customer_stats(customer_name):
    customer_name = unquote(customer_name).strip()
    spreadsheet = get_spreadsheet_with_retry()
    customers = get_customer_rows(spreadsheet)
    customer_lookup = CustomerLookup(customers)
    customer = resolve_accessible_customer(
        customers,
        current_user(),
        customer_name=customer_name,
        customer_lookup=customer_lookup,
    )
    if customer is None:
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    customer_name = customer.get("customer", customer_name)
    customer_number = customer.get("customer_number", "")

    # Orders
    order_rows = related_rows_for_customer(
        get_order_rows(spreadsheet),
        customers,
        customer,
        name_key="Customer",
        number_key="Customer number",
        customer_lookup=customer_lookup,
    )
    total_sales = 0.0
    latest_order_date = None
    currency = ""

    unique_references = set()
    for o in order_rows:
        try:
            cleaned = "".join(c for c in o["Total"] if c.isdigit() or c in ".,").replace(",", ".")
            if cleaned:
                total_sales += float(cleaned)
        except ValueError:
            pass
        if not currency and o["Currency"].strip():
            currency = o["Currency"].strip()
        d = parse_date_value(o["Order date"])
        if d and (latest_order_date is None or d > latest_order_date):
            latest_order_date = d
        if o["Reference"].strip():
            unique_references.add(o["Reference"].strip())

    # Contacts
    contact_rows = related_rows_for_customer(
        get_contact_rows(spreadsheet),
        customers,
        customer,
        customer_lookup=customer_lookup,
    )
    contacts = []
    for c in contact_rows:
        contact = {k: c[k] for k in ("customer", "date_time", "sales_person", "contact_channel", "result", "comment", "customer_contact_person", "follow_up_date",
                                     *FREEZER_COLUMNS)}
        contact["_sort_date"] = parse_date_value(c["date_time"]) or date.min
        contact["date_time"] = format_date_value(c["date_time"])
        contact["follow_up_date"] = format_date_value(c["follow_up_date"])
        contacts.append(contact)
    contacts.sort(key=lambda x: x["_sort_date"], reverse=True)
    for contact in contacts:
        contact.pop("_sort_date", None)

    sheets = ensure_email_worksheets(spreadsheet)
    timeline = build_customer_timeline(
        customer_name,
        order_rows,
        contact_rows,
        sheets,
        customer_number=customer_number,
        customer_record=customer,
        customers=customers,
        customer_lookup=customer_lookup,
    )

    return jsonify({
        "total_sales": round(total_sales, 2),
        "latest_order_date": format_date_value(latest_order_date, fallback="—"),
        "currency": currency,
        "order_count": len(unique_references),
        "contacts": contacts,
        "timeline": timeline,
    })


def build_current_priority_snapshot(
    *,
    customers,
    order_rows,
    contact_rows,
    message_rows,
    recipient_rows,
    today,
    planned_activity_rows=(),
    responsible=None,
):
    """Calculate the authoritative priority snapshot used by all endpoints."""
    order_features = build_order_features(order_rows)
    contact_features = build_contact_features(contact_rows, order_features)
    email_engagement_by_customer = build_email_engagement_snapshot(
        message_rows, recipient_rows, order_rows, today=today, customers=customers
    )
    priority_customers = build_priority_customers(
        customers,
        order_features,
        contact_features,
        None,
        today,
        limit=len(customers),
        email_features=email_engagement_by_customer,
        planned_activities=planned_activity_rows,
    )
    if responsible:
        responsible_key = normalize_key(responsible)
        priority_customers = [
            customer for customer in priority_customers
            if normalize_key(customer.get("sales_person")) == responsible_key
        ]
    return priority_customers, email_engagement_by_customer


def get_authoritative_priority_snapshot(
    spreadsheet, *, today, planned_activity_rows=None
):
    """Return one short-lived global Scoring v2 universe before owner filtering."""
    global _priority_snapshot_entry
    date_key = today.isoformat() if isinstance(today, date) else str(today)
    cache_enabled = sheet_cache_enabled(spreadsheet)
    generation = (
        _sheet_read_cache.generation
        if cache_enabled else time.monotonic_ns()
    )
    key = (id(spreadsheet), date_key, generation)
    with _priority_snapshot_condition:
        while True:
            if _priority_snapshot_entry and _priority_snapshot_entry[0] == key:
                return copy.deepcopy(_priority_snapshot_entry[1])
            if key not in _priority_snapshot_loading:
                _priority_snapshot_loading.add(key)
                break
            _priority_snapshot_condition.wait()
    try:
        customers = get_customer_rows(spreadsheet)
        order_rows = get_order_rows(spreadsheet)
        contact_rows = get_contact_rows(spreadsheet)
        message_rows, recipient_rows, _events = get_email_rows(
            spreadsheet, include_events=False
        )
        if planned_activity_rows is None:
            try:
                _sheet, _headers, indexed = get_planned_activity_snapshot(
                    spreadsheet
                )
                planned_activity_rows = [row for _index, row in indexed]
            except (WorksheetNotFound, AttributeError):
                planned_activity_rows = []
        priorities, email_snapshot = build_current_priority_snapshot(
            customers=customers,
            order_rows=order_rows,
            contact_rows=contact_rows,
            message_rows=message_rows,
            recipient_rows=recipient_rows,
            today=today,
            planned_activity_rows=planned_activity_rows or (),
        )
        payload = {
            "priorities": priorities,
            "email_engagement": email_snapshot,
            "customers": customers,
            "order_rows": order_rows,
            "contact_rows": contact_rows,
            "message_rows": message_rows,
            "recipient_rows": recipient_rows,
            "planned_activity_rows": list(planned_activity_rows or ()),
        }
        with _priority_snapshot_condition:
            if cache_enabled and _sheet_read_cache.generation == generation:
                _priority_snapshot_entry = (key, copy.deepcopy(payload))
        return copy.deepcopy(payload)
    finally:
        with _priority_snapshot_condition:
            _priority_snapshot_loading.discard(key)
            _priority_snapshot_condition.notify_all()


def calibration_score_band(value):
    score = max(0, min(100, int(round(parse_number_value(value, 0)))))
    if score <= 49:
        return "0-49"
    if score <= 69:
        return "50-69"
    if score <= 79:
        return "70-79"
    if score <= 89:
        return "80-89"
    return "90-100"


def build_calibration_rows(score_events, order_rows, customers):
    """Join persisted event scores to later orders without recomputing history."""
    by_id = {
        normalize_key(row.get("customer_id")): row
        for row in customers if normalize_key(row.get("customer_id"))
    }
    by_number = {
        normalize_key(row.get("customer_number")): row
        for row in customers if normalize_key(row.get("customer_number"))
    }
    names = defaultdict(list)
    for row in customers:
        if normalize_key(row.get("customer")):
            names[normalize_key(row.get("customer"))].append(row)

    orders_by_customer = defaultdict(list)
    for order in group_customer_orders(order_rows):
        master = None
        order_id = normalize_key(order.get("customer_id"))
        order_number = normalize_key(order.get("customer_number"))
        if order_id:
            master = by_id.get(order_id)
            if master is None:
                continue
        if master is None and order_number:
            master = by_number.get(order_number)
        if master is None:
            matches = names.get(order.get("customer_key"), [])
            master = (
                matches[0]
                if not order_id and not order_number and len(matches) == 1
                else None
            )
        customer_id = str((master or {}).get("customer_id") or "").strip()
        if customer_id:
            orders_by_customer[customer_id].append(order)

    rows = []
    for event in sorted(
        score_events,
        key=lambda row: (
            str(row.get("occurred_at") or ""), str(row.get("event_id") or "")
        ),
    ):
        occurred = parse_datetime_value(event.get("occurred_at"))
        customer_id = str(event.get("customer_id") or "").strip()
        later_orders = [
            order for order in orders_by_customer.get(customer_id, [])
            if occurred and order.get("date") and order["date"] >= occurred.date()
        ]
        first_order = min(
            later_orders,
            key=lambda order: (order["date"], order.get("reference") or ""),
        ) if later_orders else None
        rows.append({
            "event_id": str(event.get("event_id") or ""),
            "event_type": str(event.get("event_type") or ""),
            "occurred_at": str(event.get("occurred_at") or ""),
            "customer_id": customer_id,
            "suggestion_id": str(event.get("suggestion_id") or ""),
            "decision_context_hash": str(event.get("decision_context_hash") or ""),
            "primary_trigger_key": str(event.get("primary_trigger_key") or ""),
            "score_version": str(event.get("score_version") or ""),
            "lifecycle": str(event.get("lifecycle") or ""),
            "recommendation_eligible": str(event.get("recommendation_eligible") or ""),
            "suppression_reason": str(event.get("suppression_reason") or ""),
            "priority_score": event.get("priority_score", ""),
            "priority_score_band": calibration_score_band(event.get("priority_score")),
            "intent_timing": event.get("intent_timing", ""),
            "value_index": event.get("value_index", ""),
            "strategic_index": event.get("strategic_index", ""),
            "expected_order_dfp": event.get("expected_order_dfp", ""),
            "recommended_contact_type": str(event.get("recommended_contact_type") or ""),
            "actual_planned_contact_type": str(event.get("actual_planned_contact_type") or ""),
            "status_before": str(event.get("status_before") or ""),
            "status_after": str(event.get("status_after") or ""),
            "resolved_by_type": str(event.get("resolved_by_type") or ""),
            "resolved_by_id": str(event.get("resolved_by_id") or ""),
            "order_outcome": "order_after_event" if first_order else "no_later_order",
            "first_order_date_after_event": (
                first_order["date"].isoformat() if first_order else ""
            ),
            "first_order_reference_after_event": (
                str(first_order.get("reference") or "") if first_order else ""
            ),
        })
    return rows


def priority_workflow_suppressions(spreadsheet, priority_customers):
    try:
        users = get_user_rows(spreadsheet)
        owners = {}
        for user in users:
            if not is_yes(user.get("active")):
                continue
            user_name = str(user.get("user_name") or "").strip()
            owners[normalize_key(user_name)] = user_name
            owners[normalize_key(user.get("name"))] = user_name
        _sheet, _events, _headers, stored = planning_suggestion_service(
            spreadsheet
        ).snapshot()
    except (WorksheetNotFound, AttributeError):
        return {}
    except Exception:
        app.logger.exception("Could not load suggestion suppression state")
        return {}

    by_identity = {
        (
            normalize_key(row.get("user_name")),
            str(row.get("customer_id") or "").strip(),
            str(row.get("decision_context_hash") or "").strip(),
        ): row
        for _index, row in stored
    }
    now = stockholm_now().astimezone(STOCKHOLM_ZONE)
    suppressions = {}
    for priority in priority_customers:
        customer_id = str(priority.get("customer_id") or "").strip()
        owner_name = owners.get(normalize_key(priority.get("sales_person")), "")
        if not customer_id or not owner_name:
            continue
        context_hash = decision_context_hash(
            owner=owner_name,
            customer_id=customer_id,
            lifecycle=(
                priority.get("decision_context_lifecycle")
                or priority.get("lifecycle")
            ),
            order_count=priority.get("order_count"),
            latest_order_reference=priority.get("latest_order_reference"),
            latest_order_date=(
                priority.get("latest_delivery_date")
                or priority.get("latest_order_date")
            ),
            latest_contact_id=priority.get("latest_human_contact_id"),
            latest_contact_result=priority.get("latest_contact_result"),
            latest_contact_date=priority.get("latest_human_contact_date"),
            active_email_intent_event=priority.get(
                "active_email_intent_event"
            ),
        )
        row = by_identity.get((normalize_key(owner_name), customer_id, context_hash))
        status = str((row or {}).get("status") or "").strip().casefold()
        if status == "snoozed":
            due = parse_planning_instant((row or {}).get("snooze_until"))
            if due and due > now:
                suppressions[customer_id] = "snoozed"
        elif status == "dismissed":
            suppressions[customer_id] = "dismissed"
        elif status == "planned":
            suppressions[customer_id] = "suggestion_planned"
    return suppressions


@app.route("/customer-insights", methods=["GET"])
def get_customer_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = stockholm_today()
    user = current_user()
    global_snapshot = get_authoritative_priority_snapshot(
        spreadsheet, today=today
    )
    all_customers = global_snapshot["customers"]
    customer_lookup = (
        None if user_is_admin(user) else CustomerLookup(all_customers)
    )
    customers = filter_accessible_customers(all_customers, user)

    all_contact_rows = global_snapshot["contact_rows"]
    contact_rows = accessible_contact_rows(
        all_contact_rows,
        all_customers,
        user,
        customer_lookup=customer_lookup,
    )
    all_message_rows = global_snapshot["message_rows"]
    all_recipient_rows = global_snapshot["recipient_rows"]
    message_rows = all_message_rows
    recipient_rows = all_recipient_rows
    message_rows = accessible_related_rows(
        message_rows,
        all_customers,
        user,
        customer_lookup=customer_lookup,
    )
    if not user_is_admin(user):
        visible_email_ids = {
            str(message.get("email_id") or "").strip()
            for message in message_rows
        }
        recipient_rows = [
            recipient for recipient in recipient_rows
            if str(recipient.get("email_id") or "").strip()
            in visible_email_ids
        ]
    latest_live_proposals = latest_live_email_proposals_by_customer(message_rows)
    blocked_recipients = blocked_recipient_reasons(recipient_rows)

    # Latest order date and order count per customer
    all_order_rows = global_snapshot["order_rows"]
    order_rows = accessible_related_rows(
        all_order_rows,
        all_customers,
        user,
        name_key="Customer",
        number_key="Customer number",
        customer_lookup=customer_lookup,
    )
    calculation_started = time.perf_counter()
    latest_order = {}
    latest_delivery = {}
    order_references = defaultdict(set)
    for o in order_rows:
        name = normalize_key(o["Customer"])
        is_ordered = (
            parse_number_value(o.get("Quantity"), 0) > 0
            or parse_number_value(o.get("Total"), 0) > 0
        )
        if not name or not is_ordered:
            continue
        d = parse_date_value(o["Order date"])
        dd = parse_date_value(o["Delivery date"])
        ref = o["Reference"].strip()
        if d and (name not in latest_order or d > latest_order[name]):
            latest_order[name] = d
        if dd and (name not in latest_delivery or dd > latest_delivery[name]):
            latest_delivery[name] = dd
        if ref:
            order_references[name].add(ref)

    priority_started = time.perf_counter()
    priority_customers = global_snapshot["priorities"]
    email_engagement_by_customer = global_snapshot["email_engagement"]
    accessible_customer_ids = {
        str(customer.get("customer_id") or "").strip()
        for customer in customers
    }
    priority_customers = [
        priority for priority in priority_customers
        if str(priority.get("customer_id") or "").strip()
        in accessible_customer_ids
    ]
    priority_customers = apply_workflow_suppressions(
        priority_customers,
        priority_workflow_suppressions(spreadsheet, priority_customers),
    )
    record_performance_step(
        "calculation.priority",
        priority_started,
        len(priority_customers),
    )
    priority_by_name = {
        normalize_key(customer["customer"]): customer
        for customer in priority_customers
    }
    customers_by_name = {
        normalize_key(customer["customer"]): customer
        for customer in customers
    }

    # Compute insights for all customers
    all_names = (
        {normalize_key(c.get("customer")) for c in contact_rows if normalize_key(c.get("customer"))}
        | set(latest_order.keys())
        | set(order_references.keys())
        | set(latest_delivery.keys())
        | {normalize_key(c["customer"]) for c in customers if normalize_key(c.get("customer"))}
    )
    insights = {}
    for name in all_names:
        # customer_risk — based on most recent of order date or delivery date
        lo = latest_order.get(name)
        ld_check = latest_delivery.get(name)
        count = len(order_references.get(name, set()))
        risk = calculate_customer_risk(count, lo, ld_check, today)

        ld = latest_delivery.get(name)
        latest_delivery_date = format_date_value(ld)
        priority = priority_by_name.get(normalize_key(name), {})
        missad = bool(priority.get("missad_uppfoljning", False))
        customer = customers_by_name.get(normalize_key(name), {"customer": name})
        has_prior_order = bool(priority.get("order_count", 0) or name in order_references)
        days_since_delivery = (today - ld).days if ld else None
        email_type = (
            "new_customer"
            if not has_prior_order
            else "reminder"
            if ld and days_since_delivery <= EMAIL_PROPOSAL_RECENT_DELIVERY_DAYS
            else "reactivation"
        )
        relationship = {
            "email_type": email_type,
            "email_type_label": EMAIL_PROPOSAL_TYPES[email_type],
            "latest_delivery_date": latest_delivery_date,
            "days_since_delivery": days_since_delivery,
            "has_prior_order": has_prior_order,
        }
        email_proposal = build_email_proposal_status(
            customer,
            priority,
            relationship,
            latest_live_proposals,
            blocked_recipients,
            today,
        )
        email_engagement = email_engagement_for_customer(
            email_engagement_by_customer, customer
        )
        insights[name] = {
            "missad_uppfoljning": missad,
            "customer_risk": risk,
            "priority_level": priority.get("priority_level", ""),
            "priority_score": priority.get("priority_score"),
            "score_version": priority.get("score_version", SCORE_VERSION),
            "lifecycle": priority.get("lifecycle", ""),
            "intent_timing": priority.get("intent_timing"),
            "value_index": priority.get("value_index"),
            "strategic_index": priority.get("strategic_index"),
            "recommendation_eligible": priority.get("recommendation_eligible", False),
            "recommendation_suppression_reason": priority.get(
                "recommendation_suppression_reason", ""
            ),
            "primary_reason_code": priority.get("primary_reason_code", ""),
            "primary_reason_text": priority.get("primary_reason_text", ""),
            "primary_trigger_type": priority.get("primary_trigger_type", ""),
            "covered_trigger_keys": priority.get("covered_trigger_keys", []),
            "planning_status_text": priority.get("planning_status_text", ""),
            "priority_type": priority.get("priority_type", ""),
            "recommended_action": priority.get("recommended_action", ""),
            "reasons": priority.get("reasons", []),
            "next_action": priority.get("next_action", {}),
            "order_count": priority.get("order_count", 0),
            "first_order_sku_count": priority.get("first_order_sku_count", 0),
            "total_dfp": priority.get("total_dfp"),
            "expected_order_dfp": priority.get("expected_order_dfp"),
            "latest_order_date": priority.get("latest_order_date", ""),
            "latest_delivery_date": latest_delivery_date,
            "latest_delivery_month": latest_delivery_date[:7] if latest_delivery_date else "",  # "YYYY-MM"
            "expected_cycle_days": priority.get("expected_cycle_days"),
            "expected_cycle_source": priority.get("expected_cycle_source", ""),
            "expected_next_order_date": priority.get("expected_next_order_date", ""),
            "overdue_days": priority.get("overdue_days"),
            "days_since_delivery": priority.get("days_since_delivery"),
            "latest_contact_date": priority.get("latest_contact_date", ""),
            "latest_contact_result": priority.get("latest_contact_result", ""),
            "latest_contact_comment": priority.get("latest_contact_comment", ""),
            "latest_contact_class": priority.get("latest_contact_class", ""),
            "latest_contact_channel": priority.get("latest_contact_channel", ""),
            "latest_follow_up_date": priority.get("latest_follow_up_date", ""),
            "follow_up_due": priority.get("follow_up_due", False),
            "recommended_channel": priority.get("recommended_channel", "avvakta"),
            "has_order_after_latest_contact": priority.get("has_order_after_latest_contact", False),
            "email_proposal_due": email_proposal["due"],
            "email_proposal_type": email_proposal["email_type"],
            "email_proposal_type_label": email_proposal["email_type_label"],
            "email_proposal_reason": email_proposal["reason"],
            "email_proposal_blockers": email_proposal["blockers"],
            "email_proposal_recipient_count": email_proposal["eligible_recipient_count"],
            "email_proposal_latest_sent_at": email_proposal["latest_sent_at"],
            "email_followup_status": email_engagement.get("email_followup_status", ""),
            "email_followup_label": email_engagement.get("email_followup_label", ""),
            "email_followup_priority": email_engagement.get("email_followup_priority"),
            "email_followup_sent_at": email_engagement.get("email_followup_sent_at", ""),
            "email_followup_last_event_at": email_engagement.get("email_followup_last_event_at", ""),
            "email_click_without_order": email_engagement.get("email_click_without_order", False),
            "email_clicked_without_order": email_engagement.get("email_clicked_without_order", False),
            "email_followup_wait_days_remaining": email_engagement.get("email_followup_wait_days_remaining", 0),
            "email_order_within_10_days": email_engagement.get("email_order_within_10_days", False),
            # Compatibility for clients deployed before the broader proposal flow.
            "reminder_email_due": email_proposal["due"] and email_type == "reminder",
            "reminder_email_reason": email_proposal["reason"] if email_type == "reminder" else "",
            "reminder_email_blockers": email_proposal["blockers"],
            "reminder_email_recipient_count": email_proposal["eligible_recipient_count"],
            "reminder_email_latest_sent_at": email_proposal["latest_sent_at"],
        }

    record_performance_step(
        "calculation.customer_insights",
        calculation_started,
        len(insights),
    )
    return jsonify(insights)


def suggestion_error_response(exc):
    return planning_error(
        exc.code, str(exc), exc.status, **dict(exc.extra or {})
    )


def suggestion_request_revision(data, field="expected_revision"):
    try:
        revision = int(data.get(field))
    except (TypeError, ValueError):
        return None
    return revision if revision >= 1 else None


def preview_suggestion_request_revision(data, field="expected_suggestion_revision"):
    try:
        revision = int(data.get(field))
    except (TypeError, ValueError):
        return None
    return revision if revision >= 0 else None


def suggestion_preview_limit(value):
    if value in (None, ""):
        return PLANNING_SUGGESTION_PREVIEW_DEFAULT
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return PLANNING_SUGGESTION_PREVIEW_DEFAULT
    return max(0, parsed)


def suggestion_queue_payload(
    spreadsheet, owner, preview_limit=PLANNING_SUGGESTION_PREVIEW_DEFAULT
):
    with performance_step("suggestions.activity_snapshot") as measurement:
        _activity_sheet, _activity_headers, indexed_activities = (
            get_planned_activity_snapshot(spreadsheet)
        )
        activity_rows = [row for _index, row in indexed_activities]
        measurement["row_count"] = len(activity_rows)
    with performance_step("suggestions.candidates") as measurement:
        candidates = planning_suggestion_candidates(
            spreadsheet, owner, activity_rows
        )
        measurement["row_count"] = len(candidates)
    with performance_step("suggestions.queue") as measurement:
        suggestion, queue_preview, pending_count = planning_suggestion_service(
            spreadsheet
        ).queue(
            owner, candidates, activity_rows,
            preview_limit=suggestion_preview_limit(preview_limit),
        )
        measurement["row_count"] = pending_count
    return suggestion, queue_preview, pending_count


def suggestion_candidates_by_id(owner, candidates):
    return {
        deterministic_suggestion_id(
            owner.get("user_name"), candidate.get("customer_id"),
            candidate.get("decision_context_hash")
        ): candidate
        for candidate in candidates
    }


def planned_suggestion_payload_matches(
    activity, *, customer_id, contact_type, scheduled_at, note
):
    return all((
        str(activity.get("customer_id") or "").strip()
        == str(customer_id or "").strip(),
        normalize_planning_contact_type(activity.get("contact_type"))
        == normalize_planning_contact_type(contact_type),
        planning_datetime_text(activity.get("scheduled_at"))
        == planning_datetime_text(scheduled_at),
        str(activity.get("note") or "").strip() == str(note or "").strip(),
        str(activity.get("source") or "").strip().casefold()
        == "system_suggestion",
    ))


def reconcile_suggestion_activity_link(
    service, suggestion, activity, *, live_candidate=None, contact_type=""
):
    activity_id = str(activity.get("planned_activity_id") or "").strip()
    status = str(suggestion.get("status") or "pending").strip().casefold()
    linked_id = str(suggestion.get("planned_activity_id") or "").strip()
    if status == "planned" and linked_id == activity_id:
        return suggestion
    if status != "pending":
        raise SuggestionError(
            "suggestion_activity_integrity_conflict",
            "Rekommendationen och den planerade aktiviteten har motstridigt state.",
            409,
        )
    repair_request_id = f"repair-plan:{activity_id}"
    repaired, _duplicate = service.transition(
        suggestion.get("suggestion_id"),
        owner_name=suggestion.get("user_name"),
        action="plan",
        expected_revision=planning_revision(suggestion),
        request_id=repair_request_id,
        fingerprint=suggestion_mutation_fingerprint(
            "plan", suggestion.get("suggestion_id"), repair_request_id,
            {"planned_activity_id": activity_id},
        ),
        planned_activity_id=activity_id,
        actual_contact_type=contact_type,
        live_candidate=live_candidate,
    )
    return repaired


@app.route("/planning/suggestions", methods=["GET"])
def planning_suggestions():
    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        app.logger.exception("Could not open suggestion store")
        return planning_error(
            "suggestion_store_unavailable",
            "Rekommendationerna kunde inte laddas. Försök igen.",
            503,
        )
    requested_owner = request.args.get("owner") or request.args.get("user_name")
    owner, owner_error = resolve_planning_owner(
        spreadsheet,
        requested_owner,
        default_admin_to_first_seller=True,
    )
    if owner_error is not None:
        return owner_error
    preview_limit = suggestion_preview_limit(request.args.get("preview_limit"))
    try:
        suggestion, queue_preview, pending_count = suggestion_queue_payload(
            spreadsheet, owner, preview_limit=preview_limit
        )
    except Exception:
        app.logger.exception("Could not build suggestion queue")
        return planning_error(
            "suggestion_store_unavailable",
            "Rekommendationerna kunde inte laddas. Försök igen.",
            503,
        )
    return jsonify({
        "ok": True,
        "suggestion": suggestion,
        "queue_preview": queue_preview,
        "pending_count": pending_count,
        "preview_limit": preview_limit,
        "generated_at": planning_timestamp(),
        "score_version": (
            "phase1" if planning_suggestion_stub_enabled() else SCORE_VERSION
        ),
    })


@app.route("/planning/calibration-export", methods=["GET"])
def planning_calibration_export():
    spreadsheet = get_spreadsheet_with_retry()
    user = current_user()
    customers = get_customer_rows(spreadsheet)
    try:
        event_sheet = get_worksheet(spreadsheet, SCORE_EVENTS_SHEET)
        events = worksheet_to_dicts(
            event_sheet, expected_columns=SCORE_EVENT_COLUMNS
        )
    except WorksheetNotFound:
        events = []
    if not user_is_admin(user):
        allowed_owner_keys = {
            normalize_key(user.get("user_name")), normalize_key(user.get("name"))
        }
        events = [
            event for event in events
            if normalize_key(event.get("user_name")) in allowed_owner_keys
            or normalize_key(event.get("sales_person")) in allowed_owner_keys
        ]
    rows = build_calibration_rows(events, get_order_rows(spreadsheet), customers)
    return jsonify({
        "score_bands": ["0-49", "50-69", "70-79", "80-89", "90-100"],
        "rows": rows,
    })


def mutate_planning_suggestion(suggestion_id, action):
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error("invalid_request", "Begäran är ogiltig.", 400)
    client_request_id = normalize_client_request_id(data.get("client_request_id"))
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id", "Ett giltigt request-ID krävs.", 400,
            field="client_request_id"
        )
    expected_revision = suggestion_request_revision(data)
    if expected_revision is None:
        return planning_error(
            "invalid_expected_revision", "Rekommendationens revision är ogiltig.",
            400, field="expected_revision"
        )
    try:
        spreadsheet = get_spreadsheet_with_retry()
        service = planning_suggestion_service(spreadsheet)
        _sheet, _events, _headers, _row_index, row = service.find(suggestion_id)
        owner, owner_error = resolve_planning_owner(
            spreadsheet, row.get("user_name")
        )
        if owner_error is not None:
            return owner_error
        _activity_sheet, _activity_headers, indexed_activities = (
            get_planned_activity_snapshot(spreadsheet)
        )
        activity_rows = [item for _index, item in indexed_activities]
        live_candidate = suggestion_candidates_by_id(
            owner,
            planning_suggestion_candidates(spreadsheet, owner, activity_rows),
        ).get(suggestion_id)
        fingerprint = suggestion_mutation_fingerprint(
            action, suggestion_id, client_request_id
        )
        updated, duplicate = service.transition(
            suggestion_id,
            owner_name=owner.get("user_name"),
            action=action,
            expected_revision=expected_revision,
            request_id=client_request_id,
            fingerprint=fingerprint,
            live_candidate=live_candidate,
        )
        next_suggestion, queue_preview, pending_count = suggestion_queue_payload(
            spreadsheet, owner
        )
    except SuggestionError as exc:
        return suggestion_error_response(exc)
    except Exception:
        app.logger.exception("Could not mutate planning suggestion")
        return planning_error(
            "suggestion_store_unavailable",
            "Rekommendationen kunde inte sparas. Försök igen.",
            503,
        )
    return jsonify({
        "ok": True,
        "duplicate": duplicate,
        "suggestion": public_suggestion(updated, live_candidate),
        "next_suggestion": next_suggestion,
        "queue_preview": queue_preview,
        "pending_count": pending_count,
    })


@app.route("/planning/suggestions/<suggestion_id>/snooze", methods=["POST"])
def snooze_planning_suggestion(suggestion_id):
    return mutate_planning_suggestion(suggestion_id, "snooze")


@app.route("/planning/suggestions/<suggestion_id>/dismiss", methods=["POST"])
def dismiss_planning_suggestion(suggestion_id):
    return mutate_planning_suggestion(suggestion_id, "dismiss")


@app.route("/planning/suggestions/<suggestion_id>/plan", methods=["POST"])
def plan_planning_suggestion(suggestion_id):
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error("invalid_request", "Begäran är ogiltig.", 400)
    client_request_id = normalize_client_request_id(data.get("client_request_id"))
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id", "Ett giltigt request-ID krävs.", 400,
            field="client_request_id"
        )
    expected_revision = preview_suggestion_request_revision(
        data, "expected_suggestion_revision"
    )
    if expected_revision is None:
        return planning_error(
            "invalid_expected_revision", "Rekommendationens revision är ogiltig.",
            400, field="expected_suggestion_revision"
        )
    contact_type = normalize_planning_contact_type(data.get("contact_type"))
    if contact_type not in PLANNING_CONTACT_TYPES:
        return planning_error(
            "invalid_contact_type", "Välj Besök, Telefon eller Mejl.", 400,
            field="contact_type"
        )
    scheduled_at = parse_planning_datetime(data.get("scheduled_at"))
    if scheduled_at is None:
        return planning_error(
            "invalid_scheduled_at", "Ange ett giltigt datum och klockslag.", 400,
            field="scheduled_at"
        )
    note = str(data.get("note") or "").strip()
    if len(note) > 300:
        return planning_error(
            "note_too_long", "Anteckningen får vara högst 300 tecken.", 400,
            field="note"
        )
    try:
        spreadsheet = get_spreadsheet_with_retry()
        service = planning_suggestion_service(spreadsheet)
        with _planning_write_lock:
            if expected_revision == 0:
                owner, owner_error = resolve_planning_owner(
                    spreadsheet, data.get("user_name")
                )
                suggestion = {
                    "customer_id": str(data.get("customer_id") or "").strip()
                }
            else:
                _suggestion_sheet, _events, _headers, _row_index, suggestion = (
                    service.find(suggestion_id)
                )
                owner, owner_error = resolve_planning_owner(
                    spreadsheet, suggestion.get("user_name")
                )
            if owner_error is not None:
                return owner_error
            if str(data.get("customer_id") or "").strip() != str(
                suggestion.get("customer_id") or ""
            ).strip():
                return planning_error(
                    "suggestion_customer_mismatch",
                    "Rekommendationen kan inte flyttas till en annan kund.",
                    409,
                    field="customer_id",
                )
            customer = resolve_planning_customer(
                spreadsheet, {"customer_id": suggestion.get("customer_id")}
            )
            if not customer or customer_is_cancelled(customer):
                return planning_error(
                    "suggestion_stale",
                    "Rekommendationens kund eller affärskontext har ändrats.",
                    409,
                )
            _activity_sheet, _activity_headers, indexed_activities = (
                get_planned_activity_snapshot(spreadsheet)
            )
            activity_rows = [row for _index, row in indexed_activities]
            live_candidates = planning_suggestion_candidates(
                spreadsheet, owner, activity_rows
            )
            live_candidate = suggestion_candidates_by_id(
                owner, live_candidates
            ).get(suggestion_id)
            live_candidate_is_actionable = bool(
                live_candidate
                and not live_candidate.get("externally_suppressed")
                and live_candidate.get("primary_trigger_key") != "scoring_context"
            )
            if expected_revision == 0:
                if not live_candidate_is_actionable:
                    return planning_error(
                        "suggestion_stale",
                        "Rekommendationen har ändrats. Ingen aktivitet skapades.",
                        409,
                    )
                if str(live_candidate.get("customer_id") or "").strip() != str(
                    data.get("customer_id") or ""
                ).strip():
                    return planning_error(
                        "suggestion_customer_mismatch",
                        "Rekommendationen kan inte flyttas till en annan kund.",
                        409,
                        field="customer_id",
                    )
                suggestion, _materialized = service.materialize_candidate(
                    owner, live_candidate
                )
                expected_revision = planning_revision(suggestion)
            plan_request_scope = planning_request_scope(
                current_user(), "suggestion-plan", suggestion_id,
                client_request_id
            )
            activity_id = stable_planning_uuid(
                "suggestion-activity", suggestion_id, plan_request_scope
            )
            create_fingerprint = planning_create_fingerprint(
                actor=current_user(),
                owner=owner,
                customer_id=customer.get("customer_id"),
                contact_type=contact_type,
                scheduled_at=scheduled_at,
                duration_minutes=PLANNING_CONTACT_DURATIONS[contact_type],
                note=note,
                source="system_suggestion",
                source_contact_id="",
            )
            active_suggestion_activities = [
                row for row in activity_rows
                if str(row.get("source_suggestion_id") or "").strip()
                == suggestion_id
                and str(row.get("status") or "planned").strip().casefold()
                == "planned"
            ]
            if len(active_suggestion_activities) > 1:
                activity_ids = [
                    str(row.get("planned_activity_id") or "").strip()
                    for row in active_suggestion_activities
                ]
                app.logger.error(
                    "suggestion_activity_integrity_conflict suggestion_id=%s activity_ids=%s",
                    suggestion_id,
                    activity_ids,
                )
                return planning_error(
                    "suggestion_activity_integrity_conflict",
                    "Rekommendationen har flera aktiva aktiviteter och mÃ¥ste granskas.",
                    409,
                )
            if active_suggestion_activities:
                activity = active_suggestion_activities[0]
                updated = reconcile_suggestion_activity_link(
                    service,
                    suggestion,
                    activity,
                    live_candidate=live_candidate,
                    contact_type=activity.get("contact_type"),
                )
                same_payload = planned_suggestion_payload_matches(
                    activity,
                    customer_id=customer.get("customer_id"),
                    contact_type=contact_type,
                    scheduled_at=scheduled_at,
                    note=note,
                )
                if not same_payload:
                    return planning_error(
                        "suggestion_plan_already_materialized",
                        "Rekommendationen har redan en planerad aktivitet med annat innehÃ¥ll.",
                        409,
                        activity=public_planned_activity(activity),
                    )
                next_suggestion, queue_preview, pending_count = suggestion_queue_payload(
                    spreadsheet, owner
                )
                return jsonify({
                    "ok": True,
                    "duplicate": True,
                    "repaired": True,
                    "activity": public_planned_activity(activity),
                    "suggestion": public_suggestion(updated, live_candidate),
                    "next_suggestion": next_suggestion,
                    "queue_preview": queue_preview,
                    "pending_count": pending_count,
                }), 200
            existing_activity = next((
                row for row in activity_rows
                if str(row.get("planned_activity_id") or "").strip() == activity_id
                or str(row.get("client_request_id") or "").strip()
                == plan_request_scope
            ), None)
            if existing_activity:
                if str(existing_activity.get("create_fingerprint") or "").strip() not in {
                    "", create_fingerprint
                }:
                    return planning_error(
                        "idempotency_payload_mismatch",
                        "Samma request-ID har redan använts med ett annat innehåll.",
                        409,
                    )
                activity = existing_activity
            else:
                if not live_candidate_is_actionable:
                    return planning_error(
                        "suggestion_stale",
                        "Rekommendationen har ändrats. Ingen aktivitet skapades.",
                        409,
                    )
                if expected_revision != planning_revision(suggestion):
                    return planning_error(
                        "suggestion_stale",
                        "Rekommendationen har ändrats. Ingen aktivitet skapades.",
                        409,
                    )
                activity = build_planned_activity_row(
                    activity_id=activity_id,
                    owner=owner,
                    customer=customer,
                    contact_type=contact_type,
                    scheduled_at=scheduled_at,
                    note=note,
                    source="system_suggestion",
                    client_request_id=plan_request_scope,
                    create_fingerprint=create_fingerprint,
                    revision=1,
                    source_suggestion_id=suggestion_id,
                    source_trigger_key=live_candidate.get("primary_trigger_key"),
                    recommended_contact_type=live_candidate.get(
                        "recommended_contact_type"
                    ),
                )
                append_dict_row(
                    _activity_sheet, PLANNED_ACTIVITY_COLUMNS, activity
                )
            transition_fingerprint = suggestion_mutation_fingerprint(
                "plan", suggestion_id, client_request_id, {
                    "customer_id": customer.get("customer_id"),
                    "contact_type": contact_type,
                    "scheduled_at": planning_datetime_text(scheduled_at),
                    "duration_minutes": PLANNING_CONTACT_DURATIONS[contact_type],
                    "note": note,
                }
            )
            updated, duplicate = service.transition(
                suggestion_id,
                owner_name=owner.get("user_name"),
                action="plan",
                expected_revision=expected_revision,
                request_id=client_request_id,
                fingerprint=transition_fingerprint,
                planned_activity_id=activity_id,
                actual_contact_type=contact_type,
                live_candidate=live_candidate,
            )
        next_suggestion, queue_preview, pending_count = suggestion_queue_payload(
            spreadsheet, owner
        )
    except SuggestionError as exc:
        return suggestion_error_response(exc)
    except CustomerResolutionError as exc:
        return planning_error(exc.code, str(exc), exc.status)
    except Exception:
        app.logger.exception("Could not plan suggestion")
        return planning_error(
            "suggestion_store_unavailable",
            "Aktiviteten kunde inte planeras. Försök igen med samma request-ID.",
            503,
        )
    return jsonify({
        "ok": True,
        "duplicate": duplicate,
        "activity": public_planned_activity(activity),
        "suggestion": public_suggestion(updated, live_candidate),
        "next_suggestion": next_suggestion,
        "queue_preview": queue_preview,
        "pending_count": pending_count,
    }), (200 if duplicate else 201)


@app.route("/planning/activities", methods=["GET", "POST"])
def planning_activities():
    data = request.get_json(silent=True) if request.method == "POST" else {}
    data = data if isinstance(data, dict) else {}
    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        app.logger.exception("Could not open planning store")
        return planning_error(
            "planning_store_unavailable",
            "Planeringen kunde inte laddas. Försök igen.",
            503,
        )

    requested_owner = (
        data.get("user_name")
        if request.method == "POST"
        else request.args.get("user_name")
    )
    owner, owner_error = resolve_planning_owner(
        spreadsheet,
        requested_owner,
        default_admin_to_first_seller=request.method == "GET",
    )
    if owner_error is not None:
        return owner_error

    if request.method == "POST":
        client_request_id = normalize_client_request_id(
            data.get("client_request_id")
        )
        if not client_request_id:
            return planning_error(
                "invalid_client_request_id",
                "Ett giltigt request-ID krävs för att undvika dubbletter.",
                400,
                field="client_request_id",
            )
        contact_type = normalize_planning_contact_type(data.get("contact_type"))
        if contact_type not in PLANNING_CONTACT_TYPES:
            return planning_error(
                "invalid_contact_type",
                "Välj Besök, Telefon eller Mejl.",
                400,
                field="contact_type",
            )
        scheduled_at = parse_planning_datetime(data.get("scheduled_at"))
        if scheduled_at is None:
            return planning_error(
                "invalid_scheduled_at",
                "Ange ett giltigt datum och klockslag.",
                400,
                field="scheduled_at",
            )
        note = str(data.get("note") or "").strip()
        if len(note) > 300:
            return planning_error(
                "note_too_long",
                "Anteckningen får vara högst 300 tecken.",
                400,
                field="note",
            )
        source = str(data.get("source") or "manual").strip().casefold()
        if source not in {"manual", "follow_up"}:
            return planning_error(
                "invalid_activity_source",
                "Aktiviteten kan inte skapas med den källan.",
                400,
                field="source",
            )
        if not str(data.get("customer_id") or "").strip():
            return planning_error(
                "customer_id_required",
                "customer_id krävs för nya planerade aktiviteter.",
                422,
                field="customer_id",
            )
        try:
            customer = resolve_planning_customer(spreadsheet, data)
        except CustomerResolutionError as exc:
            return planning_error(
                exc.code, str(exc), exc.status, field="customer_id"
            )
        if not customer:
            return planning_error(
                "customer_not_found",
                "Butiken kunde inte hittas.",
                404,
                field="customer_row",
            )
        if not customer_access_allowed(customer, current_user()):
            return planning_error(
                "customer_not_found",
                "Butiken kunde inte hittas.",
                404,
                field="customer_id",
            )
        if not customer_owned_by_user(customer, owner):
            return planning_error(
                "planning_owner_customer_mismatch",
                "Kunden tillhör inte längre den valda säljaren.",
                409,
                field="customer_id",
            )
        if customer_is_cancelled(customer):
            return planning_error(
                "customer_cancelled",
                "Avslutade kunder kan inte få nya planerade aktiviteter.",
                409,
            )

        resolved_source_contact_id = ""
        if source == "follow_up":
            resolved_source_contact_id = ensure_followup_source_contact_id(
                spreadsheet,
                customer_name=customer.get("customer"),
                customer_id=customer.get("customer_id"),
                customer_number=customer.get("customer_number"),
                source_contact_id=data.get("source_contact_id"),
                owner=owner,
            )
            if not resolved_source_contact_id:
                return planning_error(
                    "follow_up_source_not_found",
                    "Uppföljningen kunde inte kopplas till den ursprungliga kontakten.",
                    409,
                    field="source_contact_id",
                )

        with _planning_write_lock:
            sheet, _headers, existing_rows = get_planned_activity_snapshot(
                spreadsheet
            )
            create_request_scope = planning_request_scope(
                current_user(),
                "create",
                owner.get("user_name"),
                client_request_id,
            )
            activity_id = stable_planning_uuid(
                "activity",
                owner.get("user_name"),
                create_request_scope,
            )
            source_contact_id_for_fingerprint = resolved_source_contact_id
            create_fingerprint = planning_create_fingerprint(
                actor=current_user(),
                owner=owner,
                customer_id=customer.get("customer_id"),
                contact_type=contact_type,
                scheduled_at=scheduled_at,
                duration_minutes=PLANNING_CONTACT_DURATIONS[contact_type],
                note=note,
                source=source,
                source_contact_id=source_contact_id_for_fingerprint,
            )
            for _row_index, existing in existing_rows:
                if (
                    planning_owner_matches(existing, owner)
                    and (
                        str(
                            existing.get("planned_activity_id") or ""
                        ).strip() == activity_id
                        or (
                            str(
                                existing.get("client_request_id") or ""
                            ).strip() in {
                                create_request_scope,
                                client_request_id,
                            }
                            and str(
                                existing.get("source") or ""
                            ).strip().casefold() in {"manual", "follow_up"}
                        )
                    )
                ):
                    existing_fingerprint = str(
                        existing.get("create_fingerprint") or ""
                    ).strip()
                    if (
                        existing_fingerprint
                        and existing_fingerprint != create_fingerprint
                    ):
                        return planning_error(
                            "idempotency_payload_mismatch",
                            "Samma request-ID har redan använts med ett annat innehåll.",
                            409,
                            activity=public_planned_activity(existing),
                        )
                    try:
                        mirror_synced = sync_planned_followup_mirror(
                            spreadsheet,
                            existing,
                        )
                    except Exception:
                        app.logger.exception(
                            "Could not repair follow-up mirror on create retry"
                        )
                        mirror_synced = False
                    if not mirror_synced:
                        return planning_error(
                            "follow_up_mirror_failed",
                            "Aktiviteten sparades men uppföljningsdatumet kunde inte synkas. Försök igen med samma request-ID.",
                            503,
                        )
                    return jsonify({
                        "ok": True,
                        "duplicate": True,
                        "activity": public_planned_activity(existing),
                    })

            source_contact_id = resolved_source_contact_id
            row_data = build_planned_activity_row(
                activity_id=activity_id,
                owner=owner,
                customer=customer,
                contact_type=contact_type,
                scheduled_at=scheduled_at,
                note=note,
                source=source,
                source_contact_id=source_contact_id,
                client_request_id=create_request_scope,
                create_fingerprint=planning_create_fingerprint(
                    actor=current_user(),
                    owner=owner,
                    customer_id=customer.get("customer_id"),
                    contact_type=contact_type,
                    scheduled_at=scheduled_at,
                    duration_minutes=PLANNING_CONTACT_DURATIONS[contact_type],
                    note=note,
                    source=source,
                    source_contact_id=source_contact_id,
                ),
                revision=1,
            )
            append_dict_row(
                sheet,
                PLANNED_ACTIVITY_COLUMNS,
                row_data,
            )
            try:
                mirror_synced = sync_planned_followup_mirror(
                    spreadsheet,
                    row_data,
                )
            except Exception:
                app.logger.exception(
                    "Activity saved but follow-up mirror failed"
                )
                mirror_synced = False
            if not mirror_synced:
                return planning_error(
                    "follow_up_mirror_failed",
                    "Aktiviteten sparades men uppföljningsdatumet kunde inte synkas. Försök igen med samma request-ID.",
                    503,
                )
        return jsonify({
            "ok": True,
            "duplicate": False,
            "activity": public_planned_activity(row_data),
        }), 201

    calculation_started = time.perf_counter()
    start_date = parse_planning_date(request.args.get("start"))
    end_date = parse_planning_date(request.args.get("end"))
    if start_date is None:
        return planning_error(
            "invalid_start_date",
            "Ange ett giltigt startdatum.",
            400,
            field="start",
        )
    if end_date is None:
        return planning_error(
            "invalid_end_date",
            "Ange ett giltigt slutdatum.",
            400,
            field="end",
        )
    if end_date < start_date or (end_date - start_date).days > 62:
        return planning_error(
            "invalid_date_range",
            "Datumintervallet måste vara sammanhängande och högst 63 dagar.",
            400,
        )

    try:
        _sheet, _headers, activity_rows = get_planned_activity_snapshot(
            spreadsheet
        )
        contact_sheet = get_worksheet(spreadsheet, "sales_activities")
        ensure_contact_worksheet_schema(contact_sheet)
        _contact_headers, indexed_contacts = worksheet_snapshot(
            contact_sheet,
            expected_columns=CONTACT_COLUMNS,
        )
        planning_customers = get_customer_rows(spreadsheet)
    except Exception:
        app.logger.exception("Could not read planning worksheets")
        return planning_error(
            "planning_store_unavailable",
            "Planeringen kunde inte laddas. Försök igen.",
            503,
        )

    now = stockholm_now()
    planning_customer_lookup = CustomerLookup(planning_customers)
    owner_activities_all = [
        (row_index, row)
        for row_index, row in activity_rows
        if (
            customer_owned_by_user(
                related_row_customer(
                    row,
                    planning_customers,
                    customer_lookup=planning_customer_lookup,
                ),
                owner,
            )
            and customer_access_allowed(
                related_row_customer(
                    row,
                    planning_customers,
                    customer_lookup=planning_customer_lookup,
                ),
                current_user(),
            )
        )
    ]
    activities = []
    for _row_index, row in owner_activities_all:
        scheduled = parse_planning_datetime(row.get("scheduled_at"))
        if not scheduled or not start_date <= scheduled.date() <= end_date:
            continue
        activities.append(public_planned_activity(row, now=now))
    activities.sort(
        key=lambda item: (
            item.get("scheduled_at") or "",
            item.get("route_sequence") or 999,
            item.get("customer") or "",
        )
    )

    owner_contacts = [
        (row_index, row)
        for row_index, row in indexed_contacts
        if (
            contact_currently_owned_by(
                row,
                owner,
                planning_customers,
                customer_lookup=planning_customer_lookup,
            )
            and customer_access_allowed(
                related_row_customer(
                    row,
                    planning_customers,
                    customer_lookup=planning_customer_lookup,
                ),
                current_user(),
            )
        )
    ]
    unplanned_contacts = []
    for row_index, row in owner_contacts:
        if str(row.get("planned_activity_id") or "").strip():
            continue
        contact_date = parse_date_value(row.get("date_time"))
        if contact_date and start_date <= contact_date <= end_date:
            item = public_unplanned_contact(row, row_index)
            customer = related_row_customer(
                row,
                planning_customers,
                customer_lookup=planning_customer_lookup,
            )
            item.update({
                "customer_row": customer.get("row") if customer else None,
                "customer_number": (
                    str(customer.get("customer_number") or "").strip()
                    if customer else ""
                ),
                "latest_contact_date": contact_date.isoformat(),
                "reason": str(row.get("result") or "").strip(),
            })
            unplanned_contacts.append(item)
    unplanned_contacts.sort(
        key=lambda item: item.get("date_time") or "",
        reverse=True,
    )

    include_followups = str(
        request.args.get("include_followups", "1")
    ).strip().casefold() not in {"0", "false", "no"}
    unscheduled_followups = []
    try:
        contact_rows_only = [row for _row_index, row in owner_contacts]
        contact_features = (
            build_contact_features(
                contact_rows_only,
                build_order_features(get_order_rows(spreadsheet)),
            )
            if include_followups else {}
        )
        active_followup_customer_keys = set()
        for _row_index, row in owner_activities_all:
            if (
                str(row.get("source") or "").strip().casefold() != "follow_up"
                or str(row.get("status") or "").strip().casefold()
                not in {"planned", "completed"}
            ):
                continue
            active_followup_customer_keys.update({
                normalize_key(row.get("customer_key")),
                normalize_key(row.get("customer")),
            })
        active_followup_customer_keys.discard("")
        for customer_key, feature in contact_features.items():
            follow_up_date = feature.get("latest_follow_up_date")
            if (
                not follow_up_date
                or feature.get("follow_up_resolved")
                or not start_date <= follow_up_date <= end_date
                or normalize_key(customer_key) in active_followup_customer_keys
            ):
                continue
            source_index = None
            source_row = None
            for row_index, row in owner_contacts:
                if (
                    normalize_key(row.get("customer")) == normalize_key(customer_key)
                    and parse_date_value(row.get("follow_up_date")) == follow_up_date
                ):
                    if source_index is None or row_index > source_index:
                        source_index, source_row = row_index, row
            if source_row is None:
                continue
            customer = related_row_customer(
                source_row,
                planning_customers,
                customer_lookup=planning_customer_lookup,
            )
            unscheduled_followups.append({
                "customer": str(source_row.get("customer") or "").strip(),
                "customer_key": normalize_key(customer_key),
                "customer_row": customer.get("row") if customer else None,
                "customer_number": (
                    str(customer.get("customer_number") or "").strip()
                    if customer else ""
                ),
                "follow_up_date": follow_up_date.isoformat(),
                "source_contact_id": str(
                    source_row.get("contact_id") or ""
                ).strip(),
                "source_contact_row": source_index,
                "latest_contact_result": feature.get(
                    "latest_contact_result", ""
                ),
                "contact_type": normalize_planning_contact_type(
                    source_row.get("contact_channel")
                ),
                "note": str(
                    feature.get("latest_contact_comment") or ""
                ).strip(),
            })
    except Exception:
        app.logger.warning(
            "Could not build unscheduled legacy follow-ups",
            exc_info=True,
        )

    unscheduled_followups_overdue, unscheduled_followups_upcoming = (
        build_unscheduled_followup_groups(
            indexed_contacts=owner_contacts,
            activities=[row for _row_index, row in owner_activities_all],
            customers=planning_customers,
            selected_start=start_date,
            selected_end=end_date,
            today=stockholm_today(),
        ) if include_followups else ([], [])
    )
    unscheduled_followups = (
        unscheduled_followups_overdue + unscheduled_followups_upcoming
    )

    summaries = planning_day_summaries(
        start_date,
        end_date,
        activities,
        unplanned_contacts,
    )
    day_summaries = [
        {
            **item,
            "active_count": item["activity_count"],
            "completed_count": item["completed"],
            "planned_count": item["planned"],
        }
        for item in summaries
    ]
    try:
        available_users = [
            {
                **public_user(user),
                "sales_person": str(user.get("name") or "").strip(),
                "display_name": str(user.get("name") or "").strip(),
            }
            for user in get_user_rows(spreadsheet)
            if user_can_be_sales_owner(user, planning_customers)
        ]
        available_users.sort(
            key=lambda user: normalize_key(user.get("user_name"))
        )
        if not user_is_admin(current_user()):
            available_users = [
                user
                for user in available_users
                if normalize_key(user.get("user_name"))
                == normalize_key(owner.get("user_name"))
            ]
    except Exception:
        available_users = [{
            **owner,
            "sales_person": str(owner.get("name") or "").strip(),
            "display_name": str(owner.get("name") or "").strip(),
        }]
    payload = {
        "ok": True,
        "owner": owner,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "activities": activities,
        "summaries": summaries,
        "day_summaries": day_summaries,
        "days": {item["date"]: item for item in summaries},
        "available_users": available_users,
        "unplanned_contacts": unplanned_contacts,
        "unscheduled_followups_overdue": unscheduled_followups_overdue,
        "unscheduled_followups_upcoming": unscheduled_followups_upcoming,
        "unscheduled_followups": sorted(
            unscheduled_followups,
            key=lambda item: (
                item.get("follow_up_date") or "",
                item.get("customer") or "",
            ),
        ),
    }
    record_performance_step(
        "calculation.planning",
        calculation_started,
        len(activities),
    )
    return jsonify(payload)


@app.route("/planning/activities/<activity_id>", methods=["PATCH"])
def update_planning_activity(activity_id):
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error(
            "invalid_request",
            "Begäran är ogiltig.",
            400,
        )
    client_request_id = normalize_client_request_id(
        data.get("client_request_id")
    )
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krävs.",
            400,
            field="client_request_id",
        )
    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        return planning_error(
            "planning_store_unavailable",
            "Planeringen kunde inte sparas. Försök igen.",
            503,
        )

    mutable_fields = {
        "contact_type", "scheduled_at", "note", "status", "customer_id"
    }
    requested_fields = mutable_fields.intersection(data)
    if not requested_fields:
        return planning_error(
            "no_activity_changes",
            "Inga ändringar skickades.",
            400,
        )

    updated_customer = None
    if "customer_id" in data:
        try:
            updated_customer = resolve_planning_customer(
                spreadsheet,
                {"customer_id": data.get("customer_id")},
            )
        except CustomerResolutionError as exc:
            return planning_error(
                exc.code, str(exc), exc.status, field="customer_id"
            )
        if not updated_customer:
            return planning_error(
                "customer_not_found",
                "Butiken kunde inte hittas.",
                404,
                field="customer_id",
            )
        if not customer_access_allowed(updated_customer, current_user()):
            return planning_error(
                "customer_not_found",
                "Butiken kunde inte hittas.",
                404,
                field="customer_id",
            )
        if customer_is_cancelled(updated_customer):
            return planning_error(
                "customer_cancelled",
                "Avslutade kunder kan inte få nya planerade aktiviteter.",
                409,
            )

    with _planning_write_lock:
        sheet, headers, row_index, current = find_planned_activity(
            spreadsheet,
            activity_id,
        )
        if not row_index:
            return planning_error(
                "activity_not_found",
                "Aktiviteten kunde inte hittas.",
                404,
            )
        caller = current_user()
        try:
            current_customer = resolve_planning_customer(
                spreadsheet,
                {
                    "customer_id": current.get("customer_id"),
                    "customer_number": current.get("customer_number"),
                    "customer": current.get("customer"),
                    "customer_row": current.get("customer_row"),
                },
            )
        except CustomerResolutionError:
            current_customer = None
        if not customer_access_allowed(current_customer, caller):
            return planning_error(
                "activity_not_found",
                "Aktiviteten kunde inte hittas.",
                404,
            )
        mutation_request_scope = planning_request_scope(
            caller,
            "update",
            activity_id,
            client_request_id,
        )
        current_status = str(
            current.get("status") or "planned"
        ).strip().casefold()
        current_revision = planning_revision(current)
        pre_same_mutation_request = (
            str(current.get("last_mutation_request_id") or "").strip()
            in {mutation_request_scope, client_request_id}
            or (
                not str(current.get("last_mutation_request_id") or "").strip()
                and str(current.get("client_request_id") or "").strip()
                in {mutation_request_scope, client_request_id}
            )
        )
        expected_revision = data.get("expected_revision")
        if expected_revision not in (None, ""):
            try:
                expected_revision = int(expected_revision)
            except (TypeError, ValueError):
                return planning_error(
                    "invalid_expected_revision",
                    "Aktivitetens revision är ogiltig.",
                    400,
                    field="expected_revision",
                )
        else:
            expected_revision = (
                max(1, current_revision - 1)
                if pre_same_mutation_request
                and str(current.get("last_mutation_fingerprint") or "").strip()
                else current_revision
            )
        mutation_changes = {}
        if "contact_type" in data:
            mutation_changes["contact_type"] = normalize_planning_contact_type(
                data.get("contact_type")
            )
        if "scheduled_at" in data:
            mutation_changes["scheduled_at"] = planning_datetime_text(
                data.get("scheduled_at")
            )
        if "note" in data:
            mutation_changes["note"] = str(data.get("note") or "").strip()
        if "status" in data:
            mutation_changes["status"] = str(
                data.get("status") or ""
            ).strip().casefold()
        if "customer_id" in data and updated_customer is None:
            mutation_changes["customer_id"] = str(
                data.get("customer_id") or ""
            ).strip()
        mutation_fingerprint = planning_update_fingerprint(
            actor=caller,
            activity_id=activity_id,
            expected_revision=expected_revision,
            changes=mutation_changes,
        )
        same_mutation_request = pre_same_mutation_request
        if same_mutation_request:
            stored_fingerprint = str(
                current.get("last_mutation_fingerprint") or ""
            ).strip()
            if stored_fingerprint and stored_fingerprint != mutation_fingerprint:
                return planning_error(
                    "idempotency_payload_mismatch",
                    "Samma request-ID har redan använts med ett annat innehåll.",
                    409,
                    activity=public_planned_activity(current),
                )
            try:
                mirror_synced = sync_planned_followup_mirror(
                    spreadsheet,
                    current,
                )
            except Exception:
                app.logger.exception(
                    "Could not repair follow-up mirror on update retry"
                )
                mirror_synced = False
            if not mirror_synced:
                return planning_error(
                    "follow_up_mirror_failed",
                    "Aktiviteten är uppdaterad men uppföljningsdatumet kunde inte synkas. Försök igen med samma request-ID.",
                    503,
                )
            try:
                sync_suggestion_from_activity(
                    spreadsheet,
                    current,
                    request_id=mutation_request_scope,
                )
            except Exception:
                app.logger.exception(
                    "Could not repair linked suggestion on activity retry"
                )
                return planning_error(
                    "suggestion_sync_failed",
                    "Aktiviteten sparades men rekommendationen kunde inte synkas. Försök igen med samma request-ID.",
                    503,
                )
            return jsonify({
                "ok": True,
                "duplicate": True,
                "activity": public_planned_activity(current),
            })

        if "expected_revision" in data and expected_revision != current_revision:
            return planning_error(
                "revision_conflict",
                "Aktiviteten har ändrats sedan den laddades. Ladda om planeringen och försök igen.",
                409,
                activity=public_planned_activity(current),
            )
        if "expected_revision" not in data and "expected_updated_at" not in data:
            return planning_error(
                "expected_updated_at_required",
                "Aktivitetens versionsstämpel saknas. Ladda om planeringen och försök igen.",
                400,
                field="expected_updated_at",
            )
        expected_updated_at = str(
            data.get("expected_updated_at") or ""
        ).strip()
        current_updated_at = str(
            current.get("updated_at") or ""
        ).strip()
        if (
            "expected_revision" not in data
            and expected_updated_at != current_updated_at
        ):
            return planning_error(
                "planning_changed",
                "Aktiviteten har ändrats sedan den laddades. Ladda om planeringen och försök igen.",
                409,
                activity=public_planned_activity(current),
            )
        if current_status in {"completed", "cancelled"}:
            return planning_error(
                (
                    "completed_activity_immutable"
                    if current_status == "completed"
                    else "cancelled_activity_immutable"
                ),
                (
                    "En genomförd aktivitet kan inte ändras."
                    if current_status == "completed"
                    else "En inställd aktivitet kan inte ändras."
                ),
                409,
            )

        updates = {}
        if "customer_id" in data:
            updates.update({
                "customer_id": str(
                    updated_customer.get("customer_id") or ""
                ).strip(),
                "customer_key": (
                    normalize_key(updated_customer.get("customer_number"))
                    or normalize_key(updated_customer.get("customer"))
                ),
                "customer_row": updated_customer.get("row") or "",
                "customer_number": str(
                    updated_customer.get("customer_number") or ""
                ).strip(),
                "customer": str(
                    updated_customer.get("customer") or ""
                ).strip(),
            })
        if "contact_type" in data:
            contact_type = normalize_planning_contact_type(
                data.get("contact_type")
            )
            if contact_type not in PLANNING_CONTACT_TYPES:
                return planning_error(
                    "invalid_contact_type",
                    "Välj Besök, Telefon eller Mejl.",
                    400,
                    field="contact_type",
                )
            updates["contact_type"] = contact_type
            updates["duration_minutes"] = PLANNING_CONTACT_DURATIONS[
                contact_type
            ]
        if "scheduled_at" in data:
            scheduled_at = parse_planning_datetime(data.get("scheduled_at"))
            if scheduled_at is None:
                return planning_error(
                    "invalid_scheduled_at",
                    "Ange ett giltigt datum och klockslag.",
                    400,
                    field="scheduled_at",
                )
            updates["scheduled_at"] = scheduled_at.isoformat(timespec="minutes")
            updates["time_is_estimated"] = "N"
        if "note" in data:
            note = str(data.get("note") or "").strip()
            if len(note) > 300:
                return planning_error(
                    "note_too_long",
                    "Anteckningen får vara högst 300 tecken.",
                    400,
                    field="note",
                )
            updates["note"] = note
        if "status" in data:
            status = str(data.get("status") or "").strip().casefold()
            if status not in PLANNING_STATUSES:
                return planning_error(
                    "invalid_activity_status",
                    "Aktiviteten har en ogiltig status.",
                    400,
                    field="status",
                )
            if status == "completed" and current_status != "completed":
                return planning_error(
                    "completion_requires_contact",
                    "Logga kontakten för att markera aktiviteten som genomförd.",
                    409,
                )
            updates["status"] = status

        route_content_fields = {
            "contact_type", "scheduled_at", "note", "customer_id"
        }
        if (
            str(current.get("source") or "").strip().casefold() == "route"
            and route_content_fields.intersection(data)
        ):
            updates.update({
                "source": "manual",
                "route_group_id": "",
                "route_sequence": "",
                "time_is_estimated": "N",
            })
        updates["last_mutation_request_id"] = mutation_request_scope
        updates["last_mutation_fingerprint"] = mutation_fingerprint
        updates["revision"] = current_revision + 1
        updates["updated_at"] = next_planning_updated_at(
            current.get("updated_at")
        )
        update_sheet_row(sheet, row_index, headers, updates)
        updated = {**current, **updates}
        try:
            mirror_synced = sync_planned_followup_mirror(
                spreadsheet,
                updated,
            )
        except Exception:
            app.logger.exception(
                "Activity updated but follow-up mirror failed"
            )
            mirror_synced = False
        if not mirror_synced:
            return planning_error(
                "follow_up_mirror_failed",
                "Aktiviteten är uppdaterad men uppföljningsdatumet kunde inte synkas. Försök igen med samma request-ID.",
                503,
            )
        try:
            sync_suggestion_from_activity(
                spreadsheet,
                updated,
                request_id=mutation_request_scope,
            )
        except Exception:
            app.logger.exception("Activity updated but suggestion sync failed")
            return planning_error(
                "suggestion_sync_failed",
                "Aktiviteten sparades men rekommendationen kunde inte synkas. Försök igen med samma request-ID.",
                503,
            )

    return jsonify({
        "ok": True,
        "duplicate": False,
        "activity": public_planned_activity(updated),
    })


def route_proposal_error(code, message, status):
    return jsonify({
        "ok": False,
        "error": code,
        "code": code,
        "message": message,
    }), status


def parse_route_start(data):
    start = data.get("start") if isinstance(data, dict) else None
    if not isinstance(start, dict):
        return None
    latitude = start.get("latitude")
    longitude = start.get("longitude")
    if (
        isinstance(latitude, bool)
        or isinstance(longitude, bool)
        or not isinstance(latitude, (int, float))
        or not isinstance(longitude, (int, float))
    ):
        return None
    latitude = float(latitude)
    longitude = float(longitude)
    if (
        not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90 <= latitude <= 90
        or not -180 <= longitude <= 180
    ):
        return None
    return Coordinate(latitude=latitude, longitude=longitude)


def build_route_proposal_payload(
    *,
    proposal,
    start,
    candidates,
    requested_rows,
    user,
    route_date,
    max_total_seconds=MAX_TOTAL_SECONDS,
):
    stops = []
    for stop in proposal.route.stops:
        candidate = stop.candidate
        stops.append({
            "row": candidate.row,
            "customer": candidate.customer,
            "latitude": candidate.coordinate.latitude,
            "longitude": candidate.coordinate.longitude,
            "sequence": stop.sequence,
            "priority_score": candidate.priority_score,
            "required": bool(getattr(candidate, "required", False)),
            "leg_drive_minutes": seconds_to_minutes(stop.leg_drive_seconds),
            "cumulative_drive_minutes": seconds_to_minutes(
                stop.cumulative_drive_seconds
            ),
            "cumulative_total_minutes": seconds_to_minutes(
                stop.cumulative_total_seconds
            ),
        })

    pair_count = proposal.provider_pair_count
    cache_hit_rate = (
        round(proposal.provider_cache_hits / pair_count, 3)
        if pair_count
        else 0
    )
    return {
        "ok": True,
        "cached": False,
        "generated_at": stockholm_now().isoformat(timespec="seconds"),
        "route_date": route_date.isoformat(),
        "route_owner": user_route_display_name(user),
        "start": {
            "latitude": start.latitude,
            "longitude": start.longitude,
        },
        "stops": stops,
        "summary": {
            "candidate_count": len(candidates),
            "stop_count": len(stops),
            "total_priority_score": proposal.route.total_priority_score,
            "drive_minutes": seconds_to_minutes(proposal.route.drive_seconds),
            "return_drive_minutes": seconds_to_minutes(
                proposal.route.return_drive_seconds
            ),
            "service_minutes": seconds_to_minutes(
                proposal.route.service_seconds
            ),
            "total_minutes": seconds_to_minutes(proposal.route.total_seconds),
        },
        "meta": {
            "algorithm_version": proposal.solution.algorithm,
            "solver_status": proposal.solution.solver_status,
            "optimality_proven": proposal.solution.optimality_proven,
            "shortlisted": proposal.shortlisted,
            "requested_candidate_count": len(requested_rows),
            "eligible_candidate_count": proposal.input_candidate_count,
            "road_reachable_candidate_count": (
                proposal.road_reachable_candidate_count
            ),
            "matrix_candidate_count": proposal.matrix_candidate_count,
            "excluded_missing_road_time": (
                proposal.excluded_missing_road_time
            ),
            "excluded_over_budget": proposal.excluded_over_budget,
            "routing_preference": proposal.routing_preference,
            "provider_request_count": proposal.provider_request_count,
            "cache_hit_rate": cache_hit_rate,
            "solver_duration_ms": (
                proposal.solution.calculation_duration_ms
            ),
            "calculation_duration_ms": proposal.calculation_duration_ms,
            "max_total_minutes": max_total_seconds // 60,
            "max_route_stops": MAX_ROUTE_STOPS,
            "service_minutes_per_stop": SERVICE_SECONDS_PER_STOP // 60,
            "includes_return_to_start": True,
            "daily_cache_hit": False,
        },
    }


def calculate_route_proposal_for_user(
    *,
    spreadsheet,
    start,
    client_requested_rows,
    user,
    route_date,
    required_rows=(),
    anchor_rows=(),
    max_total_seconds=MAX_TOTAL_SECONDS,
    respect_requested_rows=False,
    owner=None,
):
    try:
        snapshot = get_authoritative_priority_snapshot(
            spreadsheet, today=route_date
        )
        customers = snapshot["customers"]
        priority_customers = snapshot["priorities"]
    except Exception:
        app.logger.exception(
            "Could not build priority snapshot for route proposal"
        )
        return None, route_proposal_error(
            "priority_data_unavailable",
            "Kundinsikterna kunde inte laddas. Försök igen.",
            503,
        )

    required_rows = tuple(sorted(set(required_rows or ())))
    requested_rows = tuple(sorted(set(client_requested_rows or ())))
    customer_scope_owner = owner or (
        None if user_is_admin(user) else user
    )
    if customer_scope_owner is not None:
        owned_rows = {
            customer.get("row")
            for customer in customers
            if (
                isinstance(customer.get("row"), int)
                and customer_owned_by_user(customer, customer_scope_owner)
            )
        }
        if set(required_rows) - owned_rows:
            return None, route_proposal_error(
                "customer_not_found",
                "Ett eller flera obligatoriska stopp kunde inte hittas.",
                404,
            )
        requested_rows = tuple(sorted(
            (
                set(requested_rows).intersection(owned_rows)
                if respect_requested_rows
                else owned_rows
            )
            | set(required_rows)
        ))
        if not requested_rows and not required_rows:
            return None, route_proposal_error(
                "no_eligible_candidates",
                "Du har inga egna butiker att skapa ett ruttförslag för.",
                422,
            )

    customer_by_row = {
        customer.get("row"): customer
        for customer in customers
        if isinstance(customer.get("row"), int)
    }
    priority_by_row = {
        customer.get("row"): customer
        for customer in priority_customers
        if isinstance(customer.get("row"), int)
    }
    requested_rows = tuple(sorted(set(requested_rows) | set(required_rows)))
    required_set = set(required_rows)
    candidates = []
    invalid_required = []
    for row in requested_rows:
        customer = customer_by_row.get(row)
        priority = priority_by_row.get(row)
        if not customer or customer_is_cancelled(customer):
            if row in required_set:
                invalid_required.append({
                    "row": row,
                    "reason": (
                        "customer_cancelled"
                        if customer and customer_is_cancelled(customer)
                        else "customer_not_found"
                    ),
                })
            continue
        if (
            customer_scope_owner is not None
            and not customer_owned_by_user(customer, customer_scope_owner)
        ):
            continue
        latitude = parse_coordinate_value(
            customer.get("latitude_google") or customer.get("latitude"),
            "latitude",
        )
        longitude = parse_coordinate_value(
            customer.get("longitude_google") or customer.get("longitude"),
            "longitude",
        )
        score = (priority or {}).get("priority_score", 0)
        if latitude is None or longitude is None:
            if row in required_set:
                invalid_required.append({
                    "row": row,
                    "customer": str(customer.get("customer") or "").strip(),
                    "reason": "missing_coordinates",
                })
            continue
        try:
            score = int(score)
        except (TypeError, ValueError, OverflowError):
            if row in required_set:
                score = 0
            else:
                continue
        if isinstance(score, bool):
            score = 0 if row in required_set else None
        if score is None:
            continue
        if score <= 0 and row not in required_set:
            continue
        candidates.append(
            RouteCandidate(
                row=row,
                customer=str(customer.get("customer") or "").strip(),
                coordinate=Coordinate(
                    latitude=latitude,
                    longitude=longitude,
                ),
                priority_score=score,
                required=row in required_set,
            )
        )

    if invalid_required:
        return None, route_proposal_error(
            "required_stops_not_feasible",
            "Ett eller flera obligatoriska besök kan inte ruttas.",
            422,
        )
    if not candidates:
        return None, route_proposal_error(
            "no_eligible_candidates",
            "Inga butiker med giltig position och prioritetspoäng är tillgängliga för dagens rutt.",
            422,
        )

    candidate_count_before_preselection = len(candidates)
    if required_set:
        if len(required_set) > MAX_ROUTE_STOPS:
            return None, route_proposal_error(
                "required_stops_not_feasible",
                "Dagens fasta besök överskrider max 15 stopp.",
                422,
            )
        chronological_anchor_rows = tuple(
            row for row in anchor_rows if row in required_set
        ) or required_rows
        try:
            candidates = list(anchor_aware_preselect_candidates(
                start=start,
                candidates=candidates,
                anchor_rows=chronological_anchor_rows,
                limit=ANCHOR_ROUTE_CANDIDATE_LIMIT,
            ))
        except RouteProposalError as exc:
            return None, route_proposal_error(
                exc.code, exc.public_message, exc.http_status
            )
        stops = [{
            "row": candidate.row,
            "customer": candidate.customer,
            "latitude": candidate.coordinate.latitude,
            "longitude": candidate.coordinate.longitude,
            "priority_score": candidate.priority_score,
            "required": candidate.required,
            "sequence": 0,
            "leg_drive_minutes": 0,
            "cumulative_drive_minutes": 0,
            "cumulative_total_minutes": 0,
        } for candidate in candidates]
        return {
            "ok": True,
            "cached": False,
            "generated_at": stockholm_now().isoformat(timespec="seconds"),
            "route_date": route_date.isoformat(),
            "route_owner": user_route_display_name(owner or user),
            "start": {
                "latitude": start.latitude,
                "longitude": start.longitude,
            },
            "stops": stops,
            "summary": {
                "candidate_count": len(candidates),
                "stop_count": 0,
                "total_priority_score": 0,
                "drive_minutes": 0,
                "return_drive_minutes": 0,
                "service_minutes": 0,
                "total_minutes": 0,
            },
            "meta": {
                "algorithm_version": "anchor_aware_v1",
                "solver_status": "anchor_scheduler_pending",
                "optimality_proven": False,
                "shortlisted": len(candidates) < candidate_count_before_preselection,
                "requested_candidate_count": len(requested_rows),
                "eligible_candidate_count": candidate_count_before_preselection,
                "matrix_candidate_count": len(candidates),
                "max_total_minutes": max_total_seconds // 60,
                "max_route_stops": MAX_ROUTE_STOPS,
                "service_minutes_per_stop": SERVICE_SECONDS_PER_STOP // 60,
                "includes_return_to_start": True,
                "anchor_aware": True,
                "anchor_count": len(required_set),
                "candidate_count_before_anchor_preselection": (
                    candidate_count_before_preselection
                ),
                "candidate_count_after_anchor_preselection": len(candidates),
                "anchor_candidate_limit": ANCHOR_ROUTE_CANDIDATE_LIMIT,
            },
        }, None

    matrix_candidate_limit = route_matrix_candidate_limit()
    required_candidates = [
        candidate for candidate in candidates if candidate.required
    ]
    optional_candidates = sorted(
        (
            candidate for candidate in candidates
            if not candidate.required
        ),
        key=lambda candidate: (
            -candidate.priority_score,
            candidate.row,
        ),
    )
    candidates = [
        *required_candidates,
        *optional_candidates[
            :max(0, matrix_candidate_limit - len(required_candidates))
        ],
    ]

    try:
        proposal = calculate_route_proposal(
            start=start,
            candidates=candidates,
            provider=get_route_travel_time_provider(),
            max_total_seconds=max_total_seconds,
        )
    except RouteProposalError as exc:
        if exc.http_status >= 500:
            app.logger.warning(
                "Route proposal failed (%s): %s",
                exc.code,
                exc,
            )
        return None, route_proposal_error(
            exc.code,
            exc.public_message,
            exc.http_status,
        )
    except Exception:
        app.logger.exception("Unexpected route proposal failure")
        return None, route_proposal_error(
            "route_proposal_failed",
            "Kunde inte skapa ett ruttförslag. Försök igen.",
            500,
        )

    payload = build_route_proposal_payload(
        proposal=proposal,
        start=start,
        candidates=candidates,
        requested_rows=requested_rows,
        user=owner or user,
        route_date=route_date,
        max_total_seconds=max_total_seconds,
    )
    payload.setdefault("meta", {}).update({
        "candidate_count_before_preselection": (
            candidate_count_before_preselection
        ),
        "candidate_count_after_preselection": len(candidates),
        "matrix_candidate_limit": matrix_candidate_limit,
        "matrix_pair_count": proposal.provider_pair_count,
        "provider_cache_hits": proposal.provider_cache_hits,
        "anchor_aware": False,
        "anchor_count": 0,
        "anchor_candidate_limit": ANCHOR_ROUTE_CANDIDATE_LIMIT,
    })
    app.logger.info(
        "route_metrics candidates_before=%s candidates_after=%s "
        "matrix_candidates=%s matrix_pairs=%s provider_requests=%s "
        "cache_hits=%s calculation_ms=%s final_stops=%s",
        candidate_count_before_preselection,
        len(candidates),
        proposal.matrix_candidate_count,
        proposal.provider_pair_count,
        proposal.provider_request_count,
        proposal.provider_cache_hits,
        proposal.calculation_duration_ms,
        len(proposal.route.stops),
    )
    return payload, None


def planning_rows_for_date(rows, owner, route_date):
    result = []
    for row_index, row in rows:
        if not planning_owner_matches(row, owner):
            continue
        scheduled = parse_planning_datetime(row.get("scheduled_at"))
        if scheduled and scheduled.date() == route_date:
            result.append((row_index, row))
    return result


def planning_state_fingerprint(rows):
    values = []
    for _row_index, row in rows:
        values.append({
            "id": str(row.get("planned_activity_id") or "").strip(),
            "customer_id": str(row.get("customer_id") or "").strip(),
            "scheduled_at": planning_datetime_text(row.get("scheduled_at")),
            "contact_type": normalize_planning_contact_type(
                row.get("contact_type")
            ),
            "status": str(row.get("status") or "").strip().casefold(),
            "source": str(row.get("source") or "").strip().casefold(),
            "updated_at": str(row.get("updated_at") or "").strip(),
            "revision": planning_revision(row),
        })
    canonical = json.dumps(
        sorted(values, key=lambda item: item["id"]),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return stable_planning_uuid("planning-state", canonical)


def planning_route_apply_fingerprint(preview):
    fingerprint_stops = []
    for stop in preview.get("stops") or []:
        if not isinstance(stop, dict):
            continue
        fingerprint_stops.append({
            "customer_id": str(stop.get("customer_id") or "").strip(),
            "customer_row": stop.get("customer_row") or stop.get("row"),
            "sequence": stop.get("sequence"),
            "required": bool(stop.get("required")),
            "required_activity_ids": sorted(
                str(value or "").strip()
                for value in (stop.get("required_activity_ids") or [])
                if str(value or "").strip()
            ),
            "planned_activity_id": str(
                stop.get("planned_activity_id") or ""
            ).strip(),
            "scheduled_at": planning_datetime_text(
                stop.get("scheduled_at")
            ),
            "estimated_at": planning_datetime_text(
                stop.get("estimated_at")
            ),
            "duration_minutes": stop.get("duration_minutes"),
        })
    fingerprint_stops.sort(
        key=lambda stop: (
            int(stop.get("sequence") or 0),
            int(stop.get("customer_row") or 0),
        )
    )
    canonical = json.dumps(
        {
            "route_date": str(preview.get("route_date") or "").strip(),
            "route_start_at": planning_datetime_text(
                preview.get("route_start_at")
            ),
            "start": dict(preview.get("start") or {}),
            "return_drive_minutes": (
                (preview.get("summary") or {}).get(
                    "return_drive_minutes"
                )
            ),
            "route_end_at": planning_datetime_text(
                (preview.get("timeline") or {}).get("route_end_at")
                or (preview.get("summary") or {}).get("route_end_at")
            ),
            "stops": fingerprint_stops,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return stable_planning_uuid("route-apply-preview", canonical)


def planning_fixed_activity_intervals(fixed_non_route):
    intervals = []
    for activity in fixed_non_route:
        start = parse_planning_datetime(activity.get("scheduled_at"))
        if start is None:
            return None, planning_error(
                "invalid_fixed_activity_time",
                "En fast telefon- eller mejlaktivitet saknar giltig tid.",
                422,
                planned_activity_id=activity.get(
                    "planned_activity_id",
                    "",
                ),
            )
        try:
            duration_minutes = max(
                1,
                int(math.ceil(float(
                    activity.get("duration_minutes") or 10
                ))),
            )
        except (TypeError, ValueError, OverflowError):
            duration_minutes = 10
        intervals.append({
            "start": start,
            "end": start + timedelta(minutes=duration_minutes),
            "activity": activity,
        })
    intervals.sort(key=lambda interval: interval["start"])
    for previous, current in zip(intervals, intervals[1:]):
        if current["start"] < previous["end"]:
            return None, planning_error(
                "fixed_activity_conflict",
                "Två fasta telefon- eller mejlaktiviteter överlappar varandra.",
                422,
                planned_activity_ids=[
                    previous["activity"].get("planned_activity_id", ""),
                    current["activity"].get("planned_activity_id", ""),
                ],
            )
    return intervals, None


def planning_next_unblocked_start(
    earliest_start,
    duration_minutes,
    fixed_intervals,
):
    duration = timedelta(minutes=max(0, duration_minutes))
    start = earliest_start
    while True:
        shifted = False
        end = start + duration
        for interval in fixed_intervals:
            if interval["end"] <= start:
                continue
            if end <= interval["start"]:
                break
            if start < interval["end"] and interval["start"] < end:
                start = interval["end"]
                shifted = True
                break
        if not shifted:
            return start


def schedule_planning_route_timeline(
    *,
    stops,
    fixed_non_route,
    route_start_at,
    return_drive_minutes,
):
    fixed_intervals, fixed_error = planning_fixed_activity_intervals(
        fixed_non_route
    )
    if fixed_error is not None:
        return None, None, fixed_error

    scheduled_stops = []
    timeline_segments = []
    cursor = route_start_at
    for raw_stop in stops:
        stop = dict(raw_stop)
        try:
            drive_minutes = max(
                0,
                int(math.ceil(float(
                    stop.get("leg_drive_minutes") or 0
                ))),
            )
            service_minutes = max(
                1,
                int(math.ceil(float(
                    stop.get("duration_minutes") or 20
                ))),
            )
        except (TypeError, ValueError, OverflowError):
            return None, None, planning_error(
                "invalid_route_timeline",
                "Rutten innehåller en ogiltig tidsberäkning.",
                422,
            )

        departure = planning_next_unblocked_start(
            cursor,
            drive_minutes,
            fixed_intervals,
        )
        arrival = departure + timedelta(minutes=drive_minutes)
        if drive_minutes:
            timeline_segments.append({
                "kind": "drive",
                "customer_row": stop.get("customer_row"),
                "start": departure.isoformat(timespec="minutes"),
                "end": arrival.isoformat(timespec="minutes"),
            })

        required = bool(stop.get("required"))
        booked_at = (
            parse_planning_datetime(stop.get("scheduled_at"))
            if required else None
        )
        if required and booked_at is None:
            return None, None, planning_error(
                "required_stops_not_feasible",
                "Ett obligatoriskt besök saknar giltig bokad tid.",
                422,
                planned_activity_id=stop.get(
                    "planned_activity_id",
                    "",
                ),
            )
        earliest_service = arrival
        if booked_at and booked_at > earliest_service:
            earliest_service = booked_at
        service_start = planning_next_unblocked_start(
            earliest_service,
            service_minutes,
            fixed_intervals,
        )
        if booked_at:
            delta_minutes = (
                service_start - booked_at
            ).total_seconds() / 60
            if abs(delta_minutes) > PLANNING_ROUTE_CONFLICT_MINUTES:
                return None, None, planning_error(
                    "required_stops_not_feasible",
                    "Ett obligatoriskt besök kan inte nås inom 15 minuter från bokad tid.",
                    422,
                    planned_activity_id=stop.get(
                        "planned_activity_id",
                        "",
                    ),
                    scheduled_at=booked_at.isoformat(
                        timespec="minutes"
                    ),
                    estimated_at=service_start.isoformat(
                        timespec="minutes"
                    ),
                    delta_minutes=round(delta_minutes),
                )
        service_end = service_start + timedelta(
            minutes=service_minutes
        )
        timeline_segments.append({
            "kind": "visit",
            "customer_row": stop.get("customer_row"),
            "planned_activity_id": stop.get(
                "planned_activity_id",
                "",
            ),
            "start": service_start.isoformat(timespec="minutes"),
            "end": service_end.isoformat(timespec="minutes"),
        })
        stop.update({
            "leg_departure_at": departure.isoformat(timespec="minutes"),
            "arrival_at": arrival.isoformat(timespec="minutes"),
            "estimated_at": service_start.isoformat(timespec="minutes"),
            "service_end_at": service_end.isoformat(timespec="minutes"),
            "scheduled_at": (
                booked_at.isoformat(timespec="minutes")
                if booked_at else service_start.isoformat(
                    timespec="minutes"
                )
            ),
            "time_is_estimated": not required,
        })
        scheduled_stops.append(stop)
        cursor = service_end

    try:
        return_minutes = max(
            0,
            int(math.ceil(float(return_drive_minutes or 0))),
        )
    except (TypeError, ValueError, OverflowError):
        return None, None, planning_error(
            "invalid_route_timeline",
            "Rutten saknar en giltig returtid.",
            422,
        )
    return_departure = planning_next_unblocked_start(
        cursor,
        return_minutes,
        fixed_intervals,
    )
    route_end = return_departure + timedelta(minutes=return_minutes)
    if return_minutes:
        timeline_segments.append({
            "kind": "return_drive",
            "start": return_departure.isoformat(timespec="minutes"),
            "end": route_end.isoformat(timespec="minutes"),
        })
    elapsed_minutes = (
        route_end - route_start_at
    ).total_seconds() / 60
    if elapsed_minutes >= (MAX_TOTAL_SECONDS / 60):
        return None, None, planning_error(
            "route_timeline_exceeds_capacity",
            "Rutten, fasta aktiviteter och väntetider ryms inte inom sju timmar.",
            422,
            total_minutes=round(elapsed_minutes, 1),
        )
    return scheduled_stops, {
        "route_end_at": route_end.isoformat(timespec="minutes"),
        "return_departure_at": return_departure.isoformat(
            timespec="minutes"
        ),
        "elapsed_minutes": seconds_to_minutes(
            int(math.ceil(elapsed_minutes * 60))
        ),
        "segments": timeline_segments,
    }, None


def schedule_planning_route_with_anchors(
    *,
    stops,
    fixed_non_route,
    route_start_at,
    start,
):
    """Place optional route stops around immutable visit/time anchors."""
    fixed_intervals, fixed_error = planning_fixed_activity_intervals(
        fixed_non_route
    )
    if fixed_error is not None:
        return None, None, fixed_error
    required_stop_count = sum(1 for stop in stops if stop.get("required"))
    if (
        required_stop_count > MAX_ROUTE_STOPS
        or (not required_stop_count and len(stops) > MAX_ROUTE_STOPS)
    ):
        return None, None, planning_error(
            "required_schedule_not_feasible",
            "Dagens fasta och valfria besök överskrider max 15 stopp.",
            422,
        )

    points = [start] + [
        Coordinate(
            latitude=float(stop.get("latitude")),
            longitude=float(stop.get("longitude")),
        )
        for stop in stops
    ]
    try:
        matrix_result = get_route_travel_time_provider().get_matrix_seconds(
            points,
            points,
            ephemeral_origin_indexes=frozenset({0}),
        )
    except RouteProposalError as exc:
        return None, None, route_proposal_error(
            exc.code, exc.public_message, exc.http_status
        )
    except Exception:
        app.logger.exception("Could not build anchor route matrix")
        return None, None, planning_error(
            "route_matrix_unavailable",
            "Körtiderna för dagens fasta schema kunde inte hämtas.",
            503,
        )

    matrix = matrix_result.seconds
    indexed = [
        {**dict(stop), "_matrix_index": index}
        for index, stop in enumerate(stops, start=1)
    ]
    anchors = sorted(
        (stop for stop in indexed if stop.get("required")),
        key=lambda stop: planning_datetime_text(stop.get("scheduled_at")),
    )
    optional = [stop for stop in indexed if not stop.get("required")]
    scheduled = []
    segments = [
        {
            "kind": "fixed_activity",
            "planned_activity_id": interval["activity"].get(
                "planned_activity_id", ""
            ),
            "contact_type": interval["activity"].get("contact_type", ""),
            "start": interval["start"].isoformat(timespec="minutes"),
            "end": interval["end"].isoformat(timespec="minutes"),
        }
        for interval in fixed_intervals
    ]
    cursor = route_start_at
    current_index = 0
    route_limit = route_start_at + timedelta(seconds=MAX_TOTAL_SECONDS)

    def drive_minutes(origin_index, destination_index):
        try:
            seconds = matrix[origin_index][destination_index]
        except (IndexError, TypeError):
            seconds = None
        if seconds is None:
            return None
        return max(0, int(math.ceil(float(seconds) / 60)))

    def simulate(stop, from_cursor, from_index):
        minutes = drive_minutes(from_index, stop["_matrix_index"])
        if minutes is None:
            return None
        departure = planning_next_unblocked_start(
            from_cursor, minutes, fixed_intervals
        )
        arrival = departure + timedelta(minutes=minutes)
        try:
            service_minutes = max(
                1, int(math.ceil(float(stop.get("duration_minutes") or 20)))
            )
        except (TypeError, ValueError, OverflowError):
            service_minutes = 20
        booked_at = (
            parse_planning_datetime(stop.get("scheduled_at"))
            if stop.get("required") else None
        )
        service_start = planning_next_unblocked_start(
            max(arrival, booked_at) if booked_at else arrival,
            service_minutes,
            fixed_intervals,
        )
        service_end = service_start + timedelta(minutes=service_minutes)
        return {
            "departure": departure,
            "arrival": arrival,
            "service_start": service_start,
            "service_end": service_end,
            "drive_minutes": minutes,
            "service_minutes": service_minutes,
            "booked_at": booked_at,
        }

    def required_tail_is_feasible(from_cursor, from_index, first_anchor_index):
        tail_cursor = from_cursor
        tail_index = from_index
        for tail_anchor in anchors[first_anchor_index:]:
            tail_booked = parse_planning_datetime(
                tail_anchor.get("scheduled_at")
            )
            timing = simulate(tail_anchor, tail_cursor, tail_index)
            if (
                tail_booked is None
                or timing is None
                or timing["service_start"]
                > tail_booked + timedelta(
                    minutes=PLANNING_ROUTE_CONFLICT_MINUTES
                )
            ):
                return False
            tail_cursor = timing["service_end"]
            tail_index = tail_anchor["_matrix_index"]
        return_minutes = drive_minutes(tail_index, 0)
        if return_minutes is None:
            return False
        return_departure = planning_next_unblocked_start(
            tail_cursor, return_minutes, fixed_intervals
        )
        return (
            return_departure + timedelta(minutes=return_minutes)
            < route_limit
        )

    def append_stop(stop, timing):
        nonlocal cursor, current_index
        public_stop = {
            key: value
            for key, value in stop.items()
            if key != "_matrix_index"
        }
        public_stop.update({
            "sequence": len(scheduled) + 1,
            "leg_drive_minutes": timing["drive_minutes"],
            "leg_departure_at": timing["departure"].isoformat(
                timespec="minutes"
            ),
            "arrival_at": timing["arrival"].isoformat(timespec="minutes"),
            "estimated_at": timing["service_start"].isoformat(
                timespec="minutes"
            ),
            "service_end_at": timing["service_end"].isoformat(
                timespec="minutes"
            ),
            "scheduled_at": (
                timing["booked_at"].isoformat(timespec="minutes")
                if timing["booked_at"]
                else timing["service_start"].isoformat(timespec="minutes")
            ),
            "time_is_estimated": not bool(stop.get("required")),
        })
        if timing["drive_minutes"]:
            segments.append({
                "kind": "drive",
                "customer_id": stop.get("customer_id", ""),
                "customer_row": stop.get("customer_row"),
                "start": timing["departure"].isoformat(timespec="minutes"),
                "end": timing["arrival"].isoformat(timespec="minutes"),
            })
        segments.append({
            "kind": "visit",
            "customer_id": stop.get("customer_id", ""),
            "customer_row": stop.get("customer_row"),
            "planned_activity_id": stop.get("planned_activity_id", ""),
            "required": bool(stop.get("required")),
            "start": timing["service_start"].isoformat(timespec="minutes"),
            "end": timing["service_end"].isoformat(timespec="minutes"),
        })
        scheduled.append(public_stop)
        cursor = timing["service_end"]
        current_index = stop["_matrix_index"]

    remaining = list(optional)
    for anchor_index, anchor in enumerate(anchors):
        anchor_booked = parse_planning_datetime(anchor.get("scheduled_at"))
        if anchor_booked is None:
            return None, None, planning_error(
                "required_schedule_not_feasible",
                "Ett fast besök saknar giltig bokad tid.",
                422,
                planned_activity_id=anchor.get("planned_activity_id", ""),
            )
        anchors_still_to_schedule = len(anchors) - anchor_index
        while (
            remaining
            and len(scheduled) + anchors_still_to_schedule < MAX_ROUTE_STOPS
        ):
            best = None
            direct_to_anchor = drive_minutes(
                current_index, anchor["_matrix_index"]
            )
            if direct_to_anchor is None:
                break
            for candidate in remaining:
                optional_timing = simulate(candidate, cursor, current_index)
                if optional_timing is None:
                    continue
                anchor_timing = simulate(
                    anchor,
                    optional_timing["service_end"],
                    candidate["_matrix_index"],
                )
                if (
                    anchor_timing is None
                    or anchor_timing["service_start"]
                    > anchor_booked
                    + timedelta(minutes=PLANNING_ROUTE_CONFLICT_MINUTES)
                ):
                    continue
                if not required_tail_is_feasible(
                    anchor_timing["service_end"],
                    anchor["_matrix_index"],
                    anchor_index + 1,
                ):
                    continue
                marginal_minutes = max(1, (
                    optional_timing["drive_minutes"]
                    + optional_timing["service_minutes"]
                    + anchor_timing["drive_minutes"]
                    - direct_to_anchor
                ))
                value_rate = float(candidate.get("priority_score") or 0) / max(
                    1, marginal_minutes
                )
                score = (
                    value_rate,
                    float(candidate.get("priority_score") or 0),
                    -marginal_minutes,
                    -int(candidate.get("customer_row") or candidate.get("row") or 0),
                )
                if best is None or score > best[0]:
                    best = (score, candidate, optional_timing)
            if best is None:
                break
            _score, candidate, timing = best
            append_stop(candidate, timing)
            remaining.remove(candidate)

        anchor_timing = simulate(anchor, cursor, current_index)
        if (
            anchor_timing is None
            or anchor_timing["service_start"]
            > anchor_booked
            + timedelta(minutes=PLANNING_ROUTE_CONFLICT_MINUTES)
        ):
            estimated = (
                anchor_timing["service_start"].isoformat(timespec="minutes")
                if anchor_timing else ""
            )
            return None, None, planning_error(
                "required_schedule_not_feasible",
                "Ett fast besök kan inte nås från föregående aktivitet i tid.",
                422,
                planned_activity_id=anchor.get("planned_activity_id", ""),
                scheduled_at=anchor_booked.isoformat(timespec="minutes"),
                estimated_at=estimated,
            )
        append_stop(anchor, anchor_timing)

    while remaining and len(scheduled) < MAX_ROUTE_STOPS:
        best = None
        for candidate in remaining:
            timing = simulate(candidate, cursor, current_index)
            if timing is None:
                continue
            return_minutes = drive_minutes(candidate["_matrix_index"], 0)
            if return_minutes is None:
                continue
            return_departure = planning_next_unblocked_start(
                timing["service_end"], return_minutes, fixed_intervals
            )
            route_end = return_departure + timedelta(minutes=return_minutes)
            if route_end >= route_limit:
                continue
            if anchors:
                direct_return = drive_minutes(current_index, 0)
                if direct_return is None:
                    continue
                marginal_minutes = max(1, (
                    timing["drive_minutes"]
                    + timing["service_minutes"]
                    + return_minutes
                    - direct_return
                ))
                value_rate = float(candidate.get("priority_score") or 0) / max(
                    1, marginal_minutes
                )
                score = (
                    value_rate,
                    float(candidate.get("priority_score") or 0),
                    -marginal_minutes,
                    -int(candidate.get("customer_row") or candidate.get("row") or 0),
                )
            else:
                extra = (
                    timing["drive_minutes"]
                    + timing["service_minutes"]
                    + return_minutes
                )
                value_rate = float(candidate.get("priority_score") or 0) / max(1, extra)
                score = (value_rate, float(candidate.get("priority_score") or 0))
            if best is None or score > best[0]:
                best = (score, candidate, timing)
        if best is None:
            break
        _score, candidate, timing = best
        append_stop(candidate, timing)
        remaining.remove(candidate)

    return_minutes = drive_minutes(current_index, 0)
    if return_minutes is None:
        return None, None, planning_error(
            "required_schedule_not_feasible",
            "Returen till startpunkten saknar en körbar väg.",
            422,
        )
    return_departure = planning_next_unblocked_start(
        cursor, return_minutes, fixed_intervals
    )
    route_end = return_departure + timedelta(minutes=return_minutes)
    if route_end >= route_limit:
        return None, None, planning_error(
            "required_schedule_not_feasible",
            "Dagens fasta schema och retur ryms inte inom mindre än sju timmar.",
            422,
            scheduled_at=route_start_at.isoformat(timespec="minutes"),
            estimated_at=route_end.isoformat(timespec="minutes"),
        )
    if return_minutes:
        segments.append({
            "kind": "return_drive",
            "start": return_departure.isoformat(timespec="minutes"),
            "end": route_end.isoformat(timespec="minutes"),
        })
    segments.sort(key=lambda segment: (segment.get("start", ""), segment["kind"]))
    elapsed_minutes = int(math.ceil(
        (route_end - route_start_at).total_seconds() / 60
    ))
    return scheduled, {
        "route_end_at": route_end.isoformat(timespec="minutes"),
        "return_departure_at": return_departure.isoformat(timespec="minutes"),
        "return_drive_minutes": return_minutes,
        "elapsed_minutes": elapsed_minutes,
        "blocked_minutes": sum(
            int((item["end"] - item["start"]).total_seconds() / 60)
            for item in fixed_intervals
        ),
        "segments": segments,
        "matrix_request_count": matrix_result.request_count,
        "matrix_cache_hits": matrix_result.cache_hits,
    }, None


def planning_route_conflicts(stops, fixed_non_route):
    conflicts = []
    for stop in stops:
        estimated = parse_planning_datetime(stop.get("estimated_at"))
        scheduled = parse_planning_datetime(stop.get("scheduled_at"))
        if stop.get("required") and estimated and scheduled:
            delta_minutes = round((estimated - scheduled).total_seconds() / 60)
            if abs(delta_minutes) > PLANNING_ROUTE_CONFLICT_MINUTES:
                conflicts.append({
                    "code": "scheduled_time_conflict",
                    "planned_activity_id": stop.get("planned_activity_id", ""),
                    "customer_row": stop.get("customer_row"),
                    "customer": stop.get("customer", ""),
                    "scheduled_at": scheduled.isoformat(timespec="minutes"),
                    "estimated_at": estimated.isoformat(timespec="minutes"),
                    "delta_minutes": delta_minutes,
                    "message": (
                        f"Tidskonflikt för {stop.get('customer')}: beräknad "
                        f"ankomst {estimated.strftime('%H.%M')}, bokad tid "
                        f"{scheduled.strftime('%H.%M')}."
                    ),
                })

        if not estimated:
            continue
        route_end = estimated + timedelta(
            minutes=int(stop.get("duration_minutes") or 20)
        )
        for activity in fixed_non_route:
            fixed_start = parse_planning_datetime(activity.get("scheduled_at"))
            if not fixed_start:
                continue
            fixed_end = fixed_start + timedelta(
                minutes=int(activity.get("duration_minutes") or 10)
            )
            if estimated < fixed_end and fixed_start < route_end:
                conflicts.append({
                    "code": "activity_overlap",
                    "planned_activity_id": activity.get(
                        "planned_activity_id", ""
                    ),
                    "route_customer_row": stop.get("customer_row"),
                    "customer": activity.get("customer", ""),
                    "scheduled_at": fixed_start.isoformat(timespec="minutes"),
                    "estimated_at": estimated.isoformat(timespec="minutes"),
                    "message": (
                        f"Rutten överlappar {activity.get('contact_type_label', '').lower()} "
                        f"med {activity.get('customer')} kl. "
                        f"{fixed_start.strftime('%H.%M')}."
                    ),
                })
    return conflicts


def route_optimization_error_response(error):
    details = dict(getattr(error, "details", {}) or {})
    return planning_error(
        error.code,
        error.public_message,
        error.http_status,
        **details,
    )


def route_optimization_run_sheet(spreadsheet):
    return get_or_create_worksheet(
        spreadsheet,
        ROUTE_OPTIMIZATION_RUNS_SHEET,
        ROUTE_OPTIMIZATION_RUN_COLUMNS,
        rows=1000,
    )


def route_optimization_run_rows(spreadsheet):
    sheet = route_optimization_run_sheet(spreadsheet)
    headers, rows = worksheet_snapshot(
        sheet, expected_columns=ROUTE_OPTIMIZATION_RUN_COLUMNS
    )
    return sheet, headers, rows


def update_route_optimization_run(sheet, row_index, headers, updates, run_id):
    """Update one ledger row and verify it after an uncertain Sheets write."""
    try:
        update_sheet_row(sheet, row_index, headers, updates)
        return
    except Exception:
        invalidate_sheet_for_write(sheet)
        _headers, rows = worksheet_snapshot(
            sheet, expected_columns=ROUTE_OPTIMIZATION_RUN_COLUMNS
        )
        current = next((row for _index, row in rows if row.get("run_id") == run_id), None)
        if current and all(str(current.get(key) or "") == str(value or "") for key, value in updates.items()):
            return
        raise


def append_route_optimization_run(sheet, run):
    try:
        return append_dict_row(sheet, ROUTE_OPTIMIZATION_RUN_COLUMNS, run)
    except Exception:
        invalidate_sheet_for_write(sheet)
        _headers, rows = worksheet_snapshot(
            sheet, expected_columns=ROUTE_OPTIMIZATION_RUN_COLUMNS
        )
        existing = next((
            row_index for row_index, row in rows
            if str(row.get("run_id") or "") == str(run.get("run_id") or "")
        ), None)
        if existing is not None:
            return existing
        raise


def route_optimization_usage_week(value=None):
    current = value or stockholm_today()
    year, week, _weekday = current.isocalendar()
    return f"{year}-W{week:02d}"


def route_optimization_reset_at(value=None):
    current = value or stockholm_now()
    start = current.date() + timedelta(days=(7 - current.weekday()))
    return datetime.combine(start, datetime_time.min, tzinfo=STOCKHOLM_ZONE).isoformat()


def _route_activity_duration_seconds(row, default_minutes):
    try:
        minutes = max(1, int(math.ceil(float(row.get("duration_minutes") or default_minutes))))
    except (TypeError, ValueError, OverflowError):
        minutes = default_minutes
    return minutes * 60


def build_route_optimization_inputs(
    *, spreadsheet, owner, route_date, start, route_start_at_override=None
):
    """Build one coherent, full-owner automatic optimization universe."""
    route_start_at = route_start_at_override or route_start_datetime(route_date)
    _sheet, _headers, indexed_rows = get_planned_activity_snapshot(spreadsheet)
    planned_rows = [row for _index, row in indexed_rows]
    snapshot = get_authoritative_priority_snapshot(
        spreadsheet,
        today=route_date,
        planned_activity_rows=planned_rows,
    )
    customers = list(snapshot.get("customers") or [])
    active_customers = [customer for customer in customers if not customer_is_cancelled(customer)]
    quality = route_coordinate_quality(active_customers)
    owner_customers = [
        customer for customer in active_customers
        if customer_owned_by_user(customer, owner)
        and str(customer.get("customer_id") or "").strip()
    ]
    active_by_id = defaultdict(list)
    for customer in active_customers:
        customer_id = str(customer.get("customer_id") or "").strip()
        if customer_id:
            active_by_id[customer_id].append(customer)
    owner_customer_ids = {
        str(customer.get("customer_id") or "").strip()
        for customer in owner_customers
    }
    global_conflicts = sorted(
        customer_id for customer_id in owner_customer_ids
        if len(active_by_id.get(customer_id, ())) != 1
    )
    if global_conflicts:
        return None, planning_error(
            "customer_identity_conflict",
            "En eller flera butiker har en tvetydig customer_id.",
            409,
            customer_ids=global_conflicts,
        )
    by_id_lists = defaultdict(list)
    for customer in owner_customers:
        by_id_lists[str(customer.get("customer_id") or "").strip()].append(customer)
    duplicate_ids = sorted(customer_id for customer_id, matches in by_id_lists.items() if len(matches) != 1)
    if duplicate_ids:
        return None, planning_error(
            "customer_identity_conflict",
            "En eller flera butiker har en tvetydig customer_id.",
            409,
            customer_ids=duplicate_ids,
        )
    customers_by_id = {customer_id: matches[0] for customer_id, matches in by_id_lists.items()}
    priorities_by_id = {
        str(item.get("customer_id") or "").strip(): item
        for item in (snapshot.get("priorities") or [])
        if str(item.get("customer_id") or "").strip()
    }
    date_rows = planning_rows_for_date(indexed_rows, owner, route_date)
    active_date_rows = [
        row for _row_index, row in date_rows
        if str(row.get("status") or "planned").strip().casefold() == "planned"
    ]

    mandatory_by_customer = defaultdict(list)
    fixed_break_rows = []
    for row in active_date_rows:
        source = str(row.get("source") or "").strip().casefold()
        if source == "route":
            continue
        contact_type = normalize_planning_contact_type(row.get("contact_type"))
        if contact_type == "visit":
            customer_id = str(row.get("customer_id") or "").strip()
            if not customer_id or customer_id not in customers_by_id:
                return None, planning_error(
                    "route_required_customer_unavailable",
                    "Ett obligatoriskt besök saknar en aktuell kundkoppling.",
                    409,
                    planned_activity_ids=[str(row.get("planned_activity_id") or "").strip()],
                )
            mandatory_by_customer[customer_id].append(row)
        elif contact_type in {"phone", "email"}:
            fixed_break_rows.append(row)
    duplicates = {
        customer_id: [str(row.get("planned_activity_id") or "").strip() for row in rows]
        for customer_id, rows in mandatory_by_customer.items()
        if len(rows) > 1
    }
    if duplicates:
        return None, planning_error(
            "route_duplicate_required_customer",
            "Samma butik har flera obligatoriska besök samma dag.",
            422,
            duplicate_customers=duplicates,
        )
    if len(mandatory_by_customer) > ROUTE_OPTIMIZATION_MAX_VISITS:
        return None, planning_error(
            "route_too_many_required_visits",
            "Dagen innehåller fler än 15 obligatoriska butiksbesök.",
            422,
        )

    global_end = route_start_at + timedelta(seconds=ROUTE_OPTIMIZATION_MAX_SECONDS)
    fixed_intervals = []
    fixed_breaks = []
    fixed_activities_for_fingerprint = []
    pre_route_fixed_seconds = 0
    for row in fixed_break_rows:
        scheduled = parse_planning_datetime(row.get("scheduled_at"))
        duration_seconds = _route_activity_duration_seconds(row, 10)
        if scheduled is None:
            return None, planning_error(
                "route_fixed_activity_invalid",
                "En fast telefon- eller mejlaktivitet saknar giltig tid.",
                422,
                planned_activity_id=row.get("planned_activity_id", ""),
            )
        end = scheduled + timedelta(seconds=duration_seconds)
        if end <= route_start_at:
            pre_route_fixed_seconds += duration_seconds
        elif scheduled < route_start_at < end:
            return None, planning_error(
                "route_starts_during_fixed_activity",
                "Ruttens starttid ligger mitt i en fast aktivitet.",
                422,
                planned_activity_id=row.get("planned_activity_id", ""),
            )
        elif end > global_end:
            return None, planning_error(
                "route_fixed_activity_outside_window",
                "En fast aktivitet ligger utanför den tillåtna arbetsdagen.",
                422,
                planned_activity_id=row.get("planned_activity_id", ""),
            )
        else:
            fixed_breaks.append({
                "activity_id": str(row.get("planned_activity_id") or ""),
                "scheduled_at": scheduled,
                "duration_seconds": duration_seconds,
            })
        fixed_intervals.append((scheduled, end, row))
        fixed_activities_for_fingerprint.append({
            "activity_id": str(row.get("planned_activity_id") or ""),
            "revision": planning_revision(row),
            "contact_type": normalize_planning_contact_type(row.get("contact_type")),
            "scheduled_at": scheduled,
            "duration_seconds": duration_seconds,
            "status": str(row.get("status") or "planned").strip().casefold(),
        })

    shipments = []
    mandatory_ids = set(mandatory_by_customer)
    untrusted_required_ids = sorted(
        customer_id for customer_id in mandatory_ids
        if not quality.get(customer_id, {}).get("trusted")
    )
    if untrusted_required_ids:
        return None, planning_error(
            "route_required_coordinate_untrusted",
            "Ett eller flera obligatoriska besök saknar betrodda koordinater.",
            422,
            customer_ids=untrusted_required_ids,
            planned_activity_ids=sorted(
                str(row.get("planned_activity_id") or "").strip()
                for customer_id in untrusted_required_ids
                for row in mandatory_by_customer[customer_id]
                if str(row.get("planned_activity_id") or "").strip()
            ),
        )
    for customer_id, rows in mandatory_by_customer.items():
        row = rows[0]
        scheduled = parse_planning_datetime(row.get("scheduled_at"))
        fixed_at = None if is_yes(row.get("time_is_estimated")) else scheduled
        if fixed_at is not None:
            if fixed_at < route_start_at or fixed_at + timedelta(seconds=ROUTE_OPTIMIZATION_SERVICE_SECONDS) > global_end:
                return None, planning_error(
                    "route_required_activity_outside_window",
                    "Ett fast besök ligger utanför den tillåtna arbetsdagen.",
                    422,
                    planned_activity_id=row.get("planned_activity_id", ""),
                )
            fixed_intervals.append((
                fixed_at,
                fixed_at + timedelta(seconds=ROUTE_OPTIMIZATION_SERVICE_SECONDS),
                row,
            ))
        coordinate_entry = quality.get(customer_id, {})
        priority = priorities_by_id.get(customer_id, {})
        shipments.append({
            "customer_id": customer_id,
            "priority_score": priority.get("priority_score") or 1,
            "coordinate": coordinate_entry["coordinate"],
            "required": True,
            "fixed_at": fixed_at,
            "activity_id": str(row.get("planned_activity_id") or ""),
            "revision": planning_revision(row),
        })

    fixed_intervals.sort(key=lambda item: item[0])
    for previous, current in zip(fixed_intervals, fixed_intervals[1:]):
        if current[0] < previous[1]:
            return None, planning_error(
                "route_fixed_activity_conflict",
                "Två fasta aktiviteter överlappar varandra.",
                422,
                planned_activity_ids=sorted(filter(None, [
                    str(previous[2].get("planned_activity_id") or "").strip(),
                    str(current[2].get("planned_activity_id") or "").strip(),
                ])),
            )

    blocked_customer_ids = set()
    for row in planned_rows:
        if str(row.get("status") or "planned").strip().casefold() != "planned":
            continue
        if str(row.get("source") or "").strip().casefold() == "route":
            continue
        scheduled = parse_planning_datetime(row.get("scheduled_at"))
        customer_id = str(row.get("customer_id") or "").strip()
        if not scheduled or not customer_id:
            continue
        if scheduled.date() > route_date or (
            scheduled.date() == route_date
            and normalize_planning_contact_type(row.get("contact_type")) in {"phone", "email"}
        ):
            blocked_customer_ids.add(customer_id)
    for contact in snapshot.get("contact_rows") or []:
        if parse_date_value(contact.get("date_time")) == route_date:
            customer_id = str(contact.get("customer_id") or "").strip()
            if customer_id:
                blocked_customer_ids.add(customer_id)

    excluded_untrusted = 0
    for customer_id, customer in customers_by_id.items():
        if customer_id in mandatory_ids or customer_id in blocked_customer_ids:
            continue
        priority = priorities_by_id.get(customer_id, {})
        try:
            score = int(round(float(priority.get("priority_score") or 0)))
        except (TypeError, ValueError, OverflowError):
            score = 0
        if not 1 <= score <= 100:
            continue
        coordinate_entry = quality.get(customer_id, {})
        if not coordinate_entry.get("trusted"):
            excluded_untrusted += 1
            continue
        shipments.append({
            "customer_id": customer_id,
            "priority_score": score,
            "coordinate": coordinate_entry["coordinate"],
            "required": False,
            "fixed_at": None,
            "activity_id": "",
            "revision": 0,
        })

    if not shipments:
        return None, planning_error(
            "route_no_eligible_customers",
            "Inga butiker är tillgängliga för automatisk ruttoptimering.",
            422,
        )

    shipments.sort(key=lambda item: (not item["required"], item["customer_id"]))
    timeout_seconds = route_optimization_int_setting(
        "ROUTE_OPTIMIZATION_FIXED_TIMEOUT_SECONDS" if fixed_intervals else "ROUTE_OPTIMIZATION_TIMEOUT_SECONDS",
        180 if fixed_intervals else 90,
        minimum=30,
        maximum=300,
    )
    trusted_start = TrustedCoordinate(round(start.latitude, 5), round(start.longitude, 5))
    fingerprint = build_route_optimization_fingerprint(
        owner_user_name=owner.get("user_name"),
        route_date=route_date.isoformat(),
        route_start=route_start_at,
        route_mode="automatic",
        start=trusted_start,
        shipments=shipments,
        fixed_activities=fixed_activities_for_fingerprint,
    )
    return {
        "route_start_at": route_start_at,
        "start": trusted_start,
        "shipments": shipments,
        "fixed_breaks": fixed_breaks,
        "fixed_activities": fixed_activities_for_fingerprint,
        "pre_route_fixed_seconds": pre_route_fixed_seconds,
        "timeout_seconds": timeout_seconds,
        "fingerprint": fingerprint,
        "customers_by_id": customers_by_id,
        "date_rows": date_rows,
        "excluded_untrusted_coordinates": excluded_untrusted,
        "required_count": len(mandatory_ids),
    }, None


def _parse_run_timestamp(value):
    try:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=STOCKHOLM_ZONE)
    return parsed.astimezone(STOCKHOLM_ZONE)


def route_optimization_recovery_status(
    spreadsheet, *, actor_user_name, client_request_id, now=None
):
    """Read one actor-scoped run status without mutating the audit ledger."""
    try:
        sheet = get_worksheet(spreadsheet, ROUTE_OPTIMIZATION_RUNS_SHEET)
    except WorksheetNotFound:
        return {"state": "not_found"}
    _headers, rows = worksheet_snapshot(
        sheet, expected_columns=ROUTE_OPTIMIZATION_RUN_COLUMNS
    )
    matching = [
        row for _row_index, row in rows
        if str(row.get("actor_user_name") or "").strip() == actor_user_name
        and str(row.get("client_request_id") or "").strip() == client_request_id
    ]
    if not matching:
        return {"state": "not_found"}

    row = matching[-1]
    status = str(row.get("status") or "").strip().casefold()
    error_code = str(row.get("error_code") or "").strip()
    if status == "completed":
        return {"state": "completed"}
    if status == "failed":
        result = {"state": "failed"}
        if error_code:
            result["error_code"] = error_code
        return result
    if status == "running":
        started_at = _parse_run_timestamp(row.get("started_at"))
        try:
            timeout_seconds = int(float(row.get("timeout_seconds") or 0))
        except (TypeError, ValueError, OverflowError):
            timeout_seconds = 0
        current = now or stockholm_now()
        if (
            started_at is not None
            and timeout_seconds > 0
            and (current - started_at).total_seconds() <= timeout_seconds + 60
        ):
            return {"state": "running"}
        return {
            "state": "indeterminate",
            "error_code": error_code or "route_optimization_stale_running",
        }

    result = {"state": "indeterminate"}
    if error_code:
        result["error_code"] = error_code
    return result


def execute_route_optimization(*, spreadsheet, owner, inputs, client_request_id):
    actor = current_user()
    actor_user_name = str(actor.get("user_name") or "").strip()
    owner_user_name = str(owner.get("user_name") or "").strip()
    fingerprint = inputs["fingerprint"]
    now = stockholm_now()
    usage_week = route_optimization_usage_week(now.date())
    cache_seconds = route_optimization_int_setting(
        "ROUTE_OPTIMIZATION_CACHE_SECONDS", 1800, minimum=60, maximum=86400
    )
    owner_limit = route_optimization_int_setting(
        "ROUTE_OPTIMIZATION_WEEKLY_OWNER_LIMIT", 2, minimum=1, maximum=100
    )
    team_limit = route_optimization_int_setting(
        "ROUTE_OPTIMIZATION_WEEKLY_TEAM_LIMIT", 6, minimum=1, maximum=500
    )
    quota_started = time.perf_counter()
    quota_recorded = False

    def finish_quota_instrumentation():
        nonlocal quota_recorded
        if not quota_recorded:
            record_performance_step(
                "route_optimization.quota_reservation",
                quota_started,
            )
            quota_recorded = True

    with _route_optimization_run_lock:
        sheet, headers, rows = route_optimization_run_rows(spreadsheet)
        same_request = next((
            (row_index, row) for row_index, row in rows
            if str(row.get("actor_user_name") or "").strip() == actor_user_name
            and str(row.get("client_request_id") or "").strip() == client_request_id
        ), None)
        if same_request:
            _row_index, row = same_request
            if str(row.get("request_fingerprint") or "") != fingerprint:
                finish_quota_instrumentation()
                return None, planning_error(
                    "route_request_id_conflict",
                    "Request-ID:t har redan använts för ett annat ruttunderlag.",
                    409,
                )
            status = str(row.get("status") or "").strip().casefold()
            if status == "completed":
                try:
                    finish_quota_instrumentation()
                    return {
                        "result": json.loads(row.get("result_payload_json") or "{}"),
                        "run_id": row.get("run_id"),
                        "cached": True,
                    }, None
                except (TypeError, ValueError):
                    finish_quota_instrumentation()
                    return None, planning_error(
                        "route_result_cache_invalid",
                        "Det sparade ruttresultatet är ogiltigt.",
                        503,
                    )
            if status == "running":
                started_at = _parse_run_timestamp(row.get("started_at"))
                if started_at and (now - started_at).total_seconds() <= inputs["timeout_seconds"] + 60:
                    finish_quota_instrumentation()
                    return None, planning_error(
                        "route_optimization_in_progress",
                        "Samma ruttoptimering pågår redan.",
                        409,
                    )
                update_route_optimization_run(
                    sheet,
                    same_request[0],
                    headers,
                    {
                        "status": "indeterminate",
                        "completed_at": planning_timestamp(),
                        "error_code": "route_optimization_stale_running",
                    },
                    str(row.get("run_id") or ""),
                )
                finish_quota_instrumentation()
                return None, planning_error(
                    "route_request_already_attempted",
                    "Det tidigare ruttförsöket har okänt utfall. Starta ett nytt försök.",
                    409,
                )
            finish_quota_instrumentation()
            return None, planning_error(
                "route_request_already_attempted",
                "Detta ruttförsök är redan avslutat. Starta ett nytt försök.",
                409,
            )

        for row_index, row in rows:
            if str(row.get("request_fingerprint") or "") != fingerprint:
                continue
            status = str(row.get("status") or "").strip().casefold()
            completed_at = _parse_run_timestamp(row.get("completed_at"))
            if status == "completed" and completed_at and (now - completed_at).total_seconds() <= cache_seconds:
                try:
                    finish_quota_instrumentation()
                    return {
                        "result": json.loads(row.get("result_payload_json") or "{}"),
                        "run_id": row.get("run_id"),
                        "cached": True,
                    }, None
                except (TypeError, ValueError):
                    continue
            if status == "running":
                started_at = _parse_run_timestamp(row.get("started_at"))
                if started_at and (now - started_at).total_seconds() <= inputs["timeout_seconds"] + 60:
                    finish_quota_instrumentation()
                    return None, planning_error(
                        "route_optimization_in_progress",
                        "Samma ruttoptimering pågår redan.",
                        409,
                    )
                update_route_optimization_run(
                    sheet,
                    row_index,
                    headers,
                    {
                        "status": "indeterminate",
                        "completed_at": planning_timestamp(),
                        "error_code": "route_optimization_stale_running",
                    },
                    str(row.get("run_id") or ""),
                )

        counted = [
            row for _row_index, row in rows
            if str(row.get("usage_iso_week") or "") == usage_week
            and is_yes(row.get("counted_attempt"))
        ]
        owner_count = sum(
            1 for row in counted
            if normalize_key(row.get("user_name")) == normalize_key(owner_user_name)
        )
        if owner_count >= owner_limit or len(counted) >= team_limit:
            finish_quota_instrumentation()
            return None, planning_error(
                "route_optimization_quota_exceeded",
                "Veckans kvot för automatisk ruttoptimering är slut.",
                429,
                reset_at=route_optimization_reset_at(now),
            )

        run_id = str(uuid.uuid4())
        request_label = f"store-tracker:{run_id}"
        run = {
            "run_id": run_id,
            "actor_user_name": actor_user_name,
            "user_name": owner_user_name,
            "usage_iso_week": usage_week,
            "route_date": inputs["route_start_at"].date().isoformat(),
            "client_request_id": client_request_id,
            "request_fingerprint": fingerprint,
            "engine_version": ROUTE_ENGINE_VERSION,
            "status": "running",
            "counted_attempt": "Y",
            "started_at": planning_timestamp(),
            "completed_at": "",
            "timeout_seconds": inputs["timeout_seconds"],
            "shipment_count": len(inputs["shipments"]),
            "required_count": inputs["required_count"],
            "performed_count": "",
            "skipped_count": "",
            "excluded_untrusted_coordinates": inputs["excluded_untrusted_coordinates"],
            "google_request_label": request_label,
            "http_status": "",
            "error_code": "",
            "result_payload_json": "",
        }
        row_index = append_route_optimization_run(sheet, run)
        finish_quota_instrumentation()

    body = build_optimize_tours_request(
        run_id=run_id,
        owner_user_name=owner_user_name,
        route_start=inputs["route_start_at"],
        start=inputs["start"],
        shipments=inputs["shipments"],
        fixed_breaks=inputs["fixed_breaks"],
        pre_route_fixed_seconds=inputs["pre_route_fixed_seconds"],
        timeout_seconds=inputs["timeout_seconds"],
    )
    project = str(os.environ.get("ROUTE_OPTIMIZATION_PROJECT") or "").strip()
    try:
        solve_started = time.perf_counter()
        try:
            response, http_status = route_optimization_provider().optimize(
                project=project,
                body=body,
                timeout_seconds=inputs["timeout_seconds"],
            )
        finally:
            google_solve_duration_ms = round(
                (time.perf_counter() - solve_started) * 1000,
                1,
            )
            record_performance_step(
                "route_optimization.google_solve",
                solve_started,
            )
        validation_started = time.perf_counter()
        try:
            parsed = parse_optimize_tours_response(
                response,
                shipments=inputs["shipments"],
                owner_user_name=owner_user_name,
                route_start=inputs["route_start_at"],
                pre_route_fixed_seconds=inputs["pre_route_fixed_seconds"],
                fixed_breaks=inputs["fixed_breaks"],
            )
        finally:
            record_performance_step(
                "route_optimization.response_validation",
                validation_started,
            )
    except RouteOptimizationError as error:
        http_status_value = (
            error.provider_status
            if error.provider_status is not None
            else (http_status if "http_status" in locals() else "")
        )
        diagnostic_json = ""
        if http_status_value == 200 and "response" in locals() and isinstance(response, dict):
            routes = response.get("routes") if isinstance(response.get("routes"), list) else []
            route = routes[0] if routes else {}
            diagnostic_payload = {
                "error_code": error.code,
                "diagnostic_reason": error.details.get("diagnostic_reason") or error.code,
                "solve_duration_ms": google_solve_duration_ms if "google_solve_duration_ms" in locals() else None,
                "route_count": len(response.get("routes") or []),
                "visit_count": len(route.get("visits") or []),
                "skipped_count": len(response.get("skippedShipments") or []),
                "break_count": len(route.get("breaks") or []),
                "hasTrafficInfeasibilities": bool(route.get("hasTrafficInfeasibilities")),
                "vehicle_label_matches": route.get("vehicleLabel") == f"owner:{str(owner_user_name).strip().casefold()}",
            }
            diagnostic_json = json.dumps(
                diagnostic_payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        with _route_optimization_run_lock:
            update_route_optimization_run(
                sheet,
                row_index,
                headers,
                {
                    "status": "failed",
                    "counted_attempt": "Y" if error.counted_attempt else "N",
                    "completed_at": planning_timestamp(),
                    "http_status": http_status_value,
                    "error_code": error.code,
                    "result_payload_json": diagnostic_json,
                },
                run_id,
            )
        return None, route_optimization_error_response(error)

    solve_duration_ms = google_solve_duration_ms
    compact_result = {
        "stops": parsed["stops"],
        "summary": parsed["summary"],
        "performed_count": parsed["performed_count"],
        "skipped_count": parsed["skipped_count"],
        "solve_duration_ms": solve_duration_ms,
    }
    compact_json = json.dumps(compact_result, ensure_ascii=False, separators=(",", ":"))
    with _route_optimization_run_lock:
        update_route_optimization_run(
            sheet,
            row_index,
            headers,
            {
                "status": "completed",
                "completed_at": planning_timestamp(),
                "http_status": http_status,
                "performed_count": parsed["performed_count"],
                "skipped_count": parsed["skipped_count"],
                "result_payload_json": compact_json,
            },
            run_id,
        )
    return {"result": compact_result, "run_id": run_id, "cached": False}, None


def build_route_optimization_preview(
    *, spreadsheet, owner, route_date, start, client_request_id
):
    with performance_step("route_optimization.input_build") as measurement:
        inputs, input_error = build_route_optimization_inputs(
            spreadsheet=spreadsheet,
            owner=owner,
            route_date=route_date,
            start=start,
        )
        if inputs:
            measurement["row_count"] = len(inputs.get("shipments") or ())
    if input_error is not None:
        return None, input_error
    solved, solve_error = execute_route_optimization(
        spreadsheet=spreadsheet,
        owner=owner,
        inputs=inputs,
        client_request_id=client_request_id,
    )
    if solve_error is not None:
        return None, solve_error
    compact = solved["result"]
    stops = []
    for stop in compact.get("stops") or []:
        customer_id = str(stop.get("customer_id") or "").strip()
        customer = inputs["customers_by_id"].get(customer_id)
        if not customer:
            return None, planning_error(
                "planning_changed",
                "Kundunderlaget ändrades under optimeringen. Beräkna rutten igen.",
                409,
            )
        stops.append({
            **stop,
            "customer_row": customer.get("row"),
            "row": customer.get("row"),
            "customer_number": str(customer.get("customer_number") or "").strip(),
            "customer": str(customer.get("customer") or "").strip(),
            "address": " ".join(filter(None, [
                str(customer.get("address_google") or "").strip(),
                str(customer.get("address_number_google") or "").strip(),
            ])),
            "city": str(customer.get("city_google") or "").strip(),
            "latitude": inputs["shipments"][next(
                index for index, shipment in enumerate(inputs["shipments"])
                if shipment["customer_id"] == customer_id
            )]["coordinate"].latitude,
            "longitude": inputs["shipments"][next(
                index for index, shipment in enumerate(inputs["shipments"])
                if shipment["customer_id"] == customer_id
            )]["coordinate"].longitude,
            "contact_type": "visit",
            "contact_type_label": PLANNING_CONTACT_TYPE_LABELS["visit"],
            "time_is_estimated": not bool(stop.get("required")),
        })
    summary = dict(compact.get("summary") or {})
    pre_route_minutes = round(inputs["pre_route_fixed_seconds"] / 60, 1)
    summary.update({
        "stop_count": len(stops),
        "total_minutes": round(float(summary.get("route_minutes") or 0) + pre_route_minutes, 1),
        "non_route_minutes": pre_route_minutes,
        "conflict_count": 0,
    })
    route_payload = {
        "ok": True,
        "engine": "route_optimization",
        "engine_version": ROUTE_ENGINE_VERSION,
        "stops": stops,
        "summary": summary,
        "meta": {
            "engine": "route_optimization",
            "engine_version": ROUTE_ENGINE_VERSION,
            "run_id": solved["run_id"],
            "candidate_scope": "all_eligible_owner_customers",
            "shipment_count": len(inputs["shipments"]),
            "performed_count": compact.get("performed_count", len(stops)),
            "skipped_count": compact.get("skipped_count", 0),
            "excluded_untrusted_coordinates": inputs["excluded_untrusted_coordinates"],
            "quota_cached": bool(solved["cached"]),
            "timeout_seconds": inputs["timeout_seconds"],
            "solve_duration_ms": compact.get("solve_duration_ms", 0),
        },
    }
    generated_at = planning_timestamp()
    preview = {
        "ok": True,
        "owner": owner,
        "route_date": route_date.isoformat(),
        "route_start_at": inputs["route_start_at"].isoformat(timespec="minutes"),
        "generated_at": generated_at,
        "expires_at": (stockholm_now() + timedelta(seconds=PLANNING_PREVIEW_MAX_AGE_SECONDS)).isoformat(timespec="seconds"),
        "start": {"latitude": start.latitude, "longitude": start.longitude},
        "stops": stops,
        "summary": summary,
        "conflicts": [],
        "warnings": ([{
            "code": "route_untrusted_coordinates_excluded",
            "count": inputs["excluded_untrusted_coordinates"],
            "message": "Butiker med osäkra koordinater utelämnades.",
        }] if inputs["excluded_untrusted_coordinates"] else []),
        "timeline": {"route_end_at": summary.get("route_end_at")},
        "gps_notice": "Start och retur: din position nu",
        "plan_fingerprint": planning_state_fingerprint(inputs["date_rows"]),
        "route_optimization_fingerprint": inputs["fingerprint"],
        "route_optimization_run_id": solved["run_id"],
        "route_engine_version": ROUTE_ENGINE_VERSION,
        "route_mode": "automatic",
        "route_payload": route_payload,
    }
    token_payload = {key: value for key, value in preview.items() if key not in {"ok", "owner"}}
    token_payload["user_name"] = owner.get("user_name")
    preview["preview_token"] = planning_preview_serializer().dumps(token_payload)
    return preview, None


def build_planning_route_preview(
    *,
    spreadsheet,
    owner,
    route_date,
    start,
    candidate_rows,
):
    _sheet, _headers, all_rows = get_planned_activity_snapshot(spreadsheet)
    customers = get_customer_rows(spreadsheet)
    customer_lookup = CustomerLookup(customers)
    customer_by_row = {
        customer.get("row"): customer
        for customer in customers
        if isinstance(customer.get("row"), int)
    }
    date_rows = planning_rows_for_date(all_rows, owner, route_date)
    active_rows = [
        (row_index, row)
        for row_index, row in date_rows
        if str(row.get("status") or "").strip().casefold() == "planned"
    ]
    fixed_visit_rows = [
        (row_index, row)
        for row_index, row in active_rows
        if (
            normalize_planning_contact_type(row.get("contact_type")) == "visit"
            and str(row.get("source") or "").strip().casefold()
            in {"manual", "follow_up"}
            and not is_yes(row.get("time_is_estimated"))
        )
    ]
    changed_owner_activity_ids = sorted({
        str(row.get("planned_activity_id") or "").strip()
        for _row_index, row in fixed_visit_rows
        if not customer_owned_by_user(
            related_row_customer(
                row,
                customers,
                customer_lookup=customer_lookup,
            ),
            owner,
        )
        and str(row.get("planned_activity_id") or "").strip()
    })
    if changed_owner_activity_ids:
        return None, planning_error(
            "planning_customer_owner_changed",
            "Kundens ansvarig har ändrats. Flytta eller ta bort aktiviteten innan rutten skapas.",
            409,
            planned_activity_ids=changed_owner_activity_ids,
        )
    active_rows = [
        (row_index, row)
        for row_index, row in active_rows
        if customer_owned_by_user(
            related_row_customer(
                row,
                customers,
                customer_lookup=customer_lookup,
            ),
            owner,
        )
    ]
    fixed_visit_rows = [
        (row_index, row)
        for row_index, row in fixed_visit_rows
        if customer_owned_by_user(
            related_row_customer(
                row,
                customers,
                customer_lookup=customer_lookup,
            ),
            owner,
        )
    ]
    fixed_non_route = [
        public_planned_activity(row)
        for _row_index, row in active_rows
        if normalize_planning_contact_type(row.get("contact_type"))
        in {"phone", "email"}
        and str(row.get("source") or "").strip().casefold()
        in {"manual", "follow_up"}
    ]
    non_route_minutes = sum(
        int(item.get("duration_minutes") or 0) for item in fixed_non_route
    )
    max_total_seconds = MAX_TOTAL_SECONDS - (non_route_minutes * 60)
    if max_total_seconds <= SERVICE_SECONDS_PER_STOP:
        return None, planning_error(
            "day_capacity_exhausted",
            "Telefon- och mejlaktiviteterna lämnar inte plats för en körbar rutt.",
            422,
        )

    required_row_values = set()
    invalid_required_activity_ids = []
    for _row_index, row in fixed_visit_rows:
        try:
            customer_row = int(float(row.get("customer_row") or 0))
        except (TypeError, ValueError, OverflowError):
            customer_row = 0
        if customer_row < 2:
            invalid_required_activity_ids.append(
                str(row.get("planned_activity_id") or "").strip()
            )
        else:
            required_row_values.add(customer_row)
    if invalid_required_activity_ids:
        return None, planning_error(
            "required_stops_not_feasible",
            "Ett eller flera obligatoriska besök saknar en giltig butikskoppling.",
            422,
            planned_activity_ids=invalid_required_activity_ids,
        )
    required_rows = tuple(sorted(required_row_values))
    chronological_anchor_rows = tuple(
        int(float(row.get("customer_row") or 0))
        for _row_index, row in sorted(
            fixed_visit_rows,
            key=lambda indexed: planning_datetime_text(
                indexed[1].get("scheduled_at")
            ),
        )
    )
    required_activity_by_row = defaultdict(list)
    for _row_index, row in fixed_visit_rows:
        try:
            row_number = int(float(row.get("customer_row") or 0))
        except (TypeError, ValueError):
            row_number = 0
        if row_number:
            required_activity_by_row[row_number].append(
                public_planned_activity(row)
            )
    duplicate_required_rows = {
        row_number: [
            activity.get("planned_activity_id", "")
            for activity in activities
        ]
        for row_number, activities in required_activity_by_row.items()
        if len(activities) > 1
    }
    if duplicate_required_rows:
        return None, planning_error(
            "duplicate_required_customer",
            "Samma butik har flera obligatoriska besök samma dag. Flytta eller ställ in ett av besöken innan rutten fylls.",
            422,
            duplicate_customers=duplicate_required_rows,
        )

    explicitly_requested_rows = set(candidate_rows or ())
    owned_customer_rows = {
        customer.get("row")
        for customer in customers
        if (
            isinstance(customer.get("row"), int)
            and customer_owned_by_user(customer, owner)
        )
    }
    if not candidate_rows:
        candidate_rows = tuple(owned_customer_rows)
    else:
        candidate_rows = tuple(
            row for row in candidate_rows if row in owned_customer_rows
        )
    def activity_customer_identity(row):
        return (
            str(row.get("customer_id") or "").strip()
            or normalize_key(row.get("customer"))
        )

    blocked_candidate_identities = set()
    for _row_index, activity in all_rows:
        if str(activity.get("status") or "planned").strip().casefold() != "planned":
            continue
        scheduled = parse_planning_datetime(activity.get("scheduled_at"))
        if not scheduled:
            continue
        activity_type = normalize_planning_contact_type(
            activity.get("contact_type")
        )
        source = str(activity.get("source") or "").strip().casefold()
        if (
            scheduled.date() == route_date
            and source != "route"
            and activity_type in {"phone", "email"}
        ) or scheduled.date() > route_date:
            blocked_candidate_identities.add(
                activity_customer_identity(activity)
            )
        if (
            scheduled.date() == route_date
            and source != "route"
            and activity_type == "visit"
            and not planning_owner_matches(activity, owner)
        ):
            blocked_candidate_identities.add(
                activity_customer_identity(activity)
            )
    try:
        for contact in get_contact_rows(spreadsheet):
            if parse_date_value(contact.get("date_time")) == route_date:
                blocked_candidate_identities.add(
                    str(contact.get("customer_id") or "").strip()
                    or normalize_key(contact.get("customer"))
                )
    except Exception:
        app.logger.warning(
            "Could not apply completed-contact route exclusions",
            exc_info=True,
        )
    blocked_candidate_identities.discard("")

    candidate_warnings = []
    filtered_candidate_rows = []
    for row_number in candidate_rows:
        customer = customer_by_row.get(row_number, {})
        identity = (
            str(customer.get("customer_id") or "").strip()
            or normalize_key(customer.get("customer"))
        )
        blocked = identity in blocked_candidate_identities
        if blocked and row_number not in required_rows:
            if row_number in explicitly_requested_rows:
                candidate_warnings.append({
                    "code": "customer_already_has_planned_contact",
                    "customer_id": str(
                        customer.get("customer_id") or ""
                    ).strip(),
                    "customer_row": row_number,
                    "message": "Kunden har redan en planerad kontakt.",
                })
            else:
                continue
        filtered_candidate_rows.append(row_number)
    candidate_rows = tuple(sorted(
        set(filtered_candidate_rows) | set(required_rows)
    ))
    route_payload, route_error = calculate_route_proposal_for_user(
        spreadsheet=spreadsheet,
        start=start,
        client_requested_rows=candidate_rows,
        user=current_user(),
        owner=owner,
        route_date=route_date,
        required_rows=required_rows,
        anchor_rows=chronological_anchor_rows,
        max_total_seconds=max_total_seconds,
        respect_requested_rows=True,
    )
    if route_error is not None:
        return None, route_error

    route_start_at = route_start_datetime(route_date)
    stops = []
    for stop in route_payload.get("stops", []):
        row_number = int(stop.get("row") or 0)
        customer = customer_by_row.get(row_number) or {}
        customer_id = str(customer.get("customer_id") or "").strip()
        if not customer_id:
            return None, planning_error(
                "customer_identity_conflict",
                "En butik i ruttförslaget saknar customer_id. Uppdatera kundregistret och beräkna rutten igen.",
                409,
                customer_row=row_number,
            )
        required_activities = required_activity_by_row.get(row_number, [])
        required_activity = required_activities[0] if required_activities else None
        scheduled = (
            parse_planning_datetime(required_activity.get("scheduled_at"))
            if required_activity else None
        )
        stops.append({
            **stop,
            "customer_row": row_number,
            "customer_id": customer_id,
            "customer_number": str(
                customer.get("customer_number") or ""
            ).strip(),
            "customer": str(customer.get("customer") or "").strip(),
            "address": " ".join(filter(None, [
                str(
                    customer.get("address_google")
                    or customer.get("address")
                    or ""
                ).strip(),
                str(
                    customer.get("address_number_google")
                    or customer.get("address_number")
                    or ""
                ).strip(),
            ])),
            "city": str(
                customer.get("city_google")
                or customer.get("city")
                or ""
            ).strip(),
            "contact_type": "visit",
            "contact_type_label": PLANNING_CONTACT_TYPE_LABELS["visit"],
            "duration_minutes": SERVICE_SECONDS_PER_STOP // 60,
            "required": bool(required_activity),
            "planned_activity_id": (
                required_activity.get("planned_activity_id", "")
                if required_activity else ""
            ),
            "required_activity_ids": [
                item.get("planned_activity_id", "")
                for item in required_activities
            ],
            "scheduled_at": (
                scheduled.isoformat(timespec="minutes") if scheduled else ""
            ),
            "estimated_at": "",
            "time_is_estimated": not bool(required_activity),
        })

    route_summary = dict(route_payload.get("summary") or {})
    stops, route_timeline, timeline_error = schedule_planning_route_with_anchors(
        stops=stops,
        fixed_non_route=fixed_non_route,
        route_start_at=route_start_at,
        start=start,
    )
    if timeline_error is not None:
        return None, timeline_error
    conflicts = planning_route_conflicts(stops, fixed_non_route)
    active_total_minutes = float(
        route_timeline.get("elapsed_minutes") or 0
    )
    route_total_minutes = max(0, active_total_minutes - non_route_minutes)
    if float(active_total_minutes) >= (MAX_TOTAL_SECONDS / 60):
        return None, planning_error(
            "day_capacity_exhausted",
            "Rutten och dagens fasta aktiviteter måste rymmas inom mindre än sju timmar.",
            422,
            total_minutes=active_total_minutes,
        )
    route_summary.update({
        "stop_count": len(stops),
        "total_priority_score": sum(
            int(stop.get("priority_score") or 0) for stop in stops
        ),
        "drive_minutes": sum(
            max(0, int(stop.get("leg_drive_minutes") or 0))
            for stop in stops
        ),
        "return_drive_minutes": route_timeline.get(
            "return_drive_minutes", 0
        ),
        "service_minutes": sum(
            max(0, int(stop.get("duration_minutes") or 0))
            for stop in stops
        ),
        "route_minutes": route_total_minutes,
        "non_route_minutes": non_route_minutes,
        "blocked_minutes": route_timeline.get("blocked_minutes", 0),
        "total_minutes": active_total_minutes,
        "conflict_count": len(conflicts),
        "route_end_at": route_timeline.get("route_end_at"),
        "timeline_elapsed_minutes": route_timeline.get(
            "elapsed_minutes"
        ),
    })
    route_payload.setdefault("meta", {}).update({
        "anchor_aware": bool(required_rows),
        "anchor_count": len(required_rows),
        "anchor_candidate_limit": ANCHOR_ROUTE_CANDIDATE_LIMIT,
        "anchor_matrix_request_count": route_timeline.get(
            "matrix_request_count", 0
        ),
        "anchor_matrix_cache_hits": route_timeline.get(
            "matrix_cache_hits", 0
        ),
    })
    gps_notice = (
        "Start och retur: din position nu"
        if route_date == stockholm_today()
        else "Rutten beräknas från din position nu"
    )
    preview = {
        "ok": True,
        "owner": owner,
        "route_date": route_date.isoformat(),
        "route_start_at": route_start_at.isoformat(timespec="minutes"),
        "generated_at": planning_timestamp(),
        "expires_at": (
            stockholm_now()
            + timedelta(seconds=PLANNING_PREVIEW_MAX_AGE_SECONDS)
        ).isoformat(timespec="seconds"),
        "start": {
            "latitude": start.latitude,
            "longitude": start.longitude,
        },
        "stops": stops,
        "summary": route_summary,
        "conflicts": conflicts,
        "warnings": candidate_warnings,
        "timeline": route_timeline,
        "gps_notice": gps_notice,
        "plan_fingerprint": planning_state_fingerprint(date_rows),
        "route_payload": {
            **route_payload,
            "stops": stops,
            "summary": route_summary,
            "route_start_at": route_start_at.isoformat(timespec="minutes"),
            "gps_notice": gps_notice,
            "conflicts": conflicts,
            "timeline": route_timeline,
        },
    }
    token_payload = {
        key: value
        for key, value in preview.items()
        if key not in {"ok", "owner"}
    }
    token_payload["user_name"] = owner.get("user_name")
    preview["preview_token"] = planning_preview_serializer().dumps(
        token_payload
    )
    return preview, None


def saved_route_has_group(spreadsheet, route_group_id):
    sheet = get_or_create_worksheet(
        spreadsheet,
        ROUTE_PROPOSALS_SHEET,
        ROUTE_PROPOSAL_COLUMNS,
        rows=500,
    )
    for row in worksheet_to_dicts(
        sheet,
        expected_columns=ROUTE_PROPOSAL_COLUMNS,
    ):
        try:
            payload = json.loads(str(row.get("payload_json") or ""))
        except (TypeError, ValueError):
            continue
        if str(payload.get("route_group_id") or "").strip() == route_group_id:
            return True
    return False


def saved_route_request_conflicts(
    spreadsheet,
    route_request_key,
    preview_fingerprint,
):
    sheet = get_or_create_worksheet(
        spreadsheet,
        ROUTE_PROPOSALS_SHEET,
        ROUTE_PROPOSAL_COLUMNS,
        rows=500,
    )
    for row in worksheet_to_dicts(
        sheet,
        expected_columns=ROUTE_PROPOSAL_COLUMNS,
    ):
        try:
            payload = json.loads(str(row.get("payload_json") or ""))
        except (TypeError, ValueError):
            continue
        if (
            str(payload.get("route_request_key") or "").strip()
            == route_request_key
            and str(
                payload.get("route_preview_fingerprint") or ""
            ).strip() != preview_fingerprint
        ):
            return True
    return False


def apply_planning_route(
    *,
    spreadsheet,
    owner,
    preview,
    client_request_id,
):
    route_date = parse_planning_date(preview.get("route_date"))
    if route_date is None:
        return None, planning_error(
            "invalid_route_preview",
            "Ruttförhandsgranskningen saknar giltigt datum.",
            400,
        )
    route_request_key = stable_planning_uuid(
        "route-request",
        current_user().get("user_name"),
        owner.get("user_name"),
        route_date.isoformat(),
        client_request_id,
    )
    preview_fingerprint = planning_route_apply_fingerprint(preview)
    route_group_id = f"{route_request_key}:{preview_fingerprint}"
    preview_stops = [
        stop
        for stop in (preview.get("stops") or [])
        if isinstance(stop, dict)
    ]
    signed_customer_ids = [
        str(stop.get("customer_id") or "").strip()
        for stop in preview_stops
    ]
    if not preview_stops or any(not value for value in signed_customer_ids):
        return None, planning_error(
            "route_preview_expired_or_legacy",
            "Ruttförhandsgranskningen använder äldre kundidentitet. Beräkna rutten igen.",
            409,
        )
    current_customers = get_customer_rows(spreadsheet)
    customers_by_id = defaultdict(list)
    for customer in current_customers:
        customer_id = str(customer.get("customer_id") or "").strip()
        if customer_id:
            customers_by_id[customer_id].append(customer)
    resolved_customers_by_id = {}
    for customer_id in set(signed_customer_ids):
        matches = customers_by_id.get(customer_id, [])
        if len(matches) != 1:
            return None, planning_error(
                "customer_identity_conflict",
                "En butik i rutten kunde inte bindas entydigt via customer_id. Beräkna rutten igen.",
                409,
                customer_id=customer_id,
            )
        if not customer_owned_by_user(matches[0], owner):
            changed_activity_ids = sorted({
                str(activity_id or "").strip()
                for stop in preview_stops
                if str(stop.get("customer_id") or "").strip() == customer_id
                for activity_id in (
                    stop.get("required_activity_ids")
                    or [stop.get("planned_activity_id")]
                )
                if str(activity_id or "").strip()
            })
            return None, planning_error(
                "route_customer_owner_changed",
                "En kund i rutten har bytt ansvarig. Beräkna rutten igen.",
                409,
                planned_activity_ids=changed_activity_ids,
            )
        resolved_customers_by_id[customer_id] = matches[0]

    with _planning_write_lock:
        sheet, headers, all_rows = get_planned_activity_snapshot(spreadsheet)
        date_rows = planning_rows_for_date(all_rows, owner, route_date)
        conflicting_request_rows = [
            row
            for _row_index, row in all_rows
            if (
                planning_owner_matches(row, owner)
                and str(row.get("route_group_id") or "").strip().startswith(
                    f"{route_request_key}:"
                )
                and str(row.get("route_group_id") or "").strip()
                != route_group_id
            )
        ]
        if conflicting_request_rows or saved_route_request_conflicts(
            spreadsheet,
            route_request_key,
            preview_fingerprint,
        ):
            return None, planning_error(
                "client_request_id_conflict",
                "Request-ID:t har redan använts för ett annat ruttförslag.",
                409,
                field="client_request_id",
            )
        existing_group_rows = [
            (row_index, row)
            for row_index, row in date_rows
            if str(row.get("route_group_id") or "").strip() == route_group_id
        ]
        if (
            not existing_group_rows
            and str(preview.get("plan_fingerprint") or "").strip()
            != planning_state_fingerprint(date_rows)
        ):
            return None, planning_error(
                "planning_changed",
                "Planeringen ändrades efter förhandsgranskningen. Beräkna rutten igen.",
                409,
            )

        existing_by_id = {
            str(row.get("planned_activity_id") or "").strip(): (row_index, row)
            for row_index, row in all_rows
            if str(row.get("planned_activity_id") or "").strip()
        }
        applied_rows = []
        new_rows = []
        row_changes = []
        for stop in preview_stops:
            if not isinstance(stop, dict):
                continue
            signed_customer_id = str(
                stop.get("customer_id") or ""
            ).strip()
            if not signed_customer_id:
                return None, planning_error(
                    "route_preview_expired_or_legacy",
                    "Ruttförhandsgranskningen använder äldre kundidentitet. Beräkna rutten igen.",
                    409,
                )
            try:
                customer_row = int(stop.get("customer_row") or stop.get("row"))
                sequence = int(stop.get("sequence"))
            except (TypeError, ValueError):
                return None, planning_error(
                    "invalid_route_preview",
                    "Ruttförhandsgranskningen innehåller ett ogiltigt stopp.",
                    400,
                )
            if not 1 <= sequence <= MAX_ROUTE_STOPS:
                return None, planning_error(
                    "invalid_route_preview",
                    "Ruttförhandsgranskningen innehåller en ogiltig stoppordning.",
                    400,
                )

            required_ids = [
                str(value or "").strip()
                for value in (stop.get("required_activity_ids") or [])
                if str(value or "").strip()
            ]
            if stop.get("planned_activity_id") and not required_ids:
                required_ids = [str(stop.get("planned_activity_id")).strip()]
            if stop.get("required"):
                if not required_ids:
                    return None, planning_error(
                        "required_activity_missing",
                        "Ett obligatoriskt besök saknas i planeringen.",
                        409,
                    )
                for activity_id in required_ids:
                    existing = existing_by_id.get(activity_id)
                    if not existing:
                        return None, planning_error(
                            "required_activity_missing",
                            "Ett obligatoriskt besök ändrades. Beräkna rutten igen.",
                            409,
                        )
                    row_index, row = existing
                    if (
                        not planning_owner_matches(row, owner)
                        or str(row.get("status") or "").strip().casefold()
                        != "planned"
                        or normalize_planning_contact_type(
                            row.get("contact_type")
                        ) != "visit"
                        or str(row.get("customer_id") or "").strip()
                        != signed_customer_id
                    ):
                        return None, planning_error(
                            "required_activity_changed",
                            "Ett obligatoriskt besök ändrades. Beräkna rutten igen.",
                            409,
                        )
                    updates = {}
                    if (
                        str(row.get("route_group_id") or "").strip()
                        != route_group_id
                    ):
                        updates["route_group_id"] = route_group_id
                    try:
                        current_sequence = int(
                            float(row.get("route_sequence") or 0)
                        )
                    except (TypeError, ValueError):
                        current_sequence = 0
                    if current_sequence != sequence:
                        updates["route_sequence"] = sequence
                    if updates:
                        updates["updated_at"] = planning_timestamp()
                        updates["revision"] = planning_revision(row) + 1
                        row_changes.append((row_index, updates))
                    applied_rows.append({**row, **updates})
                continue

            activity_id = stable_planning_uuid(
                "route-activity",
                route_group_id,
                sequence,
                stop.get("customer_id") or customer_row,
            )
            existing = existing_by_id.get(activity_id)
            if existing:
                applied_rows.append(existing[1])
                continue
            customer = resolved_customers_by_id[signed_customer_id]
            if not customer or customer_is_cancelled(customer):
                return None, planning_error(
                    "route_customer_unavailable",
                    "En butik i rutten är inte längre tillgänglig.",
                    409,
                    customer_row=customer_row,
                )
            activity = build_planned_activity_row(
                activity_id=activity_id,
                owner=owner,
                customer=customer,
                contact_type="visit",
                scheduled_at=stop.get("estimated_at") or stop.get("scheduled_at"),
                note="Automatiskt dagsförslag",
                source="route",
                route_group_id=route_group_id,
                route_sequence=sequence,
                client_request_id=client_request_id,
                time_is_estimated=True,
                create_fingerprint=planning_create_fingerprint(
                    actor=current_user(),
                    owner=owner,
                    customer_id=customer.get("customer_id"),
                    contact_type="visit",
                    scheduled_at=(
                        stop.get("estimated_at")
                        or stop.get("scheduled_at")
                    ),
                    duration_minutes=PLANNING_CONTACT_DURATIONS["visit"],
                    note="Automatiskt dagsförslag",
                    source="route",
                    source_contact_id="",
                ),
                revision=1,
            )
            new_rows.append(activity)
            applied_rows.append(activity)
        cancelled_count = 0
        for row_index, row in date_rows:
            if (
                str(row.get("source") or "").strip().casefold() != "route"
                or str(row.get("status") or "").strip().casefold() != "planned"
                or str(row.get("route_group_id") or "").strip() == route_group_id
            ):
                continue
            row_changes.append((row_index, {
                "status": "cancelled",
                "last_mutation_request_id": client_request_id,
                "last_mutation_fingerprint": preview_fingerprint,
                "revision": planning_revision(row) + 1,
                "updated_at": planning_timestamp(),
            }))
            cancelled_count += 1

        batch_update_sheet_changes(
            sheet,
            headers,
            row_changes,
            new_rows,
        )

        route_payload = dict(preview.get("route_payload") or {})
        route_payload.update({
            "ok": True,
            "route_group_id": route_group_id,
            "route_request_key": route_request_key,
            "route_preview_fingerprint": preview_fingerprint,
            "client_request_id": client_request_id,
            "route_date": route_date.isoformat(),
            "route_owner": user_route_display_name(owner),
            "generated_at": preview.get("generated_at") or planning_timestamp(),
            "stops": list(preview.get("stops") or []),
            "summary": dict(preview.get("summary") or {}),
            "conflicts": list(preview.get("conflicts") or []),
            "route_start_at": preview.get("route_start_at"),
            "gps_notice": preview.get("gps_notice"),
        })
        if not saved_route_has_group(spreadsheet, route_group_id):
            save_route_proposal(
                spreadsheet,
                user_name=owner.get("user_name"),
                user_display_name=user_route_display_name(owner),
                route_date=route_date,
                payload=route_payload,
            )

    unique_applied = {}
    for row in applied_rows:
        unique_applied[str(row.get("planned_activity_id") or "")] = row
    activities = sorted(
        (
            public_planned_activity(row)
            for row in unique_applied.values()
            if row
        ),
        key=lambda item: (
            item.get("route_sequence") or 999,
            item.get("scheduled_at") or "",
        ),
    )
    return {
        "ok": True,
        "duplicate": bool(existing_group_rows and not new_rows),
        "route_group_id": route_group_id,
        "route_date": route_date.isoformat(),
        "activities": activities,
        "activity_count": len(activities),
        "imported_count": len(new_rows),
        "cancelled_route_activity_count": cancelled_count,
    }, None


def route_payload_accessible(
    payload,
    customers,
    user,
    *,
    enforce_owner_scope=False,
    customer_lookup=None,
):
    stops = [
        stop for stop in (payload or {}).get("stops", [])
        if isinstance(stop, dict)
    ]
    lookup = customer_lookup or CustomerLookup(customers)
    return bool(stops) and all(
        (
            customer_owned_by_user(
                related_row_customer(
                    stop,
                    customers,
                    customer_lookup=lookup,
                ),
                user,
            )
            if enforce_owner_scope
            else customer_access_allowed(
                related_row_customer(
                    stop,
                    customers,
                    customer_lookup=lookup,
                ),
                user,
            )
        )
        for stop in stops
    )


# LEGACY/ROLLBACK COMPATIBILITY: no longer called by the active frontend.
@app.route("/route-proposal", methods=["GET", "POST"])
def create_route_proposal():
    user = current_user()
    route_date = stockholm_today()
    user_name = str(user.get("user_name") or "").strip()

    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        app.logger.exception("Could not open route proposal store")
        return route_proposal_error(
            "route_store_unavailable",
            "Dagens ruttförslag kunde inte laddas. Försök igen.",
            503,
        )

    if request.method == "GET":
        try:
            with _route_proposal_daily_lock:
                saved = get_saved_route_proposal(
                    spreadsheet,
                    user_name,
                    route_date,
                )
        except Exception:
            app.logger.exception("Could not load saved route proposal")
            return route_proposal_error(
                "route_store_unavailable",
                "Dagens ruttförslag kunde inte laddas. Försök igen.",
                503,
            )
        if saved and route_payload_accessible(
            saved, get_customer_rows(spreadsheet), user
        ):
            return jsonify(saved)
        return route_proposal_error(
            "no_daily_route",
            "Inget ruttförslag har beräknats för dig idag.",
            404,
        )

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return route_proposal_error(
            "invalid_request",
            "Begäran om ruttförslag är ogiltig.",
            400,
        )

    start = parse_route_start(data)
    if start is None:
        return route_proposal_error(
            "invalid_start",
            "Din position är ogiltig. Försök hämta positionen igen.",
            400,
        )

    candidate_rows = data.get("candidate_rows")
    if not isinstance(candidate_rows, list):
        return route_proposal_error(
            "invalid_candidate_rows",
            "Listan med butiker är ogiltig.",
            400,
        )
    # 2,376 direct elements plus 600 shortlist elements and 24 return
    # elements stay within the default 3,000 elements/minute Routes quota.
    if len(candidate_rows) > 2376:
        return route_proposal_error(
            "too_many_candidates",
            "För många butiker skickades. Begränsa listan med ett filter och försök igen.",
            400,
        )
    if any(
        isinstance(row, bool) or not isinstance(row, int) or row < 2
        for row in candidate_rows
    ):
        return route_proposal_error(
            "invalid_candidate_rows",
            "Listan med butiker är ogiltig.",
            400,
        )

    requested_rows = tuple(sorted(set(candidate_rows)))
    if not requested_rows:
        return route_proposal_error(
            "no_eligible_candidates",
            "Inga butiker matchar de aktiva filtren.",
            422,
        )

    try:
        with _route_proposal_daily_lock:
            saved = get_saved_route_proposal(
                spreadsheet,
                user_name,
                route_date,
            )
            if saved and route_payload_accessible(
                saved, get_customer_rows(spreadsheet), user
            ):
                return jsonify(saved)

            payload, error_response = calculate_route_proposal_for_user(
                spreadsheet=spreadsheet,
                start=start,
                client_requested_rows=requested_rows,
                user=user,
                route_date=route_date,
            )
            if error_response is not None:
                return error_response
            save_route_proposal(
                spreadsheet,
                user_name=user_name,
                user_display_name=user_route_display_name(user),
                route_date=route_date,
                payload=payload,
            )
            return jsonify(payload)
    except Exception:
        app.logger.exception("Could not persist today's route proposal")
        return route_proposal_error(
            "route_store_unavailable",
            "Dagens ruttförslag kunde inte sparas. Försök igen.",
            503,
        )

# LEGACY/ROLLBACK COMPATIBILITY: retained for older clients and rollback.
@app.route("/planning/route-import", methods=["POST"])
def planning_route_import():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error(
            "invalid_request",
            "Begäran om att importera rutten är ogiltig.",
            400,
        )
    client_request_id = normalize_client_request_id(
        data.get("client_request_id")
    )
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krävs.",
            400,
            field="client_request_id",
        )
    route_date = stockholm_today()
    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        return planning_error(
            "route_store_unavailable",
            "Dagens rutt kunde inte laddas. Försök igen.",
            503,
        )
    owner, owner_error = resolve_planning_owner(
        spreadsheet,
        data.get("user_name"),
    )
    if owner_error is not None:
        return owner_error
    try:
        saved = get_saved_route_proposal(
            spreadsheet,
            owner.get("user_name"),
            route_date,
        )
    except Exception:
        app.logger.exception("Could not read daily route for planning import")
        return planning_error(
            "route_store_unavailable",
            "Dagens rutt kunde inte laddas. Försök igen.",
            503,
        )
    if not saved:
        return planning_error(
            "no_daily_route",
            "Det finns inget sparat ruttförslag för idag.",
            404,
        )
    route_customers = get_customer_rows(spreadsheet)
    route_customer_lookup = CustomerLookup(route_customers)
    _sheet, _headers, all_rows = get_planned_activity_snapshot(spreadsheet)
    date_rows = planning_rows_for_date(all_rows, owner, route_date)
    active_date_rows = [
        (row_index, row)
        for row_index, row in date_rows
        if str(row.get("status") or "").strip().casefold() == "planned"
    ]
    changed_owner_activity_ids = sorted({
        str(row.get("planned_activity_id") or "").strip()
        for _row_index, row in active_date_rows
        if (
            normalize_planning_contact_type(row.get("contact_type")) == "visit"
            and str(row.get("source") or "").strip().casefold()
            in {"manual", "follow_up"}
            and not is_yes(row.get("time_is_estimated"))
            and not customer_owned_by_user(
                related_row_customer(
                    row,
                    route_customers,
                    customer_lookup=route_customer_lookup,
                ),
                owner,
            )
            and str(row.get("planned_activity_id") or "").strip()
        )
    })
    if changed_owner_activity_ids:
        return planning_error(
            "planning_customer_owner_changed",
            "Kundens ansvarig har ändrats. Flytta eller ta bort aktiviteten innan rutten importeras.",
            409,
            planned_activity_ids=changed_owner_activity_ids,
        )
    if not route_payload_accessible(
        saved,
        route_customers,
        owner,
        enforce_owner_scope=True,
        customer_lookup=route_customer_lookup,
    ):
        return planning_error(
            "route_customer_owner_changed",
            "En kund i den sparade rutten har bytt ansvarig. Beräkna rutten igen.",
            409,
        )
    scoped_date_rows = [
        (row_index, row)
        for row_index, row in date_rows
        if customer_owned_by_user(
            related_row_customer(
                row,
                route_customers,
                customer_lookup=route_customer_lookup,
            ),
            owner,
        )
    ]
    required_by_customer_row = defaultdict(list)
    invalid_required_activity_ids = []
    fixed_non_route = []
    for _row_index, row in scoped_date_rows:
        if str(row.get("status") or "").strip().casefold() != "planned":
            continue
        public_row = public_planned_activity(row)
        if (
            public_row.get("contact_type") == "visit"
            and public_row.get("source") in {"manual", "follow_up"}
            and not public_row.get("time_is_estimated")
        ):
            if public_row.get("customer_row"):
                required_by_customer_row[public_row["customer_row"]].append(
                    public_row
                )
            else:
                invalid_required_activity_ids.append(
                    public_row.get("planned_activity_id", "")
                )
        elif (
            public_row.get("contact_type") in {"phone", "email"}
            and public_row.get("source") in {"manual", "follow_up"}
        ):
            fixed_non_route.append(public_row)

    if invalid_required_activity_ids:
        return planning_error(
            "required_stops_not_feasible",
            "Ett eller flera obligatoriska besök saknar en giltig butikskoppling.",
            422,
            planned_activity_ids=invalid_required_activity_ids,
        )
    duplicate_required_rows = {
        row_number: [
            activity.get("planned_activity_id", "")
            for activity in activities
        ]
        for row_number, activities in required_by_customer_row.items()
        if len(activities) > 1
    }
    if duplicate_required_rows:
        return planning_error(
            "duplicate_required_customer",
            "Samma butik har flera obligatoriska besök samma dag. Flytta eller ställ in ett av besöken innan rutten importeras.",
            422,
            duplicate_customers=duplicate_required_rows,
        )

    saved_stop_customer_rows = set()
    for raw_stop in saved.get("stops") or []:
        if not isinstance(raw_stop, dict):
            continue
        try:
            saved_customer_row = int(
                raw_stop.get("customer_row") or raw_stop.get("row")
            )
        except (TypeError, ValueError, OverflowError):
            continue
        if saved_customer_row >= 2:
            saved_stop_customer_rows.add(saved_customer_row)
    missing_required_rows = sorted(
        set(required_by_customer_row) - saved_stop_customer_rows
    )
    if missing_required_rows:
        return planning_error(
            "required_stops_missing_from_daily_route",
            "Den sparade rutten saknar ett obligatoriskt besök. Använd Fyll dagen automatiskt för att beräkna en komplett plan.",
            422,
            customer_rows=missing_required_rows,
        )

    non_route_minutes = sum(
        int(item.get("duration_minutes") or 0)
        for item in fixed_non_route
    )
    try:
        saved_route_minutes = float(
            (saved.get("summary") or {}).get("total_minutes")
        )
    except (TypeError, ValueError, OverflowError):
        saved_route_minutes = -1
    if not math.isfinite(saved_route_minutes) or saved_route_minutes < 0:
        return planning_error(
            "invalid_daily_route",
            "Dagens sparade rutt saknar en giltig tidsberäkning.",
            422,
        )
    imported_total_minutes = saved_route_minutes + non_route_minutes
    if imported_total_minutes >= (MAX_TOTAL_SECONDS / 60):
        return planning_error(
            "day_capacity_exhausted",
            "Rutten och dagens telefon- och mejlaktiviteter ryms inte inom sju timmar.",
            422,
            route_minutes=saved_route_minutes,
            non_route_minutes=non_route_minutes,
            total_minutes=imported_total_minutes,
        )

    saved_start_at = parse_planning_datetime(
        saved.get("route_start_at")
    )
    if saved_start_at and saved_start_at.date() == route_date:
        start_at = saved_start_at
    else:
        saved_generated_at = parse_planning_instant(
            saved.get("generated_at")
        )
        start_at = route_start_datetime(
            route_date,
            now=(
                saved_generated_at
                if saved_generated_at
                and saved_generated_at.date() == route_date
                else None
            ),
        )
    stops = []
    for raw_stop in saved.get("stops") or []:
        try:
            customer_row = int(
                raw_stop.get("customer_row") or raw_stop.get("row")
            )
            sequence = int(raw_stop.get("sequence"))
        except (TypeError, ValueError):
            continue
        required_activities = required_by_customer_row.get(customer_row, [])
        scheduled = (
            parse_planning_datetime(required_activities[0].get("scheduled_at"))
            if required_activities else None
        )
        stops.append({
            **raw_stop,
            "row": customer_row,
            "customer_row": customer_row,
            "sequence": sequence,
            "contact_type": "visit",
            "contact_type_label": PLANNING_CONTACT_TYPE_LABELS["visit"],
            "duration_minutes": SERVICE_SECONDS_PER_STOP // 60,
            "required": bool(required_activities),
            "planned_activity_id": (
                required_activities[0].get("planned_activity_id", "")
                if required_activities else ""
            ),
            "required_activity_ids": [
                item.get("planned_activity_id", "")
                for item in required_activities
            ],
            "scheduled_at": (
                scheduled.isoformat(timespec="minutes") if scheduled else ""
            ),
            "estimated_at": "",
            "time_is_estimated": not bool(required_activities),
        })
    if not stops:
        return planning_error(
            "no_daily_route_stops",
            "Dagens rutt saknar stopp att spara i Planering.",
            422,
        )
    summary = dict(saved.get("summary") or {})
    stops, route_timeline, timeline_error = schedule_planning_route_timeline(
        stops=stops,
        fixed_non_route=fixed_non_route,
        route_start_at=start_at,
        return_drive_minutes=summary.get(
            "return_drive_minutes",
            0,
        ),
    )
    if timeline_error is not None:
        return timeline_error
    conflicts = planning_route_conflicts(stops, fixed_non_route)
    summary.update({
        "route_minutes": saved_route_minutes,
        "non_route_minutes": non_route_minutes,
        "total_minutes": max(
            imported_total_minutes,
            float(route_timeline.get("elapsed_minutes") or 0),
        ),
        "conflict_count": len(conflicts),
        "route_end_at": route_timeline.get("route_end_at"),
        "timeline_elapsed_minutes": route_timeline.get(
            "elapsed_minutes"
        ),
    })
    preview = {
        "route_date": route_date.isoformat(),
        "route_start_at": start_at.isoformat(timespec="minutes"),
        "generated_at": saved.get("generated_at") or planning_timestamp(),
        "start": dict(saved.get("start") or {}),
        "stops": stops,
        "summary": summary,
        "conflicts": conflicts,
        "timeline": route_timeline,
        "gps_notice": "Planen beräknades från din position vid skapandet",
        "plan_fingerprint": planning_state_fingerprint(date_rows),
        "route_payload": {
            **saved,
            "stops": stops,
            "conflicts": conflicts,
            "timeline": route_timeline,
            "route_start_at": start_at.isoformat(timespec="minutes"),
        },
    }
    try:
        result, import_error = apply_planning_route(
            spreadsheet=spreadsheet,
            owner=owner,
            preview=preview,
            client_request_id=client_request_id,
        )
    except Exception:
        app.logger.exception("Could not import daily route into planning")
        return planning_error(
            "route_import_failed",
            "Dagens rutt kunde inte sparas i Planering. Försök igen.",
            503,
        )
    if import_error is not None:
        return import_error
    result["imported"] = True
    return jsonify(result)


@app.route("/planning/route-apply", methods=["POST"])
def planning_route_apply():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error(
            "invalid_request",
            "Begäran om att spara rutten är ogiltig.",
            400,
        )
    client_request_id = normalize_client_request_id(
        data.get("client_request_id")
    )
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krävs.",
            400,
            field="client_request_id",
        )
    token = str(data.get("preview_token") or "").strip()
    if not token:
        return planning_error(
            "missing_preview_token",
            "Ruttförhandsgranskningen saknas.",
            400,
            field="preview_token",
        )
    try:
        preview = planning_preview_serializer().loads(
            token,
            max_age=PLANNING_PREVIEW_MAX_AGE_SECONDS,
        )
    except SignatureExpired:
        return planning_error(
            "route_preview_expired",
            "Ruttförhandsgranskningen har gått ut. Beräkna rutten igen.",
            409,
        )
    except BadSignature:
        return planning_error(
            "invalid_route_preview",
            "Ruttförhandsgranskningen är ogiltig.",
            400,
        )
    if not isinstance(preview, dict):
        return planning_error(
            "invalid_route_preview",
            "Ruttförhandsgranskningen är ogiltig.",
            400,
        )
    route_date = parse_planning_date(preview.get("route_date"))
    if route_date is None or route_date < stockholm_today():
        return planning_error(
            "route_date_in_past",
            "Tidigare dagar kan inte fyllas automatiskt.",
            409,
        )

    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        return planning_error(
            "route_store_unavailable",
            "Rutten kunde inte sparas. Försök igen.",
            503,
        )
    owner, owner_error = resolve_planning_owner(
        spreadsheet,
        preview.get("user_name"),
    )
    if owner_error is not None:
        return owner_error
    requested_owner = str(data.get("user_name") or "").strip()
    if (
        requested_owner
        and normalize_key(requested_owner)
        != normalize_key(owner.get("user_name"))
    ):
        return planning_error(
            "route_preview_owner_mismatch",
            "Ruttförhandsgranskningen tillhör en annan säljare.",
            409,
        )
    if str(preview.get("route_engine_version") or "") == ROUTE_ENGINE_VERSION:
        start_data = preview.get("start") or {}
        try:
            preview_start = Coordinate(
                latitude=float(start_data.get("latitude")),
                longitude=float(start_data.get("longitude")),
            )
        except (TypeError, ValueError):
            return planning_error(
                "invalid_route_preview",
                "Ruttförhandsgranskningen saknar giltig startposition.",
                400,
            )
        current_inputs, current_error = build_route_optimization_inputs(
            spreadsheet=spreadsheet,
            owner=owner,
            route_date=route_date,
            start=preview_start,
            route_start_at_override=parse_planning_datetime(
                preview.get("route_start_at")
            ),
        )
        if current_error is not None:
            return current_error
        if str(preview.get("route_optimization_fingerprint") or "") != current_inputs["fingerprint"]:
            return planning_error(
                "planning_changed",
                "Planeringen eller kundunderlaget ändrades efter förhandsgranskningen. Beräkna rutten igen.",
                409,
            )
    try:
        result, apply_error = apply_planning_route(
            spreadsheet=spreadsheet,
            owner=owner,
            preview=preview,
            client_request_id=client_request_id,
        )
    except Exception:
        app.logger.exception("Could not apply planning route")
        return planning_error(
            "route_apply_failed",
            "Rutten kunde inte sparas. Försök igen med samma request-ID.",
            503,
        )
    if apply_error is not None:
        return apply_error
    return jsonify(result)


@app.route("/planning/route-preview-status", methods=["GET"])
def planning_route_preview_status():
    client_request_id = normalize_client_request_id(
        request.args.get("client_request_id")
    )
    if not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krÃ¤vs fÃ¶r statuskontrollen.",
            400,
            field="client_request_id",
        )
    actor_user_name = str(current_user().get("user_name") or "").strip()
    try:
        spreadsheet = get_spreadsheet_with_retry()
        status = route_optimization_recovery_status(
            spreadsheet,
            actor_user_name=actor_user_name,
            client_request_id=client_request_id,
        )
    except Exception:
        app.logger.exception("Could not read route optimization recovery status")
        return planning_error(
            "route_store_unavailable",
            "RuttfÃ¶rslagets status kunde inte laddas. FÃ¶rsÃ¶k igen.",
            503,
        )
    return jsonify({"ok": True, **status})


@app.route("/planning/route-preview", methods=["POST"])
def planning_route_preview():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error(
            "invalid_request",
            "Begäran om dagsfyllning är ogiltig.",
            400,
        )
    route_date = parse_planning_date(data.get("route_date"))
    if route_date is None:
        return planning_error(
            "invalid_route_date",
            "Ange ett giltigt datum för rutten.",
            400,
            field="route_date",
        )
    if route_date < stockholm_today():
        return planning_error(
            "route_date_in_past",
            "Tidigare dagar kan inte fyllas automatiskt.",
            409,
            field="route_date",
        )
    start = parse_route_start(data)
    if start is None:
        return planning_error(
            "invalid_start",
            "Din position är ogiltig. Försök hämta positionen igen.",
            400,
            field="start",
        )
    candidate_rows = data.get("candidate_rows", [])
    if candidate_rows is None:
        candidate_rows = []
    if (
        not isinstance(candidate_rows, list)
        or len(candidate_rows) > 2376
        or any(
            isinstance(row, bool) or not isinstance(row, int) or row < 2
            for row in candidate_rows
        )
    ):
        return planning_error(
            "invalid_candidate_rows",
            "Listan med butiker är ogiltig.",
            400,
            field="candidate_rows",
        )
    engine = route_engine_name()
    if engine not in {"legacy", "route_optimization"}:
        return planning_error(
            "invalid_route_engine",
            "Ruttmotorn är felkonfigurerad.",
            503,
        )
    client_request_id = normalize_client_request_id(data.get("client_request_id"))
    if engine == "route_optimization" and not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krävs för ruttoptimeringen.",
            400,
            field="client_request_id",
        )
    route_mode = str(data.get("route_mode") or "automatic").strip().casefold()
    if engine == "route_optimization" and route_mode != "automatic":
        return planning_error(
            "invalid_route_mode",
            "Google Route Optimization stöder endast automatiskt ruttläge.",
            400,
            field="route_mode",
        )
    if engine == "route_optimization":
        configuration = route_optimization_configuration_health()
        if not configuration["safe"]:
            return planning_error(
                "route_optimization_not_configured",
                "Ruttoptimeringen är inte konfigurerad.",
                503,
            )

    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        app.logger.exception("Could not open planning route store")
        return planning_error(
            "route_store_unavailable",
            "Ruttförslaget kunde inte laddas. Försök igen.",
            503,
        )
    owner, owner_error = resolve_planning_owner(
        spreadsheet,
        data.get("user_name"),
    )
    if owner_error is not None:
        return owner_error
    try:
        if engine == "route_optimization":
            preview, preview_error = build_route_optimization_preview(
                spreadsheet=spreadsheet,
                owner=owner,
                route_date=route_date,
                start=start,
                client_request_id=client_request_id,
            )
        else:
            preview, preview_error = build_planning_route_preview(
                spreadsheet=spreadsheet,
                owner=owner,
                route_date=route_date,
                start=start,
                candidate_rows=tuple(sorted(set(candidate_rows))),
            )
    except Exception:
        app.logger.exception("Unexpected planning route preview failure")
        return planning_error(
            "route_preview_failed",
            "Ruttförslaget kunde inte beräknas. Försök igen.",
            500,
        )
    if preview_error is not None:
        return preview_error
    return jsonify(preview)


@app.route("/followup-insights", methods=["GET"])
def get_followup_insights():
    spreadsheet = get_spreadsheet_with_retry()
    today = stockholm_today()
    weeks = build_recent_weeks(today)
    week_keys = {w["key"] for w in weeks}
    current_week_key = weeks[-1]["key"]
    previous_week_key = weeks[-2]["key"] if len(weeks) > 1 else ""
    selected_responsible = request.args.get("responsible", "").strip()

    customers_by_name = {}
    customers = get_customer_rows(spreadsheet)
    for customer in customers:
        customers_by_name[normalize_key(customer["customer"])] = customer

    contact_rows = get_contact_rows(spreadsheet)
    order_rows = get_order_rows(spreadsheet)
    message_rows, recipient_rows, _ = get_email_rows(
        spreadsheet, include_events=False
    )
    calculation_started = time.perf_counter()
    dfp_top_weeks_2026 = build_dfp_top_weeks(order_rows, year=2026, limit=5)

    responsible_options = sorted({
        c["sales_person"] for c in customers_by_name.values() if c["sales_person"]
    })

    def customer_belongs_to_selected(customer_name):
        customer = customers_by_name.get(normalize_key(customer_name))
        if not customer or not customer["sales_person"]:
            return False
        if not selected_responsible:
            return True
        return customer["sales_person"] == selected_responsible

    def contact_belongs_to_selected(contact):
        return customer_belongs_to_selected(contact["customer"])

    # DFP leaderboard is intentionally global and ignores the selected responsible filter.
    # It sums Total weight for every order row by the customer's responsible salesperson.
    dfp_counts = {w["key"]: defaultdict(float) for w in weeks}
    dfp_team_totals = {w["key"]: 0.0 for w in weeks}
    for order in order_rows:
        order_date = parse_date_value(order["Order date"])
        if not order_date:
            continue
        key = week_key(order_date)
        if key not in week_keys:
            continue

        total_weight = parse_number_value(order["Total weight"], default=0.0)
        if total_weight <= 0:
            continue

        dfp_team_totals[key] += total_weight

        customer = customers_by_name.get(normalize_key(order["Customer"]))
        if not customer or not customer["sales_person"]:
            continue
        responsible = customer["sales_person"]
        dfp_counts[key][responsible] += total_weight

    dfp_leaderboard = []
    for w in weeks:
        leaders = sorted(dfp_counts[w["key"]].items(), key=lambda item: (-item[1], item[0]))[:3]
        dfp_leaderboard.append({
            "week_key": w["key"],
            "label": w["label"],
            "team_total_dfp": format_dfp_count(dfp_team_totals[w["key"]]),
            "leaders": [
                {
                    "rank": idx + 1,
                    "sales_person": name,
                    "dfp_count": format_dfp_count(count),
                }
                for idx, (name, count) in enumerate(leaders)
            ],
        })

    contact_count_by_week = {w["key"]: 0 for w in weeks}
    positive_count_by_week = {w["key"]: 0 for w in weeks}
    contact_dates_by_customer = defaultdict(list)

    for contact in contact_rows:
        contact_date = parse_date_value(contact["date_time"])
        if not contact_date:
            continue

        customer_key = normalize_key(contact["customer"])
        if contact_belongs_to_selected(contact):
            contact_dates_by_customer[customer_key].append(contact_date)
            key = week_key(contact_date)
            if key in week_keys:
                contact_count_by_week[key] += 1
                if is_positive_contact(contact["result"]):
                    positive_count_by_week[key] += 1

    for dates in contact_dates_by_customer.values():
        dates.sort()

    current_contacts = contact_count_by_week.get(current_week_key, 0)
    previous_contacts = contact_count_by_week.get(previous_week_key, 0)
    if previous_contacts == 0:
        contact_delta_percent = 100 if current_contacts > 0 else 0
    else:
        contact_delta_percent = round(((current_contacts - previous_contacts) / previous_contacts) * 100)

    latest_order = {}
    latest_delivery = {}
    order_count_by_customer = defaultdict(int)
    orders_after_contact_by_week = {w["key"]: set() for w in weeks}

    for idx, order in enumerate(order_rows):
        customer_key = normalize_key(order["Customer"])
        order_date = parse_date_value(order["Order date"])
        delivery_date = parse_date_value(order["Delivery date"])
        ref = order["Reference"].strip() or f"row-{idx}"

        if order_date:
            if customer_key not in latest_order or order_date > latest_order[customer_key]:
                latest_order[customer_key] = order_date
        if delivery_date:
            if customer_key not in latest_delivery or delivery_date > latest_delivery[customer_key]:
                latest_delivery[customer_key] = delivery_date
        if ref:
            order_count_by_customer[customer_key] += 1

        if not order_date or not customer_belongs_to_selected(order["Customer"]):
            continue

        key = week_key(order_date)
        if key not in week_keys:
            continue

        prior_contacts = [d for d in contact_dates_by_customer.get(customer_key, []) if d <= order_date]
        if not prior_contacts:
            continue
        latest_prior_contact = prior_contacts[-1]
        if 0 <= (order_date - latest_prior_contact).days <= 10:
            orders_after_contact_by_week[key].add(ref)

    included_customer_keys = None
    if selected_responsible:
        included_customer_keys = {
            customer_key
            for customer_key, customer in customers_by_name.items()
            if customer.get("sales_person") == selected_responsible
        }
    email_performance = build_email_performance(
        message_rows,
        recipient_rows,
        order_rows,
        included_customer_keys=included_customer_keys,
        today=today,
    )

    payload = {
        "generated_at": stockholm_now().isoformat(timespec="minutes"),
        "selected_responsible": selected_responsible,
        "responsible_options": responsible_options,
        "weeks": weeks,
        "dfp_leaderboard": dfp_leaderboard,
        "dfp_top_weeks_2026": dfp_top_weeks_2026,
        "freezer_summary": build_freezer_summary(contact_rows),
        "contacts": {
            "current_week_count": current_contacts,
            "previous_week_count": previous_contacts,
            "delta_percent": contact_delta_percent,
            "delta_is_positive": current_contacts >= previous_contacts,
            "positive_by_week": [
                {"week_key": w["key"], "label": w["label"], "count": positive_count_by_week[w["key"]]}
                for w in weeks
            ],
            "orders_after_contact_by_week": [
                {"week_key": w["key"], "label": w["label"], "count": len(orders_after_contact_by_week[w["key"]])}
                for w in weeks
            ],
        },
        "email_performance": email_performance,
    }
    record_performance_step(
        "calculation.followup_insights",
        calculation_started,
        len(customers_by_name),
    )
    return jsonify(payload)


@app.route("/customers/<int:row>/contact", methods=["PATCH"])
def update_customer_contact(row):
    data = request.get_json() or {}
    spreadsheet = get_spreadsheet_with_retry()
    customer = resolve_accessible_customer(
        get_customer_rows(spreadsheet), current_user(), row=row
    )
    if customer is None:
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    sheet = get_worksheet(spreadsheet, "customers_enriched")
    headers = read_with_retry(lambda: sheet.row_values(1))
    if "name" in data:
        headers = ensure_customer_name_column(sheet, headers)

    missing_columns = []
    fields = [
        ("name",                 "name"),
        ("phone",                "phone"),
        ("email",                "email"),
        ("address_google",       "address_google"),
        ("address_number_google","address_number_google"),
        ("city_google",          "city_google"),
        ("postal_code_google",   "postal_code_google"),
        ("region_google",        "region_google"),
        ("comment",              "comment"),
    ]
    address_fields = {"address_google", "address_number_google", "city_google", "postal_code_google", "region_google"}
    address_changed = any(f in data for f in address_fields)

    for field, col_name in fields:
        if field in data and col_name not in headers:
            missing_columns.append(col_name)

    if missing_columns:
        return jsonify({"ok": False, "missing_columns": missing_columns}), 400

    for field, col_name in fields:
        if field in data:
            col_idx = headers.index(col_name) + 1
            if col_name == "comment":
                value = text_to_sheet_value(data[field], max_length=50)
            else:
                value = data[field]
            sheet.update_cell(row, col_idx, value)

    if address_changed:
        # Clear coordinates first
        for coord_col in ("latitude_google", "longitude_google"):
            if coord_col in headers:
                sheet.update_cell(row, headers.index(coord_col) + 1, "")

        # Build full address from updated values + existing sheet values
        existing = dict(zip(headers, sheet.row_values(row)))
        def val(field):
            return data.get(field, existing.get(field, "")).strip()

        address_str = f"{val('address_google')} {val('address_number_google')}, {val('postal_code_google')} {val('city_google')}, Sweden".strip(", ")

        new_lat = new_lng = None
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
        if api_key and address_str:
            try:
                resp = requests.get(
                    "https://maps.googleapis.com/maps/api/geocode/json",
                    params={"address": address_str, "key": api_key, "language": "sv"},
                    timeout=10,
                )
                geo = resp.json()
                if geo.get("results"):
                    loc = geo["results"][0]["geometry"]["location"]
                    new_lat = loc["lat"]
                    new_lng = loc["lng"]
                    lat_value = round(float(new_lat), 7)
                    lng_value = round(float(new_lng), 7)
                    if "latitude_google" in headers:
                        sheet.update_cell(row, headers.index("latitude_google") + 1, lat_value)
                    if "longitude_google" in headers:
                        sheet.update_cell(row, headers.index("longitude_google") + 1, lng_value)
            except Exception:
                pass

    if "modified" in headers:
        sheet.update_cell(row, headers.index("modified") + 1, True)
    invalidate_sheet_for_write(sheet)

    result = {"ok": True}
    if address_changed:
        result["latitude"]  = new_lat
        result["longitude"] = new_lng
    return jsonify(result)


@app.route("/customers/<customer_name>/contacts", methods=["POST"])
def add_contact(customer_name):
    customer_name = unquote(customer_name).strip()
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return planning_error(
            "invalid_request",
            "Kontaktuppgifterna är ogiltiga.",
            400,
        )

    contact_type = normalize_planning_contact_type(data.get("contact_channel"))
    physical_contact = contact_type == "visit" or not str(
        data.get("contact_channel") or ""
    ).strip()
    freezer_values = {
        field: checkbox_to_sheet_value(data.get(field, ""))
        for field in FREEZER_COLUMNS
    }
    if physical_contact and not any(freezer_values.values()):
        return jsonify({"ok": False, "error": "freezer_selection_required"}), 400
    if not str(data.get("comment") or "").strip():
        return planning_error(
            "comment_required",
            "Kommentar är obligatorisk.",
            400,
            field="comment",
        )

    raw_follow_up = data.get("follow_up")
    if isinstance(raw_follow_up, dict):
        follow_up = dict(raw_follow_up)
    else:
        follow_up = {
            "enabled": data.get("follow_up_enabled"),
            "contact_type": data.get("follow_up_contact_type"),
            "scheduled_at": data.get("follow_up_scheduled_at"),
            "note": data.get("follow_up_note"),
        }
    follow_up_enabled = is_yes(follow_up.get("enabled")) or follow_up.get(
        "enabled"
    ) is True
    follow_up_type = ""
    follow_up_at = None
    follow_up_note = ""
    if follow_up_enabled:
        follow_up_type = normalize_planning_contact_type(
            follow_up.get("contact_type")
        )
        if follow_up_type not in PLANNING_CONTACT_TYPES:
            return planning_error(
                "invalid_follow_up_contact_type",
                "Välj Besök, Telefon eller Mejl för uppföljningen.",
                400,
                field="follow_up.contact_type",
            )
        follow_up_at = parse_planning_datetime(follow_up.get("scheduled_at"))
        if follow_up_at is None:
            return planning_error(
                "invalid_follow_up_scheduled_at",
                "Ange datum och tid för uppföljningen.",
                400,
                field="follow_up.scheduled_at",
            )
        follow_up_note = str(follow_up.get("note") or "").strip()
        if len(follow_up_note) > 300:
            return planning_error(
                "follow_up_note_too_long",
                "Uppföljningsanteckningen får vara högst 300 tecken.",
                400,
                field="follow_up.note",
            )

    planned_activity_id = str(
        data.get("planned_activity_id") or ""
    ).strip()
    client_request_id = normalize_client_request_id(
        data.get("client_request_id")
    )
    if (follow_up_enabled or planned_activity_id) and not client_request_id:
        return planning_error(
            "invalid_client_request_id",
            "Ett giltigt request-ID krävs för kalenderkopplade kontakter.",
            400,
            field="client_request_id",
        )

    try:
        spreadsheet = get_spreadsheet_with_retry()
    except Exception:
        app.logger.exception("Could not open contact store")
        return planning_error(
            "contact_store_unavailable",
            "Kontakten kunde inte sparas. Försök igen.",
            503,
        )
    customers = get_customer_rows(spreadsheet)
    requested_customer = resolve_accessible_customer(
        customers,
        current_user(),
        customer_id=data.get("customer_id"),
        customer_name=customer_name,
    )
    if (
        requested_customer is None
        or normalize_key(requested_customer.get("customer"))
        != normalize_key(customer_name)
    ):
        return jsonify({"ok": False, "error": "customer_not_found"}), 404
    customer_name = str(requested_customer.get("customer") or "").strip()
    sheet = get_worksheet(spreadsheet, "sales_activities")
    headers = ensure_contact_worksheet_schema(sheet)
    session_caller = current_user()
    calendar_coupled = bool(follow_up_enabled or planned_activity_id)
    caller = session_caller
    if calendar_coupled:
        try:
            active_caller = find_active_user(
                spreadsheet,
                session_caller.get("user_name"),
            )
        except Exception:
            app.logger.exception("Could not validate contact actor")
            return planning_error(
                "user_store_unavailable",
                "Användaren kunde inte verifieras. Försök igen.",
                503,
            )
        if not active_caller:
            return planning_error(
                "planning_access_forbidden",
                "Ditt konto är inte aktivt för kalenderkopplade kontakter.",
                403,
            )
        caller = public_user(active_caller)
    owner = caller
    planned_row_index = None
    planned_headers = None
    planned_row = {}
    customer = requested_customer

    if planned_activity_id:
        (
            _planned_sheet,
            planned_headers,
            planned_row_index,
            planned_row,
        ) = find_planned_activity(spreadsheet, planned_activity_id)
        if not planned_row_index:
            return planning_error(
                "activity_not_found",
                "Den planerade aktiviteten kunde inte hittas.",
                404,
            )
        try:
            activity_owner = find_active_user(
                spreadsheet,
                planned_row.get("user_name"),
            )
        except Exception:
            app.logger.exception("Could not validate planned activity owner")
            return planning_error(
                "user_store_unavailable",
                "Aktivitetens säljare kunde inte verifieras. Försök igen.",
                503,
            )
        try:
            planned_customer = resolve_planning_customer(
                spreadsheet,
                {
                    "customer_id": planned_row.get("customer_id"),
                    "customer": planned_row.get("customer"),
                    "customer_row": planned_row.get("customer_row"),
                    "customer_number": planned_row.get("customer_number"),
                },
            )
        except CustomerResolutionError as exc:
            return planning_error(exc.code, str(exc), exc.status)
        valid_customer_names = {
            normalize_key(planned_row.get("customer")),
            normalize_key(
                planned_customer.get("customer") if planned_customer else ""
            ),
        }
        valid_customer_names.discard("")
        if normalize_key(customer_name) not in valid_customer_names:
            return planning_error(
                "activity_customer_mismatch",
                "Aktiviteten hör till en annan butik.",
                409,
            )
        current_activity_owner = canonical_owner_for_customer(
            spreadsheet, planned_customer
        )
        if not current_activity_owner or not user_can_be_sales_owner(
            current_activity_owner, customers
        ):
            return planning_error(
                "activity_owner_not_sales_user",
                "Aktiviteten saknar en aktiv säljare och kan inte loggas.",
                422,
            )
        if (
            normalize_key(current_activity_owner.get("user_name"))
            != normalize_key(caller.get("user_name"))
            and not user_is_admin(caller)
        ):
            return planning_error(
                "planning_owner_forbidden",
                "Du får inte logga en annan säljares aktivitet.",
                403,
            )
        if str(planned_row.get("status") or "").strip().casefold() in {
            "cancelled",
            "skipped",
        }:
            return planning_error(
                "activity_not_active",
                "Aktiviteten är inte längre aktiv.",
                409,
            )
        owner = public_user(current_activity_owner)
        if not contact_type:
            contact_type = normalize_planning_contact_type(
                planned_row.get("contact_type")
            )
        if planned_customer:
            customer = planned_customer
        customer_name = str(
            (planned_customer or {}).get("customer")
            or planned_row.get("customer")
            or customer_name
        ).strip()
    elif follow_up_enabled:
        requested_owner_name = str(data.get("user_name") or "").strip()
        if (
            user_is_admin(caller)
            and not customer_owned_by_user(requested_customer, caller)
            and not requested_owner_name
        ):
            return planning_error(
                "planning_owner_required",
                "Välj en aktiv säljare för den kalenderkopplade kontakten.",
                422,
                field="user_name",
            )
        requested_owner_name = (
            requested_owner_name
            or str(caller.get("user_name") or "").strip()
        )
        if (
            normalize_key(requested_owner_name)
            != normalize_key(caller.get("user_name"))
            and not user_is_admin(caller)
        ):
            return planning_error(
                "planning_owner_forbidden",
                "Du får inte skapa en uppföljning åt en annan säljare.",
                403,
            )
        try:
            requested_owner = find_active_user(
                spreadsheet,
                requested_owner_name,
            )
        except Exception:
            app.logger.exception("Could not validate follow-up owner")
            return planning_error(
                "user_store_unavailable",
                "Den valda säljaren kunde inte verifieras. Försök igen.",
                503,
            )
        if not requested_owner:
            return planning_error(
                "planning_owner_not_found",
                "Den valda säljaren är inte aktiv.",
                404,
                field="user_name",
            )
        if not user_can_be_sales_owner(requested_owner, customers):
            if normalize_key(requested_owner_name) == normalize_key(
                caller.get("user_name")
            ) and not user_is_admin(caller):
                return planning_error(
                    "planning_access_forbidden",
                    "Ditt konto saknar behörighet att skapa uppföljningar.",
                    403,
                )
            return planning_error(
                "planning_owner_not_sales_user",
                "Den valda användaren kan inte äga en uppföljning.",
                422,
                field="user_name",
            )
        owner = public_user(requested_owner)

    if customer is None:
        try:
            customer = resolve_planning_customer(
                spreadsheet,
                {
                    "customer_id": data.get("customer_id"),
                    "customer": customer_name,
                    "customer_row": planned_row.get("customer_row"),
                    "customer_number": planned_row.get("customer_number"),
                },
            )
        except CustomerResolutionError as exc:
            return planning_error(exc.code, str(exc), exc.status)
    if not customer:
        return planning_error(
            "customer_not_found",
            "Butiken kunde inte hittas.",
            404,
        )
    if not customer_access_allowed(customer, session_caller):
        return planning_error(
            "customer_not_found",
            "Butiken kunde inte hittas.",
            404,
        )
    if calendar_coupled and not customer_owned_by_user(customer, owner):
        return planning_error(
            "planning_owner_customer_mismatch",
            "Kunden tillhör inte längre den valda säljaren.",
            409,
        )

    if follow_up_enabled:
        if customer is None:
            try:
                customer = resolve_planning_customer(
                    spreadsheet,
                    {
                        "customer_id": (
                            data.get("customer_id")
                            or planned_row.get("customer_id")
                        ),
                        "customer": customer_name,
                        "customer_row": planned_row.get("customer_row"),
                        "customer_number": planned_row.get("customer_number"),
                    },
                )
            except CustomerResolutionError as exc:
                return planning_error(exc.code, str(exc), exc.status)
        if not customer:
            return planning_error(
                "customer_not_found",
                "Butiken kunde inte hittas för uppföljningen.",
                404,
            )
        if customer_is_cancelled(customer):
            return planning_error(
                "customer_cancelled",
                "Avslutade kunder kan inte få en ny uppföljning.",
                409,
            )

    contact_channel = (
        planning_contact_label(contact_type)
        if contact_type else str(data.get("contact_channel") or "").strip()
    )
    mirrored_follow_up_date = (
        follow_up_at.date().isoformat()
        if follow_up_enabled and follow_up_at
        else str(data.get("follow_up_date") or "").strip()
    )
    if planned_activity_id:
        contact_id = planned_contact_id_for_payload(
            owner=owner,
            planned_activity_id=planned_activity_id,
            customer_name=customer_name,
            customer_key=(
                planned_row.get("customer_number")
                or planned_row.get("customer_key")
                or planned_row.get("customer_row")
            ),
            contact_channel=contact_channel,
            data=data,
            freezer_values=freezer_values,
            follow_up_enabled=follow_up_enabled,
            follow_up_type=follow_up_type,
            follow_up_at=follow_up_at,
            follow_up_note=follow_up_note,
            mirrored_follow_up_date=mirrored_follow_up_date,
        )
    elif client_request_id:
        contact_id = stable_planning_uuid(
            "contact",
            owner.get("user_name"),
            client_request_id,
        )
    else:
        contact_id = str(uuid.uuid4())
    follow_up_activity_id = (
        stable_planning_uuid(
            "follow-up",
            owner.get("user_name"),
            contact_id,
        )
        if follow_up_enabled else ""
    )

    contact_saved = False
    activity_completed = not bool(planned_activity_id)
    follow_up_saved = not follow_up_enabled
    duplicate_contact = False
    follow_up_activity = None
    partial_errors = []

    with _planning_write_lock:
        if planned_activity_id:
            (
                _live_planned_sheet,
                planned_headers,
                planned_row_index,
                planned_row,
            ) = find_planned_activity(spreadsheet, planned_activity_id)
            if not planned_row_index:
                return planning_error(
                    "activity_not_found",
                    "Den planerade aktiviteten kunde inte hittas.",
                    404,
                )
            live_status = str(
                planned_row.get("status") or "planned"
            ).strip().casefold()
            live_completed_id = str(
                planned_row.get("completed_contact_id") or ""
            ).strip()
            if live_status in {"cancelled", "skipped"}:
                return planning_error(
                    "activity_not_active",
                    "Aktiviteten är inte längre aktiv.",
                    409,
                )

        _contact_headers, contact_rows = worksheet_snapshot(
            sheet,
            expected_columns=CONTACT_COLUMNS,
        )
        contacts_with_id = [
            (row_index, row)
            for row_index, row in contact_rows
            if str(row.get("contact_id") or "").strip() == contact_id
        ]
        if len(contacts_with_id) > 1:
            return planning_error(
                "duplicate_contact_id",
                "Kontaktloggen innehåller flera poster med samma kontakt-ID.",
                409,
            )
        existing_contact_row = None
        existing_contact = {}
        if contacts_with_id:
            existing_contact_row, existing_contact = contacts_with_id[0]

        if planned_activity_id:
            contacts_for_activity = [
                (row_index, row)
                for row_index, row in contact_rows
                if str(
                    row.get("planned_activity_id") or ""
                ).strip() == planned_activity_id
            ]
            if len(contacts_for_activity) > 1:
                return planning_error(
                    "duplicate_planned_activity_contacts",
                    "Aktiviteten har redan flera kontaktloggar och måste granskas innan den kan ändras.",
                    409,
                )
            if contacts_for_activity:
                planned_contact_row, planned_contact = (
                    contacts_for_activity[0]
                )
                planned_contact_id = str(
                    planned_contact.get("contact_id") or ""
                ).strip()
                if planned_contact_id != contact_id:
                    return planning_error(
                        "planned_activity_contact_conflict",
                        "Aktiviteten har redan loggats med andra kontaktuppgifter.",
                        409,
                    )
                existing_contact_row = planned_contact_row
                existing_contact = planned_contact
            elif existing_contact_row:
                return planning_error(
                    "client_request_id_conflict",
                    "Kontakt-ID:t har redan använts för en annan aktivitet.",
                    409,
                    field="client_request_id",
                )
            if (
                live_status == "completed"
                and live_completed_id
                and live_completed_id != contact_id
            ):
                return planning_error(
                    "planned_activity_contact_conflict",
                    "Aktiviteten har redan genomförts med andra kontaktuppgifter.",
                    409,
                )
            if (
                live_status == "completed"
                and live_completed_id == contact_id
                and not existing_contact_row
            ):
                return planning_error(
                    "completed_contact_missing",
                    "Aktivitetens genomförda kontakt saknas i kontaktloggen.",
                    409,
                )

        if existing_contact_row:
            existing_activity_id = str(
                existing_contact.get("planned_activity_id") or ""
            ).strip()
            if (
                existing_activity_id != planned_activity_id
                or (
                    not planned_activity_id
                    and normalize_key(existing_contact.get("customer"))
                    != normalize_key(customer_name)
                )
            ):
                return planning_error(
                    "client_request_id_conflict",
                    "Request-ID:t har redan använts för en annan kontakt.",
                    409,
                    field="client_request_id",
                )
            contact_saved = True
            duplicate_contact = True
        else:
            row_data = {
                "date_time": data.get(
                    "date_time",
                    stockholm_now().strftime("%Y-%m-%d %H:%M"),
                ),
                "sales_person": user_route_display_name(owner),
                "customer": customer_name,
                "customer_id": str(
                    (customer or {}).get("customer_id")
                    or planned_row.get("customer_id")
                    or ""
                ).strip(),
                "contact_channel": contact_channel,
                "result": str(data.get("result") or "").strip(),
                "comment": text_to_sheet_value(data.get("comment")),
                "customer_contact_person": text_to_sheet_value(
                    data.get("customer_contact_person")
                ),
                "follow_up_date": mirrored_follow_up_date,
                "contact_id": contact_id,
                "planned_activity_id": planned_activity_id,
                **freezer_values,
            }
            try:
                append_dict_row(
                    sheet,
                    CONTACT_COLUMNS,
                    row_data,
                    single_value_columns=FREEZER_COLUMNS,
                )
                contact_saved = True
            except Exception:
                app.logger.exception("Could not append sales activity")
                return planning_error(
                    "contact_store_unavailable",
                    "Kontakten kunde inte sparas. Försök igen.",
                    503,
                )

        if planned_activity_id:
            try:
                (
                    planned_sheet,
                    planned_headers,
                    planned_row_index,
                    planned_row,
                ) = find_planned_activity(spreadsheet, planned_activity_id)
                if not planned_row_index:
                    raise RuntimeError("planned activity disappeared")
                current_completed_id = str(
                    planned_row.get("completed_contact_id") or ""
                ).strip()
                current_status = str(
                    planned_row.get("status") or ""
                ).strip().casefold()
                if current_status == "completed" and current_completed_id not in {
                    "",
                    contact_id,
                }:
                    return planning_error(
                        "activity_already_completed",
                        "Aktiviteten har redan genomförts med en annan kontakt.",
                        409,
                    )
                if not (
                    current_status == "completed"
                    and current_completed_id == contact_id
                ):
                    completion_updates = {
                        "status": "completed",
                        "completed_contact_id": contact_id,
                        "last_mutation_request_id": planning_request_scope(
                            caller,
                            "complete",
                            planned_activity_id,
                            client_request_id,
                        ),
                        "last_mutation_fingerprint": canonical_payload_fingerprint({
                            "operation": "planned_activity.complete.v1",
                            "actor": normalize_key(caller.get("user_name")),
                            "planned_activity_id": planned_activity_id,
                            "contact_id": contact_id,
                        }),
                        "revision": planning_revision(planned_row) + 1,
                        "updated_at": planning_timestamp(),
                    }
                    update_sheet_row(
                        planned_sheet,
                        planned_row_index,
                        planned_headers,
                        completion_updates,
                    )
                activity_completed = True
                if (
                    str(planned_row.get("source") or "").strip().casefold()
                    == "follow_up"
                ):
                    sync_followup_date_to_source_contact(
                        spreadsheet,
                        planned_row.get("source_contact_id"),
                        "",
                    )
            except Exception as exc:
                app.logger.exception("Contact saved but activity completion failed")
                partial_errors.append({
                    "step": "complete_activity",
                    "message": str(exc)[:200],
                })

        if follow_up_enabled and customer:
            try:
                follow_sheet, _follow_headers, follow_rows = (
                    get_planned_activity_snapshot(spreadsheet)
                )
                existing_follow_up = next(
                    (
                        row
                        for _row_index, row in follow_rows
                        if str(
                            row.get("planned_activity_id") or ""
                        ).strip() == follow_up_activity_id
                    ),
                    None,
                )
                if existing_follow_up is not None:
                    follow_up_activity = existing_follow_up
                    follow_up_saved = True
                else:
                    follow_up_activity = build_planned_activity_row(
                        activity_id=follow_up_activity_id,
                        owner=owner,
                        customer=customer,
                        contact_type=follow_up_type,
                        scheduled_at=follow_up_at,
                        note=follow_up_note,
                        source="follow_up",
                        source_contact_id=contact_id,
                        client_request_id=planning_request_scope(
                            caller,
                            "create_follow_up",
                            owner.get("user_name"),
                            client_request_id,
                        ),
                        create_fingerprint=planning_create_fingerprint(
                            actor=caller,
                            owner=owner,
                            customer_id=customer.get("customer_id"),
                            contact_type=follow_up_type,
                            scheduled_at=follow_up_at,
                            duration_minutes=PLANNING_CONTACT_DURATIONS[
                                follow_up_type
                            ],
                            note=follow_up_note,
                            source="follow_up",
                            source_contact_id=contact_id,
                        ),
                        revision=1,
                    )
                    append_dict_row(
                        follow_sheet,
                        PLANNED_ACTIVITY_COLUMNS,
                        follow_up_activity,
                    )
                    follow_up_saved = True
            except Exception as exc:
                app.logger.exception("Contact saved but follow-up append failed")
                partial_errors.append({
                    "step": "create_follow_up",
                    "message": str(exc)[:200],
                })

    if contact_saved:
        try:
            resolve_suggestions_for_contact(
                spreadsheet,
                owner=owner,
                customer_id=str((customer or {}).get("customer_id") or "").strip(),
                contact_id=contact_id,
                request_id=(client_request_id or contact_id),
            )
        except Exception:
            # Recommendation state is deliberately isolated from the existing
            # contact/calendar save path. A later queue reconciliation retries.
            app.logger.exception("Contact saved but suggestion resolution failed")

    if partial_errors:
        return jsonify({
            "ok": False,
            "status": "partial",
            "error": "partial_save",
            "code": "partial_save",
            "message": (
                "Kontakten sparades men hela kalenderuppdateringen kunde inte "
                "slutföras. Försök igen."
            ),
            "contact_id": contact_id,
            "contact_saved": contact_saved,
            "planned_activity_id": planned_activity_id,
            "activity_completed": activity_completed,
            "follow_up": {
                "enabled": follow_up_enabled,
                "saved": follow_up_saved,
                "planned_activity_id": follow_up_activity_id,
            },
            "partial_errors": partial_errors,
        }), 207

    response = {
        "ok": True,
        "status": "saved",
        "duplicate": duplicate_contact,
        "contact_id": contact_id,
        "contact_saved": contact_saved,
        "planned_activity_id": planned_activity_id,
        "activity_completed": activity_completed,
        "follow_up": {
            "enabled": follow_up_enabled,
            "saved": follow_up_saved,
            "planned_activity_id": follow_up_activity_id,
            "activity": (
                public_planned_activity(follow_up_activity)
                if follow_up_activity else None
            ),
        },
    }
    return jsonify(response)

@app.route("/config")
def config():
    return jsonify({"mapsApiKey": os.environ.get("GOOGLE_MAPS_API_KEY", "")})


if __name__ == "__main__":
    start_brevo_background_workers()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT") or 5000), debug=False)
