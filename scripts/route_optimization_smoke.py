"""Manual Google Route Optimization synthetic schema/solve smoke tool.

Both modes make an external request with synthetic data only.  VALIDATE_ONLY
checks Google's request schema; a paid solve requires its separate explicit
flag.  This script must never read CRM data.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo


WEB_APP = Path(__file__).resolve().parents[1] / "web-app"
sys.path.insert(0, str(WEB_APP))

from route_optimization import (  # noqa: E402
    RouteOptimizationError,
    RouteOptimizationProvider,
    TrustedCoordinate,
    build_optimize_tours_request,
    load_service_account_credentials,
    parse_optimize_tours_response,
)


SYNTHETIC_ROUTE_START = datetime(2026, 1, 15, 9, 0, tzinfo=ZoneInfo("Europe/Stockholm"))
PAID_RESPONSE_DIR = WEB_APP.parent / ".codex_tmp" / "route_optimization_smoke"


def synthetic_shipments():
    return [{
        "customer_id": f"00000000-0000-4000-8000-{index:012d}",
        "coordinate": TrustedCoordinate(
            57.70950 + index / 10000,
            11.97520 + index / 10000,
        ),
        "priority_score": 40 + index,
        "required": False,
    } for index in range(1, 21)]


def synthetic_request(*, solving_mode):
    start = TrustedCoordinate(57.70887, 11.97456)
    return build_optimize_tours_request(
        run_id="manual-synthetic-smoke",
        owner_user_name="synthetic-owner",
        route_start=SYNTHETIC_ROUTE_START,
        start=start,
        shipments=synthetic_shipments(),
        timeout_seconds=90,
        solving_mode=solving_mode,
    )


def persist_paid_response(response):
    PAID_RESPONSE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = PAID_RESPONSE_DIR / f"paid-response-{timestamp}.json"
    path.write_text(
        json.dumps(response, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path.resolve()


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--validate-only", action="store_true")
    group.add_argument("--paid-synthetic-solve", action="store_true")
    args = parser.parse_args()
    project = str(os.environ.get("ROUTE_OPTIMIZATION_PROJECT") or "").strip()
    if not project:
        parser.error("ROUTE_OPTIMIZATION_PROJECT is required")
    credentials = load_service_account_credentials()
    mode = "DEFAULT_SOLVE" if args.paid_synthetic_solve else "VALIDATE_ONLY"
    body = synthetic_request(solving_mode=mode)
    started = time.perf_counter()
    response, status = RouteOptimizationProvider(credentials=credentials).optimize(
        project=project,
        body=body,
        timeout_seconds=90,
    )
    solve_duration_seconds = time.perf_counter() - started
    result = {
        "ok": 200 <= status < 300,
        "mode": "paid-synthetic-solve" if args.paid_synthetic_solve else "validate-only",
        "http_status": status,
        "shipment_count": len(body["model"]["shipments"]),
        "route_count": len(response.get("routes") or []),
        "solve_duration_seconds": round(solve_duration_seconds, 3),
        "raw_response_path": None,
        "parser_accepted": None,
    }
    if args.paid_synthetic_solve:
        raw_response_path = persist_paid_response(response)
        result["raw_response_path"] = str(raw_response_path)
        try:
            parsed = parse_optimize_tours_response(
                response,
                shipments=synthetic_shipments(),
                owner_user_name="synthetic-owner",
                route_start=SYNTHETIC_ROUTE_START,
            )
            result["parser_accepted"] = True
            result["summary"] = parsed["summary"]
        except RouteOptimizationError as exc:
            result["parser_accepted"] = False
            result["parser_error"] = {
                "type": type(exc).__name__,
                "code": getattr(exc, "code", None),
            }
            print(json.dumps(result, separators=(",", ":")))
            return 2
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
