"""Manual Google Route Optimization configuration/synthetic smoke tool.

The default/--validate-only path is local and free.  A paid request is possible
only with the explicit --paid-synthetic-solve flag and must never use CRM data.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo


WEB_APP = Path(__file__).resolve().parents[1] / "web-app"
sys.path.insert(0, str(WEB_APP))

from route_optimization import (  # noqa: E402
    RouteOptimizationProvider,
    TrustedCoordinate,
    build_optimize_tours_request,
    load_service_account_credentials,
)


def synthetic_request():
    start = TrustedCoordinate(57.70887, 11.97456)
    return build_optimize_tours_request(
        run_id="manual-synthetic-smoke",
        owner_user_name="synthetic-owner",
        route_start=datetime(2026, 1, 15, 9, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
        start=start,
        shipments=[{
            "customer_id": "00000000-0000-4000-8000-000000000001",
            "coordinate": TrustedCoordinate(57.70950, 11.97520),
            "priority_score": 50,
            "required": False,
        }],
        timeout_seconds=90,
    )


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--validate-only", action="store_true")
    group.add_argument("--paid-synthetic-solve", action="store_true")
    args = parser.parse_args()
    project = str(os.environ.get("ROUTE_OPTIMIZATION_PROJECT") or "").strip()
    credentials = load_service_account_credentials()
    body = synthetic_request()
    if not args.paid_synthetic_solve:
        print(json.dumps({
            "ok": bool(project and credentials and body),
            "mode": "validate-only",
            "project_configured": bool(project),
            "shipment_count": len(body["model"]["shipments"]),
        }, separators=(",", ":")))
        return 0 if project else 2
    response, status = RouteOptimizationProvider(credentials=credentials).optimize(
        project=project,
        body=body,
        timeout_seconds=90,
    )
    print(json.dumps({
        "ok": 200 <= status < 300,
        "mode": "paid-synthetic-solve",
        "http_status": status,
        "route_count": len(response.get("routes") or []),
    }, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
