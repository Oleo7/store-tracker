"""Read-only replay of an exact Store Tracker checkout through Flask.

The only replacements are the Sheets I/O boundary and wall clock. Actual
worksheet parsers, feature builders, priority model, workflow suppression,
owner filtering and /customer-insights handler all execute from the checkout.
No credentials, source writes or external network requests are permitted.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
import copy
from datetime import datetime
import hashlib
import importlib.metadata
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from freezegun import freeze_time


def model_source_hashes(repo):
    sources = {}
    for name in ("app.py", "priority.py", "value_prioritization.py", "customer_obligations.py",
                 "contact_channel.py", "contact_permissions.py", "planning_suggestions.py", "route_proposal.py", "route_optimization.py", "routing_objective.py", "index.html"):
        source = repo / "web-app" / name
        if source.is_file():
            sources["web-app/" + name] = hashlib.sha256(source.read_bytes()).hexdigest()
    return sources


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--as-of", required=True, help="Timezone-aware frozen replay time")
    parser.add_argument("--model-header", default="v3.5", help="API read-model version; use v3.3 for earlier PR48")
    parser.add_argument("--policy", default="{}")
    parser.add_argument("--scenario", default="baseline")
    parser.add_argument("--skip-user-endpoints", action="store_true")
    parser.add_argument("--verification-only", action="store_true", help="Omit repeated portfolio payloads from a user verification shard")
    parser.add_argument("--user-name", help="Verify one actual user for sharded replay; default verifies all")
    parser.add_argument("--progress", action="store_true", help="Log endpoint/count progress, never customer data")
    args = parser.parse_args()
    try:
        replay_time = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
    except ValueError:
        parser.error("--as-of must be an ISO 8601 timestamp")
    if replay_time.tzinfo is None or replay_time.utcoffset() is None:
        parser.error("--as-of must include an explicit timezone offset")
    repo = Path(args.repo).resolve()
    snapshot_path = Path(args.snapshot).resolve()
    if Path(args.output).resolve() == snapshot_path:
        parser.error("--output must not overwrite the source snapshot")
    raw = snapshot_path.read_bytes()
    snapshot = json.loads(raw)
    os.environ["APP_ENV"] = "test"
    os.environ["GOOGLE_CREDENTIALS"] = "{}"
    os.environ["FLASK_SECRET_KEY"] = "local-crm-review-only"
    sys.path.insert(0, str(repo / "web-app"))
    source_hashes = model_source_hashes(repo)

    with freeze_time(args.as_of):
        import app as application
        import priority
        import requests

        policy_overrides = json.loads(args.policy)
        if policy_overrides:
            import value_prioritization
            value_prioritization.POLICY.update(policy_overrides)
            value_prioritization.validate_policy(value_prioritization.POLICY)

        blocked_operations = []

        class ReadOnlyWorksheet:
            def __init__(self, title, source):
                self.title = title
                self.values = copy.deepcopy(source["values"])
                props = source.get("properties", {}).get("gridProperties", {})
                self.row_count = props.get("rowCount", len(self.values))
                self.col_count = props.get("columnCount", max(map(len, self.values), default=0))
                self.reads = 0

            def get_all_values(self):
                self.reads += 1
                width = max(map(len, self.values), default=0)
                return [list(row) + [""] * (width - len(row)) for row in self.values]

            def row_values(self, row):
                self.reads += 1
                return list(self.values[row - 1]) if 0 < row <= len(self.values) else []

            def __getattr__(self, name):
                if name in {"append_row", "append_rows", "update", "update_cell",
                            "batch_update", "resize", "insert_cols", "delete_columns",
                            "clear", "batch_clear", "delete_rows", "insert_rows"}:
                    def blocked(*_args, **_kwargs):
                        blocked_operations.append(f"{self.title}.{name}")
                        raise RuntimeError(f"BLOCKED WRITE: {self.title}.{name}")
                    return blocked
                raise AttributeError(name)

        class ReadOnlySpreadsheet:
            # Exercise the same actual cache path as production gspread. The
            # adapter remains read-only; no cached calculations are fabricated.
            _store_tracker_enable_read_cache = True

            def __init__(self):
                self.sheets = {name: ReadOnlyWorksheet(name, sheet)
                               for name, sheet in snapshot["sheets"].items()}

            def worksheet(self, title):
                if title not in self.sheets:
                    raise application.WorksheetNotFound(title)
                return self.sheets[title]

            def add_worksheet(self, *_args, **_kwargs):
                blocked_operations.append("add_worksheet")
                raise RuntimeError("BLOCKED WRITE: add_worksheet")

            def values_batch_update(self, *_args, **_kwargs):
                blocked_operations.append("values_batch_update")
                raise RuntimeError("BLOCKED WRITE: values_batch_update")

        spreadsheet = ReadOnlySpreadsheet()

        def network_blocked(*_args, **_kwargs):
            blocked_operations.append("outbound_http")
            raise RuntimeError("BLOCKED: replay attempted an external network request")

        application.app.config.update(TESTING=True, SECRET_KEY="local-crm-review-only")
        application.app.logger.setLevel(logging.ERROR)
        application._email_sheets_cache = None
        application.invalidate_priority_snapshot()
        with ExitStack() as stack:
            stack.enter_context(patch.object(application, "get_spreadsheet_with_retry", return_value=spreadsheet))
            stack.enter_context(patch.object(application, "get_spreadsheet", return_value=spreadsheet))
            stack.enter_context(patch.object(requests.sessions.Session, "request", side_effect=network_blocked))
            now = application.stockholm_now()
            if args.progress:
                print("Building full backend snapshot", file=sys.stderr, flush=True)
            global_snapshot = application.get_authoritative_priority_snapshot(
                spreadsheet, today=application.stockholm_today())
            pre_workflow = global_snapshot["priorities"]
            workflow = application.priority_workflow_suppressions(spreadsheet, pre_workflow)
            priorities = application.apply_workflow_suppressions(pre_workflow, workflow)
            users = application.get_user_rows(spreadsheet)
            public_users = [application.public_user(user) for user in users
                            if application.is_yes(user.get("active"))]
            # The actual admin role is sourced from CRM. No permissions are
            # added. Both snapshots are partitioned by identical customer owner.
            admin_user = next(user for user in public_users if application.user_is_admin(user))
            client = application.app.test_client()
            with client.session_transaction() as session:
                session["user"] = admin_user
            response = client.get("/customer-insights", headers={"X-PB-Value-Model": args.model_header})
            if response.status_code != 200:
                raise RuntimeError(f"Admin endpoint failed: {response.status_code}")
            admin_payload = response.get_json()
            model_version = getattr(priority, "VALUE_MODEL_VERSION", None)
            if model_version:
                assert admin_payload.get("__by_customer_id"), "Value model returned no canonical customer records"
                active_ids = {row["customer_id"] for row in priorities}
                assert active_ids <= set(admin_payload["__by_customer_id"]), "Active customers are absent from the API"
                models = Counter(admin_payload["__by_customer_id"][cid].get("value_model_version") for cid in active_ids)
                assert models == {model_version: len(priorities)}, f"API/model coverage mismatch: {models}"
            per_user = {}
            attention_by_user = {}
            has_attention = any(rule.rule == "/planning/attention" for rule in application.app.url_map.iter_rules())
            if not args.skip_user_endpoints:
                selected_users = [u for u in public_users if not args.user_name or u.get("user_name") == args.user_name]
                if not selected_users:
                    raise ValueError("Requested replay user was not found")
                for user_number, user in enumerate(selected_users, start=1):
                    if args.progress:
                        print(f"Checking user endpoint {user_number}/{len(public_users)}", file=sys.stderr, flush=True)
                    with client.session_transaction() as session:
                        session["user"] = user
                    result = client.get("/customer-insights", headers={"X-PB-Value-Model": args.model_header})
                    if result.status_code != 200:
                        raise RuntimeError(f"User endpoint failed: {result.status_code}")
                    payload = result.get_json()
                    expected = application.filter_accessible_customers(global_snapshot["customers"], user)
                    expected_ids = {c["customer_id"] for c in expected if c.get("customer_id")}
                    actual_ids = set(payload.get("__by_customer_id", {}))
                    if "__by_customer_id" in payload:
                        assert actual_ids == expected_ids, f"Owner visibility mismatch for {user['name']}"
                        for cid, item in payload["__by_customer_id"].items():
                            for field in ("business_value_raw", "contact_priority_raw", "work_priority_raw", "priority_score", "list_guidance"):
                                assert item.get(field) == admin_payload["__by_customer_id"][cid].get(field), (user["name"], cid, field)
                    per_user[user["name"]] = {
                        "is_admin": application.user_is_admin(user),
                        "http_status": result.status_code,
                        "visible_customer_count": len(expected_ids),
                        "payload_count": len(payload),
                        "id_keys_verified": "__by_customer_id" in payload,
                    }
                    if has_attention:
                        items, offset, total = [], 0, None
                        while total is None or offset < total:
                            result = client.get("/planning/attention", query_string={"offset": offset, "limit": 200})
                            assert result.status_code == 200, f"Attention endpoint failed: {result.status_code}"
                            page = result.get_json()
                            total = page["total"]
                            batch = page["items"]
                            assert batch or offset >= total, "Attention pagination stopped before its stated total"
                            items.extend(batch)
                            offset += len(batch)
                            if args.progress:
                                print(f"  Attention verified {offset}/{total}", file=sys.stderr, flush=True)
                        assert len(items) == total
                        assert len({row["obligation_id"] for row in items}) == len(items), "Duplicate obligation in attention"
                        for row in items:
                            assert row["customer_id"] in expected_ids, "Attention owner boundary mismatch"
                            assert row["item_type"] in {"missed_calendar_activity", "overdue_explicit_follow_up"}
                            assert row["due_date"] < now.date().isoformat(), "Today/future commitment leaked into attention"
                        attention_by_user[user["name"]] = items
                        per_user[user["name"]].update(attention_count=len(items), attention_read_verified=True)

            # An application helper could catch the boundary exception. Fail
            # the replay anyway; a swallowed write attempt is not a pure GET.
            assert not blocked_operations, f"Replay attempted prohibited operations: {blocked_operations}"
            assert source_hashes == model_source_hashes(repo), "Model source changed during replay; run again on a stable checkout"

            output = {
                "provenance": {
                    "repo": str(repo),
                    "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
                    "as_of": now.isoformat(),
                    "snapshot_path": str(snapshot_path),
                    "snapshot_file_sha256": hashlib.sha256(raw).hexdigest(),
                    "scenario": args.scenario,
                    "user_verification_scope": args.user_name or ("skipped" if args.skip_user_endpoints else "all"),
                    "policy_overrides": policy_overrides,
                    "model": getattr(priority, "VALUE_MODEL_VERSION", None),
                    "score_version": priority.SCORE_VERSION,
                    "python": sys.version,
                    "dependencies": {name: importlib.metadata.version(name) for name in
                        ("flask", "flask-cors", "gunicorn", "gspread", "google-auth", "python-dotenv", "requests", "tzdata", "freezegun")},
                    "replacements": ["Google Sheets I/O adapter", "frozen system wall clock", "blocked outbound HTTP"],
                    "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).strip()),
                    "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD", "--"], cwd=repo)).hexdigest(),
                    "model_source_sha256": source_hashes,
                    "http_status": response.status_code,
                    "worksheet_reads": {name: sheet.reads for name, sheet in spreadsheet.sheets.items()},
                    "writes": 0,
                },
                "summary": {
                    "customers_loaded": len(global_snapshot["customers"]),
                    "active_priorities": len(priorities),
                    "order_rows": len(global_snapshot["order_rows"]),
                    "contact_rows": len(global_snapshot["contact_rows"]),
                    "message_rows": len(global_snapshot["message_rows"]),
                    "recipient_rows": len(global_snapshot["recipient_rows"]),
                    "planned_activity_rows": len(global_snapshot["planned_activity_rows"]),
                    "workflow_suppressions": dict(Counter(workflow.values())),
                    "list_status": dict(Counter(p.get("list_guidance", {}).get("status_key", "") for p in priorities)),
                    "list_reason": dict(Counter(p.get("list_guidance", {}).get("reason_code", "") for p in priorities)),
                    "seller_counts": dict(Counter(p["sales_person"] for p in priorities)),
                    "user_endpoints": per_user,
                },
                "priorities": priorities,
                "pre_workflow_priorities": pre_workflow,
                "workflow_suppressions": workflow,
                "admin_endpoint": admin_payload,
                "attention_by_user": attention_by_user,
                "customer_state": global_snapshot.get("customer_state"),
            }
            if args.verification_only:
                for field in ("priorities", "pre_workflow_priorities", "admin_endpoint", "customer_state", "workflow_suppressions"):
                    output.pop(field, None)
            path = Path(args.output)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str))
            print(json.dumps({"output": str(path), "provenance": output["provenance"], "summary": output["summary"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
