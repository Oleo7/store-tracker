"""Read-only review of legacy follow-ups completed by unsuccessful attempts.

This command accepts an explicitly supplied local CRM snapshot. It never opens
credentials, connects to Google Sheets, or writes a migration to any database.
The production obligation reader owns the classification; this CLI only adapts
the snapshot format and writes an auditable local report. The output contains
customer/source identifiers and must stay outside a public repository.

Example:
    python scripts/review_followup_history.py --snapshot /private/crm.json \
        --as-of 2026-10-18T10:53:07+02:00 --output /private/followup-review.json
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def snapshot_records(snapshot, title):
    """Return complete, padded source rows without positional synthetic IDs."""
    try:
        sheet = snapshot["sheets"][title]
    except (KeyError, TypeError) as error:
        raise ValueError(f"Snapshot is missing required sheet {title!r}") from error
    values = sheet.get("values")
    if not isinstance(values, list) or not values:
        raise ValueError(f"Snapshot sheet {title!r} has no header row")
    headers = [str(value or "").strip() for value in values[0]]
    if len([header for header in headers if header]) != len(set(header for header in headers if header)):
        raise ValueError(f"Snapshot sheet {title!r} has duplicate nonempty headers")
    records = []
    for row in values[1:]:
        if not isinstance(row, list):
            raise ValueError(f"Snapshot sheet {title!r} has a non-matrix row")
        if not any(str(value or "").strip() for value in row):
            continue
        padded = list(row) + [""] * max(0, len(headers) - len(row))
        records.append({header: padded[index] for index, header in enumerate(headers) if header})
    return records


def parse_as_of(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("as-of must be an ISO 8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("as-of must include an explicit timezone offset")
    return parsed


def build_report(snapshot, *, as_of, source_sha256, classify, replay=None):
    contacts = snapshot_records(snapshot, "sales_activities")
    plans = snapshot_records(snapshot, "planned_activities")
    candidates = classify(contacts, plans, as_of=as_of)
    # No automatic apply plan is generated: a completed contact attempt and a
    # resolved promise have different semantics. Later unlinked activity alone
    # is insufficient evidence of resolution.
    review_obligations = []
    if replay is not None:
        provenance = replay.get("provenance", {})
        if provenance.get("snapshot_file_sha256") != source_sha256:
            raise ValueError("Replay and review use different CRM snapshots")
        if parse_as_of(provenance.get("as_of", "")) != as_of:
            raise ValueError("Replay and review use different as_of instants")
        state = replay.get("customer_state")
        if not isinstance(state, dict):
            raise ValueError("Replay does not include the authoritative obligation state")
        fields = (
            "obligation_id", "customer_id", "customer", "owner_user_name", "purpose",
            "source_contact_id", "source_contact_row", "source_created_at", "source_fingerprint",
            "original_due_date", "due_date", "planned_activity_id", "status", "evidence_quality",
            "review_reasons", "revision", "snooze_until", "latest_attempt_at", "latest_attempt_result",
        )
        for obligations in state.get("obligations", {}).values():
            for item in obligations:
                if item.get("status") == "needs_review":
                    review_obligations.append({field: item.get(field) for field in fields})
        review_obligations.sort(key=lambda item: (item["customer_id"], item["obligation_id"]))
    return {
        "report_version": "followup-history-review-v1",
        "read_only": True,
        "as_of": as_of.isoformat(),
        "snapshot_sha256": source_sha256,
        "source_counts": {"contacts": len(contacts), "planned_activities": len(plans)},
        "candidate_count": len(candidates),
        "classification_counts": dict(Counter(
            item.get("review_classification", "needs_review") for item in candidates
        )),
        "includes_authoritative_obligation_review": replay is not None,
        "review_obligation_count": len(review_obligations),
        "review_obligation_evidence_counts": dict(Counter(
            item.get("evidence_quality", "unknown") for item in review_obligations
        )),
        "instructions": [
            "Do not reopen completed calendar attempts automatically.",
            "Resolve each underlying promise using its source ID and explicit linked evidence.",
            "Check source fingerprints/revisions before any separately authorized mutation.",
            "Treat contact permission as a separate guard, not proof of promise resolution.",
            "A repeated review does not authorize a repeated migration or customer contact.",
        ],
        "candidates": candidates,
        "review_obligations": review_obligations,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--as-of", required=True, type=parse_as_of)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--replay", type=Path, help="Actual backend replay from the same snapshot/time; includes all review obligations")
    args = parser.parse_args(argv)
    raw = args.snapshot.read_bytes()
    snapshot = json.loads(raw)
    sys.path.insert(0, str(ROOT / "web-app"))
    from customer_obligations import classify_legacy_completed_followups
    replay = json.loads(args.replay.read_bytes()) if args.replay else None
    report = build_report(snapshot, as_of=args.as_of,
                          source_sha256=hashlib.sha256(raw).hexdigest(),
                          classify=classify_legacy_completed_followups, replay=replay)
    if args.output.resolve() in {args.snapshot.resolve(), args.replay.resolve() if args.replay else None}:
        parser.error("output must not overwrite an input file")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps({"output": str(args.output), "read_only": True,
                      "candidate_count": report["candidate_count"],
                      "review_obligation_count": report["review_obligation_count"],
                      "classification_counts": report["classification_counts"]},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
