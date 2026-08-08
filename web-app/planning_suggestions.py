"""Sparse, idempotent planning suggestion state for phase 1.

The module deliberately knows nothing about scoring or future trigger rules.  It
accepts already ranked candidate contexts, joins them with persisted workflow
state, and owns the phase-1 state transitions and append-only event log.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import uuid

from gspread.exceptions import WorksheetNotFound
from gspread.utils import rowcol_to_a1


SUGGESTIONS_SHEET = "planning_suggestions"
SUGGESTION_COLUMNS = [
    "suggestion_id",
    "decision_context_hash",
    "primary_trigger_type",
    "primary_trigger_key",
    "covered_trigger_keys_json",
    "customer_id",
    "customer_key",
    "customer_row",
    "customer",
    "user_name",
    "sales_person",
    "recommended_contact_type",
    "reason_code",
    "reason_text_at_creation",
    "priority_score_at_creation",
    "expected_order_dfp_at_creation",
    "lifecycle_at_creation",
    "status",
    "snooze_until",
    "planned_activity_id",
    "dismissed_at",
    "resolved_at",
    "resolved_by_type",
    "resolved_by_id",
    "generated_at",
    "updated_at",
    "last_evaluated_at",
    "score_version",
    "revision",
    "last_mutation_request_id",
    "last_mutation_fingerprint",
    "intent_timing_at_creation",
    "value_index_at_creation",
    "strategic_index_at_creation",
    "recommendation_eligible_at_creation",
    "suppression_reason_at_creation",
]

SCORE_EVENTS_SHEET = "score_events"
SCORE_EVENT_COLUMNS = [
    "event_id",
    "event_type",
    "occurred_at",
    "customer_id",
    "user_name",
    "sales_person",
    "suggestion_id",
    "decision_context_hash",
    "primary_trigger_type",
    "primary_trigger_key",
    "score_version",
    "lifecycle",
    "recommendation_eligible",
    "suppression_reason",
    "priority_score",
    "intent_timing",
    "value_index",
    "strategic_index",
    "expected_order_dfp",
    "recommended_contact_type",
    "actual_planned_contact_type",
    "status_before",
    "status_after",
    "resolved_by_type",
    "resolved_by_id",
    "client_request_id",
]

ACTIVE_STATUSES = {"pending", "snoozed", "planned"}
ALL_STATUSES = ACTIVE_STATUSES | {"dismissed", "resolved", "expired"}


class SuggestionError(Exception):
    def __init__(self, code, message, status=400, **extra):
        super().__init__(message)
        self.code = code
        self.status = status
        self.extra = extra


def _text(value):
    return str(value or "").strip()


def _key(value):
    return " ".join(_text(value).casefold().split())


def _canonical_hash(payload):
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def decision_context_hash(*, owner, customer_id, lifecycle="", order_count=0,
                          latest_order_reference="", latest_order_date="",
                          latest_contact_id="", latest_contact_result="",
                          latest_contact_date="", active_email_intent_event=""):
    """Hash stable business facts only; never live score, copy, or wall time."""
    return _canonical_hash({
        "owner": _key(owner),
        "customer_id": _text(customer_id),
        "lifecycle": _key(lifecycle),
        "order_count": int(order_count or 0),
        "latest_order_reference": _text(latest_order_reference),
        "latest_order_date": _text(latest_order_date),
        "latest_contact_id": _text(latest_contact_id),
        "latest_contact_result": _key(latest_contact_result),
        "latest_contact_date": _text(latest_contact_date),
        "active_email_intent_event": _text(active_email_intent_event),
    })


def deterministic_suggestion_id(owner, customer_id, context_hash):
    identity = ":".join((_key(owner), _text(customer_id), _text(context_hash)))
    return str(uuid.uuid5(
        uuid.NAMESPACE_URL, f"polarbar-planning-suggestion:{identity}"
    ))


def mutation_fingerprint(action, suggestion_id, request_id, payload=None):
    return _canonical_hash({
        "action": _key(action),
        "suggestion_id": _text(suggestion_id),
        "client_request_id": _text(request_id),
        "payload": payload or {},
    })


def _revision(row):
    try:
        return max(1, int(float(row.get("revision") or 1)))
    except (TypeError, ValueError):
        return 1


def _instant(value, zone):
    try:
        result = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if result.tzinfo is None:
        result = result.replace(tzinfo=zone)
    return result.astimezone(zone)


def _timestamp(now):
    return now.isoformat(timespec="seconds")


def _ensure_columns(sheet, columns):
    headers = [_text(item) for item in sheet.row_values(1)]
    if not headers:
        sheet.append_row(columns)
        return list(columns)
    missing = [column for column in columns if column not in headers]
    if missing:
        start = len(headers) + 1
        sheet.insert_cols([[column] for column in missing], col=start)
        headers.extend(missing)
    return headers


def _get_or_create(spreadsheet, title, columns, rows):
    try:
        sheet = spreadsheet.worksheet(title)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=title, rows=rows, cols=max(10, len(columns))
        )
        sheet.append_row(columns)
        return sheet
    _ensure_columns(sheet, columns)
    return sheet


def _snapshot(sheet, expected_columns):
    values = sheet.get_all_values()
    if not values:
        return list(expected_columns), []
    headers = [_text(item) for item in values[0]]
    rows = []
    for row_index, values_row in enumerate(values[1:], start=2):
        padded = values_row + [""] * (len(headers) - len(values_row))
        raw = dict(zip(headers, padded))
        rows.append((row_index, {
            column: raw.get(column, "") for column in expected_columns
        }))
    return headers, rows


def _append(sheet, columns, row):
    headers = _ensure_columns(sheet, columns)
    values = [[row.get(header, "") for header in headers]]
    first_row = max(2, len(sheet.get_all_values()) + 1)
    if getattr(sheet, "row_count", 0) and first_row > sheet.row_count:
        sheet.resize(rows=sheet.row_count + 100)
    sheet.batch_update([{
        "range": f"A{first_row}:{rowcol_to_a1(first_row, len(headers))}",
        "values": values,
    }], value_input_option="RAW")
    return first_row


def _update(sheet, row_index, headers, changes):
    data = []
    for name, value in changes.items():
        if name not in headers:
            continue
        cell = rowcol_to_a1(row_index, headers.index(name) + 1)
        data.append({"range": f"{cell}:{cell}", "values": [[value]]})
    if data:
        sheet.batch_update(data, value_input_option="RAW")


def public_suggestion(row, live_candidate=None):
    candidate = live_candidate or {}
    try:
        customer_row = int(float(row.get("customer_row") or 0)) or None
    except (TypeError, ValueError):
        customer_row = None
    try:
        priority = int(round(float(
            candidate.get("priority_score", row.get("priority_score_at_creation"))
            or 0
        )))
    except (TypeError, ValueError):
        priority = 0
    return {
        "suggestion_id": _text(row.get("suggestion_id")),
        "customer_id": _text(row.get("customer_id")),
        "customer_row": customer_row,
        "customer": _text(row.get("customer")),
        "priority_score": max(0, min(100, priority)),
        "reason_text": _text(
            candidate.get("reason_text") or row.get("reason_text_at_creation")
        ),
        "recommended_contact_type": _text(
            row.get("recommended_contact_type") or "phone"
        ),
        "trigger_key": _text(
            candidate.get("primary_trigger_key") or row.get("primary_trigger_key")
        ),
        "status": _key(row.get("status")) or "pending",
        "revision": _revision(row),
    }


class PlanningSuggestionService:
    def __init__(self, spreadsheet, *, lock, now, zone):
        self.spreadsheet = spreadsheet
        self.lock = lock
        self.now = now.astimezone(zone)
        self.zone = zone

    def ensure_schema(self):
        suggestion_sheet = _get_or_create(
            self.spreadsheet, SUGGESTIONS_SHEET, SUGGESTION_COLUMNS, 2000
        )
        event_sheet = _get_or_create(
            self.spreadsheet, SCORE_EVENTS_SHEET, SCORE_EVENT_COLUMNS, 4000
        )
        return suggestion_sheet, event_sheet

    def snapshot(self):
        suggestion_sheet, event_sheet = self.ensure_schema()
        headers, rows = _snapshot(suggestion_sheet, SUGGESTION_COLUMNS)
        return suggestion_sheet, event_sheet, headers, rows

    def _event(self, event_sheet, event_type, row, *, request_id="",
               before="", after="", actual_contact_type="",
               resolved_by_type="", resolved_by_id=""):
        event_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            "polarbar-score-event:" + ":".join((
                _text(row.get("suggestion_id")), _key(event_type),
                _text(request_id) or _text(after) or _timestamp(self.now),
            )),
        ))
        _headers, events = _snapshot(event_sheet, SCORE_EVENT_COLUMNS)
        if any(_text(item.get("event_id")) == event_id for _, item in events):
            return event_id
        _append(event_sheet, SCORE_EVENT_COLUMNS, {
            "event_id": event_id,
            "event_type": _text(event_type),
            "occurred_at": _timestamp(self.now),
            "customer_id": _text(row.get("customer_id")),
            "user_name": _text(row.get("user_name")),
            "sales_person": _text(row.get("sales_person")),
            "suggestion_id": _text(row.get("suggestion_id")),
            "decision_context_hash": _text(row.get("decision_context_hash")),
            "primary_trigger_type": _text(row.get("primary_trigger_type")),
            "primary_trigger_key": _text(row.get("primary_trigger_key")),
            "score_version": _text(row.get("score_version")),
            "lifecycle": _text(row.get("lifecycle_at_creation")),
            "recommendation_eligible": _text(
                row.get("recommendation_eligible_at_creation") or "Y"
            ),
            "suppression_reason": _text(
                row.get("suppression_reason_at_creation")
            ),
            "priority_score": row.get("priority_score_at_creation", ""),
            "intent_timing": row.get("intent_timing_at_creation", ""),
            "value_index": row.get("value_index_at_creation", ""),
            "strategic_index": row.get("strategic_index_at_creation", ""),
            "expected_order_dfp": row.get("expected_order_dfp_at_creation", ""),
            "recommended_contact_type": _text(
                row.get("recommended_contact_type") or "phone"
            ),
            "actual_planned_contact_type": _text(actual_contact_type),
            "status_before": _text(before),
            "status_after": _text(after),
            "resolved_by_type": _text(resolved_by_type),
            "resolved_by_id": _text(resolved_by_id),
            "client_request_id": _text(request_id),
        })
        return event_id

    @staticmethod
    def _live_event_row(row, candidate=None):
        candidate = candidate or {}
        if not candidate:
            return row
        return {
            **row,
            "primary_trigger_type": _text(
                candidate.get("primary_trigger_type")
                or row.get("primary_trigger_type")
            ),
            "primary_trigger_key": _text(
                candidate.get("primary_trigger_key")
                or row.get("primary_trigger_key")
            ),
            "lifecycle_at_creation": _text(
                candidate.get("lifecycle") or row.get("lifecycle_at_creation")
            ),
            "priority_score_at_creation": candidate.get(
                "priority_score", row.get("priority_score_at_creation", "")
            ),
            "expected_order_dfp_at_creation": candidate.get(
                "expected_order_dfp",
                row.get("expected_order_dfp_at_creation", ""),
            ),
            "intent_timing_at_creation": candidate.get(
                "intent_timing", row.get("intent_timing_at_creation", "")
            ),
            "value_index_at_creation": candidate.get(
                "value_index", row.get("value_index_at_creation", "")
            ),
            "strategic_index_at_creation": candidate.get(
                "strategic_index", row.get("strategic_index_at_creation", "")
            ),
            "recommendation_eligible_at_creation": (
                "Y" if candidate.get("recommendation_eligible", True) else "N"
            ),
            "suppression_reason_at_creation": _text(
                candidate.get("recommendation_suppression_reason")
            ),
        }

    def _candidate_row(self, owner, candidate):
        context_hash = _text(candidate.get("decision_context_hash"))
        suggestion_id = deterministic_suggestion_id(
            owner.get("user_name"), candidate.get("customer_id"), context_hash
        )
        now_text = _timestamp(self.now)
        return {
            "suggestion_id": suggestion_id,
            "decision_context_hash": context_hash,
            "primary_trigger_type": _text(
                candidate.get("primary_trigger_type") or "phase1_test"
            ),
            "primary_trigger_key": _text(
                candidate.get("primary_trigger_key") or "phase1_test"
            ),
            "covered_trigger_keys_json": json.dumps(
                candidate.get("covered_trigger_keys") or [
                    candidate.get("primary_trigger_key") or "phase1_test"
                ], ensure_ascii=False, separators=(",", ":")
            ),
            "customer_id": _text(candidate.get("customer_id")),
            "customer_key": _text(candidate.get("customer_key")),
            "customer_row": candidate.get("customer_row") or "",
            "customer": _text(candidate.get("customer")),
            "user_name": _text(owner.get("user_name")),
            "sales_person": _text(owner.get("name")),
            "recommended_contact_type": "phone",
            "reason_code": _text(candidate.get("reason_code") or "phase1_test"),
            "reason_text_at_creation": _text(
                candidate.get("reason_text") or "Följ upp kunden"
            ),
            "priority_score_at_creation": candidate.get("priority_score", 0),
            "expected_order_dfp_at_creation": candidate.get(
                "expected_order_dfp", ""
            ),
            "lifecycle_at_creation": _text(candidate.get("lifecycle")),
            "status": "pending",
            "generated_at": now_text,
            "updated_at": now_text,
            "last_evaluated_at": now_text,
            "score_version": _text(candidate.get("score_version") or "phase1"),
            "revision": 1,
            "intent_timing_at_creation": candidate.get("intent_timing", ""),
            "value_index_at_creation": candidate.get("value_index", ""),
            "strategic_index_at_creation": candidate.get("strategic_index", ""),
            "recommendation_eligible_at_creation": (
                "Y" if candidate.get("recommendation_eligible", True) else "N"
            ),
            "suppression_reason_at_creation": _text(
                candidate.get("recommendation_suppression_reason")
            ),
        }

    def queue(self, owner, candidates, activity_rows=()):
        """Reconcile stored state, sparsely materialize only the visible top row."""
        with self.lock:
            sheet, events, headers, stored = self.snapshot()
            owner_key = _key(owner.get("user_name"))
            stored_by_id = {
                _text(row.get("suggestion_id")): (index, row)
                for index, row in stored
                if _key(row.get("user_name")) == owner_key
            }
            activities = {
                _text(row.get("planned_activity_id")): row
                for row in activity_rows
                if _text(row.get("planned_activity_id"))
            }
            candidate_by_id = {}
            ordered = []
            seen_customer_ids = set()
            for candidate in candidates:
                customer_id = _text(candidate.get("customer_id"))
                if not customer_id or customer_id in seen_customer_ids:
                    continue
                seen_customer_ids.add(customer_id)
                suggestion_id = deterministic_suggestion_id(
                    owner.get("user_name"), candidate.get("customer_id"),
                    candidate.get("decision_context_hash")
                )
                candidate_by_id[suggestion_id] = candidate
                ordered.append((suggestion_id, candidate))

            # Reconcile due snoozes and linked activity terminal states.
            for suggestion_id, (row_index, row) in list(stored_by_id.items()):
                status = _key(row.get("status")) or "pending"
                changes = {}
                event_type = ""
                same_customer_has_new_context = any(
                    _text(candidate.get("customer_id"))
                    == _text(row.get("customer_id"))
                    for candidate in candidate_by_id.values()
                )
                if status == "snoozed":
                    due = _instant(row.get("snooze_until"), self.zone)
                    if suggestion_id not in candidate_by_id:
                        if same_customer_has_new_context:
                            changes = {
                                "status": "resolved",
                                "resolved_at": _timestamp(self.now),
                                "resolved_by_type": "business_context",
                                "resolved_by_id": "",
                            }
                            event_type = "suggestion_resolved"
                        else:
                            changes = {"status": "expired"}
                            event_type = "suggestion_expired"
                    elif due and self.now >= due:
                        changes = {"status": "pending", "snooze_until": ""}
                elif status == "planned":
                    activity = activities.get(_text(row.get("planned_activity_id")))
                    activity_status = _key((activity or {}).get("status"))
                    if (
                        suggestion_id not in candidate_by_id
                        and same_customer_has_new_context
                    ):
                        changes = {
                            "status": "resolved",
                            "resolved_at": _timestamp(self.now),
                            "resolved_by_type": "business_context",
                            "resolved_by_id": "",
                        }
                        event_type = "suggestion_resolved"
                    elif not activity or activity_status in {"cancelled", "skipped"}:
                        if suggestion_id in candidate_by_id:
                            changes = {"status": "pending", "planned_activity_id": ""}
                            event_type = "linked_activity_cancelled"
                        else:
                            changes = {"status": "expired"}
                            event_type = "suggestion_expired"
                    elif activity_status == "completed":
                        changes = {
                            "status": "resolved",
                            "resolved_at": _timestamp(self.now),
                            "resolved_by_type": "activity",
                            "resolved_by_id": _text(activity.get("planned_activity_id")),
                        }
                        event_type = "suggestion_resolved"
                elif status in ACTIVE_STATUSES and suggestion_id not in candidate_by_id:
                    if same_customer_has_new_context:
                        changes = {
                            "status": "resolved",
                            "resolved_at": _timestamp(self.now),
                            "resolved_by_type": "business_context",
                            "resolved_by_id": "",
                        }
                        event_type = "suggestion_resolved"
                    else:
                        changes = {"status": "expired"}
                        event_type = "suggestion_expired"
                if changes:
                    before = status
                    changes.update({
                        "revision": _revision(row) + 1,
                        "updated_at": _timestamp(self.now),
                        "last_evaluated_at": _timestamp(self.now),
                    })
                    _update(sheet, row_index, headers, changes)
                    row = {**row, **changes}
                    stored_by_id[suggestion_id] = (row_index, row)
                    if event_type:
                        self._event(
                            events, event_type, row, before=before,
                            after=changes.get("status", before)
                        )

            visible = []
            for suggestion_id, candidate in ordered:
                if candidate.get("externally_suppressed"):
                    continue
                stored_entry = stored_by_id.get(suggestion_id)
                if not stored_entry:
                    visible.append((suggestion_id, candidate, None))
                    continue
                _row_index, row = stored_entry
                if _key(row.get("status")) == "pending":
                    visible.append((suggestion_id, candidate, row))

            pending_count = len(visible)
            if not visible:
                return None, 0
            suggestion_id, candidate, row = visible[0]
            if row is None:
                row = self._candidate_row(owner, candidate)
                _append(sheet, SUGGESTION_COLUMNS, row)
            self._event(events, "suggestion_created", self._live_event_row(row, candidate),
                        before="", after="pending")
            return public_suggestion(row, candidate), pending_count

    def find(self, suggestion_id):
        sheet, events, headers, rows = self.snapshot()
        matches = [
            (index, row) for index, row in rows
            if _text(row.get("suggestion_id")) == _text(suggestion_id)
        ]
        if not matches:
            raise SuggestionError(
                "suggestion_not_found", "Rekommendationen kunde inte hittas.", 404
            )
        if len(matches) > 1:
            raise SuggestionError(
                "duplicate_suggestion_id",
                "Rekommendationen finns i flera exemplar och måste granskas.",
                409,
            )
        row_index, row = matches[0]
        return sheet, events, headers, row_index, row

    def transition(self, suggestion_id, *, owner_name, action,
                   expected_revision, request_id, fingerprint,
                   planned_activity_id="", actual_contact_type="",
                   resolved_by_type="", resolved_by_id="", live_candidate=None):
        targets = {
            "snooze": "snoozed",
            "dismiss": "dismissed",
            "plan": "planned",
            "resolve": "resolved",
            "expire": "expired",
            "reopen": "pending",
        }
        if action not in targets:
            raise SuggestionError("invalid_suggestion_transition", "Ogiltig statusövergång.")
        with self.lock:
            sheet, events, headers, row_index, row = self.find(suggestion_id)
            if _key(row.get("user_name")) != _key(owner_name):
                raise SuggestionError(
                    "suggestion_not_found", "Rekommendationen kunde inte hittas.", 404
                )
            stored_request = _text(row.get("last_mutation_request_id"))
            stored_fingerprint = _text(row.get("last_mutation_fingerprint"))
            if stored_request == _text(request_id):
                if stored_fingerprint and stored_fingerprint != fingerprint:
                    raise SuggestionError(
                        "idempotency_payload_mismatch",
                        "Samma request-ID har redan använts med ett annat innehåll.",
                        409,
                    )
                return row, True
            current_revision = _revision(row)
            if int(expected_revision) != current_revision:
                raise SuggestionError(
                    "suggestion_stale",
                    "Rekommendationen har ändrats. Den aktuella kön har laddats om.",
                    409,
                )
            before = _key(row.get("status")) or "pending"
            target = targets[action]
            allowed = {
                "snooze": {"pending"},
                "dismiss": {"pending"},
                "plan": {"pending"},
                "resolve": ACTIVE_STATUSES,
                "expire": ACTIVE_STATUSES,
                "reopen": {"planned"},
            }
            if before not in allowed[action]:
                raise SuggestionError(
                    "suggestion_not_pending",
                    "Rekommendationen är inte längre tillgänglig för åtgärden.",
                    409,
                )
            changes = {
                "status": target,
                "revision": current_revision + 1,
                "updated_at": _timestamp(self.now),
                "last_evaluated_at": _timestamp(self.now),
                "last_mutation_request_id": _text(request_id),
                "last_mutation_fingerprint": fingerprint,
            }
            event_type = f"suggestion_{target}"
            if action == "snooze":
                changes["snooze_until"] = _timestamp(
                    self.now + timedelta(days=7)
                )
                event_type = "suggestion_snoozed"
            elif action == "dismiss":
                changes["dismissed_at"] = _timestamp(self.now)
                event_type = "suggestion_dismissed"
            elif action == "plan":
                changes["planned_activity_id"] = _text(planned_activity_id)
                event_type = "suggestion_planned"
            elif action == "resolve":
                changes.update({
                    "resolved_at": _timestamp(self.now),
                    "resolved_by_type": _text(resolved_by_type),
                    "resolved_by_id": _text(resolved_by_id),
                })
                event_type = "suggestion_resolved"
            elif action == "expire":
                event_type = "suggestion_expired"
            elif action == "reopen":
                changes["planned_activity_id"] = ""
                event_type = "linked_activity_cancelled"
            _update(sheet, row_index, headers, changes)
            updated = {**row, **changes}
            event_row = self._live_event_row(updated, live_candidate)
            self._event(
                events, event_type, event_row, request_id=request_id,
                before=before, after=target,
                actual_contact_type=actual_contact_type,
                resolved_by_type=resolved_by_type,
                resolved_by_id=resolved_by_id,
            )
            if action == "resolve" and _key(resolved_by_type) == "activity":
                self._event(
                    events, "activity_completed", event_row,
                    request_id=request_id, before=before, after=target,
                    actual_contact_type=actual_contact_type,
                    resolved_by_type=resolved_by_type,
                    resolved_by_id=resolved_by_id,
                )
            return updated, False

    def resolve_customer(self, *, owner_name, customer_id, resolved_by_type,
                         resolved_by_id, request_id):
        with self.lock:
            sheet, events, headers, rows = self.snapshot()
            changed = []
            for row_index, row in rows:
                if (
                    _key(row.get("user_name")) != _key(owner_name)
                    or _text(row.get("customer_id")) != _text(customer_id)
                    or _key(row.get("status")) not in ACTIVE_STATUSES
                ):
                    continue
                before = _key(row.get("status"))
                changes = {
                    "status": "resolved",
                    "resolved_at": _timestamp(self.now),
                    "resolved_by_type": _text(resolved_by_type),
                    "resolved_by_id": _text(resolved_by_id),
                    "revision": _revision(row) + 1,
                    "updated_at": _timestamp(self.now),
                    "last_evaluated_at": _timestamp(self.now),
                }
                _update(sheet, row_index, headers, changes)
                updated = {**row, **changes}
                self._event(
                    events, "suggestion_resolved", updated,
                    request_id=request_id, before=before, after="resolved",
                    resolved_by_type=resolved_by_type,
                    resolved_by_id=resolved_by_id,
                )
                changed.append(updated)
            return changed


def build_phase1_stub_candidates(owner, customers, contacts=(), orders=()):
    """Deterministic development/test-only candidates; caller owns the guard."""
    owner_names = {_key(owner.get("user_name")), _key(owner.get("name"))}
    owned = [
        item for item in customers
        if _key(item.get("sales_person")) in owner_names
        and not _key(item.get("cancelled_flag")) in {
            "1", "y", "yes", "ja", "true", "cancelled", "canceled", "avslutad"
        }
        and _text(item.get("customer_id"))
    ]
    results = []
    for position, customer in enumerate(owned):
        customer_id = _text(customer.get("customer_id"))
        customer_name = _key(customer.get("customer"))
        customer_orders = [
            row for row in orders
            if _text(row.get("customer_id")) == customer_id
            or _key(row.get("Customer")) == customer_name
        ]
        customer_contacts = [
            row for row in contacts
            if _text(row.get("customer_id")) == customer_id
            or _key(row.get("customer")) == customer_name
        ]
        latest_order = customer_orders[-1] if customer_orders else {}
        latest_contact = customer_contacts[-1] if customer_contacts else {}
        context_hash = decision_context_hash(
            owner=owner.get("user_name"),
            customer_id=customer_id,
            lifecycle="phase1_test",
            order_count=len(customer_orders),
            latest_order_reference=latest_order.get("Reference"),
            latest_order_date=(
                latest_order.get("Delivery date") or latest_order.get("Order date")
            ),
            latest_contact_id=latest_contact.get("contact_id"),
            latest_contact_result=latest_contact.get("result"),
            latest_contact_date=latest_contact.get("date_time"),
        )
        results.append({
            "decision_context_hash": context_hash,
            "customer_id": customer_id,
            "customer_key": _text(customer.get("customer_number")) or customer_name,
            "customer_row": customer.get("row") or "",
            "customer": _text(customer.get("customer")),
            "priority_score": max(0, 90 - position * 5),
            "reason_code": "phase1_test",
            "reason_text": "Följ upp kunden",
            "primary_trigger_type": "phase1_test",
            "primary_trigger_key": "phase1_test",
            "covered_trigger_keys": ["phase1_test"],
            "lifecycle": "phase1_test",
            "score_version": "phase1",
        })
    return results
