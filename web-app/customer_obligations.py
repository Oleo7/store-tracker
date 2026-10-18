"""Pure, source-scoped customer commitments and explicit state events.

Reading this module never writes or infers fulfillment from another contact,
an order, or an unrelated booking. Legacy completion with insufficient evidence
is reviewable history; it is never an automatic instruction to call a customer.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import hashlib
import json
from zoneinfo import ZoneInfo

STOCKHOLM = ZoneInfo("Europe/Stockholm")
STATE_EVENT_COLUMNS = [
    "event_id", "kind", "entity_id", "customer_id", "owner_user_name",
    "actor_user_name", "revision", "occurred_at", "client_request_id",
    "fingerprint", "payload_json", "recorded_at",
]
OPEN_STATUSES = {"open", "scheduled", "needs_review"}
CLOSED_STATUSES = {"resolved", "cancelled"}
REFUSALS = {"återkom ej", "kontakta ej", "ring ej"}
ALLOWANCES = {"kontakt tillåten"}
UNREACHED = {"ej anträffbar", "unreachable", "not_reached"}


def text(value):
    return str(value or "").strip()


def key(value):
    return " ".join(text(value).casefold().split())


def instant(value):
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        raw = text(value)
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            for fmt in ("%Y/%m/%d", "%d/%m/%Y", "%d.%m.%Y"):
                try:
                    parsed = datetime.strptime(raw, fmt)
                    break
                except ValueError:
                    continue
            else:
                from priority import parse_datetime
                parsed = parse_datetime(raw)
                if parsed is None:
                    return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=STOCKHOLM)
    return parsed.astimezone(STOCKHOLM)


def day(value):
    parsed = instant(value)
    return parsed.date() if parsed else None


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def source_fingerprint(row):
    """Exclude mutable follow_up_date and spreadsheet position from identity."""
    return fingerprint({name: text(row.get(name)) for name in (
        "customer_id", "date_time", "sales_person", "contact_channel", "comment",
    )})


def stable_contact_source_id(row):
    return text(row.get("contact_id")) or "legacy:" + source_fingerprint(row)[:32]


def obligation_id_for_contact(row):
    return "followup:" + stable_contact_source_id(row)


def obligation_id_for_activity(row):
    if text(row.get("obligation_id")):
        return text(row.get("obligation_id"))
    source = text(row.get("source_contact_id"))
    return ("followup:" + source) if source else "calendar:" + text(row.get("planned_activity_id"))


def _rows(rows):
    return [row[1] if isinstance(row, (tuple, list)) and len(row) == 2
            and isinstance(row[1], dict) else row for row in rows or ()]


def event_payload(event):
    payload = event.get("payload_json") or event.get("payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict):
        raise ValueError("Customer state event payload is not an object")
    return dict(payload)


def event_revision(row):
    try:
        return max(0, int(float(row.get("revision") or 0)))
    except (ValueError, TypeError):
        return 0


def latest_state_events(events, *, as_of):
    current = instant(as_of)
    latest = {}
    for event in _rows(events):
        when = instant(event.get("occurred_at"))
        recorded = instant(event.get("recorded_at") or event.get("created_at"))
        if when is None or when > current or (recorded and recorded > current):
            continue
        identity = (text(event.get("kind")), text(event.get("customer_id")),
                    text(event.get("entity_id")))
        marker = (when, event_revision(event), text(event.get("event_id")))
        if identity not in latest or marker > latest[identity][0]:
            latest[identity] = (marker, event)
    return {identity: value[1] for identity, value in latest.items()}


def classify_legacy_completed_followups(contact_rows, planned_activity_rows, *, as_of):
    """Read-only migration candidates, not a list of proven open promises.

    Exact source/activity links are evidence. Later unlinked dialogue or a new
    booking is reported as uncertainty and cannot resolve the old commitment.
    No contact notes or credentials are included in this report.
    """
    current = instant(as_of)
    contacts = [row for row in _rows(contact_rows)
                if instant(row.get("date_time")) and instant(row.get("date_time")) <= current]
    by_id = {text(row.get("contact_id")): row for row in contacts if text(row.get("contact_id"))}
    plans = _rows(planned_activity_rows)
    result = []
    for plan in plans:
        if key(plan.get("source")) != "follow_up" or key(plan.get("status")) != "completed":
            continue
        attempt = by_id.get(text(plan.get("completed_contact_id")), {})
        if key(attempt.get("result")) not in UNREACHED:
            continue
        source = by_id.get(text(plan.get("source_contact_id")), {})
        if text(source.get("follow_up_date")):
            continue
        cid = text(plan.get("customer_id"))
        oid = obligation_id_for_activity(plan)
        outcome = key(attempt.get("obligation_outcome"))
        linked = [row for row in contacts if text(row.get("obligation_id")) == oid
                  and key(row.get("obligation_outcome")) in CLOSED_STATUSES
                  and key(row.get("result")) not in UNREACHED]
        linked.sort(key=lambda row: (instant(row.get("date_time")), stable_contact_source_id(row)))
        later_plans = [row for row in plans
                       if text(row.get("customer_id")) == cid
                       and text(row.get("planned_activity_id")) != text(plan.get("planned_activity_id"))
                       and obligation_id_for_activity(row) == oid
                       and key(row.get("status")) in {"planned", "cancelled", "superseded"}]
        later_contacts = [row for row in contacts
                          if text(row.get("customer_id")) == cid
                          and instant(row.get("date_time")) > instant(attempt.get("date_time"))]
        classification = "needs_review"
        if linked:
            classification = key(linked[-1].get("obligation_outcome"))
        elif any(key(row.get("status")) == "planned" for row in later_plans):
            classification = "scheduled"
        elif outcome == "open":
            classification = "open"
        result.append({
            "candidate_key": fingerprint([cid, text(plan.get("planned_activity_id")),
                                          text(plan.get("completed_contact_id"))]),
            "customer_id": cid, "obligation_id": oid,
            "planned_activity_id": text(plan.get("planned_activity_id")),
            "source_contact_id": text(plan.get("source_contact_id")),
            "completed_contact_id": text(plan.get("completed_contact_id")),
            "source_fingerprint": source_fingerprint(source) if source else "",
            "planned_activity_fingerprint": fingerprint(plan),
            "attempt_fingerprint": source_fingerprint(attempt),
            "review_classification": classification,
            "evidence": {
                "source_found": bool(source), "source_date_missing": True,
                "explicit_resolution_contact_ids": [text(row.get("contact_id")) for row in linked],
                "linked_replacement_activity_ids": [text(row.get("planned_activity_id")) for row in later_plans],
                "later_unlinked_contact_count": len(later_contacts),
            },
        })
    return sorted(result, key=lambda row: (row["customer_id"], row["planned_activity_id"]))


def build_customer_state(customers, contact_rows, planned_activity_rows, state_events=(), *,
                         as_of, owner_user_names=None):
    # Share the actual model's quarantine and exact unique ID/number/name rules.
    from priority import _canonicalize_rows, _known_at
    current = instant(as_of)
    if current is None:
        raise ValueError("An explicit as_of instant is required")
    masters = {text(row.get("customer_id")): row for row in customers if text(row.get("customer_id"))}
    by_name = defaultdict(list)
    for cid, master in masters.items():
        by_name[key(master.get("customer"))].append(cid)

    def customer_id(row):
        cid = text(row.get("customer_id"))
        if cid:
            return cid if cid in masters else ""
        matches = by_name.get(key(row.get("customer")), ())
        return matches[0] if len(matches) == 1 else ""

    contacts = []
    by_contact = {}
    canonical_contacts = _canonicalize_rows(_rows(contact_rows), customers,
                                            name_field="customer", number_field="customer_number")
    for index, row in enumerate(canonical_contacts, start=2):
        when = instant(row.get("date_time"))
        cid = customer_id(row)
        if (not cid or when is None or when > current or row.get("_identity_unresolved")
                or not _known_at(row, current)):
            continue
        row = {**row, "customer_id": cid, "_source_row": index}
        contacts.append(row)
        by_contact[stable_contact_source_id(row)] = row

    plans = []
    canonical_plans = _canonicalize_rows(_rows(planned_activity_rows), customers,
                                         name_field="customer", number_field="customer_number")
    for row in canonical_plans:
        cid = customer_id(row)
        created = instant(row.get("created_at"))
        if not cid or (created and created > current) or row.get("_identity_unresolved") or not _known_at(row, current):
            continue
        if owner_user_names is not None:
            owner = key(owner_user_names.get(cid))
            if not owner or key(row.get("user_name")) != owner:
                continue
        plans.append({**row, "customer_id": cid})

    commitments = {}

    def make(cid, oid, *, source=None, activity=None):
        source, activity = source or {}, activity or {}
        due = day(source.get("follow_up_date")) or day(activity.get("scheduled_at"))
        explicit = bool(source or text(activity.get("source_contact_id"))
                        or key(activity.get("source")) == "follow_up")
        master = masters[cid]
        owner = ((owner_user_names or {}).get(cid) if owner_user_names is not None
                 else text(master.get("sales_person")))
        result = {
            "obligation_id": oid, "customer_id": cid,
            "customer": text(master.get("customer")), "customer_row": master.get("row"),
            "customer_number": text(master.get("customer_number")),
            "owner_user_name": text(owner), "purpose": "explicit_followup" if explicit else "calendar_activity",
            "source_scope": "obligation:" + oid, "status": "open",
            "source_contact_id": stable_contact_source_id(source) if source else text(activity.get("source_contact_id")),
            "source_contact_row": source.get("_source_row"),
            "source_created_at": text(source.get("date_time")) or text(activity.get("created_at")),
            "agreed_at": text(source.get("date_time")) or text(activity.get("agreed_at")),
            "customer_agreement_confirmed": key(activity.get("appointment_confirmed")) in {"y", "true", "1"},
            "created_at": text(source.get("date_time")) or text(activity.get("created_at")),
            "source_fingerprint": source_fingerprint(source) if source else fingerprint(activity),
            "source_state_fingerprint": fingerprint({name: source.get(name, "") for name in (
                "customer_id", "date_time", "follow_up_date", "contact_id", "result", "comment",
                "contact_channel", "obligation_id", "obligation_outcome", "purpose")}) if source else fingerprint(activity),
            "original_due_date": due.isoformat() if due else "",
            "due_date": due.isoformat() if due else "", "due_at": "",
            "planned_activity_id": "", "original_planned_activity_id": text(activity.get("planned_activity_id")),
            "contact_type": text(activity.get("contact_type")) or text(source.get("contact_channel")),
            "note": text(activity.get("note")) or text(source.get("comment")),
            "appointment_confirmed": activity.get("appointment_confirmed", ""),
            "picking_help": activity.get("picking_help", ""),
            "latest_attempt_at": "", "latest_attempt_result": "", "resolution_event_id": "",
            "snooze_until": "", "revision": 0, "evidence_quality": "linked" if source else "calendar",
            "review_reasons": [], "_plans": [], "_attempts": [],
        }
        if explicit and due and (current.date() - due).days > 90:
            result.update(status="needs_review", evidence_quality="stale_explicit_followup")
            result["review_reasons"].append("stale_explicit_followup")
        return result

    for row in contacts:
        if day(row.get("follow_up_date")):
            oid = obligation_id_for_contact(row)
            commitments[oid] = make(row["customer_id"], oid, source=row)

    for plan in plans:
        aid = text(plan.get("planned_activity_id"))
        if not aid:
            continue
        source = by_contact.get(text(plan.get("source_contact_id")))
        if source and source["customer_id"] != plan["customer_id"]:
            source = None
        oid = text(plan.get("obligation_id")) or (obligation_id_for_contact(source) if source else obligation_id_for_activity(plan))
        # Only an unambiguous exact legacy source match may deduplicate a row.
        if not source and not text(plan.get("source_contact_id")) and key(plan.get("source")) == "follow_up":
            matches = [row for row in contacts if row["customer_id"] == plan["customer_id"]
                       and day(row.get("follow_up_date")) == day(plan.get("scheduled_at"))
                       and key(row.get("comment")) == key(plan.get("note"))]
            if len(matches) == 1:
                source = matches[0]
                oid = obligation_id_for_contact(source)
        if oid not in commitments:
            commitments[oid] = make(plan["customer_id"], oid, source=source, activity=plan)
        commitments[oid]["_plans"].append(plan)
        if not commitments[oid]["original_planned_activity_id"]:
            commitments[oid]["original_planned_activity_id"] = aid

    for contact in contacts:
        oid = text(contact.get("obligation_id"))
        if not oid:
            aid = text(contact.get("planned_activity_id"))
            linked_plan = next((p for p in plans if text(p.get("planned_activity_id")) == aid), None) if aid else None
            if linked_plan:
                oid = next((identity for identity, item in commitments.items()
                            if any(text(p.get("planned_activity_id")) == aid for p in item["_plans"])), "")
        if oid in commitments and commitments[oid]["customer_id"] == contact["customer_id"]:
            commitments[oid]["_attempts"].append(contact)

    # The former CRM stored next-contact dates without obligation identities.
    # Earlier members of that sequence remain reviewable evidence, rather than
    # each becoming a newly asserted sales promise. Never infer fulfillment.
    latest_legacy_date = {}
    for contact in contacts:
        if not day(contact.get("follow_up_date")) or text(contact.get("contact_request_fingerprint")) or text(contact.get("purpose")):
            continue
        marker = (instant(contact.get("date_time")), contact.get("_source_row", 0), stable_contact_source_id(contact))
        cid = contact["customer_id"]
        if cid not in latest_legacy_date or marker > latest_legacy_date[cid]:
            latest_legacy_date[cid] = marker
    for contact in contacts:
        oid = obligation_id_for_contact(contact)
        if oid not in commitments or text(contact.get("contact_request_fingerprint")) or text(contact.get("purpose")):
            continue
        marker = (instant(contact.get("date_time")), contact.get("_source_row", 0), stable_contact_source_id(contact))
        if marker < latest_legacy_date.get(contact["customer_id"], marker):
            commitments[oid].update(status="needs_review", evidence_quality="legacy_sequence_uncertain")
            commitments[oid]["review_reasons"].append("separate_historical_scope_not_verified")

    for item in commitments.values():
        linked_plans = sorted(item.pop("_plans"), key=lambda row: (
            instant(row.get("updated_at")) or instant(row.get("created_at")) or datetime(1900, 1, 1, tzinfo=STOCKHOLM),
            event_revision(row), text(row.get("planned_activity_id"))))
        active = [p for p in linked_plans if key(p.get("status") or "planned") == "planned"]
        attempts = sorted(item.pop("_attempts"), key=lambda row: (
            instant(row.get("date_time")), row.get("_source_row", 0), stable_contact_source_id(row)))
        item["source_revision"] = max([event_revision(row) for row in linked_plans] or [0])
        item["planning_state_fingerprint"] = fingerprint([{name: row.get(name, "") for name in (
            "planned_activity_id", "customer_id", "source_contact_id", "obligation_id", "scheduled_at", "status",
            "revision", "completed_contact_id", "note", "contact_type", "appointment_confirmed", "picking_help")}
            for row in linked_plans])
        item["revision"] = item["source_revision"]
        source_times = [instant(row.get("updated_at")) or instant(row.get("created_at")) for row in linked_plans]
        source_times.extend(instant(row.get("date_time")) for row in attempts)
        item["derived_at"] = max([when for when in source_times if when] or
                                 [datetime(1900, 1, 1, tzinfo=STOCKHOLM)]).isoformat()
        if attempts:
            latest = attempts[-1]
            item.update(latest_attempt_at=text(latest.get("date_time")), latest_attempt_result=text(latest.get("result")))
            outcome = key(latest.get("obligation_outcome"))
            if key(latest.get("result")) in UNREACHED:
                outcome = "open" if outcome else "needs_review"
            if outcome in CLOSED_STATUSES:
                item.update(status=outcome, resolution_event_id=text(latest.get("contact_id")))
            elif outcome == "open":
                item.update(status="open", evidence_quality="explicit_open")
            elif item["purpose"] == "explicit_followup" and any(key(p.get("status")) == "completed" for p in linked_plans):
                linked_completion = next((p for p in linked_plans
                                          if key(p.get("status")) == "completed"
                                          and text(p.get("completed_contact_id")) == text(latest.get("contact_id"))
                                          and text(p.get("planned_activity_id")) == text(latest.get("planned_activity_id"))
                                          and text(p.get("source_contact_id")) == item["source_contact_id"]), None)
                if not outcome and linked_completion and key(latest.get("result")) in {"positiv", "neutral", "negativ"}:
                    item.update(status="resolved", evidence_quality="legacy_linked_completion",
                                resolution_event_id=text(latest.get("contact_id")))
                else:
                    item.update(status="needs_review", evidence_quality="legacy_completion_uncertain")
                    item["review_reasons"].append("legacy_completion_does_not_prove_fulfillment")
        if active and item["status"] not in CLOSED_STATUSES:
            selected = active[-1]
            due = instant(selected.get("scheduled_at"))
            item.update(status="scheduled", planned_activity_id=text(selected.get("planned_activity_id")),
                        due_date=due.date().isoformat() if due else item["due_date"],
                        due_at=due.isoformat() if due else "", contact_type=text(selected.get("contact_type")),
                        note=text(selected.get("note")) or item["note"],
                        appointment_confirmed=selected.get("appointment_confirmed", ""),
                        picking_help=selected.get("picking_help", ""))
        elif linked_plans and item["status"] not in CLOSED_STATUSES:
            last = linked_plans[-1]
            last_status = key(last.get("status"))
            if last_status in {"cancelled", "superseded"}:
                if item["purpose"] == "calendar_activity":
                    item.update(status="cancelled", resolution_event_id=text(last.get("planned_activity_id")))
                else:
                    item.update(status="needs_review", evidence_quality="calendar_closed_promise_unresolved")
                    item["review_reasons"].append("calendar_closed_without_promise_outcome")
            elif last_status == "completed" and item["purpose"] == "calendar_activity":
                item.update(status="resolved", resolution_event_id=text(last.get("completed_contact_id")))
            elif last_status == "completed" and not attempts:
                item.update(status="needs_review", evidence_quality="missing_completion_contact")

    permissions = {}
    for row in sorted(contacts, key=lambda row: (instant(row.get("date_time")), row.get("_source_row", 0), stable_contact_source_id(row))):
        result = key(row.get("result"))
        if result in REFUSALS | ALLOWANCES:
            permissions[row["customer_id"]] = {
                "status": "do_not_contact" if result in REFUSALS else "allowed",
                "event_id": stable_contact_source_id(row), "source_contact_id": stable_contact_source_id(row),
                "occurred_at": instant(row.get("date_time")).isoformat(),
                "updated_at": instant(row.get("date_time")).isoformat(), "revision": 0,
                "reason": text(row.get("comment")), "actor_user_name": text(row.get("sales_user_name")) or text(row.get("sales_person")),
            }
    workflows = defaultdict(list)
    for (kind, cid, entity_id), event in latest_state_events(state_events, as_of=current).items():
        if cid not in masters:
            continue
        payload = event_payload(event)
        payload.update(revision=event_revision(event), updated_at=text(event.get("occurred_at")),
                       event_id=text(event.get("event_id")))
        if kind == "obligation":
            item = commitments.get(entity_id)
            if item is None:
                # An explicit saved snapshot may survive a source-row edit.
                item = make(cid, entity_id)
                commitments[entity_id] = item
            if item["customer_id"] != cid:
                continue
            derived = dict(item)
            item.update(payload)
            item["source_state_fingerprint"] = derived.get("source_state_fingerprint", "")
            item["planning_state_fingerprint"] = derived.get("planning_state_fingerprint", "")
            item["revision"] = event_revision(event) + derived.get("source_revision", 0)
            # Later source-scoped contact outcomes or replacements cannot be
            # overwritten by an earlier saved scheduled snapshot.
            if (instant(derived.get("derived_at")) > instant(event.get("occurred_at"))
                    and payload.get("status") == "scheduled"):
                for field in ("status", "planned_activity_id", "due_date", "due_at", "latest_attempt_at",
                              "latest_attempt_result", "resolution_event_id", "evidence_quality"):
                    item[field] = derived.get(field, "")
            item.update(customer_id=cid, obligation_id=entity_id,
                        owner_user_name=text((owner_user_names or {}).get(cid)) if owner_user_names is not None else item["owner_user_name"])
            # Calendar editor changes remain authoritative for the same link.
            if item["status"] == "scheduled":
                linked = next((p for p in plans if text(p.get("planned_activity_id")) == item.get("planned_activity_id")), None)
                if linked and key(linked.get("status")) == "planned":
                    scheduled = instant(linked.get("scheduled_at"))
                    item.update(due_at=scheduled.isoformat() if scheduled else "", due_date=scheduled.date().isoformat() if scheduled else item["due_date"],
                                contact_type=text(linked.get("contact_type")), note=text(linked.get("note")))
                elif linked and key(linked.get("status")) in {"cancelled", "superseded"}:
                    item.update(status="needs_review" if item["purpose"] == "explicit_followup" else "cancelled",
                                evidence_quality="calendar_closed_promise_unresolved")
                elif linked and key(linked.get("status")) == "completed":
                    # A saved scheduled snapshot cannot resurrect this exact
                    # completed attempt, including writes in the same second.
                    # Only a new linked activity can schedule the promise again.
                    for field in ("status", "planned_activity_id", "due_date", "due_at", "latest_attempt_at",
                                  "latest_attempt_result", "resolution_event_id", "evidence_quality"):
                        item[field] = derived.get(field, "")
        elif kind == "permission":
            previous = permissions.get(cid)
            if previous is None or instant(event.get("occurred_at")) >= instant(previous.get("occurred_at")):
                payload["occurred_at"] = text(event.get("occurred_at"))
                permissions[cid] = payload
        elif kind == "workflow":
            workflows[cid].append(payload)

    for permission in permissions.values():
        permission["revision"] = int(fingerprint({name: permission.get(name, "") for name in (
            "status", "event_id", "source_contact_id", "occurred_at", "reason", "actor_user_name")})[:12], 16)
    grouped = defaultdict(list)
    for item in commitments.values():
        # A safe JS integer acts as an optimistic revision for both materialized
        # and virtual obligations. Editing a legacy date in Sheets changes it.
        item["revision"] = int(fingerprint({name: item.get(name, "") for name in (
            "customer_id", "owner_user_name", "source_state_fingerprint", "planning_state_fingerprint",
            "status", "due_date", "due_at", "original_due_date", "planned_activity_id", "source_contact_id",
            "note", "contact_type", "snooze_until", "latest_attempt_at", "latest_attempt_result",
            "resolution_event_id", "event_id", "updated_at")})[:12], 16)
        grouped[item["customer_id"]].append(item)
    for items in grouped.values():
        items.sort(key=lambda row: (row["due_date"], row["obligation_id"]))
    return {"obligations": dict(grouped), "contact_permissions": permissions,
            "workflows": dict(workflows), "as_of": current.isoformat()}


def attention_items(state, *, as_of, owner_user_name=None):
    """Only previous calendar dates and previous explicit promise dates."""
    current = instant(as_of)
    items = []
    for obligations in state.get("obligations", {}).values():
        for item in obligations:
            if item.get("status") not in OPEN_STATUSES:
                continue
            if owner_user_name is not None and key(item.get("owner_user_name")) != key(owner_user_name):
                continue
            due = day(item.get("due_date"))
            snooze = instant(item.get("snooze_until"))
            if due is None or due >= current.date() or (snooze and snooze > current):
                continue
            if item.get("planned_activity_id") and item.get("status") == "scheduled":
                item_type = "missed_calendar_activity"
            elif item.get("purpose") == "explicit_followup":
                item_type = "overdue_explicit_follow_up"
            else:
                continue
            items.append({**item, "item_type": item_type,
                          "days_overdue": (current.date() - due).days,
                          "review_only": item.get("status") == "needs_review",
                          "actions": ["resolve", "cancel", "reschedule", "snooze", "restore"]})
    return sorted(items, key=lambda row: (row["due_date"], key(row.get("customer")), row["obligation_id"]))
