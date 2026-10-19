"""Pure, source-scoped customer commitments and explicit state events.

Reading never writes. Customer promises need source-scoped completion. Legacy
internal reminders are scheduling cues, not proof of a customer promise: later
work can supersede them without claiming that an unrelated promise was fulfilled.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import hashlib
import json
import re
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


def contact_anchor(row):
    """Immutable-enough legacy anchor; editable notes/owner/result are not IDs.

    A collision is quarantined by build_customer_state, never disambiguated by
    spreadsheet position. Correcting an event timestamp still needs an explicit
    source mapping; only persisted IDs can survive arbitrary identity edits.
    """
    when = instant(row.get("date_time") or row.get("source_created_at"))
    channel = key(row.get("contact_channel") or row.get("contact_type"))
    channel = {"telefon": "phone", "mejl": "email", "bes\u00f6k": "visit", "besok": "visit"}.get(channel, channel)
    return fingerprint([text(row.get("customer_id")), when.isoformat() if when else "", channel])


def stable_contact_source_id(row):
    return (text(row.get("_stable_source_id")) or text(row.get("contact_id"))
            or "legacy2:" + contact_anchor(row)[:32])


def confirmed(value):
    return key(value) in {"true", "y", "yes", "1", "ja"}


def commitment_evidence(source, activity):
    """A date is a reminder. Confirmation/purpose is independent evidence.

    Text clues only preserve a possible separate service task for handling.
    They never authorize contact, prove agreement, or auto-resolve a promise.
    """
    agreed = any(confirmed(row.get(field)) for row, field in (
        (source, "follow_up_agreement_confirmed"),
        (activity, "customer_agreement_confirmed"), (activity, "appointment_confirmed")))
    purpose = key(source.get("follow_up_purpose") or activity.get("purpose"))
    explicit = purpose in {"customer_followup", "explicit_followup", "service_followup", "service"}
    # Old persisted source-scoped promises remain promises, but do not gain an
    # exception to a negative dialogue merely from the word 'explicit'.
    explicit = explicit or not source.get("follow_up_purpose") and key(source.get("purpose")) in {"customer_followup", "service_followup", "explicit_followup"}
    note = key(activity.get("note") or source.get("comment"))
    service_hint = bool(re.search(r"reklamation|krediter|ers\u00e4ttningsleverans|leveransproblem|utlovad|lovat kunden", note)
        or re.search(r"(?:bokad|bokat|bekr\u00e4ftad|bekr\u00e4ftat|kommit \u00f6verens|vi ska|vi kommer|ska k\u00f6ra).{0,90}(?:demo|provsmakning)", note)
        or re.search(r"(?:demo|provsmakning).{0,45}(?:bokad|bokat|bekr\u00e4ftad|bekr\u00e4ftat)", note))
    if agreed or explicit:
        return "customer_promise", agreed
    if service_hint:
        return "uncertain_service", False
    if source or text(activity.get("source_contact_id")) or key(activity.get("source")) == "follow_up":
        return "internal_reminder", False
    return "planned_task", False


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
                         as_of, owner_user_names=None, order_rows=()):
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
    saved_events = latest_state_events(state_events, as_of=current)
    # A previous v3.3 event may refer to the former note-based legacy key. Its
    # saved immutable source time bridges that key to the unique current row.
    saved_anchors = defaultdict(set)
    for (kind, cid, entity_id), event in saved_events.items():
        if kind == "obligation" and entity_id.startswith("followup:legacy"):
            payload = event_payload(event)
            source_time = instant(payload.get("source_created_at") or payload.get("agreed_at") or payload.get("created_at"))
            if source_time:
                saved_anchors[(cid, source_time.isoformat())].add(entity_id[len("followup:"):])
    canonical_contacts = _canonicalize_rows(_rows(contact_rows), customers,
                                            name_field="customer", number_field="customer_number")
    anchor_counts = defaultdict(int)
    event_counts = defaultdict(int)
    for row in canonical_contacts:
        when = instant(row.get("date_time"))
        if when and when <= current and _known_at(row, current):
            event_counts[(customer_id(row), when.isoformat())] += 1
            if not text(row.get("contact_id")):
                anchor_counts[contact_anchor(row)] += 1
    for index, row in enumerate(canonical_contacts, start=2):
        when = instant(row.get("date_time"))
        cid = customer_id(row)
        if (not cid or when is None or when > current or row.get("_identity_unresolved")
                or not _known_at(row, current)):
            continue
        row = {**row, "customer_id": cid, "_source_row": index}
        aliases = saved_anchors.get((cid, when.isoformat()), set())
        if len(aliases) == 1 and event_counts[(cid, when.isoformat())] == 1:
            row["_stable_source_id"] = next(iter(aliases))
        row["_identity_ambiguous"] = bool(not text(row.get("contact_id")) and anchor_counts[contact_anchor(row)] > 1)
        contacts.append(row)
        for alias in {stable_contact_source_id(row), text(row.get("contact_id")),
                      "legacy:" + source_fingerprint(row)[:32], "legacy2:" + contact_anchor(row)[:32]} - {""}:
            by_contact[alias] = row

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
        kind, agreed = commitment_evidence(source, activity)
        explicit = kind in {"customer_promise", "uncertain_service"}
        master = masters[cid]
        owner = ((owner_user_names or {}).get(cid) if owner_user_names is not None
                 else text(master.get("sales_person")))
        result = {
            "obligation_id": oid, "customer_id": cid,
            "customer": text(master.get("customer")), "customer_row": master.get("row"),
            "customer_number": text(master.get("customer_number")),
            "owner_user_name": text(owner),
            "purpose": "explicit_followup" if explicit else "internal_followup" if kind == "internal_reminder" else "calendar_activity",
            "commitment_kind": kind, "identity_ambiguous": bool(source.get("_identity_ambiguous")),
            "source_scope": "obligation:" + oid, "status": "open",
            "source_contact_id": stable_contact_source_id(source) if source else text(activity.get("source_contact_id")),
            "source_contact_row": source.get("_source_row"),
            "source_created_at": text(source.get("date_time")) or text(activity.get("created_at")),
            "agreed_at": text(activity.get("agreed_at")) or (text(source.get("date_time")) if agreed else ""),
            "customer_agreement_confirmed": agreed,
            "created_at": text(source.get("date_time")) or text(activity.get("created_at")),
            "source_fingerprint": source_fingerprint(source) if source else fingerprint(activity),
            "source_state_fingerprint": fingerprint({name: source.get(name, "") for name in (
                "customer_id", "date_time", "follow_up_date", "contact_id", "result", "comment",
                "contact_channel", "obligation_id", "obligation_outcome", "purpose",
                "follow_up_purpose", "follow_up_agreement_confirmed")}) if source else fingerprint(activity),
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
        if kind == "uncertain_service":
            result.update(status="needs_review", evidence_quality="possible_separate_service_task")
            result["review_reasons"].append("service_scope_not_documented")
        if result["identity_ambiguous"]:
            result.update(evidence_quality="ambiguous_legacy_identity")
            result["review_reasons"].append("duplicate_legacy_source_anchor")
            if explicit:
                result["status"] = "needs_review"
        return result

    for row in contacts:
        if day(row.get("follow_up_date")):
            oid = obligation_id_for_contact(row)
            candidate = make(row["customer_id"], oid, source=row)
            variant = {field: row.get(field, "") for field in (
                "date_time", "contact_channel", "comment", "follow_up_date", "result", "contact_id")}
            if oid in commitments:
                # Preserve every ambiguous source variant; no note/position-based
                # suffix may become a fake persistent identity. Mutations refuse
                # the group until an explicit source ID is assigned.
                previous = commitments[oid]
                previous["source_variants"].append(variant)
                previous["source_variants"].sort(key=fingerprint)
                previous["source_count"] = len(previous["source_variants"])
                previous["identity_ambiguous"] = True
                previous["source_state_fingerprint"] = fingerprint(previous["source_variants"])
                previous["evidence_quality"] = "ambiguous_legacy_identity"
                previous["review_reasons"] = ["duplicate_legacy_source_anchor"]
                if candidate["purpose"] == "explicit_followup" or previous["purpose"] == "explicit_followup":
                    previous.update(purpose="explicit_followup", status="needs_review", commitment_kind="uncertain_service")
                previous["customer_agreement_confirmed"] = False
            else:
                candidate.update(source_variants=[variant], source_count=1)
                commitments[oid] = candidate

    plan_obligation_ids = {}
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
        item = commitments[oid]
        kind, agreed = commitment_evidence(source or {}, plan)
        if kind == "customer_promise" and not item.get("identity_ambiguous"):
            item.update(purpose="explicit_followup", commitment_kind=kind)
        if agreed and not item.get("identity_ambiguous"):
            item["customer_agreement_confirmed"] = True
            # Moving an internal appointment is not a new customer agreement.
            # Only the booking's own confirmation may advance agreed_at.
            if confirmed(plan.get("customer_agreement_confirmed")) or confirmed(plan.get("appointment_confirmed")):
                item["agreed_at"] = text(plan.get("agreed_at")) or text(plan.get("created_at")) or item.get("source_created_at", "")
        item["_plans"].append(plan)
        plan_obligation_ids[aid] = oid
        if not commitments[oid]["original_planned_activity_id"]:
            commitments[oid]["original_planned_activity_id"] = aid

    for contact in contacts:
        oid = text(contact.get("obligation_id"))
        if not oid:
            aid = text(contact.get("planned_activity_id"))
            oid = plan_obligation_ids.get(aid, "") if aid else ""
        if oid in commitments and commitments[oid]["customer_id"] == contact["customer_id"]:
            commitments[oid]["_attempts"].append(contact)

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
            if last_status in {"cancelled", "skipped", "superseded"}:
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
    for (kind, cid, entity_id), event in saved_events.items():
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
                elif linked and key(linked.get("status")) in {"cancelled", "skipped", "superseded"}:
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

    reconcile_internal_reminders(commitments, contacts, plans, saved_events,
                                 customers=customers, order_rows=order_rows, as_of=current)

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
            "resolution_event_id", "event_id", "updated_at", "commitment_kind",
            "customer_agreement_confirmed", "identity_ambiguous", "resolution_basis")})[:12], 16)
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
    return sorted(items, key=lambda row: (bool(row.get("review_only")), row["due_date"],
                                          key(row.get("customer")), row["obligation_id"]))


def reconcile_internal_reminders(commitments, contacts, plans, saved_events, *, customers, order_rows, as_of):
    """Read projection of superseded sales reminders; never a CRM mutation.

    A linked active calendar task is retained. Separate customer promises and
    uncertain service tasks never close from customer-wide evidence. Ordinary
    historical dates can be replaced by a newer scheduling decision, reached
    dialogue after their due date, or a later real commercial order. Every
    decision keeps its source and the superseding evidence ID/date.
    """
    from priority import build_order_features, is_human_contact, normalize_contact_result
    by_customer = defaultdict(list)
    plans_by_customer = defaultdict(list)
    for row in contacts:
        by_customer[row["customer_id"]].append(row)
    for row in plans:
        plans_by_customer[row["customer_id"]].append(row)
    last_orders = {}
    if order_rows:
        for feature in build_order_features(order_rows, customers=customers, as_of=as_of).values():
            cid = text(feature.get("customer_id"))
            when = feature.get("last_order_date")
            if cid and when:
                last_orders[cid] = (when, text(feature.get("latest_order_reference")))
    for item in commitments.values():
        kind = item.get("commitment_kind")
        if kind not in {"internal_reminder", "planned_task"} or item.get("identity_ambiguous"):
            continue
        if item.get("customer_agreement_confirmed") or item.get("status") in CLOSED_STATUSES:
            continue
        # A saved source-scoped outcome/snooze is deliberate user work. It is
        # not an old implicit date and must not be replaced by an inference.
        if ("obligation", item["customer_id"], item["obligation_id"]) in saved_events:
            continue
        source_at = instant(item.get("source_created_at") or item.get("created_at"))
        due = day(item.get("due_date"))
        if source_at is None or due is None:
            continue
        cid = item["customer_id"]
        linked = [p for p in plans_by_customer[cid] if
                  text(p.get("planned_activity_id")) in {item.get("planned_activity_id"), item.get("original_planned_activity_id")}
                  or text(p.get("source_contact_id")) == item.get("source_contact_id") and item.get("source_contact_id")]
        # Active calendar rows retain their exact task/date. Only truly missed
        # unconfirmed internal work can be superseded by reached later work.
        active = [p for p in linked if key(p.get("status") or "planned") == "planned"]
        if active:
            if due >= as_of.date():
                continue
        elif linked:
            latest = max(linked, key=lambda p: instant(p.get("updated_at") or p.get("created_at")) or source_at)
            if key(latest.get("status")) in {"cancelled", "superseded", "skipped"}:
                item.update(status="cancelled", resolution_basis="internal_activity_closed",
                            resolution_event_id=text(latest.get("planned_activity_id")))
                continue
            if item.get("status") == "needs_review":
                # An unsuccessful attempt leaves a retry cue, not a claim that
                # a distinct promise remains unfulfilled.
                item.update(status="open", evidence_quality="internal_retry_cue", review_reasons=[])
        candidates = []
        for c in by_customer[cid]:
            when = instant(c.get("date_time"))
            if when is None or when <= source_at or not is_human_contact(c):
                continue
            result = normalize_contact_result(c.get("result"))
            if day(c.get("follow_up_date")) and kind == "internal_reminder":
                candidates.append((when, "internal_schedule_replaced", stable_contact_source_id(c)))
            elif result in {"Positiv", "Neutral", "Negativ"} and when.date() >= due:
                candidates.append((when, "internal_followup_handled", stable_contact_source_id(c)))
        for plan in plans_by_customer[cid]:
            created = instant(plan.get("created_at"))
            scheduled = instant(plan.get("scheduled_at"))
            if (kind == "internal_reminder" and not active and created and created > source_at
                    and scheduled and scheduled.date() >= due and key(plan.get("status") or "planned") == "planned"
                    and text(plan.get("planned_activity_id")) != item.get("original_planned_activity_id")):
                candidates.append((created, "internal_reminder_covered_by_plan", text(plan.get("planned_activity_id"))))
        order = last_orders.get(cid)
        if order and order[0] > source_at.date() and (kind == "internal_reminder" or order[0] >= due):
            candidates.append((instant(order[0]), "internal_sales_goal_replaced_by_order", order[1]))
        if candidates:
            when, basis, event_id = max(candidates)
            item.update(status="cancelled" if "replaced" in basis or "covered" in basis else "resolved",
                        resolution_basis=basis, resolution_event_id=event_id,
                        resolution_at=when.isoformat(), evidence_quality="historical_scheduling_evidence", review_reasons=[])
        elif item.get("status") == "needs_review":
            item.update(status="open", evidence_quality="legacy_internal_reminder", review_reasons=[])
