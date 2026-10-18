"""V3.3 preview: independent business potential and contact urgency.

DFP-based opportunity indices, NOT calibrated probabilities, SEK forecasts
or causal sales uplift. Keep legacy priority_score for planning and routing.
Constants are centralized for a future validated admin-adjustable policy.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
from statistics import median
import math
import unicodedata

VERSION = "v3.3-preview"
POLICY_VERSION = "2026-10-reviewed"
POLICY = {
    "shrinkage_purchases": 3.0, "horizon_days": 90,
    "realization_prospect": 0.50, "realization_first_order": 0.70,
    "realization_repeat": 0.80, "first_order_frequency": 1.20,
    # All parameters are provisional until portfolio replay and backtesting.
    # Provisional anchor: A-group median 21 DFP x prospect factor 0.50
    # maps to 80/100 after 90d (10.5/(10.5+2.625)=0.8).
    # Does NOT imply an 80% purchase chance; must be portfolio-calibrated.
    "maximum_repeat_frequency": 6.0, "score_scale_dfp": 2.625,
    "unknown_dialogue_factor": 0.70, "retry_business_days": 2,
    "stale_repeat_after_cycles": 3.0, "stale_repeat_factor": 0.75,
    "very_stale_repeat_after_cycles": 6.0, "very_stale_repeat_factor": 0.60,
    "stale_first_order_after_days": 180, "stale_first_order_factor": 0.70,
    # Segment + store-format medians are shrunk toward the segment median.
    "format_prior_min_buyers": 8, "format_prior_strength": 12.0,
    "stale_followup_review_days": 90,
    "stale_followup_order_review_days": 60,
    # Explicit operating assumptions, not empirically calibrated chances.
    "positive_prospect_followup_min_days": 7,
    "positive_buyer_followup_min_days": 10,
    "reported_order_sync_business_days": 2,
    "reactivation_dialogue_gap_days": 30,
    "same_purpose_dialogue_gap_days": 3,
    "positive_buyer_followup_max_days": 45,
    "material_reorder_overdue_days": 14,
    "material_reorder_min_dialogue_days": 7,
    "recent_order_hold_days": 14,
    "unconfirmed_order_review_days": 60,
    "negative_cooldown_days": 60,
    "negative_60_to_90_factor": 0.35,
    "negative_90_to_180_factor": 0.55,
    "negative_after_180_factor": 0.75,
}
FALLBACK_DFP = {"A": 21.0, "B": 12.0, "C": 9.0}
CONTACT_CURVE = ((0, 0.0), (7, 0.02), (20, 0.15), (30, 0.35),
                 (40, 0.55), (50, 0.75), (60, 0.90), (90, 1.0))
# A genuine positive dialogue with no later order is a stronger reason to
# follow up, but it does not make a call appropriate immediately afterwards.
# Both curves converge after 90 days. Provisional operating rule (not an
# empirically estimated conversion probability).
POSITIVE_PROSPECT_CURVE = (
    (0, 0.0), (7, 0.02), (14, 0.15), (20, 0.45), (30, 0.75),
    (40, 0.85), (50, 0.92), (60, 0.95), (90, 1.0),
)
# A buying customer's positive dialogue without a subsequent order is a
# warmer lead, not proof that an immediate extra call is justified.
POSITIVE_BUYER_CURVE = (
    (0, 0.0), (7, 0.02), (10, 0.18), (14, 0.40),
    (20, 0.65), (30, 0.85), (45, 0.95), (60, 0.98), (90, 1.0),
)
# 'Order lagd' can represent an order logged without speaking to the buyer.
# It does not prove a human dialogue. Explicit refusals count as dialogue
# but also create a separate persistent no-contact safety guard.
REACHED_RESULTS = frozenset({"Positiv", "Neutral", "Negativ"})
DO_NOT_CONTACT_RESULTS = frozenset({"återkom ej", "kontakta ej", "ring ej"})


def store_format(name: str) -> str:
    """Recognized grocery-store formats; not a replacement for CRM segment."""
    label = unicodedata.normalize("NFKD", str(name or "").casefold())
    label = "".join(c for c in label if not unicodedata.combining(c))
    for pattern, kind in (
        ("ica maxi", "ica_maxi"),
        ("maxi ica", "ica_maxi"),
        ("ica kvantum", "ica_kvantum"),
        ("ica supermarket", "ica_supermarket"),
        ("ica nara", "ica_nara"),
        ("stora coop", "stora_coop"),
        ("coop", "coop"),
        ("hemkop", "hemkop"),
    ):
        if pattern in label:
            return kind
    return ""


def empirical_format_prior(segment_prior_dfp: float, values: list[float],
                           policy: dict = POLICY) -> float:
    """Shrunk within-segment empirical median; no arbitrary segment bonus."""
    cleaned = [float(v) for v in values if v is not None
               and math.isfinite(float(v)) and float(v) > 0]
    if len(cleaned) < int(policy["format_prior_min_buyers"]):
        return float(segment_prior_dfp)
    strength = float(policy["format_prior_strength"])
    share = len(cleaned) / (len(cleaned) + strength)
    return ((1 - share) * float(segment_prior_dfp)
            + share * float(median(cleaned)))


def complete_deliveries(feature: dict, today: date) -> list[float]:
    """Only known positive DFP on completed commercial delivery dates."""
    result = []
    source = (feature.get("confirmed_delivery_dfp_by_date", {})
              if "confirmed_delivery_dfp_by_date" in feature
              else feature.get("delivery_dfp_by_date", {}))
    for day, amount in (source or {}).items():
        try:
            delivery_date = date.fromisoformat(str(day)[:10])
            value = float(amount)
        except (ValueError, TypeError):
            continue
        if delivery_date <= today and math.isfinite(value) and value > 0:
            result.append(value)
    return result


def segment_prior(values: list[float], segment: str, all_values: list[float]) -> float:
    if values:
        return float(median(values))
    if all_values:
        return float(median(all_values))
    return FALLBACK_DFP.get(segment, 9.0)


def validate_policy(policy: dict) -> None:
    """Reject invalid policy, even while admin editing is intentionally disabled."""
    positives = ("shrinkage_purchases", "horizon_days", "score_scale_dfp",
                 "retry_business_days", "maximum_repeat_frequency",
                 "format_prior_min_buyers", "format_prior_strength",
                 "stale_followup_review_days",
                 "stale_followup_order_review_days",
                 "positive_buyer_followup_min_days", "positive_buyer_followup_max_days",
                 "positive_prospect_followup_min_days", "reported_order_sync_business_days",
                 "reactivation_dialogue_gap_days", "same_purpose_dialogue_gap_days",
                 "material_reorder_overdue_days", "material_reorder_min_dialogue_days",
                 "recent_order_hold_days", "unconfirmed_order_review_days",
                 "negative_cooldown_days")
    for key in positives:
        value = float(policy[key])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid model parameter {key}")
    for key in ("realization_prospect", "realization_first_order",
                "realization_repeat", "unknown_dialogue_factor",
                "stale_repeat_factor", "very_stale_repeat_factor",
                "stale_first_order_factor", "negative_60_to_90_factor",
                "negative_90_to_180_factor", "negative_after_180_factor"):
        value = float(policy[key])
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"Invalid model factor {key}")
    if float(policy["very_stale_repeat_after_cycles"]) <= float(policy["stale_repeat_after_cycles"]):
        raise ValueError("Staleness bands must increase")
    if int(policy["stale_followup_order_review_days"]) > int(policy["stale_followup_review_days"]):
        raise ValueError("Stale follow-up review thresholds must increase")
    if int(policy["positive_buyer_followup_min_days"]) > int(policy["positive_buyer_followup_max_days"]):
        raise ValueError("Positive buyer follow-up window must increase")
    if int(policy["unconfirmed_order_review_days"]) <= int(policy["recent_order_hold_days"]):
        raise ValueError("Unconfirmed order review must follow recent order hold")
    if not (policy["negative_60_to_90_factor"] <= policy["negative_90_to_180_factor"] <= policy["negative_after_180_factor"]):
        raise ValueError("Negative-contact penalty must decrease with age")


validate_policy(POLICY)


def display_score(raw: float, policy: dict = POLICY) -> int:
    """Monotonic index; sort by raw values to preserve large opportunities."""
    raw = float(raw or 0)
    if not math.isfinite(raw):
        raise ValueError("Non-finite opportunity")
    raw = max(0.0, raw)
    scale = float(policy["score_scale_dfp"])
    return round(100.0 * raw / (raw + scale))


def business_opportunity(*, delivery_values: list[float], prior_dfp: float,
                         delivery_count: int, cycle_days: int | None,
                         days_since_delivery: int | None = None,
                         policy: dict = POLICY) -> dict:
    """Shared shrinkage estimate of horizon DFP opportunity for all segments.

    Volumes are confirmed delivery-day totals. The realization multipliers
    are explicit, provisional policy assumptions, NOT calibrated probabilities.
    """
    k = float(policy["shrinkage_purchases"])
    prior = max(0.0, float(prior_dfp or 0))
    mean = sum(delivery_values) / len(delivery_values) if delivery_values else prior
    typical = ((len(delivery_values) * mean + k * prior) /
               (len(delivery_values) + k)) if len(delivery_values) + k else prior
    horizon = float(policy["horizon_days"])
    if delivery_count <= 0:
        frequency = horizon / 90.0
        realization = float(policy["realization_prospect"])
        confidence = "segmentbaserad"
    elif delivery_count == 1:
        frequency = float(policy["first_order_frequency"]) * horizon / 90.0
        realization = float(policy["realization_first_order"])
        confidence = "en tidigare leverans"
        if days_since_delivery is not None and days_since_delivery > int(policy["stale_first_order_after_days"]):
            realization *= float(policy["stale_first_order_factor"])
    else:
        cycle = max(14, int(cycle_days or 45))
        frequency = min(float(policy["maximum_repeat_frequency"]), max(1.0, horizon / cycle))
        realization = float(policy["realization_repeat"])
        confidence = "kundhistorik" if len(delivery_values) >= 3 else "begränsad köphistorik"
        if days_since_delivery is not None:
            elapsed_cycles = max(0, days_since_delivery) / cycle
            if elapsed_cycles > float(policy["very_stale_repeat_after_cycles"]):
                realization *= float(policy["very_stale_repeat_factor"])
            elif elapsed_cycles > float(policy["stale_repeat_after_cycles"]):
                realization *= float(policy["stale_repeat_factor"])
    raw = max(0.0, typical * frequency * realization)
    return {
        "business_value_raw": round(raw, 6),
        "business_value_score": display_score(raw, policy),
        "business_expected_dfp_per_delivery": round(typical, 2),
        "business_deliveries_used": len(delivery_values),
        "business_value_confidence": confidence,
        "business_frequency_estimate": round(frequency, 3),
        "business_realization_factor": round(realization, 4),
    }


def _interpolated_contact_factor(days_since_reached: int, curve: tuple) -> float:
    day = max(0, int(days_since_reached))
    for idx in range(1, len(curve)):
        right_day, right_value = curve[idx]
        if day <= right_day:
            left_day, left_value = curve[idx - 1]
            fraction = (day - left_day) / (right_day - left_day)
            return left_value + fraction * (right_value - left_value)
    return curve[-1][1]


def contact_factor(days_since_reached: int | None,
                   latest_result: str = "", policy: dict = POLICY) -> float:
    if days_since_reached is None:
        # Unknown history is not the same as proven "never reached".
        # Failed attempts never imply successful dialogue or conversion.
        return float(policy["unknown_dialogue_factor"])
    return _interpolated_contact_factor(days_since_reached, CONTACT_CURVE)


def negative_dialogue_factor(age_days: int, policy: dict = POLICY) -> float:
    """Persistent lower priority after a genuine negative reply.

    The first 60 days are handled by a contact cooldown. Afterwards, the
    reduction weakens gradually but never implies a fresh positive dialogue.
    A later order or reached neutral/positive reply clears this signal.
    """
    if age_days < int(policy["negative_cooldown_days"]):
        return 0.0
    if age_days < 90:
        return float(policy["negative_60_to_90_factor"])
    if age_days < 180:
        return float(policy["negative_90_to_180_factor"])
    return float(policy["negative_after_180_factor"])


def business_days_elapsed(start: date | None, end: date) -> int:
    if not start or start >= end:
        return 0
    days, current = 0, start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def first_order_contact_factor(days_since_delivery: int | None, sku_count: int | None) -> float:
    """Reviewed v2.2 phase policy, normalized independently of dialogue recency.

    This is a timing index, not a probability. Unknown SKU is not one SKU.
    """
    day = days_since_delivery
    if day is None or not 7 <= day <= 90:
        return 0.0
    phase = (0.35 if day <= 10 else 0.20 if day <= 23 else 0.75 if day <= 30
             else 0.90 if day <= 45 else 0.80 if day <= 60 else 0.60)
    return min(1.0, phase + (0.08 if sku_count == 1 and day <= 60 else 0.0))


def contact_opportunity(*, business_value_raw: float,
                        days_since_reached: int | None,
                        latest_attempt_date: date | None,
                        today: date, latest_result: str,
                        status: str, trigger: str,
                        positive_prospect_signal: bool = False,
                        positive_buyer_signal: bool = False,
                        negative_dialogue_signal: bool = False,
                        materially_overdue: bool = False,
                        days_since_delivery: int | None = None,
                        first_order_sku_count: int | None = None,
                        decision: dict | None = None,
                        policy: dict = POLICY) -> dict:
    """Score the FINAL decision. Never independently override its availability."""
    bucket = {"overdue_followup": 0, "act_now": 0, "idle": 1,
              "planned": 2, "wait": 3, "data_missing": 4}.get(status, 4)
    relevance = contact_factor(days_since_reached, latest_result, policy)
    first_phase = (trigger in {"first_order_onboarding", "second_purchase_due", "first_order_reorder"}
                   or (decision or {}).get("timing_source") == "first_order_phase")
    if first_phase:
        # Do not multiply this by, or maximize it with, the routine curve.
        relevance = first_order_contact_factor(days_since_delivery, first_order_sku_count)
    else:
        age = days_since_reached
        if (positive_prospect_signal and age is not None
                and int(policy["positive_prospect_followup_min_days"]) <= age <= 45):
            relevance = max(relevance, _interpolated_contact_factor(age, POSITIVE_PROSPECT_CURVE))
        if (positive_buyer_signal and age is not None
                and int(policy["positive_buyer_followup_min_days"]) <= age <= 45):
            relevance = max(relevance, _interpolated_contact_factor(age, POSITIVE_BUYER_CURVE))
        if trigger in {"established_reorder_due", "positive_buyer_followup"} and materially_overdue:
            relevance = max(relevance, 0.75)
        if trigger in {"overdue_explicit_follow_up", "follow_up_due_today", "overdue_planned_activity"}:
            relevance = max(relevance, 0.75)
        if trigger in {"stockfiller_click_followup", "product_sheet_click_followup"}:
            relevance = max(relevance, 0.75)
        if trigger in {"unanswered_retry_due", "unanswered_verify_contact_due"} and (age is None or age >= 30):
            relevance = max(relevance, 0.75)
    if negative_dialogue_signal and not (decision or {}).get("explicit_agreement_overrides_negative"):
        relevance *= negative_dialogue_factor(days_since_reached if days_since_reached is not None else 0, policy)
    eligible = ((decision or {}).get("can_start_unscheduled_contact") if decision is not None
                else status in {"act_now", "overdue_followup"} and trigger != "stale_follow_up_review")
    if not eligible or (decision or {}).get("review_only"):
        relevance = 0.0
        if (decision or {}).get("review_only") and status != "data_missing":
            bucket = 1
    raw = max(0.0, float(business_value_raw or 0) * relevance)
    return {"contact_priority_raw": round(raw, 6),
            "contact_priority_score": display_score(raw, policy),
            "contact_priority_bucket": bucket,
            "contact_relevance_factor": round(relevance, 4),
            "contact_factor": round(relevance, 4)}


def next_business_day_retry(start: date, business_days: int) -> date:
    current, elapsed = start, 0
    while elapsed < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            elapsed += 1
    return current


def _decision_day(value):
    if isinstance(value, datetime):
        return value.astimezone(ZoneInfo("Europe/Stockholm")).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(ZoneInfo("Europe/Stockholm")).date() if parsed.tzinfo else parsed.date()
    except (TypeError, ValueError):
        return None


def _decision_date_label(value, today):
    months = ("jan", "feb", "mar", "apr", "maj", "jun", "jul", "aug", "sep", "okt", "nov", "dec")
    text = f"{value.day} {months[value.month - 1]}"
    return f"{text} {value.year}" if value.year != today.year else text


def _decision_plan_reason(row, scheduled, *, overdue=False):
    moment = _decision_moment(scheduled)
    if moment is None:
        return "Kontakt är planerad"
    local = datetime.fromtimestamp(moment, ZoneInfo("Europe/Stockholm"))
    kind = str(row.get("contact_type") or "").casefold()
    label = {"phone": "Telefon", "telefon": "Telefon", "email": "Mejl", "mejl": "Mejl",
             "visit": "Besök", "besök": "Besök", "besok": "Besök"}.get(kind, "Kontakt")
    return f"{label} {'skulle ha genomförts' if overdue else 'planerad'} {local.strftime('%Y-%m-%d %H:%M')}"


def _decision_moment(value):
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Europe/Stockholm"))
    return parsed.timestamp()


def store_contact_guidance(
    *, legacy: dict, today: date, business_value_raw: float,
    latest_attempt_date: date | None, latest_attempt_result: str,
    latest_reached_date: date | None, latest_reached_result: str,
    last_opt_out_date: date | None,
    latest_follow_up_date: date | None, follow_up_reached_resolved: bool,
    unanswered_sequence_count: int, last_order_date: date | None,
    delivery_count: int, days_since_delivery: int | None,
    overdue_days: int | None, future_delivery: date | None,
    expected_cycle_days: int | None = None,
    follow_up_has_later_order: bool = False,
    follow_up_created_date: date | None = None,
    unverified_order_without_delivery: bool = False,
    positive_prospect_signal: bool = False,
    positive_buyer_signal: bool = False,
    pending_unconfirmed_order_date: date | None = None,
    qualified_email_trigger: str = "", qualified_email_reason: str = "", policy: dict = POLICY,
    latest_attempt_at=None, latest_reached_at=None, follow_up_created_at=None,
    latest_reached_event_order=None, follow_up_event_order=None,
    source_contact_id="", reported_order_date=None,
    first_order_sku_count=None, legacy_value_index=0, segment="",
    reached_by_purpose=None, completed_by_purpose=None, first_delivery_date=None,
    obligations=None, planned_activity=None, planned_activity_options=(), workflow_suppression=None,
    invalid_identity=False, ignore_planning=False,
    _selected_obligation_id=None,
) -> dict:
    """One read-only contact decision for list, calendar and workflow consumers.

    Hard restrictions apply to every channel. Obligations remain visible while
    a contact is blocked. A booking suppresses duplicate contact only in its
    own scope; an unrelated overdue promise is never silently closed.
    """
    # Retain the actual inputs for a bounded second pass through this same
    # decision function. Candidate evaluation never rebuilds customer features
    # and keeps the full obligation list, including independent review cases.
    candidate_arguments = dict(locals()) if _selected_obligation_id is None else None
    guidance = dict(legacy or {})
    plan = planned_activity or {}
    plan_row = plan.get("row", plan)
    scheduled = plan.get("scheduled_at") or plan_row.get("scheduled_at")
    plan_day = _decision_day(scheduled)
    plan_id = str(plan_row.get("planned_activity_id") or "")
    planned_current = bool(plan_day and plan_day >= today)
    all_plans = list(planned_activity_options or ()) or ([plan] if plan else [])
    plans_by_id = {str((p.get("row", p)).get("planned_activity_id") or ""): p for p in all_plans}
    open_items = [dict(item) for item in obligations or ()
                  if str(item.get("status") or "open") not in {"resolved", "cancelled"}]
    # Compatibility for callers before the persisted obligation migration.
    # Supplying even an empty map is authoritative and prevents resurrection.
    if obligations is None and latest_follow_up_date and not follow_up_reached_resolved:
        open_items.append({"obligation_id": "legacy-followup:" + str(source_contact_id or latest_follow_up_date),
                           "source_contact_id": source_contact_id, "status": "open",
                           "purpose": "explicit_followup", "due_date": latest_follow_up_date,
                           "created_at": follow_up_created_at or follow_up_created_date,
                           "event_order": follow_up_event_order, "evidence_quality": "legacy_unlinked"})
    for item in open_items:
        item["_due"] = _decision_day(item.get("due_at") or item.get("due_date"))
        linked = plans_by_id.get(str(item.get("planned_activity_id") or ""))
        if not linked and item.get("source_contact_id"):
            source_matches = [p for p in all_plans if str(p.get("row", p).get("source_contact_id") or "") == str(item["source_contact_id"])]
            if len(source_matches) == 1:
                linked = source_matches[0]
        linked_day = _decision_day(linked.get("scheduled_at") or linked.get("row", linked).get("scheduled_at")) if linked else None
        if linked_day:
            # A linked reschedule owns the effective due date; removing the
            # booking blocker must not resurrect the superseded old deadline.
            item["_due"] = linked_day
            item["planned_activity_id"] = str(linked.get("row", linked).get("planned_activity_id") or "")
        item["_represented"] = bool(not ignore_planning and linked_day and linked_day >= today)
    open_items.sort(key=lambda item: (item["_due"] or date.max, str(item.get("obligation_id") or "")))
    due_items = [item for item in open_items if item["_due"] and item["_due"] <= today and not item["_represented"]]
    handling_items = [item for item in due_items if item["_due"] < today]
    review_items = [item for item in open_items if item.get("status") == "needs_review" or (
        item.get("_due") and item["_due"] < today and not item.get("_represented") and (
            (today - item["_due"]).days >= int(policy["stale_followup_review_days"])
            or last_order_date and last_order_date >= item["_due"]
            and (today - item["_due"]).days >= int(policy["stale_followup_order_review_days"])))]
    # The primary customer action describes the current booking's scope.
    # Independent old commitments remain in Att hantera, but cannot turn a
    # valid other appointment into a zero-priority historical review case.
    scope_due_items = ([item for item in due_items if plan_id and str(item.get("planned_activity_id") or "") == plan_id]
                       if planned_current else due_items)
    selected = (next((item for item in scope_due_items
                      if str(item.get("obligation_id") or "") == _selected_obligation_id), None)
                if _selected_obligation_id is not None else
                scope_due_items[0] if scope_due_items else None)
    # A missed calendar appointment is an explicit commitment even in legacy
    # snapshots with no obligation table yet. Today's appointments are planned.
    if selected is None and plan_day and plan_day < today:
        selected = {"obligation_id": "calendar:" + plan_id, "planned_activity_id": plan_id,
                    "purpose": "calendar_activity", "status": "open", "_due": plan_day,
                    "created_at": plan_row.get("created_at"), "source_contact_id": plan_row.get("source_contact_id", "")}
        handling_items.append(selected)
    booking_selected = bool(planned_current and not ignore_planning)
    if booking_selected:
        selected = next((item for item in open_items if item.get("_represented")
                         and str(item.get("planned_activity_id") or "") == plan_id), None) or {
            "purpose": plan_row.get("purpose") or "calendar_activity", "planned_activity_id": plan_id,
            "source_contact_id": plan_row.get("source_contact_id", ""),
            "created_at": plan_row.get("agreed_at") or plan_row.get("created_at"), "_due": plan_day}
    future_items = [item for item in open_items if item["_due"] and item["_due"] > today and not item["_represented"]]
    future_selected = bool(not selected and future_items and not planned_current and not ignore_planning)
    if future_selected:
        selected = future_items[0]
    selected = selected or {}
    planned_reason = (_decision_plan_reason(plan_row, scheduled) if booking_selected else
                      f"Uppföljning planerad {selected['_due'].isoformat()} · Tid ej satt" if future_selected else "")
    explicit = bool(selected)
    scope = str(selected.get("source_scope") or ("obligation:" + str(selected["obligation_id"]) if selected.get("obligation_id") else "planned_activity:" + plan_id if booking_selected else ""))
    selected_source = str(selected.get("source_contact_id") or "")
    created = selected.get("agreed_at") or selected.get("source_created_at") or selected.get("created_at")
    if not created and selected_source and selected_source == str(source_contact_id):
        created = follow_up_created_at or follow_up_created_date
    created_ts = _decision_moment(created)
    reached_ts = _decision_moment(latest_reached_at or latest_reached_date)
    attempt_ts = _decision_moment(latest_attempt_at or latest_attempt_date)
    selected_order = selected.get("event_order")
    if selected_order is None and selected_source and selected_source == str(source_contact_id):
        selected_order = follow_up_event_order
    agreement_evidence = bool(str(selected.get("purpose") or "") == "explicit_followup"
                              or selected.get("customer_agreement_confirmed") is True
                              or plan_row.get("customer_agreement_confirmed") is True)
    agreement_after_negative = bool(explicit and agreement_evidence and created_ts is not None and reached_ts is not None and (
        created_ts > reached_ts or (created_ts == reached_ts and selected_order is not None
            and latest_reached_event_order is not None and selected_order >= latest_reached_event_order)))
    negative_active = bool(latest_reached_result == "Negativ" and latest_reached_date
        and (not last_order_date or last_order_date <= latest_reached_date))
    age = (today - latest_reached_date).days if latest_reached_date else None
    positive_window = bool(age is not None and (
        positive_prospect_signal and int(policy["positive_prospect_followup_min_days"]) <= age <= 45
        or positive_buyer_signal and int(policy["positive_buyer_followup_min_days"]) <= age <= 45))
    negative_cooldown = bool(negative_active and age is not None and 0 <= age < int(policy["negative_cooldown_days"]))
    requires_handling = bool(handling_items)
    obligation_ids = [str(item["obligation_id"]) for item in open_items if item.get("obligation_id")]
    if selected.get("obligation_id") and str(selected["obligation_id"]) not in obligation_ids:
        obligation_ids.append(str(selected["obligation_id"]))

    def choose(status, action, reason, *, code, key="", until=None, review=False):
        can_start = status in {"act_now", "overdue_followup"} and not review
        result = dict(guidance)
        result.update(status_key=status, status_label={"act_now": "Agera nu", "overdue_followup": "Att hantera",
            "planned": "Planerad", "wait": "Avvakta", "idle": "Ingen åtgärd nu", "data_missing": "Data saknas"}[status],
            action_label=action, action_key=key or code, action_type=code,
            action_scope=scope or "routine:" + code, reason_code=code, reason_codes=[code], reason_text=reason,
            waiting_until=until.isoformat() if isinstance(until, date) else until,
            contact_not_before=until.isoformat() if isinstance(until, date) else until,
            can_contact_now=can_start, can_start_unscheduled_contact=can_start,
            can_execute_planned_contact=bool(plan_id and plan_day and plan_day <= today and not review
                and (status == "planned" or can_start)),
            requires_handling=requires_handling, review_only=review,
            requires_review=bool(review_items) or review,
            review_obligation_ids=[str(item["obligation_id"]) for item in review_items if item.get("obligation_id")],
            planned_activity_id=plan_id or str(selected.get("planned_activity_id") or ""),
            has_planned_contact=planned_current or future_selected,
            next_contact_at=(scheduled.isoformat() if isinstance(scheduled, (date, datetime)) else scheduled)
                if planned_current else str(selected.get("due_at") or selected.get("due_date") or "") if future_selected else None,
            overdue_activity_id=(str(selected.get("planned_activity_id") or "") if selected.get("_due") and selected["_due"] < today else ""),
            obligation_ids=obligation_ids, obligation_id=str(selected.get("obligation_id") or ""),
            obligation_revision=selected.get("revision"),
            source_contact_id=selected_source if explicit else str(source_contact_id or ""),
            explicit_agreement_overrides_negative=agreement_after_negative,
            timing_source="first_order_phase" if delivery_count == 1 and days_since_delivery is not None and 7 <= days_since_delivery <= 90
                and (code in {"first_order_onboarding", "second_purchase_due"}
                     or code in {"unanswered_retry_due", "unanswered_verify_contact_due"} and not positive_window) else "contact_recency",
            underlying_action_type=("first_order_onboarding" if days_since_delivery is not None and days_since_delivery <= 10 else "second_purchase_due")
                if delivery_count == 1 and days_since_delivery is not None and 7 <= days_since_delivery <= 90 else code,
            recommended_contact_type="", policy_version=POLICY_VERSION,
            evidence_quality=selected.get("evidence_quality") or "current_snapshot")
        if review and selected.get("obligation_id") and str(selected["obligation_id"]) not in result["review_obligation_ids"]:
            result["review_obligation_ids"].append(str(selected["obligation_id"]))
        # A blocked historical scope must not hide a different, valid task
        # from the customer list. Current bookings retain their own primary
        # decision, including real contact restrictions. For unbooked work,
        # try due scopes in stable date/ID order and use exactly the same
        # negative, order, retry and snooze policy for each one's evidence.
        if (candidate_arguments is not None and not planned_current
                and not result["can_start_unscheduled_contact"]
                and not result["can_execute_planned_contact"]):
            review_ids = list(result["review_obligation_ids"])
            for item in due_items:
                candidate_id = str(item.get("obligation_id") or "")
                if not candidate_id or candidate_id == str(selected.get("obligation_id") or ""):
                    continue
                arguments = dict(candidate_arguments)
                arguments.update(
                    _selected_obligation_id=candidate_id,
                    obligations=open_items,
                    # Never lend an unrelated missed appointment's identity,
                    # channel or agreement evidence to an unbooked promise.
                    planned_activity=plans_by_id.get(str(item.get("planned_activity_id") or "")),
                )
                candidate = store_contact_guidance(**arguments)
                for review_id in candidate.get("review_obligation_ids", ()):
                    if review_id not in review_ids:
                        review_ids.append(review_id)
                if (candidate.get("obligation_id") == candidate_id
                        and not candidate.get("review_only")
                        and (candidate.get("can_start_unscheduled_contact")
                             or candidate.get("can_execute_planned_contact"))):
                    candidate["review_obligation_ids"] = review_ids
                    candidate["requires_review"] = bool(review_ids) or candidate["requires_review"]
                    return candidate
        return result

    if invalid_identity or guidance.get("status_key") == "data_missing":
        return choose("data_missing", "Kontrollera kunddata", "Kundidentitet eller aktiv ansvarig säljare saknas", code="invalid_customer_identity", review=True)
    if last_opt_out_date:
        return choose("wait", "Kontakta inte", "Återkom ej är registrerat. Ny kontakt kräver uttrycklig registrerad tillåtelse.", code="do_not_contact")
    if negative_cooldown and not agreement_after_negative:
        return choose("wait", "Granska kontaktöverenskommelsen" if explicit else "Avvakta",
            "Senare negativ dialog kräver granskning av det äldre åtagandet" if explicit else "Negativ dialog – kontaktpaus gäller",
            code="negative_contact_cooldown", until=latest_reached_date + timedelta(days=int(policy["negative_cooldown_days"])), review=explicit)

    # Generic snooze/dismiss applies only to the recommendation it belongs to.
    # Obligation snooze is a separate explicit rescheduling operation.
    workflow = workflow_suppression or ""
    workflow_scope = workflow.get("source_scope") or workflow.get("action_scope") if isinstance(workflow, dict) else ""
    workflow_reason = str(workflow.get("reason") or workflow.get("status") or "") if isinstance(workflow, dict) else str(workflow)
    selected_snooze = _decision_day(selected.get("snooze_until"))
    if selected_snooze and selected_snooze > today:
        return choose("wait", "Avvakta överenskommen uppföljning", "Åtagandet har en registrerad senare kontaktgräns", code="obligation_snoozed", until=selected_snooze)
    if workflow_reason and workflow_reason != "suggestion_planned" and (
            (not explicit) or (workflow_scope and workflow_scope == scope)):
        return choose("wait", "Avvakta" if workflow_reason == "snoozed" else "Återuppta förslag",
                      "Förslaget är snoozat" if workflow_reason == "snoozed" else "Tidigare förslag dolt", code=workflow_reason)

    order_after_attempt = bool(last_order_date and latest_attempt_date and last_order_date >= latest_attempt_date)
    retry_needed = bool(latest_attempt_result == "Ej anträffbar" and latest_attempt_date and not order_after_attempt)
    if retry_needed:
        retry = next_business_day_retry(latest_attempt_date, int(policy["retry_business_days"]))
        # Only a NEW explicit agreement for today can override the failed-
        # attempt pause. An old overdue promise remains open but cannot do so.
        new_agreement_today = bool(explicit and agreement_evidence and selected.get("_due") == today
            and created_ts is not None and attempt_ts is not None and created_ts > attempt_ts)
        if today < retry and not new_agreement_today:
            return choose("wait", "Avvakta till nästa försök", f"Ej anträffbar – nytt försök tidigast {retry.isoformat()}", code="unanswered_retry_wait", until=retry)

    if explicit:
        selected_day = selected.get("_due")
        stale = bool(selected_day and selected_day < today and (
            (today - selected_day).days >= int(policy["stale_followup_review_days"])
            or last_order_date and last_order_date >= selected_day
            and (today - selected_day).days >= int(policy["stale_followup_order_review_days"])))
        if selected.get("status") == "needs_review" or stale:
            return choose("overdue_followup" if requires_handling else "idle", "Granska gammal uppföljning",
                "Åtagandet är öppet men måste klarläggas före ny kontakt", code="stale_follow_up_review", review=True)
        # Known service promises survive an order. A generic sales appointment
        # is still subject to order and delivery guards below.
        service_promise = str(selected.get("purpose") or "") not in {"", "calendar_activity", "routine", "sales", "reorder", "secure_next_order"}
        if service_promise or (not booking_selected and not future_selected):
            if booking_selected or future_selected:
                return choose("planned", "Nästa kontakt", planned_reason, code="future_planned_activity" if booking_selected else "future_follow_up_date", key="open_planning")
            if selected_day == today:
                return choose("act_now", "Följ upp idag", "Uttrycklig uppföljning förfaller i dag", code="follow_up_due_today", key="follow_up_today")
            overdue_reason = (_decision_plan_reason(plan_row, scheduled, overdue=True)
                              if selected.get("purpose") == "calendar_activity" else
                              f"Uttrycklig uppföljning {selected_day.isoformat()} har passerat" if selected_day else
                              "Uttryckligt åtagande är fortfarande öppet")
            return choose("overdue_followup", "Planera om uppföljningen", overdue_reason,
                code="overdue_planned_activity" if selected.get("purpose") == "calendar_activity" else "overdue_explicit_follow_up", key="reschedule_followup")

    # Order acquisition signals are resolved by a later commercial order;
    # recording Order lagd is a sync hint, never proof of dialogue/delivery.
    if future_delivery and future_delivery > today:
        return choose("wait", "Invänta leverans", f"Registrerad leverans {future_delivery.isoformat()}", code="future_delivery", until=future_delivery)
    if reported_order_date and (not last_order_date or last_order_date < reported_order_date):
        sync_until = next_business_day_retry(reported_order_date, int(policy["reported_order_sync_business_days"]))
        if today < sync_until:
            return choose("wait", "Invänta ordersynk", "Order lagd är registrerat men ordern har ännu inte synkats", code="reported_order_sync_wait", until=sync_until)
        return choose("idle", "Kontrollera registrerad order", "Order lagd saknar synkad order efter två arbetsdagar", code="reported_order_sync_review", review=True)
    if pending_unconfirmed_order_date and pending_unconfirmed_order_date <= today:
        order_age = (today - pending_unconfirmed_order_date).days
        if order_age <= int(policy["recent_order_hold_days"]):
            return choose("wait", "Avvakta registrerad order", "Ny order saknar leveransdatum", code="recent_order_pending_confirmation",
                          until=pending_unconfirmed_order_date + timedelta(days=int(policy["recent_order_hold_days"]) + 1))
        if order_age <= int(policy["unconfirmed_order_review_days"]):
            return choose("act_now", "Kontrollera orderstatus", "Registrerad order saknar leveransdatum", code="unconfirmed_order_status_due", key="check_order_status")
        return choose("idle", "Granska historisk orderstatus" if delivery_count else "Granska order-/leveransstatus", "Äldre order saknar känt leveransdatum", code="stale_unconfirmed_order_review" if delivery_count else "order_without_verified_delivery", review=True)
    if delivery_count == 0 and unverified_order_without_delivery:
        return choose("idle", "Granska order-/leveransstatus", "Registrerad order utan känt leveransdatum", code="order_without_verified_delivery", review=True)
    if booking_selected or future_selected:
        return choose("planned", "Nästa kontakt", planned_reason, code="future_planned_activity" if booking_selected else "future_follow_up_date", key="open_planning")
    if guidance.get("reason_code") == "recent_email_engagement_wait":
        return choose("wait", "Invänta mejlutfall", "Kvalificerad mejlsignal inväntar uppföljningsfönstret", code="recent_email_engagement_wait")

    reached_by_purpose = reached_by_purpose or {}
    current_purpose = "first_order_onboarding" if delivery_count == 1 and days_since_delivery is not None and 7 <= days_since_delivery <= 10 else "second_purchase_due"
    purpose_aliases = {"first_order_onboarding": ("first_order_onboarding", "onboarding", "check_placement"),
                       "second_purchase_due": ("second_purchase_due", "first_order_reorder", "secure_repeat_purchase")}
    purpose_contact = max((_decision_day(reached_by_purpose.get(key)) for key in purpose_aliases[current_purpose]
                           if _decision_day(reached_by_purpose.get(key))), default=None)
    first_day = first_delivery_date or (today - timedelta(days=days_since_delivery) if days_since_delivery is not None else None)
    completed_contact = max((_decision_day((completed_by_purpose or {}).get(key)) for key in purpose_aliases[current_purpose]
                            if _decision_day((completed_by_purpose or {}).get(key))), default=None)
    onboarding_done = bool(current_purpose == "first_order_onboarding" and completed_contact and first_day and completed_contact > first_day)
    if onboarding_done:
        return choose("idle", "Ingen åtgärd nu", "Uppföljning av första leveransen är genomförd", code="first_order_onboarding_completed")
    if delivery_count == 1 and days_since_delivery is not None and 7 <= days_since_delivery <= 90:
        # Before/on-delivery dialogue cannot mean onboarding was completed.
        recent_after_delivery = latest_reached_date if latest_reached_date and first_day and latest_reached_date > first_day else None
        gap_contact = purpose_contact or recent_after_delivery
        if gap_contact and 0 <= (today - gap_contact).days < int(policy["same_purpose_dialogue_gap_days"]):
            return choose("wait", "Avvakta kort kontaktpaus", "Nylig dialog efter första leveransen", code="recent_same_purpose_dialogue_wait", until=gap_contact + timedelta(days=int(policy["same_purpose_dialogue_gap_days"])))
    if qualified_email_trigger in {"stockfiller_click_followup", "product_sheet_click_followup"}:
        action = "Följ upp beställningsintresset" if qualified_email_trigger == "stockfiller_click_followup" else "Följ upp produktintresset"
        return choose("act_now", action, qualified_email_reason or "Aktuell kvalificerad mejlsignal utan senare order", code=qualified_email_trigger, key="follow_up_order_interest")
    if retry_needed and positive_window:
        return choose("act_now", "Kontrollera kontaktväg" if unanswered_sequence_count >= 3 else "Försök nå kunden igen",
            "Positiv dialog kvarstår efter ett obesvarat försök; ingen senare order",
            code="unanswered_verify_contact_due" if unanswered_sequence_count >= 3 else "unanswered_retry_due",
            key="verify_contact_details" if unanswered_sequence_count >= 3 else "retry_unanswered")
    if positive_prospect_signal and age is not None and int(policy["positive_prospect_followup_min_days"]) <= age <= 45:
        return choose("act_now", "Följ upp positiv dialog", f"Positiv dialog för {age} dagar sedan · ingen senare order", code="positive_dialogue_followup", key="follow_up_dialogue")
    if positive_buyer_signal and age is not None and int(policy["positive_buyer_followup_min_days"]) <= age <= 45:
        return choose("act_now", "Följ upp positiv dialog", f"Positiv dialog för {age} dagar sedan · ingen senare order", code="positive_buyer_followup", key="follow_up_dialogue")

    reactivation = bool(delivery_count == 1 and days_since_delivery is not None and days_since_delivery > 90
                        or delivery_count >= 2 and overdue_days is not None and overdue_days > 90)
    material_reorder = bool(delivery_count >= 2 and not reactivation and overdue_days is not None
        and overdue_days >= int(policy["material_reorder_overdue_days"])
        and (age is None or age >= int(policy["material_reorder_min_dialogue_days"])))
    if reactivation and age is not None and age < int(policy["reactivation_dialogue_gap_days"]):
        return choose("wait", "Avvakta återaktivering", "Nylig faktisk dialog inför återaktivering", code="recent_reactivation_dialogue_wait", until=latest_reached_date + timedelta(days=int(policy["reactivation_dialogue_gap_days"])))
    first_phase = delivery_count == 1 and days_since_delivery is not None and 7 <= days_since_delivery <= 90
    if age is not None and age < 20 and not first_phase and not material_reorder:
        return choose("wait", "Avvakta rutinuppföljning", f"Faktisk dialog för {max(0, age)} dagar sedan", code="recent_reached_routine_wait", until=latest_reached_date + timedelta(days=20))
    if delivery_count == 1 and days_since_delivery is not None and days_since_delivery < 7:
        return choose("idle", "Ingen åtgärd nu", "Första leveransen är nyligen genomförd", code="first_order_new")
    if retry_needed:
        return choose("act_now", "Kontrollera kontaktväg" if unanswered_sequence_count >= 3 else "Försök nå kunden igen",
            "Obesvarat försök behöver följas upp; ingen senare order har registrerats",
            code="unanswered_verify_contact_due" if unanswered_sequence_count >= 3 else "unanswered_retry_due",
            key="verify_contact_details" if unanswered_sequence_count >= 3 else "retry_unanswered")
    if reactivation:
        history = "En tidigare leverans" if delivery_count == 1 else "Tidigare återkommande kund"
        return choose("act_now", "Återaktivera kunden", f"{history} · {days_since_delivery} dagar sedan senaste leveransen",
            code="single_order_reactivation_due" if delivery_count == 1 else "repeat_reactivation_due", key="reactivate_customer")
    if delivery_count >= 2:
        if overdue_days is not None and overdue_days >= 0:
            due_label = _decision_date_label(today - timedelta(days=overdue_days), today)
            return choose("act_now", "Säkra försenat återköp" if material_reorder else "Säkra återköpet", f"Beräknad nästa order {due_label}", code="established_reorder_due", key="secure_next_order")
        if days_since_delivery is not None and days_since_delivery >= 45 and (age is None or age >= 60):
            return choose("act_now", "Vårda kundrelationen", "Länge sedan faktisk dialog; återköp är ännu inte förfallet", code="relationship_care_due", key="contact_customer")
        return choose("idle", "Ingen åtgärd nu", "Återköp ännu inte förfallet", code="reorder_not_due")
    if delivery_count == 1:
        if days_since_delivery is None or days_since_delivery < 7:
            return choose("idle", "Ingen åtgärd nu", "Första leveransen är nyligen genomförd", code="first_order_new")
        if days_since_delivery <= 10:
            if str(segment).upper() in {"A", "B"} or first_order_sku_count == 1 or legacy_value_index >= 70:
                return choose("act_now", "Kontrollera placering och start", f"Första leverans för {days_since_delivery} dagar sedan – planera uppföljning", code="first_order_onboarding", key="check_placement")
            return choose("idle", "Invänta andra köpets uppföljning", "Ingen särskild tidig onboardingindikator", code="first_order_onboarding_not_required")
        return choose("act_now", "Säkra andra köpet", f"{days_since_delivery} dagar sedan första leveransen · dags att säkra andra köpet", code="second_purchase_due", key="secure_repeat_purchase")
    # Potential controls rank, never a hard prohibition on routine prospects.
    return choose("act_now", "Ta nästa steg mot första ordern",
                  "Tidigare faktisk dialog saknas i historiken" if age is None else f"{age} dagar sedan faktisk dialog",
                  code="value_based_contact_due", key="contact_customer")
