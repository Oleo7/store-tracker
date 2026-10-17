"""V3.2 preview: independent business potential and contact urgency.

DFP-based opportunity indices, NOT calibrated probabilities, SEK forecasts
or causal sales uplift. Keep legacy priority_score for planning and routing.
Constants are centralized for a future validated admin-adjustable policy.
"""
from __future__ import annotations

from datetime import date, timedelta
from statistics import median
import math
import unicodedata

VERSION = "v3.2-preview"
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
    "routine_prospect_min_raw_dfp": 6.0,
    "stale_followup_review_days": 90,
    "stale_followup_order_review_days": 60,
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
                 "routine_prospect_min_raw_dfp", "stale_followup_review_days",
                 "stale_followup_order_review_days")
    for key in positives:
        value = float(policy[key])
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"Invalid model parameter {key}")
    for key in ("realization_prospect", "realization_first_order",
                "realization_repeat", "unknown_dialogue_factor",
                "stale_repeat_factor", "very_stale_repeat_factor",
                "stale_first_order_factor"):
        value = float(policy[key])
        if not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"Invalid model factor {key}")
    if float(policy["very_stale_repeat_after_cycles"]) <= float(policy["stale_repeat_after_cycles"]):
        raise ValueError("Staleness bands must increase")
    if int(policy["stale_followup_order_review_days"]) > int(policy["stale_followup_review_days"]):
        raise ValueError("Stale follow-up review thresholds must increase")


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


def business_days_elapsed(start: date | None, end: date) -> int:
    if not start or start >= end:
        return 0
    days, current = 0, start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days += 1
    return days


def contact_opportunity(*, business_value_raw: float,
                        days_since_reached: int | None,
                        latest_attempt_date: date | None,
                        today: date, latest_result: str,
                        status: str, trigger: str,
                        positive_prospect_signal: bool = False,
                        policy: dict = POLICY) -> dict:
    """Effective sortable contact priority; suppress unavailable customers."""
    bucket = {
        "overdue_followup": 0, "act_now": 0,
        "idle": 1, "planned": 2, "wait": 3, "data_missing": 4,
    }.get(status, 4)
    relevance = contact_factor(days_since_reached, latest_result, policy)
    if positive_prospect_signal and days_since_reached is not None:
        relevance = max(
            relevance,
            _interpolated_contact_factor(days_since_reached, POSITIVE_PROSPECT_CURVE),
        )
    if status == "overdue_followup" and trigger != "stale_follow_up_review":
        relevance = max(relevance, 0.75)
    if trigger == "stale_follow_up_review":
        bucket = 1  # needs review, not a fresh outbound call
    if trigger in {"stockfiller_click_followup", "product_sheet_click_followup"}:
        relevance = max(relevance, 0.75)
    # An unanswered attempt after a dialogue this week is not an urgent
    # new business opportunity. Older cases can have an overdue retry need.
    if trigger in {"unanswered_retry_due", "unanswered_verify_contact_due"} and (
        days_since_reached is None or days_since_reached >= 30
    ):
        relevance = max(relevance, 0.75)
    waiting_for_retry = (
        latest_result == "Ej anträffbar" and latest_attempt_date is not None
        and business_days_elapsed(latest_attempt_date, today)
            < int(policy["retry_business_days"])
    )
    if status in {"planned", "wait", "data_missing", "idle"} or waiting_for_retry or trigger == "stale_follow_up_review":
        relevance = 0.0
        if waiting_for_retry:
            bucket = 3
    raw = max(0.0, float(business_value_raw or 0) * relevance)
    return {
        "contact_priority_raw": round(raw, 6),
        "contact_priority_score": display_score(raw, policy),
        "contact_priority_bucket": bucket,
        "contact_relevance_factor": round(relevance, 4),
    }


def next_business_day_retry(start: date, business_days: int) -> date:
    current = start
    elapsed = 0
    while elapsed < business_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            elapsed += 1
    return current


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
    qualified_email_trigger: str = "", policy: dict = POLICY,
) -> dict:
    """List-specific decision. Legacy calendar/suggestion contracts stay intact.

    Returns a single source of truth for the two-score Butiker experience.
    Explicit commitments and contact restrictions take priority over routine
    value ranking. This never mutates the underlying planned activities.
    """
    guidance = dict(legacy or {})
    def choose(status: str, action: str, reason: str, *,
               code: str = "", key: str = "", until: date | None = None) -> dict:
        result = dict(guidance)
        result.update(
            status_key=status,
            status_label={
                "act_now": "Agera nu", "overdue_followup": "Missad uppföljning",
                "planned": "Planerad", "wait": "Avvakta",
                "idle": "Ingen åtgärd nu", "data_missing": "Data saknas",
            }[status],
            action_label=action,
            action_key=key or code or "none",
            reason_code=code,
            reason_text=reason,
            waiting_until=until.isoformat() if until else None,
            can_contact_now=(status in {"act_now", "overdue_followup"}
                             and code != "stale_follow_up_review"),
        )
        if status not in {"act_now", "overdue_followup", "planned"} or code == "stale_follow_up_review":
            result["recommended_contact_type"] = ""
        return result

    if guidance.get("status_key") == "data_missing":
        return guidance
    if last_opt_out_date:
        return choose("wait", "Kontakta inte",
                      "Återkom ej är registrerat – ändra endast efter uttrycklig ny tillåtelse",
                      code="do_not_contact")
    # Actual future booking or date-specific follow-up cannot be replaced by
    # a generic value-based suggestion.
    if guidance.get("status_key") == "planned":
        return guidance
    if future_delivery and future_delivery > today:
        return choose("wait", "Avvakta",
                      f"Invänta registrerad leverans {future_delivery.isoformat()}",
                      code="future_delivery", until=future_delivery)
    # A negative dialogue occurring AFTER the original promise wins over the
    # older due date. A negative dialogue that itself contains a new follow-up
    # promise must still honour that explicit commitment.
    if (latest_reached_result == "Negativ" and latest_reached_date
        and (not last_order_date or last_order_date < latest_reached_date)
        and 0 <= (today - latest_reached_date).days < 60
        and (follow_up_created_date is None
             or latest_reached_date > follow_up_created_date)):
        return choose("wait", "Avvakta",
                      "Senare negativ dialog ersätter äldre uppföljningslöfte",
                      code="negative_contact_cooldown",
                      until=latest_reached_date + timedelta(days=60))
    # An explicit promise is not resolved merely by an outbound email or by
    # a failed call. Reached dialogue (or explicit recorded cancellation) is
    # required to close it.
    if latest_follow_up_date and not follow_up_reached_resolved:
        if latest_follow_up_date < today:
            overdue_age = (today - latest_follow_up_date).days
            if (overdue_age >= int(policy["stale_followup_review_days"])
                    or (follow_up_has_later_order and
                        overdue_age >= int(policy["stale_followup_order_review_days"]))):
                return choose(
                    "overdue_followup", "Granska gammal uppföljning",
                    "Äldre uttryckligt åtagande är fortfarande öppet. "
                    "Kontrollera om det kvarstår innan en ny kontakt planeras.",
                    code="stale_follow_up_review",
                )
            return choose("overdue_followup", "Planera om uppföljningen",
                          f"Uttrycklig uppföljning från {latest_follow_up_date.isoformat()} är förfallen",
                          code="overdue_explicit_follow_up")
        if latest_follow_up_date == today:
            return choose("act_now", "Följ upp idag",
                          "Uttrycklig uppföljning förfaller i dag",
                          code="follow_up_due_today")
    if guidance.get("status_key") == "overdue_followup":
        return guidance

    if latest_reached_result == "Negativ" and latest_reached_date and (
        not last_order_date or last_order_date < latest_reached_date
    ) and 0 <= (today - latest_reached_date).days < 60:
        return choose("wait", "Avvakta", "Negativ dialog – kontaktpaus gäller",
                      code="negative_contact_cooldown",
                      until=latest_reached_date + timedelta(days=60))

    if latest_attempt_result == "Ej anträffbar" and latest_attempt_date:
        retry = next_business_day_retry(
            latest_attempt_date, int(policy["retry_business_days"])
        )
        if today < retry:
            return choose("wait", "Avvakta till nästa försök",
                          f"Ej anträffbar – nytt försök tidigast {retry.isoformat()}",
                          code="unanswered_retry_wait", until=retry)
        if unanswered_sequence_count >= 3:
            return choose("act_now", "Kontrollera kontaktväg",
                          "Tre obesvarade försök – kontrollera kontaktuppgifter eller inköpare",
                          code="unanswered_verify_contact_due", key="verify_contact_details")
        return choose("act_now", "Försök nå kunden igen",
                      f"Senaste kontaktförsöket {latest_attempt_date.isoformat()} var Ej anträffbar",
                      code="unanswered_retry_due", key="retry_unanswered")

    # Preserve intentional snoozes/dismissals and outbound-email waits. The
    # earlier v2 'recent contact' block is superseded by reached/attempt rules.
    old_reason = str(guidance.get("reason_code") or "")
    if guidance.get("status_key") == "wait" and old_reason not in {"recent_human_contact"}:
        return guidance

    age = (today - latest_reached_date).days if latest_reached_date else None
    if age is not None and age < 20 and not qualified_email_trigger:
        return choose("wait", "Avvakta rutinuppföljning",
                      f"Faktisk dialog för {max(0,age)} dagar sedan",
                      code="recent_reached_routine_wait",
                      until=latest_reached_date + timedelta(days=20))

    if qualified_email_trigger:
        # A genuine inbound click can justify an early contact; it is not
        # the same as a sent email or a stale email-open pixel.
        if qualified_email_trigger in {"stockfiller_click_followup", "product_sheet_click_followup"}:
            return choose("act_now", "Följ upp intresset",
                          "Aktuell kvalificerad mejlsignal, ingen senare order",
                          code=qualified_email_trigger, key="follow_up_order_interest")

    if delivery_count == 0 and unverified_order_without_delivery:
        return choose(
            "idle", "Granska order-/leveransstatus",
            "Kunden har en registrerad order men ingen verifierad leverans. "
            "Behandla inte kunden som ett obearbetat prospekt.",
            code="order_without_verified_delivery")
    if delivery_count >= 2:
        if overdue_days is not None and overdue_days >= 0:
            return choose("act_now", "Säkra återköpet",
                          f"Beräknat återköp är {overdue_days} dagar försenat",
                          code="established_reorder_due", key="secure_next_order")
        # A recent delivery should not trigger a fabricated reorder problem.
        if (days_since_delivery is not None and days_since_delivery >= 45
                and (age is None or age >= 60)):
            return choose("act_now", "Vårda kundrelationen",
                          "Länge sedan faktisk dialog – återköp är inte förfallet",
                          code="relationship_care_due", key="contact_customer")
        return choose("idle", "Ingen åtgärd nu",
                      "Återköp ännu inte förfallet", code="reorder_not_due")
    if delivery_count == 1:
        if days_since_delivery is not None and days_since_delivery < 7:
            return choose("idle", "Ingen åtgärd nu",
                          "Första leveransen är nyligen genomförd",
                          code="first_order_new")
        if days_since_delivery is not None and days_since_delivery <= 10:
            return choose("act_now", "Kontrollera placering och start",
                          f"Första leverans för {days_since_delivery} dagar sedan",
                          code="first_order_onboarding")
        if days_since_delivery is not None and days_since_delivery >= 11:
            return choose("act_now", "Säkra andra köpet" if days_since_delivery <= 90 else "Återaktivera kunden",
                          f"En tidigare leverans, {days_since_delivery} dagar sedan",
                          code="second_purchase_due" if days_since_delivery <= 90 else "single_order_reactivation_due")
    if positive_prospect_signal and delivery_count == 0 and age is not None and 20 <= age <= 45:
        return choose(
            "act_now", "Följ upp positiv dialog",
            f"Positiv dialog för {age} dagar sedan – ingen senare order",
            code="positive_dialogue_followup", key="follow_up_dialogue",
        )
    if (delivery_count == 0
            and business_value_raw >= float(policy["routine_prospect_min_raw_dfp"])
            and (age is None or age >= 20)):
        return choose("act_now", "Ta nästa steg mot första ordern",
                      ("Tidigare faktisk dialog saknas i historiken"
                       if age is None else f"{age} dagar sedan faktisk dialog"),
                      code="value_based_contact_due", key="contact_customer")

    if delivery_count == 0 and business_value_raw > 0 and (age is None or age >= 20):
        return choose("idle", "Bevaka prospekt",
                      "Lägre beräknad potential – visas även vid sortering på Affärsvärde",
                      code="routine_prospect_below_threshold")
    return choose("idle", "Ingen åtgärd nu",
                  "Ingen förfallen uppföljning eller aktuell kontaktmöjlighet",
                  code="no_current_trigger")
