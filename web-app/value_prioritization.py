"""V3.1 preview: independent business potential and contact urgency.

DFP-based opportunity indices, NOT calibrated probabilities, SEK forecasts
or causal sales uplift. Keep legacy priority_score for planning and routing.
Constants are centralized for a future validated admin-adjustable policy.
"""
from __future__ import annotations

from datetime import date, timedelta
from statistics import median
import math

VERSION = "v3.1-preview"
POLICY = {
    "shrinkage_purchases": 3.0, "horizon_days": 90,
    "realization_prospect": 0.50, "realization_first_order": 0.70,
    "realization_repeat": 0.80, "first_order_frequency": 1.20,
    # All parameters are provisional until portfolio replay and backtesting.
    "maximum_repeat_frequency": 6.0, "score_scale_dfp": 6.0,
    "unknown_dialogue_factor": 0.70, "retry_business_days": 2,
    "stale_repeat_after_cycles": 3.0, "stale_repeat_factor": 0.75,
    "very_stale_repeat_after_cycles": 6.0, "very_stale_repeat_factor": 0.60,
    "stale_first_order_after_days": 180, "stale_first_order_factor": 0.70,
}
FALLBACK_DFP = {"A": 21.0, "B": 12.0, "C": 9.0}
CONTACT_CURVE = ((0, 0.0), (7, 0.02), (20, 0.15), (30, 0.35),
                 (40, 0.55), (50, 0.75), (60, 0.90), (90, 1.0))
# 'Order lagd' can represent an order logged without speaking to the buyer.
# It does not prove a human dialogue. Explicit refusals count as dialogue
# but also create a separate persistent no-contact safety guard.
REACHED_RESULTS = frozenset({"Positiv", "Neutral", "Negativ"})
DO_NOT_CONTACT_RESULTS = frozenset({"återkom ej", "kontakta ej", "ring ej"})


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
                 "retry_business_days", "maximum_repeat_frequency")
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


def contact_factor(days_since_reached: int | None,
                   latest_result: str = "", policy: dict = POLICY) -> float:
    if days_since_reached is None:
        # Unknown history is not the same as proven "never reached".
        # Failed attempts never imply successful dialogue or conversion.
        return float(policy["unknown_dialogue_factor"])
    day = max(0, int(days_since_reached))
    for idx in range(1, len(CONTACT_CURVE)):
        right_day, right_value = CONTACT_CURVE[idx]
        if day <= right_day:
            left_day, left_value = CONTACT_CURVE[idx - 1]
            fraction = (day - left_day) / (right_day - left_day)
            return left_value + fraction * (right_value - left_value)
    return 1.0


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
                        status: str, trigger: str, policy: dict = POLICY) -> dict:
    """Effective sortable contact priority; suppress unavailable customers."""
    bucket = {
        "overdue_followup": 0, "act_now": 0,
        "idle": 1, "planned": 2, "wait": 3, "data_missing": 4,
    }.get(status, 4)
    relevance = contact_factor(days_since_reached, latest_result, policy)
    if status == "overdue_followup":
        relevance = max(relevance, 0.75)
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
    if status in {"planned", "wait", "data_missing"} or waiting_for_retry:
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
