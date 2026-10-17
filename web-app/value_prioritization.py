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
    "maximum_repeat_frequency": 3.0, "score_scale_dfp": 3.0,
    "unknown_dialogue_factor": 0.80, "retry_business_days": 2,
}
FALLBACK_DFP = {"A": 21.0, "B": 12.0, "C": 9.0}
CONTACT_CURVE = ((0, 0.0), (7, 0.02), (20, 0.15), (30, 0.35),
                 (40, 0.55), (50, 0.75), (60, 0.90), (90, 1.0))
REACHED_RESULTS = frozenset({"Positiv", "Neutral", "Negativ", "Order lagd"})


def complete_deliveries(feature: dict, today: date) -> list[float]:
    """Only known positive DFP on completed commercial delivery dates."""
    result = []
    for day, amount in (feature.get("delivery_dfp_by_date") or {}).items():
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


def display_score(raw: float, policy: dict = POLICY) -> int:
    """Monotonic smooth 0-100 scale, NOT a conversion probability."""
    raw = max(0.0, float(raw or 0))
    scale = float(policy["score_scale_dfp"])
    return round(100.0 * raw / (raw + scale)) if scale > 0 else 0


def business_opportunity(*, delivery_values: list[float], prior_dfp: float,
                         delivery_count: int, cycle_days: int | None,
                         policy: dict = POLICY) -> dict:
    """Shared shrinkage estimate of 90-day opportunity for all A/B/C stores."""
    k = float(policy["shrinkage_purchases"])
    prior = max(0.0, float(prior_dfp or 0))
    mean = sum(delivery_values) / len(delivery_values) if delivery_values else prior
    typical = ((len(delivery_values) * mean + k * prior) /
               (len(delivery_values) + k)) if len(delivery_values) + k else prior
    if delivery_count <= 0:
        frequency = 1.0
        realization = float(policy["realization_prospect"])
    elif delivery_count == 1:
        frequency = float(policy["first_order_frequency"])
        realization = float(policy["realization_first_order"])
    else:
        frequency = min(float(policy["maximum_repeat_frequency"]),
                        max(1.0, float(policy["horizon_days"]) /
                            max(14, int(cycle_days or 45))))
        realization = float(policy["realization_repeat"])
    raw = max(0.0, typical * frequency * realization)
    return {
        "business_value_raw": round(raw, 6),
        "business_value_score": display_score(raw, policy),
        "business_expected_dfp_per_delivery": round(typical, 2),
        "business_deliveries_used": len(delivery_values),
    }


def contact_factor(days_since_reached: int | None,
                   latest_result: str = "", policy: dict = POLICY) -> float:
    if days_since_reached is None:
        # Unknown history is not the same as proven "never reached".
        return (1.0 if latest_result == "Ej anträffbar"
                else float(policy["unknown_dialogue_factor"]))
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
    if trigger in {
        "positive_dialogue_followup", "stockfiller_click_followup",
        "product_sheet_click_followup", "unanswered_retry_due",
        "unanswered_verify_contact_due",
    }:
        relevance = max(relevance, 0.75)
    if status in {"planned", "wait", "data_missing"} or (
        latest_result == "Ej anträffbar" and latest_attempt_date is not None
        and business_days_elapsed(latest_attempt_date, today)
            < int(policy["retry_business_days"])
    ):
        relevance = 0.0
    raw = max(0.0, float(business_value_raw or 0) * relevance)
    return {
        "contact_priority_raw": round(raw, 6),
        "contact_priority_score": display_score(raw, policy),
        "contact_priority_bucket": bucket,
        "contact_relevance_factor": round(relevance, 4),
    }
