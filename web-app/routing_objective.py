"""One v3 route objective: maximum priority sum, then minimum driving time.

Scores use the same 0-100 index as Butiker. A six-decimal integer quantum avoids
floating-point ties depending on addition order. No weight on contact channel,
DFP, stop count, score-at-creation or another scoring policy exists here.
"""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import math

PRIORITY_UNITS_PER_POINT = 1_000_000
ROUTE_RANKING_CONTRACT = "v35-priority-lexicographic-1"


def priority_units(value):
    if isinstance(value, bool) or value is None:
        raise ValueError("Missing current v3 priority")
    try:
        number = Decimal(str(value))
        if not number.is_finite() or not 0 <= number <= 100:
            raise ValueError("Current v3 priority must be in [0, 100]")
        return int((number * PRIORITY_UNITS_PER_POINT).to_integral_value(rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Invalid current v3 priority") from exc


def priority_value(value):
    return priority_units(value) / PRIORITY_UNITS_PER_POINT


def route_objective_key(values, drive_seconds, stable_identity=()):
    if not math.isfinite(float(drive_seconds)) or drive_seconds < 0:
        raise ValueError("Invalid driving time")
    return (-sum(priority_units(v) for v in values), drive_seconds, tuple(stable_identity))


def skipped_priority_penalty(value, maximum_drive_seconds):
    """A one-quantum priority loss dominates EVERY feasible travel-time saving.

    Vehicle travel cost is one unit per driving second. The one-vehicle day has
    a hard duration bound; all other optional costs are zero. This encodes the
    lexicographic objective, not a calibrated commercial/time trade-off.
    """
    if maximum_drive_seconds <= 0:
        raise ValueError("A positive hard day limit is required")
    return priority_units(value) * (int(maximum_drive_seconds) + 1)
