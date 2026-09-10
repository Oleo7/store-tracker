"""Shared commercial volume contract. Never infer DFP from Quantity."""

import math


def order_volume(row):
    value = row.get("Total weight")
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(str(value).replace("\xa0", "").replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def commercial_order(total, volume):
    """Positive sales establish identity/history even when volume is unknown."""
    return total > 0 and (volume is None or volume > 0)
