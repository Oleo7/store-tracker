"""Shared Stockholm workday rules for both route engines."""

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


STOCKHOLM = ZoneInfo("Europe/Stockholm")
WORKDAY_START = time(8)
LUNCH_START = time(12)
LUNCH_END = time(12, 45)
WORKDAY_END = time(17)
WORKDAY_SECONDS = 9 * 60 * 60
LUNCH_SECONDS = 45 * 60
WORKDAY_POLICY_VERSION = "stockholm-08-17-lunch-v1"


def workday_time(route_date, clock):
    return datetime.combine(route_date, clock, tzinfo=STOCKHOLM)


def effective_route_start(route_date, now):
    current = now.replace(tzinfo=STOCKHOLM) if now.tzinfo is None else now.astimezone(STOCKHOLM)
    start = workday_time(route_date, WORKDAY_START)
    if route_date == current.date():
        rounded = current.replace(second=0, microsecond=0)
        minutes = current.minute + bool(current.second or current.microsecond)
        rounded += timedelta(minutes=((minutes + 4) // 5) * 5 - current.minute)
        start = max(start, rounded)
    return skip_lunch_start(start)


def skip_lunch_start(start):
    start = start.replace(tzinfo=STOCKHOLM) if start.tzinfo is None else start.astimezone(STOCKHOLM)
    return max(start, workday_time(start.date(), LUNCH_END)) if (
        LUNCH_START <= start.time() < LUNCH_END
    ) else max(start, workday_time(start.date(), WORKDAY_START))


def route_workday_end(start):
    return workday_time(start.astimezone(STOCKHOLM).date(), WORKDAY_END)


def available_route_seconds(start):
    return max(0, int((route_workday_end(start) - start).total_seconds()))


def lunch_breaks(start):
    """Lunch is synthetic and never a planned customer activity."""
    local = start.astimezone(STOCKHOLM)
    if local >= workday_time(local.date(), LUNCH_START):
        return []
    return [{
        "activity_id": "system:lunch",
        "contact_type": "lunch",
        "scheduled_at": workday_time(local.date(), LUNCH_START),
        "duration_seconds": LUNCH_SECONDS,
    }]
