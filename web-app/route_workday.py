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
WORKDAY_POLICY_VERSION = "stockholm-08-17-bookings-first-lunch-v2"


class RouteLunchNotFeasible(ValueError):
    """The booked schedule has no uninterrupted 45-minute lunch slot."""


def workday_time(route_date, clock):
    return datetime.combine(route_date, clock, tzinfo=STOCKHOLM)


def effective_route_start(route_date, now, *, include_lunch=True):
    current = now.replace(tzinfo=STOCKHOLM) if now.tzinfo is None else now.astimezone(STOCKHOLM)
    start = workday_time(route_date, WORKDAY_START)
    if route_date == current.date():
        rounded = current.replace(second=0, microsecond=0)
        minutes = current.minute + bool(current.second or current.microsecond)
        rounded += timedelta(minutes=((minutes + 4) // 5) * 5 - current.minute)
        start = max(start, rounded)
    return skip_lunch_start(start) if include_lunch else start


def skip_lunch_start(start, confirmed_intervals=()):
    return plan_route_lunch(start, confirmed_intervals)[0]


def route_workday_end(start):
    return workday_time(start.astimezone(STOCKHOLM).date(), WORKDAY_END)


def available_route_seconds(start):
    return max(0, int((route_workday_end(start) - start).total_seconds()))


def plan_route_lunch(start, confirmed_intervals=(), *, appointment_tolerance_seconds=0):
    """Choose the closest free lunch slot; ties prefer the earlier slot.

    Only a nominal booking collision changes normal lunch. When moving it,
    protect the existing arrival tolerance as well as service, so the moved
    lunch cannot narrow a confirmed booking's window. A passed lunch is never
    scheduled again.
    """
    local = start.replace(tzinfo=STOCKHOLM) if start.tzinfo is None else start.astimezone(STOCKHOLM)
    local = max(local, workday_time(local.date(), WORKDAY_START))
    normal_start = workday_time(local.date(), LUNCH_START)
    normal_end = workday_time(local.date(), LUNCH_END)
    if local >= normal_end:
        return local, []
    bookings = sorted((begin.astimezone(STOCKHOLM), end.astimezone(STOCKHOLM))
                      for begin, end in confirmed_intervals)
    if not any(begin < normal_end and normal_start < end for begin, end in bookings):
        if local >= normal_start:
            return normal_end, []
        lunch_start = normal_start
    else:
        duration = timedelta(seconds=LUNCH_SECONDS)
        tolerance = timedelta(seconds=appointment_tolerance_seconds)
        bookings = [(begin - tolerance, end + tolerance) for begin, end in bookings]
        cursor = local
        candidates = []
        workday_end = route_workday_end(local)
        for begin, end in bookings + [(workday_end, workday_end)]:
            gap_end = min(begin, workday_end)
            if gap_end - cursor >= duration:
                candidates.append(max(cursor, min(normal_start, gap_end - duration)))
            cursor = max(cursor, end)
        if not candidates:
            raise RouteLunchNotFeasible("Ingen sammanhängande 45-minuters lunch ryms mellan de bokade besöken före 17:00.")
        lunch_start = min(candidates, key=lambda value: (abs((value - normal_start).total_seconds()), value))
    return local, [{
        "activity_id": "system:lunch",
        "contact_type": "lunch",
        "scheduled_at": lunch_start,
        "duration_seconds": LUNCH_SECONDS,
    }]


def lunch_breaks(start, confirmed_intervals=()):
    """Lunch is synthetic and never a planned customer activity."""
    return plan_route_lunch(start, confirmed_intervals)[1]
