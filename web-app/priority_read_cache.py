"""Immutable JSON-compatible containers for shared, process-local read models.

Only audited read views opt in to sharing. Other callers receive a normal,
independent mutable deep copy. Nested containers cannot be changed by a view.
"""
import copy
from collections import Counter
from datetime import datetime, timedelta, timezone
import re


_DATE_TEXT = re.compile(r'^(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[/.]\d{1,2}[/.]\d{4})(?:[ T]|$)')


def _immutable(*args, **kwargs):
    raise TypeError('Shared priority snapshots are immutable')


class FrozenDict(dict):
    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = _immutable

    def __deepcopy__(self, memo):
        result = {}
        memo[id(self)] = result
        result.update((copy.deepcopy(k, memo), copy.deepcopy(v, memo)) for k, v in self.items())
        return result


class FrozenList(list):
    __setitem__ = __delitem__ = append = extend = insert = pop = remove = clear = sort = reverse = __iadd__ = __imul__ = _immutable

    def __deepcopy__(self, memo):
        result = []
        memo[id(self)] = result
        result.extend(copy.deepcopy(v, memo) for v in self)
        return result


def freeze(value, memo=None):
    if isinstance(value, (FrozenDict, FrozenList)):
        return value
    memo = {} if memo is None else memo
    if id(value) in memo:
        return memo[id(value)]
    if isinstance(value, dict):
        result = FrozenDict()
        memo[id(value)] = result
        dict.update(result, ((k, freeze(v, memo)) for k, v in value.items()))
        return result
    if isinstance(value, list):
        result = FrozenList()
        memo[id(value)] = result
        list.extend(result, (freeze(v, memo) for v in value))
        return result
    if isinstance(value, tuple):
        return tuple(freeze(v, memo) for v in value)
    return value


def next_time_transition(payload, current, parse_instant, *, include_start=False):
    """Conservative validity bound: midnight or any timestamp in source/state.

    No rounding of business time. Includes future recorded/created facts,
    permissions, plans, event payloads and snooze deadlines. Extra boundaries
    are harmless; omitting a relevant one would allow a stale decision.
    """
    boundary = datetime.combine(current.date() + timedelta(days=1), datetime.min.time(), tzinfo=current.tzinfo)
    start = datetime.combine(current.date(), datetime.min.time(), tzinfo=current.tzinfo)
    current_ts = current.timestamp()
    seen = set()
    parsed = {}

    def visit(value, field=''):
        nonlocal boundary, start
        if isinstance(value, (dict, list, tuple)):
            if id(value) in seen:
                return
            seen.add(id(value))
            if isinstance(value, dict):
                for key, child in value.items():
                    if key not in {'as_of', '_calculated_as_of', 'snapshot_created_at'}:
                        visit(child, str(key).casefold().replace(' ', '_'))
            else:
                for child in value:
                    visit(child, field)
        elif isinstance(value, (str, datetime)):
            if isinstance(value, str):
                # Phone numbers, UUIDs, prices and SKU codes are not dates.
                # Excel serials and compact dates remain supported on actual
                # temporal fields through the same parser as the model.
                temporal = field in {'date', 'time', 'date_time'} or field.endswith(('_at', '_date', '_until', '_time'))
                if not value or not temporal and not _DATE_TEXT.match(value):
                    return
            if value not in parsed:
                instant = parse_instant(value)
                if instant is None:
                    from priority import parse_datetime
                    instant = parse_datetime(value)
                    if instant and instant.tzinfo is None:
                        instant = instant.replace(tzinfo=current.tzinfo)
                parsed[value] = instant
            instant = parsed[value]
            if instant:
                instant_ts = instant.timestamp()
                next_tick = lambda: (instant.astimezone(timezone.utc) + timedelta(microseconds=1)).astimezone(current.tzinfo)
                if instant_ts <= current_ts and instant_ts >= start.timestamp():
                    # Earlier requests in the SAME interval can share a build
                    # even if they arrived at the cache out of timestamp order.
                    start = instant if instant_ts == current_ts else next_tick()
                if current_ts <= instant_ts < boundary.timestamp():
                    # At equality strict '< now' transitions require the next tick.
                    boundary = instant if instant_ts > current_ts else next_tick()
    visit(payload)
    return (start, boundary) if include_start else boundary


def compact_records(records):
    """Losslessly omit repeated scalar defaults and duplicate guidance.

    The browser restores exactly the same record fields before handing them to
    the UI. Nothing is truncated, and older clients retain their existing API.
    """
    rows = {cid: dict(row) for cid, row in records.items()}
    for row in rows.values():
        if isinstance(row.get('list_guidance'), dict) and 'customer_guidance' in row and row['customer_guidance'] == row['list_guidance']:
            row.pop('customer_guidance', None)
            row['__guidance_alias'] = True
    defaults = {}
    common = set.intersection(*(set(row) for row in rows.values())) if rows else set()
    for field in sorted(common):
        counts = Counter((type(row[field]), row[field]) for row in rows.values()
                         if row[field] is None or type(row[field]) in (str, int, float, bool))
        if not counts:
            continue
        (value_type, value), count = counts.most_common(1)[0]
        if count < len(rows) / 2:
            continue
        defaults[field] = value
        for row in rows.values():
            if type(row[field]) is value_type and row[field] == value:
                del row[field]
    return {'__by_customer_id': rows, '__record_defaults': defaults}
