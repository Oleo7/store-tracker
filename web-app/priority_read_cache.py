"""Immutable JSON-compatible containers for shared, process-local read models.

Only audited read views opt in to sharing. Other callers receive a normal,
independent mutable deep copy. Nested containers cannot be changed by a view.
"""
import copy
from datetime import datetime, timedelta


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


def next_time_transition(payload, current, parse_instant):
    """Conservative validity bound: midnight or any timestamp in source/state.

    No rounding of business time. Includes future recorded/created facts,
    permissions, plans, event payloads and snooze deadlines. Extra boundaries
    are harmless; omitting a relevant one would allow a stale decision.
    """
    boundary = datetime.combine(current.date() + timedelta(days=1), datetime.min.time(), tzinfo=current.tzinfo)
    seen = set()
    parsed = {}

    def visit(value):
        nonlocal boundary
        if isinstance(value, (dict, list, tuple)):
            if id(value) in seen:
                return
            seen.add(id(value))
            children = (v for k, v in value.items() if k not in {'as_of', 'snapshot_created_at'}) if isinstance(value, dict) else value
            for child in children:
                visit(child)
        elif isinstance(value, (str, datetime)):
            if isinstance(value, str) and (len(value) < 10 or not value[:4].isdigit()):
                return
            if value not in parsed:
                parsed[value] = parse_instant(value)
            instant = parsed[value]
            if instant and current <= instant < boundary:
                # At equality strict '< now' transitions require the next tick.
                boundary = instant if instant > current else instant + timedelta(microseconds=1)
    visit(payload)
    return boundary
