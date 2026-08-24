"""Process-local availability helpers for read-heavy Google Sheets access."""

from __future__ import annotations

from dataclasses import dataclass
import random
import threading
import time

from requests.exceptions import ConnectionError, Timeout


RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _response(error):
    return getattr(error, "response", None)


def error_status(error):
    response = _response(error)
    value = getattr(response, "status_code", None)
    if value is None:
        value = getattr(error, "status_code", None)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def retry_after_seconds(error):
    response = _response(error)
    headers = getattr(response, "headers", {}) or {}
    try:
        return max(0.0, float(headers.get("Retry-After") or 0))
    except (TypeError, ValueError):
        return 0.0


def is_retryable_read_error(error):
    return (
        isinstance(error, (ConnectionError, Timeout, TimeoutError))
        or error_status(error) in RETRYABLE_STATUS_CODES
    )


def read_with_retry(
    operation,
    *,
    attempts=4,
    base_delay=0.25,
    max_delay=4.0,
    sleep=time.sleep,
    random_value=random.random,
    on_retry=None,
):
    """Retry read-only operations for transient failures only."""
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as error:
            if attempt >= attempts - 1 or not is_retryable_read_error(error):
                raise
            retry_after = retry_after_seconds(error)
            exponential = min(max_delay, base_delay * (2 ** attempt))
            delay = max(retry_after, exponential * (0.75 + 0.5 * random_value()))
            if on_retry:
                on_retry(error, attempt + 1, attempts, delay)
            sleep(delay)


@dataclass(frozen=True)
class _Entry:
    expires_at: float
    rows: tuple


@dataclass(frozen=True)
class CacheReadInfo:
    cache_hit: bool
    waited_seconds: float
    performed_load: bool
    stored: bool
    invalidated_during_load: bool


class SheetReadCache:
    """Thread-safe TTL cache with per-dataset single-flight loading."""

    def __init__(self, *, ttl_seconds=12.0, monotonic=time.monotonic):
        self.ttl_seconds = float(ttl_seconds)
        self.monotonic = monotonic
        self._condition = threading.Condition(threading.RLock())
        self._entries = {}
        self._loading = set()
        self._worksheets = {}
        self._generation = 0
        self._global_epoch = 0
        self._spreadsheet_epochs = {}
        self._dataset_generations = {}

    @property
    def generation(self):
        with self._condition:
            return self._generation

    @staticmethod
    def _dataset_key(spreadsheet, title):
        return id(spreadsheet), str(title)

    @staticmethod
    def _copy(rows):
        return [list(row) for row in rows]

    def _version_token(self, key):
        spreadsheet_id, _title = key
        return (
            self._global_epoch,
            self._spreadsheet_epochs.get(spreadsheet_id, 0),
            self._dataset_generations.get(key, 0),
        )

    def generation_signature(self, spreadsheet, titles):
        spreadsheet_id = id(spreadsheet)
        normalized = sorted({str(title) for title in titles if str(title)})
        with self._condition:
            return tuple(
                (
                    title,
                    self._version_token((spreadsheet_id, title)),
                )
                for title in normalized
            )

    def worksheet(self, spreadsheet, title, *, loader=None):
        key = self._dataset_key(spreadsheet, title)
        with self._condition:
            existing = self._worksheets.get(key)
            if existing is not None:
                return existing
        operation = loader or (lambda: spreadsheet.worksheet(title))
        sheet = read_with_retry(operation)
        with self._condition:
            return self._worksheets.setdefault(key, sheet)

    def values_with_info(self, spreadsheet, title, *, loader):
        key = self._dataset_key(spreadsheet, title)
        waited_seconds = 0.0
        with self._condition:
            while True:
                now = self.monotonic()
                entry = self._entries.get(key)
                if entry is not None and entry.expires_at > now:
                    return self._copy(entry.rows), CacheReadInfo(
                        cache_hit=True,
                        waited_seconds=waited_seconds,
                        performed_load=False,
                        stored=False,
                        invalidated_during_load=False,
                    )
                if key not in self._loading:
                    self._loading.add(key)
                    load_token = self._version_token(key)
                    break
                wait_started_at = self.monotonic()
                self._condition.wait()
                waited_seconds += max(
                    0.0, self.monotonic() - wait_started_at
                )
        try:
            loaded = read_with_retry(loader)
            frozen = tuple(tuple(cell for cell in row) for row in (loaded or []))
            with self._condition:
                stored = self._version_token(key) == load_token
                if stored:
                    self._entries[key] = _Entry(
                        expires_at=self.monotonic() + self.ttl_seconds,
                        rows=frozen,
                    )
            return self._copy(frozen), CacheReadInfo(
                cache_hit=False,
                waited_seconds=waited_seconds,
                performed_load=True,
                stored=stored,
                invalidated_during_load=not stored,
            )
        finally:
            with self._condition:
                self._loading.discard(key)
                self._condition.notify_all()

    def values(self, spreadsheet, title, *, loader):
        rows, info = self.values_with_info(
            spreadsheet, title, loader=loader
        )
        return rows, info.cache_hit

    def invalidate(self, spreadsheet=None, *titles, worksheets=False):
        normalized = {str(title) for title in titles if str(title)}
        spreadsheet_id = id(spreadsheet) if spreadsheet is not None else None
        with self._condition:
            if spreadsheet_id is None and not normalized:
                self._global_epoch += 1
                self._entries.clear()
            elif spreadsheet_id is not None and not normalized:
                self._spreadsheet_epochs[spreadsheet_id] = (
                    self._spreadsheet_epochs.get(spreadsheet_id, 0) + 1
                )
                for key in list(self._entries):
                    if key[0] == spreadsheet_id:
                        self._entries.pop(key, None)
            else:
                known_keys = set(self._entries)
                known_keys.update(self._loading)
                known_keys.update(self._worksheets)
                known_keys.update(self._dataset_generations)
                if spreadsheet_id is not None:
                    target_keys = {
                        (spreadsheet_id, title) for title in normalized
                    }
                else:
                    target_keys = {
                        key for key in known_keys if key[1] in normalized
                    }
                for key in target_keys:
                    self._entries.pop(key, None)
                    self._dataset_generations[key] = (
                        self._dataset_generations.get(key, 0) + 1
                    )
            if worksheets:
                for key in list(self._worksheets):
                    if spreadsheet_id is not None and key[0] != spreadsheet_id:
                        continue
                    if normalized and key[1] not in normalized:
                        continue
                    self._worksheets.pop(key, None)
            self._generation += 1
            self._condition.notify_all()

    def clear(self):
        self.invalidate(worksheets=True)
