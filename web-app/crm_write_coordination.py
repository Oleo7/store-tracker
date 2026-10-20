"""Cooperative CRM writer lease. Sheets users can bypass it; this is not a CAS.

All automated writers share one key per spreadsheet. No schema writes, Redis
credentials, customer data or tokens are logged by this module.
"""
from contextlib import contextmanager
from functools import wraps
import hashlib
import os
import secrets
import threading
import time


class CoordinationError(RuntimeError):
    pass


_local = threading.local()
_process_lock = threading.RLock()
_clients = {}
_clients_lock = threading.Lock()
RENEW = """if redis.call('get',KEYS[1]) == ARGV[1] then
return redis.call('pexpire',KEYS[1],ARGV[2]) else return 0 end"""
RELEASE = """if redis.call('get',KEYS[1]) == ARGV[1] then
return redis.call('del',KEYS[1]) else return 0 end"""


def lock_url(environment=None):
    env = os.environ if environment is None else environment
    return (env.get('CRM_WRITE_LOCK_URL') or env.get('PLANNING_DISTRIBUTED_LOCK_URL') or '').strip()


def lock_key(sheet_id=None, environment=None):
    env = os.environ if environment is None else environment
    configured_id = env.get('CRM_LOCK_SHEET_KEY')
    if sheet_id and configured_id and sheet_id != configured_id:
        raise CoordinationError('CRM-låsets kalkylarksidentitet matchar inte skrivmålet.')
    sheet_id = sheet_id or configured_id
    if not sheet_id:
        mode = env.get('APP_ENV', '').lower()
        sheet_id = env.get('STAGING_SHEET_KEY') if mode == 'staging' else (
            env.get('PRODUCTION_SHEET_KEY') if mode == 'production' else env.get('SHEET_KEY'))
    if not sheet_id:
        raise CoordinationError('CRM-låset saknar kalkylarksidentitet.')
    return 'polarbar:crm-write:' + hashlib.sha256(sheet_id.encode()).hexdigest()


def redis_client(url):
    with _clients_lock:
        cache_key = (os.getpid(), url)
        if cache_key not in _clients:
            from redis import Redis
            _clients[cache_key] = Redis.from_url(url, socket_timeout=3, socket_connect_timeout=3,
                                                retry_on_timeout=False, max_connections=8)
        return _clients[cache_key]


class Lease:
    def __init__(self, client, key, *, ttl=180, wait=30):
        if ttl < 90:
            raise CoordinationError('CRM lease must outlive the bounded Sheets request.')
        self.client, self.key, self.ttl, self.wait = client, key, ttl, wait
        self.token = secrets.token_hex(32)
        self.stopped, self.lost = threading.Event(), threading.Event()
        self.valid_until = 0
        self.thread = None

    def acquire(self):
        deadline = time.monotonic() + self.wait
        while True:
            started = time.monotonic()
            try:
                acquired = self.client.set(self.key, self.token, nx=True, px=int(self.ttl * 1000))
            except Exception as exc:
                raise CoordinationError('CRM-låstjänsten kunde inte nås. Ingen ny skrivning påbörjas.') from exc
            if acquired:
                self.valid_until = started + self.ttl
                try:
                    self.check()
                    self.thread = threading.Thread(target=self._renew, daemon=True, name='crm-lease-renew')
                    self.thread.start()
                    return self
                except BaseException:
                    self.close()
                    raise
            if time.monotonic() >= deadline:
                raise CoordinationError('CRM uppdateras av en annan process. Försök igen med samma request-ID.')
            time.sleep(.1)

    def check(self):
        # Leave at least twice the 30-second HTTP read timeout before expiry.
        if self.lost.is_set() or time.monotonic() >= self.valid_until - 60:
            self.lost.set()
            raise CoordinationError('CRM-låset har förlorats eller kan inte verifieras. Försök igen med samma request-ID.')
        try:
            owned = self.client.get(self.key)
            if isinstance(owned, bytes):
                owned = owned.decode()
            if owned != self.token:
                raise CoordinationError('CRM-låsets ägare har ändrats.')
        except Exception as exc:
            self.lost.set()
            raise CoordinationError('CRM-låsets ägarskap kunde inte verifieras.') from exc
        if time.monotonic() >= self.valid_until - 60:
            self.lost.set()
            raise CoordinationError('CRM-låsets giltighetstid räcker inte för en skrivning.')

    def renew(self):
        started = time.monotonic()
        # Never revive an expired local lease, even if Redis still has its token.
        if self.lost.is_set() or started >= self.valid_until:
            self.lost.set()
            return False
        try:
            if not self.client.eval(RENEW, 1, self.key, self.token, int(self.ttl * 1000)):
                self.lost.set()
                return False
            self.valid_until = started + self.ttl
            return True
        except Exception:
            self.lost.set()
            return False

    def _renew(self):
        while not self.stopped.wait(self.ttl / 6):
            if not self.renew():
                return

    def close(self):
        self.stopped.set()
        if self.thread:
            self.thread.join(timeout=4)
        try:
            self.client.eval(RELEASE, 1, self.key, self.token)
        except Exception:
            # Expiry releases the key; never mask an interrupted operation.
            pass


def assert_write_owned():
    lease = getattr(_local, 'lease', None)
    if lock_url() and lease is None:
        raise CoordinationError('CRM-skrivning saknar gemensamt lås.')
    if lease:
        lease.check()


@contextmanager
def crm_write_lock(sheet_id=None):
    if not _process_lock.acquire(timeout=30):
        raise CoordinationError('CRM uppdateras redan. Försök igen med samma request-ID.')
    try:
        lease = getattr(_local, 'lease', None)
        if lease is not None:
            if sheet_id and lease.key != lock_key(sheet_id):
                raise CoordinationError('Nested CRM operation targets a different spreadsheet.')
            lease.check()
            yield lease
            return
        url = lock_url()
        if not url:
            if os.environ.get('CRM_AUTOMATED_WRITERS_COORDINATED', '').lower() == 'true':
                raise CoordinationError('Samordnad CRM-skrivning kräver CRM_WRITE_LOCK_URL.')
            yield None
            return
        lease = Lease(redis_client(url), lock_key(sheet_id)).acquire()
        _local.lease = lease
        try:
            yield lease
            lease.check()
        finally:
            _local.lease = None
            lease.close()
    finally:
        _process_lock.release()


def coordinated_write(function):
    @wraps(function)
    def wrapped(*args, **kwargs):
        source = kwargs.get('spreadsheet')
        if source is None and args:
            source = args[0]
        sheet_id = getattr(source, 'sheet_key', None) or getattr(source, 'id', None)
        with crm_write_lock(sheet_id):
            return function(*args, **kwargs)
    return wrapped


def guard_client(client):
    """Bound transport and check ownership at every actual Sheets mutation.

    A timeout can mean 'committed, response lost'. Callers must read back by
    stable ID; the guard never retries a mutation automatically.
    """
    http = client.http_client
    http.set_timeout((5, 30))
    original = http.request
    def guarded(method, *args, **kwargs):
        mutation = method.lower() not in {'get', 'head', 'options'}
        if mutation:
            assert_write_owned()
        response = original(method, *args, **kwargs)
        if mutation:
            assert_write_owned()
        return response
    http.request = guarded
    return client
