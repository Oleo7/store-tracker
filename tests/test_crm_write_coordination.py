"""No production services: actual Lua scripts run in a local Redis emulator."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from contextlib import contextmanager
import os
import threading
import time
from unittest import TestCase
from unittest.mock import patch, MagicMock

import fakeredis
import crm_write_coordination as c
from crm_geocoding import apply_geocoding_results, GEO_COLUMNS


class WriterLeaseTests(TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis()
        self.key = 'isolated-crm-test'

    def lease(self, **kw):
        return c.Lease(self.redis, self.key, **kw)

    def test_second_writer_times_out_without_stealing_owner(self):
        first = self.lease().acquire()
        try:
            with self.assertRaises(c.CoordinationError):
                self.lease(wait=0).acquire()
            self.assertEqual(self.redis.get(self.key).decode(), first.token)
        finally:
            first.close()

    def test_renewal_is_owner_checked_and_lost_owner_cannot_unlock_successor(self):
        first = self.lease().acquire()
        self.redis.set(self.key, 'successor', px=180000)
        self.assertFalse(first.renew())
        with self.assertRaises(c.CoordinationError):
            first.check()
        first.close()
        self.assertEqual(self.redis.get(self.key), b'successor')

    def test_expired_lock_and_local_deadline_abort(self):
        lease = self.lease().acquire()
        try:
            lease.valid_until = time.monotonic()-1
            self.assertFalse(lease.renew())
            with self.assertRaises(c.CoordinationError):
                lease.check()
        finally:
            lease.close()

    def test_renew_preserves_token_and_extends_ttl(self):
        lease = self.lease().acquire()
        try:
            self.redis.pexpire(self.key, 1000)
            self.assertTrue(lease.renew())
            self.assertGreater(self.redis.pttl(self.key), 170000)
            lease.check()
        finally:
            lease.close()

    def test_redis_failure_never_degrades_to_process_local(self):
        with patch.object(self.redis, 'set', side_effect=ConnectionError('offline')):
            with self.assertRaises(c.CoordinationError):
                self.lease().acquire()

    def test_renewal_transport_failure_marks_permanent_loss(self):
        lease = self.lease().acquire()
        try:
            with patch.object(self.redis, 'eval', side_effect=ConnectionError('offline')):
                self.assertFalse(lease.renew())
            with self.assertRaises(c.CoordinationError):
                lease.check()
        finally:
            lease.close()

    def test_cancellation_releases_only_its_own_lease(self):
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL': 'redis://test', 'CRM_LOCK_SHEET_KEY': 'isolated'}), patch.object(c, 'redis_client', return_value=self.redis):
            with self.assertRaises(KeyboardInterrupt):
                with c.crm_write_lock():
                    raise KeyboardInterrupt()
            self.assertIsNone(self.redis.get(c.lock_key()))
            with c.crm_write_lock():
                c.assert_write_owned()

    def test_nested_writers_share_one_lease(self):
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL': 'redis://test', 'CRM_LOCK_SHEET_KEY': 'isolated'}), patch.object(c, 'redis_client', return_value=self.redis):
            with c.crm_write_lock() as first:
                with c.crm_write_lock() as nested:
                    self.assertIs(first, nested)
                first.check()
            self.assertIsNone(self.redis.get(c.lock_key()))

    def test_coordinated_flag_requires_a_url(self):
        with patch.dict(os.environ, {'CRM_AUTOMATED_WRITERS_COORDINATED': 'true', 'CRM_WRITE_LOCK_URL': '', 'PLANNING_DISTRIBUTED_LOCK_URL': ''}):
            with self.assertRaises(c.CoordinationError):
                with c.crm_write_lock():
                    self.fail('must not enter')

    def test_sheet_identity_is_the_same_for_web_and_jobs(self):
        self.assertEqual(c.lock_key(environment={'APP_ENV':'production','PRODUCTION_SHEET_KEY':'same'}),
                         c.lock_key(environment={'SHEET_KEY':'same'}))
        self.assertNotEqual(c.lock_key('same'), c.lock_key('staging'))

    def test_guard_aborts_mutation_after_lost_lease(self):
        client = MagicMock()
        c.guard_client(client)
        with patch.dict(os.environ, {'CRM_WRITE_LOCK_URL':'redis://test'}):
            with self.assertRaises(c.CoordinationError):
                client.http_client.request('post', 'https://sheets.invalid')
        client.http_client.set_timeout.assert_called_once_with((5, 30))

    def test_independent_clients_coordinate_through_redis(self):
        # No process-local lock here: independent Lease instances use Redis SET NX.
        active = maximum = 0
        meter = threading.Lock()
        def writer(_):
            nonlocal active, maximum
            lease = self.lease().acquire()
            try:
                with meter:
                    active += 1
                    maximum = max(maximum, active)
                time.sleep(.02)
                lease.check()
                with meter:
                    active -= 1
            finally:
                lease.close()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(writer, range(8)))
        self.assertEqual(maximum, 1)

    def test_configured_identity_cannot_silently_target_another_sheet(self):
        with self.assertRaises(c.CoordinationError):
            c.lock_key('actual-target', environment={'CRM_LOCK_SHEET_KEY':'wrong-target'})

    def test_independent_processes_coordinate_over_the_redis_protocol(self):
        import subprocess
        import sys
        server = fakeredis.TcpFakeServer(('127.0.0.1', 0), server_type='redis')
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = 'redis://127.0.0.1:' + str(server.server_address[1])
        program = """
import sys,time
from redis import Redis
from crm_write_coordination import Lease
r=Redis.from_url(sys.argv[1],socket_timeout=3)
r.incr('ready')
started=time.monotonic()
while int(r.get('ready') or 0)<2:
    assert time.monotonic()-started<5
    time.sleep(.01)
for _ in range(4):
    lease=Lease(r,'process-contention').acquire()
    try:
        assert r.incr('active')==1
        time.sleep(.02)
        lease.check()
        assert r.decr('active')==0
    finally:
        lease.close()
"""
        children=[]
        try:
            children=[subprocess.Popen([sys.executable,'-c',program,url],stdout=subprocess.PIPE,stderr=subprocess.PIPE) for _ in range(2)]
            for child in children:
                stdout,stderr=child.communicate(timeout=15)
                self.assertEqual(child.returncode,0,(stdout+stderr).decode())
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill(); child.communicate()
            server.shutdown(); server.server_close();thread.join(timeout=3)


class GeographyConflictTests(TestCase):
    def setUp(self):
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'web-app' / 'tests'))
        from test_planning import FakeWorksheet
        self.headers = ['customer_id', 'customer', 'sales_person', 'note', *GEO_COLUMNS]
        self.source = [{'customer_id':'c1','customer':'Butik','sales_person':'Sofia','note':'keep',
                        **dict.fromkeys(GEO_COLUMNS, '')}]
        self.sheet = FakeWorksheet('customers_enriched', self.headers, self.source)
        self.geo = {'c1': ('Göteborg', 'Gatan', '1', '41100', 'Västra Götaland', 57.7, 11.9)}

    def test_simultaneous_customer_edit_survives_geotagging(self):
        self.sheet.values[1][2] = 'Olle'
        self.sheet.values[1][3] = 'Återkom ej'
        self.assertEqual(apply_geocoding_results(self.sheet, self.source, self.geo, sheet_id='test'), 1)
        self.assertEqual(self.sheet.dict_rows()[0]['sales_person'], 'Olle')
        self.assertEqual(self.sheet.dict_rows()[0]['note'], 'Återkom ej')
        self.assertEqual(self.sheet.dict_rows()[0]['latitude_google'], 57.7)

    def test_manual_geography_edit_wins(self):
        self.sheet.values[1][self.headers.index('city_google')] = 'Manuell ort'
        before = deepcopy(self.sheet.values)
        self.assertEqual(apply_geocoding_results(self.sheet, self.source, self.geo, sheet_id='test'), 0)
        self.assertEqual(self.sheet.values, before)

    def test_duplicate_id_never_selects_a_row_by_position(self):
        self.sheet.values.append(list(self.sheet.values[1]))
        self.assertEqual(apply_geocoding_results(self.sheet, self.source, self.geo, sheet_id='test'), 0)
