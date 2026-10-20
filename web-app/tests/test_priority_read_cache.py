import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import patch

from test_planning import default_spreadsheet, NOW
import app as a
from priority_read_cache import freeze, next_time_transition


class PriorityReadCacheTests(TestCase):
    def setUp(self):
        self.book = default_spreadsheet()
        self.book._store_tracker_enable_read_cache = True
        a._sheet_read_cache.clear()
        a.invalidate_priority_snapshot()
        self.addCleanup(a._sheet_read_cache.clear)
        self.addCleanup(a.invalidate_priority_snapshot)
        self.clock = patch.object(a, 'stockholm_now', return_value=NOW)
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def snapshot(self, **kwargs):
        return a.get_authoritative_priority_snapshot(self.book, today=NOW.date(), **kwargs)

    def test_live_requests_share_microsecond_differences_but_replay_is_exact(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            self.snapshot(as_of=NOW, live=True, read_only=True)
            self.snapshot(as_of=NOW+timedelta(microseconds=17), live=True, read_only=True)
            self.assertEqual(build.call_count, 1)
            self.snapshot(as_of=NOW)
            self.snapshot(as_of=NOW+timedelta(microseconds=17))
            self.assertEqual(build.call_count, 3)

    def test_four_simultaneous_views_build_once(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: self.snapshot(as_of=NOW, live=True, read_only=True), range(4)))
            self.assertEqual(build.call_count, 1)
            self.assertEqual(results[0], results[-1])

    def test_nested_raw_and_decision_data_cannot_leak_mutation(self):
        first = self.snapshot(read_only=True)
        with self.assertRaises(TypeError):
            first['customers'][0]['sales_person'] = 'Other seller'
        with self.assertRaises(TypeError):
            first['priorities'][0]['list_guidance']['can_contact_now'] = True
        first['priorities'][0]['sales_person'] = 'Local projection'
        second = self.snapshot(read_only=True)
        self.assertNotEqual(second['priorities'][0]['sales_person'], 'Local projection')
        mutable = self.snapshot()
        mutable['customers'][0]['sales_person'] = 'Mutable caller'
        mutable['priorities'][0]['list_guidance'].clear()
        self.assertNotEqual(self.snapshot()['customers'][0]['sales_person'], 'Mutable caller')
        self.assertTrue(self.snapshot()['priorities'][0]['list_guidance'])

    def test_cache_expiry_observes_direct_sheet_edit_without_write_invalidation(self):
        with patch.object(a._sheet_read_cache, 'monotonic', return_value=100.0):
            first = self.snapshot(read_only=True)
        sheet = self.book.worksheet('customers_enriched')
        column = sheet.values[0].index('sales_person')
        sheet.values[1][column] = 'Sofia'
        with patch.object(a._sheet_read_cache, 'monotonic', return_value=113.0):
            second = self.snapshot(read_only=True)
        self.assertEqual(first['customers'][0]['sales_person'], 'Olle')
        self.assertEqual(second['customers'][0]['sales_person'], 'Sofia')

    def test_planning_due_boundary_invalidates_live_result(self):
        sheet = self.book.worksheet(a.PLANNED_ACTIVITIES_SHEET)
        due = NOW+timedelta(seconds=1)
        row = dict(planned_activity_id='boundary', customer_id='11111111-1111-4111-8111-111111111111',
                   customer='Butik A', user_name='olle', sales_person='Olle', status='planned', scheduled_at=due.isoformat())
        sheet.values.append([row.get(h, '') for h in sheet.values[0]])
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            self.snapshot(as_of=NOW, live=True)
            self.snapshot(as_of=due+timedelta(microseconds=1), live=True)
            self.assertEqual(build.call_count, 2)

    def test_mutation_fresh_scope_bypasses_shared_model(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            self.snapshot(as_of=NOW, live=True)
            with patch.object(a._fresh_sheet_scope, 'entry', (self.book, {}), create=True):
                self.snapshot(as_of=NOW, live=True)
            self.assertEqual(build.call_count, 2)

    def test_snapshot_retention_is_bounded(self):
        for i in range(5):
            self.snapshot(as_of=NOW+timedelta(microseconds=i))
        self.assertLessEqual(len(a._priority_snapshot_entries), 2)

    def test_freezing_preserves_json_and_copy_returns_independent_mutable_values(self):
        original = {'a': [{'b': ['x']}], 'none': None}
        frozen = freeze(original)
        self.assertEqual(json.dumps(frozen), json.dumps(original))
        cloned = copy.deepcopy(frozen)
        cloned['a'][0]['b'].append('y')
        self.assertEqual(frozen['a'][0]['b'], ['x'])
        for mutation in (lambda: frozen.update(x=1), lambda: frozen['a'].append(1),
                         lambda: frozen['a'].__iadd__([1])):
            with self.assertRaises(TypeError):
                mutation()

    def test_midnight_snooze_future_fact_and_due_equality_bound_validity(self):
        for field in ('recorded_at', 'occurred_at', 'created_at', 'snooze_until', 'scheduled_at', 'date_time'):
            due = NOW+timedelta(seconds=3)
            self.assertEqual(next_time_transition({field: due.isoformat()}, NOW, a.parse_planning_instant), due)
        equal = next_time_transition({'scheduled_at': NOW.isoformat()}, NOW, a.parse_planning_instant)
        self.assertEqual(equal, NOW+timedelta(microseconds=1))
        self.assertEqual(next_time_transition({}, NOW, a.parse_planning_instant).date(), NOW.date()+timedelta(days=1))
