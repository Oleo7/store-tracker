import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
from unittest import TestCase
from unittest.mock import patch

from test_planning import default_spreadsheet, NOW
import app as a
from priority_read_cache import freeze, next_time_transition, compact_records


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

    def test_live_requests_can_reach_cache_in_reverse_timestamp_order(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            self.snapshot(as_of=NOW+timedelta(microseconds=17), live=True, read_only=True)
            self.snapshot(as_of=NOW, live=True, read_only=True)
            self.assertEqual(build.call_count, 1)

    def test_calendar_without_planned_contacts_does_not_score_portfolio(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as scoring, patch.object(
                a, 'build_customer_state', wraps=a.build_customer_state) as state:
            self.snapshot(as_of=NOW, live=True, read_only=True, state_only=True)
            self.assertEqual(scoring.call_count, 0)
            self.snapshot(as_of=NOW+timedelta(microseconds=5), live=True, read_only=True)
            self.assertEqual(scoring.call_count, 1)
            self.assertEqual(state.call_count, 1)

    def test_explicit_invalidation_during_build_prevents_publication(self):
        original = a.build_current_priority_snapshot
        def invalidating_build(**kwargs):
            result = original(**kwargs)
            a.invalidate_priority_snapshot()
            return result
        with patch.object(a, 'build_current_priority_snapshot', side_effect=invalidating_build):
            self.snapshot()
        self.assertEqual(a._priority_snapshot_entries, {})

    def test_compact_response_is_lossless_including_missing_and_null_fields(self):
        records = {'a': {'score': 0, 'flag': False, 'list_guidance': {'x': ['a']}, 'customer_guidance': {'x': ['a']}},
                   'b': {'score': None, 'flag': False, 'list_guidance': {'x': ['b']}, 'customer_guidance': {'x': ['b']}},
                   'c': {'score': 0, 'flag': True, 'other': 'unique', 'list_guidance': None},
                   'd': {'score': 0, 'flag': False, 'list_guidance': None, 'customer_guidance': None}}
        wire = compact_records(records)
        restored = {}
        for cid, row in wire['__by_customer_id'].items():
            row = {**wire['__record_defaults'], **row}
            if row.pop('__guidance_alias', False):
                row['customer_guidance'] = dict(row['list_guidance'])
            restored[cid] = row
        self.assertEqual(restored, records)
        self.assertEqual(records['a']['score'], 0)

    def test_nested_raw_and_decision_data_cannot_leak_mutation(self):
        first = self.snapshot(read_only=True)
        with self.assertRaises(TypeError):
            first['customers'][0]['sales_person'] = 'Other seller'
        with self.assertRaises(TypeError):
            first['priorities'][0]['list_guidance']['can_contact_now'] = True
        with self.assertRaises(TypeError):
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

    def test_ttl_refresh_reuses_scores_only_after_verifying_unchanged_sources(self):
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            with patch.object(a._sheet_read_cache, 'monotonic', return_value=100.0):
                first = self.snapshot(read_only=True)
            sheet = self.book.worksheet('customers_enriched')
            with patch.object(sheet, 'get_all_values', wraps=sheet.get_all_values) as read:
                with patch.object(a._sheet_read_cache, 'monotonic', return_value=113.0):
                    second = self.snapshot(read_only=True)
                self.assertGreater(read.call_count, 0)
            self.assertEqual(build.call_count, 1)
            self.assertEqual(first, second)

    def test_future_refusal_becomes_effective_even_when_sources_are_identical(self):
        sheet = self.book.worksheet('sales_activities')
        due = NOW+timedelta(seconds=1)
        row = dict(contact_id='future-refusal', customer_id='11111111-1111-4111-8111-111111111111',
                   customer='Butik A', sales_person='Olle', contact_channel='Telefon',
                   date_time=due.isoformat(), created_at=due.isoformat(), result='Återkom ej')
        sheet.values.append([row.get(h, '') for h in sheet.values[0]])
        with patch.object(a, 'build_current_priority_snapshot', wraps=a.build_current_priority_snapshot) as build:
            first = self.snapshot(as_of=NOW, live=True)
            second = self.snapshot(as_of=due+timedelta(microseconds=1), live=True)
            self.assertEqual(build.call_count, 2)
        before = next(r for r in first['priorities'] if r['customer_id'] == row['customer_id'])
        after = next(r for r in second['priorities'] if r['customer_id'] == row['customer_id'])
        self.assertNotEqual(before.get('contact_permission_status'), 'do_not_contact')
        self.assertEqual(after['contact_permission_status'], 'do_not_contact')

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

    def test_fresh_mutation_scope_does_not_wait_for_a_shared_read_builder(self):
        a._priority_snapshot_loading.add(('blocked-read',))
        try:
            with patch.object(a._fresh_sheet_scope, 'entry', (self.book, {}), create=True):
                result = self.snapshot(as_of=NOW)
            self.assertTrue(result['priorities'])
        finally:
            a._priority_snapshot_loading.discard(('blocked-read',))

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

    def test_time_interval_orders_both_dst_folds_by_instant(self):
        before = a.parse_planning_instant('2026-10-25T02:45:00+02:00')
        event = a.parse_planning_instant('2026-10-25T02:15:00+01:00')
        after = a.parse_planning_instant('2026-10-25T02:30:00+01:00')
        lower, upper = next_time_transition({'recorded_at': event.isoformat()}, before, a.parse_planning_instant, include_start=True)
        self.assertEqual(upper.timestamp(), event.timestamp())
        self.assertLess(before.timestamp(), upper.timestamp())
        lower, upper = next_time_transition({'recorded_at': event.isoformat()}, after, a.parse_planning_instant, include_start=True)
        self.assertGreater(lower.timestamp(), event.timestamp())
        self.assertLess(lower.timestamp(), after.timestamp())
