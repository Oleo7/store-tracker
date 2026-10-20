"""Offline, read-only endpoint benchmark. Private inputs/outputs must stay out of Git.

Run from the checkout: python scripts/benchmark_read_views.py --snapshot PRIVATE.json
--output PRIVATE-results.json. No customer values are included in the summary.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import cProfile
from datetime import datetime, timedelta
from itertools import count
import gc
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import pstats
import statistics
import sys
import time
import tracemalloc
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--repo')
    parser.add_argument('--profile-only', action='store_true')
    parser.add_argument('--no-profile', action='store_true')
    parser.add_argument('--only')
    parser.add_argument('--stability-only', action='store_true')
    parser.add_argument('--output', required=True)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--as-of', default='2026-10-20T10:00:00+02:00')
    args = parser.parse_args()
    root = Path(args.repo).resolve() if args.repo else Path(__file__).resolve().parents[1]
    os.environ.update(APP_ENV='test', GOOGLE_CREDENTIALS='{}', FLASK_SECRET_KEY='offline-benchmark', PERFORMANCE_LOGGING_ENABLED='false')
    sys.path.insert(0, str(root / 'web-app'))
    import app as a
    import requests
    import psutil
    raw = Path(args.snapshot).read_bytes()
    source = json.loads(raw)
    now = datetime.fromisoformat(args.as_of)

    class Sheet:
        def __init__(self, title, values):
            self.title, self.values, self.reads = title, values, 0
        def get_all_values(self):
            self.reads += 1
            width = max(map(len, self.values), default=0)
            return [list(row) + [''] * (width-len(row)) for row in self.values]
        def row_values(self, row):
            self.reads += 1
            return list(self.values[row-1]) if row <= len(self.values) else []
        def __getattr__(self, name):
            raise RuntimeError('Offline adapter forbids writes/unsupported operations: '+name)

    class Book:
        _store_tracker_enable_read_cache = True
        def __init__(self):
            self.sheets = {title: Sheet(title, data['values']) for title, data in source['sheets'].items()}
        def worksheet(self, title):
            if title not in self.sheets:
                raise a.WorksheetNotFound(title)
            return self.sheets[title]
        def __getattr__(self, name):
            raise RuntimeError('Offline adapter forbids writes: '+name)

    book = Book()
    a.app.config.update(TESTING=True, SECRET_KEY='offline-benchmark')
    a.app.logger.setLevel(logging.ERROR)
    a._sheet_read_cache.ttl_seconds = 3600  # isolate warm CPU from network TTL
    results = {'snapshot_sha256': hashlib.sha256(raw).hexdigest(), 'as_of': args.as_of, 'python': sys.version, 'runs': []}
    with ExitStack() as stack:
        for name in ('get_spreadsheet_with_retry', 'get_spreadsheet'):
            stack.enter_context(patch.object(a, name, return_value=book))
        clock_ticks = count(1)
        # Freeze the business instant while keeping each live call distinct.
        # A fully frozen clock would hide the production microsecond-key bug.
        stack.enter_context(patch.object(a, 'stockholm_now', side_effect=lambda: now + timedelta(microseconds=next(clock_ticks)) if not args.profile_only else now))
        stack.enter_context(patch.object(a, 'stockholm_today', return_value=now.date()))
        stack.enter_context(patch.object(requests.sessions.Session, 'request', side_effect=RuntimeError('Offline: network forbidden')))
        users = [u for u in a.get_user_rows(book) if a.is_yes(u.get('active'))]
        admin = next(u for u in users if a.user_is_admin(u))
        seller = next(u for u in users if not a.user_is_admin(u) and a.user_can_be_sales_owner(u, a.get_customer_rows(book)))
        endpoints = ['/customers', '/customer-insights', '/planning/activities?start=2026-10-19&end=2026-10-25', '/planning/activities?start=2026-10-19&end=2026-10-25&include_followups=0', '/planning/attention', '/followup-insights']

        def call(user, endpoint, compact=False):
            with a.app.test_client() as client:
                with client.session_transaction() as session:
                    session['user'] = user
                headers = {'X-PB-Value-Model': 'v3.5'}
                if compact:
                    headers['X-PB-Insights-Format'] = 'compact'
                started = time.perf_counter()
                response = client.get(endpoint, headers=headers)
                elapsed = time.perf_counter()-started
                if response.status_code != 200:
                    raise RuntimeError(f'{endpoint}: HTTP {response.status_code}')
                return elapsed, len(response.data)

        for role, user in ([] if args.profile_only or args.stability_only else [('admin', admin), ('seller', seller)]):
            for endpoint in endpoints:
                if args.only and not endpoint.startswith(args.only):
                    continue
                for compact in ([False, True] if endpoint == '/customer-insights' else [False]):
                    a._sheet_read_cache.clear()
                    a.invalidate_priority_snapshot()
                    cold, size = call(user, endpoint, compact)
                    for concurrency in (1, 2, 4):
                        durations = []
                        cpu = time.process_time()
                        reads = sum(s.reads for s in book.sheets.values())
                        with ThreadPoolExecutor(max_workers=concurrency) as pool:
                            for _ in range(args.rounds):
                                durations.extend(x[0] for x in pool.map(lambda _: call(user, endpoint, compact), range(concurrency)))
                        ordered = sorted(durations)
                        row = dict(role=role, endpoint=endpoint, compact=compact, concurrency=concurrency,
                                   cold_seconds=cold, median_seconds=statistics.median(durations), p95_seconds=ordered[min(len(ordered)-1, int(len(ordered)*.95))],
                                   response_bytes=size, cpu_seconds=time.process_time()-cpu,
                                   rss_bytes=psutil.Process().memory_info().rss,
                                   sheet_loads=sum(s.reads for s in book.sheets.values())-reads)
                        results['runs'].append(row)
                        print(json.dumps(row), flush=True)
        if args.stability_only:
            import threading
            process = psutil.Process()
            stopped = threading.Event()
            rss_samples = []
            def sample_memory():
                while not stopped.is_set():
                    rss_samples.append(process.memory_info().rss)
                    stopped.wait(.02)
            sampler = threading.Thread(target=sample_memory, daemon=True)
            sampler.start()
            original = a.build_current_priority_snapshot
            active_builds = max_builds = builds = 0
            build_lock = threading.Lock()
            def tracked_build(**kwargs):
                nonlocal active_builds, max_builds, builds
                with build_lock:
                    active_builds += 1
                    builds += 1
                    max_builds = max(max_builds, active_builds)
                try:
                    return original(**kwargs)
                finally:
                    with build_lock:
                        active_builds -= 1
            a._sheet_read_cache.clear()
            a.invalidate_priority_snapshot()
            health, heavy, plateau = [], [], []
            cpu_started = time.process_time()
            try:
                with patch.object(a, 'build_current_priority_snapshot', side_effect=tracked_build), ThreadPoolExecutor(max_workers=4) as pool:
                    for wave in range(12):
                        jobs = [pool.submit(call, user, path, compact) for user, path, compact in (
                            (admin, '/customer-insights', True),
                            (seller, '/planning/activities?start=2026-10-19&end=2026-10-25&include_followups=0', False),
                            (seller, '/planning/attention', False))]
                        health_job = pool.submit(call, seller, '/health')
                        health.append(health_job.result()[0])
                        heavy.extend(job.result()[0] for job in jobs)
                        gc.collect()
                        plateau.append(process.memory_info().rss)
            finally:
                stopped.set()
                sampler.join(2)
            results['stability'] = dict(waves=12, heavy_requests=len(heavy),
                peak_rss_bytes=max(rss_samples), rss_after_gc=plateau,
                health_p95_seconds=sorted(health)[int(len(health)*.95)],
                health_max_seconds=max(health), heavy_median_seconds=statistics.median(heavy),
                scoring_builds=builds, max_simultaneous_scoring_builds=max_builds,
                cpu_seconds=time.process_time()-cpu_started,
                limitations='Flask test clients on Windows; includes GIL contention but excludes HTTP worker queueing and Render CPU/memory limits')
            print(json.dumps(results['stability']), flush=True)
        if args.no_profile:
            Path(args.output).write_text(json.dumps(results, indent=2), encoding='utf-8')
            return
        a.invalidate_priority_snapshot()
        profiler = cProfile.Profile()
        tracemalloc.start()
        profiler.enable()
        payload = a.get_authoritative_priority_snapshot(book, today=now.date())
        profiler.disable()
        results['tracemalloc_current_peak'] = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        stream = io.StringIO()
        pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats('cumtime').print_stats(35)
        results['profile'] = stream.getvalue()
        results['active_priorities'] = len(payload['priorities'])
        results['gc'] = gc.get_stats()
    Path(args.output).write_text(json.dumps(results, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
