"""Offline CRM acceptance and route-save benchmark; never writes source data.

Use --snapshot PRIVATE.json --baseline-root BASELINE --output SAFE-summary.json.
Only aggregate counts and timings are emitted. Provider times are deterministic;
these results measure CPU/memory, not Google network latency or route quality.
"""
import argparse
from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import sys
import time
from unittest.mock import patch


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--snapshot',required=True)
    parser.add_argument('--repo',default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument('--baseline-root',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--rounds',type=int,default=3)
    parser.add_argument('--as-of',help='ISO instant; defaults to snapshot capture time')
    parser.add_argument('--audit-only',action='store_true')
    args=parser.parse_args()
    root=Path(args.repo).resolve()
    os.environ.update(APP_ENV='test',GOOGLE_CREDENTIALS='{}',FLASK_SECRET_KEY='offline-acceptance',
                      CRM_WRITE_LOCK_URL='',PLANNING_DISTRIBUTED_LOCK_URL='',CRM_AUTOMATED_WRITERS_COORDINATED='false',
                      PERFORMANCE_LOGGING_ENABLED='false')
    sys.path[:0]=[str(root/'web-app'),str(root/'web-app'/'tests')]
    import app as a
    import test_planning as fixture
    import requests
    import psutil
    from customer_obligations import attention_items
    spec=importlib.util.spec_from_file_location('baseline_obligations',Path(args.baseline_root)/'web-app'/'customer_obligations.py')
    old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
    path=Path(args.snapshot); raw=path.read_bytes(); source=json.loads(raw)
    from zoneinfo import ZoneInfo
    now=datetime.fromisoformat(args.as_of or source['captured_at']).astimezone(ZoneInfo('Europe/Stockholm'))
    def book():
        sheets=[]
        for title,data in source['sheets'].items():
            sheet=fixture.FakeWorksheet(title)
            sheet.values=deepcopy(data['values'])
            sheet.row_count=max(len(sheet.values)+100,100)
            sheets.append(sheet)
        result=fixture.FakeSpreadsheet(sheets)
        result._store_tracker_enable_read_cache=True
        return result
    results={'snapshot_sha256':hashlib.sha256(raw).hexdigest(),'as_of':now.isoformat(),
             'source_rows':{k:max(0,len(v['values'])-1) for k,v in source['sheets'].items()},'routes':[]}
    process=psutil.Process()
    with ExitStack() as stack:
        stack.enter_context(patch.object(a,'stockholm_now',return_value=now))
        stack.enter_context(patch.object(a,'stockholm_today',return_value=now.date()))
        stack.enter_context(patch.object(requests.sessions.Session,'request',side_effect=AssertionError('Offline acceptance forbids network')))
        stack.enter_context(patch.object(a,'get_route_travel_time_provider',return_value=fixture.ConstantRoadProvider()))
        b=book();a.app.config.update(TESTING=True,SECRET_KEY='offline-acceptance')
        users=a.get_user_rows(b);customers=a.get_customer_rows(b)
        model=a.get_authoritative_priority_snapshot(b,today=now.date(),as_of=now,read_only=True)
        score_fields=('customer_id','business_value_raw','business_value_precise','contact_priority_precise','work_priority_precise')
        results['v35_priority_sha256']=hashlib.sha256(json.dumps(sorted([{k:r.get(k) for k in score_fields} for r in model['priorities']],key=lambda r:r['customer_id']),sort_keys=True).encode()).hexdigest()
        state=model['customer_state'];before_state=deepcopy(state)
        before=old.attention_items(state,as_of=now);after=attention_items(state,as_of=now)
        assert before_state==state
        results['attention']={'before':len(before),'after':len(after),'hidden':len(before)-len(after),
            'before_by_owner':dict(Counter(r['owner_user_name'] for r in before)),
            'after_by_owner':dict(Counter(r['owner_user_name'] for r in after)),
            'obligation_count_before':sum(len(v) for v in state['obligations'].values()),
            'obligation_count_after':sum(len(v) for v in state['obligations'].values()),'source_state_unchanged':True}
        for user in ([] if args.audit_only else users):
            if not a.is_yes(user.get('active')) or not a.user_can_be_sales_owner(user,customers):
                continue
            candidate_ids={str(r.get('customer_id') or '') for r in model['priorities'] if r.get('work_can_start_unscheduled_contact')}
            candidate_rows=[r['row'] for r in customers if str(r.get('customer_id') or '') in candidate_ids and a.customer_owned_by_user(r,user)][:8]
            if not candidate_rows:
                continue
            existing_days={a.parse_date_value(r.get('scheduled_at')) for r in model['planned_activity_rows'] if r.get('status')=='planned' and a.planning_owner_matches(r,user)}
            route_date=next(now.date()+timedelta(days=d) for d in range(7,40) if (now.date()+timedelta(days=d)).weekday()<5 and now.date()+timedelta(days=d) not in existing_days)
            saves=[];cpu=[];rss=[];counts=[];customers_scored=[]
            print('Validating isolated route for',user['user_name'],flush=True)
            for iteration in range(args.rounds):
                b=book();a._sheet_read_cache.clear();a.invalidate_priority_snapshot()
                before_sheets={k:deepcopy(s.values) for k,s in b.sheets.items()}
                with a.app.test_request_context():
                    from flask import session
                    session['user']=user
                    with patch.object(a,'get_spreadsheet_with_retry',return_value=b):
                        preview,error=a.build_planning_route_preview(spreadsheet=b,owner=user,
                            route_date=route_date,start=a.Coordinate(57.7,11.9),candidate_rows=candidate_rows)
                        if error:
                            raise RuntimeError('preview failed '+str(error[0].get_json()))
                        actual=a.build_current_priority_snapshot
                        def measured(**kwargs):
                            customers_scored.append(len(kwargs['customers']))
                            return actual(**kwargs)
                        started=time.perf_counter();cstarted=time.process_time()
                        with patch.object(a,'build_current_priority_snapshot',side_effect=measured):
                            result,error=a.apply_planning_route(spreadsheet=b,owner=user,preview=preview,
                                preview_token=preview['preview_token'],client_request_id='offline-acceptance-'+str(iteration))
                        saves.append(time.perf_counter()-started);cpu.append(time.process_time()-cstarted)
                        rss.append(process.memory_info().rss)
                        if error:
                            raise RuntimeError('save failed '+str(error[0].get_json()))
                        counts.append(result['imported_count'])
                        retry,error=a.apply_planning_route(spreadsheet=b,owner=user,preview=preview,
                            preview_token=preview['preview_token'],client_request_id='offline-acceptance-'+str(iteration))
                        assert error is None and retry['duplicate']
                        _,_,plans=a.read_planned_activity_snapshot(b,use_cache=False)
                        ids=[r['planned_activity_id'] for _,r in plans]
                        assert len(ids)==len(set(ids))
                        for title,values in before_sheets.items():
                            if title not in {'planned_activities','route_proposals'}:
                                assert b.sheets[title].values==values,title
                        original_plans=before_sheets['planned_activities']
                        id_col=original_plans[0].index('planned_activity_id')
                        assert {r[id_col] for r in original_plans[1:] if len(r)>id_col} <= set(ids)
            results['routes'].append({'owner':user['user_name'],'route_date':route_date.isoformat(),'rounds':args.rounds,
                'median_save_seconds':statistics.median(saves),'p95_save_seconds':max(saves),
                'median_cpu_seconds':statistics.median(cpu),'peak_rss_bytes':max(rss),
                'customers_scored_per_save':customers_scored,'new_visit_counts':counts,
                'duplicate_retry_verified':True,'source_contacts_orders_permissions_unchanged':True,
                'original_activity_ids_retained':True})
    assert path.read_bytes()==raw
    Path(args.output).write_text(json.dumps(results,indent=2),encoding='utf-8')
    print(json.dumps(results,indent=2))


if __name__=='__main__':
    main()
