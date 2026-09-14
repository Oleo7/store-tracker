"""Read-only synthetic comparison against master b98ed5d; prints JSON, never accesses CRM.

Run from the repository root: python scripts/commercial_v22_replay.py
All seller portfolios below are fabricated and identical by design.
"""
import ast
from collections import Counter
from datetime import datetime, timedelta
import json
from pathlib import Path
import subprocess
import sys
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'web-app'), str(ROOT / 'web-app/tests')]
import app
import priority
from test_planning import default_spreadsheet

BASE = 'b98ed5d2af3291ac4aa1aaa4e4f2daf2a3a3deea'
NOW = datetime(2026, 9, 10, 12, tzinfo=app.STOCKHOLM_ZONE)
old = types.ModuleType('priority_v21_baseline')
exec(subprocess.check_output(['git', 'show', f'{BASE}:web-app/priority.py'], cwd=ROOT).decode('utf-8'), old.__dict__)
old_app = ast.parse(subprocess.check_output(['git', 'show', f'{BASE}:web-app/app.py'], cwd=ROOT).decode('utf-8'))
legacy_functions = ast.Module(body=[node for node in old_app.body if isinstance(node, ast.FunctionDef) and node.name in {'build_route_optimization_inputs', 'active_planned_activity_queue_state'}], type_ignores=[])

customers, orders, contacts, planned, email_features, workflow = [], [], [], [], {}, {}
patterns = ['repeat', 'single', 'prospect_a', 'prospect_c', 'recent', 'negative', 'future', 'planned', 'overdue', 'replaced', 'stale_positive', 'click']
for seller in ('Johan', 'Daniel', 'Sofia'):
    for index in range(36):
        kind = patterns[index % len(patterns)]
        key = f'{seller.lower()}-{index}'
        row = len(customers) + 2
        store = dict(customer_id=key, customer_number=key, customer=key, row=row, sales_person=seller,
                     customer_segment='A' if kind == 'prospect_a' else 'B' if kind == 'single' else 'C',
                     latitude_google=57 + row / 10000, longitude_google=12 + row / 10000)
        if kind == 'prospect_c':
            store['latitude_google'] = ''
        customers.append(store)
        if kind not in {'prospect_a', 'prospect_c', 'stale_positive', 'click'}:
            for delivery in (['2026-01-01'] if kind == 'single' else ['2026-01-01', '2026-02-01']):
                orders.append({'customer_id': key, 'Customer': key, 'Customer number': key, 'Reference': f'{key}-{delivery}',
                               'Order date': delivery, 'Delivery date': delivery, 'Total weight': '10', 'Quantity': '99', 'Total': '200'})
        if kind == 'future':
            orders.append({**orders[-1], 'Reference': f'{key}-future', 'Order date': '2026-09-09', 'Delivery date': '2026-09-20'})
        if kind in {'recent', 'negative', 'replaced', 'stale_positive'}:
            age = {'recent': 1, 'negative': 30, 'replaced': 5, 'stale_positive': 40}[kind]
            contacts.append(dict(customer_id=key, customer=key, contact_id=f'contact-{key}', date_time=f'{NOW.date() - timedelta(days=age)} 10:00:00',
                                 sales_person=seller, result='Negativ' if kind == 'negative' else 'Positiv' if kind == 'stale_positive' else 'Neutral'))
        if kind in {'planned', 'overdue', 'replaced'}:
            planned.append(dict(customer_id=key, customer=key, planned_activity_id=f'planned-{key}', user_name=seller.lower(), sales_person=seller,
                                status='planned', contact_type='phone', source='manual', scheduled_at='2026-09-12T10:00:00+02:00' if kind == 'planned' else '2026-09-01T10:00:00+02:00'))
        if kind == 'click':
            email_features[key] = dict(customer_id=key, customer_key=key, email_followup_status='stockfiller_clicked_no_order',
                                       email_stockfiller_first_clicked_at='2026-09-05 10:00:00', email_followup_email_id=f'email-{key}')
        if kind == 'single' and index >= 12:
            workflow[key] = 'snoozed' if index < 24 else 'dismissed'

report = {'data': 'SYNTHETIC ONLY. No current complete authorized local CRM snapshot was available. July CSV fragments were not replayed.', 'baseline': BASE, 'as_of': NOW.isoformat(), 'portfolio_per_seller': 36, 'variants': []}
for label, module, policy in [('master_v2.1', old, 'v2.1'), ('corrected_rules_v2.1_weights', priority, 'v2.1'), ('full_v2.2', priority, 'v2.2')]:
    features = module.build_order_features(orders)
    ranked = module.build_priority_customers(customers, features, module.build_contact_features(contacts, features), None, NOW.date(),
        limit=len(customers), planned_activities=planned, email_features=email_features, workflow_suppressions=workflow, scoring_version=policy)
    snapshot = dict(customers=customers, priorities=ranked, contact_rows=contacts)
    result = {'variant': label, 'sellers': []}
    for seller in ('Johan', 'Daniel', 'Sofia'):
        owner = dict(user_name=seller.lower(), name=seller)
        globals_ = dict(app.__dict__)
        globals_.update(stockholm_now=lambda: NOW, get_authoritative_priority_snapshot=lambda *a, **kw: snapshot,
                        read_planned_activity_snapshot=lambda *a: (None, [], list(enumerate(planned, 2))),
                        priority_workflow_suppressions=lambda *a: workflow)
        if module is old:
            exec(compile(legacy_functions, '<baseline-route>', 'exec'), globals_)
        else:
            globals_['build_route_optimization_inputs'] = types.FunctionType(app.build_route_optimization_inputs.__code__, globals_)
            globals_['build_route_optimization_inputs'].__kwdefaults__ = app.build_route_optimization_inputs.__kwdefaults__
        with app.app.test_request_context(), patch.object(app, 'stockholm_now', return_value=NOW):
            inputs, error = globals_['build_route_optimization_inputs'](spreadsheet=default_spreadsheet(), owner=owner, route_date=NOW.date(), start=app.Coordinate(57.7, 11.9))
            active, overdue = globals_['active_planned_activity_queue_state'](planned, owner, contact_rows=contacts, now=NOW)
        own = [item for item in ranked if item['sales_person'] == seller]
        queue = [item for item in own if item['recommendation_eligible'] and item['primary_trigger_type'] and item['customer_id'] not in active]
        top = queue[:30]
        reasons = Counter(item['recommendation_suppression_reason'] for item in own if item['recommendation_suppression_reason'])
        route_ids = {row['customer_id'] for row in inputs['shipments']} if inputs else set()
        result['sellers'].append(dict(seller=seller,
            reactivation_with_relevant_trigger=sum(item['lifecycle'] == 'reactivation' and bool(item['primary_trigger_type']) for item in own),
            next_action_after_suppression=len(queue), top30_previous=sum(item['delivery_count'] > 0 for item in top),
            top30_prospects=sum(item['delivery_count'] == 0 for item in top), overdue=len(overdue), route_candidates=len(route_ids),
            restrictions=dict(reasons), active_planning=len(active), untrusted_coordinates=3))
    report['variants'].append(result)
print(json.dumps(report, ensure_ascii=False, indent=2))
