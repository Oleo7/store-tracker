"""Explicit CURRENT-v3 optimizer fixtures (never a runtime fallback).

Many transport tests specify a score to isolate geography/time handling. Those
fixtures must include the current index and permission contract, just as real
backend output does. Tests for missing data deliberately do not call this helper.
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo
from priority import SCORE_VERSION, build_priority_customers, build_order_features, build_contact_features
from customer_obligations import build_customer_state


def current_rank(row):
    score = row['priority_score']
    suppressed = bool(row.get('recommendation_suppression_reason'))
    guidance = dict(row.get('list_guidance') or row.get('customer_guidance') or {})
    guidance.setdefault('can_start_unscheduled_contact', not suppressed)
    guidance.setdefault('can_contact_now', not suppressed)
    return {**row, 'value_model_version':SCORE_VERSION, 'score_version':SCORE_VERSION,
            'work_priority_precise':float(score), 'work_priority_score':round(score),
            'contact_priority_precise':0.0 if suppressed else float(score),
            'contact_priority_score':0 if suppressed else round(score),
            'work_priority_raw':float(score), 'contact_priority_raw':0.0 if suppressed else float(score),
            'commercial_value':float(score), 'list_guidance':guidance,
            'ranking_mode':'v3', 'work_can_start_unscheduled_contact':not suppressed}


def current_snapshot(customers, scores, plans=(), contacts=(), orders=(), as_of=None):
    current = as_of or datetime.now(ZoneInfo('Europe/Stockholm'))
    if current.tzinfo is None:current=current.replace(tzinfo=ZoneInfo('Europe/Stockholm'))
    owners={str(c.get('customer_id') or ''):str(c.get('sales_person') or '').casefold() for c in customers}
    st=build_customer_state(customers,contacts,plans,as_of=current,owner_user_names=owners,order_rows=orders)
    of=build_order_features(list(orders),customers=customers,as_of=current)
    cf=build_contact_features(list(contacts),of,customers=customers,as_of=current)
    ps=build_priority_customers(customers,of,cf,None,current.date(),limit=len(customers),as_of=current,
        planned_activities=plans,obligation_states=st['obligations'],contact_permissions=st['contact_permissions'],planning_owner_user_names=owners)
    mapped=dict(scores) if isinstance(scores, dict) else {str(r.get('customer_id') or r.get('row')):r['priority_score'] for r in scores}
    for p in ps:
        score=mapped.get(str(p.get('customer_id')),mapped.get(str(p.get('row')),0))
        allowed=p['can_start_unscheduled_contact']; work=p['work_can_start_unscheduled_contact']
        p.update(priority_score=round(score),work_priority_score=round(score) if work else 0,
            work_priority_precise=float(score) if work else 0.0,work_priority_raw=float(score) if work else 0.0,
            contact_priority_score=round(score) if allowed else 0,contact_priority_precise=float(score) if allowed else 0.0,
            contact_priority_raw=float(score) if allowed else 0.0)
    return {'customers':customers,'priorities':ps,'order_rows':list(orders),'contact_rows':list(contacts),
            'planned_activity_rows':list(plans),'customer_state':st,'customer_state_events':[],
            'snapshot_created_at':current.isoformat(),'email_engagement':{}}
