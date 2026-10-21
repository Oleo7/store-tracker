"""Customer activity contract: synthetic snapshots and actual isolated Flask writes.

Replaces source-specific promise outcomes with chronological seller work. No
production data, external sends or CRM credentials are used.
"""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from unittest import TestCase
from unittest.mock import patch
from zoneinfo import ZoneInfo

from test_planning import PlanningApiTestCase, NOW
import app as app_module
from customer_obligations import build_customer_state, attention_items, instant
from priority import build_priority_customers, build_order_features, build_contact_features, apply_workflow_suppressions

ZONE = ZoneInfo('Europe/Stockholm')
AT = datetime(2026, 10, 21, 12, tzinfo=ZONE)
CID = '11111111-1111-4111-8111-111111111111'
C = {'customer_id': CID, 'customer': 'Butik A', 'sales_person': 'Olle', 'customer_segment': 'A', 'phone': '0701234567'}


def plan(aid='p1', due='2026-10-19T09:00:00+02:00', **extra):
    return {**dict(planned_activity_id=aid, customer_id=CID, customer='Butik A', user_name='olle',
                contact_type='phone', scheduled_at=due, created_at='2026-10-01T09:00:00+02:00', status='planned'), **extra}


def contact(cid='contact', at='2026-10-20T09:00:00+02:00', result='Neutral', **extra):
    return {**dict(contact_id=cid, customer_id=CID, customer='Butik A', date_time=at,
                contact_channel='Telefon', sales_person='Olle', result=result), **extra}


def state(plans=(), contacts=(), events=(), customers=None, at=AT, **extra):
    return build_customer_state(customers or [C], contacts, plans, events, as_of=at, **extra)


def missed(model, at=AT, **extra):
    return attention_items(model, as_of=at, **extra)


class ActivityChronologyTests(TestCase):
    def test_old_virtual_closure_does_not_close_new_source_linked_booking(self):
        source=contact(at='2026-10-01T09:00+02:00',follow_up_date='2026-10-10')
        for status in ['resolved','cancelled','skipped']:
            with self.subTest(status=status):
                event=dict(kind='obligation',customer_id=CID,entity_id='followup:contact',
                    occurred_at='2026-10-15T10:00+02:00',payload_json=json.dumps({'status':status}))
                old=state(contacts=[source],events=[event])['obligations'][CID][0]
                self.assertEqual(old['status'],status)
                booking=plan('new','2026-10-25T09:00+01:00',source_contact_id='contact',
                    created_at='2026-10-20T10:00+02:00',obligation_id='followup:contact')
                self.assertEqual(state([booking],[source],[event])['obligations'][CID][0]['status'],'scheduled')

    def test_closure_is_scoped_to_activity_even_when_source_and_creation_time_match(self):
        source=contact(at='2026-10-01T09:00+02:00')
        bookings=[plan(aid,'2026-10-25T09:00+01:00',source_contact_id='contact',
                       obligation_id='followup:contact') for aid in ['old','new']]
        for field in ['planned_activity_id','original_planned_activity_id']:
            with self.subTest(field=field):
                event=dict(kind='obligation',customer_id=CID,entity_id='followup:contact',
                    occurred_at='2026-10-20T10:00+02:00',payload_json=json.dumps({'status':'resolved',field:'old'}))
                items=state(bookings,[source],[event])['obligations'][CID]
                self.assertEqual({r['planned_activity_id']:r['status'] for r in items},{'old':'resolved','new':'scheduled'})

    def test_three_misses_one_customer_and_notes_not_separate_cases(self):
        rows=[plan('old', '2026-10-17T10:00+02:00', note='reklamation'),
              plan('middle', '2026-10-18T10:00+02:00', customer_agreement_confirmed='Y'),
              plan('latest', note='lovat demo', appointment_confirmed='Y')]
        before=deepcopy(rows); result=missed(state(rows))
        self.assertEqual(len(result), 1); self.assertEqual(result[0]['planned_activity_id'], 'latest')
        self.assertEqual(result[0]['older_missed_count'], 2); self.assertEqual(rows,before)
        self.assertEqual(result[0]['actions'], ['log_contact', 'reschedule', 'skip'])

    def test_same_name_different_ids(self):
        other={**C,'customer_id':'other'}
        rows=[plan(),{**plan('p2'),'customer_id':'other'}]
        self.assertEqual({r['customer_id'] for r in missed(state(rows,customers=[C,other]))},{CID,'other'})

    def test_owner_transfer_preserves_work_and_actor(self):
        rows=[plan()]; contacts=[contact(at='2026-10-01T08:00+02:00')]
        model=state(rows,contacts,owner_user_names={CID:'sofia'})
        self.assertEqual(len(missed(model,owner_user_name='SOFIA')),1)
        self.assertEqual(missed(model,owner_user_name='olle'),[])
        self.assertEqual(contacts[0]['sales_person'],'Olle')

    def test_all_contact_results_cover_misses_without_completing_them(self):
        for result in ['Positiv','Neutral','Negativ','Ej anträffbar','Order lagd!']:
            with self.subTest(result=result):
                model=state([plan('p1'),plan('p2')],[contact(result=result)])
                self.assertEqual(missed(model),[])
                self.assertEqual({r['status'] for r in model['obligations'][CID]},{'superseded'})
                self.assertEqual(bool(model['latest_reached']),result in {'Positiv','Neutral','Negativ'})

    def test_backdated_contact_and_recorded_time(self):
        rows=[plan()]
        old=contact(at='2026-10-18T11:00+02:00',recorded_at='2026-10-21T09:00+02:00')
        self.assertEqual(len(missed(state(rows,[old]))),1)
        later={**old,'date_time':'2026-10-20T10:00+02:00','recorded_at':'2026-10-22T09:00+02:00'}
        self.assertEqual(len(missed(state(rows,[later]))),1)
        self.assertEqual(missed(state(rows,[{**later,'recorded_at':'2026-10-21T09:00+02:00'}])),[])

    def test_future_contact_not_evidence(self):
        self.assertEqual(len(missed(state([plan()],[contact(at='2026-10-22T09:00+02:00')]))),1)

    def test_new_plan_replaces_only_earlier_work(self):
        early=plan('future','2026-10-23T10:00+02:00')
        self.assertEqual(len(missed(state([plan(),early]))),1)
        late={**early,'created_at':'2026-10-20T10:00+02:00'}
        model=state([plan(),late]); self.assertEqual(missed(model),[])
        next_model=state([plan(),late],at=AT+timedelta(days=4))
        self.assertEqual([r['planned_activity_id'] for r in missed(next_model,at=AT+timedelta(days=4))],['future'])

    def test_note_edit_is_not_planning(self):
        rows=[plan(),plan('future','2026-10-23T10:00+02:00',updated_at='2026-10-21T10:00+02:00',note='ändrad')]
        self.assertEqual(len(missed(state(rows))),1)
        rows[1]['planning_decided_at']='2026-10-20T09:00+02:00'
        self.assertEqual(missed(state(rows)),[])

    def test_cancelled_replacement_does_not_resurrect_history(self):
        for status in ['cancelled','skipped']:
            with self.subTest(status=status):
                model=state([plan(),plan('future','2026-10-23T10:00+02:00',
                    planning_decided_at='2026-10-20T09:00+02:00',status=status)])
                self.assertEqual(missed(model),[])
                self.assertFalse(any(r['status']=='scheduled' for r in model['obligations'][CID]))

    def test_skipping_older_miss_preserves_newer_miss(self):
        rows=[{**plan('old','2026-10-17T09:00+02:00'),'status':'skipped'},plan('latest')]
        self.assertEqual([r['planned_activity_id'] for r in missed(state(rows))],['latest'])
        rows[1]['status']='skipped'
        self.assertEqual(missed(state(rows)),[])

    def test_future_skip_is_not_contact_or_new_cutoff(self):
        model=state([plan(),{**plan('future','2026-10-23T09:00+02:00'),'status':'skipped'}])
        self.assertEqual(len(missed(model)),1); self.assertEqual(model['latest_attempts'],{})

    def test_new_contact_keeps_both_future_appointments(self):
        rows=[plan(),plan('tomorrow','2026-10-22T10:00+02:00'),plan('demo','2026-10-24T10:00+02:00')]
        model=state(rows,[contact()]);self.assertEqual(missed(model),[])
        self.assertEqual({r['planned_activity_id'] for r in model['obligations'][CID] if r['status']=='scheduled'},{'tomorrow','demo'})

    def test_route_pending_has_no_planning_effect(self):
        rows=[plan(),{**plan('route','2026-10-23T09:00+02:00',planning_decided_at='2026-10-20T10:00+02:00'),'status':'route_pending'}]
        self.assertEqual(len(missed(state(rows))),1)
        rows[1]['status']='planned'; self.assertEqual(missed(state(rows)),[])

    def test_history_permission_and_manual_states_are_inert(self):
        events=[dict(kind=kind,customer_id=CID,entity_id='calendar:p1',occurred_at='2026-10-18T10:00+02:00',
                     payload_json=json.dumps({'status':status,'snooze_until':'2026-11-21'}))
                for kind,status in [('permission','do_not_contact'),('workflow','dismissed'),('obligation','needs_review')]]
        model=state([plan()],events=events)
        self.assertEqual(len(missed(model)),1);self.assertEqual(model['contact_permissions'],{})
        self.assertFalse(missed(model)[0]['review_only'])

    def test_legacy_closure_not_counted_as_contact(self):
        event=dict(kind='obligation',customer_id=CID,entity_id='calendar:p1',occurred_at='2026-10-20T10:00+02:00',payload_json='{"status":"resolved"}')
        model=state([plan()],events=[event]);self.assertEqual(missed(model),[])
        self.assertEqual(model['latest_reached'],{}); self.assertEqual(model['latest_attempts'],{})

    def test_virtual_followup_has_no_fabricated_booking_time(self):
        model=state(contacts=[contact(at='2026-10-01T09:00+02:00',follow_up_date='2026-10-19')])
        item=missed(model)[0];self.assertEqual(item['due_at'],'');self.assertEqual(item['planned_activity_id'],'')
        self.assertFalse(item['appointment_confirmed'])

    def test_invalid_identity_and_deleted_customer_isolated(self):
        rows=[plan(),{**plan('unknown'),'customer_id':'deleted'},
              {**plan('badsource'),'source_contact_id':'foreign'}]
        other={**C,'customer_id':'foreign-customer'}
        model=state(rows,[contact(cid='foreign',customer_id='foreign-customer')],customers=[C,other])
        self.assertEqual([r['planned_activity_id'] for r in missed(model)],['p1'])
        self.assertEqual(state(rows,customers=[{**C,'cancelled_flag':'Y'}])['obligations'],{})

    def test_ambiguous_legacy_identity_never_becomes_mutable(self):
        legacy=contact(at='2026-10-01T09:00+02:00',follow_up_date='2026-10-19');legacy.pop('contact_id')
        self.assertEqual(missed(state(contacts=[legacy,{**legacy,'comment':'annan anteckning'}])),[])

    def test_search_only_latest_representative(self):
        model=state([plan('old','2026-10-17T10:00+02:00',note='gammal'),plan('new',note='senaste')])
        self.assertEqual(missed(model,query='gammal'),[]);self.assertEqual(len(missed(model,query='senaste')),1)

    def test_stockholm_midnight_and_dst(self):
        for at in ['2026-03-29T00:01+01:00','2026-10-25T00:01+02:00']:
            now=instant(at);yesterday=(now-timedelta(days=1)).date().isoformat()
            model=state([plan(due=yesterday+'T23:00:00'+at[-6:],created_at='2026-01-01T09:00+01:00')],at=now)
            self.assertEqual(len(missed(model,at=now)),1)
        model=state([plan(due='2026-10-21T08:00+02:00')]);self.assertEqual(missed(model),[])

    def test_order_import_and_technical_email_never_handle_miss(self):
        for extra in [dict(activity_source='crm_email',email_id='e'),dict(result='Öppnat mejl')]:
            row={**contact(),**extra};self.assertEqual(len(missed(state([plan()],[row]))),1)
        orders=[{'customer_id':CID,'Customer':'Butik A','Reference':'o','Delivery date':'2026-10-20','Total weight':'20'}]
        self.assertEqual(len(missed(state([plan()],order_rows=orders))),1)

    def test_workflow_cannot_change_commercial_value_or_priority(self):
        raw=[{'customer_id':CID,'priority_score':75,'business_value_raw':42}]
        self.assertEqual(apply_workflow_suppressions(raw,{CID:'dismissed'}),raw)

    def test_same_day_events_keep_actual_instant_order(self):
        rows=[plan('early','2026-10-19T09:00+02:00'),plan('late','2026-10-19T15:00+02:00')]
        model=state(rows,[contact(at='2026-10-19T12:00+02:00')])
        self.assertEqual([r['planned_activity_id'] for r in missed(model)],['late'])

    def test_repeated_dst_hour_is_ordered_by_instant(self):
        rows=[plan('early','2026-10-25T02:15+02:00'),plan('late','2026-10-25T02:45+01:00')]
        now=instant('2026-10-26T10:00+01:00')
        model=state(rows,[contact(at='2026-10-25T02:30+01:00')],at=now)
        self.assertEqual([r['planned_activity_id'] for r in missed(model,at=now)],['late'])

    def test_future_contact_in_repeated_dst_hour_is_not_known_yet(self):
        now=instant('2026-10-25T02:30+02:00')
        model=state([plan(due='2026-10-24T09:00+02:00')],
                    [contact(at='2026-10-25T02:15+01:00')],at=now)
        self.assertEqual(len(missed(model,at=now)),1)
        self.assertEqual(model['latest_attempts'],{})

    def test_bad_legacy_event_isolated_from_valid_work(self):
        event=dict(kind='obligation',customer_id=CID,entity_id='calendar:p1',occurred_at='2026-10-20T10:00+02:00',payload_json='{invalid')
        self.assertEqual(len(missed(state([plan()],events=[event]))),1)


class ActivityLoopApiTests(PlanningApiTestCase):
    def seed_misses(self):
        for n in [23,24,25]:
            self.append_planning_row(planned_activity_id=f'miss-{n}',scheduled_at=f'2026-07-{n}T09:00+02:00',
                created_at='2026-07-01T09:00+02:00',note='lovat demo' if n==25 else '')

    def attention(self, suffix=''):
        response=self.client.get('/planning/attention?user_name=olle'+suffix)
        self.assertEqual(response.status_code,200,response.get_json());return response.get_json()

    def log(self, **extra):
        payload=dict(customer_id=CID,date_time=NOW.isoformat(),contact_channel='Telefon',result='Neutral',
                     comment='Faktisk kontakt',client_request_id='loop-contact')
        payload.update(extra);return self.client.post('/customers/Butik%20A/contacts',json=payload)

    def new_plan(self,**extra):
        payload=dict(customer_id=CID,scheduled_at='2026-07-28T14:00+02:00',contact_type='phone',client_request_id='new-loop-plan')
        payload.update(extra);return self.client.post('/planning/activities',json=payload)

    def test_log_from_all_three_entry_points(self):
        self.seed_misses();self.assertEqual(self.attention()['total'],1)
        for result in ['Neutral','Positiv','Negativ','Ej anträffbar','Order lagd!']:
            with self.subTest(result=result):
                response=self.log(result=result,client_request_id='loop-'+result)
                self.assertEqual(response.status_code,200,response.get_json())
                self.assertEqual(self.attention()['total'],0)
        self.assertEqual(len(self.contact_rows()),5)
        self.assertEqual({r['status'] for r in self.planning_rows()},{'planned'})

    def test_linked_contact_completes_only_selected_activity(self):
        self.seed_misses();response=self.log(planned_activity_id='miss-25')
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)
        self.assertEqual(sum(r['status']=='completed' for r in self.planning_rows()),1)
        self.assertEqual(len(self.contact_rows()),1)

    def test_successful_plan_suppresses_misses_and_keeps_future_booking(self):
        self.seed_misses();self.append_planning_row(planned_activity_id='demo')
        response=self.new_plan();self.assertEqual(response.status_code,201,response.get_json())
        self.assertEqual(self.attention()['total'],0);self.assertEqual(len(self.planning_rows()),5)

    def test_failed_plan_leaves_missed_work(self):
        self.seed_misses()
        with patch.object(app_module,'append_dict_row',side_effect=RuntimeError('injected write failure')):
            response=self.new_plan()
        self.assertGreaterEqual(response.status_code,400);self.assertEqual(self.attention()['total'],1)

    def test_skip_latest_and_restore_without_contact(self):
        self.seed_misses();row=self.planning_rows()[-1]
        response=self.client.patch('/planning/activities/miss-25',json=dict(status='skipped',expected_revision=1,client_request_id='skip-latest'))
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)
        self.assertEqual(self.contact_rows(),[])
        response=self.client.patch('/planning/activities/miss-25',json=dict(status='planned',expected_revision=2,client_request_id='restore-latest'))
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(self.attention()['items'][0]['planned_activity_id'],'miss-25')

    def test_old_skip_does_not_hide_newer_miss(self):
        self.seed_misses();response=self.client.patch('/planning/activities/miss-23',json=dict(status='skipped',expected_revision=1,client_request_id='skip-old'))
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(self.attention()['items'][0]['planned_activity_id'],'miss-25')

    def test_note_edit_not_reschedule(self):
        self.seed_misses();self.append_planning_row(planned_activity_id='future',created_at='2026-07-01T09:00+02:00')
        response=self.client.patch('/planning/activities/future',json=dict(note='ny anteckning',expected_revision=1,client_request_id='note'))
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],1)
        response=self.client.patch('/planning/activities/future',json=dict(scheduled_at='2026-07-29T10:00+02:00',expected_revision=2,client_request_id='move'))
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)

    def test_retired_apis_reject_without_writing(self):
        before=deepcopy({k:v.values for k,v in self.spreadsheet.sheets.items()})
        for path in ['/customer-workflow/action','/customer-contact-permission','/planning/suggestions/x/snooze',
                     '/planning/suggestions/x/dismiss','/planning/suggestions/x/resume']:
            with self.subTest(path=path):
                response=self.client.post(path,json={});self.assertEqual(response.status_code,410,response.get_json())
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})

    def test_removed_outcomes_and_direct_completed_cannot_mutate(self):
        self.seed_misses()
        for fields in [{'obligation_outcome':'resolved'},{'contact_permission':{'status':'allowed'}},{'result':'Återkom ej'}]:
            self.assertEqual(self.log(**fields).status_code,410)
        response=self.client.patch('/planning/activities/miss-25',json=dict(status='completed',expected_revision=1,client_request_id='complete'))
        self.assertEqual(response.status_code,409);self.assertEqual(self.contact_rows(),[])

    def test_future_contact_rejected_and_backdated_leaves_miss(self):
        self.seed_misses();self.assertEqual(self.log(date_time='2026-07-28T09:00+02:00').status_code,400)
        self.assertEqual(self.log(date_time='2026-07-24T10:00+02:00').status_code,200)
        self.assertEqual(self.attention()['items'][0]['planned_activity_id'],'miss-25')

    def test_partial_followup_retry_one_contact_one_plan(self):
        self.seed_misses();followup=dict(enabled=True,contact_type='phone',scheduled_at='2026-07-28T14:00+02:00')
        original=app_module.append_dict_row
        def fail_plan(sheet,*args,**kwargs):
            if sheet.title==app_module.PLANNED_ACTIVITIES_SHEET:raise RuntimeError('injected plan failure')
            return original(sheet,*args,**kwargs)
        with patch.object(app_module,'append_dict_row',side_effect=fail_plan):response=self.log(follow_up=followup)
        self.assertEqual(response.status_code,207,response.get_json());self.assertTrue(response.get_json()['contact_saved'])
        self.assertFalse(response.get_json()['follow_up']['saved']);self.assertEqual(self.attention()['total'],0)
        self.assertEqual(self.log(follow_up=followup).status_code,200)
        self.assertEqual(len(self.contact_rows()),1);self.assertEqual(len(self.planning_rows()),4)

    def test_contact_timeout_after_append_retry_is_idempotent(self):
        original=app_module.append_dict_row
        def timeout(sheet,*args,**kwargs):
            original(sheet,*args,**kwargs)
            if sheet.title=='sales_activities':raise TimeoutError('response lost after write')
        with patch.object(app_module,'append_dict_row',side_effect=timeout):self.assertEqual(self.log().status_code,503)
        self.assertEqual(self.log().status_code,200);self.assertEqual(len(self.contact_rows()),1)

    def test_virtual_followup_skip_and_reschedule(self):
        self.append_contact_row(customer_id=CID,contact_id='legacy',date_time='2026-07-01T09:00+02:00',follow_up_date='2026-07-24')
        item=self.attention()['items'][0]
        payload=dict(customer_id=CID,obligation_id=item['obligation_id'],expected_revision=item['revision'],client_request_id='legacy-skip',action='skip')
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)
        self.assertEqual(self.client.post('/customer-obligations/action',json=payload).status_code,200)
        self.assertEqual(len(self.contact_rows()),1)

    def test_virtual_reschedule_creates_real_activity(self):
        self.append_contact_row(customer_id=CID,contact_id='legacy',date_time='2026-07-01T09:00+02:00',follow_up_date='2026-07-24')
        item=self.attention()['items'][0]
        payload=dict(customer_id=CID,obligation_id=item['obligation_id'],expected_revision=item['revision'],client_request_id='legacy-move',action='reschedule',scheduled_at='2026-07-28T14:00+02:00',contact_type='phone')
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)
        self.assertEqual(self.client.post('/customer-obligations/action',json=payload).status_code,200)
        self.assertEqual(len(self.planning_rows()),1)

    def test_missing_data_fails_read_without_claiming_empty(self):
        with patch.object(app_module,'get_contact_rows',side_effect=RuntimeError('contact read unavailable')):
            response=self.client.get('/planning/attention?user_name=olle')
        self.assertEqual(response.status_code,503,response.get_json())

    def test_grouping_before_pagination_and_search(self):
        self.seed_misses();self.append_planning_row(customer_row=4,planned_activity_id='other',scheduled_at='2026-07-24T10:00+02:00',created_at='2026-07-01T10:00+02:00')
        first=self.attention('&limit=1');second=self.attention('&limit=1&offset=1')
        self.assertEqual(first['total'],2);self.assertEqual(second['total'],2)
        self.assertNotEqual(first['items'][0]['customer_id'],second['items'][0]['customer_id'])
        self.assertEqual(self.attention('&q=lovat')['total'],1)

    def test_calendar_and_next_followup_agree_with_attention(self):
        self.seed_misses();self.assertEqual(self.log().status_code,200)
        response=self.client.get('/planning/activities?start=2026-07-23&end=2026-07-28&include_followups=0')
        self.assertEqual(response.status_code,200,response.get_json())
        rows=response.get_json()['activities'];self.assertEqual({r['status'] for r in rows},{'superseded'})
        self.assertEqual(self.attention()['total'],0)


class ActivityLoopSafetyTests(PlanningApiTestCase):
    seed_misses = ActivityLoopApiTests.seed_misses
    attention = ActivityLoopApiTests.attention
    log = ActivityLoopApiTests.log
    new_plan = ActivityLoopApiTests.new_plan

    def legacy_without_id(self):
        self.append_contact_row(customer_id=CID,contact_id='',date_time='2026-07-01T09:00+02:00',follow_up_date='2026-07-24')
        item=self.attention()['items'][0]
        return dict(customer_id=CID,obligation_id=item['obligation_id'],expected_revision=item['revision'],
            client_request_id='normalize-legacy',action='reschedule',scheduled_at='2026-07-28T14:00+02:00',contact_type='phone')

    def test_legacy_without_id_reschedule_then_patch_and_retry(self):
        payload=self.legacy_without_id()
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertEqual(response.status_code,200,response.get_json())
        row=self.planning_rows()[0]
        self.assertEqual(row['source_contact_id'],self.contact_rows()[0]['contact_id'])
        self.assertTrue(row['source_contact_id'].startswith('legacy2:'))
        self.assertEqual(self.client.post('/customer-obligations/action',json=payload).status_code,200)
        data=dict(scheduled_at='2026-07-29T14:00+02:00',expected_revision=1,client_request_id='move-normalized')
        for _ in range(2):
            moved=self.client.patch('/planning/activities/'+row['planned_activity_id'],json=data)
            self.assertEqual(moved.status_code,200,moved.get_json())
        self.assertEqual(self.contact_rows()[0]['follow_up_date'],'2026-07-29')
        self.assertEqual(len(self.planning_rows()),1)
        self.assertEqual(len(self.contact_rows()),1)

    def test_normalized_legacy_retry_after_activity_append_failure(self):
        payload=self.legacy_without_id()
        with patch.object(app_module,'append_dict_row',side_effect=RuntimeError('activity append failed')):
            self.assertEqual(self.client.post('/customer-obligations/action',json=payload).status_code,503)
        self.assertTrue(self.contact_rows()[0]['contact_id'])
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(len(self.planning_rows()),1)

    def test_normalized_legacy_retry_after_id_write_timeout(self):
        payload=self.legacy_without_id()
        original=app_module.update_sheet_row
        def timeout(sheet,*args,**kwargs):
            original(sheet,*args,**kwargs)
            if sheet.title=='sales_activities':
                raise TimeoutError('source ID saved but response lost')
        with patch.object(app_module,'update_sheet_row',side_effect=timeout):
            self.assertEqual(self.client.post('/customer-obligations/action',json=payload).status_code,503)
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(len(self.planning_rows()),1)

    def test_ambiguous_legacy_source_is_rejected_before_activity_write(self):
        payload=self.legacy_without_id()
        self.append_contact_row(customer_id=CID,contact_id='',date_time='2026-07-01T09:00+02:00',
                                follow_up_date='2026-07-24',comment='different note, same source anchor')
        before=deepcopy({k:v.values for k,v in self.spreadsheet.sheets.items()})
        response=self.client.post('/customer-obligations/action',json=payload)
        self.assertIn(response.status_code,[404,409],response.get_json())
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})

    def test_source_id_collision_with_other_customer_is_rejected(self):
        payload=self.legacy_without_id()
        source_id=payload['obligation_id'].removeprefix('followup:')
        self.append_contact_row(customer_id='22222222-2222-4222-8222-222222222222',customer='Butik B',
            contact_id=source_id,date_time='2026-07-01T09:00+02:00',follow_up_date='')
        self.assertEqual(app_module.ensure_followup_source_contact_id(self.spreadsheet,
            customer_name='Butik A',customer_id=CID,source_contact_id=source_id),'')
        response=self.client.post('/customer-obligations/action',json=payload)
        # The shared projection quarantines this collision before the adapter.
        self.assertEqual(response.status_code,404,response.get_json())
        self.assertEqual(self.planning_rows(),[])
        self.assertEqual(self.contact_rows()[0]['contact_id'],'')

    def test_future_booking_remains_available_in_calendar_after_virtual_closure(self):
        self.append_contact_row(customer_id=CID,contact_id='old-source',
            date_time='2026-07-01T09:00+02:00',follow_up_date='2026-07-24')
        sheet=self.spreadsheet.add_worksheet(app_module.CUSTOMER_STATE_EVENTS_SHEET,rows=100,cols=12)
        sheet.append_row(app_module.STATE_EVENT_COLUMNS)
        event=dict(event_id='old-closure',kind='obligation',customer_id=CID,entity_id='followup:old-source',
            occurred_at='2026-07-25T10:00+02:00',payload_json=json.dumps({'status':'resolved'}))
        sheet.append_row([event.get(k,'') for k in app_module.STATE_EVENT_COLUMNS])
        self.append_planning_row(planned_activity_id='new-booking',source='follow_up',source_contact_id='old-source')
        response=self.client.get('/planning/activities?start=2026-07-28&end=2026-07-29&include_followups=0')
        self.assertEqual(response.status_code,200,response.get_json())
        activity=next(r for r in response.get_json()['activities'] if r['planned_activity_id']=='new-booking')
        self.assertEqual(activity['status'],'planned')
        self.assertEqual(activity['obligation_status'],'scheduled')

    def test_stale_patch_after_later_contact_is_rejected_but_future_booking_can_move(self):
        self.seed_misses()
        self.append_planning_row(planned_activity_id='future',created_at='2026-07-01T09:00+02:00')
        self.spreadsheet._store_tracker_enable_read_cache=True
        self.addCleanup(app_module._sheet_read_cache.clear)
        self.addCleanup(app_module.invalidate_priority_snapshot)
        loaded=self.client.get('/planning/activities?start=2026-07-23&end=2026-07-29&include_followups=0')
        self.assertEqual(loaded.status_code,200,loaded.get_json())
        self.assertEqual(next(r for r in loaded.get_json()['activities'] if r['planned_activity_id']=='miss-25')['status'],'planned')
        self.assertEqual(self.log().status_code,200)
        before=deepcopy({k:v.values for k,v in self.spreadsheet.sheets.items()})
        for changes in [dict(scheduled_at='2026-07-29T14:00+02:00'),dict(status='planned')]:
            response=self.client.patch('/planning/activities/miss-25',json={**changes,
                'expected_revision':1,'client_request_id':'stale-move'})
            self.assertEqual(response.status_code,409,response.get_json())
            self.assertEqual(response.get_json()['error'],'superseded_activity_immutable')
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})
        response=self.client.patch('/planning/activities/future',json=dict(scheduled_at='2026-07-29T14:00+02:00',
            expected_revision=1,client_request_id='active-move'))
        self.assertEqual(response.status_code,200,response.get_json())

    def test_stale_patch_after_later_planning_is_rejected(self):
        self.seed_misses()
        self.assertEqual(self.new_plan().status_code,201)
        before=deepcopy({k:v.values for k,v in self.spreadsheet.sheets.items()})
        response=self.client.patch('/planning/activities/miss-25',json=dict(scheduled_at='2026-07-29T14:00+02:00',
            expected_revision=1,client_request_id='replaced-move'))
        self.assertEqual(response.status_code,409,response.get_json())
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})

    def legacy(self):
        self.append_contact_row(customer_id=CID,contact_id='legacy',date_time='2026-07-01T09:00+02:00',follow_up_date='2026-07-24')
        return self.attention()['items'][0]

    def test_virtual_reschedule_partial_retry_preserves_later_contact(self):
        item=self.legacy()
        data=dict(customer_id=CID,obligation_id=item['obligation_id'],expected_revision=item['revision'],
                  client_request_id='recover-legacy',action='reschedule',scheduled_at='2026-07-28T14:00+02:00',contact_type='phone')
        with patch.object(app_module,'append_customer_state_event',side_effect=RuntimeError('event write failed')):
            self.assertEqual(self.client.post('/customer-obligations/action',json=data).status_code,503)
        self.assertEqual(self.log().status_code,200)
        response=self.client.post('/customer-obligations/action',json=data)
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(len(self.planning_rows()),1);self.assertEqual(len(self.contact_rows()),2)
        self.assertEqual(self.attention()['total'],0)

    def test_virtual_source_edit_rejects_old_revision(self):
        item=self.legacy();sheet=self.spreadsheet.worksheet('sales_activities')
        sheet.values[1][sheet.values[0].index('follow_up_date')]='2026-07-25'
        response=self.client.post('/customer-obligations/action',json=dict(customer_id=CID,obligation_id=item['obligation_id'],
            expected_revision=item['revision'],client_request_id='stale-legacy',action='skip'))
        self.assertEqual(response.status_code,409);self.assertEqual(len(self.contact_rows()),1)

    def test_other_seller_cannot_read_or_modify_customer(self):
        self.seed_misses();self.login('sofia')
        self.assertEqual(self.log().status_code,404)
        response=self.client.patch('/planning/activities/miss-25',json=dict(status='skipped',expected_revision=1,client_request_id='forbidden'))
        self.assertEqual(response.status_code,404)
        self.assertEqual(self.client.get('/planning/attention?user_name=olle').status_code,403)

    def test_deleted_customer_not_rebound_by_name_or_cache(self):
        self.seed_misses();self.attention()
        sheet=self.spreadsheet.worksheet('customers_enriched');sheet.values.pop(1)
        self.assertEqual(self.log().status_code,404)
        self.assertEqual(self.new_plan().status_code,404)
        self.assertEqual(self.attention()['total'],0)

    def test_owner_transfer_new_seller_can_log_old_activity(self):
        self.seed_misses();sheet=self.spreadsheet.worksheet('customers_enriched')
        sheet.values[1][sheet.values[0].index('sales_person')]='Sofia'
        self.assertEqual(self.log().status_code,404)
        self.login('sofia')
        response=self.log(planned_activity_id='miss-25')
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(self.client.get('/planning/attention?user_name=sofia').get_json()['total'],0)

    def test_virtual_latest_skip_handles_older_virtual_misses(self):
        self.legacy();self.append_contact_row(customer_id=CID,contact_id='second',date_time='2026-07-02T09:00+02:00',follow_up_date='2026-07-25')
        item=self.attention()['items'][0]
        response=self.client.post('/customer-obligations/action',json=dict(customer_id=CID,obligation_id=item['obligation_id'],
            expected_revision=item['revision'],client_request_id='skip-group',action='skip'))
        self.assertEqual(response.status_code,200,response.get_json());self.assertEqual(self.attention()['total'],0)
        model=app_module.customer_state_snapshot(self.spreadsheet)
        self.assertEqual({r['status'] for r in model['obligations'][CID]},{'skipped','superseded'})

    def test_actual_appointment_and_picking_help_survive(self):
        response=self.new_plan(contact_type='visit',appointment_confirmed=True,picking_help=True)
        self.assertEqual(response.status_code,201,response.get_json())
        row=self.planning_rows()[0];self.assertEqual(row['appointment_confirmed'],'Y');self.assertEqual(row['picking_help'],'Y')
        self.assertEqual(row['customer_agreement_confirmed'],'N')

    def test_current_model_business_value_unchanged_by_manual_state(self):
        self.seed_misses();before=app_module.get_authoritative_priority_snapshot(self.spreadsheet,today=NOW.date(),as_of=NOW)
        sheet=self.spreadsheet.add_worksheet(app_module.CUSTOMER_STATE_EVENTS_SHEET,rows=100,cols=12);sheet.append_row(app_module.STATE_EVENT_COLUMNS)
        for kind in ['workflow','permission']:
            event=dict(event_id=kind,kind=kind,customer_id=CID,entity_id='old',occurred_at='2026-07-26T09:00+02:00',payload_json=json.dumps({'status':'dismissed' if kind=='workflow' else 'do_not_contact'}))
            sheet.append_row([event.get(k,'') for k in app_module.STATE_EVENT_COLUMNS])
        after=app_module.get_authoritative_priority_snapshot(self.spreadsheet,today=NOW.date(),as_of=NOW)
        b=next(r for r in before['priorities'] if r['customer_id']==CID);a=next(r for r in after['priorities'] if r['customer_id']==CID)
        for field in ['business_value_raw','work_priority_raw','contact_priority_raw','priority_score']:
            self.assertEqual(a.get(field),b.get(field),field)

    def test_unreachable_retry_never_revives_old_calendar_work(self):
        self.seed_misses();self.assertEqual(self.log(result='Ej anträffbar').status_code,200)
        future=NOW+timedelta(days=7)
        model=app_module.customer_state_snapshot(self.spreadsheet,now=future)
        self.assertEqual(attention_items(model,as_of=future),[]);self.assertEqual(model['latest_reached'],{})
        priorities,_=app_module.build_current_priority_snapshot(customers=app_module.get_customer_rows(self.spreadsheet),
            contact_rows=self.contact_rows(),planned_activity_rows=self.planning_rows(),order_rows=[],message_rows=[],recipient_rows=[],
            today=future.date(),as_of=future,users=app_module.get_user_rows(self.spreadsheet),prepared_state=model)
        self.assertGreater(next(r for r in priorities if r['customer_id']==CID)['contact_priority_raw'],0)

    def test_cancel_replacement_has_no_next_followup(self):
        self.seed_misses();response=self.new_plan();aid=response.get_json()['activity']['planned_activity_id']
        self.assertEqual(self.client.patch('/planning/activities/'+aid,json=dict(status='cancelled',expected_revision=1,client_request_id='cancel-new')).status_code,200)
        self.assertEqual(self.attention()['total'],0)
        masters=app_module.get_customer_rows(self.spreadsheet)
        self.assertIsNone(app_module.select_next_customer_follow_up(masters[0],masters,self.contact_rows(),self.planning_rows(),now=NOW))

    def test_read_only_projection_and_missing_event_store(self):
        self.seed_misses();before=deepcopy({k:v.values for k,v in self.spreadsheet.sheets.items()})
        self.attention();self.client.get('/customer-insights')
        self.assertEqual(before,{k:v.values for k,v in self.spreadsheet.sheets.items()})
        self.assertNotIn(app_module.CUSTOMER_STATE_EVENTS_SHEET,self.spreadsheet.sheets)

    def test_contact_replay_after_intervening_plan_preserves_both(self):
        self.seed_misses();self.assertEqual(self.log().status_code,200);self.assertEqual(self.new_plan().status_code,201)
        self.assertEqual(self.log().status_code,200)
        self.assertEqual(len(self.contact_rows()),1);self.assertEqual(len(self.planning_rows()),4)
        self.assertEqual(self.planning_rows()[-1]['status'],'planned')

    def test_cached_deleted_customer_cannot_receive_any_activity_write(self):
        self.spreadsheet._store_tracker_enable_read_cache=True
        self.addCleanup(app_module._sheet_read_cache.clear)
        self.addCleanup(app_module.invalidate_priority_snapshot)
        self.seed_misses();self.attention()
        self.spreadsheet.worksheet('customers_enriched').values.pop(1)
        self.assertEqual(self.log().status_code,404)
        self.assertEqual(self.new_plan().status_code,404)
        response=self.client.patch('/planning/activities/miss-25',json=dict(status='skipped',expected_revision=1,client_request_id='deleted-cached'))
        self.assertEqual(response.status_code,404)
        self.assertEqual(self.contact_rows(),[])
        self.assertEqual({r['status'] for r in self.planning_rows()},{'planned'})

    def test_retired_agreement_fields_cannot_change_contact_policy(self):
        self.assertEqual(self.new_plan(customer_agreement_confirmed=True).status_code,410)
        response=self.log(follow_up={'enabled':True,'contact_type':'phone','scheduled_at':'2026-07-28T14:00+02:00','customer_agreement_confirmed':True})
        self.assertEqual(response.status_code,410)
        self.assertEqual(self.contact_rows(),[]);self.assertEqual(self.planning_rows(),[])
