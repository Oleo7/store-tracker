"""V3.4 history semantics and raw route-ranking contracts.

Synthetic data only. Actual reducer, feature/model pipeline and Flask write
endpoints are used. No production credentials or reconstructed score formula.
"""
from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
from unittest import TestCase
from unittest.mock import patch
from zoneinfo import ZoneInfo
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from customer_obligations import build_customer_state, attention_items, stable_contact_source_id, source_fingerprint
from priority import build_order_features, build_contact_features, build_priority_customers
from route_proposal import Coordinate, RouteCandidate
from route_optimization import shipment_commercial_value, shipment_penalty
import app as application
from test_planning import PlanningApiTestCase

NOW = datetime(2026, 10, 19, 9, tzinfo=ZoneInfo('Europe/Stockholm'))
C = {'customer_id':'c1', 'customer':'Testbutik', 'customer_number':'100',
     'sales_person':'olle', 'customer_segment':'A', 'phone':'0701234567', 'row':2}


def contact(days=30, result='Neutral', **fields):
    return {'customer_id':'c1', 'customer':'Testbutik', 'customer_number':'100',
            'date_time':(NOW-timedelta(days=days)).isoformat(), 'sales_person':'olle',
            'contact_channel':'Telefon', 'result':result, **fields}


def order(days=1, **fields):
    day=(NOW-timedelta(days=days)).date().isoformat()
    return {'customer_id':'c1', 'Customer':'Testbutik', 'Customer number':'100',
            'Reference':'o'+str(days), 'Order date':day, 'Delivery date':day,
            'Total weight':'20', 'Total':'1000', 'SKU':'raspberry', **fields}


def plan(days=1, **fields):
    return {'customer_id':'c1', 'customer':'Testbutik', 'user_name':'olle',
            'planned_activity_id':'p1', 'status':'planned', 'source':'manual',
            'created_at':(NOW-timedelta(days=20)).isoformat(),
            'scheduled_at':(NOW+timedelta(days=days)).isoformat(), 'contact_type':'phone', **fields}


def state(contacts=(), plans=(), orders=(), events=(), now=NOW):
    return build_customer_state([C], list(contacts), list(plans), events, as_of=now,
                                owner_user_names={'c1':'olle'}, order_rows=list(orders))


def model(contacts=(), plans=(), orders=(), events=(), now=NOW):
    st=state(contacts, plans, orders, events, now)
    of=build_order_features(list(orders), customers=[C], as_of=now)
    cf=build_contact_features(list(contacts), of, customers=[C], as_of=now)
    row=build_priority_customers([C],of,cf,None,now.date(),now=now,
         planned_activities=list(plans), obligation_states=st['obligations'],
         contact_permissions=st['contact_permissions'], planning_owner_user_names={'c1':'olle'})[0]
    return row, st


class HistoryReconciliationTests(TestCase):
    def test_comment_owner_result_and_row_order_do_not_change_identity(self):
        a=contact(comment='Original', follow_up_date='2026-10-10')
        b={**a, 'comment':'Corrected', 'sales_person':'new seller', 'result':'Positiv'}
        self.assertEqual(stable_contact_source_id(a),stable_contact_source_id(b))
        before=state([a])['obligations']['c1'][0]
        after=state([b])['obligations']['c1'][0]
        self.assertEqual(before['obligation_id'],after['obligation_id'])
        self.assertNotEqual(before['revision'],after['revision'])
        unrelated=contact(5, contact_id='unrelated')
        left=state([a,unrelated]);right=state([unrelated,a])
        self.assertEqual(left['obligations']['c1'][0]['obligation_id'],right['obligations']['c1'][0]['obligation_id'])

    def test_duplicate_anchor_preserves_both_sources_and_refuses_mutation(self):
        a=contact(comment='Reminder A',follow_up_date='2026-10-10')
        b={**a,'comment':'Reminder B','follow_up_date':'2026-10-12'}
        item=state([a,b])['obligations']['c1'][0]
        self.assertTrue(item['identity_ambiguous'])
        self.assertEqual(item['source_count'],2)
        self.assertEqual({x['comment'] for x in item['source_variants']},{'Reminder A','Reminder B'})
        with self.assertRaises(application.CustomerStateConflict):
            application.validate_customer_state_revision({'expected_revision':item['revision']},item)

    def test_old_saved_identity_survives_edit_without_reopening(self):
        original=contact(comment='old text',follow_up_date='2026-09-25')
        sid='legacy:'+source_fingerprint(original)[:32]
        event={'kind':'obligation','customer_id':'c1','entity_id':'followup:'+sid,
               'event_id':'close1','revision':1,'occurred_at':'2026-10-01T10:00+02:00',
               'payload_json':json.dumps({'status':'resolved','source_contact_id':sid,
                                         'source_created_at':original['date_time'],
                                         'purpose':'internal_followup'})}
        st=state([{**original,'comment':'corrected'}],events=[event])
        self.assertEqual(len(st['obligations']['c1']),1)
        self.assertEqual(st['obligations']['c1'][0]['status'],'resolved')
        self.assertEqual(st['obligations']['c1'][0]['obligation_id'],'followup:'+sid)

    def test_later_reached_or_order_replaces_internal_not_customer_promise(self):
        for later in ('dialogue','order'):
            for promise in (False,True):
                with self.subTest(later=later,promise=promise):
                    c=contact(30,follow_up_date='2026-10-01',contact_id='source',
                              follow_up_agreement_confirmed='Y' if promise else 'N')
                    cs=[c]+([contact(5)] if later=='dialogue' else [])
                    orders=[order(5)] if later=='order' else []
                    st=state(cs,orders=orders);item=st['obligations']['c1'][0]
                    self.assertEqual(item['status'] in {'resolved','cancelled'},not promise)
                    if not promise:
                        self.assertTrue(item['resolution_basis'])
                        self.assertTrue(item['resolution_event_id'])
                    else:
                        self.assertEqual(len(attention_items(st,as_of=NOW)),1)

    def test_later_failed_attempt_is_not_fulfillment(self):
        c=contact(30,follow_up_date='2026-10-01',contact_id='source')
        st=state([c,contact(2,'Ej antr\u00e4ffbar')]);item=st['obligations']['c1'][0]
        self.assertEqual(item['status'],'open')
        self.assertNotIn('resolution_basis',item)
        self.assertEqual(attention_items(st,as_of=NOW),[])

    def test_multiple_old_dates_become_one_current_internal_schedule(self):
        cs=[contact(100-i*10,contact_id='c'+str(i),follow_up_date=(NOW-timedelta(days=90-i*10)).date().isoformat()) for i in range(6)]
        st=state(cs);items=st['obligations']['c1']
        self.assertEqual(len(items),6)
        self.assertEqual(sum(x['status']=='open' for x in items),1)
        self.assertEqual(attention_items(st,as_of=NOW),[])
        self.assertEqual(sum(bool(x.get('resolution_basis')) for x in items),5)

    def test_service_hint_is_preserved_but_not_agreement_proof(self):
        cs=[contact(110,follow_up_date='2026-07-01',comment='Reklamation kvar att behandla'),contact(2)]
        st=state(cs,orders=[order(1)]);item=st['obligations']['c1'][0]
        self.assertEqual(item['commitment_kind'],'uncertain_service')
        self.assertFalse(item['customer_agreement_confirmed'])
        self.assertEqual(item['status'],'needs_review')
        self.assertEqual(len(attention_items(st,as_of=NOW)),1)

    def test_replacement_future_plan_covers_reminder_but_not_separate_service(self):
        for promise in (False,True):
            c=contact(40,follow_up_date='2026-10-01',follow_up_agreement_confirmed=promise)
            st=state([c],[plan(4)])
            source=next(x for x in st['obligations']['c1'] if x['source_contact_id'])
            self.assertEqual(source['status']=='cancelled',not promise)
            self.assertEqual(len(attention_items(st,as_of=NOW)),int(promise))

    def test_saved_snooze_and_explicit_resolution_are_not_inferred_away(self):
        c=contact(40,contact_id='c',follow_up_date='2026-10-01')
        event={'kind':'obligation','customer_id':'c1','entity_id':'followup:c','event_id':'saved',
               'occurred_at':'2026-10-10T09:00+02:00','revision':1,
               'payload_json':json.dumps({'status':'open','snooze_until':'2026-10-25T09:00+02:00'})}
        st=state([c,contact(1)],orders=[order(1)],events=[event]);item=st['obligations']['c1'][0]
        self.assertEqual(item['status'],'open');self.assertTrue(item['snooze_until'])
        self.assertEqual(attention_items(st,as_of=NOW),[])


class ContactAndRankingTests(TestCase):
    def test_date_alone_never_lifts_negative_contact_pause(self):
        for proof in (False,True):
            cs=[contact(5,'Negativ',follow_up_date='2026-10-18',contact_id='n',follow_up_agreement_confirmed=proof)]
            row,_=model(cs)
            self.assertEqual(row['can_start_unscheduled_contact'],proof)
            self.assertEqual(row['list_guidance']['explicit_agreement_overrides_negative'],proof)

    def test_date_only_future_reminder_is_not_green_booking(self):
        row,_=model([contact(30,follow_up_date='2026-10-25')])
        self.assertEqual(row['list_guidance']['reason_code'],'internal_followup_wait')
        self.assertFalse(row['has_planned_contact']);self.assertFalse(row['planned_activity_id'])
        row,_=model([contact(30,follow_up_date='2026-10-25',follow_up_agreement_confirmed=True)])
        self.assertEqual(row['list_guidance']['status_key'],'wait');self.assertFalse(row['has_planned_contact'])

    def test_stale_separate_task_does_not_zero_normal_commercial_work(self):
        c=contact(150,contact_id='promise',follow_up_date='2026-06-01',follow_up_agreement_confirmed=True)
        row,st=model([c],orders=[order(70),order(35)])
        self.assertTrue(row['can_start_unscheduled_contact'])
        self.assertGreater(row['work_priority_raw'],0)
        self.assertTrue(row['requires_review'])
        self.assertNotEqual(row['action_scope'],'obligation:followup:promise')
        self.assertEqual(len(attention_items(st,as_of=NOW)),1)

    def test_safety_gate_remains_when_old_history_no_longer_blocks(self):
        c=contact(150,contact_id='p',follow_up_date='2026-06-01',follow_up_agreement_confirmed=True)
        for result in ('Negativ','\u00c5terkom ej','Ej antr\u00e4ffbar'):
            row,_=model([c,contact(0,result)])
            self.assertFalse(row['can_start_unscheduled_contact']);self.assertEqual(row['work_priority_raw'],0)

    def test_direct_feature_call_uses_same_reconciled_history(self):
        cs=[contact(100,follow_up_date='2026-07-20'),contact(30)]
        row,_=model(cs)
        of=build_order_features([],customers=[C],as_of=NOW)
        cf=build_contact_features(cs,of,customers=[C],as_of=NOW)
        direct=build_priority_customers([C],of,cf,None,NOW.date(),now=NOW)[0]
        self.assertEqual(row['list_guidance']['reason_code'],direct['list_guidance']['reason_code'])
        self.assertEqual(row['work_priority_raw'],direct['work_priority_raw'])

    def test_first_order_phase_preserved_and_does_not_replace_real_booking(self):
        for age in (6,7,10,11,23,24,30,31,45,60,90,91):
            with self.subTest(age=age):
                row,_=model(orders=[order(age)],contacts=[contact(age+1,'Positiv')])
                self.assertNotIn('stale_follow_up_review',row['list_guidance']['reason_code'])
                if age==6:self.assertEqual(row['work_priority_raw'],0)
                if 7<=age<=10:self.assertEqual(row['list_guidance']['reason_code'],'first_order_onboarding')
                if 11<=age<=90:self.assertEqual(row['list_guidance']['reason_code'],'second_purchase_due')
        planned,_=model(orders=[order(24)],plans=[plan(2)])
        self.assertTrue(planned['has_planned_contact']);self.assertEqual(planned['contact_priority_raw'],0)
        self.assertGreater(planned['work_priority_raw'],0)

    def test_self_ordering_wait_then_new_order_clears_internal_cue(self):
        c=contact(25,'Positiv',follow_up_date='2026-10-21',comment='Kunden best\u00e4ller sj\u00e4lv i Stockfiller')
        row,_=model([c]);self.assertEqual(row['list_guidance']['reason_code'],'self_ordering_wait')
        later,_=model([c],now=NOW+timedelta(days=3))
        self.assertEqual(later['list_guidance']['reason_code'],'self_ordering_followup_due')
        ordered,_=model([c],orders=[order(1)])
        self.assertNotEqual(ordered['list_guidance']['reason_code'],'self_ordering_wait')

    def test_ranking_rollback_preserves_safety_and_obligations(self):
        c=contact(100,follow_up_date='2026-06-01',follow_up_agreement_confirmed=True)
        v3,st=model([c])
        with patch.dict(os.environ,{'PRIORITY_RANKING_MODE':'v2.2'}):
            old,st2=model([c])
            self.assertEqual(old['work_priority_score'],old['priority_score'])
            self.assertEqual(old['ranking_mode'],'v3')
            self.assertEqual(old['work_priority_raw'],v3['work_priority_raw'])
            self.assertEqual(old['can_start_unscheduled_contact'],v3['can_start_unscheduled_contact'])
            banned,_=model([c,contact(1,'\u00c5terkom ej')]);self.assertEqual(banned['work_priority_raw'],0)
        self.assertEqual(st,st2)

    def test_normalized_route_index_is_not_raw_dfp_or_old_score(self):
        a={'value_model_version':'v3.5','score_version':'v3.5','priority_score':99,
           'contact_priority_score':95,'contact_priority_precise':95.1,'contact_priority_raw':50.,
           'work_priority_raw':60.,'work_priority_precise':96.2,'work_priority_score':96}
        b={**a,'priority_score':5,'contact_priority_precise':95.2,'contact_priority_raw':9999.}
        va=application.route_priority_values(a);vb=application.route_priority_values(b)
        self.assertEqual(va['priority_score'],vb['priority_score'])
        self.assertEqual(va['commercial_value'],95.1)
        self.assertEqual(vb['commercial_value'],95.2)
        self.assertGreater(shipment_penalty(vb),shipment_penalty(va))
        self.assertEqual(application.route_priority_values(a,required=True)['commercial_value'],96.2)

    def test_route_corrupt_value_fails_closed(self):
        for bad in (None,True,float('nan'),float('inf'),-1):
            p={'value_model_version':'v3.5','priority_score':99,
               'contact_priority_raw':bad,'contact_priority_score':90}
            self.assertIsNone(application.route_priority_values(p))


class AdditionalCommercialRegressionTests(TestCase):
    def test_old_report_is_annotation_not_confirmed_purchase_or_permanent_pause(self):
        row,_=model([contact(100,'Order lagd!')])
        self.assertGreater(row['work_priority_raw'],0)
        self.assertEqual(row['business_verified_delivery_count'],0)
        self.assertIn('historical_order_report',{w['code'] for w in row['data_warnings']})
        fresh,_=model([contact(10,'Order lagd!')])
        self.assertEqual(fresh['action_type'],'reported_order_sync_review')
        self.assertEqual(fresh['work_priority_raw'],0)
        rejected,_=model([contact(100,'Order lagd!'),contact(1,'Negativ')])
        self.assertEqual(rejected['action_type'],'negative_contact_cooldown')
        self.assertEqual(rejected['work_priority_raw'],0)

    def test_later_real_delivery_supersedes_acquisition_not_reference_match(self):
        row,_=model([contact(20,'Order lagd!')],orders=[order(15,**{'Order date':'2026-09-20'})])
        self.assertNotEqual(row['action_type'],'reported_order_sync_review')
        self.assertEqual(row['business_verified_delivery_count'],1)
        self.assertIn('delivery_after_report',{w['code'] for w in row['data_warnings']})

    def test_self_ordering_evidence_survives_an_unanswered_attempt(self):
        cs=[contact(25,'Positiv',follow_up_date='2026-10-25',comment='Kunden beställer själv i Stockfiller'),
            contact(5,'Ej anträffbar',comment='Inget svar')]
        row,_=model(cs)
        self.assertEqual(row['action_type'],'self_ordering_wait')

    def test_stale_service_does_not_erase_future_internal_reminder(self):
        cs=[contact(150,contact_id='service',follow_up_date='2026-06-01',follow_up_agreement_confirmed=True),
            contact(25,contact_id='reminder',follow_up_date='2026-10-25')]
        row,_=model(cs)
        self.assertEqual(row['action_type'],'internal_followup_wait')
        self.assertEqual(row['work_priority_raw'],0)
        self.assertIn('followup:service',row['review_obligation_ids'])

    def test_new_order_does_not_remove_retry_pause_for_separate_promise(self):
        cs=[contact(20,contact_id='service',follow_up_date='2026-10-17',follow_up_agreement_confirmed=True),
            contact(0,'Ej anträffbar',contact_id='attempt',obligation_id='followup:service',obligation_outcome='open')]
        row,_=model(cs,orders=[order(0)])
        self.assertEqual(row['action_type'],'unanswered_retry_wait')
        self.assertFalse(row['can_start_unscheduled_contact'])

    def test_newer_reached_dialogue_applies_even_with_older_phase_contact(self):
        cs=[contact(8,'Neutral',purpose='second_purchase_due'),contact(1,'Positiv')]
        row,_=model(cs,orders=[order(24)])
        self.assertEqual(row['action_type'],'recent_same_purpose_dialogue_wait')

    def test_explicit_completion_on_delivery_day_counts_but_plain_dialogue_does_not(self):
        plain,_=model([contact(8,'Positiv')],orders=[order(8)])
        self.assertEqual(plain['action_type'],'first_order_onboarding')
        completed,_=model([contact(8,'Positiv',purpose='first_order_onboarding',onboarding_completed='Y')],orders=[order(8)])
        self.assertEqual(completed['action_type'],'first_order_onboarding_completed')


class TimestampAndIdentityRegressionTests(TestCase):
    def test_different_same_time_event_cannot_claim_later_agreement(self):
        stamp=(NOW-timedelta(days=5)).isoformat()
        cs=[contact(5,'Negativ',contact_id='negative',date_time=stamp),
            contact(5,'Order lagd!',contact_id='separate',date_time=stamp,
                    follow_up_date='2026-10-18',follow_up_agreement_confirmed=True)]
        row,_=model(cs)
        self.assertEqual(row['action_type'],'negative_contact_cooldown')
        self.assertFalse(row['can_start_unscheduled_contact'])
        self.assertFalse(row['list_guidance']['explicit_agreement_overrides_negative'])

    def test_broken_calendar_id_never_becomes_a_green_or_executable_booking(self):
        row,_=model(plans=[plan(1,planned_activity_id='')])
        self.assertEqual(row['action_type'],'invalid_planned_activity_identity')
        self.assertFalse(row['has_planned_contact'])
        self.assertFalse(row['can_start_unscheduled_contact'])
        self.assertEqual(row['work_priority_raw'],0)


class SourceWriteSafetyTests(PlanningApiTestCase):
    def test_colliding_legacy_sources_cannot_be_silently_promoted(self):
        cid='11111111-1111-4111-8111-111111111111'
        self.append_contact_row(customer_id=cid,date_time='2026-07-20 09:00',comment='one',follow_up_date='2026-07-25')
        self.append_contact_row(customer_id=cid,date_time='2026-07-20 09:00',comment='two',follow_up_date='2026-07-26')
        rows=self.contact_rows();sid=stable_contact_source_id(rows[0]);before=deepcopy(rows)
        value=application.ensure_followup_source_contact_id(self.spreadsheet,customer_id=cid,customer_name='Butik A',source_contact_id=sid)
        self.assertEqual(value,'');self.assertEqual(self.contact_rows(),before)

    def test_saved_legacy_alias_reschedule_after_note_edit_keeps_identity(self):
        cid = '11111111-1111-4111-8111-111111111111'
        self.append_contact_row(customer_id=cid, contact_id='', date_time='2026-07-20 09:00',
                                comment='Original promise', follow_up_date='2026-07-25',
                                follow_up_purpose='customer_followup', follow_up_agreement_confirmed='Y')
        original = self.contact_rows()[0]
        old_id = 'legacy:' + source_fingerprint(original)[:32]
        item = application.customer_state_snapshot(self.spreadsheet)['obligations'][cid][0]
        item.update(obligation_id='followup:' + old_id, source_contact_id=old_id, status='open')
        sheet = self.spreadsheet.add_worksheet(application.CUSTOMER_STATE_EVENTS_SHEET, rows=100, cols=12)
        sheet.append_row(application.STATE_EVENT_COLUMNS)
        event = {'event_id':'old-event', 'kind':'obligation', 'entity_id':item['obligation_id'],
                 'customer_id':cid, 'owner_user_name':'olle', 'actor_user_name':'olle', 'revision':1,
                 'occurred_at':'2026-07-26T09:00:00+02:00', 'payload_json':json.dumps(item)}
        sheet.append_row([event.get(k,'') for k in application.STATE_EVENT_COLUMNS])
        contacts = self.spreadsheet.worksheet('sales_activities')
        headers = contacts.values[0]
        contacts.values[1][headers.index('comment')] = 'Corrected note, same promise'
        fresh = application.customer_state_snapshot(self.spreadsheet)['obligations'][cid][0]
        self.assertEqual(fresh['obligation_id'],item['obligation_id'])
        data = {'customer_id':cid, 'obligation_id':fresh['obligation_id'], 'action':'reschedule',
                'expected_revision':fresh['revision'], 'client_request_id':'alias-reschedule',
                'scheduled_at':'2026-07-28T10:00+02:00', 'contact_type':'phone'}
        response = self.client.post('/customer-obligations/action',json=data)
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(response.get_json()['obligation']['obligation_id'],item['obligation_id'])
        self.assertEqual(self.contact_rows()[0]['contact_id'],old_id)
        self.assertEqual(self.contact_rows()[0]['comment'],'Corrected note, same promise')
        repeated = self.client.post('/customer-obligations/action',json=data)
        self.assertEqual(repeated.status_code,200,repeated.get_json())
        self.assertEqual(len(self.planning_rows()),1)

    def test_future_internal_followup_clears_after_order_in_detail_projection(self):
        customer = application.get_customer_rows(self.spreadsheet)[0]
        cid = customer['customer_id']
        self.append_contact_row(customer_id=cid,contact_id='old-reminder',date_time='2026-07-01 09:00',
                                follow_up_date='2026-08-20')
        orders = [{'customer_id':cid,'Customer':customer['customer'],'Reference':'new-order',
                   'Order date':'2026-07-25','Delivery date':'2026-07-26','Total weight':'20','Total':'500'}]
        next_item = application.select_next_customer_follow_up(customer,[customer],self.contact_rows(),[],
                                                               now=application.stockholm_now(),order_rows=orders)
        self.assertIsNone(next_item)
