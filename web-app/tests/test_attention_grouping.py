"""Backend attention grouping, preserving every source row and commitment."""
from copy import deepcopy
from datetime import datetime
from unittest import TestCase
from zoneinfo import ZoneInfo
from test_planning import PlanningApiTestCase, NOW
from customer_obligations import build_customer_state, attention_items
import app as a

AS_OF = datetime(2026,10,20,12,tzinfo=ZoneInfo('Europe/Stockholm'))


def plan(aid, scheduled, **extra):
    return {'planned_activity_id':aid,'customer_id':'c1','customer':'Coop Sydport Mariestad',
            'user_name':'sofia','sales_person':'Sofia','scheduled_at':scheduled,
            'created_at':'2026-08-01T12:00:00+02:00','status':'planned','contact_type':'phone',
            'source':'manual','appointment_confirmed':'N', **extra}


def state(plans):
    customers = [{'customer_id':'c1','customer':'Coop Sydport Mariestad','sales_person':'Sofia'},
                 {'customer_id':'c2','customer':'Coop Sydport Mariestad','sales_person':'Sofia'}]
    return build_customer_state(customers, [], plans, as_of=AS_OF)


class OrdinaryAttentionTests(TestCase):
    def coop(self, **latest):
        return [plan('august','2026-08-25T09:00:00+02:00'),
                plan('october','2026-10-02T09:00:00+02:00'),
                plan('latest','2026-10-19T09:00:00+02:00',contact_type='email',**latest)]

    def test_coop_sydport_three_channels_one_latest_row_without_mutation(self):
        source = self.coop(); before=deepcopy(source)
        model = state(source); original=deepcopy(model)
        items=attention_items(model,as_of=AS_OF)
        self.assertEqual([r['planned_activity_id'] for r in items],['latest'])
        self.assertEqual(items[0]['contact_type'],'email')
        self.assertEqual(items[0]['older_missed_count'],2)
        self.assertEqual(source,before)
        self.assertEqual(model,original)
        self.assertEqual(len(model['obligations']['c1']),3)
        self.assertEqual({r['status'] for r in model['obligations']['c1']},{'scheduled'})

    def test_same_customer_different_sellers_are_separate_groups(self):
        model=state(self.coop())
        extra=deepcopy(model['obligations']['c1'][0])
        extra.update(owner_user_name='olle',obligation_id='calendar:olle')
        model['obligations']['c1'].append(extra)
        self.assertEqual(len(attention_items(model,as_of=AS_OF)),2)
        for owner in ['Sofia','OLLE']:
            self.assertEqual(len(attention_items(model,as_of=AS_OF,owner_user_name=owner)),1)

    def test_identical_names_distinct_customer_ids_never_merge(self):
        source=self.coop()+[plan('other','2026-10-18T09:00:00+02:00',customer_id='c2')]
        items=attention_items(state(source),as_of=AS_OF)
        self.assertEqual({r['customer_id'] for r in items},{'c1','c2'})
        self.assertEqual([r['planned_activity_id'] for r in items],['latest','other'])

    def test_terminal_latest_prevents_older_generic_tasks_from_resurfacing(self):
        for status in ['completed','cancelled','superseded']:
            with self.subTest(status=status):
                rows=self.coop(status=status)
                self.assertEqual([r['planned_activity_id'] for r in attention_items(state(rows),as_of=AS_OF)],['october'])
        self.assertEqual(attention_items(state(self.coop(status='skipped')),as_of=AS_OF),[])

    def test_future_rescheduling_covers_older_generic_tasks(self):
        rows=self.coop()+[plan('future','2026-10-23T09:00:00+02:00')]
        self.assertEqual(len(attention_items(state(rows),as_of=AS_OF)),1)
        rows[-1]['planning_decided_at']='2026-10-20T09:00+02:00'
        self.assertEqual(attention_items(state(rows),as_of=AS_OF),[])

    def test_distinct_promises_and_uncertain_service_keep_their_identity(self):
        rows=self.coop()+[plan('promise','2026-10-18T09:00+02:00',customer_agreement_confirmed='Y'),plan('service','2026-10-17T09:00+02:00',note='Reklamation: lovat demo')]
        model=state(rows);items=attention_items(model,as_of=AS_OF)
        self.assertEqual([r['planned_activity_id'] for r in items],['latest'])
        self.assertEqual(len(model['obligations']['c1']),5)

    def test_new_future_separate_promise_does_not_hide_missed_generic_work(self):
        source=self.coop()+[plan('future-promise','2026-10-23T09:00:00+02:00',customer_agreement_confirmed='Y')]
        self.assertEqual([r['planned_activity_id'] for r in attention_items(state(source),as_of=AS_OF)],['latest'])

    def test_same_day_latest_selected_by_time_and_ties_are_stable(self):
        plans=[plan('early','2026-10-19T08:00:00+02:00'),plan('late','2026-10-19T15:00:00+02:00')]
        for rows in (plans, list(reversed(plans))):
            self.assertEqual(attention_items(state(rows),as_of=AS_OF)[0]['planned_activity_id'],'late')

    def test_missing_identity_never_groups_by_name(self):
        model=state(self.coop())
        for row in model['obligations']['c1']: row['customer_id']=''
        self.assertEqual(attention_items(model,as_of=AS_OF),[])

    def test_snoozed_latest_keeps_older_tasks_hidden(self):
        model=state(self.coop())
        model['obligations']['c1'][-1]['snooze_until']='2026-10-23T12:00+02:00'
        self.assertEqual([r['planned_activity_id'] for r in attention_items(model,as_of=AS_OF)],['latest'])

    def test_pending_route_does_not_replace_a_real_missed_followup(self):
        source=self.coop()+[plan('pending','2026-10-23T09:00:00+02:00',status='route_pending',source='route')]
        self.assertEqual([r['planned_activity_id'] for r in attention_items(state(source),as_of=AS_OF)],['latest'])

    def test_search_filters_representatives_without_revealing_hidden_old_notes(self):
        source=self.coop();source[0]['note']='bara gammal anteckning'
        model=state(source)
        self.assertEqual(len(attention_items(model,as_of=AS_OF,query='sydport')),1)
        self.assertEqual(attention_items(model,as_of=AS_OF,query='bara gammal'),[])


class AttentionGroupingApiTests(PlanningApiTestCase):
    def seed(self):
        for aid, day, channel in [('old',23,'phone'),('middle',24,'phone'),('latest',25,'email')]:
            self.append_planning_row(planned_activity_id=aid,scheduled_at=f'2026-07-{day}T09:00:00+02:00',
                                     contact_type=channel,appointment_confirmed='N',created_at='2026-07-01T09:00+02:00')
        self.append_planning_row(planned_activity_id='other',customer_row=4,
                                 scheduled_at='2026-07-24T09:00:00+02:00',appointment_confirmed='N',created_at='2026-07-01T09:00+02:00')

    def attention(self, query=''):
        response=self.client.get('/planning/attention?user_name=olle'+query)
        self.assertEqual(response.status_code,200,response.get_json())
        return response.get_json()

    def test_pagination_and_search_run_after_grouping(self):
        self.seed()
        first=self.attention('&limit=1')
        second=self.attention('&limit=1&offset=1')
        end=self.attention('&limit=1&offset=2')
        self.assertEqual(first['total'],2)
        self.assertEqual(first['items'][0]['planned_activity_id'],'latest')
        self.assertEqual(second['items'][0]['planned_activity_id'],'other')
        self.assertEqual(end['items'],[])
        self.assertEqual(self.attention('&q=Butik%20A')['total'],1)
        self.assertEqual(len(self.planning_rows()),4)

    def test_cancel_latest_calendar_activity_does_not_resurrect_history(self):
        self.seed()
        latest=next(r for r in self.planning_rows() if r['planned_activity_id']=='latest')
        response=self.client.patch('/planning/activities/latest',json={
            'client_request_id':'close-latest','expected_revision':latest['revision'],'status':'cancelled'})
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual({r['planned_activity_id'] for r in self.attention()['items']},{'middle','other'})
        self.assertEqual(len(self.planning_rows()),4)
        self.assertEqual({r['status'] for r in self.planning_rows() if r['planned_activity_id'] in {'old','middle'}},{'planned'})

    def test_manual_obligation_resolution_changes_group_projection_only(self):
        self.seed();before=deepcopy(self.planning_rows());item=self.attention()['items'][0]
        response=self.client.post('/customer-obligations/action',json={'client_request_id':'removed-resolve','customer_id':item['customer_id'],'obligation_id':item['obligation_id'],'action':'resolve'})
        self.assertEqual(response.status_code,410)
        self.assertEqual(self.planning_rows(),before)

    def test_seller_filter_does_not_include_other_owner(self):
        self.seed()
        self.append_planning_row(planned_activity_id='sofia',customer_row=3,
            owner={'user_name':'sofia','name':'Sofia'},scheduled_at='2026-07-25T09:00:00+02:00',created_at='2026-07-01T09:00+02:00')
        self.assertEqual(self.attention()['total'],2)
        self.login('admin')
        response=self.client.get('/planning/attention?user_name=sofia')
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual([r['planned_activity_id'] for r in response.get_json()['items']],['sofia'])
