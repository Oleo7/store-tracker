"""Regression contracts for truthful guidance and date-bound delivery volumes."""
from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as app_module
import priority as p

TODAY = date(2026, 9, 18)
NOW = datetime(2026, 9, 18, 12)


def customer(segment='C', customer_id='c', number='100'):
    return dict(customer_id=customer_id, customer_number=number, customer='Butik',
                row=2, sales_person='Olle', customer_segment=segment,
                phone='0701234567', email='buyer@example.com')


def order(ref, age, volume=13, customer_id='c', number='100'):
    day = (TODAY - timedelta(days=age)).isoformat()
    return {'Reference': ref, 'Customer': 'Butik', 'customer_id': customer_id,
            'Customer number': number, 'Order date': min(day, TODAY.isoformat()),
            'Delivery date': day, 'Total weight': volume, 'Quantity': '999',
            'Total': '1000', 'SKU': ref}


def contact(age=7, result='Neutral', follow_up='', contact_id='contact'):
    return dict(customer_id='c', customer='Butik', contact_id=contact_id,
                date_time=(NOW - timedelta(days=age)).isoformat(),
                sales_person='Olle', contact_channel='Telefon', result=result,
                follow_up_date=follow_up)


def email(kind='stockfiller', age=7):
    return {'customer_id': 'c', 'email_followup_status': kind + '_clicked_no_order',
            'email_followup_email_id': 'email', 'email_followup_wait_days_remaining': max(0, 3-age),
            'email_followup_proposal_label': 'P\u00e5minnelse',
            f'email_{kind}_first_clicked_at': (NOW - timedelta(days=age)).isoformat()}


def activity(days=1):
    return dict(planned_activity_id='activity', customer_id='c', sales_person='Olle',
                contact_type='phone', status='planned',
                scheduled_at=(NOW + timedelta(days=days)).isoformat())


def scored(*, store=None, orders=(), contacts=(), mail=None, planned=(), today=TODAY,
           suppressions=None, features=None):
    features = features if features is not None else p.build_order_features(list(orders))
    return p.build_priority_customers(
        [store or customer()], features, p.build_contact_features(list(contacts), features),
        None, today, email_features={'id:c': mail} if mail else {},
        planned_activities=list(planned), now=datetime.combine(today, NOW.time()),
        workflow_suppressions=suppressions,
    )[0]


class GuidanceIntegrityTests(TestCase):
    def assert_canonical(self, item):
        guidance = item['customer_guidance']
        self.assertEqual(guidance['reason_code'], item['primary_trigger_type'])
        self.assertEqual(guidance['reason_text'], item['primary_reason_text'])
        self.assertNotIn('Uttrycklig', guidance['reason_text'])
        self.assertNotEqual(guidance['action_key'], 'follow_up_today')
        return guidance

    def test_first_delivery_boundaries_and_exact_copy(self):
        for days in (0, 6, 7, 10, 11, 23, 24, 90, 91):
            with self.subTest(days=days):
                item = scored(store=customer('B'), orders=[order('first', days)])
                g = item['customer_guidance']
                if 7 <= days <= 10:
                    self.assertEqual(g['action_label'], 'Kontrollera placering och start')
                    self.assertEqual(g['reason_text'], f'F\u00f6rsta leverans f\u00f6r {days} dagar sedan \u2013 planera uppf\u00f6ljning')
                    self.assert_canonical(item)
                elif 11 <= days <= 90:
                    self.assertEqual(g['action_label'], 'S\u00e4kra \u00e5terk\u00f6pet')
                    self.assertEqual(g['reason_text'], f'{days} dagar sedan f\u00f6rsta leveransen \u00b7 dags att s\u00e4kra andra k\u00f6pet')
                    self.assertEqual(g['focus_label'], 'En leverans, 11\u201390 dagar sedan')
                    self.assertEqual(g['focus_key'], 'second_purchase')
                    self.assert_canonical(item)
                elif days == 91:
                    self.assertEqual(g['focus_key'], 'reactivation')
                else:
                    self.assertEqual(g['status_key'], 'idle')
                    self.assertNotIn('11\u201390', g['focus_label'])

    def test_reorder_date_is_compact_swedish_and_year_aware(self):
        rows = [order('last', 20), order('middle', 40), order('first', 60)]
        item = scored(orders=rows)
        self.assertEqual(item['expected_next_order_date'], '2026-09-18')
        self.assertEqual(self.assert_canonical(item)['reason_text'], 'Ber\u00e4knad n\u00e4sta order 18 sep')
        self.assertEqual(p._guidance_date(date(2025,12,31), TODAY), '31 dec 2025')
        self.assertEqual(p._guidance_date(date(2027,1,1), TODAY), '1 jan 2027')

    def test_click_matrix_preserves_actual_signal_across_customer_focuses(self):
        cases = [([], 'B', 'warm_opportunity'), ([], 'A', 'a_prospect'),
                 ([order('one',30)], 'C', 'second_purchase'),
                 ([order('one',30),order('two',50)], 'C', 'repeat_purchase'),
                 ([order('one',200)], 'C', 'reactivation'),
                 ([order('one',200),order('two',220)], 'C', 'reactivation')]
        for kind in ('stockfiller','product_sheet'):
            for rows, segment, focus in cases:
                with self.subTest(kind=kind,focus=focus,count=len(rows)):
                    item = scored(store=customer(segment),orders=rows,mail=email(kind))
                    g = self.assert_canonical(item)
                    self.assertEqual(g['focus_key'],focus)
                    self.assertIn('Stockfiller-klick' if kind=='stockfiller' else 'Produktbladsklick',g['reason_text'])
                    self.assertIn('ingen senare order',g['reason_text'])
                    if focus=='a_prospect':
                        self.assertEqual(item['primary_trigger_type'],'a_prospect_due')
                        self.assertEqual(g['action_label'],'Ta n\u00e4sta steg mot f\u00f6rsta ordern')
                    elif focus=='warm_opportunity':
                        self.assertEqual(g['action_label'],'F\u00f6lj upp m\u00f6jligheten')
                    else:
                        self.assertEqual(g['action_label'],'F\u00f6lj upp best\u00e4llningsintresset' if kind=='stockfiller' else 'F\u00f6lj upp produktintresset')

    def test_dialogue_matrix_is_specific_but_does_not_replace_a_primary_reorder(self):
        for rows, segment, focus in [([], 'B','warm_opportunity'),([], 'A','a_prospect'),
            ([order('one',5)],'C','second_purchase'),
            ([order('one',10),order('two',30)],'C','repeat_purchase'),
            ([order('one',200)],'C','reactivation')]:
            with self.subTest(focus=focus):
                age = 7 if not rows else 3
                item=scored(store=customer(segment),orders=rows,contacts=[contact(age,'Positiv')])
                g=self.assert_canonical(item)
                self.assertEqual(g['focus_key'],focus)
                self.assertIn(f'Positiv dialog f\u00f6r {age} dagar sedan \u00b7 ingen senare order',g['reason_text'])
                if rows: self.assertEqual(g['action_label'],'F\u00f6lj upp dialogen')
        reorder=scored(orders=[order('first',19)],contacts=[contact(3,'Positiv')])
        self.assertEqual(reorder['primary_trigger_type'],'first_order_reorder')
        self.assertIn('positive_dialogue_followup',reorder['covered_trigger_keys'])
        self.assertEqual(self.assert_canonical(reorder)['action_label'],'S\u00e4kra \u00e5terk\u00f6pet')

    def test_a_prospect_explanation_never_changes_context_identity(self):
        baseline=scored(store=customer('A'))
        for kwargs in ({'contacts':[contact(10,'Positiv')]},{'mail':email()},
                       {'contacts':[contact(10,'Positiv')],'mail':email()}):
            item=scored(store=customer('A'),**kwargs)
            self.assertEqual(item['primary_trigger_type'],'a_prospect_due')
            self.assertEqual(app_module.priority_decision_context_hash(item,'olle'),
                             app_module.priority_decision_context_hash(baseline,'olle'))
            self.assert_canonical(item)
        both=scored(store=customer('A'),contacts=[contact(10,'Positiv')],mail=email())
        self.assertIn('Stockfiller-klick',both['primary_reason_text'])
        self.assertNotIn('Positiv dialog',both['primary_reason_text'])

    def test_reactivation_distinguishes_one_delivery_from_repeat_history(self):
        for count in (1,2):
            item=scored(orders=[order(str(n),200+n*20) for n in range(count)])
            g=self.assert_canonical(item)
            self.assertEqual(g['action_label'],'\u00c5teraktivera kunden')
            self.assertEqual(g['reason_text'],
                ('En tidigare leverans' if count==1 else 'Tidigare \u00e5terkommande kund')+
                ' \u00b7 200 dagar sedan senaste leveransen')

    def test_strategic_trigger_uses_its_real_reason_not_reactivation_fallback(self):
        # This trigger currently overlaps other stronger triggers in many cases.
        # Test its presentation directly without changing live eligibility rules.
        for text in ('Strategisk kund \u2013 aldrig kontaktad',
                     'Strategisk kund \u2013 46 dagar sedan senaste kontakt'):
            snapshot={'primary_trigger_type':'strategic_contact_due',
                      'primary_trigger_key':'strategic_contact_due',
                      'covered_trigger_keys':['strategic_contact_due'],
                      'primary_reason_code':'strategic_contact_due','primary_reason_text':text}
            with patch.object(p,'_phase3_trigger_snapshot',return_value=snapshot):
                item=scored(orders=[order('old',200)])
            g=self.assert_canonical(item)
            self.assertEqual(g['action_label'],'Ta ny kontakt')
            self.assertEqual(g['reason_text'],text)

    def test_true_followup_today_overrides_commercial_copy_only_when_date_exists(self):
        for segment in ('A','C'):
            for mail in (None,email()):
                item=scored(store=customer(segment),mail=mail,
                            contacts=[contact(10,'Positiv',TODAY.isoformat())])
                g=item['customer_guidance']
                self.assertEqual(g['status_key'],'act_now')
                self.assertEqual(g['action_key'],'follow_up_today')
                self.assertEqual(g['reason_code'],'follow_up_due_today')
                self.assertEqual(g['reason_text'],'Uttrycklig uppf\u00f6ljning f\u00f6rfaller i dag')
        resolved=scored(contacts=[contact(10,'Positiv',TODAY.isoformat())],orders=[order('today',0)])
        self.assertNotEqual(resolved['customer_guidance']['action_key'],'follow_up_today')

    def test_real_plans_overdue_and_suppressions_stay_authoritative(self):
        for planned,status in [([activity(1)],'planned'),([activity(-1)],'overdue_followup'),
                               ([activity(-1),activity(1)],'planned')]:
            g=scored(store=customer('A'),mail=email(),planned=planned)['customer_guidance']
            self.assertEqual(g['status_key'],status)
            self.assertIn('Telefon',g['reason_text'])
        for days,status in [(1,'planned'),(-1,'overdue_followup')]:
            g=scored(store=customer('A'),mail=email(),contacts=[contact(10,'Neutral',
                     (TODAY+timedelta(days=days)).isoformat())])['customer_guidance']
            self.assertEqual(g['status_key'],status)
        for kwargs,reason in [({'orders':[order('future',-4)]},'future_delivery'),
                             ({'contacts':[contact(1)]},'recent_human_contact'),
                             ({'contacts':[contact(3,'Negativ')]},'negative_contact_cooldown'),
                             ({'mail':email(age=2)},'recent_email_engagement_wait'),
                             ({'suppressions':{'c':'snoozed'}},'snoozed')]:
            item=scored(store=customer('A'),**kwargs)
            self.assertEqual(item['customer_guidance']['status_key'],'wait')
            self.assertEqual(item['customer_guidance']['reason_code'],reason)
            self.assertNotIn('Uttrycklig',item['customer_guidance']['reason_text'])

    def test_inactive_or_handled_signals_are_not_described_as_live(self):
        for kwargs in ({'mail':email(age=15)},
                       {'mail':email(),'contacts':[contact(4)]},
                       {'contacts':[contact(31,'Positiv')]}):
            item=scored(store=customer('A'),**kwargs)
            self.assertEqual(item['primary_reason_text'],'Strategisk kund utan tidigare order')
        later=scored(store=customer('A'),mail=email(),orders=[order('later',4)])
        self.assertNotIn('Stockfiller',later['customer_guidance']['reason_text'])

    def test_unknown_trigger_has_neutral_fallback_not_fabricated_followup(self):
        snapshot={'primary_trigger_type':'new_signal','primary_trigger_key':'new_signal',
                  'covered_trigger_keys':['new_signal'],'primary_reason_code':'new_signal',
                  'primary_reason_text':'Verifierad ny signal'}
        for reason in ('Verifierad ny signal',''):
            with patch.object(p,'_phase3_trigger_snapshot',return_value={**snapshot,'primary_reason_text':reason}):
                g=scored()['customer_guidance']
            self.assertEqual(g['action_label'],'F\u00f6lj upp kunden')
            self.assertEqual(g['reason_text'],reason or 'Aktuell signal beh\u00f6ver f\u00f6ljas upp')
            self.assertNotIn('Uttrycklig',g['reason_text'])


class DeliveryVolumeIntegrityTests(TestCase):
    def test_latest_and_nearest_future_have_their_own_volumes(self):
        item=scored(orders=[order('older',30,11),order('latest',10,13),
                            order('next',-4,15),order('later',-11,99)])
        self.assertEqual(item['latest_delivery_date'],'2026-09-08')
        self.assertEqual(item['latest_delivery_dfp'],13)
        self.assertEqual(item['next_delivery_date'],'2026-09-22')
        self.assertEqual(item['next_delivery_dfp'],15)
        # Historical/forecast contract is intentionally not silently redefined.
        self.assertEqual(item['latest_order_dfp'],99)

    def test_volume_sums_rows_and_multiple_references_on_same_delivery(self):
        item=scored(orders=[order('a',10,8),order('a',10,5),order('b',10,7),
                            order('c',-4,10),order('d',-4,5)])
        self.assertEqual(item['delivery_count'],1)
        self.assertEqual(item['latest_delivery_dfp'],20)
        self.assertEqual(item['next_delivery_dfp'],15)

    def test_no_completed_delivery_cannot_borrow_first_future_volume(self):
        item=scored(store=customer('A'),orders=[order('future',-4,15)])
        self.assertEqual(item['delivery_count'],0)
        self.assertEqual(item['latest_delivery_date'],'')
        self.assertIsNone(item['latest_delivery_dfp'])
        self.assertEqual(item['next_delivery_dfp'],15)
        self.assertNotIn('11\u201390',item['customer_guidance']['focus_label'])
        self.assertEqual(item['customer_guidance']['status_key'],'wait')

    def test_delivery_date_boundary_reuses_same_date_independent_features(self):
        features=p.build_order_features([order('old',10,13),order('today',0,15),order('next',-4,17)])
        before=scored(features=features,today=TODAY-timedelta(days=1))
        after=scored(features=features,today=TODAY)
        self.assertEqual((before['latest_delivery_dfp'],before['next_delivery_dfp']),(13,15))
        self.assertEqual((after['latest_delivery_dfp'],after['next_delivery_dfp']),(15,17))
        self.assertEqual(after['latest_delivery_date'],TODAY.isoformat())

    def test_missing_and_partial_volumes_are_unknown_not_zero_or_latest_order(self):
        for rows in ([order('a',10,None)],
                     [order('a',10,8),order('a',10,None)],
                     [order('a',10,8),order('b',10,None)]):
            item=scored(orders=[*rows,order('next',-4,15)])
            self.assertEqual(item['latest_delivery_date'],'2026-09-08')
            self.assertIsNone(item['latest_delivery_dfp'])
            self.assertEqual(item['next_delivery_dfp'],15)
        for value in ('bad','NaN','inf',True,None):
            self.assertIsNone(scored(orders=[order('missing',10,value)])['latest_delivery_dfp'])
        self.assertIsNone(scored(orders=[order('last',10,13),order('next',-4,None)])['next_delivery_dfp'])

    def test_noncommercial_rows_and_internal_orders_never_supply_delivery_volume(self):
        rows=[order('real',10,13), {**order('credit',3,99),'Total':'-100'},
              {**order('sample',2,99),'Total':'0'},order('zero',1,0),order('negative',1,-7),
              {**order('internal',1,100),'Customer':'Polarb\u00e4r - Ink\u00f6p'}]
        item=scored(orders=rows)
        self.assertEqual(item['latest_delivery_dfp'],13)
        self.assertEqual(item['latest_delivery_date'],'2026-09-08')
        self.assertEqual(item['delivery_count'],1)

    def test_no_history_or_no_future_is_null(self):
        empty=scored()
        self.assertIsNone(empty['latest_delivery_dfp'])
        self.assertIsNone(empty['next_delivery_dfp'])
        self.assertEqual(empty['next_delivery_date'],'')
        self.assertIsNone(scored(orders=[order('past',10)])['next_delivery_dfp'])

    def test_legacy_precomputed_features_do_not_fabricate_a_date_volume_pair(self):
        features=p.build_order_features([order('old',10,13),order('new',-4,15)])
        for row in features.values(): row.pop('delivery_dfp_by_date')
        item=scored(features=features)
        self.assertIsNone(item['latest_delivery_dfp'])
        self.assertIsNone(item['next_delivery_dfp'])
        self.assertEqual(item['latest_order_dfp'],15)

    def test_inconsistent_dates_within_one_order_do_not_get_a_false_combined_total(self):
        item=scored(orders=[order('split',10,13),order('split',-4,15)])
        self.assertIsNone(item['next_delivery_dfp'])
        self.assertEqual(item['latest_order_dfp'],28)

    def test_same_name_customers_keep_delivery_volumes_separate(self):
        stores=[customer(customer_id='c',number='100'),customer(customer_id='other',number='200')]
        features=p.build_order_features([order('same-ref',10,13),order('same-ref',10,99,'other','200')])
        result=p.build_priority_customers(stores,features,{},None,TODAY,limit=2,now=NOW)
        self.assertEqual({r['customer_id']:r['latest_delivery_dfp'] for r in result},{'c':13,'other':99})

    def test_existing_order_date_fallback_remains_paired_and_scoring_unchanged(self):
        item=scored(orders=[{**order('fallback',10,13),'Delivery date':''}])
        self.assertEqual(item['latest_delivery_date'],'2026-09-08')
        self.assertEqual(item['latest_delivery_dfp'],13)
        self.assertEqual(item['delivery_count'],1)


# Exercise the actual shared snapshot -> insights -> planning API boundary.
from test_planning import PlanningApiTestCase


class GuidanceIntegrityApiTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        self.old_stub = app_module.app.config.get('PLANNING_SUGGESTIONS_STUB')
        app_module.app.config['PLANNING_SUGGESTIONS_STUB'] = False
        self.addCleanup(self.restore_stub)
        instant = NOW.replace(tzinfo=app_module.STOCKHOLM_ZONE)
        self.clock = patch.object(app_module,'stockholm_now',return_value=instant)
        self.day = patch.object(app_module,'stockholm_today',return_value=TODAY)
        self.clock.start(); self.day.start()
        app_module.invalidate_priority_snapshot()

    def tearDown(self):
        self.day.stop()
        self.clock.stop()
        super().tearDown()

    def restore_stub(self):
        if self.old_stub is None: app_module.app.config.pop('PLANNING_SUGGESTIONS_STUB',None)
        else: app_module.app.config['PLANNING_SUGGESTIONS_STUB'] = self.old_stub
        app_module.invalidate_priority_snapshot()

    def append_order(self, ref, age, volume):
        row = {**order(ref,age,volume), 'Customer':'Butik A','Customer number':'C-1',
               'customer_id':'11111111-1111-4111-8111-111111111111'}
        self.spreadsheet.worksheet('order_rows').append_row([
            row.get(col,'') for col in app_module.ORDER_COLUMNS])

    def test_insights_and_candidate_transport_same_paired_delivery_values(self):
        self.append_order('old',30,11); self.append_order('latest',10,13)
        self.append_order('next',-4,15); self.append_order('later',-11,99)
        response=self.client.get('/customer-insights')
        self.assertEqual(response.status_code,200)
        ins=response.get_json()['butik a']
        expected={'latest_delivery_date':'2026-09-08','latest_delivery_dfp':13,
                  'next_delivery_date':'2026-09-22','next_delivery_dfp':15}
        for key,value in expected.items(): self.assertEqual(ins[key],value)
        candidates=app_module.planning_suggestion_candidates(
            self.spreadsheet, {'user_name':'olle','name':'Olle'})
        candidate=next(c for c in candidates if c['customer']=='Butik A')
        for key,value in expected.items(): self.assertEqual(candidate['contact_context'][key],value)
        self.assertEqual(candidate['customer_guidance'],ins['customer_guidance'])
        self.assertTrue(candidate['externally_suppressed'])

    def test_second_purchase_guidance_matches_actual_suggestion(self):
        self.append_order('first',11,13)
        ins=self.client.get('/customer-insights').get_json()['butik a']
        response=self.client.get('/planning/suggestions?preview_limit=20')
        self.assertEqual(response.status_code,200)
        payload=response.get_json()
        suggestions=[payload.get('suggestion')]+payload.get('preview',[])
        selected=next(s for s in suggestions if s and s.get('customer')=='Butik A')
        self.assertEqual(selected['customer_guidance'],ins['customer_guidance'])
        self.assertEqual(selected['reason_text'],ins['primary_reason_text'])
        self.assertEqual(selected['action_label'],'S\u00e4kra \u00e5terk\u00f6pet')
        self.assertEqual(selected['contact_context']['latest_delivery_dfp'],13)

    def test_insights_serializes_unknown_delivery_volume_as_null(self):
        self.append_order('first',10,None); self.append_order('future',-4,15)
        ins=self.client.get('/customer-insights').get_json()['butik a']
        self.assertIn('latest_delivery_dfp',ins)
        self.assertIsNone(ins['latest_delivery_dfp'])
        self.assertEqual(ins['next_delivery_dfp'],15)
