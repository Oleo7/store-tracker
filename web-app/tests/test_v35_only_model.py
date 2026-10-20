"""Release contract: only v3, independent business rules, lexicographic routes.

All CRM examples are synthetic. Assertions use actual model, Flask endpoints
and solvers. No production fallbacks or copied scoring formulas are introduced.
"""
import ast
from copy import deepcopy
from datetime import timedelta
from itertools import permutations
from pathlib import Path
import os
import random
import sys
from unittest import TestCase
from unittest.mock import patch

WEB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB))
import app as application
import priority
import value_prioritization as value
import route_proposal as local_route
import route_optimization as google_route
from routing_objective import priority_units, route_objective_key, skipped_priority_penalty, ROUTE_RANKING_CONTRACT
from planning_suggestions import public_suggestion
import test_history_priority_v34 as history
import test_route_contact_policy as contact_policy
import test_route_optimization as google_fixture


class OnlyCurrentModelTests(TestCase):
    def test_no_runtime_old_formula_or_mode_or_test_reference_import(self):
        banned = {'calculate_priority_score_v2', '_build_priority_customers_legacy',
                  'PRIORITY_RANKING_MODE', 'PRIORITY_SCORING_POLICY', 'SCORING_POLICIES'}
        sources = [*WEB.glob('*.py'), *(WEB.parent/'scripts').glob('*.py')]
        for file in sources:
            tree = ast.parse(file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    self.assertNotIn(node.id, banned, (file.name,node.lineno))
                elif isinstance(node, ast.Constant) and isinstance(node.value,str):
                    self.assertNotIn(node.value,banned,(file.name,node.lineno))
                elif isinstance(node,(ast.Import,ast.ImportFrom)):
                    self.assertNotIn('historical_priority_reference',ast.unparse(node),file.name)
        self.assertFalse(hasattr(priority,'calculate_priority_score_v2'))
        self.assertFalse(hasattr(priority,'ranking_mode'))

    def test_obsolete_environment_never_changes_the_only_model(self):
        before,_=history.model()
        with patch.dict(os.environ,{'PRIORITY_RANKING_MODE':'v2.2','PRIORITY_SCORING_POLICY':'legacy'}):
            after,_=history.model()
        for field in ('score_version','priority_score','work_priority_precise','list_guidance'):
            self.assertEqual(before[field],after[field])
        self.assertEqual(after['score_version'],'v3.5')

    def test_retired_explicit_api_model_arguments_fail_instead_of_switching(self):
        for mode in ('v2.2','v2.1','legacy'):
            with self.subTest(mode=mode),self.assertRaises(ValueError):
                priority.build_priority_customers([history.C],{}, {},None,history.NOW.date(),scoring_version=mode)

    def test_old_scorer_cannot_be_called_from_actual_feature_pipeline(self):
        with patch.object(priority,'calculate_priority_score_v2',create=True,
                          side_effect=AssertionError('Retired scorer called')):
            row,_=history.model(orders=[history.order(24)],contacts=[history.contact(25,'Positiv')])
        self.assertEqual(row['action_type'],'second_purchase_due')
        self.assertEqual(row['priority_score'],row['work_priority_score'])
        self.assertEqual(row['priority_score_precise'],row['work_priority_precise'])

    def test_adapters_reject_missing_invalid_v3_even_with_old_score_100(self):
        valid,_=history.model()
        mutations=[{'value_model_version':'v2.2'},{'score_version':'v2.2'},
                   {'contact_priority_precise':None},{'contact_priority_precise':True},
                   {'contact_priority_precise':float('nan')},{'contact_priority_precise':float('inf')},
                   {'contact_priority_precise':-1},{'contact_priority_precise':101},
                   {'contact_priority_score':None},{'contact_priority_score':True},
                   {'contact_priority_score':-1}]
        for change in mutations:
            broken={**valid,'priority_score':100,'priority_score_at_creation':100,**change}
            with self.subTest(change=change):
                self.assertIsNone(application.route_priority_values(broken))
                self.assertFalse(application.email_contact_decision(broken).get('allowed'))

    def test_old_suggestion_never_reuses_stored_ranking_or_action(self):
        old={'priority_score_at_creation':100,'score_version':'v2.2','reason_text_at_creation':'Old call',
             'customer_id':'c','customer':'Test','status':'pending'}
        result=public_suggestion(old)
        self.assertTrue(result['score_unavailable'])
        self.assertEqual(result['priority_score'],0)
        self.assertEqual(result['reason_text'],'')
        self.assertEqual(result['action_label'],'')
        self.assertFalse(result['can_call'])

    def test_scoring_aliases_and_workflow_never_leave_old_value_live(self):
        row,_=history.model()
        self.assertGreater(row['priority_score'],0)
        scope=row['list_guidance']['action_scope']
        paused=priority.apply_workflow_suppressions([row],{'c1':{'status':'snoozed','source_scope':scope}})[0]
        for field in ('priority_score','priority_score_precise','contact_priority_precise','work_priority_precise'):
            self.assertEqual(paused[field],0,field)
        self.assertGreater(paused['business_value_raw'],0)

    def test_ten_day_positive_buyer_and_actual_earlier_promise(self):
        orders=[history.order(70),history.order(1)]
        # Use older deliveries with a non-overdue long cycle to isolate dialogue.
        orders=[history.order(70),history.order(15)]
        for age in (9,10):
            row,_=history.model(contacts=[history.contact(age,'Positiv')],orders=orders)
            if age==9:self.assertFalse(row['can_start_unscheduled_contact'])
            else:self.assertEqual(row['action_type'],'positive_buyer_followup')
        promise=history.contact(5,'Positiv',contact_id='p',follow_up_date=history.NOW.date().isoformat(),
            follow_up_purpose='customer_followup',follow_up_agreement_confirmed='Y')
        row,_=history.model(contacts=[promise],orders=orders)
        self.assertTrue(row['can_start_unscheduled_contact'])
        self.assertEqual(row['action_type'],'follow_up_due_today')

    def test_self_ordering_and_first_order_are_independent_v3_rules(self):
        for days,expected in ((6,'first_order_new'),(8,'first_order_onboarding'),(24,'second_purchase_due'),(91,'single_order_reactivation_due')):
            row,_=history.model(orders=[history.order(days)])
            self.assertEqual(row['action_type'],expected)
        source=history.contact(25,'Positiv',follow_up_date=(history.NOW.date()+timedelta(days=2)).isoformat(),
            comment='Kunden best\u00e4ller sj\u00e4lv i Stockfiller')
        row,_=history.model(contacts=[source])
        self.assertEqual(row['action_type'],'self_ordering_wait')

    def test_legacy_preview_rejected_before_any_source_access_or_write(self):
        with application.app.test_request_context(), patch.object(application,'get_customer_rows',
                side_effect=AssertionError('Old preview reached source')):
            result,error=application.apply_planning_route(spreadsheet=None,owner={},preview={'stops':[]},client_request_id='old')
        self.assertIsNone(result);self.assertEqual(error[1],409)
        self.assertEqual(error[0].get_json()['code'],'route_preview_expired_or_legacy')

    def test_old_google_cache_cannot_reuse_matching_geography(self):
        inputs={'input_fingerprint':'same','fingerprint':'new'}
        self.assertFalse(application._route_operation_input_matches(
            {'engine_version':'ro-v1','input_fingerprint':'same','request_fingerprint':'new'},inputs))
        self.assertFalse(application._route_operation_input_matches(
            {'engine_version':google_route.ROUTE_ENGINE_VERSION,'input_fingerprint':'same','request_fingerprint':'old'},inputs))


class RouteObjectiveTests(TestCase):
    @staticmethod
    def candidate(index,score,required=False):
        return local_route.RouteCandidate(index+1,'Synthetic '+str(index),local_route.Coordinate(57.7,11.9),
            priority_score=round(score),required=required,commercial_value=score)

    def test_one_micro_point_always_dominates_all_feasible_driving(self):
        self.assertLess(route_objective_key([80.000001],32400),route_objective_key([80],0))
        self.assertGreater(skipped_priority_penalty(80.000001,32400)-skipped_priority_penalty(80,32400),32400)
        self.assertLess(route_objective_key([40,40],5),route_objective_key([80],6))

    def test_actual_solver_selects_high_priority_even_when_farther(self):
        cs=[self.candidate(0,95),self.candidate(1,85)]
        solution=local_route.solve_route(cs,((2000,10),(0,5000),(5000,0)),return_seconds=(2000,10),
            max_total_seconds=6000,service_seconds_per_stop=100,max_route_stops=1)
        self.assertEqual(solution.route_indices,(0,))
        tied=[self.candidate(0,85),self.candidate(1,85)]
        solution=local_route.solve_route(tied,((2000,10),(0,5000),(5000,0)),return_seconds=(2000,10),
            max_total_seconds=6000,service_seconds_per_stop=100,max_route_stops=1)
        self.assertEqual(solution.route_indices,(1,))

    def test_actual_solver_maximizes_sum_not_highest_single_customer(self):
        cs=[self.candidate(0,95),self.candidate(1,65),self.candidate(2,65)]
        solution=local_route.solve_route(cs,((40,1,1),(0,100,100),(100,0,1),(100,1,0)),
            return_seconds=(40,1,1),max_total_seconds=101,service_seconds_per_stop=20,max_route_stops=3)
        self.assertEqual(set(solution.route_indices),{1,2})

    def test_actual_exact_solver_matches_independent_exhaustive_objective(self):
        rng=random.Random(3550)
        for sample in range(35):
            n=5; scores=[rng.choice((20,40,80))+rng.choice((0,.000001)) for _ in range(n)]
            cs=[self.candidate(i,s,required=(i==0 and sample%3==0)) for i,s in enumerate(scores)]
            matrix=[[rng.randint(1,70) for _ in range(n)] for _ in range(n+1)]
            for i in range(n):matrix[i+1][i]=0
            returns=[rng.randint(1,70) for _ in cs]
            required={i for i,c in enumerate(cs) if c.required}; options=[]
            for count in range(1,4):
                for path in permutations(range(n),count):
                    if not required.issubset(path):continue
                    drive=matrix[0][path[0]]+sum(matrix[a+1][b] for a,b in zip(path,path[1:]))+returns[path[-1]]
                    if drive+20*count<=200:
                        options.append((route_objective_key([scores[i] for i in path],drive,[cs[i].row for i in path]),path))
            if not options:continue
            solution=local_route.solve_route(cs,matrix,return_seconds=returns,max_total_seconds=200,
                service_seconds_per_stop=20,max_route_stops=3)
            self.assertEqual(solution.route_indices,min(options)[1],sample)
            self.assertTrue(solution.optimality_proven)

    def test_provider_request_uses_only_priority_then_driving_cost(self):
        shipments=[google_fixture.shipment(1,score=80.000001),google_fixture.shipment(2,score=80)]
        body=google_route.build_optimize_tours_request(run_id='test',owner_user_name='olle',
            route_start=google_fixture.NOW,start=google_fixture.START,shipments=shipments)
        vehicle=body['model']['vehicles'][0]
        self.assertEqual(vehicle['costPerHour'],0)
        self.assertEqual(vehicle['costPerTraveledHour'],3600)
        self.assertNotIn('quadraticSoftMaxDuration',vehicle['routeDurationLimit'])
        self.assertGreater(body['model']['shipments'][0]['penaltyCost']-body['model']['shipments'][1]['penaltyCost'],google_route.ROUTE_MAX_SECONDS)

    def test_route_modules_never_fall_back_to_legacy_score(self):
        for invalid in (None,True,float('nan'),float('inf'),-1,101):
            item=google_fixture.shipment();item['priority_score']=99;item['commercial_value']=invalid
            with self.subTest(value=invalid),self.assertRaises((ValueError,google_route.RouteOptimizationError)):
                google_route.shipment_penalty(item)
        with self.assertRaises(local_route.RouteVerificationError):
            _=local_route.RouteCandidate(1,'Old',local_route.Coordinate(57.7,11.9),100).objective_value


class ActualRouteConsumersTests(TestCase):
    def setUp(self):
        self.fixture=contact_policy.RouteContactPolicyTests()
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)

    def test_channel_recommendation_cannot_change_either_route_input_or_order(self):
        baseline=self.fixture.snapshot(); expected={}
        for channel in ('visit','phone','email'):
            snapshot=deepcopy(baseline)
            for row in snapshot['priorities']:
                row['recommended_contact_type']=channel
                row['list_guidance']['recommended_contact_type']=channel
            for local in (False,True):
                result,error=self.fixture.route(snapshot,legacy=local)
                self.assertIsNone(error)
                rows=result['stops'] if local else result['shipments']
                outcome=[(r.get('customer_id') or r.get('row'),r.get('commercial_value')) for r in rows]
                if local in expected:self.assertEqual(outcome,expected[local])
                else:expected[local]=outcome

    def test_actual_booked_phone_same_route_day_excludes_visit_in_both_engines(self):
        for day_delta,excluded in ((0,True),(1,False)):
            snapshot=self.fixture.snapshot(plans=[contact_policy.appointment(day_delta,contact_type='phone')])
            for local in (False,True):
                result,error=self.fixture.route(snapshot,legacy=local)
                self.assertIsNone(error)
                rows=result['stops'] if local else result['shipments']
                found=any(r.get('customer_id')==contact_policy.CID or r.get('row')==2 for r in rows)
                self.assertEqual(found,not excluded,(local,day_delta))

    def test_both_routes_ignore_old_100_when_current_priority_missing(self):
        snapshot=self.fixture.snapshot();row=self.fixture.priority(snapshot)
        row['priority_score']=100;row['contact_priority_precise']=None
        for local in (False,True):
            result,error=self.fixture.route(snapshot,legacy=local)
            self.assertIsNone(error)
            rows=result['stops'] if local else result['shipments']
            self.assertFalse(any(r.get('customer_id')==contact_policy.CID or r.get('row')==2 for r in rows))


class CurrentUiAndApplyTests(TestCase):
    def test_browser_helpers_never_reuse_old_score_or_guidance(self):
        import subprocess
        import shutil
        if not shutil.which('node'):self.skipTest('Node required')
        run=subprocess.run(['node',str(WEB/'tests'/'v35_no_fallback_frontend.cjs')],capture_output=True,text=True,timeout=20)
        self.assertEqual(run.returncode,0,run.stdout+run.stderr)

    def test_saving_a_local_route_rechecks_current_priority_before_mutations(self):
        import tests.test_planning as planning_fixture
        fixture=planning_fixture.PlanningRouteApiTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        response,_=fixture.create_preview([fixture.route_stop(2,1,21)])
        self.assertEqual(response.status_code,200,response.get_json())
        token=response.get_json()['preview_token']
        customers=application.get_customer_rows(fixture.spreadsheet)
        current={'customers':customers,'priorities':[
            {'customer_id':c['customer_id'],'value_model_version':'v2.2','score_version':'v2.2',
             'priority_score':100,'list_guidance':{'can_start_unscheduled_contact':True}}
            for c in customers]}
        before=deepcopy(fixture.planning_rows())
        with patch.object(application,'get_authoritative_priority_snapshot',return_value=current),patch.object(
                application,'priority_workflow_suppressions',return_value={}):
            result=fixture.client.post('/planning/route-apply',json={'client_request_id':'old-score-injection','preview_token':token})
        self.assertEqual(result.status_code,409,result.get_json())
        self.assertEqual(result.get_json()['code'],'route_contact_changed')
        self.assertEqual(fixture.planning_rows(),before)
