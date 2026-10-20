"""Independent PR48 audit counterexamples, synthetic inputs only."""
import itertools
import os
import sys
from pathlib import Path
from datetime import timedelta
from unittest import TestCase
from unittest.mock import patch

import test_history_priority_v34 as history
import app as application
import route_proposal as routes
import test_planning as planning_fixture
NOW = planning_fixture.NOW


class CommercialAuditTests(TestCase):
    def test_f2_positive_first_buyer_waits_ten_days(self):
        scores = []
        for age in range(12):
            row, _ = history.model(orders=[history.order(28)], contacts=[history.contact(age, "Positiv")])
            scores.append(row["work_priority_precise"])
            self.assertEqual(row["can_start_unscheduled_contact"], age >= 10, (age, row["action_type"]))
        self.assertEqual(scores[:10], [0] * 10)
        self.assertGreaterEqual(scores[11], scores[10])

    def test_f2_repeat_buyer_and_documented_separate_promise(self):
        for age in [0, 3, 9, 10, 11, 20, 30, 45]:
            contacts = [history.contact(age, "Positiv", contact_id="positive")]
            row, _ = history.model(orders=[history.order(90), history.order(60)], contacts=contacts)
            self.assertEqual(row["can_start_unscheduled_contact"], age >= 10)
        row, _ = history.model(orders=[history.order(28)], contacts=[history.contact(2, "Positiv",
            contact_id="promise", follow_up_purpose="service_followup", follow_up_agreement_confirmed="Y",
            follow_up_date=history.NOW.date().isoformat())])
        self.assertTrue(row["can_start_unscheduled_contact"])
        blocked, _ = history.model(orders=[history.order(28)], contacts=[
            history.contact(2, "Positiv", contact_id="promise", follow_up_purpose="service_followup",
                follow_up_agreement_confirmed="Y", follow_up_date=history.NOW.date().isoformat()),
            history.contact(1, "Återkom ej", contact_id="ban")])
        self.assertFalse(blocked["can_start_unscheduled_contact"])

    def test_f4_calendar_does_not_clarify_service_evidence(self):
        source = history.contact(150, "Neutral", contact_id="service", comment="Reklamation, leveransproblem",
                                 follow_up_date=(history.NOW-timedelta(days=20)).date().isoformat())
        for days, confirmed in itertools.product([-20, 0, 5], ["N", "Y"]):
            plan = history.plan(days, source="follow_up", source_contact_id="service",
                created_at=(history.NOW-timedelta(days=150)).isoformat(), appointment_confirmed=confirmed)
            row, state = history.model(contacts=[source], plans=[plan])
            item = state["obligations"]["c1"][0]
            self.assertEqual(item["status"], "needs_review")
            self.assertFalse(row["can_execute_planned_contact"])
        routine, state = history.model(contacts=[source])
        self.assertTrue(routine["can_start_unscheduled_contact"])
        self.assertEqual(state["obligations"]["c1"][0]["status"], "needs_review")

    def test_f5_permissions_ignore_sheet_row_order(self):
        rows = [history.contact(90, "Återkom ej", contact_id="deny"),
                history.contact(90, "Kontakt tillåten", contact_id="allow")]
        for permutation in itertools.permutations(rows):
            row, state = history.model(contacts=permutation)
            self.assertEqual(row["action_type"], "do_not_contact")
            self.assertFalse(row["can_start_unscheduled_contact"])
            self.assertEqual(state["contact_permissions"]["c1"]["status"], "do_not_contact")
        row, state = history.model(contacts=rows+[history.contact(89, "Kontakt tillåten", contact_id="later")])
        self.assertTrue(row["can_start_unscheduled_contact"])
        self.assertEqual(state["contact_permissions"]["c1"]["status"], "allowed")

    def test_f8_retry_sequence_expires_without_removing_customer(self):
        for age in [5, 30, 31, 60, 90, 3650]:
            row, _ = history.model(contacts=[history.contact(age, "Ej anträffbar")])
            self.assertTrue(row["can_start_unscheduled_contact"])
            self.assertEqual(row["action_type"] == "unanswered_retry_due", age <= 30, age)
        promise = history.contact(150, "Neutral", contact_id="service", follow_up_purpose="service_followup",
            follow_up_agreement_confirmed="Y", follow_up_date=(history.NOW-timedelta(days=1)).date().isoformat())
        row, state = history.model(contacts=[promise, history.contact(90, "Ej anträffbar", contact_id="attempt")])
        self.assertTrue(row["can_start_unscheduled_contact"])
        self.assertNotIn(state["obligations"]["c1"][0]["status"], {"resolved", "cancelled"})


class CalendarAuditTests(planning_fixture.PlanningApiTestCase):
    def test_f4_only_traced_manual_clarification_enables_service(self):
        self.append_contact_row(contact_id="service", date_time="2026-01-01 09:00", result="Neutral",
            comment="Reklamation, leveransproblem", follow_up_date="2026-07-20")
        def item():
            return application.customer_state_snapshot(self.spreadsheet)["obligations"]["11111111-1111-4111-8111-111111111111"][0]
        self.assertEqual(item()["status"],"needs_review")
        base={"customer_id":"11111111-1111-4111-8111-111111111111", "obligation_id":"followup:service",
              "action":"reschedule", "scheduled_at":"2026-07-28T09:00+02:00", "contact_type":"phone"}
        response=self.client.post("/customer-obligations/action",json={**base,"client_request_id":"unclarified",
            "expected_revision":item()["revision"]})
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(item()["status"],"needs_review")
        response=self.client.post("/customer-obligations/action",json={**base,"client_request_id":"clarified",
            "expected_revision":item()["revision"],"customer_agreement_confirmed":True,
            "reason":"Kunden bekräftade reklamationens omfattning och återkontakten."})
        self.assertEqual(response.status_code,200,response.get_json())
        self.assertEqual(item()["status"],"scheduled")
        self.assertEqual(item()["review_status"],"clarified")
        self.assertTrue(item()["review_resolution"]["actor_user_name"])

    def test_f3_cannot_reassign_a_source_linked_activity(self):
        self.append_contact_row(contact_id="source", follow_up_date="2026-07-28",
                                follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")
        plan = self.append_planning_row(planned_activity_id="linked", source="follow_up",
            source_contact_id="source", obligation_id="followup:source")
        response = self.client.patch("/planning/activities/linked", json={"client_request_id":"reassign",
            "expected_revision":plan["revision"], "customer_id":"33333333-3333-4333-8333-333333333333"})
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(self.planning_rows()[0]["customer_id"], plan["customer_id"])

    def test_f3_legacy_cross_customer_plan_is_quarantined(self):
        self.append_contact_row(contact_id="source", follow_up_date="2026-07-28",
                                follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")
        self.append_planning_row(planned_activity_id="bad", source="follow_up", source_contact_id="source",
            obligation_id="followup:source", customer_row=4)
        state = application.customer_state_snapshot(self.spreadsheet)
        a = state["obligations"]["11111111-1111-4111-8111-111111111111"][0]
        self.assertNotEqual(a.get("planned_activity_id"), "bad")
        b = state["obligations"]["33333333-3333-4333-8333-333333333333"][0]
        self.assertEqual(b["status"], "needs_review")
        self.assertTrue(b["identity_conflict"])

    def test_server_emergency_stop_and_production_writer_gate(self):
        with application.app.test_request_context():
            result, error = application.apply_planning_route(spreadsheet=self.spreadsheet, owner={},
                preview={"ranking_contract":"v35-priority-lexicographic-1"}, client_request_id="old-contract")
            self.assertIsNone(result)
            self.assertEqual(error[0].get_json()["code"], "route_preview_expired_or_legacy")
        with patch.dict(os.environ, {"CRM_RISKY_OPERATIONS_STOPPED":"true"}):
            for url, method in [("/planning/route-apply","post"), ("/planning/activities","post"),
                                ("/customers/2/email-proposal/send","post"),
                                ("/customer-contact-permission","post"), ("/route-proposal","get")]:
                response = getattr(self.client, method)(url, json={})
                self.assertEqual(response.status_code, 503, url)
        with patch.dict(application.app.config, {"TESTING":False}), patch.dict(os.environ,
                {"APP_ENV":"production", "CRM_EXCLUSIVE_WRITER_CONFIRMED":"false"}):
            with application.app.test_request_context():
                result, error = application.apply_planning_route(spreadsheet=self.spreadsheet, owner={},
                    preview={"ranking_contract":application.ROUTE_RANKING_CONTRACT}, client_request_id="gate")
                self.assertIsNone(result)
                self.assertEqual(error[0].get_json()["code"], "exclusive_writer_required")


class FreshRouteAuditTests(planning_fixture.PlanningApiTestCase):
    route_stop = planning_fixture.PlanningRouteApiTests.route_stop
    route_payload = planning_fixture.PlanningRouteApiTests.route_payload
    create_preview = planning_fixture.PlanningRouteApiTests.create_preview

    def test_f1_both_engines_required_optional_external_changes(self):
        for engine, required, change in itertools.product(["legacy", "google"], [False, True], ["ban", "owner", "booking"]):
            with self.subTest(engine=engine, required=required, change=change):
                fixture = planning_fixture.PlanningRouteApiTests()
                fixture.setUp()
                try:
                    fixture.spreadsheet._store_tracker_enable_read_cache = True
                    application._sheet_read_cache.clear(); application.invalidate_priority_snapshot()
                    if required:
                        fixture.append_planning_row(planned_activity_id="required", appointment_confirmed="Y")
                    response, _ = fixture.create_preview([fixture.route_stop(2,1,21)])
                    self.assertEqual(response.status_code, 200, response.get_json())
                    preview = application.planning_preview_serializer().loads(response.get_json()["preview_token"])
                    if engine == "google":
                        inputs, error = application.build_route_optimization_inputs(spreadsheet=fixture.spreadsheet,
                            owner={"user_name":"olle", "name":"Olle"}, route_date=NOW.date()+timedelta(days=1),
                            start=application.Coordinate(57.7,11.9),
                            route_start_at_override=application.parse_planning_datetime(preview.get("route_start_at")))
                        self.assertIsNone(error)
                        preview.update(route_engine_version=application.ROUTE_ENGINE_VERSION,
                                       route_optimization_fingerprint=inputs["fingerprint"])
                    token = application.planning_preview_serializer().dumps(preview)
                    application.get_authoritative_priority_snapshot(fixture.spreadsheet,today=NOW.date()+timedelta(days=1))
                    if change == "ban":
                        fixture.append_contact_row(contact_id="external-ban",result="Återkom ej",follow_up_date="")
                    elif change == "owner":
                        sheet = fixture.spreadsheet.worksheet("customers_enriched")
                        sheet.values[1][sheet.values[0].index("sales_person")] = "Sofia"
                    else:
                        fixture.append_planning_row(planned_activity_id="external-phone",contact_type="phone")
                    before = fixture.planning_rows()
                    with patch.object(application,"route_optimization_provider",side_effect=AssertionError("apply must not call Google")):
                        for _ in range(2):
                            result = fixture.client.post("/planning/route-apply",json={"client_request_id":"changed", "preview_token":token})
                            self.assertEqual(result.status_code,409,result.get_json())
                            self.assertEqual(fixture.planning_rows(),before)
                finally:
                    fixture.tearDown(); fixture.doCleanups()
                    application._sheet_read_cache.clear(); application.invalidate_priority_snapshot()

    def test_f1_warm_cache_external_ban_never_saves(self):
        self.spreadsheet._store_tracker_enable_read_cache = True
        application._sheet_read_cache.clear(); application.invalidate_priority_snapshot()
        self.addCleanup(application._sheet_read_cache.clear)
        self.addCleanup(application.invalidate_priority_snapshot)
        response, _ = self.create_preview([self.route_stop(2, 1, 21)])
        self.assertEqual(response.status_code, 200, response.get_json())
        token = response.get_json()["preview_token"]
        application.get_authoritative_priority_snapshot(self.spreadsheet, today=NOW.date()+timedelta(days=1))
        self.append_contact_row(contact_id="external-ban", result="Återkom ej", follow_up_date="",
                                date_time="2026-07-27 10:00")
        for _ in range(2):
            result = self.client.post("/planning/route-apply", json={"client_request_id":"f1", "preview_token":token})
            self.assertEqual(result.status_code, 409, result.get_json())
            self.assertEqual(len(self.planning_rows()), 0)

    def test_f1_read_failure_fails_closed(self):
        response, _ = self.create_preview([self.route_stop(2, 1, 21)])
        with patch.object(self.spreadsheet.worksheet("sales_activities"), "get_all_values", side_effect=RuntimeError("synthetic read outage")):
            result = self.client.post("/planning/route-apply", json={"client_request_id":"f1-error",
                "preview_token":response.get_json()["preview_token"]})
        self.assertGreaterEqual(result.status_code, 500)
        self.assertEqual(len(self.planning_rows()), 0)


class RouteAuditTests(TestCase):
    def test_f9_sparse_pair_survives_shortlisting(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
        from benchmark_route_shortlist import benchmark, star_case, small_case
        result = benchmark(star_case())
        self.assertEqual(result["score"], 140)
        self.assertEqual(result["drive_seconds"], 221)
        for seed in range(3):
            result = benchmark(small_case(seed))
            self.assertEqual(result["score_gap"], 0)
            self.assertEqual(result["secondary_gap_seconds"], 0)

    def test_f6_seconds_break_equal_score_ties(self):
        stops=[{"customer_id":name,"customer_row":i+2,"latitude":57.7+i*.01,"longitude":11.9,
            "commercial_value":80.0,"priority_score":80,"required":False,"duration_minutes":20}
            for i,name in enumerate(["a","b"])]
        class Road:
            def get_matrix_seconds(self,*args,**kwargs):
                return routes.TravelTimeResult([[0,119,61],[119,0,600],[61,600,0]],1,0)
        with application.app.test_request_context(), patch.object(application,"get_route_travel_time_provider",return_value=Road()):
            selected, meta, error = application.schedule_planning_route_with_anchors(stops=stops,
                fixed_non_route=[],route_start_at=history.NOW.replace(hour=16,minute=35),start=routes.Coordinate(57.7,11.9))
        self.assertIsNone(error)
        self.assertEqual([s["customer_id"] for s in selected],["b"])
        self.assertEqual(meta["objective_drive_seconds"],122)
        self.assertIn(":01", selected[0]["arrival_at"])
        self.assertFalse(meta["selection_optimality_proven"])
        self.assertTrue(meta["candidate_pool_optimality_proven"])

    def test_f7_exact_day_end_is_feasible(self):
        candidates=[routes.RouteCandidate(1,"a",routes.Coordinate(57.7,11.9),95,commercial_value=95),
                    routes.RouteCandidate(2,"b",routes.Coordinate(57.8,11.9),85,commercial_value=85)]
        for budget, expected in [(59,(1,)),(60,(0,)),(61,(0,))]:
            solution=routes.solve_route(candidates,[[20,10],[0,100],[100,0]],return_seconds=[20,10],
                max_total_seconds=budget,service_seconds_per_stop=20,max_route_stops=1)
            self.assertEqual(solution.route_indices,expected)
