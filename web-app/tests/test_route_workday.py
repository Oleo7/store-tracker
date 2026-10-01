from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app as app_module
from route_optimization import (
    RouteOptimizationError, TrustedCoordinate, build_optimize_tours_request,
    parse_optimize_tours_response,
)
from route_workday import available_route_seconds, lunch_breaks, plan_route_lunch, RouteLunchNotFeasible
from tests.test_planning import ConstantRoadProvider, default_spreadsheet


ZONE = ZoneInfo("Europe/Stockholm")
DAY = datetime(2026, 10, 2, tzinfo=ZONE).date()
OWNER = {"user_name": "olle", "name": "Olle", "role": "Säljare", "admin": False}
GPS = app_module.Coordinate(56.55555, 12.44444)
A = "11111111-1111-4111-8111-111111111111"
C = "33333333-3333-4333-8333-333333333333"


def at(hour, minute=0):
    return datetime(2026, 10, 2, hour, minute, tzinfo=ZONE)


class RouteWorkdayTests(TestCase):
    def setUp(self):
        self.sheet = default_spreadsheet()
        self.planned = self.sheet.worksheet(app_module.PLANNED_ACTIVITIES_SHEET)
        self.now = patch.object(app_module, "stockholm_now", return_value=at(8) - timedelta(days=1))
        self.now.start()
        self.addCleanup(self.now.stop)
        self.context = app_module.app.test_request_context()
        self.context.push()
        self.addCleanup(self.context.pop)

    def activity(self, **changes):
        row = {
            "planned_activity_id": "contact-a", "user_name": "olle", "sales_person": "Olle",
            "customer_id": A, "customer_row": 2, "customer": "Butik A",
            "contact_type": "phone", "scheduled_at": at(10, 30).isoformat(),
            "status": "planned", "source": "manual", "appointment_confirmed": "N",
            "time_is_estimated": "N", "duration_minutes": 120, "revision": 1,
        }
        row.update(changes)
        self.planned.values.append([row.get(key, "") for key in app_module.PLANNED_ACTIVITY_COLUMNS])
        return row

    def snapshot(self):
        customers = app_module.get_customer_rows(self.sheet)
        return {
            "customers": customers,
            "priorities": [{"row": item["row"], "customer_id": item["customer_id"], "priority_score": 80}
                           for item in customers],
            "planned_activity_rows": self.planned.dict_rows(), "contact_rows": [],
        }

    def inputs(self, snapshot=None):
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=snapshot or self.snapshot()):
            inputs, error = app_module.build_route_optimization_inputs(
                spreadsheet=self.sheet, owner=OWNER, route_date=DAY, start=GPS,
            )
        self.assertIsNone(error, error)
        return inputs

    def request(self, inputs):
        return build_optimize_tours_request(
            run_id="workday-test", owner_user_name="olle", route_start=inputs["route_start_at"],
            start=inputs["start"], shipments=inputs["shipments"], fixed_breaks=inputs["fixed_breaks"],
            pre_route_fixed_seconds=inputs["pre_route_fixed_seconds"],
        )

    def legacy(self, candidate_rows=()):
        with patch.object(app_module, "get_authoritative_priority_snapshot", return_value=self.snapshot()), patch.object(
            app_module, "get_route_travel_time_provider", return_value=ConstantRoadProvider(),
        ), patch.object(app_module, "current_user", return_value=OWNER):
            preview, error = app_module.build_planning_route_preview(
                spreadsheet=self.sheet, owner=OWNER, route_date=DAY, start=GPS, candidate_rows=candidate_rows,
            )
        self.assertIsNone(error, error)
        return preview

    def test_same_day_phone_email_only_exclude_optional_candidates(self):
        baseline = self.inputs()
        for channel in ("phone", "email"):
            for hour in (0, 7, 8, 10, 12, 16, 23):
                for confirmed in ("N", "Y"):
                    with self.subTest(channel=channel, hour=hour, confirmed=confirmed):
                        self.planned.values = self.planned.values[:1]
                        self.activity(contact_type=channel, scheduled_at=at(hour).isoformat(),
                                      appointment_confirmed=confirmed)
                        before = [list(row) for row in self.planned.values]
                        inputs = self.inputs()
                        self.assertEqual([item["customer_id"] for item in inputs["shipments"]], [C])
                        self.assertEqual(inputs["fixed_breaks"], baseline["fixed_breaks"])
                        self.assertEqual(inputs["fixed_activities"], [])
                        self.assertEqual(inputs["pre_route_fixed_seconds"], 0)
                        self.assertEqual(inputs["timeout_seconds"], baseline["timeout_seconds"])
                        request = self.request(inputs)
                        self.assertEqual(request["model"]["vehicles"][0]["breakRule"], {
                            "breakRequests": [{"earliestStartTime": "2026-10-02T10:00:00Z",
                                               "latestStartTime": "2026-10-02T10:00:00Z", "minDuration": "2700s"}],
                        })
                        self.assertEqual(request["model"]["vehicles"][0]["routeDurationLimit"]["maxDuration"], "32400s")
                        legacy = self.legacy()
                        self.assertEqual([item["customer_id"] for item in legacy["stops"]], [C])
                        self.assertEqual(legacy["summary"]["non_route_minutes"], 0)
                        self.assertFalse(any(item.get("contact_type") in {"phone", "email"}
                                             for item in legacy["timeline"]["segments"]))
                        self.assertEqual(self.planned.values, before)

    def test_other_dates_and_inactive_contacts_do_not_exclude_candidates(self):
        for channel in ("phone", "email"):
            cases = [("planned", -1), ("planned", 1)] + [
                (status, 0) for status in ("completed", "cancelled", "skipped", "superseded")
            ]
            for status, offset in cases:
                with self.subTest(channel=channel, status=status, offset=offset):
                    self.planned.values = self.planned.values[:1]
                    self.activity(contact_type=channel, status=status,
                                  scheduled_at=(at(10) + timedelta(days=offset)).isoformat())
                    self.assertEqual({item["customer_id"] for item in self.inputs()["shipments"]}, {A, C})
                    self.assertEqual({item["customer_id"] for item in self.legacy()["stops"]}, {A, C})

    def test_contact_identity_and_time_flags_cannot_bypass_same_day_exclusion(self):
        for channel in ("phone", "email"):
            for changes in ({"time_is_estimated": "Y", "appointment_confirmed": "Y"},
                            {"customer_row": 4, "customer": "Butik C"},
                            {"customer_id": ""}):
                with self.subTest(channel=channel, changes=changes):
                    self.planned.values = self.planned.values[:1]
                    self.activity(contact_type=channel, **changes)
                    self.assertEqual([item["customer_id"] for item in self.inputs()["shipments"]], [C])
                    # An explicit list from an older client cannot override this rule.
                    self.assertEqual([item["customer_id"] for item in self.legacy((2, 4))["stops"]], [C])
        self.planned.values = self.planned.values[:1]
        self.activity(user_name="other", sales_person="Other Seller")
        self.assertEqual({item["customer_id"] for item in self.inputs()["shipments"]}, {A, C})

    def test_legacy_cache_requires_current_planning_snapshot_and_workday_policy(self):
        def cached_payload():
            rows = app_module.planning_rows_for_date(
                app_module.read_planned_activity_snapshot(self.sheet)[2], OWNER, DAY,
            )
            return {"workday_policy": app_module.WORKDAY_POLICY_VERSION,
                    "plan_fingerprint": app_module.planning_state_fingerprint(rows)}

        baseline = cached_payload()
        current = lambda payload: app_module.legacy_route_cache_current(payload, self.sheet, OWNER, DAY)
        self.assertTrue(current(baseline))
        self.assertFalse(current({**baseline, "workday_policy": "old-policy"}))
        self.activity()
        self.assertFalse(current(baseline))
        blocked = cached_payload()
        self.assertTrue(current(blocked))
        columns = app_module.PLANNED_ACTIVITY_COLUMNS
        for field, value in (("status", "completed"), ("status", "cancelled"),
                             ("status", "skipped"), ("status", "superseded"),
                             ("scheduled_at", (at(10) + timedelta(days=1)).isoformat()),
                             ("customer_id", C)):
            with self.subTest(field=field, value=value):
                original = self.planned.values[1][columns.index(field)]
                self.planned.values[1][columns.index(field)] = value
                self.assertFalse(current(blocked))
                self.planned.values[1][columns.index(field)] = original
        self.planned.values.pop()
        self.assertFalse(current(blocked))
        self.assertTrue(current(baseline))

    def test_queue_suppression_for_other_day_contact_is_overridden_only_in_routes(self):
        contact = self.activity(scheduled_at=(at(10) + timedelta(days=1)).isoformat())
        snapshot = self.snapshot()
        snapshot["priorities"][0].update({
            "recommendation_suppression_reason": "future_planned_activity",
            "recommendation_suppression_source_type": "planned_activity",
            "recommendation_suppression_source_id": contact["planned_activity_id"],
        })
        self.assertEqual({item["customer_id"] for item in self.inputs(snapshot)["shipments"]}, {A, C})
        for reason in ("recent_human_contact", "negative_contact_cooldown", "future_delivery", "snoozed", "dismissed", "explicit_follow_up"):
            snapshot["priorities"][0]["recommendation_suppression_reason"] = reason
            self.assertEqual([item["customer_id"] for item in self.inputs(snapshot)["shipments"]], [C])

    def test_existing_visits_survive_phone_email_and_keep_confirmation_semantics(self):
        for confirmed in ("N", "Y"):
            with self.subTest(confirmed=confirmed):
                self.planned.values = self.planned.values[:1]
                self.activity()
                self.activity(planned_activity_id="email-a", contact_type="email")
                self.activity(planned_activity_id="visit-a", contact_type="visit", appointment_confirmed=confirmed)
                inputs = self.inputs()
                visit = next(item for item in inputs["shipments"] if item["customer_id"] == A)
                self.assertIs(visit["required"], True)
                self.assertEqual(visit["fixed_at"], at(10, 30) if confirmed == "Y" else None)
                legacy = next(item for item in self.legacy()["stops"] if item["customer_id"] == A)
                self.assertIs(legacy["required"], True)
                self.assertEqual(legacy["time_is_estimated"], confirmed == "N")
                if confirmed == "N":
                    self.assertNotEqual(app_module.parse_planning_datetime(legacy["estimated_at"]), at(10, 30))
                else:
                    self.assertEqual(app_module.parse_planning_datetime(legacy["estimated_at"]), at(10, 30))

    def test_input_fingerprint_tracks_contact_candidate_changes_deterministically(self):
        baseline = self.inputs()["input_fingerprint"]
        self.activity()
        blocked = self.inputs()["input_fingerprint"]
        self.assertNotEqual(blocked, baseline)
        self.assertEqual(blocked, self.inputs()["input_fingerprint"])
        self.activity(planned_activity_id="second-contact-a", contact_type="email")
        duplicate = self.inputs()["input_fingerprint"]
        self.assertNotEqual(duplicate, blocked)
        self.planned.values[1:] = reversed(self.planned.values[1:])
        self.assertEqual(duplicate, self.inputs()["input_fingerprint"])
        self.planned.values[1:] = reversed(self.planned.values[1:])
        self.planned.values.pop()
        self.assertEqual(blocked, self.inputs()["input_fingerprint"])
        columns = app_module.PLANNED_ACTIVITY_COLUMNS
        for field, value in (("status", "completed"), ("status", "cancelled"),
                             ("scheduled_at", (at(10) + timedelta(days=1)).isoformat()),
                             ("customer_id", C)):
            original = self.planned.values[1][columns.index(field)]
            self.planned.values[1][columns.index(field)] = value
            self.assertNotEqual(self.inputs()["input_fingerprint"], blocked)
            self.planned.values[1][columns.index(field)] = original
        self.planned.values[1][columns.index("scheduled_at")] = at(23).isoformat()
        self.assertEqual(self.inputs()["input_fingerprint"], blocked)
        self.planned.values.pop()
        self.assertEqual(self.inputs()["input_fingerprint"], baseline)

    def test_workday_start_end_and_gps_for_future_and_today(self):
        inputs = self.inputs()
        request = self.request(inputs)
        self.assertEqual(inputs["route_start_at"], at(8))
        self.assertEqual(request["model"]["globalEndTime"], "2026-10-02T15:00:00Z")
        self.assertEqual(request["model"]["vehicles"][0]["startLocation"], {
            "latitude": GPS.latitude, "longitude": GPS.longitude,
        })
        self.assertEqual(request["model"]["vehicles"][0]["endLocation"], request["model"]["vehicles"][0]["startLocation"])
        for current, expected in ((at(7), at(8)), (at(10, 1), at(10, 5)), (at(10, 30), at(10, 30)),
                                  (at(12, 20), at(12, 45)), (at(12, 45), at(12, 45)), (at(13), at(13))):
            with self.subTest(current=current), patch.object(app_module, "stockholm_now", return_value=current):
                inputs = self.inputs()
                self.assertEqual(inputs["route_start_at"], expected)
                request = self.request(inputs)
                self.assertEqual(request["model"]["globalEndTime"], "2026-10-02T15:00:00Z")
                self.assertEqual(request["model"]["vehicles"][0]["routeDurationLimit"]["maxDuration"], f"{available_route_seconds(expected)}s")
                self.assertEqual(bool(inputs["fixed_breaks"]), current < at(12))
                self.assertEqual(app_module.parse_planning_datetime(self.legacy()["route_start_at"]), expected)
        for current in (at(17), at(17, 1), at(23)):
            with patch.object(app_module, "stockholm_now", return_value=current), patch.object(
                app_module, "get_authoritative_priority_snapshot", return_value=self.snapshot(),
            ):
                for builder in (app_module.build_route_optimization_inputs, app_module.build_planning_route_preview):
                    args = dict(spreadsheet=self.sheet, owner=OWNER, route_date=DAY, start=GPS)
                    if builder is app_module.build_planning_route_preview:
                        args["candidate_rows"] = ()
                    result, error = builder(**args)
                    self.assertIsNone(result)
                    self.assertEqual(error[0].get_json()["code"], "route_workday_finished")

    def test_stockholm_workday_handles_winter_offset(self):
        winter = datetime(2026, 11, 2, 8, tzinfo=ZONE)
        request = build_optimize_tours_request(run_id="winter", owner_user_name="olle", route_start=winter,
                                             start=TrustedCoordinate(GPS.latitude, GPS.longitude), shipments=[])
        self.assertEqual(request["model"]["globalStartTime"], "2026-11-02T07:00:00Z")
        self.assertEqual(request["model"]["globalEndTime"], "2026-11-02T16:00:00Z")

    def test_confirmed_booking_moves_lunch_in_both_engines(self):
        self.activity(contact_type="visit", scheduled_at=at(12, 15).isoformat(), appointment_confirmed="Y")
        inputs = self.inputs()
        required = next(item for item in inputs["shipments"] if item["required"])
        self.assertEqual(required["fixed_at"], at(12, 15))
        self.assertEqual(inputs["fixed_breaks"][0]["scheduled_at"], at(11, 15))
        preview = self.legacy()
        booked = next(stop for stop in preview["stops"] if stop["required"])
        self.assertEqual(app_module.parse_planning_datetime(booked["estimated_at"]), at(12, 15))
        self.assertFalse(booked["time_is_estimated"])
        lunch = next(segment for segment in preview["timeline"]["segments"] if segment["kind"] == "lunch")
        self.assertEqual(app_module.parse_planning_datetime(lunch["start"]), at(11, 15))
        self.assertEqual(app_module.parse_planning_datetime(lunch["end"]), at(12))

    def test_booking_ending_at_noon_keeps_normal_lunch_in_both_engines(self):
        self.activity(contact_type="visit", scheduled_at=at(11, 40).isoformat(), appointment_confirmed="Y")
        self.assertEqual(self.inputs()["fixed_breaks"][0]["scheduled_at"], at(12))
        lunch = next(segment for segment in self.legacy()["timeline"]["segments"] if segment["kind"] == "lunch")
        self.assertEqual(app_module.parse_planning_datetime(lunch["start"]), at(12))

    def test_multiple_bookings_move_lunch_deterministically_in_both_engines(self):
        self.activity(contact_type="visit", scheduled_at=at(11, 45).isoformat(), appointment_confirmed="Y")
        self.activity(planned_activity_id="visit-c", contact_type="visit", customer_id=C, customer_row=4,
                      customer="Butik C", scheduled_at=at(12, 45).isoformat(), appointment_confirmed="Y")
        first = self.inputs()
        self.assertEqual(first["fixed_breaks"][0]["scheduled_at"], at(10, 45))
        self.assertEqual({item["fixed_at"] for item in first["shipments"]}, {at(11, 45), at(12, 45)})
        self.planned.values[1:] = reversed(self.planned.values[1:])
        self.assertEqual(first["fixed_breaks"], self.inputs()["fixed_breaks"])
        preview = self.legacy()
        self.assertEqual({app_module.parse_planning_datetime(stop["estimated_at"]) for stop in preview["stops"]},
                         {at(11, 45), at(12, 45)})
        lunch = next(segment for segment in preview["timeline"]["segments"] if segment["kind"] == "lunch")
        self.assertEqual(app_module.parse_planning_datetime(lunch["start"]), at(10, 45))

    def test_booking_during_lunch_does_not_get_skipped_by_today_start_clamp(self):
        self.activity(contact_type="visit", scheduled_at=at(12, 15).isoformat(), appointment_confirmed="Y")
        with patch.object(app_module, "stockholm_now", return_value=at(12, 10)):
            inputs = self.inputs()
            self.assertEqual(inputs["route_start_at"], at(12, 10))
            self.assertEqual(inputs["fixed_breaks"][0]["scheduled_at"], at(12, 50))
            preview = self.legacy()
            self.assertEqual(app_module.parse_planning_datetime(preview["route_start_at"]), at(12, 10))


class LunchSelectionTests(TestCase):
    def test_normal_lunch_is_unchanged_and_flexible_visits_do_not_move_it(self):
        start, pauses = plan_route_lunch(at(8), [(at(11), at(11, 20))])
        self.assertEqual(start, at(8))
        self.assertEqual(pauses, lunch_breaks(at(8)))
        self.assertEqual(pauses[0]["scheduled_at"], at(12))

    def test_nearest_slot_respects_available_day_and_ties_choose_earlier(self):
        booking = [(at(12, 15), at(12, 35))]
        self.assertEqual(plan_route_lunch(at(8), booking)[1][0]["scheduled_at"], at(11, 30))
        self.assertEqual(plan_route_lunch(at(11, 40), booking)[1][0]["scheduled_at"], at(12, 35))
        tied_booking = [(at(12, 12) + timedelta(seconds=30), at(12, 32) + timedelta(seconds=30))]
        self.assertEqual(plan_route_lunch(at(8), tied_booking)[1][0]["scheduled_at"],
                         at(11, 27) + timedelta(seconds=30))

    def test_no_uninterrupted_lunch_slot_has_specific_local_error(self):
        bookings = [(at(8) + timedelta(minutes=index * 36),
                     at(8) + timedelta(minutes=index * 36 + 20)) for index in range(15)]
        with self.assertRaises(RouteLunchNotFeasible):
            plan_route_lunch(at(8), bookings)
        stops = [{"required": True, "appointment_confirmed": True, "contact_type": "visit",
                  "scheduled_at": begin.isoformat(), "latitude": GPS.latitude, "longitude": GPS.longitude}
                 for begin, _end in bookings]
        with app_module.app.app_context(), patch.object(app_module, "get_route_travel_time_provider") as provider:
            _stops, _timeline, error = app_module.schedule_planning_route_with_anchors(
                stops=stops, fixed_non_route=[], route_start_at=at(8), start=GPS,
            )
        self.assertEqual(error[1], 422)
        self.assertEqual(error[0].get_json()["error"], "route_lunch_not_feasible")
        provider.assert_not_called()

    def test_legacy_timeline_keeps_booking_and_moves_lunch_instead_of_visit(self):
        with app_module.app.app_context():
            scheduled, timeline, error = app_module.schedule_planning_route_timeline(
                stops=[{"required": True, "appointment_confirmed": True, "contact_type": "visit",
                        "scheduled_at": at(12, 15).isoformat(), "duration_minutes": 20, "leg_drive_minutes": 10},
                       {"required": False, "duration_minutes": 20, "leg_drive_minutes": 10}],
                fixed_non_route=[], route_start_at=at(11, 40), return_drive_minutes=10,
            )
        self.assertIsNone(error, error)
        self.assertEqual(app_module.parse_planning_datetime(scheduled[0]["estimated_at"]), at(12, 15))
        self.assertFalse(scheduled[0]["time_is_estimated"])
        self.assertTrue(scheduled[1]["time_is_estimated"])
        self.assertEqual(app_module.parse_planning_datetime(scheduled[1]["leg_departure_at"]), at(12, 35))
        lunch = next(segment for segment in timeline["segments"] if segment["kind"] == "lunch")
        self.assertEqual(app_module.parse_planning_datetime(lunch["start"]), at(12, 50))
        self.assertEqual(app_module.parse_planning_datetime(lunch["end"]), at(13, 35))


class LunchValidationTests(TestCase):
    def setUp(self):
        self.items = [{"customer_id": A, "coordinate": TrustedCoordinate(57.7, 11.9),
                       "required": False, "priority_score": 80, "fixed_at": None}]

    def response(self, visit_start, end=None):
        def utc(value):
            return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return {"routes": [{"vehicleLabel": "owner:olle", "vehicleStartTime": utc(at(8)),
                            "vehicleEndTime": utc(end or at(17)),
                            "visits": [{"shipmentIndex": 0, "shipmentLabel": f"customer:{A}",
                                        "isPickup": True, "startTime": utc(visit_start)}],
                            "breaks": [{"startTime": utc(at(12)), "duration": "2700s"}],
                            "transitions": [{}, {}]}], "skippedShipments": []}

    def parse(self, response):
        return parse_optimize_tours_response(response, shipments=self.items, owner_user_name="olle",
                                            route_start=at(8), fixed_breaks=lunch_breaks(at(8)))

    def test_google_response_rejects_visits_overlapping_lunch(self):
        for start in (at(11, 40), at(12, 45)):
            self.assertEqual(self.parse(self.response(start))["performed_count"], 1)
        for start in (at(11, 50), at(12), at(12, 25)):
            with self.subTest(start=start), self.assertRaises(RouteOptimizationError):
                self.parse(self.response(start))

    def test_google_driving_requires_time_outside_lunch(self):
        response = self.response(at(11, 30), at(12, 50))
        response["routes"][0]["transitions"][-1]["travelDuration"] = "1200s"
        with self.assertRaises(RouteOptimizationError):
            self.parse(response)
        response["routes"][0]["transitions"][-1]["travelDuration"] = "900s"
        self.assertEqual(self.parse(response)["performed_count"], 1)

    def test_return_must_finish_by_absolute_1700(self):
        self.assertEqual(self.parse(self.response(at(16, 40), at(17)))["summary"]["route_end_at"], "2026-10-02T15:00+00:00")
        for start, end in ((at(16, 50), at(17)), (at(16), at(17, 1))):
            with self.subTest(start=start, end=end), self.assertRaises(RouteOptimizationError):
                self.parse(self.response(start, end))

    def test_legacy_visits_drives_and_return_avoid_lunch(self):
        for start, drive in ((at(11, 20), 20), (at(11, 30), 20), (at(11, 50), 20), (at(12, 20), 0)):
            with self.subTest(start=start), app_module.app.app_context():
                stops, timeline, error = app_module.schedule_planning_route_timeline(
                    stops=[{"customer_row": 2, "leg_drive_minutes": drive, "duration_minutes": 20}],
                    fixed_non_route=[], route_start_at=start, return_drive_minutes=10,
                )
                self.assertIsNone(error)
                for segment in timeline["segments"]:
                    if segment["kind"] != "lunch":
                        begin = app_module.parse_planning_datetime(segment["start"])
                        end = app_module.parse_planning_datetime(segment["end"])
                        self.assertFalse(begin < at(12, 45) and at(12) < end)
                self.assertLessEqual(app_module.parse_planning_datetime(timeline["route_end_at"]), at(17))
