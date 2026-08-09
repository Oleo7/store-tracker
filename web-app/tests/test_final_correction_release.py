from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module  # noqa: E402
from contact_channel import recommend_contact_channel  # noqa: E402
from planning_suggestions import SCORE_EVENTS_SHEET, SUGGESTIONS_SHEET  # noqa: E402
from route_proposal import (  # noqa: E402
    Coordinate,
    RouteCandidate,
    TravelTimeResult,
    anchor_aware_preselect_candidates,
)
from test_planning import ConstantRoadProvider, PlanningApiTestCase  # noqa: E402
from test_priority_v4 import (  # noqa: E402
    contact,
    customer,
    email_rows,
    order,
    scored,
)


class ProgressiveQueueTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        sheet = self.spreadsheet.worksheet("customers_enriched")
        headers = sheet.row_values(1)
        for index in range(71):
            row = {
                "customer": f"Previewkund {index:02d}",
                "customer_id": f"preview-{index:02d}",
                "customer_number": f"P-{index:02d}",
                "sales_person": "Olle",
                "customer_segment": "C",
            }
            sheet.append_row([row.get(column, "") for column in headers])

    def tearDown(self):
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        super().tearDown()

    def queue(self, limit=None):
        suffix = "" if limit is None else f"?preview_limit={limit}"
        response = self.client.get(f"/planning/suggestions{suffix}")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def test_progressive_preview_uses_one_sparse_materialized_top(self):
        responses = [self.queue()]
        responses.extend(self.queue(limit) for limit in range(15, 76, 5))

        self.assertEqual(responses[0]["pending_count"], 73)
        self.assertEqual(
            [len(response["queue_preview"]) for response in responses],
            [10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 72],
        )
        complete = responses[-1]
        self.assertEqual(
            len(complete["queue_preview"]), complete["pending_count"] - 1
        )
        self.assertTrue(all(
            item["revision"] == 0 and not item["materialized"]
            for item in complete["queue_preview"]
        ))
        self.assertEqual(
            len(self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()), 1
        )
        created = [
            row for row in self.spreadsheet.worksheet(SCORE_EVENTS_SHEET).dict_rows()
            if row["event_type"] == "suggestion_created"
        ]
        self.assertEqual(len(created), 1)

    def test_preview_limit_is_validated_without_product_hard_cap(self):
        self.assertEqual(self.queue("inte-ett-tal")["preview_limit"], 10)
        negative = self.queue(-5)
        self.assertEqual(negative["preview_limit"], 0)
        self.assertEqual(negative["queue_preview"], [])
        complete = self.queue(999)
        self.assertEqual(complete["preview_limit"], 999)
        self.assertEqual(len(complete["queue_preview"]), 72)


class ChannelAndEmailIntentTests(TestCase):
    def snapshot(self, kind, proposal_type, today):
        messages, recipients = email_rows(
            kind=kind, proposal_type=proposal_type
        )
        return app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=today, customers=[customer()]
        )["id:cid-1"]

    def test_segment_a_positive_dialogue_is_visit_without_score_change(self):
        item = scored(
            today=date(2026, 8, 12),
            contacts=[contact("2026-08-01 10:00:00", result="Positiv")],
            customers=[customer(segment="A")],
        )[0]
        score_before = item["priority_score"]
        channel = recommend_contact_channel(
            lifecycle=item["lifecycle"],
            trigger_key="positive_dialogue_followup",
            segment="A",
            has_human_contact=True,
            phone="0701234567",
            email_available=True,
        )
        self.assertEqual(channel["base_contact_type"], "visit")
        self.assertEqual(channel["recommended_contact_type"], "visit")
        self.assertEqual(item["priority_score"], score_before)

    def test_email_primary_overrides_segment_and_lifecycle_with_fallback(self):
        phone = recommend_contact_channel(
            lifecycle="reactivation", trigger_key="email_open_followup",
            segment="A", phone="0701234567", email_available=True,
        )
        email = recommend_contact_channel(
            lifecycle="reactivation", trigger_key="product_sheet_click_followup",
            segment="A", phone="saknas", email_available=True,
        )
        self.assertEqual(phone["base_contact_type"], "phone")
        self.assertEqual(phone["recommended_contact_type"], "phone")
        self.assertEqual(email["recommended_contact_type"], "email")

    def test_proposal_type_by_engagement_matrix_has_dynamic_copy_and_phone(self):
        proposal_labels = {
            "reminder": "Påminnelse",
            "reactivation": "Återaktivering",
            "new_customer": "Nykund",
        }
        engagement = {
            "open": (date(2026, 8, 12), "email_open_followup", "öppnat mejlförslag"),
            "product": (
                date(2026, 8, 5), "product_sheet_click_followup",
                "produktbladsklick",
            ),
            "stockfiller": (
                date(2026, 8, 5), "stockfiller_click_followup",
                "Stockfiller-klick",
            ),
        }
        for proposal_type, proposal_label in proposal_labels.items():
            for kind, (today, trigger, engagement_label) in engagement.items():
                with self.subTest(proposal_type=proposal_type, kind=kind):
                    feature = self.snapshot(kind, proposal_type, today)
                    item = scored(today=today, email_feature=feature)[0]
                    self.assertEqual(item["primary_trigger_type"], trigger)
                    self.assertEqual(
                        item["primary_reason_text"],
                        f"Följ upp {engagement_label} – {proposal_label}",
                    )
                    channel = recommend_contact_channel(
                        lifecycle=item["lifecycle"], trigger_key=trigger,
                        segment=item["segment"], phone="0701234567",
                        email_available=True,
                    )
                    self.assertEqual(channel["base_contact_type"], "phone")

    def test_open_wait_uses_first_human_open_and_suppresses_generic_trigger(self):
        messages, recipients = email_rows(kind="open", proposal_type="reactivation")
        recipients[0]["last_opened_at"] = "2026-08-09 10:00:00"
        waiting_snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 11),
            customers=[customer(segment="A")],
        )["id:cid-1"]
        waiting = scored(
            today=date(2026, 8, 11), email_feature=waiting_snapshot,
            customers=[customer(segment="A")],
        )[0]
        ready_snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 12),
            customers=[customer(segment="A")],
        )["id:cid-1"]
        ready = scored(
            today=date(2026, 8, 12), email_feature=ready_snapshot,
            customers=[customer(segment="A")],
        )[0]

        self.assertEqual(waiting_snapshot["email_followup_wait_days_remaining"], 1)
        self.assertEqual(
            waiting["recommendation_suppression_reason"],
            "recent_email_engagement_wait",
        )
        self.assertEqual(ready["primary_trigger_type"], "email_open_followup")
        self.assertEqual(
            waiting["active_email_intent_event"], ready["active_email_intent_event"]
        )

    def test_stronger_click_restarts_wait_and_stockfiller_wins(self):
        messages, recipients = email_rows(kind="open")
        recipients[0].update({
            "product_sheet_click_count": "1",
            "product_sheet_first_clicked_at": "2026-08-12 10:00:00",
            "product_sheet_last_clicked_at": "2026-08-12 10:00:00",
        })
        product_wait = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 14),
            customers=[customer()],
        )["id:cid-1"]
        self.assertEqual(product_wait["email_followup_wait_days_remaining"], 1)
        self.assertEqual(product_wait["email_followup_status"], "product_sheet_clicked_no_order")

        recipients[0].update({
            "stockfiller_click_count": "1",
            "stockfiller_first_clicked_at": "2026-08-15 10:00:00",
            "stockfiller_last_clicked_at": "2026-08-15 10:00:00",
        })
        stock = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 18),
            customers=[customer()],
        )["id:cid-1"]
        self.assertEqual(stock["email_followup_status"], "stockfiller_clicked_no_order")
        baseline = scored(today=date(2026, 8, 18))[0]
        self.assertEqual(
            scored(today=date(2026, 8, 18), email_feature=stock)[0]["intent_timing"],
            baseline["intent_timing"] + 8,
        )

    def test_scanner_and_non_live_messages_do_not_create_intent(self):
        scanner = {
            "event_type": "opened", "event_time": "2026-08-02 10:00:00",
            "payload_json": "transac-phishing-consumer",
        }
        self.assertEqual(app_module._recipient_summary({}, [scanner])["open_count"], 0)
        human = {
            "event_type": "opened", "event_time": "2026-08-03 10:00:00",
            "payload_json": '{"user-agent":"Mozilla/5.0"}',
        }
        human_summary = app_module._recipient_summary({}, [scanner, human])
        self.assertEqual(human_summary["open_count"], 1)
        self.assertEqual(human_summary["first_opened_at"], human["event_time"])
        first_only_messages, first_only_recipients = email_rows(kind="open")
        first_only_recipients[0].pop("last_opened_at")
        first_only = app_module.build_email_engagement_snapshot(
            first_only_messages, first_only_recipients, [],
            today=date(2026, 8, 12), customers=[customer()],
        )["id:cid-1"]
        self.assertEqual(first_only["email_followup_status"], "opened_no_click")
        self.assertEqual(first_only["email_followup_wait_days_remaining"], 0)
        messages, recipients = email_rows(kind="open")
        messages[0]["is_test"] = "Y"
        self.assertEqual(
            app_module.build_email_engagement_snapshot(
                messages, recipients, [], today=date(2026, 8, 20),
                customers=[customer()],
            ),
            {},
        )

    def test_farjestaden_reactivation_open_becomes_email_primary_phone(self):
        feature = self.snapshot("open", "reactivation", date(2026, 8, 12))
        item = scored(
            today=date(2026, 8, 12),
            orders=[order("O-1", "2026-02-01"), order("O-2", "2026-03-01")],
            contacts=[contact("2025-12-01 10:00:00")],
            email_feature=feature,
            customers=[customer(name="Ica Kvantum Färjestaden", segment="A")],
        )[0]
        channel = recommend_contact_channel(
            lifecycle=item["lifecycle"], trigger_key=item["primary_trigger_type"],
            segment=item["segment"], phone="0701234567", email_available=True,
        )
        self.assertEqual(item["lifecycle"], "reactivation")
        self.assertEqual(item["primary_trigger_type"], "email_open_followup")
        self.assertEqual(
            item["primary_reason_text"],
            "Följ upp öppnat mejlförslag – Återaktivering",
        )
        self.assertEqual(channel["recommended_contact_type"], "phone")


class MatrixProvider:
    def __init__(self, seconds):
        self.seconds = tuple(tuple(row) for row in seconds)
        self.call_shapes = []

    def get_matrix_seconds(
        self, origins, destinations, *, ephemeral_origin_indexes=frozenset()
    ):
        self.call_shapes.append((len(origins), len(destinations)))
        return TravelTimeResult(
            seconds=self.seconds,
            pair_count=len(origins) * len(destinations),
            request_count=1,
            routing_preference="TRAFFIC_UNAWARE",
        )


class AnchorAwareRouteTests(TestCase):
    def candidate(self, row, score, lat, lon, required=False):
        return RouteCandidate(
            row=row, customer=f"Kund {row}",
            coordinate=Coordinate(lat, lon), priority_score=score,
            required=required,
        )

    def test_preselection_round_robin_keeps_anchor_and_geographic_rankings(self):
        start = Coordinate(0, 0)
        anchor = self.candidate(2, 0, 0, 10, required=True)
        high = self.candidate(3, 100, 40, 40)
        near = self.candidate(4, 30, 0, 10.1)
        corridor = self.candidate(5, 40, 0, 5)
        efficient = self.candidate(6, 80, 0, 1)
        selected = anchor_aware_preselect_candidates(
            start=start,
            candidates=[anchor, high, near, corridor, efficient],
            anchor_rows=[2],
            limit=5,
        )
        self.assertEqual(selected[0].row, 2)
        self.assertEqual({item.row for item in selected}, {2, 3, 4, 5, 6})

    def test_anchor_candidate_pool_is_at_most_35_and_unanchored_uses_old_solver(self):
        customers = []
        priorities = []
        for index in range(40):
            row = index + 2
            customers.append({
                "row": row, "customer_id": f"cid-{row}",
                "customer": f"Kund {row}", "sales_person": "Olle",
                "latitude": 57.0 + index / 100, "longitude": 12.0 + index / 100,
            })
            priorities.append({"row": row, "priority_score": 100 - index})
        snapshot = {"customers": customers, "priorities": priorities}
        owner = {"user_name": "olle", "name": "Olle"}
        with patch.object(
            app_module, "get_authoritative_priority_snapshot", return_value=snapshot
        ):
            anchored, error = app_module.calculate_route_proposal_for_user(
                spreadsheet=object(), start=Coordinate(57, 12),
                client_requested_rows=tuple(range(2, 42)), user=owner, owner=owner,
                route_date=date(2026, 8, 10), required_rows=(2,),
                anchor_rows=(2,), respect_requested_rows=True,
            )
        self.assertIsNone(error)
        self.assertEqual(len(anchored["stops"]), 35)
        self.assertTrue(any(stop["row"] == 2 and stop["required"] for stop in anchored["stops"]))
        self.assertTrue(anchored["meta"]["anchor_aware"])

        one_snapshot = {"customers": customers[:1], "priorities": priorities[:1]}
        with patch.object(
            app_module, "get_authoritative_priority_snapshot", return_value=one_snapshot
        ), patch.object(
            app_module, "anchor_aware_preselect_candidates",
            side_effect=AssertionError("unanchored path must not preselect anchors"),
        ), patch.object(
            app_module, "get_route_travel_time_provider",
            return_value=ConstantRoadProvider(seconds=60),
        ):
            unanchored, error = app_module.calculate_route_proposal_for_user(
                spreadsheet=object(), start=Coordinate(57, 12),
                client_requested_rows=(2,), user=owner, owner=owner,
                route_date=date(2026, 8, 10), respect_requested_rows=True,
            )
        self.assertIsNone(error)
        self.assertFalse(unanchored["meta"]["anchor_aware"])

    def test_anchor_scheduler_avoids_large_backtrack_and_keeps_hard_limits(self):
        start_at = datetime(2026, 8, 10, 9, 0, tzinfo=app_module.STOCKHOLM_ZONE)
        stops = [{
            "row": 2, "customer_row": 2, "customer_id": "anchor",
            "customer": "Fast besök", "latitude": 57.7, "longitude": 12.8,
            "priority_score": 0, "required": True,
            "scheduled_at": "2026-08-10T13:00:00+02:00",
            "duration_minutes": 20, "planned_activity_id": "anchor-1",
        }, {
            "row": 3, "customer_row": 3, "customer_id": "backtrack",
            "customer": "Stor omväg", "latitude": 57.9, "longitude": 11.7,
            "priority_score": 90, "required": False,
            "duration_minutes": 20,
        }]
        for index in range(14):
            row = index + 4
            stops.append({
                "row": row, "customer_row": row, "customer_id": f"rational-{row}",
                "customer": f"Rationellt stopp {row}",
                "latitude": 57.7, "longitude": 12.2 + index / 100,
                "priority_score": 84 - index, "required": False,
                "duration_minutes": 20,
            })

        point_count = len(stops) + 1
        matrix = [[300 for _ in range(point_count)] for _ in range(point_count)]
        for index in range(point_count):
            matrix[index][index] = 0
        anchor_index = 1
        backtrack_index = 2
        matrix[0][anchor_index] = matrix[anchor_index][0] = 3600
        for index in range(3, point_count):
            matrix[0][index] = matrix[index][0] = 900
            matrix[index][anchor_index] = matrix[anchor_index][index] = 600
        for index in range(point_count):
            if index != backtrack_index:
                matrix[index][backtrack_index] = 10800
                matrix[backtrack_index][index] = 10800
        matrix[0][backtrack_index] = 1200

        provider = MatrixProvider(matrix)
        with patch.object(
            app_module, "get_route_travel_time_provider", return_value=provider
        ):
            scheduled, timeline, error = app_module.schedule_planning_route_with_anchors(
                stops=stops, fixed_non_route=[], route_start_at=start_at,
                start=Coordinate(57.7, 11.9),
            )
        self.assertIsNone(error)
        self.assertNotIn("backtrack", {stop["customer_id"] for stop in scheduled})
        anchor_stop = next(stop for stop in scheduled if stop["required"])
        self.assertEqual(anchor_stop["scheduled_at"], "2026-08-10T13:00+02:00")
        self.assertLessEqual(len(scheduled), 15)
        self.assertLess(
            app_module.parse_planning_datetime(timeline["route_end_at"]),
            start_at + timedelta(hours=7),
        )
        self.assertEqual(provider.call_shapes, [(17, 17)])

    def test_optional_stop_is_scheduled_between_two_immutable_anchors(self):
        start_at = datetime(2026, 8, 10, 9, 0, tzinfo=app_module.STOCKHOLM_ZONE)
        stops = [{
            "row": 2, "customer_row": 2, "customer_id": "anchor-one",
            "customer": "Fast besök 1", "latitude": 57.7, "longitude": 12.2,
            "priority_score": 0, "required": True,
            "scheduled_at": "2026-08-10T10:00:00+02:00",
            "duration_minutes": 20, "planned_activity_id": "anchor-1",
        }, {
            "row": 3, "customer_row": 3, "customer_id": "anchor-two",
            "customer": "Fast besök 2", "latitude": 57.7, "longitude": 13.0,
            "priority_score": 0, "required": True,
            "scheduled_at": "2026-08-10T14:00:00+02:00",
            "duration_minutes": 20, "planned_activity_id": "anchor-2",
        }, {
            "row": 4, "customer_row": 4, "customer_id": "between",
            "customer": "Mellanstopp", "latitude": 57.7, "longitude": 12.6,
            "priority_score": 80, "required": False, "duration_minutes": 20,
        }]
        minutes = (
            (0, 30, 120, 60),
            (30, 0, 180, 15),
            (30, 180, 0, 15),
            (60, 60, 15, 0),
        )
        provider = MatrixProvider([
            [value * 60 for value in row] for row in minutes
        ])
        with patch.object(
            app_module, "get_route_travel_time_provider", return_value=provider
        ):
            scheduled, timeline, error = app_module.schedule_planning_route_with_anchors(
                stops=stops, fixed_non_route=[], route_start_at=start_at,
                start=Coordinate(57.7, 11.9),
            )
        self.assertIsNone(error)
        self.assertEqual(
            [stop["customer_id"] for stop in scheduled],
            ["anchor-one", "between", "anchor-two"],
        )
        self.assertEqual(scheduled[0]["scheduled_at"], "2026-08-10T10:00+02:00")
        self.assertEqual(scheduled[2]["scheduled_at"], "2026-08-10T14:00+02:00")
        self.assertLess(
            app_module.parse_planning_datetime(timeline["route_end_at"]),
            start_at + timedelta(hours=7),
        )
