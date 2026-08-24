from pathlib import Path
import sys
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module
from test_planning import PlanningApiTestCase


class CustomerNextFollowUpTests(PlanningApiTestCase):
    def get_stats(self):
        response = self.client.get("/customers/Butik%20A/stats")
        self.assertEqual(response.status_code, 200, response.get_json())
        return response.get_json()

    def test_legacy_candidate_comes_from_true_latest_contact_datetime(self):
        self.append_contact_row(
            contact_id="same-day-earlier",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-27 08:00",
            follow_up_date="2026-08-01",
        )
        self.append_contact_row(
            contact_id="same-day-latest",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-27 16:00",
            follow_up_date="2026-08-05",
        )

        payload = self.get_stats()

        self.assertEqual(payload["contacts"][0]["follow_up_date"], "2026-08-01")
        self.assertEqual(payload["next_follow_up"], {
            "source": "latest_contact",
            "date": "2026-08-05",
            "time": "",
            "contact_type": "",
            "contact_type_label": "",
            "contact_id": "same-day-latest",
        })

    def test_earliest_future_planned_activity_is_selected_once(self):
        self.append_contact_row(
            contact_id="latest-with-later-follow-up",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-27 09:00",
            follow_up_date="2026-07-31",
        )
        self.append_planning_row(
            planned_activity_id="past-planned",
            scheduled_at="2026-07-27T10:11:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="future-completed",
            scheduled_at="2026-07-28T08:00:00+02:00",
            status="completed",
        )
        self.append_planning_row(
            planned_activity_id="future-later",
            scheduled_at="2026-07-30T09:00:00+02:00",
        )
        self.append_planning_row(
            planned_activity_id="future-earliest",
            scheduled_at="2026-07-29T14:00:00+02:00",
            contact_type="phone",
            customer="Tidigare namn",
        )

        with (
            patch.object(
                app_module,
                "get_contact_rows",
                wraps=app_module.get_contact_rows,
            ) as get_contacts,
            patch.object(
                app_module,
                "read_planned_activity_snapshot",
                wraps=app_module.read_planned_activity_snapshot,
            ) as get_activities,
        ):
            payload = self.get_stats()

        self.assertEqual(get_contacts.call_count, 1)
        self.assertEqual(get_activities.call_count, 1)
        self.assertEqual(payload["next_follow_up"], {
            "source": "planned_activity",
            "date": "2026-07-29",
            "time": "14:00",
            "scheduled_at": "2026-07-29T14:00+02:00",
            "contact_type": "phone",
            "contact_type_label": "Telefon",
            "planned_activity_id": "future-earliest",
        })

    def test_earlier_legacy_date_beats_later_planned_activity(self):
        self.append_contact_row(
            contact_id="legacy-earlier",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-27 09:00",
            follow_up_date="2026-07-28",
        )
        self.append_planning_row(
            planned_activity_id="planned-later",
            scheduled_at="2026-07-29T09:00:00+02:00",
        )

        payload = self.get_stats()

        self.assertEqual(payload["next_follow_up"]["source"], "latest_contact")
        self.assertEqual(payload["next_follow_up"]["date"], "2026-07-28")

    def test_past_legacy_follow_up_is_not_selected(self):
        self.append_contact_row(
            contact_id="past-legacy",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-26 09:00",
            follow_up_date="2026-07-26",
        )

        payload = self.get_stats()

        self.assertIsNone(payload["next_follow_up"])

    def test_planned_activity_wins_same_calendar_day(self):
        self.append_contact_row(
            contact_id="legacy-same-day",
            customer_id="11111111-1111-4111-8111-111111111111",
            date_time="2026-07-27 09:00",
            follow_up_date="2026-07-29",
        )
        self.append_planning_row(
            planned_activity_id="planned-same-day",
            scheduled_at="2026-07-29T16:30:00+02:00",
            contact_type="visit",
        )

        payload = self.get_stats()

        self.assertEqual(payload["next_follow_up"]["source"], "planned_activity")
        self.assertEqual(payload["next_follow_up"]["time"], "16:30")
        self.assertEqual(payload["next_follow_up"]["contact_type_label"], "Besök")
