"""Calendar ownership must use login identity, not display-name spelling."""
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch

WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import app as app_module
from test_planning import NOW, PlanningApiTestCase


class SellerBookingIdentityTests(TestCase):
    def fixture(self, login="Sofia", display="Sofia Andersson", customer_owner=None):
        user = {"user_name": login, "name": display, "active": "Y"}
        customer = {
            "row": 2, "customer_id": "store-1", "customer": "Butik",
            "customer_number": "100", "sales_person": customer_owner or login,
            "customer_segment": "A", "phone": "0701234567",
        }
        activity = {
            "planned_activity_id": "future-1", "customer_id": "store-1",
            "customer": "Butik", "user_name": login, "sales_person": display,
            "contact_type": "phone", "status": "planned", "source": "manual",
            "scheduled_at": (NOW + timedelta(days=1)).isoformat(timespec="minutes"),
        }
        return customer, user, activity

    def score(self, customer, users, activities):
        with patch.object(app_module, "stockholm_now", return_value=NOW):
            priorities, _ = app_module.build_current_priority_snapshot(
                customers=[customer], users=users, order_rows=[], contact_rows=[],
                message_rows=[], recipient_rows=[], today=NOW.date(),
                planned_activity_rows=activities,
            )
        return priorities[0]

    def test_short_full_and_normalized_aliases_share_real_booking(self):
        for login, display in (
            ("Sofia", "Sofia Andersson"), ("Daniel", "Daniel Laine"),
            ("Johan", "Johan Testsson"),
        ):
            for alias in (login, display, "  " + display.upper() + "  "):
                with self.subTest(login=login, alias=alias):
                    customer, user, activity = self.fixture(login, display, alias)
                    priority = self.score(customer, [user], [activity])
                    guidance = priority["customer_guidance"]
                    self.assertEqual(guidance["status_key"], "planned")
                    self.assertEqual(guidance["planned_activity_id"], "future-1")
                    self.assertEqual(guidance["recommended_contact_type"], "phone")
                    self.assertFalse(priority["recommendation_eligible"])
                    active, overdue = app_module.active_planned_activity_queue_state(
                        [activity], user, now=NOW
                    )
                    self.assertEqual(active, {"store-1"})
                    self.assertEqual(overdue, [])

    def test_login_overrides_changed_or_conflicting_display_name(self):
        customer, user, activity = self.fixture()
        changed_name = {**activity, "sales_person": "Old display name"}
        self.assertEqual(
            self.score(customer, [user], [changed_name])["customer_guidance"]["status_key"],
            "planned",
        )
        wrong_login = {**activity, "user_name": "Daniel", "sales_person": "Sofia"}
        self.assertEqual(
            self.score(customer, [user], [wrong_login])["customer_guidance"]["status_key"],
            "act_now",
        )

    def test_inactive_unknown_and_ambiguous_owner_do_not_match(self):
        customer, user, activity = self.fixture()
        cases = [
            ({**customer, "sales_person": "Unknown"}, [user]),
            (customer, [{**user, "active": "N"}]),
            (customer, []),
            ({**customer, "sales_person": "Shared"}, [
                {**user, "name": "Shared"},
                {"user_name": "Other", "name": "Shared", "active": "Y"},
            ]),
        ]
        for target, users in cases:
            with self.subTest(owner=target["sales_person"], users=users):
                result = self.score(target, users, [activity])
                self.assertNotEqual(result["customer_guidance"]["status_key"], "planned")
        missing_login = {**activity, "user_name": ""}
        self.assertNotEqual(
            self.score(customer, [user], [missing_login])["customer_guidance"]["status_key"],
            "planned",
        )

    def test_future_booking_replaces_missed_and_cancellation_restores_it(self):
        customer, user, activity = self.fixture()
        missed = {
            **activity, "planned_activity_id": "missed-1",
            "scheduled_at": (NOW - timedelta(days=1)).isoformat(timespec="minutes"),
        }
        for source in ("manual", "system_suggestion", "follow_up"):
            with self.subTest(source=source):
                future = {**activity, "source": source}
                guidance = self.score(customer, [user], [missed, future])["customer_guidance"]
                self.assertEqual(guidance["status_key"], "planned")
                self.assertEqual(guidance["overdue_activity_id"], "")
                for terminal in ("cancelled", "skipped", "completed"):
                    remaining = [missed, {**future, "status": terminal}]
                    guidance = self.score(customer, [user], remaining)["customer_guidance"]
                    self.assertEqual(guidance["status_key"], "overdue_followup")
                    self.assertEqual(guidance["overdue_activity_id"], "missed-1")
                active, overdue = app_module.active_planned_activity_queue_state(
                    [missed, future], user, now=NOW
                )
                self.assertEqual(active, {"store-1"})
                self.assertEqual(overdue, [])

    def test_customer_transfer_does_not_inherit_previous_sellers_booking(self):
        customer, user, activity = self.fixture()
        daniel = {"user_name": "Daniel", "name": "Daniel Laine", "active": "Y"}
        moved = {**customer, "sales_person": "Daniel"}
        guidance = self.score(moved, [user, daniel], [activity])["customer_guidance"]
        self.assertEqual(guidance["status_key"], "act_now")
        own_activity = {**activity, "user_name": "Daniel", "sales_person": "Daniel Laine"}
        self.assertEqual(
            self.score(moved, [user, daniel], [own_activity])["customer_guidance"]["status_key"],
            "planned",
        )

    def test_different_customer_id_never_matches_by_same_name(self):
        customer, user, activity = self.fixture()
        wrong_customer = {**activity, "customer_id": "other-store"}
        guidance = self.score(customer, [user], [wrong_customer])["customer_guidance"]
        self.assertEqual(guidance["status_key"], "act_now")

    def test_display_only_changes_do_not_change_score_or_context(self):
        customer, user, activity = self.fixture()
        same = {**activity, "sales_person": "Sofia"}
        before = self.score(customer, [user], [same])
        after = self.score(customer, [user], [activity])
        for field in ("priority_score", "intent_timing", "value_index", "history_index", "strategic_index"):
            self.assertEqual(before[field], after[field])
        self.assertEqual(
            app_module.priority_decision_context_hash(before, user["user_name"]),
            app_module.priority_decision_context_hash(after, user["user_name"]),
        )

    def test_owner_sensitive_cache_signature_and_users_dependency(self):
        _, _, activity = self.fixture()
        signature = app_module.priority_planned_activity_signature([activity])
        other = app_module.priority_planned_activity_signature([
            {**activity, "user_name": "Daniel"}
        ])
        self.assertNotEqual(signature, other)
        self.assertIn("users", app_module.PRIORITY_SNAPSHOT_CACHE_TITLES)

    def test_matching_does_not_mutate_inputs(self):
        customer, user, activity = self.fixture()
        inputs = (customer, [user], [activity])
        original = deepcopy(inputs)
        self.score(*inputs)
        self.assertEqual(inputs, original)


class SellerBookingApiTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        users = self.spreadsheet.worksheet(app_module.USERS_SHEET)
        users.update_cell(3, users.values[0].index("name") + 1, "Sofia Andersson")
        self.login("sofia")

    def test_customer_insights_and_queue_agree_for_full_name_booking(self):
        activity = self.append_planning_row(
            owner={"user_name": "sofia", "name": "Sofia Andersson"},
            customer_row=3, source="manual",
        )
        response = self.client.get("/customer-insights")
        self.assertEqual(response.status_code, 200, response.get_json())
        guidance = response.get_json()["butik b"]["customer_guidance"]
        self.assertEqual(guidance["status_key"], "planned")
        self.assertEqual(guidance["planned_activity_id"], activity["planned_activity_id"])
        response = self.client.get("/planning/suggestions")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertIsNone(response.get_json()["suggestion"])

    def test_admin_sees_the_same_booking_and_no_customer_data_are_rewritten(self):
        activity = self.append_planning_row(
            owner={"user_name": "sofia", "name": "Sofia Andersson"},
            customer_row=3, source="manual",
        )
        before = deepcopy(self.spreadsheet.worksheet("customers_enriched").values)
        self.login("admin")
        response = self.client.get("/customer-insights")
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(
            response.get_json()["butik b"]["customer_guidance"]["planned_activity_id"],
            activity["planned_activity_id"],
        )
        self.assertEqual(before, self.spreadsheet.worksheet("customers_enriched").values)

    def test_future_booking_can_be_cancelled_without_leaving_green_status(self):
        activity = self.append_planning_row(
            owner={"user_name": "sofia", "name": "Sofia Andersson"},
            customer_row=3, source="manual",
        )
        response = self.client.get("/customer-insights")
        self.assertEqual(response.get_json()["butik b"]["customer_guidance"]["status_key"], "planned")
        changed = self.client.patch(
            f"/planning/activities/{activity['planned_activity_id']}",
            json={"client_request_id": "cancel-owner-match", "expected_revision": 1, "status": "cancelled"},
        )
        self.assertEqual(changed.status_code, 200, changed.get_json())
        response = self.client.get("/customer-insights")
        self.assertEqual(response.get_json()["butik b"]["customer_guidance"]["status_key"], "act_now")
