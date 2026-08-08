from pathlib import Path
import sys
from unittest import TestCase
from unittest.mock import patch


WEB_APP_DIR = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(WEB_APP_DIR))
sys.path.insert(0, str(TESTS_DIR))

import app as app_module  # noqa: E402
from planning_suggestions import SUGGESTIONS_SHEET  # noqa: E402
from test_planning import PlanningApiTestCase  # noqa: E402


class CanonicalOwnershipHardeningTests(PlanningApiTestCase):
    def test_admin_with_current_customers_can_be_owner_without_becoming_seller(self):
        users = self.spreadsheet.worksheet(app_module.USERS_SHEET)
        olle_row = next(
            row for row in users.dict_rows() if row["user_name"] == "olle"
        )
        role_column = users.values[0].index("role") + 1
        admin_column = users.values[0].index("admin") + 1
        name_column = users.values[0].index("name") + 1
        users.update_cell(2, role_column, "Administratör")
        users.update_cell(2, admin_column, "Y")
        users.update_cell(2, name_column, "Olle Rönningberg")
        updated = next(
            row for row in users.dict_rows() if row["user_name"] == "olle"
        )
        customers = app_module.get_customer_rows(self.spreadsheet)

        self.assertFalse(app_module.user_is_seller(updated))
        self.assertTrue(app_module.user_can_be_sales_owner(updated, customers))
        ordinary_admin = next(
            row for row in users.dict_rows() if row["user_name"] == "admin"
        )
        self.assertFalse(
            app_module.user_can_be_sales_owner(ordinary_admin, customers)
        )

        with self.client.session_transaction() as flask_session:
            flask_session["user"] = app_module.public_user(updated)
        explicit = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02&user_name=olle"
        )
        defaulted = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        )
        self.assertEqual(explicit.status_code, 200, explicit.get_json())
        self.assertEqual(defaulted.status_code, 200, defaulted.get_json())
        self.assertEqual(defaulted.get_json()["owner"]["user_name"], "olle")

    def test_contact_owner_uses_canonical_customer_not_historical_actor(self):
        customers = app_module.get_customer_rows(self.spreadsheet)
        lookup = app_module.CustomerLookup(customers)
        contact = {
            "customer_id": customers[0]["customer_id"],
            "customer": "Tidigare namn",
            "sales_person": "Sofia",
        }
        self.assertTrue(app_module.contact_currently_owned_by(
            contact,
            {"user_name": "olle", "name": "Olle"},
            customers,
            customer_lookup=lookup,
        ))
        self.assertFalse(app_module.contact_currently_owned_by(
            contact,
            {"user_name": "sofia", "name": "Sofia"},
            customers,
            customer_lookup=lookup,
        ))

    def test_contact_event_resolves_current_owner_not_actor(self):
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        self.addCleanup(
            app_module.app.config.pop, "PLANNING_SUGGESTIONS_STUB", None
        )
        self.login("sofia")
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        self.assertEqual(suggestion["customer"], "Butik B")

        app_module.resolve_suggestions_for_contact(
            self.spreadsheet,
            owner={"user_name": "olle", "name": "Olle"},
            customer_id=suggestion["customer_id"],
            contact_id="contact-by-another-actor",
            request_id="contact-event-current-owner",
        )
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["user_name"], "sofia")
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_id"], "contact-by-another-actor")

    def test_live_email_event_resolves_current_owner_not_sender(self):
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        self.addCleanup(
            app_module.app.config.pop, "PLANNING_SUGGESTIONS_STUB", None
        )
        self.login("sofia")
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        app_module.resolve_suggestions_for_email(
            self.spreadsheet,
            owner={"user_name": "olle", "name": "Olle"},
            customer_id=suggestion["customer_id"],
            email_id="live-email-current-owner",
        )
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["status"], "resolved")
        self.assertEqual(stored["resolved_by_type"], "email")
        self.assertEqual(stored["resolved_by_id"], "live-email-current-owner")

    def test_transfer_resolves_previous_owner_before_new_owner_materializes(self):
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True
        self.addCleanup(
            app_module.app.config.pop, "PLANNING_SUGGESTIONS_STUB", None
        )
        old = self.client.get("/planning/suggestions").get_json()["suggestion"]
        customers = self.spreadsheet.worksheet("customers_enriched")
        owner_column = customers.values[0].index("sales_person") + 1
        customers.update_cell(2, owner_column, "Sofia")

        self.login("sofia")
        new = self.client.get("/planning/suggestions").get_json()["suggestion"]
        rows = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()
        old_row = next(row for row in rows if row["suggestion_id"] == old["suggestion_id"])
        self.assertEqual(old_row["status"], "resolved")
        self.assertEqual(old_row["resolved_by_type"], "ownership_transfer")
        self.assertEqual(new["customer_id"], old["customer_id"])
        self.assertNotEqual(new["suggestion_id"], old["suggestion_id"])
        self.assertEqual(
            len([row for row in rows if row["status"] in {"pending", "snoozed", "planned"}]),
            1,
        )

    def test_transferred_legacy_followup_is_visible_only_to_current_owner(self):
        canonical = app_module.get_customer_by_row(self.spreadsheet, 2)
        self.append_contact_row(
            customer_id=canonical["customer_id"],
            sales_person="Olle",
            follow_up_date="2026-07-30",
            contact_id="historical-olle-followup",
        )
        customers = self.spreadsheet.worksheet("customers_enriched")
        owner_column = customers.values[0].index("sales_person") + 1
        customers.update_cell(2, owner_column, "Sofia")

        self.login("sofia")
        sofia = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        ).get_json()
        self.login("olle")
        olle = self.client.get(
            "/planning/activities?start=2026-07-27&end=2026-08-02"
        ).get_json()
        self.assertIn(
            "historical-olle-followup",
            {row["source_contact_id"] for row in sofia["unscheduled_followups"]},
        )
        self.assertNotIn(
            "historical-olle-followup",
            {row["source_contact_id"] for row in olle["unscheduled_followups"]},
        )

    def test_renamed_rows_remain_in_stats_and_same_name_other_id_isolated(self):
        canonical = app_module.get_customer_by_row(self.spreadsheet, 2)
        order_sheet = self.spreadsheet.worksheet("order_rows")
        order = {
            "Reference": "RENAMED-ORDER",
            "Order date": "2026-07-01",
            "Delivery date": "2026-07-02",
            "Customer": "Tidigare namn",
            "Customer number": canonical["customer_number"],
            "customer_id": canonical["customer_id"],
            "Quantity": "5",
            "Total": "500",
            "Currency": "SEK",
        }
        order_sheet.append_row([
            order.get(column, "") for column in app_module.ORDER_COLUMNS
        ])
        self.append_contact_row(
            customer="Tidigare namn",
            customer_id=canonical["customer_id"],
            comment="Canonical rename",
        )
        self.append_contact_row(
            customer="Butik A",
            customer_id="22222222-2222-4222-8222-222222222222",
            comment="Same display name, other id",
        )

        response = self.client.get("/customers/Butik%20A/stats")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200, payload)
        self.assertEqual(payload["order_count"], 1)
        comments = {row["comment"] for row in payload["contacts"]}
        self.assertIn("Canonical rename", comments)
        self.assertNotIn("Same display name, other id", comments)


class GlobalScoringHardeningTests(TestCase):
    def test_responsible_filter_is_applied_after_global_scoring(self):
        customers = [
            {"customer_id": "a", "sales_person": "Olle"},
            {"customer_id": "b", "sales_person": "Sofia"},
        ]
        scored = [
            {"customer_id": "a", "sales_person": "Olle", "priority_score": 74},
            {"customer_id": "b", "sales_person": "Sofia", "priority_score": 63},
        ]
        with (
            patch.object(app_module, "build_order_features", return_value={}),
            patch.object(app_module, "build_contact_features", return_value={}),
            patch.object(
                app_module, "build_email_engagement_snapshot", return_value={}
            ),
            patch.object(
                app_module, "build_priority_customers", return_value=scored
            ) as build_scores,
        ):
            result, _email = app_module.build_current_priority_snapshot(
                customers=customers,
                order_rows=[],
                contact_rows=[],
                message_rows=[],
                recipient_rows=[],
                today=app_module.stockholm_today(),
                responsible="Olle",
            )
        self.assertIsNone(build_scores.call_args.args[3])
        self.assertEqual([row["customer_id"] for row in result], ["a"])


class PartialPlanRepairTests(PlanningApiTestCase):
    def setUp(self):
        super().setUp()
        app_module.app.config["PLANNING_SUGGESTIONS_STUB"] = True

    def tearDown(self):
        app_module.app.config.pop("PLANNING_SUGGESTIONS_STUB", None)
        super().tearDown()

    def seed_partial_activity(self, suggestion, **overrides):
        return self.append_planning_row(
            planned_activity_id=overrides.pop("planned_activity_id", "partial-activity"),
            contact_type=overrides.pop("contact_type", "phone"),
            scheduled_at=overrides.pop(
                "scheduled_at", "2026-07-30T09:00:00+02:00"
            ),
            note=overrides.pop("note", "Partial repair"),
            source="system_suggestion",
            source_suggestion_id=suggestion["suggestion_id"],
            recommended_contact_type="phone",
            **overrides,
        )

    def plan(self, suggestion, request_id, **overrides):
        payload = {
            "client_request_id": request_id,
            "expected_suggestion_revision": suggestion["revision"],
            "customer_id": suggestion["customer_id"],
            "contact_type": "phone",
            "scheduled_at": "2026-07-30T09:00:00+02:00",
            "note": "Partial repair",
            **overrides,
        }
        return self.client.post(
            f"/planning/suggestions/{suggestion['suggestion_id']}/plan",
            json=payload,
        )

    def test_existing_matching_activity_repairs_pending_without_append(self):
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        activity = self.seed_partial_activity(suggestion)
        response = self.plan(suggestion, "repair-same-payload")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200, payload)
        self.assertTrue(payload["duplicate"])
        self.assertTrue(payload["repaired"])
        self.assertEqual(len(self.planning_rows()), 1)
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["status"], "planned")
        self.assertEqual(stored["planned_activity_id"], activity["planned_activity_id"])

    def test_existing_different_payload_repairs_link_then_returns_conflict(self):
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        activity = self.seed_partial_activity(suggestion)
        response = self.plan(
            suggestion, "repair-different-payload", note="Different"
        )
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(
            response.get_json()["code"],
            "suggestion_plan_already_materialized",
        )
        self.assertEqual(len(self.planning_rows()), 1)
        stored = self.spreadsheet.worksheet(SUGGESTIONS_SHEET).dict_rows()[0]
        self.assertEqual(stored["planned_activity_id"], activity["planned_activity_id"])

    def test_multiple_active_activities_return_integrity_conflict(self):
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        self.seed_partial_activity(suggestion, planned_activity_id="partial-a")
        self.seed_partial_activity(suggestion, planned_activity_id="partial-b")
        response = self.plan(suggestion, "repair-multiple")
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertEqual(
            response.get_json()["code"],
            "suggestion_activity_integrity_conflict",
        )
        self.assertEqual(len(self.planning_rows()), 2)

    def test_cancelled_activity_does_not_block_new_materialization(self):
        suggestion = self.client.get(
            "/planning/suggestions"
        ).get_json()["suggestion"]
        self.seed_partial_activity(
            suggestion,
            planned_activity_id="cancelled-partial",
            status="cancelled",
        )
        response = self.plan(suggestion, "new-after-cancel")
        self.assertEqual(response.status_code, 201, response.get_json())
        self.assertEqual(len(self.planning_rows()), 2)
        active = [
            row for row in self.planning_rows()
            if row["status"] == "planned"
            and row["source_suggestion_id"] == suggestion["suggestion_id"]
        ]
        self.assertEqual(len(active), 1)


class PerformanceInstrumentationHardeningTests(TestCase):
    def test_suggestions_endpoint_and_granular_steps_are_instrumented(self):
        self.assertIn("/planning/suggestions", app_module.PERFORMANCE_ENDPOINTS)
        source = (WEB_APP_DIR / "app.py").read_text(encoding="utf-8")
        for step in (
            "suggestions.activity_snapshot",
            "suggestions.candidates",
            "suggestions.scoring",
            "suggestions.queue",
        ):
            self.assertIn(step, source)

    def test_frontend_recovers_from_materialization_conflicts(self):
        html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn("suggestion_plan_already_materialized", html)
        self.assertIn("suggestion_activity_integrity_conflict", html)
        self.assertIn("loadPlanningWeek()", html)
        self.assertIn("loadPlanningRecommendation()", html)


if __name__ == "__main__":
    import unittest

    unittest.main()
