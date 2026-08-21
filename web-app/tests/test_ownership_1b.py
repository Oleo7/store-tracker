from io import BytesIO
from pathlib import Path
import sys
from unittest import TestCase, main
from unittest.mock import patch
from zipfile import ZipFile


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module


class Worksheet:
    def __init__(self, title, values):
        self.title = title
        self.values = [list(row) for row in values]

    def get_all_values(self):
        return [list(row) for row in self.values]


class Spreadsheet:
    def __init__(self, sheets=None):
        self.sheets = sheets or {}

    def worksheet(self, title):
        return self.sheets[title]


def customer(row, name, owner, customer_id):
    return {
        "row": row,
        "customer": name,
        "customer_id": customer_id,
        "customer_number": f"C-{row}",
        "sales_person": owner,
        "cancelled_flag": "",
        "customer_segment": "A",
        "latitude_google": "57.7",
        "longitude_google": "11.9",
    }


class Ownership1BTests(TestCase):
    def setUp(self):
        app_module.app.config.update(
            TESTING=True,
            SECRET_KEY="ownership-1b-test",
        )
        self.client = app_module.app.test_client()
        self.customers = [
            customer(2, "Own Store", "Olle", "own-id"),
            customer(3, "Other Store", "Sofia", "other-id"),
            customer(4, "Unowned Store", "", "unowned-id"),
        ]

    def login(self, *, admin_value="", name="Olle"):
        profile = app_module.public_user({
            "user_name": name.casefold(),
            "name": name,
            "role": "Säljare",
            "admin": admin_value,
        })
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = profile

    def test_customer_lookup_matches_resolver_precedence_and_ambiguity(self):
        canonical = customer(2, "Canonical Store", "Olle", "canonical-id")
        canonical["customer_number"] = " C-100 "
        duplicate_number_a = customer(
            3, "Duplicate Name", "Olle", "duplicate-a"
        )
        duplicate_number_a["customer_number"] = "DUP-1"
        duplicate_number_b = customer(
            4, " duplicate name ", "Sofia", "duplicate-b"
        )
        duplicate_number_b["customer_number"] = " dup-1 "
        customers = [canonical, duplicate_number_a, duplicate_number_b]
        lookup = app_module.CustomerLookup(customers)

        cases = [
            (
                "customer_id wins",
                {
                    "customer_id": " canonical-id ",
                    "customer_number": "DUP-1",
                    "customer_name": "Duplicate Name",
                    "row": 4,
                },
                canonical,
            ),
            ("customer number", {"customer_number": "c-100"}, canonical),
            ("customer name", {"customer_name": " canonical store "}, canonical),
            ("customer row", {"row": "2"}, canonical),
            (
                "missing strong id does not fall back",
                {
                    "customer_id": "missing-id",
                    "customer_number": "C-100",
                    "customer_name": "Canonical Store",
                    "row": 2,
                },
                None,
            ),
        ]
        for label, identifiers, expected in cases:
            with self.subTest(label=label):
                indexed = app_module.resolve_customer(
                    customers,
                    customer_lookup=lookup,
                    **identifiers,
                )
                unshared = app_module.resolve_customer(customers, **identifiers)
                self.assertIs(indexed, expected)
                self.assertIs(unshared, expected)

        for label, identifiers in (
            ("ambiguous number", {"customer_number": "dup-1", "row": 2}),
            ("ambiguous name", {"customer_name": "DUPLICATE NAME", "row": 2}),
        ):
            with self.subTest(label=label):
                with self.assertRaises(app_module.CustomerResolutionError):
                    app_module.resolve_customer(
                        customers,
                        customer_lookup=lookup,
                        **identifiers,
                    )

    def test_only_users_admin_y_is_admin_and_frontend_uses_boolean(self):
        for value in ("Y", " y ", "y"):
            with self.subTest(value=value):
                profile = app_module.public_user({"admin": value})
                self.assertTrue(profile["admin"])
                self.assertTrue(app_module.user_is_admin(profile))
        for value in ("yes", "true", "1", "on", "ja", True, "N", ""):
            with self.subTest(value=value):
                profile = app_module.public_user({"admin": value})
                self.assertFalse(profile["admin"])
                self.assertFalse(app_module.user_is_admin(profile))

        html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        self.assertIn("return currentUser?.admin === true;", html)
        self.assertNotIn('id="chip-responsible"', html)
        self.assertIn('id="insights-responsible"', html)

    def test_customer_list_and_insights_are_owner_filtered_but_admin_gets_all(self):
        headers = list(app_module.CUSTOMER_COLUMNS)
        rows = [
            [item.get(header, "") for header in headers]
            for item in self.customers
        ]
        spreadsheet = Spreadsheet({
            "customers_enriched": Worksheet(
                "customers_enriched", [headers, *rows]
            ),
        })

        with (
            patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=spreadsheet,
            ),
            patch.object(app_module, "get_contact_rows", return_value=[]),
        ):
            self.login()
            seller_response = self.client.get("/customers")
            self.login(admin_value="Y")
            admin_response = self.client.get("/customers")

        self.assertEqual(
            [row["customer"] for row in seller_response.get_json()],
            ["Own Store"],
        )
        self.assertEqual(len(admin_response.get_json()), 3)

        with (
            patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=object(),
            ),
            patch.object(
                app_module, "get_customer_rows", return_value=self.customers
            ),
            patch.object(app_module, "get_contact_rows", return_value=[]),
            patch.object(app_module, "get_order_rows", return_value=[]),
            patch.object(
                app_module, "get_email_rows", return_value=([], [], [])
            ),
            patch.object(
                app_module,
                "build_current_priority_snapshot",
                side_effect=lambda **kwargs: (kwargs["customers"], {}),
            ),
        ):
            self.login()
            seller_insights = self.client.get("/customer-insights")
            self.login(admin_value="Y")
            admin_insights = self.client.get("/customer-insights")

        self.assertEqual(set(seller_insights.get_json()), {"own store"})
        self.assertEqual(
            set(admin_insights.get_json()),
            {"own store", "other store", "unowned store"},
        )

    def test_direct_customer_reads_and_writes_return_404_for_other_owner(self):
        spreadsheet = object()
        with (
            patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=spreadsheet,
            ),
            patch.object(
                app_module, "get_customer_rows", return_value=self.customers
            ),
            patch.object(app_module, "get_order_rows") as get_orders,
            patch.object(
                app_module, "build_email_proposal_draft"
            ) as build_draft,
            patch.object(app_module, "ensure_email_worksheets") as ensure_email,
        ):
            self.login()
            stats = self.client.get("/customers/Other%20Store/stats")
            edit = self.client.patch(
                "/customers/3/contact", json={"phone": "000"}
            )
            draft = self.client.get(
                "/customers/3/email-proposal-draft"
            )
            send = self.client.post(
                "/customers/3/email-proposal/send",
                json={"draft_id": "unauthorized-send"},
            )
            contact = self.client.post(
                "/customers/Other%20Store/contacts",
                json={
                    "customer_id": "own-id",
                    "contact_channel": "Besök",
                    "comment": "Test",
                    "polarbar": "1",
                },
            )

        self.assertEqual(
            [
                stats.status_code,
                edit.status_code,
                draft.status_code,
                send.status_code,
                contact.status_code,
            ],
            [404, 404, 404, 404, 404],
        )
        get_orders.assert_not_called()
        build_draft.assert_not_called()
        ensure_email.assert_not_called()

    def test_old_planning_activity_and_required_route_row_do_not_bypass_owner(self):
        self.login()
        old_activity = {
            "planned_activity_id": "old-activity",
            "user_name": "olle",
            "customer_id": "other-id",
            "customer": "Other Store",
            "customer_row": "3",
            "status": "planned",
            "updated_at": "2026-08-02 10:00:00",
            "revision": "1",
        }
        with (
            patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=object(),
            ),
            patch.object(
                app_module, "get_customer_rows", return_value=self.customers
            ),
            patch.object(
                app_module,
                "find_planned_activity",
                return_value=(object(), [], 2, old_activity),
            ),
        ):
            response = self.client.patch(
                "/planning/activities/old-activity",
                json={
                    "client_request_id": "old-activity-edit",
                    "note": "Försök",
                    "expected_revision": 1,
                },
            )
        self.assertEqual(response.status_code, 404)

        with app_module.app.app_context():
            with (
                patch.object(
                    app_module, "get_customer_rows", return_value=self.customers
                ),
                patch.object(app_module, "get_contact_rows", return_value=[]),
                patch.object(app_module, "get_order_rows", return_value=[]),
                patch.object(
                    app_module, "get_email_rows", return_value=([], [], [])
                ),
                patch.object(
                    app_module,
                    "build_current_priority_snapshot",
                    return_value=(self.customers, {}),
                ),
            ):
                payload, error = app_module.calculate_route_proposal_for_user(
                    spreadsheet=object(),
                    start=app_module.Coordinate(57.7, 11.9),
                    client_requested_rows=(2,),
                    user={
                        "user_name": "olle",
                        "name": "Olle",
                        "role": "Säljare",
                        "admin": False,
                    },
                    route_date=app_module.stockholm_today(),
                    required_rows=(3,),
                    respect_requested_rows=True,
                )
        self.assertIsNone(payload)
        self.assertEqual(error[1], 404)

    def test_contact_log_is_global_for_account_manager_and_admin(self):
        contacts = [
            {
                "customer": "Own Store",
                "customer_id": "own-id",
                "date_time": "2026-08-01 10:00",
                "sales_person": "Olle",
                "contact_channel": "Telefon",
                "result": "Positiv",
                "comment": "Egen",
            },
            {
                "customer": "Other Store",
                "customer_id": "other-id",
                "date_time": "2026-08-01 11:00",
                "sales_person": "Sofia",
                "contact_channel": "Telefon",
                "result": "Neutral",
                "comment": "Historisk",
            },
        ]
        with (
            patch.object(
                app_module,
                "get_spreadsheet_with_retry",
                return_value=object(),
            ),
            patch.object(app_module, "get_contact_rows", return_value=contacts),
        ):
            self.login()
            seller_response = self.client.get("/contact-log")
            seller_filtered_response = self.client.get(
                "/contact-log?responsible=Sofia"
            )
            seller_export = self.client.get("/contact-log/export")
            seller_filtered_export = self.client.get(
                "/contact-log/export?responsible=Sofia"
            )
            self.login(admin_value="Y")
            admin_response = self.client.get("/contact-log")

        seller_payload = seller_response.get_json()
        admin_payload = admin_response.get_json()
        self.assertEqual(seller_response.status_code, 200)
        self.assertEqual(seller_payload["total_count"], 2)
        self.assertEqual(admin_response.get_json()["total_count"], 2)
        self.assertEqual(seller_payload["rows"], admin_payload["rows"])
        self.assertEqual(
            seller_payload["filters"]["responsible"],
            [
                {"value": "Olle", "label": "Olle"},
                {"value": "Sofia", "label": "Sofia"},
            ],
        )
        filtered_payload = seller_filtered_response.get_json()
        self.assertEqual(filtered_payload["filtered_count"], 1)
        self.assertEqual(filtered_payload["rows"][0]["Kund"], "Other Store")

        self.assertEqual(seller_export.status_code, 200)
        self.assertEqual(seller_filtered_export.status_code, 200)
        with ZipFile(BytesIO(seller_export.data)) as workbook:
            worksheet = workbook.read("xl/worksheets/sheet1.xml").decode("utf-8")
        self.assertIn("Own Store", worksheet)
        self.assertIn("Other Store", worksheet)
        with ZipFile(BytesIO(seller_filtered_export.data)) as workbook:
            filtered_worksheet = workbook.read(
                "xl/worksheets/sheet1.xml"
            ).decode("utf-8")
        self.assertNotIn("Own Store", filtered_worksheet)
        self.assertIn("Other Store", filtered_worksheet)

    def test_contact_log_and_export_still_require_authentication(self):
        for path in ("/contact-log", "/contact-log/export"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(
                    response.get_json()["error"], "authentication_required"
                )

    def test_general_contact_and_customer_access_remains_owner_filtered(self):
        contacts = [
            {
                "customer": "Own Store",
                "customer_id": "own-id",
                "date_time": "2026-08-01 10:00",
            },
            {
                "customer": "Other Store",
                "customer_id": "other-id",
                "date_time": "2026-08-01 11:00",
            },
        ]
        seller = app_module.public_user({
            "user_name": "olle",
            "name": "Olle",
            "role": "Säljare",
            "admin": "N",
        })

        visible_contacts = app_module.accessible_contact_rows(
            contacts, self.customers, seller,
        )
        visible_customers = app_module.filter_accessible_customers(
            self.customers, seller,
        )

        self.assertEqual(
            [row["customer"] for row in visible_contacts], ["Own Store"]
        )
        self.assertEqual(
            [row["customer"] for row in visible_customers], ["Own Store"]
        )


if __name__ == "__main__":
    main()
