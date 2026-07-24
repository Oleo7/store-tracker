import sys
import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
WEB_APP = ROOT / "web-app"
sys.path.insert(0, str(WEB_APP))

import app as app_module  # noqa: E402
from reminder_email import (  # noqa: E402
    EMAIL_EVENTS_COLUMNS,
    EMAIL_MESSAGES_COLUMNS,
    EMAIL_RECIPIENTS_COLUMNS,
    USER_COLUMNS,
    build_email_proposal_copy,
    build_email_proposal_template_defaults,
    build_new_customer_order_rows,
    build_reactivation_order_rows,
    build_settings_product_catalog,
    build_default_copy,
    build_latest_order_context,
    canonicalize_proposal_order_rows,
    classify_customer_relationship,
    brevo_event_time,
    first_name,
    recipient_greeting_name,
    normalize_brevo_event,
    render_reminder_email,
    round_store_count_to_ten,
    split_email_values,
)


class FakeWorksheet:
    def __init__(self, title, headers, rows=None):
        self.title = title
        self.values = [list(headers)] + [
            [row.get(header, "") for header in headers] if isinstance(row, dict) else list(row)
            for row in (rows or [])
        ]

    def get_all_values(self):
        return [list(row) for row in self.values]

    def row_values(self, row):
        return list(self.values[row - 1]) if 0 < row <= len(self.values) else []

    def append_row(self, values, value_input_option=None):
        self.values.append(list(values))

    def append_rows(self, values, value_input_option=None):
        self.values.extend([list(row) for row in values])

    def update_cell(self, row, column, value):
        while len(self.values) < row:
            self.values.append([])
        while len(self.values[row - 1]) < column:
            self.values[row - 1].append("")
        self.values[row - 1][column - 1] = value

    def update(self, range_name, values):
        # Only header expansion is needed by the application helpers.
        if range_name.startswith("A1:"):
            self.values[0] = list(values[0])

    def batch_update(self, data, value_input_option=None):
        for item in data:
            match = __import__("re").match(r"A(\d+):[A-Z]+(\d+)", item["range"])
            if not match:
                continue
            row_number = int(match.group(1))
            while len(self.values) < row_number:
                self.values.append([])
            self.values[row_number - 1] = list(item["values"][0])


class FakeSpreadsheet:
    def __init__(self, sheets):
        self.sheets = {sheet.title: sheet for sheet in sheets}

    def worksheet(self, title):
        return self.sheets[title]


class ReminderEmailHelperTests(unittest.TestCase):
    def test_recipient_greeting_prefers_personal_email_and_rejects_placeholders(self):
        self.assertEqual(
            recipient_greeting_name("rebecca.rydberg@nara.ica.se", ""),
            "Rebecca",
        )
        self.assertEqual(
            recipient_greeting_name("hanna.dahlberg@nara.ica.se", ""),
            "Hanna",
        )
        self.assertEqual(
            recipient_greeting_name("coopsimrishamn.gem@coop.se", ""),
            "",
        )
        self.assertEqual(
            recipient_greeting_name("stenungsund.bc@coopvast.se", ""),
            "",
        )
        self.assertEqual(
            recipient_greeting_name("djupfryst.mora@maxi.ica.se", "Jonathan"),
            "Jonathan",
        )
        self.assertEqual(
            recipient_greeting_name("djupfryst.mora@maxi.ica.se", ""),
            "",
        )
        self.assertEqual(first_name("Kund ICA Nära Ahlgrens Torg"), "")
        self.assertEqual(first_name(". ICA Maxi Nyköping"), "")

    def test_brevo_api_event_aliases_and_utc_time_are_normalized(self):
        self.assertEqual(normalize_brevo_event({"event": "requests"}), "sent")
        self.assertEqual(normalize_brevo_event({"event": "clicks"}), "clicked")
        self.assertEqual(
            brevo_event_time({"date": "2026-07-21T05:00:00Z"}),
            "2026-07-21 07:00:00",
        )

    def test_recipient_values_are_split_validated_and_deduplicated(self):
        result = split_email_values(
            "Buyer@Example.com; info@example.com\ninvalid",
            "buyer@example.com, second@example.se",
        )
        self.assertEqual(
            [item["email"] for item in result],
            ["Buyer@Example.com", "info@example.com", "invalid", "second@example.se"],
        )
        self.assertEqual([item["valid"] for item in result], [True, True, False, True])
        self.assertEqual(first_name("."), "")

    def test_latest_order_uses_delivery_then_order_date_and_sums_products(self):
        rows = [
            {"Customer": "Butiken", "Reference": "A", "Delivery date": "2026-01-10", "Order date": "2026-01-05", "Product": "Hallon", "Quantity": "2", "Unit": "DFP", "placedBy": "Anna Andersson", "buyerEmail": "anna@example.com"},
            {"Customer": "Butiken", "Reference": "B", "Delivery date": "2026-02-10", "Order date": "2026-02-01", "Product": "Mango", "Quantity": "2", "Unit": "DFP", "placedBy": "Bo Butik", "buyerEmail": "bo@example.com"},
            {"Customer": "Butiken", "Reference": "B", "Delivery date": "2026-02-10", "Order date": "2026-02-01", "Product": "Mango", "Quantity": "3", "Unit": "DFP", "placedBy": "Bo Butik", "buyerEmail": "bo@example.com"},
        ]
        context = build_latest_order_context(rows, "butiken")
        self.assertEqual(context["reference"], "B")
        self.assertEqual(context["delivery_date"], "2026-02-10")
        self.assertEqual(context["placed_by"], "Bo Butik")
        self.assertEqual(context["order_rows"], [{"product": "Mango", "quantity": "5", "unit": "DFP"}])

    def test_default_copy_has_generic_fallback(self):
        copy = build_default_copy("Testbutiken", "", False)
        self.assertEqual(copy["subject"], "Polarbär sänker priset! Dags att fylla på?")
        self.assertTrue(copy["intro_text"].startswith("Hej (namn)\n\n"))
        self.assertIn("påfyllnadsförslag utifrån er senaste order", copy["intro_text"])
        self.assertIn("**Svara bara KÖR så lägger jag ordern,**", copy["intro_text"])
        self.assertEqual(
            copy["closing_text"],
            "I produktbladet finns nya priser, marginaler och hela sortimentet.",
        )

    def test_html_and_text_rendering_escape_content_and_include_ctas(self):
        rendered = render_reminder_email(
            greeting_name="Anna",
            subject="Hej <test>",
            intro_text="Hej (namn)\n\nKontroll <script>",
            closing_text="**Fri frakt ingår fortfarande.**",
            order_rows=[{"product": "Mango & Hallon", "quantity": "2", "unit": "DFP"}],
            product_sheet_url="https://drive.google.com/product",
            stockfiller_url="https://order.stockfiller.com/",
            sender={"name": "Olle", "role": "Account Manager", "phone": "070-1"},
        )
        self.assertNotIn("<script>", rendered["html"])
        self.assertIn("Mango &amp; Hallon", rendered["html"])
        self.assertIn("https://drive.google.com/product", rendered["html"])
        self.assertIn("https://order.stockfiller.com/", rendered["html"])
        self.assertIn("Hej Anna,", rendered["html"])
        self.assertNotIn("Hej (namn)", rendered["html"])
        self.assertEqual(rendered["html"].count("Hej Anna,"), 1)
        self.assertIn("Se sortiment och priser", rendered["html"])
        self.assertIn("Beställ i Stockfiller", rendered["html"])
        self.assertLess(
            rendered["html"].index("Beställ i Stockfiller"),
            rendered["html"].index("Se sortiment och priser"),
        )
        self.assertLess(
            rendered["text"].index("Beställ i Stockfiller"),
            rendered["text"].index("Se sortiment och priser"),
        )
        self.assertIn("<strong>Fri frakt ingår fortfarande.</strong>", rendered["html"])
        self.assertNotIn("**Fri frakt", rendered["text"])
        self.assertIn("Olle", rendered["text"])

        without_name = render_reminder_email(
            greeting_name="",
            subject="Test",
            intro_text="Hej (namn)\n\nKontroll",
            closing_text="Klart",
            order_rows=[],
            product_sheet_url="",
            stockfiller_url="",
            sender={"name": "Olle"},
        )
        self.assertIn("Hej,", without_name["text"])
        self.assertNotIn("Hej!", without_name["text"])

    def test_reminder_filter_requires_due_customer_without_recent_contact_or_email(self):
        customer = {
            "customer": "Testbutiken",
            "cancelled_flag": "",
            "email_last_order": "buyer@example.com",
            "email": "info@example.com",
        }
        priority = {
            "order_count": 3,
            "overdue_days": 8,
            "latest_contact_date": "2026-07-01",
            "next_action": {"action_type": "reorder"},
        }
        status = app_module.build_reminder_email_status(
            customer,
            priority,
            {},
            {},
            date(2026, 7, 18),
        )
        self.assertTrue(status["due"])
        self.assertEqual(status["eligible_recipient_count"], 2)

        recent_contact = dict(priority, latest_contact_date="2026-07-15")
        status = app_module.build_reminder_email_status(
            customer,
            recent_contact,
            {},
            {},
            date(2026, 7, 18),
        )
        self.assertFalse(status["due"])
        self.assertIn("recent_sales_contact", status["blockers"])

        status = app_module.build_reminder_email_status(
            customer,
            priority,
            {app_module.normalize_key("Testbutiken"): datetime(2026, 7, 12, 9, 0)},
            {},
            date(2026, 7, 18),
        )
        self.assertFalse(status["due"])
        self.assertIn("recent_reminder_email", status["blockers"])

    def test_relationship_classification_is_exclusive_at_sixty_day_boundary(self):
        rows = [
            {"Customer": "På gränsen", "Delivery date": "2026-05-20", "Quantity": "1"},
            {"Customer": "Gammal", "Delivery date": "2026-05-19", "Quantity": "1"},
        ]
        today = date(2026, 7, 19)
        self.assertEqual(
            classify_customer_relationship(rows, "På gränsen", today=today)["email_type"],
            "reminder",
        )
        self.assertEqual(
            classify_customer_relationship(rows, "Gammal", today=today)["email_type"],
            "reactivation",
        )
        self.assertEqual(
            classify_customer_relationship(rows, "Helt ny", today=today)["email_type"],
            "new_customer",
        )

    def test_settings_catalog_drives_fixed_proposal_mix_and_current_names(self):
        settings = {
            "sku_10001": "Jordgubbar i mörk choklad + vit choklad",
            "sku_10002": "Hallon i mjölkchoklad + vit choklad",
            "sku_10003": "Jordgubbar i mjölkchoklad + vit choklad",
            "sku_10004": "Hallon i mörk choklad + vit choklad",
            "sku_10005": "Blåbär i mörk choklad + vit choklad",
            "sku_10006": "Mango i mjölkchoklad + vit choklad",
        }
        catalog = build_settings_product_catalog(settings)
        expected = [settings[key] for key in ("sku_10003", "sku_10005", "sku_10002", "sku_10006")]
        reactivation = build_reactivation_order_rows(catalog)
        new_customer = build_new_customer_order_rows(catalog)
        self.assertEqual([row["product"] for row in reactivation], expected)
        self.assertEqual([row["quantity"] for row in reactivation], ["4"] * 4)
        self.assertEqual([row["quantity"] for row in new_customer], ["3"] * 4)
        self.assertTrue(all("new_for_customer" not in row for row in reactivation))

        current = canonicalize_proposal_order_rows(
            [{"product": "Jordgubb i mjölkchoklad + vit choklad", "quantity": "2"}],
            catalog,
        )
        self.assertEqual(current[0]["product"], settings["sku_10003"])
        self.assertEqual(current[0]["quantity"], "2")

    def test_proposal_copy_uses_new_sales_copy_and_calculated_intervals(self):
        self.assertEqual(round_store_count_to_ten(344), 340)
        self.assertEqual(round_store_count_to_ten(345), 350)
        self.assertEqual(round_store_count_to_ten(348), 350)

        reminder = build_email_proposal_copy(
            "reminder", "Aktiv butik", latest_delivery_date="2026-07-10",
            has_order_rows=True, today=date(2026, 7, 20),
        )
        self.assertEqual(reminder["subject"], "Polarbär sänker priset! Dags att fylla på?")
        self.assertIn("Det är 10 dagar sedan", reminder["intro_text"])
        self.assertEqual(reminder["intro_text"].count("påfyllnadsförslag"), 1)
        self.assertIn("**Fri frakt** som vanligt!", reminder["intro_text"])

        reactivation = build_email_proposal_copy(
            "reactivation", "Gamla butiken", latest_delivery_date="2026-05-11",
            has_order_rows=True, unique_store_count=348, today=date(2026, 7, 20),
        )
        self.assertEqual(reactivation["subject"], "Lägre priser på Polarbär! Dags att ta in?")
        self.assertIn("Tiden springer! Det var hela 10 veckor sedan", reactivation["intro_text"])
        self.assertIn("**sänkt ordinarie pris** **till 32 kr/KFP!**", reactivation["intro_text"])
        self.assertIn("**83% återköpsgrad**", reactivation["intro_text"])
        self.assertIn("**Svara bara KÖR så hjälper jag med ordern.**", reactivation["intro_text"])
        self.assertEqual(
            reactivation["closing_text"],
            "Klicka bara in på Stockfiller för att beställa själv eller se hela sortimentet, "
            "priser och logistik i produktbladet nedan.",
        )

        new_customer = build_email_proposal_copy(
            "new_customer", "Nya butiken", has_order_rows=True, unique_store_count=345,
            today=date(2026, 7, 20),
        )
        self.assertEqual(
            new_customer["subject"], "Testa Polarbär för 29 kr/KFP – fri frakt"
        )
        self.assertIn("**29 kr/KFP på hela första ordern**", new_customer["intro_text"])
        self.assertIn("**83% återköpsgrad**", new_customer["intro_text"])
        self.assertIn("Över 350 butiker", new_customer["closing_text"])
        self.assertIn("**Svara bara KÖR så lägger jag ordern. Fri frakt,**", new_customer["closing_text"])

        for copy in (reminder, reactivation, new_customer):
            self.assertEqual(copy["stockfiller_label"], "Beställ i Stockfiller")
            self.assertEqual(copy["product_sheet_label"], "Se sortiment och priser")

    def test_template_overrides_keep_dynamic_customer_values(self):
        defaults = build_email_proposal_template_defaults("reactivation")
        self.assertIn("{{veckor}}", defaults["intro_text"])
        custom = {
            **defaults,
            "subject": "Nytt ämne för {{butiksnamn}}",
            "intro_text": "Hej (namn)\n\nDet var {{veckor}} veckor sedan.",
            "closing_text": "Klart för {{butiksnamn}}.",
            "stockfiller_label": "Beställ här",
            "product_sheet_label": "Se allt",
        }
        copy = build_email_proposal_copy(
            "reactivation",
            "Butik X",
            latest_delivery_date="2026-05-11",
            today=date(2026, 7, 20),
            template=custom,
        )
        self.assertEqual(copy["subject"], "Nytt ämne för Butik X")
        self.assertIn("Det var 10 veckor sedan.", copy["intro_text"])
        self.assertEqual(copy["closing_text"], "Klart för Butik X.")
        self.assertEqual(copy["stockfiller_label"], "Beställ här")
        self.assertEqual(copy["product_sheet_label"], "Se allt")


class AuthenticationTests(unittest.TestCase):
    def setUp(self):
        users = FakeWorksheet("users", USER_COLUMNS, [
            {"user_name": "olle", "name": "Olle", "role": "Account Manager", "email": "olle@eatpolarbar.com", "phone": "070", "password": "ExactPass", "active": "Y"},
            {"user_name": "inactive", "name": "Inaktiv", "role": "Säljare", "email": "inactive@polarbar.se", "phone": "", "password": "pass", "active": "N"},
        ])
        self.spreadsheet = FakeSpreadsheet([users])
        app_module.app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.patcher = patch.object(app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet)
        self.patcher.start()
        self.client = app_module.app.test_client()

    def tearDown(self):
        self.patcher.stop()

    def test_login_is_case_insensitive_for_username_and_exact_for_password(self):
        response = self.client.post("/login", json={"user_name": "OLLE", "password": "ExactPass"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["user"]["name"], "Olle")
        self.assertIn("HttpOnly", response.headers.get("Set-Cookie", ""))
        session_response = self.client.get("/session")
        self.assertEqual(session_response.status_code, 200)
        self.assertTrue(session_response.get_json()["authenticated"])

        self.client.post("/logout")
        self.assertEqual(self.client.get("/session").status_code, 401)

    def test_wrong_password_and_inactive_user_are_rejected(self):
        wrong = self.client.post("/login", json={"user_name": "olle", "password": "exactpass"})
        inactive = self.client.post("/login", json={"user_name": "inactive", "password": "pass"})
        self.assertEqual(wrong.status_code, 401)
        self.assertEqual(inactive.status_code, 401)


class TimelineAndWebhookTests(unittest.TestCase):
    def _sheets(self):
        messages = FakeWorksheet("email_messages", EMAIL_MESSAGES_COLUMNS, [{
            "email_id": "mail-1", "customer": "Butiken", "sender_name": "Olle",
            "subject": "Påfyllning", "is_test": "N", "status": "sent",
            "sent_at": "2026-07-01 09:00:00", "product_sheet_url": "https://drive.google.com/p",
            "stockfiller_url": "https://order.stockfiller.com/",
        }])
        recipients = FakeWorksheet("email_recipients", EMAIL_RECIPIENTS_COLUMNS, [
            {
                "email_id": "mail-1", "customer": "Butiken", "intended_email": "buyer@example.com",
                "actual_email": "buyer@example.com", "brevo_message_id": "msg-1", "send_status": "sent",
                "sent_at": "2026-07-01 09:00:00", "open_count": "3", "last_opened_at": "2026-07-02 10:00:00",
                "product_sheet_click_count": "1", "product_sheet_last_clicked_at": "2026-07-03 11:00:00",
            },
            {
                "email_id": "mail-1", "customer": "Butiken", "intended_email": "owner@example.com",
                "actual_email": "owner@example.com", "brevo_message_id": "msg-2", "send_status": "sent",
                "sent_at": "2026-07-01 09:00:00", "open_count": "5", "last_opened_at": "2026-07-02 12:00:00",
                "product_sheet_click_count": "4", "product_sheet_last_clicked_at": "2026-07-03 13:00:00",
            },
        ])
        events = FakeWorksheet("email_events", EMAIL_EVENTS_COLUMNS, [])
        return {
            app_module.EMAIL_MESSAGES_SHEET: messages,
            app_module.EMAIL_RECIPIENTS_SHEET: recipients,
            app_module.EMAIL_EVENTS_SHEET: events,
        }

    def test_timeline_aggregates_events_and_attributes_day_ten_only_once(self):
        sheets = self._sheets()
        orders = [
            {"Customer": "Butiken", "Reference": "ORDER-0", "Order date": "2026-07-02", "Total": "75", "Currency": "SEK", "Unit": "DFP", "Quantity": "1"},
            {"Customer": "Butiken", "Reference": "ORDER-1", "Order date": "2026-07-11", "Total": "100", "Currency": "SEK", "Unit": "DFP", "Quantity": "2"},
            {"Customer": "Butiken", "Reference": "ORDER-1", "Order date": "2026-07-11", "Total": "50", "Currency": "SEK", "Unit": "DFP", "Quantity": "1"},
            {"Customer": "Butiken", "Reference": "ORDER-2", "Order date": "2026-07-12", "Total": "80", "Currency": "SEK", "Unit": "DFP", "Quantity": "1"},
        ]
        timeline = app_module.build_customer_timeline("Butiken", orders, [], sheets)
        types = [item["event_type"] for item in timeline]
        self.assertIn("email_proposal_sent", types)
        self.assertIn("email_proposal_opened", types)
        self.assertIn("product_sheet_clicked", types)
        self.assertEqual(types.count("subsequent_order"), 2)
        self.assertEqual(types.count("email_proposal_opened"), 1)
        self.assertEqual(types.count("product_sheet_clicked"), 1)
        self.assertNotIn("email_delivered", types)
        opened = next(item for item in timeline if item["event_type"] == "email_proposal_opened")
        self.assertEqual(opened["title"], "Öppnat")
        self.assertEqual(opened["importance"], "secondary")
        self.assertNotIn("gånger", opened["title"])
        self.assertFalse(any(detail.get("label") == "Antal" for detail in opened.get("details", [])))

        attributed = [item for item in timeline if item["event_type"] == "subsequent_order"]
        references = {
            next(detail["value"] for detail in item["details"] if detail["label"] == "Orderreferens")
            for item in attributed
        }
        self.assertEqual(references, {"ORDER-0", "ORDER-1"})
        day_ten = next(
            item for item in attributed
            if any(detail.get("value") == "ORDER-1" for detail in item["details"])
        )
        self.assertIn({"label": "Ordervärde", "value": "150,00 SEK"}, day_ten["details"])
        self.assertIn({"label": "Antal DFP", "value": "3"}, day_ten["details"])

    def test_webhook_is_deduplicated_and_updates_open_summary(self):
        sheets = self._sheets()
        spreadsheet = FakeSpreadsheet(list(sheets.values()))
        payload = {"event": "opened", "message-id": "<msg-1>", "email": "buyer@example.com", "date": "2026-07-04 12:00:00"}
        self.assertTrue(app_module.process_brevo_event(spreadsheet, sheets, payload))
        self.assertFalse(app_module.process_brevo_event(spreadsheet, sheets, payload))
        recipient = app_module.worksheet_to_dicts(sheets[app_module.EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS)[0]
        self.assertEqual(recipient["open_count"], 1)
        self.assertEqual(len(sheets[app_module.EMAIL_EVENTS_SHEET].values), 2)

    def test_duplicate_event_repairs_summary_after_partial_sheet_failure(self):
        sheets = self._sheets()
        spreadsheet = FakeSpreadsheet(list(sheets.values()))
        payload = {
            "event": "delivered", "message-id": "msg-1",
            "email": "buyer@example.com", "date": "2026-07-04T10:00:00Z",
        }
        self.assertTrue(app_module.process_brevo_event(spreadsheet, sheets, payload))
        recipient_headers = sheets[app_module.EMAIL_RECIPIENTS_SHEET].values[0]
        delivered_index = recipient_headers.index("delivered_at")
        sheets[app_module.EMAIL_RECIPIENTS_SHEET].values[1][delivered_index] = ""

        self.assertFalse(app_module.process_brevo_event(spreadsheet, sheets, payload))
        recipient = app_module.worksheet_to_dicts(
            sheets[app_module.EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
        )[0]
        self.assertEqual(recipient["delivered_at"], "2026-07-04 12:00:00")

    def test_brevo_security_scanner_open_is_kept_raw_but_not_counted(self):
        sheets = self._sheets()
        spreadsheet = FakeSpreadsheet(list(sheets.values()))
        scanner = {
            "event": "opened", "message-id": "msg-1", "email": "buyer@example.com",
            "date": "2026-07-04T10:00:00Z",
            "user_agent": "Brevo/1.0 (transac-phishing-consumer 2.33.4)",
        }
        human = {
            "event": "opened", "message-id": "msg-1", "email": "buyer@example.com",
            "date": "2026-07-04T10:01:00Z", "user_agent": "Mozilla/5.0",
        }
        self.assertEqual(app_module.process_brevo_events(spreadsheet, sheets, [scanner, human]), 2)
        recipient = app_module.worksheet_to_dicts(
            sheets[app_module.EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
        )[0]
        self.assertEqual(recipient["open_count"], 1)
        self.assertEqual(len(sheets[app_module.EMAIL_EVENTS_SHEET].values), 3)

    def test_reconciliation_backfills_delivery_open_and_product_click(self):
        sheets = self._sheets()
        spreadsheet = FakeSpreadsheet(list(sheets.values()))
        events = [
            {"event": "delivered", "messageId": "msg-1", "email": "buyer@example.com", "date": "2026-07-02T08:00:00Z"},
            {"event": "opened", "messageId": "msg-1", "email": "buyer@example.com", "date": "2026-07-02T09:00:00Z"},
            {"event": "clicks", "messageId": "msg-1", "email": "buyer@example.com", "date": "2026-07-02T10:00:00Z", "link": "https://drive.google.com/p"},
        ]
        app_module._brevo_reconcile_lock = __import__("threading").Lock()
        with patch.object(app_module, "get_spreadsheet_with_retry", return_value=spreadsheet), \
             patch.object(app_module, "ensure_email_worksheets", return_value=sheets), \
             patch.object(app_module, "fetch_brevo_events", return_value=events):
            result = app_module.reconcile_recent_brevo_events(days=30)

        self.assertEqual(result["inserted_events"], 3)
        recipient = app_module.worksheet_to_dicts(
            sheets[app_module.EMAIL_RECIPIENTS_SHEET], expected_columns=EMAIL_RECIPIENTS_COLUMNS
        )[0]
        self.assertEqual(recipient["delivered_at"], "2026-07-02 10:00:00")
        self.assertEqual(recipient["open_count"], 1)
        self.assertEqual(recipient["product_sheet_click_count"], 1)

    def test_order_is_attributed_to_latest_prior_reminder(self):
        sheets = self._sheets()
        app_module.append_dict_row(sheets[app_module.EMAIL_MESSAGES_SHEET], EMAIL_MESSAGES_COLUMNS, {
            "email_id": "mail-2", "customer": "Butiken", "sender_name": "Sofia",
            "subject": "Ny påminnelse", "is_test": "N", "status": "sent",
            "sent_at": "2026-07-05 09:00:00",
        })
        app_module.append_dict_row(sheets[app_module.EMAIL_RECIPIENTS_SHEET], EMAIL_RECIPIENTS_COLUMNS, {
            "email_id": "mail-2", "customer": "Butiken", "intended_email": "buyer@example.com",
            "actual_email": "buyer@example.com", "brevo_message_id": "msg-latest",
            "send_status": "sent", "sent_at": "2026-07-05 09:00:00",
        })
        orders = [{
            "Customer": "Butiken", "Reference": "ORDER-LATEST", "Order date": "2026-07-06",
            "Total": "200", "Currency": "SEK", "Unit": "DFP", "Quantity": "3",
        }]
        timeline = app_module.build_customer_timeline("Butiken", orders, [], sheets)
        order = next(item for item in timeline if item["event_type"] == "subsequent_order")
        self.assertEqual(order["email_id"], "mail-2")


class EmailPerformanceTests(unittest.TestCase):
    @staticmethod
    def _message(email_id, customer, sent_at, **overrides):
        return {
            "email_id": email_id,
            "customer": customer,
            "email_type": "reminder",
            "sender_name": "Olle",
            "subject": "Mejlförslag",
            "is_test": "N",
            "status": "sent",
            "sent_at": sent_at,
            **overrides,
        }

    @staticmethod
    def _recipient(email_id, customer, email, **overrides):
        return {
            "email_id": email_id,
            "customer": customer,
            "intended_email": email,
            "actual_email": email,
            "send_status": "sent",
            "sent_at": "2026-07-01 09:00:00",
            **overrides,
        }

    def test_report_counts_unique_stores_and_attributes_only_day_ten(self):
        messages = [
            self._message("a-1", "Butik A", "2026-07-01 09:00:00"),
            self._message("a-2", "Butik A", "2026-07-05 09:00:00"),
            self._message("b-1", "Butik B", "2026-07-01 09:00:00"),
            self._message("c-1", "Butik C", "2026-07-01 09:00:00"),
            self._message("test-1", "Butik D", "2026-07-01 09:00:00", is_test="Y"),
            self._message("failed-1", "Butik E", "2026-07-01 09:00:00", status="failed"),
        ]
        recipients = [
            self._recipient(
                "a-1", "Butik A", "buyer-a@example.com",
                delivered_at="2026-07-01 09:01:00",
                open_count="5", last_opened_at="2026-07-01 10:00:00",
                product_sheet_click_count="7",
                product_sheet_first_clicked_at="2026-07-02 10:00:00",
                product_sheet_last_clicked_at="2026-07-03 10:00:00",
            ),
            self._recipient(
                "a-1", "Butik A", "owner-a@example.com",
                delivered_at="2026-07-01 09:01:00",
                product_sheet_click_count="3",
                product_sheet_first_clicked_at="2026-07-02 11:00:00",
                product_sheet_last_clicked_at="2026-07-03 11:00:00",
            ),
            self._recipient(
                "a-2", "Butik A", "buyer-a@example.com",
                stockfiller_click_count="4",
                stockfiller_first_clicked_at="2026-07-06 10:00:00",
                stockfiller_last_clicked_at="2026-07-07 10:00:00",
            ),
            self._recipient(
                "b-1", "Butik B", "buyer-b@example.com",
                product_sheet_click_count="9",
                product_sheet_first_clicked_at="2026-07-02 10:00:00",
                product_sheet_last_clicked_at="2026-07-03 10:00:00",
            ),
            self._recipient(
                "c-1", "Butik C", "buyer-c@example.com",
                delivered_at="2026-07-01 09:01:00",
            ),
            self._recipient(
                "test-1", "Butik D", "buyer-d@example.com",
                delivered_at="2026-07-01 09:01:00",
                stockfiller_click_count="20",
                stockfiller_first_clicked_at="2026-07-02 10:00:00",
                stockfiller_last_clicked_at="2026-07-02 10:00:00",
            ),
        ]
        orders = [
            {
                "Customer": "Butik A", "Reference": "ORDER-A", "Order date": "2026-07-15",
                "Total": "100", "Currency": "SEK", "Unit": "DFP", "Quantity": "2",
            },
            {
                "Customer": "Butik A", "Reference": "ORDER-A", "Order date": "2026-07-15",
                "Total": "50", "Currency": "SEK", "Unit": "DFP", "Quantity": "1",
            },
            {
                "Customer": "Butik B", "Reference": "ORDER-B", "Order date": "2026-07-12",
                "Total": "80", "Currency": "SEK", "Unit": "DFP", "Quantity": "1",
            },
        ]

        report = app_module.build_email_performance(
            messages, recipients, orders, today=date(2026, 7, 20)
        )
        self.assertEqual(report["unique_store_count"], 3)
        self.assertEqual(report["sent_email_count"], 5)
        self.assertEqual(report["delivered_email_count"], 5)
        self.assertEqual(report["product_clicked_store_count"], 2)
        self.assertEqual(report["stockfiller_clicked_store_count"], 1)
        self.assertEqual(report["product_clicked_recipient_count"], 3)
        self.assertEqual(report["stockfiller_clicked_recipient_count"], 1)
        self.assertEqual(report["ordered_store_count"], 1)
        self.assertEqual(report["attributed_order_count"], 1)
        self.assertEqual(report["attributed_order_value_by_currency"], {"SEK": 150.0})
        self.assertEqual(report["attributed_dfp"], 3.0)
        self.assertEqual(report["product_clicked_store_rate"], 66.7)
        self.assertEqual(report["stockfiller_clicked_store_rate"], 33.3)
        self.assertEqual(report["ordered_store_rate"], 33.3)

        selected = app_module.build_email_performance(
            messages,
            recipients,
            orders,
            included_customer_keys={"butik b"},
            today=date(2026, 7, 20),
        )
        self.assertEqual(selected["unique_store_count"], 1)
        self.assertEqual(selected["product_clicked_store_count"], 1)
        self.assertEqual(selected["ordered_store_count"], 0)

    def test_followup_priority_and_three_day_wait(self):
        messages = [
            self._message("stock", "Stock", "2026-07-01 09:00:00"),
            self._message("product", "Product", "2026-07-01 09:00:00"),
            self._message("open", "Open", "2026-07-01 09:00:00"),
            self._message("delivery", "Delivery", "2026-07-01 09:00:00"),
        ]
        recipients = [
            self._recipient(
                "stock", "Stock", "stock@example.com",
                stockfiller_click_count="2",
                stockfiller_first_clicked_at="2026-07-02 10:00:00",
                stockfiller_last_clicked_at="2026-07-03 10:00:00",
            ),
            self._recipient(
                "product", "Product", "product@example.com",
                product_sheet_click_count="6",
                product_sheet_first_clicked_at="2026-07-02 10:00:00",
                product_sheet_last_clicked_at="2026-07-03 10:00:00",
            ),
            self._recipient(
                "open", "Open", "open@example.com",
                delivered_at="2026-07-01 09:01:00",
                open_count="4", last_opened_at="2026-07-02 10:00:00",
            ),
            self._recipient(
                "delivery", "Delivery", "delivery@example.com",
                delivered_at="2026-07-01 09:01:00",
            ),
        ]

        day_two = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 7, 4)
        )
        self.assertFalse(day_two["stock"]["email_click_without_order"])
        self.assertEqual(day_two["stock"]["email_followup_wait_days_remaining"], 1)

        day_three = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 7, 5)
        )
        self.assertEqual(day_three["stock"]["email_followup_status"], "stockfiller_clicked_no_order")
        self.assertEqual(day_three["stock"]["email_followup_priority"], 1)
        self.assertTrue(day_three["stock"]["email_click_without_order"])
        self.assertEqual(day_three["product"]["email_followup_status"], "product_sheet_clicked_no_order")
        self.assertEqual(day_three["product"]["email_followup_priority"], 2)
        self.assertTrue(day_three["product"]["email_click_without_order"])
        self.assertEqual(day_three["open"]["email_followup_status"], "opened_no_click")
        self.assertEqual(day_three["open"]["email_followup_priority"], 3)
        self.assertFalse(day_three["open"]["email_click_without_order"])
        self.assertEqual(day_three["delivery"]["email_followup_status"], "delivered_no_activity")
        self.assertEqual(day_three["delivery"]["email_followup_priority"], 4)

        order_day_ten = [{
            "Customer": "Stock", "Reference": "ORDER-10", "Order date": "2026-07-11",
            "Total": "100", "Currency": "SEK", "Unit": "DFP", "Quantity": "2",
        }]
        converted = app_module.build_email_engagement_snapshot(
            messages, recipients, order_day_ten, today=date(2026, 7, 20)
        )
        self.assertEqual(converted["stock"]["email_followup_status"], "ordered_within_10_days")
        self.assertFalse(converted["stock"]["email_click_without_order"])

        order_day_eleven = [{
            "Customer": "Stock", "Reference": "ORDER-11", "Order date": "2026-07-12",
            "Total": "100", "Currency": "SEK", "Unit": "DFP", "Quantity": "2",
        }]
        not_attributed = app_module.build_email_engagement_snapshot(
            messages, recipients, order_day_eleven, today=date(2026, 7, 20)
        )
        self.assertEqual(not_attributed["stock"]["email_followup_status"], "stockfiller_clicked_no_order")
        self.assertTrue(not_attributed["stock"]["email_click_without_order"])

    def test_wait_period_uses_the_prioritized_link_type(self):
        messages = [self._message("both", "Båda", "2026-07-01 09:00:00")]
        recipients = [self._recipient(
            "both", "Båda", "both@example.com",
            product_sheet_click_count="1",
            product_sheet_first_clicked_at="2026-07-01 10:00:00",
            product_sheet_last_clicked_at="2026-07-02 10:00:00",
            stockfiller_click_count="1",
            stockfiller_first_clicked_at="2026-07-04 10:00:00",
            stockfiller_last_clicked_at="2026-07-04 10:00:00",
        )]

        waiting = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 7, 5)
        )["båda"]
        self.assertEqual(waiting["email_followup_status"], "stockfiller_clicked_no_order")
        self.assertEqual(waiting["email_followup_wait_days_remaining"], 2)
        self.assertFalse(waiting["email_click_without_order"])

        ready = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 7, 7)
        )["båda"]
        self.assertEqual(ready["email_followup_wait_days_remaining"], 0)
        self.assertTrue(ready["email_click_without_order"])

    def test_invalid_order_rows_are_ignored_and_reused_references_stay_separate(self):
        rows = [
            {
                "Customer": "Butik A", "Reference": "ZERO", "Order date": "2026-07-02",
                "Total": "0", "Currency": "SEK", "Unit": "DFP", "Quantity": "0",
            },
            {
                "Customer": "Butik A", "Reference": "RETURN", "Order date": "2026-07-03",
                "Total": "-100", "Currency": "SEK", "Unit": "DFP", "Quantity": "-2",
            },
            {
                "Customer": "Butik A", "Reference": "REUSED", "Order date": "2026-07-04",
                "Total": "100", "Currency": "SEK", "Unit": "DFP", "Quantity": "2",
            },
            {
                "Customer": "Butik A", "Reference": "REUSED", "Order date": "2026-07-05",
                "Total": "150", "Currency": "SEK", "Unit": "DFP", "Quantity": "3",
            },
        ]

        grouped = app_module.group_customer_orders(rows)
        self.assertEqual(len(grouped), 2)
        self.assertEqual([order["date"] for order in grouped], [date(2026, 7, 4), date(2026, 7, 5)])
        self.assertEqual(sum(order["total"] for order in grouped), 250)
        self.assertEqual(sum(order["dfp"] for order in grouped), 5)

    def test_frontend_contains_filter_report_and_compact_timeline_rules(self):
        html = (WEB_APP / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="chip-email-click-no-order"', html)
        self.assertIn("email_click_without_order", html)
        self.assertIn('id="email-performance-report"', html)
        self.assertIn("renderEmailPerformance", html)
        self.assertIn('item.importance !== "secondary"', html)
        self.assertIn('id="settings-btn"', html)
        self.assertIn('id="view-settings"', html)
        self.assertIn("saveEmailTemplateSettings", html)
        self.assertIn("/email-proposal-settings/", html)
        self.assertIn("#email-order-list .email-order-row", html)


class EmailInsightsEndpointTests(unittest.TestCase):
    def setUp(self):
        customer_headers = [
            "customer", "cancelled_flag", "sales_person", "customer_segment",
            "customer_number", "name", "phone", "email", "email_last_order",
            "city_google", "region_google",
        ]
        customers = FakeWorksheet("customers_enriched", customer_headers, [
            {
                "customer": "Butik A", "sales_person": "Sofia", "customer_segment": "A",
                "customer_number": "A-1", "email": "a@example.com",
            },
            {
                "customer": "Butik B", "sales_person": "Olle", "customer_segment": "B",
                "customer_number": "B-1", "email": "b@example.com",
            },
        ])
        orders = FakeWorksheet("order_rows", app_module.ORDER_COLUMNS, [])
        contacts = FakeWorksheet("sales_activities", app_module.CONTACT_COLUMNS, [])
        messages = FakeWorksheet("email_messages", EMAIL_MESSAGES_COLUMNS, [
            {
                "email_id": "a-mail", "customer": "Butik A", "email_type": "new_customer",
                "sender_name": "Sofia", "subject": "A", "is_test": "N", "status": "sent",
                "sent_at": "2026-07-17 09:00:00",
            },
            {
                "email_id": "b-mail", "customer": "Butik B", "email_type": "new_customer",
                "sender_name": "Olle", "subject": "B", "is_test": "N", "status": "sent",
                "sent_at": "2026-07-17 09:00:00",
            },
            {
                "email_id": "historical-mail", "customer": "Historisk butik",
                "email_type": "new_customer", "sender_name": "Olle", "subject": "Historisk",
                "is_test": "N", "status": "sent", "sent_at": "2026-07-17 09:00:00",
            },
        ])
        recipients = FakeWorksheet("email_recipients", EMAIL_RECIPIENTS_COLUMNS, [
            {
                "email_id": "a-mail", "customer": "Butik A", "intended_email": "a@example.com",
                "actual_email": "a@example.com", "send_status": "sent",
                "product_sheet_click_count": "2",
                "product_sheet_first_clicked_at": "2026-07-18 10:00:00",
                "product_sheet_last_clicked_at": "2026-07-19 10:00:00",
            },
            {
                "email_id": "b-mail", "customer": "Butik B", "intended_email": "b@example.com",
                "actual_email": "b@example.com", "send_status": "sent",
                "stockfiller_click_count": "3",
                "stockfiller_first_clicked_at": "2026-07-18 10:00:00",
                "stockfiller_last_clicked_at": "2026-07-19 10:00:00",
            },
            {
                "email_id": "historical-mail", "customer": "Historisk butik",
                "intended_email": "historical@example.com", "actual_email": "historical@example.com",
                "send_status": "sent", "delivered_at": "2026-07-17 09:01:00",
            },
        ])
        events = FakeWorksheet("email_events", EMAIL_EVENTS_COLUMNS, [])
        self.spreadsheet = FakeSpreadsheet([
            customers, orders, contacts, messages, recipients, events,
        ])
        app_module.app.config.update(TESTING=True, SECRET_KEY="test-secret")
        app_module._email_sheets_cache = None
        self.spreadsheet_patcher = patch.object(
            app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet
        )
        self.today_patcher = patch.object(
            app_module, "stockholm_today", return_value=date(2026, 7, 22)
        )
        self.spreadsheet_patcher.start()
        self.today_patcher.start()
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = {
                "user_name": "olle", "name": "Olle", "role": "Account Manager",
                "email": "olle@eatpolarbar.com", "phone": "070",
            }

    def tearDown(self):
        self.today_patcher.stop()
        self.spreadsheet_patcher.stop()
        app_module._email_sheets_cache = None

    def test_customer_insights_exposes_click_followup_fields(self):
        response = self.client.get("/customer-insights")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["butik a"]["email_click_without_order"])
        self.assertEqual(payload["butik a"]["email_followup_priority"], 2)
        self.assertEqual(
            payload["butik a"]["email_followup_label"],
            "Produktbladsklick utan order",
        )
        self.assertTrue(payload["butik b"]["email_click_without_order"])
        self.assertEqual(payload["butik b"]["email_followup_priority"], 1)

    def test_followup_insights_email_report_respects_responsible_filter(self):
        response = self.client.get("/followup-insights?responsible=Sofia")
        self.assertEqual(response.status_code, 200)
        report = response.get_json()["email_performance"]
        self.assertEqual(report["unique_store_count"], 1)
        self.assertEqual(report["product_clicked_store_count"], 1)
        self.assertEqual(report["stockfiller_clicked_store_count"], 0)

    def test_followup_insights_without_responsible_keeps_historical_customers(self):
        response = self.client.get("/followup-insights")
        self.assertEqual(response.status_code, 200)
        report = response.get_json()["email_performance"]
        self.assertEqual(report["unique_store_count"], 3)


class ReminderSendRouteTests(unittest.TestCase):
    def setUp(self):
        customer_headers = [
            "customer", "cancelled_flag", "customer_number", "name", "email",
            "email_last_order", "sales_person",
        ]
        customers = FakeWorksheet("customers_enriched", customer_headers, [
            {
                "customer": "Butiken", "customer_number": "C-1", "name": "Klara Kund",
                "email": "klara@example.com", "email_last_order": "anna@example.com",
                "sales_person": "Olle",
            },
            {
                "customer": "Gamla butiken", "customer_number": "C-2", "name": "Gunnar Kund",
                "email": "gunnar@example.com", "email_last_order": "gunnar@example.com",
                "sales_person": "Olle",
            },
            {
                "customer": "Nya butiken", "customer_number": "C-3", "name": "Nina Kund",
                "email": "nina@example.com", "email_last_order": "", "sales_person": "Olle",
            },
        ])
        order_rows = FakeWorksheet("order_rows", app_module.ORDER_COLUMNS, [
            {
                "Reference": "REF-1", "Order date": "2026-06-01", "Delivery date": "2026-06-05",
                "Customer": "Butiken", "placedBy": "Anna Andersson", "buyerEmail": "anna@example.com",
                "Product": "Mango", "Quantity": "2", "Unit": "DFP", "Total": "100", "Currency": "SEK",
            },
            {
                "Reference": "REF-OLD", "Order date": "2025-01-01", "Delivery date": "2025-01-05",
                "Customer": "Gamla butiken", "placedBy": "Gunnar Kund", "buyerEmail": "gunnar@example.com",
                "Product": "Hallon", "Quantity": "2", "Unit": "DFP", "Total": "100", "Currency": "SEK",
            },
        ])
        contacts = FakeWorksheet("sales_activities", app_module.CONTACT_COLUMNS, [])
        settings = FakeWorksheet("settings", ["key", "value", "description"], [
            {"key": "reminder_product_sheet_url", "value": "https://drive.google.com/product"},
            {"key": "reactivation_product_sheet_url", "value": "https://drive.google.com/reactivation"},
            {"key": "new_customer_product_sheet_url", "value": "https://drive.google.com/new-customer"},
            {"key": "reminder_stockfiller_url", "value": "https://order.stockfiller.com/"},
            {"key": "sku_10001", "value": "Jordgubbar i mörk choklad + vit choklad"},
            {"key": "sku_10002", "value": "Hallon i mjölkchoklad + vit choklad"},
            {"key": "sku_10003", "value": "Jordgubbar i mjölkchoklad + vit choklad"},
            {"key": "sku_10004", "value": "Hallon i mörk choklad + vit choklad"},
            {"key": "sku_10005", "value": "Blåbär i mörk choklad + vit choklad"},
            {"key": "sku_10006", "value": "Mango i mjölkchoklad + vit choklad"},
        ])
        messages = FakeWorksheet("email_messages", EMAIL_MESSAGES_COLUMNS, [])
        recipients = FakeWorksheet("email_recipients", EMAIL_RECIPIENTS_COLUMNS, [])
        events = FakeWorksheet("email_events", EMAIL_EVENTS_COLUMNS, [])
        self.spreadsheet = FakeSpreadsheet([customers, order_rows, contacts, settings, messages, recipients, events])
        app_module.app.config.update(TESTING=True, SECRET_KEY="test-secret")
        self.spreadsheet_patcher = patch.object(app_module, "get_spreadsheet_with_retry", return_value=self.spreadsheet)
        self.spreadsheet_patcher.start()
        self.client = app_module.app.test_client()
        with self.client.session_transaction() as flask_session:
            flask_session["user"] = {
                "user_name": "olle", "name": "Olle", "role": "Account Manager",
                "email": "olle@eatpolarbar.com", "phone": "070",
            }

    def tearDown(self):
        self.spreadsheet_patcher.stop()

    def _draft(self):
        response = self.client.get("/customers/2/email-proposal-draft")
        self.assertEqual(response.status_code, 200)
        return response.get_json()["draft"]

    @staticmethod
    def _payload(draft):
        return {
            "draft_id": draft["draft_id"],
            "email_type": draft["email_type"],
            "created_at": draft["created_at"],
            "latest_order_reference": draft["latest_order_reference"],
            "subject": draft["subject"],
            "intro_text": draft["intro_text"],
            "closing_text": draft["closing_text"],
            "order_rows": draft["order_rows"],
            "links": draft["links"],
            "recipients": [{**recipient, "selected": True} for recipient in draft["recipients"]],
            "confirm_warnings": True,
        }

    def test_draft_selects_template_link_and_order_mix_for_all_relationships(self):
        reminder = self._draft()
        self.assertEqual(reminder["email_type"], "reminder")
        self.assertEqual(reminder["links"]["product_sheet_url"], "https://drive.google.com/product")

        reactivation_response = self.client.get("/customers/3/email-proposal-draft")
        self.assertEqual(reactivation_response.status_code, 200)
        reactivation = reactivation_response.get_json()["draft"]
        self.assertEqual(reactivation["email_type"], "reactivation")
        self.assertEqual(
            reactivation["links"]["product_sheet_url"],
            "https://drive.google.com/reactivation",
        )
        self.assertEqual(reactivation["subject"], "Lägre priser på Polarbär! Dags att ta in?")
        self.assertIn("Tiden springer!", reactivation["intro_text"])
        self.assertIn("sänkt ordinarie pris", reactivation["intro_text"])
        self.assertIn("83% återköpsgrad", reactivation["intro_text"])
        self.assertIn("Klicka bara in på Stockfiller", reactivation["closing_text"])
        self.assertEqual([row["quantity"] for row in reactivation["order_rows"]], ["4"] * 4)
        self.assertTrue(all("new_for_customer" not in row for row in reactivation["order_rows"]))

        new_customer_response = self.client.get("/customers/4/email-proposal-draft")
        self.assertEqual(new_customer_response.status_code, 200)
        new_customer = new_customer_response.get_json()["draft"]
        self.assertEqual(new_customer["email_type"], "new_customer")
        self.assertEqual(
            new_customer["links"]["product_sheet_url"],
            "https://drive.google.com/new-customer",
        )
        self.assertIn("83% återköpsgrad", new_customer["intro_text"])
        self.assertIn("populärt på sociala medier", new_customer["closing_text"])
        self.assertEqual(len(new_customer["order_rows"]), 4)
        self.assertEqual([row["quantity"] for row in new_customer["order_rows"]], ["3"] * 4)
        self.assertEqual(len(new_customer["product_catalog"]), 6)

    def test_template_settings_are_saved_and_used_by_new_customer_drafts(self):
        response = self.client.get("/email-proposal-settings")
        self.assertEqual(response.status_code, 200)
        settings = response.get_json()
        self.assertEqual(set(settings["templates"]), {"reminder", "reactivation", "new_customer"})
        self.assertEqual(settings["templates"]["reminder"]["order_mode"], "latest_order")
        self.assertEqual(
            [row["quantity"] for row in settings["templates"]["reactivation"]["order_rows"]],
            ["4"] * 4,
        )

        payload = {
            "subject": "Välkommen tillbaka {{butiksnamn}}",
            "intro_text": "Hej (namn)\n\nDet var {{veckor}} veckor sedan.",
            "closing_text": "Beställ när det passar.",
            "stockfiller_label": "Öppna Stockfiller",
            "product_sheet_label": "Öppna produktbladet",
            "order_mode": "fixed",
            "order_rows": [{
                "product": "Mango i mjölkchoklad + vit choklad",
                "quantity": "7",
                "unit": "DFP",
            }],
        }
        update = self.client.put("/email-proposal-settings/reactivation", json=payload)
        self.assertEqual(update.status_code, 200)
        saved = update.get_json()["template"]
        self.assertTrue(saved["customized"])
        self.assertEqual(saved["stockfiller_label"], "Öppna Stockfiller")
        self.assertEqual(saved["order_rows"][0]["quantity"], "7")

        draft_response = self.client.get("/customers/3/email-proposal-draft")
        self.assertEqual(draft_response.status_code, 200)
        draft = draft_response.get_json()["draft"]
        self.assertEqual(draft["subject"], "Välkommen tillbaka Gamla butiken")
        self.assertNotIn("{{veckor}}", draft["intro_text"])
        self.assertEqual(draft["closing_text"], "Beställ när det passar.")
        self.assertEqual(draft["cta_labels"]["stockfiller"], "Öppna Stockfiller")
        self.assertEqual(draft["cta_labels"]["product_sheet"], "Öppna produktbladet")
        self.assertEqual(draft["order_rows"], [{
            "product": "Mango i mjölkchoklad + vit choklad",
            "quantity": "7",
            "unit": "DFP",
        }])

    def test_template_settings_validate_live_email_defaults(self):
        base = {
            "subject": "Ämne",
            "intro_text": "Hej (namn)",
            "closing_text": "",
            "stockfiller_label": "Beställ",
            "product_sheet_label": "Läs mer",
            "order_mode": "fixed",
            "order_rows": [],
        }
        missing_rows = self.client.put("/email-proposal-settings/new_customer", json=base)
        self.assertEqual(missing_rows.status_code, 400)
        self.assertEqual(missing_rows.get_json()["error"], "missing_order_rows")

        missing_subject = self.client.put(
            "/email-proposal-settings/new_customer",
            json={**base, "subject": "", "order_rows": [{"product": "Mango", "quantity": "1"}]},
        )
        self.assertEqual(missing_subject.status_code, 400)
        self.assertEqual(missing_subject.get_json()["error"], "missing_email_content")

    def test_test_mode_redirects_two_recipients_and_keeps_sales_timeline_clean(self):
        draft = self._draft()
        self.assertEqual([row["greeting_name"] for row in draft["recipients"]], ["Anna", "Klara"])
        with patch.object(app_module, "send_brevo_transactional_email", side_effect=["msg-a", "msg-b"]) as send:
            response = self.client.post("/customers/2/email-proposal/send", json=self._payload(draft))
        self.assertEqual(response.status_code, 200)
        result = response.get_json()
        self.assertTrue(result["is_test"])
        self.assertEqual(send.call_count, 2)
        self.assertEqual({call.kwargs["recipient_email"] for call in send.call_args_list}, {"olle@eatpolarbar.com"})

        recipient_rows = app_module.worksheet_to_dicts(
            self.spreadsheet.worksheet("email_recipients"), expected_columns=EMAIL_RECIPIENTS_COLUMNS
        )
        self.assertEqual({row["brevo_message_id"] for row in recipient_rows}, {"msg-a", "msg-b"})
        self.assertEqual(len(self.spreadsheet.worksheet("sales_activities").values), 1)
        timeline = app_module.build_customer_timeline(
            "Butiken", app_module.get_order_rows(self.spreadsheet), [], {
                app_module.EMAIL_MESSAGES_SHEET: self.spreadsheet.worksheet("email_messages"),
                app_module.EMAIL_RECIPIENTS_SHEET: self.spreadsheet.worksheet("email_recipients"),
                app_module.EMAIL_EVENTS_SHEET: self.spreadsheet.worksheet("email_events"),
            },
        )
        self.assertEqual(timeline, [])

        duplicate = self.client.post("/customers/2/email-proposal/send", json=self._payload(draft))
        self.assertEqual(duplicate.status_code, 409)
        self.assertEqual(duplicate.get_json()["error"], "duplicate_send")

    def test_partial_failure_is_saved_per_recipient(self):
        draft = self._draft()
        with patch.object(app_module, "send_brevo_transactional_email", side_effect=["msg-ok", RuntimeError("Brevo error")]):
            response = self.client.post("/customers/2/email-proposal/send", json=self._payload(draft))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "partial")
        rows = app_module.worksheet_to_dicts(
            self.spreadsheet.worksheet("email_recipients"), expected_columns=EMAIL_RECIPIENTS_COLUMNS
        )
        self.assertEqual([row["send_status"] for row in rows], ["sent", "failed"])

    def test_live_mode_uses_intended_addresses_and_creates_one_sales_activity(self):
        with patch.object(app_module, "EMAIL_SEND_MODE", "live"):
            draft = self._draft()
            with patch.object(app_module, "send_brevo_transactional_email", side_effect=["msg-live-a", "msg-live-b"]) as send:
                response = self.client.post("/customers/2/email-proposal/send", json=self._payload(draft))
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.get_json()["is_test"])
        self.assertEqual(
            {call.kwargs["recipient_email"] for call in send.call_args_list},
            {"anna@example.com", "klara@example.com"},
        )
        activities = app_module.worksheet_to_dicts(
            self.spreadsheet.worksheet("sales_activities"), expected_columns=app_module.CONTACT_COLUMNS
        )
        self.assertEqual(len(activities), 1)
        self.assertEqual(activities[0]["result"], "Mejlförslag skickat – Påminnelse")
        self.assertEqual(activities[0]["email_id"], draft["draft_id"])

    def test_hard_bounced_address_is_unselected_and_cannot_be_forced(self):
        app_module.append_dict_row(self.spreadsheet.worksheet("email_recipients"), EMAIL_RECIPIENTS_COLUMNS, {
            "email_id": "old", "customer": "Butiken", "intended_email": "anna@example.com",
            "bounce_type": "hardbounce",
        })
        draft = self._draft()
        bounced = next(row for row in draft["recipients"] if row["email"] == "anna@example.com")
        self.assertFalse(bounced["selected"])
        self.assertIn("Permanent", bounced["blocked_reason"])
        payload = self._payload(draft)
        response = self.client.post("/customers/2/email-proposal/send", json=payload)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["error"], "recipient_blocked")


if __name__ == "__main__":
    unittest.main()
