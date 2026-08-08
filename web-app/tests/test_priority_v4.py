from datetime import date
from pathlib import Path
import sys
from unittest import TestCase


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from planning_suggestions import decision_context_hash, deterministic_suggestion_id  # noqa: E402
from priority import build_contact_features, build_order_features, build_priority_customers  # noqa: E402


def customer(customer_id="cid-1", number="100", name="Butik", segment="C", row=2):
    return {
        "row": row, "customer_id": customer_id, "customer_number": number,
        "customer": name, "sales_person": "Olle", "customer_segment": segment,
    }


def order(reference, delivered, customer_id="cid-1", number="100", name="Butik"):
    return {
        "Reference": reference, "Order date": delivered, "Delivery date": delivered,
        "customer_id": customer_id, "Customer number": number, "Customer": name,
        "Quantity": "20", "Total weight": "20", "Total": "400",
    }


def contact(when, result="Neutral", contact_id="contact-1", follow_up=""):
    return {
        "contact_id": contact_id, "customer_id": "cid-1", "customer": "Butik",
        "date_time": when, "sales_person": "Olle", "contact_channel": "Telefon",
        "result": result, "follow_up_date": follow_up,
    }


def email_rows(*, kind="stockfiller", customer_id="cid-1", number="100", name="Butik"):
    message = {
        "email_id": "email-1", "customer_id": customer_id,
        "customer_number": number, "customer": name, "sent_at": "2026-08-01 09:00:00",
        "status": "sent", "is_test": "N",
    }
    recipient = {
        "email_id": "email-1", "send_status": "sent",
        "intended_email": "buyer@example.com", "actual_email": "buyer@example.com",
    }
    if kind == "stockfiller":
        recipient.update({
            "stockfiller_click_count": "2",
            "stockfiller_first_clicked_at": "2026-08-02 10:00:00",
            "stockfiller_last_clicked_at": "2026-08-03 11:00:00",
        })
    elif kind == "product":
        recipient.update({
            "product_sheet_click_count": "1",
            "product_sheet_first_clicked_at": "2026-08-02 10:00:00",
            "product_sheet_last_clicked_at": "2026-08-02 10:00:00",
        })
    elif kind == "open":
        recipient.update({"open_count": "1", "last_opened_at": "2026-08-02 10:00:00"})
    elif kind == "delivered":
        recipient.update({"delivered_at": "2026-08-02 10:00:00"})
    return [message], [recipient]


def scored(*, today, orders=(), contacts=(), email_feature=None, customers=None, planned=()):
    customers = customers or [customer()]
    order_features = build_order_features(list(orders))
    email_features = {}
    if email_feature:
        email_features = {"id:cid-1": email_feature}
    return build_priority_customers(
        customers, order_features, build_contact_features(list(contacts), order_features),
        "Olle", today, limit=len(customers), email_features=email_features,
        planned_activities=list(planned),
    )


def suggestion_identity(item):
    context = decision_context_hash(
        owner="olle", customer_id=item["customer_id"],
        lifecycle=item["decision_context_lifecycle"], order_count=item["order_count"],
        latest_order_reference=item["latest_order_reference"],
        latest_order_date=item["latest_delivery_date"],
        latest_contact_id=item["latest_human_contact_id"],
        latest_contact_result=item["latest_contact_result"],
        latest_contact_date=item["latest_human_contact_date"],
        active_email_intent_event=item["active_email_intent_event"],
    )
    return context, deterministic_suggestion_id("olle", item["customer_id"], context)


class Phase4EmailIntentTests(TestCase):
    def snapshot(self, kind, today=date(2026, 8, 2), customers=None):
        messages, recipients = email_rows(kind=kind)
        result = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=today, customers=customers or [customer()]
        )
        return result["id:cid-1"]

    def test_stockfiller_and_product_clicks_apply_exact_modifiers_immediately(self):
        baseline = scored(today=date(2026, 8, 2))[0]
        stock = scored(today=date(2026, 8, 5), email_feature=self.snapshot("stockfiller", date(2026, 8, 5)))[0]
        product = scored(today=date(2026, 8, 5), email_feature=self.snapshot("product", date(2026, 8, 5)))[0]

        self.assertEqual(stock["intent_timing"], baseline["intent_timing"] + 8)
        self.assertEqual(product["intent_timing"], baseline["intent_timing"] + 4)
        self.assertEqual(stock["primary_trigger_type"], "stockfiller_click_followup")
        self.assertEqual(product["primary_trigger_type"], "product_sheet_click_followup")
        self.assertEqual(self.snapshot("stockfiller", date(2026, 8, 5))["email_followup_wait_days_remaining"], 0)

    def test_stockfiller_click_precedes_product_sheet_click(self):
        messages, recipients = email_rows(kind="stockfiller")
        recipients[0].update({
            "product_sheet_click_count": "1",
            "product_sheet_first_clicked_at": "2026-08-02 09:00:00",
            "product_sheet_last_clicked_at": "2026-08-02 09:00:00",
        })
        snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 5), customers=[customer()]
        )["id:cid-1"]
        item = scored(today=date(2026, 8, 5), email_feature=snapshot)[0]
        self.assertEqual(item["primary_trigger_type"], "stockfiller_click_followup")

    def test_waiting_click_has_no_modifier_or_trigger(self):
        feature = self.snapshot("stockfiller")
        feature["email_followup_wait_days_remaining"] = 1
        baseline = scored(today=date(2026, 8, 2))[0]
        waiting = scored(today=date(2026, 8, 2), email_feature=feature)[0]
        self.assertEqual(waiting["intent_timing"], baseline["intent_timing"])
        self.assertNotIn("stockfiller_click_followup", waiting["covered_trigger_keys"])
        ready = scored(
            today=date(2026, 8, 5),
            email_feature=self.snapshot("stockfiller", date(2026, 8, 5)),
        )[0]
        self.assertEqual(waiting["active_email_intent_event"], ready["active_email_intent_event"])
        identities = []
        for item in (waiting, ready):
            context = decision_context_hash(
                owner="olle", customer_id=item["customer_id"],
                lifecycle=item["decision_context_lifecycle"], order_count=item["order_count"],
                latest_order_reference=item["latest_order_reference"],
                latest_order_date=item["latest_delivery_date"],
                latest_contact_id=item["latest_human_contact_id"],
                latest_contact_result=item["latest_contact_result"],
                latest_contact_date=item["latest_human_contact_date"],
                active_email_intent_event=item["active_email_intent_event"],
            )
            identities.append((context, deterministic_suggestion_id("olle", "cid-1", context)))
        self.assertEqual(identities[0], identities[1])

    def test_open_has_no_modifier_or_recommendation_trigger(self):
        baseline = scored(today=date(2026, 8, 2))[0]
        opened = scored(today=date(2026, 8, 2), email_feature=self.snapshot("open"))[0]
        self.assertEqual(opened["intent_timing"], baseline["intent_timing"])
        self.assertNotIn("email", opened["primary_trigger_type"])
        self.assertEqual(opened["active_email_intent_event"], "")
        delivered = scored(today=date(2026, 8, 2), email_feature=self.snapshot("delivered"))[0]
        self.assertEqual(delivered["intent_timing"], baseline["intent_timing"])
        self.assertEqual(delivered["active_email_intent_event"], "")

    def test_email_modifier_changes_only_intent_and_score_components_stay_business_based(self):
        baseline = scored(today=date(2026, 8, 5))[0]
        clicked = scored(
            today=date(2026, 8, 5),
            email_feature=self.snapshot("stockfiller", date(2026, 8, 5)),
        )[0]
        for field in ("value_index", "strategic_index", "expected_order_dfp"):
            self.assertEqual(clicked[field], baseline[field])
        self.assertEqual(clicked["intent_timing"], baseline["intent_timing"] + 8)

    def test_separate_click_event_changes_context_event_identity(self):
        first = self.snapshot("stockfiller", date(2026, 8, 5))
        messages, recipients = email_rows(kind="stockfiller")
        messages[0]["email_id"] = "email-2"
        recipients[0]["email_id"] = "email-2"
        recipients[0]["stockfiller_first_clicked_at"] = "2026-08-06 10:00:00"
        recipients[0]["stockfiller_last_clicked_at"] = "2026-08-06 10:00:00"
        second_snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 9), customers=[customer()]
        )["id:cid-1"]
        first_item = scored(today=date(2026, 8, 5), email_feature=first)[0]
        second_item = scored(today=date(2026, 8, 9), email_feature=second_snapshot)[0]
        self.assertNotEqual(
            first_item["active_email_intent_event"], second_item["active_email_intent_event"]
        )

    def test_future_activity_and_explicit_followup_suppress_click(self):
        feature = self.snapshot("stockfiller", date(2026, 8, 5))
        with_activity = scored(
            today=date(2026, 8, 5), email_feature=feature,
            planned=[{
                "planned_activity_id": "future", "customer_id": "cid-1",
                "customer": "Butik", "status": "planned",
                "scheduled_at": "2026-08-10 09:00:00",
            }],
        )[0]
        explicit = contact("2026-08-01 10:00:00", follow_up="2026-08-10")
        with_followup = scored(
            today=date(2026, 8, 5), contacts=[explicit], email_feature=feature
        )[0]
        self.assertEqual(with_activity["recommendation_suppression_reason"], "future_planned_activity")
        self.assertEqual(with_followup["recommendation_suppression_reason"], "explicit_follow_up")
        self.assertNotIn("stockfiller_click_followup", with_activity["covered_trigger_keys"])
        self.assertNotIn("stockfiller_click_followup", with_followup["covered_trigger_keys"])

    def test_click_event_and_context_are_stable_as_time_passes(self):
        early_feature = self.snapshot("stockfiller", date(2026, 8, 5))
        later_feature = self.snapshot("stockfiller", date(2026, 8, 20))
        early = scored(today=date(2026, 8, 5), email_feature=early_feature)[0]
        later = scored(today=date(2026, 8, 20), email_feature=later_feature)[0]
        self.assertEqual(early["active_email_intent_event"], later["active_email_intent_event"])
        hashes = []
        for item in (early, later):
            context = decision_context_hash(
                owner="olle", customer_id=item["customer_id"],
                lifecycle=item["decision_context_lifecycle"], order_count=item["order_count"],
                latest_order_reference=item["latest_order_reference"],
                latest_order_date=item["latest_delivery_date"],
                latest_contact_id=item["latest_human_contact_id"],
                latest_contact_result=item["latest_contact_result"],
                latest_contact_date=item["latest_human_contact_date"],
                active_email_intent_event=item["active_email_intent_event"],
            )
            hashes.append((context, deterministic_suggestion_id("olle", "cid-1", context)))
        self.assertEqual(hashes[0], hashes[1])

    def test_later_order_or_human_contact_handles_click(self):
        feature = self.snapshot("stockfiller", date(2026, 8, 5))
        after_order = scored(
            today=date(2026, 8, 8), orders=[order("O-1", "2026-08-03")],
            email_feature=feature,
        )[0]
        after_contact = scored(
            today=date(2026, 8, 8), contacts=[contact("2026-08-03 12:00:00")],
            email_feature=feature,
        )[0]
        self.assertEqual(after_order["active_email_intent_event"], "")
        self.assertEqual(after_contact["active_email_intent_event"], "")

    def test_reorder_precedes_email_but_covered_keys_include_both(self):
        feature = self.snapshot("stockfiller", date(2026, 8, 5))
        item = scored(
            today=date(2026, 8, 8),
            orders=[order("O-1", "2026-06-01"), order("O-2", "2026-06-21")],
            email_feature=feature,
        )[0]
        self.assertEqual(item["primary_trigger_type"], "established_reorder_due")
        self.assertIn("stockfiller_click_followup", item["covered_trigger_keys"])

    def test_same_name_email_binds_only_to_matching_canonical_customer(self):
        customers = [
            customer("cid-1", "100", "Samma", row=2),
            customer("cid-2", "200", "Samma", row=3),
        ]
        messages, recipients = email_rows(name="Samma")
        snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 5), customers=customers
        )
        result = scored(
            today=date(2026, 8, 5), customers=customers,
            email_feature=snapshot["id:cid-1"],
        )
        by_id = {item["customer_id"]: item for item in result}
        self.assertEqual(by_id["cid-1"]["primary_trigger_type"], "stockfiller_click_followup")
        self.assertEqual(by_id["cid-2"]["primary_trigger_type"], "")

    def test_renamed_customer_keeps_email_intent_by_customer_id(self):
        messages, recipients = email_rows(name="Gammalt namn")
        master = customer(name="Nytt namn")
        snapshot = app_module.build_email_engagement_snapshot(
            messages, recipients, [], today=date(2026, 8, 5), customers=[master]
        )["id:cid-1"]
        item = scored(
            today=date(2026, 8, 5), customers=[master], email_feature=snapshot
        )[0]
        self.assertEqual(item["primary_trigger_type"], "stockfiller_click_followup")


class Phase4LegacyFollowupTests(TestCase):
    def test_past_unresolved_followup_creates_stable_legacy_trigger(self):
        legacy = contact("2026-07-01 10:00:00", contact_id="", follow_up="2026-07-10")
        first = scored(today=date(2026, 8, 2), contacts=[legacy])[0]
        later = scored(today=date(2026, 8, 8), contacts=[legacy])[0]
        self.assertEqual(first["primary_trigger_type"], "legacy_missed_followup")
        self.assertEqual(first["latest_human_contact_id"], later["latest_human_contact_id"])
        self.assertTrue(first["latest_human_contact_id"])

    def test_later_contact_resolves_legacy_need(self):
        rows = [
            contact("2026-07-01 10:00:00", contact_id="old", follow_up="2026-07-10"),
            contact("2026-07-20 10:00:00", contact_id="new"),
        ]
        item = scored(today=date(2026, 8, 2), contacts=rows)[0]
        self.assertNotIn("legacy_missed_followup", item["covered_trigger_keys"])

    def test_later_order_resolves_legacy_need(self):
        legacy = contact("2026-07-01 10:00:00", contact_id="source", follow_up="2026-07-10")
        item = scored(
            today=date(2026, 8, 2), contacts=[legacy],
            orders=[order("later", "2026-07-20")],
        )[0]
        self.assertNotIn("legacy_missed_followup", item["covered_trigger_keys"])

    def test_modern_linked_activity_prevents_legacy_duplicate(self):
        legacy = contact("2026-07-01 10:00:00", contact_id="source-1", follow_up="2026-07-10")
        item = scored(
            today=date(2026, 8, 2), contacts=[legacy],
            planned=[{
                "planned_activity_id": "activity-1", "customer_id": "cid-1",
                "customer": "Butik", "status": "planned",
                "scheduled_at": "2026-07-10 09:00:00", "source_contact_id": "source-1",
            }],
        )[0]
        self.assertNotIn("legacy_missed_followup", item["covered_trigger_keys"])

    def test_ready_email_triggers_beat_legacy_and_keep_exact_modifiers(self):
        legacy = contact(
            "2026-07-01 10:00:00", contact_id="legacy-email",
            follow_up="2026-07-10",
        )
        baseline = scored(today=date(2026, 8, 5), contacts=[legacy])[0]
        cases = (
            ("stockfiller", "stockfiller_click_followup", 8),
            ("product", "product_sheet_click_followup", 4),
        )
        for kind, trigger, modifier in cases:
            with self.subTest(kind=kind):
                messages, recipients = email_rows(kind=kind)
                snapshot = app_module.build_email_engagement_snapshot(
                    messages, recipients, [], today=date(2026, 8, 5),
                    customers=[customer()],
                )["id:cid-1"]
                item = scored(
                    today=date(2026, 8, 5), contacts=[legacy],
                    email_feature=snapshot,
                )[0]
                self.assertEqual(item["primary_trigger_type"], trigger)
                self.assertIn(trigger, item["covered_trigger_keys"])
                self.assertIn("legacy_missed_followup", item["covered_trigger_keys"])
                self.assertEqual(item["intent_timing"], baseline["intent_timing"] + modifier)

    def test_positive_dialogue_beats_legacy_for_prospect_and_existing_customer(self):
        cases = (
            ([], 10),
            ([order("FIRST", "2026-07-15")], 20),
        )
        for orders, modifier in cases:
            with self.subTest(existing=bool(orders)):
                positive = contact(
                    "2026-07-16 10:00:00", result="Positiv",
                    contact_id="legacy-positive", follow_up="2026-07-20",
                )
                neutral = contact(
                    "2026-07-16 10:00:00", result="Neutral",
                    contact_id="legacy-neutral", follow_up="2026-07-20",
                )
                baseline = scored(
                    today=date(2026, 8, 5), orders=orders, contacts=[neutral]
                )[0]
                item = scored(
                    today=date(2026, 8, 5), orders=orders, contacts=[positive]
                )[0]
                self.assertEqual(item["primary_trigger_type"], "positive_dialogue_followup")
                self.assertIn("positive_dialogue_followup", item["covered_trigger_keys"])
                self.assertIn("legacy_missed_followup", item["covered_trigger_keys"])
                self.assertEqual(item["intent_timing"], baseline["intent_timing"] + modifier)

    def test_future_explicit_followup_still_blocks_email_and_positive(self):
        ready = app_module.build_email_engagement_snapshot(
            *email_rows(kind="stockfiller"), [], today=date(2026, 8, 5),
            customers=[customer()],
        )["id:cid-1"]
        future_positive = contact(
            "2026-07-16 10:00:00", result="Positiv",
            contact_id="future-positive", follow_up="2026-08-10",
        )
        baseline = scored(
            today=date(2026, 8, 5),
            contacts=[contact(
                "2026-07-16 10:00:00", result="Neutral",
                contact_id="future-neutral", follow_up="2026-08-10",
            )],
        )[0]
        item = scored(
            today=date(2026, 8, 5), contacts=[future_positive],
            email_feature=ready,
        )[0]
        self.assertEqual(item["recommendation_suppression_reason"], "explicit_follow_up")
        self.assertNotIn("stockfiller_click_followup", item["covered_trigger_keys"])
        self.assertNotIn("positive_dialogue_followup", item["covered_trigger_keys"])
        self.assertEqual(item["intent_timing"], baseline["intent_timing"])

    def test_wait_boundary_with_legacy_keeps_context_and_changes_live_primary(self):
        legacy = contact(
            "2026-07-01 10:00:00", contact_id="legacy-wait",
            follow_up="2026-07-10",
        )
        waiting_messages, waiting_recipients = email_rows(kind="stockfiller")
        waiting_feature = app_module.build_email_engagement_snapshot(
            waiting_messages, waiting_recipients, [], today=date(2026, 8, 4),
            customers=[customer()],
        )["id:cid-1"]
        ready_feature = app_module.build_email_engagement_snapshot(
            waiting_messages, waiting_recipients, [], today=date(2026, 8, 5),
            customers=[customer()],
        )["id:cid-1"]
        waiting = scored(
            today=date(2026, 8, 4), contacts=[legacy],
            email_feature=waiting_feature,
        )[0]
        ready = scored(
            today=date(2026, 8, 5), contacts=[legacy],
            email_feature=ready_feature,
        )[0]
        self.assertEqual(waiting["primary_trigger_type"], "legacy_missed_followup")
        self.assertEqual(ready["primary_trigger_type"], "stockfiller_click_followup")
        self.assertTrue(waiting["active_email_intent_event"])
        self.assertEqual(waiting["intent_timing"] + 8, ready["intent_timing"])
        self.assertEqual(suggestion_identity(waiting), suggestion_identity(ready))

    def test_full_precedence_keeps_reorder_above_positive_email_and_legacy(self):
        legacy_positive = contact(
            "2026-07-01 10:00:00", result="Positiv",
            contact_id="all-signals", follow_up="2026-07-10",
        )
        ready = app_module.build_email_engagement_snapshot(
            *email_rows(kind="stockfiller"), [], today=date(2026, 8, 5),
            customers=[customer()],
        )["id:cid-1"]
        item = scored(
            today=date(2026, 8, 5), contacts=[legacy_positive],
            orders=[order("O-1", "2026-05-01"), order("O-2", "2026-05-21")],
            email_feature=ready,
        )[0]
        self.assertEqual(item["primary_trigger_type"], "established_reorder_due")
        self.assertEqual(
            item["covered_trigger_keys"],
            [
                "established_reorder_due", "positive_dialogue_followup",
                "stockfiller_click_followup", "legacy_missed_followup",
            ],
        )


class Phase4CalibrationTests(TestCase):
    def test_score_band_boundaries(self):
        values = {
            0: "0-49", 49: "0-49", 50: "50-69", 69: "50-69",
            70: "70-79", 79: "70-79", 80: "80-89", 89: "80-89",
            90: "90-100", 100: "90-100",
        }
        for score_value, expected in values.items():
            with self.subTest(score=score_value):
                self.assertEqual(app_module.calibration_score_band(score_value), expected)

    def test_export_is_deterministic_and_uses_persisted_score(self):
        events = [{
            "event_id": "e-1", "event_type": "suggestion_planned",
            "occurred_at": "2026-08-01 09:00:00", "customer_id": "cid-1",
            "suggestion_id": "s-1", "decision_context_hash": "ctx",
            "primary_trigger_key": "stockfiller_click_followup",
            "score_version": "v2", "lifecycle": "prospect",
            "recommendation_eligible": "Y", "priority_score": "71",
            "intent_timing": "68", "value_index": "70", "strategic_index": "25",
            "actual_planned_contact_type": "phone", "status_after": "planned",
        }]
        orders = [order("ORDER-AFTER", "2026-08-03")]
        first = app_module.build_calibration_rows(events, orders, [customer()])
        second = app_module.build_calibration_rows(events, orders, [customer()])
        self.assertEqual(first, second)
        self.assertEqual(first[0]["priority_score"], "71")
        self.assertEqual(first[0]["priority_score_band"], "70-79")
        self.assertEqual(first[0]["order_outcome"], "order_after_event")
        self.assertEqual(first[0]["first_order_reference_after_event"], "ORDER-AFTER")
