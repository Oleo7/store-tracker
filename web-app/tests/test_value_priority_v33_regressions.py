"""Reviewed policy scenarios through the production feature + decision pipeline.

Fixtures are synthetic. No CRM access or reconstructed scoring implementation.
"""
from datetime import datetime, timedelta
from copy import deepcopy
from pathlib import Path
from unittest import TestCase
from zoneinfo import ZoneInfo
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from priority import build_order_features, build_contact_features, build_priority_customers, apply_workflow_suppressions

NOW = datetime(2026, 10, 18, 9, 43, 38, tzinfo=ZoneInfo("Europe/Stockholm"))


def customer(cid="a", name="Butik A", segment="A"):
    return {"customer_id": cid, "customer": name, "customer_number": cid.upper(),
            "sales_person": "Olle", "customer_segment": segment, "phone": "0701234567", "row": 2}


def contact(days=30, result="Neutral", *, now=NOW, **extra):
    value = {"customer_id": "a", "customer": "Butik A", "customer_number": "A", "contact_id": "contact-" + str(days),
             "date_time": (now - timedelta(days=days)).isoformat(), "result": result,
             "contact_channel": "Telefon", "sales_person": "Olle"}
    # This helper represents documented customer promises, unlike plain dates
    # in test_history_priority_v34.py.
    if extra.get("follow_up_date"):
        value.update(follow_up_purpose="customer_followup", follow_up_agreement_confirmed="Y")
    value.update(extra)
    return value


def order(days=30, dfp=21, *, now=NOW, **extra):
    day = (now.date() - timedelta(days=days)).isoformat()
    value = {"customer_id": "a", "Customer": "Butik A", "Customer number": "A", "Reference": "order-" + str(days),
             "Order date": day, "Delivery date": day, "Total weight": str(dfp), "Total": "4000", "SKU": "Vanilj"}
    value.update(extra)
    return value


def obligation(due_days=3, **extra):
    value = {"obligation_id": "promise", "customer_id": "a", "owner_user_name": "olle",
             "status": "open", "purpose": "explicit_followup", "customer_agreement_confirmed": True, "source_scope": "obligation:promise",
             "due_date": (NOW.date() - timedelta(days=due_days)).isoformat(),
             "source_contact_id": "source", "source_created_at": (NOW - timedelta(days=30)).isoformat(), "revision": 1}
    value.update(extra)
    return value


def booking(days=3, **extra):
    value = {"planned_activity_id": "plan", "customer_id": "a", "customer": "Butik A", "customer_number": "A",
             "user_name": "olle", "sales_person": "Olle", "status": "planned", "contact_type": "phone",
             "scheduled_at": (NOW + timedelta(days=days)).isoformat()}
    value.update(extra)
    return value


def replay(*, customers=None, orders=(), contacts=(), plans=(), obligations=None, permissions=None,
           now=NOW, owners=None, workflow=None, old_callers=False):
    customers = customers or [customer()]
    options = {} if old_callers else {"customers": customers, "as_of": now}
    of = build_order_features(list(orders), **options)
    cf = build_contact_features(list(contacts), of, **options)
    rows = build_priority_customers(customers, of, cf, None, now.date(), limit=len(customers), now=now,
        planned_activities=plans, obligation_states=obligations, contact_permissions=permissions,
        planning_owner_user_names=owners, workflow_suppressions=workflow)
    return {row["customer_id"]: row for row in rows}


class ReviewedContactPolicyTests(TestCase):
    def assertCoherent(self, row):
        guidance = row["list_guidance"]
        self.assertEqual(row["customer_guidance"], guidance)
        self.assertEqual(row["can_start_unscheduled_contact"], guidance["can_contact_now"])
        self.assertEqual(row["recommendation_eligible"], guidance["can_contact_now"])
        if not guidance["can_contact_now"]:
            self.assertEqual(row["contact_priority_raw"], 0)
        if guidance["review_only"] or guidance["status_key"] in {"wait", "idle", "data_missing"}:
            self.assertEqual(guidance["recommended_contact_type"], "")
        if guidance["can_contact_now"]:
            self.assertIn(guidance["recommended_contact_type"], {"phone", "visit", "email"})

    def test_new_order_resolves_generic_retry_for_first_and_repeat_buyers(self):
        for history in ([], [order(60), order(30)]):
            with self.subTest(history=len(history)):
                row = replay(orders=history + [order(1)], contacts=[contact(5, "Ej anträffbar")])["a"]
                self.assertNotIn("retry", row["list_guidance"]["reason_code"])
                self.assertEqual(row["contact_priority_raw"], 0)
                self.assertCoherent(row)

    def test_reported_order_waits_two_business_days_then_requests_internal_review(self):
        report = contact(2, "Order lagd!")  # Friday -> Tuesday.
        row = replay(contacts=[report])["a"]
        self.assertEqual(row["list_guidance"]["reason_code"], "reported_order_sync_wait")
        self.assertEqual(row["contact_not_before"], "2026-10-20")
        later = replay(contacts=[report], now=NOW + timedelta(days=2))["a"]
        self.assertTrue(later["review_only"])
        self.assertFalse(later["requires_handling"])
        self.assertEqual(later["list_guidance"]["reason_code"], "reported_order_sync_review")
        self.assertCoherent(row)
        self.assertCoherent(later)

    def test_reported_order_does_not_prove_dialogue_or_delivery(self):
        row = replay(contacts=[contact(0, "Order lagd!")])["a"]
        self.assertEqual(row["business_verified_delivery_count"], 0)
        self.assertIsNone(row["days_since_reached"])
        self.assertEqual(row["business_deliveries_used"], 0)

    def test_new_synced_order_replaces_reported_order_wait(self):
        row = replay(contacts=[contact(2, "Order lagd!")], orders=[order(1, **{"Delivery date": "2026-10-21"})])["a"]
        self.assertEqual(row["list_guidance"]["reason_code"], "future_delivery")
        self.assertEqual(row["contact_not_before"], "2026-10-21")

    def test_old_promise_remains_handling_during_retry_pause(self):
        row = replay(contacts=[contact(1, "Ej anträffbar")], obligations={"a": [obligation(10)]})["a"]
        self.assertTrue(row["requires_handling"])
        self.assertEqual(row["list_guidance"]["reason_code"], "unanswered_retry_wait")
        self.assertEqual(row["contact_not_before"], "2026-10-20")
        paused = apply_workflow_suppressions([row], {"a": "snoozed"})[0]
        self.assertEqual(paused["list_guidance"]["reason_code"], "unanswered_retry_wait")
        self.assertCoherent(paused)

    def test_new_agreement_for_today_can_override_failed_attempt_pause(self):
        promise=obligation(0,source_created_at=(NOW-timedelta(hours=1)).isoformat())
        row=replay(contacts=[contact(1,'Ej anträffbar')],obligations={'a':[promise]})['a']
        self.assertFalse(row['can_start_unscheduled_contact'])
        self.assertEqual(row['action_type'],'unanswered_retry_wait')
        actual=booking(0,appointment_confirmed='Y',created_at=(NOW-timedelta(hours=1)).isoformat())
        row=replay(contacts=[contact(1,'Ej anträffbar')],plans=[actual])['a']
        self.assertTrue(row['can_execute_planned_contact'])
        self.assertGreater(row['work_priority_raw'],0)
        self.assertCoherent(row)

    def test_order_does_not_close_unrelated_service_promise(self):
        row = replay(orders=[order(1)], obligations={"a": [obligation(3)]})["a"]
        self.assertTrue(row["requires_handling"])
        self.assertEqual(row["list_guidance"]["reason_code"], "overdue_planned_activity")
        self.assertCoherent(row)


    def test_legacy_agreement_in_negative_event_cannot_override_contact_policy(self):
        source=contact(0,'Negativ',contact_id='source',follow_up_date=NOW.date().isoformat())
        row=replay(contacts=[source],obligations={'a':[obligation(0,source_created_at=source['date_time'])]})['a']
        self.assertFalse(row['can_start_unscheduled_contact'])
        self.assertEqual(row['action_type'],'negative_contact_cooldown')
        self.assertCoherent(row)


    def test_first_order_phase_profile_is_independent_of_routine_recency(self):
        phases = {7: .35, 10: .35, 11: .20, 23: .20, 24: .75, 30: .75,
                  31: .90, 45: .90, 46: .80, 60: .80, 61: .60, 90: .60}
        for days, phase in phases.items():
            for sku in ("", "Vanilj"):
                with self.subTest(days=days, sku=sku):
                    row = replay(orders=[order(days, SKU=sku)], contacts=[contact(days, "Neutral")])["a"]
                    expected = phase + (.08 if sku and days <= 60 else 0)
                    self.assertAlmostEqual(row["contact_factor"], expected)
                    self.assertAlmostEqual(row["contact_priority_raw"], row["business_value_raw"] * expected, places=5)
                    self.assertCoherent(row)

    def test_first_order_retry_keeps_its_underlying_phase(self):
        row = replay(orders=[order(90)], contacts=[contact(90, "Neutral"), contact(5, "Ej anträffbar")])["a"]
        self.assertEqual(row["list_guidance"]["reason_code"], "unanswered_retry_due")
        self.assertEqual(row["list_guidance"]["underlying_action_type"], "second_purchase_due")
        self.assertEqual(row["contact_factor"], .60)

    def test_first_order_onboarding_is_not_lost_to_blanket_twenty_day_wait(self):
        for contact_age in (8, 12):
            row = replay(orders=[order(8)], contacts=[contact(contact_age, "Positiv")])["a"]
            self.assertEqual(row["list_guidance"]["reason_code"], "first_order_onboarding")
            self.assertGreater(row["contact_priority_raw"], 0)

    def test_unknown_sku_is_not_assumed_to_be_one(self):
        row = replay(orders=[order(24, SKU="")])["a"]
        self.assertIsNone(row["business_first_order_sku_count"])
        self.assertEqual(row["contact_factor"], .75)

    def test_low_value_c_without_sku_indicator_has_no_early_onboarding(self):
        large = order(30, 1000, customer_id="b", Customer="Butik B", **{"Customer number": "B"})
        row = replay(customers=[customer(segment="C"), customer("b", "Butik B", "A")],
                     orders=[order(8, 1, SKU=""), large])["a"]
        self.assertLess(row["value_index"], 70)
        self.assertEqual(row["list_guidance"]["reason_code"], "first_order_onboarding_not_required")

    def test_open_onboarding_attempt_is_not_completed_and_has_only_short_gap(self):
        attempt = contact(1, "Neutral", purpose="first_order_onboarding", obligation_outcome="open")
        row = replay(orders=[order(8)], contacts=[attempt])["a"]
        self.assertEqual(row["list_guidance"]["reason_code"], "recent_same_purpose_dialogue_wait")
        later = replay(orders=[order(8)], contacts=[attempt], now=NOW + timedelta(days=2))["a"]
        self.assertEqual(later["list_guidance"]["reason_code"], "first_order_onboarding")
        attempt["obligation_outcome"] = "resolved"
        done = replay(orders=[order(8)], contacts=[attempt])["a"]
        self.assertEqual(done["list_guidance"]["reason_code"], "first_order_onboarding_completed")

    def test_positive_prospect_window_and_day_46_transition(self):
        for days, expected in ((6, "recent_reached_routine_wait"), (7, "positive_dialogue_followup"),
                               (45, "positive_dialogue_followup"), (46, "value_based_contact_due")):
            row = replay(customers=[customer(segment="C")], contacts=[contact(days, "Positiv")])["a"]
            self.assertEqual(row["list_guidance"]["reason_code"], expected)
            if days >= 7:
                self.assertGreater(row["contact_priority_raw"], 0)
            self.assertCoherent(row)

    def test_reactivation_uses_overdue_age_and_thirty_day_dialogue_gap(self):
        history = [order(300), order(270)]
        for days in (10, 29, 30):
            row = replay(orders=history, contacts=[contact(days, "Neutral")])["a"]
            self.assertEqual(row["lifecycle"], "reactivation")
            self.assertEqual(row["list_guidance"]["reason_code"], "recent_reactivation_dialogue_wait" if days < 30 else "repeat_reactivation_due")
            self.assertCoherent(row)

    def test_low_value_prospect_ranks_low_instead_of_disappearing(self):
        rows = replay(customers=[customer(segment="C"), customer("b", "Butik B", "A")],
                      orders=[order(180, 1, customer_id="b", Customer="Butik B", **{"Customer number": "B"})])
        row = rows["a"]
        self.assertLess(row["business_value_raw"], 6)
        self.assertTrue(row["can_start_unscheduled_contact"])
        self.assertGreater(row["contact_priority_raw"], 0)


    def test_ambiguous_name_only_history_is_not_attached_to_either_customer(self):
        stores = [customer(), customer("b", "Butik A", "B")]
        ambiguous = contact(100, "Återkom ej", customer_id="", customer_number="")
        rows = replay(customers=stores, contacts=[ambiguous, contact(30, "Neutral")])
        self.assertNotEqual(rows["a"]["contact_permission_status"], "do_not_contact")
        self.assertNotEqual(rows["b"]["contact_permission_status"], "do_not_contact")

    def test_date_collision_does_not_erase_known_dated_delivery(self):
        clean = [order(60), order(30)]
        collision = order(30, 8, Reference="undated", **{"Delivery date": ""})
        baseline = replay(orders=clean)["a"]
        row = replay(orders=clean + [collision])["a"]
        self.assertEqual(row["business_verified_delivery_count"], 2)
        self.assertEqual(row["business_latest_verified_delivery_dfp"], 21)
        self.assertEqual(row["business_value_raw"], baseline["business_value_raw"])

    def test_future_actual_contacts_orders_and_recordings_do_not_leak(self):
        future_contact = contact(0, "Återkom ej", date_time=(NOW + timedelta(hours=1)).isoformat())
        future_recording = contact(10, "Återkom ej", recorded_at=(NOW + timedelta(hours=1)).isoformat())
        future_order = order(0, 900, **{"Order date": (NOW + timedelta(hours=1)).isoformat()})
        row = replay(orders=[future_order], contacts=[contact(30), future_contact, future_recording], old_callers=True)["a"]
        self.assertEqual(row["latest_reached_date"], "2026-09-18")
        self.assertEqual(row["business_verified_delivery_count"], 0)
        self.assertTrue(row["can_start_unscheduled_contact"])

    def test_known_future_delivery_and_booking_remain_availability_facts(self):
        row = replay(orders=[order(10, **{"Delivery date": "2026-11-01"})], plans=[booking(3)])["a"]
        self.assertTrue(row["has_planned_contact"])
        self.assertEqual(row["list_guidance"]["reason_code"], "future_delivery")
        self.assertEqual(row["business_verified_delivery_count"], 0)
        self.assertEqual(row["work_priority_raw"], 0)

    def test_stockholm_midnight_and_repeated_dst_hour_use_real_event_order(self):
        midnight = datetime.fromisoformat("2026-10-18T00:30:00+02:00")
        event = contact(0, "Neutral", date_time="2026-10-17T23:00:00Z")
        self.assertIsNone(replay(contacts=[event], now=midnight)["a"]["days_since_reached"])
        first_hour = datetime.fromisoformat("2026-10-25T02:30:00+02:00")
        second_hour = contact(0, "Återkom ej", date_time="2026-10-25T02:15:00+01:00")
        row = replay(contacts=[contact(30), second_hour], now=first_hour)["a"]
        self.assertNotEqual(row["contact_permission_status"], "do_not_contact")

    def test_booking_has_work_priority_but_no_duplicate_contact_priority(self):
        unplanned = replay()["a"]
        planned = replay(plans=[booking(3)])["a"]
        self.assertEqual(planned["contact_priority_raw"], 0)
        self.assertEqual(planned["work_priority_raw"], unplanned["contact_priority_raw"])
        self.assertFalse(planned["can_execute_planned_contact"])
        today = replay(plans=[booking(0)], contacts=[contact(1, "Neutral")])["a"]
        self.assertEqual(today["work_priority_raw"], 0)
        self.assertTrue(today["can_execute_planned_contact"])
        self.assertFalse(today["requires_handling"])
        self.assertCoherent(planned)
        self.assertCoherent(today)


    def test_seller_created_booking_does_not_fabricate_permission_after_negative(self):
        plan = booking(0, created_at=(NOW - timedelta(hours=1)).isoformat())
        row = replay(plans=[plan], contacts=[contact(1, "Negativ")])["a"]
        self.assertFalse(row["can_execute_planned_contact"])
        self.assertEqual(row["list_guidance"]["reason_code"], "negative_contact_cooldown")

    def test_today_booking_keeps_work_priority_zero_during_new_order_wait(self):
        for delivery, reason in (("", "recent_order_pending_confirmation"),
                                 ((NOW.date() + timedelta(days=2)).isoformat(), "future_delivery")):
            with self.subTest(delivery=delivery):
                row = replay(plans=[booking(0)], orders=[order(1, **{"Delivery date": delivery})])["a"]
                self.assertEqual(row["list_guidance"]["reason_code"], reason)
                self.assertTrue(row["has_planned_contact"])
                self.assertEqual(row["contact_priority_raw"], 0)
                self.assertEqual(row["work_priority_raw"], 0)
                self.assertFalse(row["work_can_start_unscheduled_contact"])
                self.assertFalse(row["can_execute_planned_contact"])
                self.assertCoherent(row)

    def test_future_booking_covers_only_its_linked_obligation(self):
        plan = booking(3, obligation_id="promise", source_contact_id="source")
        linked = obligation(5, planned_activity_id="plan", status="scheduled")
        covered = replay(plans=[plan], obligations={"a": [linked]})["a"]
        self.assertFalse(covered["requires_handling"])
        unrelated = obligation(5, obligation_id="other", source_contact_id="different", source_scope="obligation:other")
        row = replay(plans=[plan], obligations={"a": [linked, unrelated]})["a"]
        self.assertTrue(row["requires_handling"])
        self.assertFalse(row["can_start_unscheduled_contact"])
        self.assertEqual(row["list_guidance"]["status_key"], "planned")
        self.assertIn("other", row["obligation_ids"])


    def test_blocked_candidates_cannot_create_outbound_permission(self):
        old = obligation(100, obligation_id="old", source_scope="obligation:old", status="needs_review")
        fresh = obligation(3, obligation_id="fresh", source_scope="obligation:fresh")
        scenarios = [
            ([contact(10, "Negativ")], fresh, "negative_contact_cooldown"),
            ([contact(1, "Ej anträffbar")], fresh, "unanswered_retry_wait"),
        ]
        for contacts, candidate, reason in scenarios:
            with self.subTest(reason=reason):
                row = replay(contacts=contacts, obligations={"a": [old, candidate]})["a"]
                self.assertEqual(row["obligation_id"], "fresh")
                self.assertEqual(row["list_guidance"]["reason_code"], reason)
                self.assertEqual(row["work_priority_raw"], 0)
                self.assertFalse(row["can_start_unscheduled_contact"])
                self.assertFalse(row["can_execute_planned_contact"])
                self.assertEqual(set(row["obligation_ids"]), {"old", "fresh"})
                self.assertCoherent(row)


    def test_due_promise_cannot_replace_current_bookings_real_order_restriction(self):
        old = obligation(100, obligation_id="old", source_scope="obligation:old", status="needs_review")
        fresh = obligation(3, obligation_id="fresh", source_scope="obligation:fresh")
        for day in (0, 3):
            with self.subTest(booking_in_days=day):
                row = replay(plans=[booking(day)], orders=[order(1, **{"Delivery date": ""})],
                             obligations={"a": [old, fresh]})["a"]
                self.assertEqual(row["planned_activity_id"], "plan")
                self.assertEqual(row["list_guidance"]["reason_code"], "recent_order_pending_confirmation")
                self.assertTrue(row["has_planned_contact"])
                self.assertEqual(row["work_priority_raw"], 0)
                self.assertFalse(row["can_start_unscheduled_contact"])
                self.assertFalse(row["can_execute_planned_contact"])
                self.assertEqual(set(row["obligation_ids"]), {"old", "fresh"})
                self.assertEqual(row["review_obligation_ids"],[])
                self.assertCoherent(row)


    def test_later_of_two_bookings_represents_its_own_promise(self):
        plans = [booking(1, planned_activity_id="first"), booking(4, planned_activity_id="later", source_contact_id="source")]
        row = replay(plans=plans, obligations={"a": [obligation(40, planned_activity_id="later", status="scheduled")]})["a"]
        self.assertFalse(row["requires_handling"])
        self.assertEqual(row["contact_priority_raw"], 0)

    def test_closed_obligation_does_not_resurrect_from_source_follow_up_date(self):
        source = contact(30, "Positiv", contact_id="source", follow_up_date="2026-10-01")
        row = replay(contacts=[source], obligations={"a": [obligation(17, status="resolved")]})["a"]
        self.assertFalse(row["requires_handling"])
        self.assertNotIn(row["list_guidance"]["reason_code"], {"overdue_explicit_follow_up", "follow_up_due_today"})


    def test_inactive_owner_is_a_real_guard(self):
        row = replay(owners={"a": ""})["a"]
        self.assertEqual(row["list_guidance"]["status_key"], "data_missing")
        self.assertCoherent(row)

    def test_snooze_cannot_zero_an_unrelated_due_promise(self):
        row = replay(obligations={"a": [obligation(5)]})["a"]
        paused = apply_workflow_suppressions([row], {"a": "snoozed"})[0]
        self.assertEqual(paused["contact_priority_raw"], row["contact_priority_raw"])
        self.assertCoherent(paused)
