"""Value/contact ranking read-model regression tests. No production CRM writes."""
from datetime import date
from pathlib import Path
from unittest import TestCase
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from priority import (build_priority_customers, build_order_features,
                      build_contact_features, apply_workflow_suppressions)
from value_prioritization import (contact_factor, business_days_elapsed, complete_deliveries,
    business_opportunity, validate_policy, POLICY, next_business_day_retry,
    store_format, empirical_format_prior, contact_opportunity)

TODAY = date(2026, 10, 17)


def customer(name="Butik", cid="a", segment="A"):
    return {"customer": name, "customer_id": cid, "customer_segment": segment,
            "sales_person": "Olle", "row": 2}


def delivery(cid, name, day, dfp, reference="one"):
    return {"customer_id": cid, "Customer": name, "Reference": reference,
            "Delivery date": day, "Order date": day, "Total weight": str(dfp), "Total": "4000"}


def contact(cid, name, day, result):
    return {"customer_id": cid, "customer": name, "date_time": day + " 12:00",
            "result": result, "contact_channel": "Telefon", "sales_person": "Olle"}


def get(customers, orders=(), contacts=(), planned=()):
    orders = build_order_features(list(orders))
    # Legacy test scenarios explicitly concern customer-promised callbacks.
    contacts = [{**row, "follow_up_purpose": "customer_followup", "follow_up_agreement_confirmed": "Y"}
                if row.get("follow_up_date") else row for row in contacts]
    contacts = build_contact_features(list(contacts), orders)
    return build_priority_customers(customers, orders, contacts,
                                    "Olle", TODAY, limit=len(customers),
                                    planned_activities=planned)


class ValuePriorityStage1Tests(TestCase):
    def test_unanswered_is_not_real_dialogue_and_creates_retry_action(self):
        row = get([customer()],
                  [delivery("a", "Butik", "2025-12-19", 30)],
                  [contact("a", "Butik", "2026-06-30", "Positiv"),
                   contact("a", "Butik", "2026-09-22", "Ej anträffbar")])[0]
        self.assertEqual(row["days_since_reached"], 109)
        self.assertEqual(row["latest_reached_date"], "2026-06-30")
        self.assertEqual(row["latest_human_contact_date"], "2026-09-22")
        self.assertEqual(row["primary_trigger_type"], "unanswered_retry_due")
        self.assertEqual(row["customer_guidance"]["status_key"], "act_now")
        self.assertGreater(row["contact_priority_score"], 0)

    def test_recent_dialogue_very_low_contact_priority(self):
        recent = get([customer()], contacts=[contact("a", "Butik", "2026-10-15", "Positiv")])[0]
        old = get([customer()], contacts=[contact("a", "Butik", "2026-06-15", "Positiv")])[0]
        self.assertGreater(recent["business_value_score"], 0)
        self.assertLess(recent["contact_priority_score"], 10)
        self.assertGreater(old["contact_priority_score"], recent["contact_priority_score"])

    def test_big_c_buyer_can_be_worth_more_than_a_prospect(self):
        stores = [customer("Aprospekt", "a", "A"), customer("Cstor", "c", "C")]
        orders = [
            delivery("c", "Cstor", "2026-01-10", 50, "1"),
            delivery("c", "Cstor", "2026-02-10", 50, "2"),
            delivery("c", "Cstor", "2026-03-10", 50, "3"),
            delivery("c", "Cstor", "2026-04-10", 50, "4"),
        ]
        rows = {r["customer_id"]: r for r in get(stores, orders)}
        self.assertGreater(rows["c"]["business_value_raw"], rows["a"]["business_value_raw"])

    def test_future_delivery_not_historical(self):
        future = delivery("a", "Butik", "2026-11-01", 900)
        future["Order date"] = "2026-10-01"  # Known order, future planned delivery.
        row = get([customer()], [future])[0]
        self.assertEqual(row["delivery_count"], 0)
        self.assertEqual(row["business_deliveries_used"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "wait")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_plan_keeps_value_and_removes_actionable_score(self):
        planned = [{"planned_activity_id": "actual-booking", "customer_id": "a", "customer": "Butik", "status": "planned",
                    "scheduled_at": "2026-10-20T11:00:00", "sales_person": "Olle", "contact_type": "phone"}]
        row = get([customer()], planned=planned)[0]
        self.assertGreater(row["business_value_score"], 0)
        self.assertEqual(row["contact_priority_score"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "planned")


    def test_negative_contact_still_suppressed(self):
        row = get([customer()], contacts=[contact("a", "Butik", "2026-10-12", "Negativ")])[0]
        self.assertEqual(row["customer_guidance"]["status_key"], "wait")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_three_failed_attempts_request_contact_verification(self):
        rows = [contact("a", "Butik", day, "Ej anträffbar")
                for day in ("2026-09-25", "2026-10-02", "2026-10-12")]
        row = get([customer()], contacts=rows)[0]
        self.assertEqual(row["primary_trigger_type"], "unanswered_verify_contact_due")
        self.assertEqual(row["customer_guidance"]["action_label"], "Kontrollera kontaktväg")

    def test_business_day_retry(self):
        self.assertEqual(business_days_elapsed(date(2026, 10, 16), TODAY), 0)
        self.assertEqual(business_days_elapsed(date(2026, 10, 14), TODAY), 2)

    def test_missing_or_future_volumes_excluded(self):
        self.assertEqual(complete_deliveries({"delivery_dfp_by_date": {
            "2026-01-01": None, "2026-02-01": 5, "2026-11-01": 999}}, TODAY), [5.0])


    def test_formula_uses_n_k_and_segment_mean_without_fixed_bonus(self):
        value = business_opportunity(
            delivery_values=[20.0, 40.0], prior_dfp=21.0,
            delivery_count=2, cycle_days=45,
        )
        self.assertAlmostEqual(value["business_expected_dfp_per_delivery"], 24.6)
        self.assertEqual(value["business_deliveries_used"], 2)

    def test_recent_reached_a_prospect_waits_but_old_one_is_actionable(self):
        latest = get([customer()], contacts=[contact("a","Butik","2026-10-10","Neutral")])[0]
        old = get([customer()], contacts=[contact("a","Butik","2026-08-18","Neutral")])[0]
        self.assertEqual(latest["list_guidance"]["status_key"], "wait")
        self.assertEqual(latest["contact_priority_score"], 0)
        self.assertEqual(old["list_guidance"]["status_key"], "act_now")
        self.assertGreater(old["contact_priority_score"], latest["contact_priority_score"])
        self.assertEqual(old["business_value_raw"], latest["business_value_raw"])


    def test_retry_two_business_days_and_three_calls_since_reached(self):
        self.assertEqual(next_business_day_retry(date(2026,10,12),2),date(2026,10,14))
        self.assertEqual(next_business_day_retry(date(2026,10,16),2),date(2026,10,20))
        wait = get([customer()], contacts=[contact("a","Butik","2026-10-16","Ej anträffbar")])[0]
        self.assertEqual(wait["list_guidance"]["status_key"], "wait")
        self.assertEqual(wait["list_guidance"]["waiting_until"], "2026-10-20")
        retry = get([customer()], contacts=[contact("a","Butik","2026-10-14","Ej anträffbar")])[0]
        self.assertEqual(retry["list_guidance"]["action_key"], "retry_unanswered")
        self.assertEqual(retry["list_guidance"]["status_key"], "act_now")
        check = get([customer()], contacts=[
            contact("a","Butik","2026-09-01","Ej anträffbar"),
            contact("a","Butik","2026-09-10","Neutral"),
            contact("a","Butik","2026-10-02","Ej anträffbar"),
            contact("a","Butik","2026-10-08","Ej anträffbar"),
            contact("a","Butik","2026-10-12","Ej anträffbar"),
        ])[0]
        self.assertEqual(check["list_guidance"]["action_key"], "verify_contact_details")

    def test_order_date_only_is_not_confirmed_delivery_in_stage_one(self):
        order = delivery("a","Butik","2026-01-01",40)
        order["Delivery date"] = ""
        out = get([customer()], orders=[order])[0]
        self.assertEqual(out["business_verified_delivery_count"],0)
        self.assertEqual(out["business_deliveries_used"],0)
        self.assertEqual(out["business_value_confidence"],"segmentbaserad")
        self.assertEqual(out["list_guidance"]["status_key"],"act_now")
        self.assertIn("historical_undated_order", {w["code"] for w in out["data_warnings"]})
        self.assertGreater(out["contact_priority_score"],0)

    def test_stale_repeat_keeps_value_but_reduces_realization_assumption(self):
        active = business_opportunity(delivery_values=[30,30,30],
            prior_dfp=21,delivery_count=3,cycle_days=30,days_since_delivery=30)
        inactive = business_opportunity(delivery_values=[30,30,30],
            prior_dfp=21,delivery_count=3,cycle_days=30,days_since_delivery=250)
        self.assertLess(inactive["business_value_raw"],active["business_value_raw"])
        self.assertEqual(inactive["business_expected_dfp_per_delivery"],
                         active["business_expected_dfp_per_delivery"])

    def test_model_policy_rejects_invalid_parameters(self):
        for changes in ({"score_scale_dfp":0},
                        {"realization_prospect":1.1},
                        {"shrinkage_purchases":-1}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    validate_policy({**POLICY,**changes})

    def test_separate_fields_and_legacy_score_preserved(self):
        row = get([customer()])[0]
        self.assertIn("priority_score", row)
        self.assertIn("business_value_raw", row)
        self.assertIn("contact_priority_raw", row)
        self.assertEqual(row["value_model_version"], "v3.5")


    def test_contact_priority_follows_real_dialogue_age_not_value_score(self):
        scores = []
        value_scores = []
        for age in (7, 20, 30, 40, 50, 60, 90):
            from datetime import timedelta
            reached = (TODAY - timedelta(days=age)).isoformat()
            row = get([customer()], contacts=[
                contact("a", "Butik", reached, "Neutral")
            ])[0]
            scores.append(row["contact_priority_score"])
            value_scores.append(row["business_value_score"])
        self.assertEqual(len(set(value_scores)), 1)
        self.assertEqual(scores[0], 0)
        self.assertEqual(scores, sorted(scores))
        self.assertGreaterEqual(scores[-1], 75)
        self.assertGreater(scores[5], scores[3])

    def test_store_formats_personalize_a_prospects_without_fixed_bonus(self):
        stores = [
            customer("ICA Maxi Ny", "new-maxi", "A"),
            customer("Stora Coop Ny", "new-coop", "A")
        ]
        deliveries = []
        for idx in range(8):
            mx = f"maxi-{idx}"
            co = f"coop-{idx}"
            stores.extend([
                customer(f"ICA Maxi Kund {idx}", mx, "A"),
                customer(f"Stora Coop Kund {idx}", co, "A"),
            ])
            deliveries.extend([
                delivery(mx, f"ICA Maxi Kund {idx}", "2026-01-15", 30, mx),
                delivery(co, f"Stora Coop Kund {idx}", "2026-01-15", 15, co),
            ])
        by_id = {r["customer_id"]: r for r in get(stores, deliveries)}
        large = by_id["new-maxi"]
        lower = by_id["new-coop"]
        self.assertGreater(large["business_prior_dfp"], lower["business_prior_dfp"])
        self.assertGreater(large["business_value_raw"], lower["business_value_raw"])
        self.assertEqual(large["business_prior_basis"], "segment_and_format")
        self.assertEqual(lower["business_prior_basis"], "segment_and_format")
        self.assertEqual(store_format("ICA Nära Klippan"), "ica_nara")
        self.assertEqual(store_format("Maxi ICA Högsbo"), "ica_maxi")

    def test_small_format_sample_falls_back_to_segment(self):
        self.assertAlmostEqual(empirical_format_prior(21, [100] * 7), 21)
        self.assertGreater(empirical_format_prior(21, [30] * 8), 21)

    def test_low_value_uncontacted_c_prospect_is_visible_but_not_urgent(self):
        by_id = {r["customer_id"]: r for r in get([
            customer("ICA Maxi A", "a", "A"),
            customer("ICA Nära C", "c", "C"),
        ])}
        self.assertEqual(by_id["c"]["list_guidance"]["status_key"], "act_now")
        self.assertGreater(by_id["c"]["contact_priority_score"], 0)
        self.assertLess(by_id["c"]["contact_priority_raw"], by_id["a"]["contact_priority_raw"])
        self.assertGreater(by_id["c"]["business_value_score"], 0)
        self.assertEqual(by_id["a"]["list_guidance"]["status_key"], "act_now")


    def test_business_cycle_uses_verified_deliveries_not_order_date_fallback(self):
        one = delivery("a", "Butik", "2026-04-01", 20, "one")
        two = delivery("a", "Butik", "2026-05-01", 20, "two")
        unconfirmed = delivery("a", "Butik", "2026-10-01", 50, "pending")
        unconfirmed["Delivery date"] = ""
        row = get([customer()], orders=[one, two, unconfirmed])[0]
        self.assertEqual(row["business_verified_delivery_count"], 2)
        self.assertEqual(row["business_days_since_delivery"],
                         (TODAY - date(2026, 5, 1)).days)
        self.assertGreater(row["business_overdue_days"], 100)
        # A registered but undelivered order replaces a false reorder prompt.
        self.assertEqual(row["list_guidance"]["reason_code"],
                         "unconfirmed_order_status_due")
        baseline = get([customer()], [one, two])[0]
        self.assertEqual(row["business_expected_cycle_days"],
                         baseline["business_expected_cycle_days"])
        self.assertEqual(row["business_value_raw"], baseline["business_value_raw"])


    def test_negative_response_after_older_promise_prevents_recontact(self):
        original = contact("a", "Butik", "2026-09-01", "Positiv")
        original["follow_up_date"] = "2026-10-02"
        later_negative = contact("a", "Butik", "2026-10-12", "Negativ")
        row = get([customer()], contacts=[original, later_negative])[0]
        self.assertEqual(row["list_guidance"]["status_key"], "wait")
        self.assertEqual(row["list_guidance"]["reason_code"], "negative_contact_cooldown")
        self.assertEqual(row["contact_priority_score"], 0)


    def test_positive_dialogue_without_order_boosts_30_day_a_prospect(self):
        # Same A-store value, same contact date; only result differs.
        from datetime import timedelta
        day = (TODAY - timedelta(days=30)).isoformat()
        neutral = get([customer()], contacts=[contact("a", "Butik", day, "Neutral")])[0]
        positive = get([customer()], contacts=[contact("a", "Butik", day, "Positiv")])[0]
        self.assertEqual(positive["business_value_raw"], neutral["business_value_raw"])
        self.assertEqual(positive["business_value_score"], neutral["business_value_score"])
        self.assertGreaterEqual(positive["contact_priority_score"], 73)
        self.assertGreaterEqual(
            positive["contact_priority_score"] - neutral["contact_priority_score"], 15
        )
        self.assertEqual(positive["list_guidance"]["action_label"], "Följ upp positiv dialog")
        self.assertEqual(positive["list_guidance"]["reason_code"], "positive_dialogue_followup")
        self.assertEqual(neutral["list_guidance"]["reason_code"], "value_based_contact_due")

    def test_positive_prospect_score_still_respects_recent_contact_age(self):
        from datetime import timedelta
        scores = []
        for days in (7, 20, 30, 40, 50, 60, 90):
            day = (TODAY - timedelta(days=days)).isoformat()
            item = get([customer()], contacts=[contact("a", "Butik", day, "Positiv")])[0]
            scores.append(item["contact_priority_score"])
            if days == 7:
                self.assertEqual(item["list_guidance"]["status_key"], "act_now")
                self.assertGreater(item["contact_priority_score"], 0)
            if days > 45:
                neutral = get([customer()], contacts=[contact("a", "Butik", day, "Neutral")])[0]
                self.assertEqual(item["contact_priority_raw"], neutral["contact_priority_raw"])
        self.assertEqual(scores[:4], sorted(scores[:4]))
        self.assertGreater(scores[2], scores[1])

    def test_positive_signal_applies_to_c_prospect_despite_low_routine_value(self):
        day = "2026-09-17"  # exactly 30 days old
        neutral = get([customer(segment="C")], contacts=[contact("a", "Butik", day, "Neutral")])[0]
        positive = get([customer(segment="C")], contacts=[contact("a", "Butik", day, "Positiv")])[0]
        self.assertEqual(neutral["list_guidance"]["status_key"], "act_now")
        self.assertEqual(positive["list_guidance"]["status_key"], "act_now")
        self.assertGreater(positive["contact_priority_score"], 0)
        self.assertEqual(positive["business_value_raw"], neutral["business_value_raw"])

    def test_failed_attempt_does_not_erase_earlier_positive_signal(self):
        contacts = [contact("a", "Butik", "2026-09-17", "Positiv"),
                    contact("a", "Butik", "2026-10-12", "Ej anträffbar")]
        item = get([customer()], contacts=contacts)[0]
        self.assertIn(item["list_guidance"]["reason_code"],
                      {"unanswered_retry_due", "unanswered_verify_contact_due"})
        self.assertGreaterEqual(item["contact_priority_score"], 73)

    def test_new_order_or_new_neutral_dialogue_clears_positive_boost(self):
        pos = contact("a", "Butik", "2026-09-17", "Positiv")
        neutral_after = contact("a", "Butik", "2026-10-10", "Neutral")
        after_neutral = get([customer()], contacts=[pos, neutral_after])[0]
        self.assertEqual(after_neutral["list_guidance"]["status_key"], "wait")
        self.assertEqual(after_neutral["contact_priority_score"], 0)
        ordered = get([customer()],
                      orders=[delivery("a", "Butik", "2026-10-12", 30)],
                      contacts=[pos])[0]
        self.assertNotEqual(ordered["list_guidance"]["reason_code"],
                            "positive_dialogue_followup")


    def test_positive_factor_example_matches_58_to_75_target(self):
        neutral = contact_opportunity(
            business_value_raw=10.5, days_since_reached=30,
            latest_attempt_date=TODAY, today=TODAY, latest_result="Neutral",
            status="act_now", trigger="value_based_contact_due",
        )
        positive = contact_opportunity(
            business_value_raw=10.5, days_since_reached=30,
            latest_attempt_date=TODAY, today=TODAY, latest_result="Positiv",
            status="act_now", trigger="positive_dialogue_followup",
            positive_prospect_signal=True,
        )
        self.assertEqual(neutral["contact_priority_score"], 58)
        self.assertEqual(positive["contact_priority_score"], 75)
        self.assertAlmostEqual(positive["contact_relevance_factor"], 0.75)


    def test_buyer_positive_dialogue_is_warmer_than_neutral_before_reorder_due(self):
        orders = [
            delivery("a", "Butik", "2026-09-01", 30, "a-1"),
            delivery("a", "Butik", "2026-10-01", 30, "a-2"),
        ]
        positive = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-03", "Positiv")
        ])[0]
        neutral = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-03", "Neutral")
        ])[0]
        self.assertEqual(positive["business_value_raw"], neutral["business_value_raw"])
        self.assertEqual(neutral["list_guidance"]["status_key"], "wait")
        self.assertEqual(neutral["contact_priority_score"], 0)
        self.assertEqual(positive["list_guidance"]["reason_code"], "positive_buyer_followup")
        self.assertEqual(positive["list_guidance"]["action_label"], "Följ upp positiv dialog")
        self.assertGreater(positive["contact_priority_raw"], neutral["contact_priority_raw"])

    def test_positive_buyer_boosts_materially_overdue_ranking_without_changing_value(self):
        orders = [
            delivery("a", "Butik", "2026-07-01", 40, "a-1"),
            delivery("a", "Butik", "2026-08-01", 40, "a-2"),
        ]
        positive = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-09-17", "Positiv")
        ])[0]
        neutral = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-09-17", "Neutral")
        ])[0]
        self.assertEqual(positive["business_value_raw"], neutral["business_value_raw"])
        self.assertEqual(positive["list_guidance"]["reason_code"], "positive_buyer_followup")
        self.assertEqual(neutral["list_guidance"]["reason_code"], "established_reorder_due")
        self.assertGreater(positive["contact_priority_raw"], neutral["contact_priority_raw"])

    def test_materially_late_reorder_breaks_routine_wait_only_after_minimum_gap(self):
        orders = [
            delivery("a", "Butik", "2026-07-01", 40, "a-1"),
            delivery("a", "Butik", "2026-08-01", 40, "a-2"),
        ]
        recent = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-12", "Neutral")
        ])[0]
        due = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-07", "Neutral")
        ])[0]
        self.assertEqual(recent["list_guidance"]["status_key"], "wait")
        self.assertEqual(recent["contact_priority_score"], 0)
        self.assertGreaterEqual(due["business_overdue_days"], 14)
        self.assertEqual(due["list_guidance"]["reason_code"], "established_reorder_due")
        self.assertTrue(due["list_guidance"]["can_contact_now"])
        self.assertGreaterEqual(due["contact_relevance_factor"], 0.75)

    def test_fresh_pending_order_pauses_reorder_without_changing_business_value(self):
        orders = [
            delivery("a", "Butik", "2026-07-01", 40, "a-1"),
            delivery("a", "Butik", "2026-08-01", 40, "a-2"),
        ]
        baseline = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-07", "Neutral")
        ])[0]
        pending = delivery("a", "Butik", "2026-10-12", 60, "a-pending")
        pending["Delivery date"] = ""
        with_order = get([customer()], orders=[*orders, pending], contacts=[
            contact("a", "Butik", "2026-10-07", "Neutral")
        ])[0]
        self.assertEqual(with_order["business_value_raw"], baseline["business_value_raw"])
        self.assertEqual(with_order["business_verified_delivery_count"], 2)
        self.assertEqual(with_order["latest_unconfirmed_order_date"], "2026-10-12")
        self.assertEqual(with_order["list_guidance"]["reason_code"],
                         "recent_order_pending_confirmation")
        self.assertEqual(with_order["list_guidance"]["status_key"], "wait")
        self.assertEqual(with_order["contact_priority_score"], 0)

    def test_unconfirmed_order_older_than_wait_window_requests_status_check(self):
        orders = [
            delivery("a", "Butik", "2026-07-01", 40, "a-1"),
            delivery("a", "Butik", "2026-08-01", 40, "a-2"),
        ]
        pending = delivery("a", "Butik", "2026-09-25", 50, "a-pending")
        pending["Delivery date"] = ""
        row = get([customer()], orders=[*orders, pending])[0]
        self.assertEqual(row["list_guidance"]["reason_code"], "unconfirmed_order_status_due")
        self.assertEqual(row["list_guidance"]["action_label"], "Kontrollera orderstatus")
        self.assertNotEqual(row["list_guidance"]["reason_code"], "established_reorder_due")


    def test_very_old_missing_delivery_is_annotation_not_permanent_contact_prohibition(self):
        orders = [
            delivery("a", "Butik", "2026-01-01", 30, "a-1"),
            delivery("a", "Butik", "2026-02-01", 30, "a-2"),
        ]
        uncertain = delivery("a", "Butik", "2026-03-01", 30, "missing")
        uncertain["Delivery date"] = ""
        result = get([customer()], orders=[*orders, uncertain])[0]
        self.assertEqual(result["list_guidance"]["status_key"], "act_now")
        self.assertEqual(result["list_guidance"]["reason_code"], "repeat_reactivation_due")
        self.assertEqual(result["business_verified_delivery_count"], 2)
        self.assertIn("historical_undated_order", {w["code"] for w in result["data_warnings"]})
        self.assertGreater(result["contact_priority_score"], 0)
        self.assertGreater(result["business_value_score"], 0)

    def test_confirmed_new_delivery_resets_reorder_alarm(self):
        orders = [
            delivery("a", "Butik", "2026-07-01", 40, "a-1"),
            delivery("a", "Butik", "2026-08-01", 40, "a-2"),
            delivery("a", "Butik", "2026-10-11", 40, "a-3"),
        ]
        row = get([customer()], orders=orders, contacts=[
            contact("a", "Butik", "2026-10-07", "Neutral")
        ])[0]
        self.assertLess(row["business_overdue_days"], 0)
        self.assertNotEqual(row["list_guidance"]["reason_code"], "established_reorder_due")

    def test_negative_dialogue_remains_lower_priority_after_sixty_days(self):
        from datetime import timedelta
        for days in (70, 110, 200):
            with self.subTest(days=days):
                day = (TODAY - timedelta(days=days)).isoformat()
                neg = get([customer()], contacts=[
                    contact("a", "Butik", day, "Negativ")
                ])[0]
                neutral = get([customer()], contacts=[
                    contact("a", "Butik", day, "Neutral")
                ])[0]
                self.assertEqual(neg["business_value_raw"], neutral["business_value_raw"])
                self.assertLess(neg["contact_priority_raw"], neutral["contact_priority_raw"])
                self.assertLess(neg["contact_priority_score"], neutral["contact_priority_score"])
        recent = get([customer()], contacts=[
            contact("a", "Butik", "2026-09-20", "Negativ")
        ])[0]
        self.assertEqual(recent["list_guidance"]["reason_code"], "negative_contact_cooldown")
        self.assertEqual(recent["contact_priority_score"], 0)

    def test_failed_attempt_after_negative_dialogue_does_not_restore_priority(self):
        from datetime import timedelta
        negative_date = (TODAY - timedelta(days=70)).isoformat()
        missed = contact("a", "Butik", "2026-10-12", "Ej anträffbar")
        negative = get([customer()], contacts=[
            contact("a", "Butik", negative_date, "Negativ"), missed
        ])[0]
        neutral = get([customer()], contacts=[
            contact("a", "Butik", negative_date, "Neutral"), missed
        ])[0]
        self.assertEqual(negative["list_guidance"]["reason_code"], "unanswered_retry_due")
        self.assertLess(negative["contact_priority_raw"], neutral["contact_priority_raw"])


    def test_reached_positive_after_negative_is_not_penalized_as_negative(self):
        neg = contact("a", "Butik", "2026-06-01", "Negativ")
        pos = contact("a", "Butik", "2026-09-17", "Positiv")
        renewed = get([customer()], contacts=[neg, pos])[0]
        positive_only = get([customer()], contacts=[pos])[0]
        self.assertEqual(renewed["contact_priority_raw"], positive_only["contact_priority_raw"])
        self.assertEqual(renewed["business_value_raw"], positive_only["business_value_raw"])


    def test_same_day_order_does_not_override_negative_human_reply(self):
        # Day-level Order date cannot prove the purchase happened AFTER
        # the negative reply; do not invent a same-day change of mind.
        ordered = delivery("a", "Butik", "2026-10-10", 30, "a-order")
        negative = contact("a", "Butik", "2026-10-10", "Negativ")
        row = get([customer()], orders=[ordered], contacts=[negative])[0]
        self.assertEqual(row["list_guidance"]["reason_code"], "negative_contact_cooldown")
        self.assertEqual(row["contact_priority_score"], 0)
        self.assertGreater(row["business_value_score"], 0)

    def test_contact_negative_policy_parameters_must_be_valid(self):
        for changes in (
            {"negative_60_to_90_factor": 1.2},
            {"negative_90_to_180_factor": 0.1},
            {"material_reorder_overdue_days": 0},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    validate_policy({**POLICY, **changes})


class ValuePriorityFrontendStage1Tests(TestCase):
    pass  # Frontend policy cases replaced by customer activity and browser contracts.
