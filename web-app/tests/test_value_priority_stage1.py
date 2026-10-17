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
    store_format, empirical_format_prior)

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
        row = get([customer()], [delivery("a", "Butik", "2026-11-01", 900)])[0]
        self.assertEqual(row["delivery_count"], 0)
        self.assertEqual(row["business_deliveries_used"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "wait")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_plan_keeps_value_and_removes_actionable_score(self):
        planned = [{"customer_id": "a", "customer": "Butik", "status": "planned",
                    "scheduled_at": "2026-10-20T11:00:00", "sales_person": "Olle", "contact_type": "phone"}]
        row = get([customer()], planned=planned)[0]
        self.assertGreater(row["business_value_score"], 0)
        self.assertEqual(row["contact_priority_score"], 0)
        self.assertEqual(row["customer_guidance"]["status_key"], "planned")

    def test_snooze_never_changes_value(self):
        row = get([customer()])[0]
        updated = apply_workflow_suppressions([row], {"a": "snoozed"})[0]
        self.assertEqual(updated["business_value_score"], row["business_value_score"])
        self.assertEqual(updated["contact_priority_score"], 0)

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

    def test_customer_did_not_opt_in_after_earlier_do_not_contact(self):
        value = get([customer()], contacts=[
            contact("a","Butik","2026-06-01","Återkom ej"),
            contact("a","Butik","2026-10-12","Ej anträffbar"),
        ])[0]
        self.assertEqual(value["list_guidance"]["reason_code"], "do_not_contact")
        self.assertEqual(value["list_guidance"]["action_label"], "Kontakta inte")
        self.assertEqual(value["contact_priority_score"], 0)
        self.assertGreater(value["business_value_score"], 0)

    def test_failed_call_does_not_complete_explicit_followup_in_list(self):
        followup = contact("a","Butik","2026-09-01","Positiv")
        followup["follow_up_date"] = "2026-09-20"
        fail = contact("a","Butik","2026-10-02","Ej anträffbar")
        value = get([customer()], contacts=[followup,fail])[0]
        self.assertEqual(value["list_guidance"]["status_key"], "overdue_followup")
        self.assertEqual(value["list_guidance"]["reason_code"], "overdue_explicit_follow_up")
        self.assertGreater(value["contact_priority_score"], 0)

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
        self.assertEqual(out["list_guidance"]["status_key"],"idle")
        self.assertEqual(out["list_guidance"]["action_label"],"Granska order-/leveransstatus")
        self.assertEqual(out["contact_priority_score"],0)

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
        self.assertEqual(row["value_model_version"], "v3.2-preview")


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

    def test_small_format_sample_falls_back_to_segment(self):
        self.assertAlmostEqual(empirical_format_prior(21, [100] * 7), 21)
        self.assertGreater(empirical_format_prior(21, [30] * 8), 21)

    def test_low_value_uncontacted_c_prospect_is_visible_but_not_urgent(self):
        by_id = {r["customer_id"]: r for r in get([
            customer("ICA Maxi A", "a", "A"),
            customer("ICA Nära C", "c", "C"),
        ])}
        self.assertEqual(by_id["c"]["list_guidance"]["status_key"], "idle")
        self.assertEqual(by_id["c"]["contact_priority_score"], 0)
        self.assertGreater(by_id["c"]["business_value_score"], 0)
        self.assertEqual(by_id["a"]["list_guidance"]["status_key"], "act_now")

    def test_stale_explicit_followup_is_flagged_for_review_not_cold_call(self):
        old = contact("a", "Butik", "2026-01-02", "Positiv")
        old["follow_up_date"] = "2026-03-10"
        row = get([customer()], contacts=[old])[0]
        guidance = row["list_guidance"]
        self.assertEqual(guidance["reason_code"], "stale_follow_up_review")
        self.assertEqual(guidance["status_key"], "overdue_followup")
        self.assertFalse(guidance["can_contact_now"])
        self.assertEqual(row["contact_priority_bucket"], 1)
        self.assertEqual(row["contact_priority_score"], 0)
        self.assertGreater(row["business_value_score"], 0)

    def test_recent_unresolved_followup_keeps_real_outbound_priority(self):
        plan = contact("a", "Butik", "2026-09-20", "Positiv")
        plan["follow_up_date"] = "2026-10-08"
        row = get([customer()], contacts=[plan])[0]
        self.assertEqual(row["list_guidance"]["reason_code"],
                         "overdue_explicit_follow_up")
        self.assertTrue(row["list_guidance"]["can_contact_now"])
        self.assertGreater(row["contact_priority_score"], 0)

    def test_old_followup_with_later_order_requires_review_not_autoclose(self):
        followup = contact("a", "Butik", "2026-06-01", "Positiv")
        followup["follow_up_date"] = "2026-08-01"
        row = get([customer()], orders=[
            delivery("a", "Butik", "2026-09-01", 22),
        ], contacts=[followup])[0]
        self.assertEqual(row["list_guidance"]["reason_code"],
                         "stale_follow_up_review")
        self.assertEqual(row["list_guidance"]["status_key"],
                         "overdue_followup")

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
        self.assertEqual(row["list_guidance"]["reason_code"],
                         "established_reorder_due")



    def test_negative_response_after_older_promise_prevents_recontact(self):
        original = contact("a", "Butik", "2026-09-01", "Positiv")
        original["follow_up_date"] = "2026-10-02"
        later_negative = contact("a", "Butik", "2026-10-12", "Negativ")
        row = get([customer()], contacts=[original, later_negative])[0]
        self.assertEqual(row["list_guidance"]["status_key"], "wait")
        self.assertEqual(row["list_guidance"]["reason_code"], "negative_contact_cooldown")
        self.assertEqual(row["contact_priority_score"], 0)

    def test_negative_dialogue_with_new_promised_followup_keeps_commitment(self):
        negative = contact("a", "Butik", "2026-10-01", "Negativ")
        negative["follow_up_date"] = "2026-10-06"
        row = get([customer()], contacts=[negative])[0]
        self.assertEqual(row["list_guidance"]["status_key"], "overdue_followup")
        self.assertEqual(row["list_guidance"]["reason_code"], "overdue_explicit_follow_up")

    def test_ninety_day_explicit_followup_stays_visible_for_review(self):
        followup = contact("a", "Butik", "2026-06-01", "Positiv")
        followup["follow_up_date"] = "2026-07-09"
        row = get([customer()], contacts=[followup])[0]
        self.assertEqual(row["list_guidance"]["status_key"], "overdue_followup")
        self.assertEqual(row["list_guidance"]["reason_code"], "stale_follow_up_review")
        self.assertEqual(row["contact_priority_bucket"], 1)
        self.assertEqual(row["contact_priority_score"], 0)



class ValuePriorityFrontendStage1Tests(TestCase):
    def test_frontend_sort_and_stage2_remains(self):
        html = (Path(__file__).resolve().parents[1] / "index.html").read_text(encoding="utf-8")
        for expected in ['id="list-sort-mode"', 'value="contact">Kontaktprioritet',
                         'value="value">Affärsvärde', 'contactPrioritySort(a, b)',
                         'businessValueSort(a, b)', 'score("contact_priority_score")',
                         'score("business_value_score")', 'id="planning-recommendation"',
                         'id="planning-backlog"']:
            self.assertIn(expected, html)
