from datetime import date, datetime, timedelta
from pathlib import Path
import sys
from unittest import TestCase


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from priority import (  # noqa: E402
    apply_workflow_suppressions,
    build_contact_features,
    build_order_features,
    build_priority_customers,
)


TODAY = date(2026, 8, 10)
NOW = datetime(2026, 8, 10, 12, 0)


def customer(*, customer_id="store-1", name="Butik", segment="C", owner="Olle"):
    return {
        "row": 2,
        "customer_id": customer_id,
        "customer_number": "100",
        "customer": name,
        "sales_person": owner,
        "customer_segment": segment,
        "phone": "0701234567",
    }


def order(reference, delivered, *, ordered=None, dfp=40, customer_id="store-1"):
    return {
        "Reference": reference,
        "Order date": (ordered or delivered).isoformat(),
        "Delivery date": delivered.isoformat(),
        "customer_id": customer_id,
        "Customer number": "100",
        "Customer": "Butik",
        "Quantity": str(dfp),
        "Total weight": str(dfp),
        "Total": "1000",
    }


def contact(when, *, result="Neutral", follow_up="", contact_id="contact-1"):
    return {
        "contact_id": contact_id,
        "customer_id": "store-1",
        "customer": "Butik",
        "date_time": when.isoformat(sep=" ", timespec="minutes"),
        "sales_person": "Olle",
        "contact_channel": "Telefon",
        "result": result,
        "follow_up_date": follow_up,
    }


def scored(*, customers=None, orders=(), contacts=(), planned=(), today=TODAY,
           now=NOW, scoring_version="v2.2"):
    customers = customers or [customer()]
    order_features = build_order_features(list(orders))
    return build_priority_customers(
        customers,
        order_features,
        build_contact_features(list(contacts), order_features),
        None,
        today,
        limit=len(customers),
        planned_activities=list(planned),
        scoring_version=scoring_version,
        now=now,
    )


class CustomerGuidanceTests(TestCase):
    def guidance(self, **kwargs):
        return scored(**kwargs)[0]["customer_guidance"]

    def test_torp_first_delivery_fixture_is_second_purchase_and_score_is_unchanged(self):
        torp = customer(name="Stora Coop Torp", segment="A")
        torp_order = order("TORP-1", TODAY - timedelta(days=24), dfp=40)
        item = scored(
            customers=[torp],
            orders=[torp_order],
            contacts=[contact(NOW - timedelta(days=14))],
        )[0]

        self.assertEqual(item["priority_score"], 82)
        self.assertEqual(item["customer_guidance"]["focus_key"], "second_purchase")
        self.assertEqual(item["customer_guidance"]["status_key"], "act_now")
        self.assertEqual(
            item["customer_guidance"]["action_label"], "Säkra återköpet"
        )
        self.assertEqual(item["primary_trigger_type"], "first_order_reorder")
        self.assertEqual(
            scored(
                customers=[torp], orders=[torp_order],
                contacts=[contact(NOW - timedelta(days=14))],
                scoring_version="v2.1",
            )[0]["customer_guidance"]["action_label"],
            "Säkra återköpet",
        )

    def test_repeat_purchase_focus_survives_planning_and_statistical_overdue(self):
        deliveries = [
            order(str(index), TODAY - timedelta(days=70 - (index * 10)), dfp=22)
            for index in range(8)
        ]
        future = {
            "planned_activity_id": "next-contact",
            "customer_id": "store-1",
            "sales_person": "Olle",
            "contact_type": "phone",
            "scheduled_at": "2026-08-10 15:00",
            "status": "planned",
        }
        planned_item = scored(orders=deliveries, planned=[future])[0]
        self.assertEqual(planned_item["delivery_count"], 8)
        self.assertEqual(
            planned_item["customer_guidance"]["focus_key"], "repeat_purchase"
        )
        self.assertEqual(planned_item["customer_guidance"]["status_key"], "planned")

        due_item = scored(
            orders=deliveries,
            today=TODAY + timedelta(days=15),
            now=NOW + timedelta(days=15),
        )[0]
        self.assertEqual(due_item["customer_guidance"]["focus_key"], "repeat_purchase")
        self.assertEqual(due_item["customer_guidance"]["status_key"], "act_now")
        self.assertNotEqual(
            due_item["customer_guidance"]["status_key"], "overdue_followup"
        )

    def test_reactivation_keeps_existing_boundary(self):
        one_delivery = [order("old", TODAY - timedelta(days=90))]
        at_boundary = scored(orders=one_delivery)[0]
        after_boundary = scored(
            orders=one_delivery,
            today=TODAY + timedelta(days=1),
            now=NOW + timedelta(days=1),
        )[0]
        self.assertEqual(
            at_boundary["customer_guidance"]["focus_key"], "second_purchase"
        )
        self.assertEqual(
            after_boundary["customer_guidance"]["focus_key"], "reactivation"
        )

    def test_a_prospect_is_persistent_without_score_or_45_day_gate(self):
        strategic = customer(segment="A")
        neutral = contact(NOW - timedelta(days=12))
        item = scored(customers=[strategic], contacts=[neutral])[0]
        self.assertLess(item["priority_score"], 70)
        self.assertEqual(item["primary_trigger_type"], "a_prospect_due")
        self.assertEqual(item["customer_guidance"]["focus_key"], "a_prospect")
        self.assertEqual(item["customer_guidance"]["status_key"], "act_now")

        positive = scored(
            customers=[strategic],
            contacts=[contact(NOW - timedelta(days=12), result="Positiv")],
        )[0]
        self.assertEqual(positive["customer_guidance"]["focus_key"], "a_prospect")
        self.assertEqual(positive["primary_trigger_type"], "a_prospect_due")

    def test_a_prospect_context_ignores_contact_churn_but_not_order(self):
        base = scored(customers=[customer(segment="A")])[0]
        contacted = scored(
            customers=[customer(segment="A")],
            contacts=[contact(NOW - timedelta(days=12), contact_id="new-contact")],
        )[0]
        first_hash = app_module.priority_decision_context_hash(base, "olle")
        second_hash = app_module.priority_decision_context_hash(contacted, "olle")
        self.assertEqual(first_hash, second_hash)

        with_order = scored(
            customers=[customer(segment="A")],
            orders=[order("future", TODAY + timedelta(days=5), ordered=TODAY)],
        )[0]
        self.assertNotEqual(
            first_hash, app_module.priority_decision_context_hash(with_order, "olle")
        )

    def test_a_prospect_context_changes_when_strategic_segment_changes(self):
        strategic = scored(customers=[customer(segment="A")])[0]
        ordinary = scored(customers=[customer(segment="B")])[0]
        self.assertNotEqual(
            app_module.priority_decision_context_hash(strategic, "olle"),
            app_module.priority_decision_context_hash(ordinary, "olle"),
        )

    def test_first_future_order_removes_a_prospect_and_waits_for_delivery(self):
        rows = [
            order("future-a", TODAY + timedelta(days=5), ordered=TODAY),
            order("future-b", TODAY + timedelta(days=5), ordered=TODAY),
        ]
        item = scored(customers=[customer(segment="A")], orders=rows)[0]
        self.assertEqual(item["delivery_count"], 0)
        self.assertEqual(item["customer_guidance"]["focus_key"], "second_purchase")
        self.assertEqual(item["customer_guidance"]["status_key"], "wait")
        self.assertEqual(item["customer_guidance"]["reason_code"], "future_delivery")
        self.assertEqual(item["latest_delivery_date"], "")
        self.assertEqual(item["next_delivery_date"], "2026-08-15")

    def test_next_delivery_is_nearest_future_delivery(self):
        item = scored(
            customers=[customer(segment="A")],
            orders=[
                order("later", TODAY + timedelta(days=12), ordered=TODAY),
                order("nearer", TODAY + timedelta(days=5), ordered=TODAY),
            ],
        )[0]
        self.assertEqual(item["next_delivery_date"], "2026-08-15")
        self.assertEqual(item["latest_delivery_date"], "")

    def test_future_plan_replaces_overdue_status_for_manual_and_suggestion_sources(self):
        missed = {
            "planned_activity_id": "missed",
            "customer_id": "store-1",
            "sales_person": "Olle",
            "contact_type": "visit",
            "scheduled_at": "2026-08-09 09:00",
            "status": "planned",
        }
        for source in ("manual", "system_suggestion"):
            with self.subTest(source=source):
                future = {
                    **missed,
                    "planned_activity_id": f"future-{source}",
                    "scheduled_at": "2026-08-11 09:00",
                    "source": source,
                    "source_suggestion_id": "suggestion-1" if source == "system_suggestion" else "",
                }
                item = scored(planned=[missed, future])[0]
                guidance = item["customer_guidance"]
                self.assertEqual(guidance["status_key"], "planned")
                self.assertEqual(guidance["planned_activity_id"], f"future-{source}")
                self.assertEqual(guidance["overdue_activity_id"], "")

        overdue = scored(planned=[missed])[0]["customer_guidance"]
        self.assertEqual(overdue["status_key"], "overdue_followup")
        self.assertEqual(overdue["overdue_activity_id"], "missed")

    def test_date_only_followup_is_due_today_and_overdue_tomorrow(self):
        row = contact(NOW - timedelta(days=5), follow_up=TODAY.isoformat())
        today_guidance = self.guidance(contacts=[row])
        tomorrow_guidance = self.guidance(
            contacts=[row],
            today=TODAY + timedelta(days=1),
            now=NOW + timedelta(days=1),
        )
        self.assertEqual(today_guidance["status_key"], "act_now")
        self.assertEqual(tomorrow_guidance["status_key"], "overdue_followup")

    def test_workflow_snooze_never_hides_real_overdue_followup(self):
        missed = {
            "planned_activity_id": "missed",
            "customer_id": "store-1",
            "sales_person": "Olle",
            "contact_type": "phone",
            "scheduled_at": "2026-08-09 09:00",
            "status": "planned",
        }
        base = scored(customers=[customer(segment="A")], planned=[missed])[0]
        updated = apply_workflow_suppressions(
            [base], {"store-1": "snoozed"}
        )[0]
        self.assertEqual(
            updated["customer_guidance"]["status_key"], "overdue_followup"
        )
        self.assertTrue(updated["customer_guidance"]["can_contact_now"])

    def test_non_actionable_statuses_do_not_recommend_contact_channel(self):
        waiting = self.guidance(
            customers=[customer(segment="A")],
            contacts=[contact(NOW - timedelta(days=1))],
        )
        idle = self.guidance(customers=[customer(segment="C")])
        self.assertEqual(waiting["status_key"], "wait")
        self.assertEqual(waiting["recommended_contact_type"], "")
        self.assertEqual(idle["status_key"], "idle")
        self.assertEqual(idle["recommended_contact_type"], "")

    def test_terminal_activities_do_not_leave_green_status(self):
        for status in ("completed", "cancelled", "skipped"):
            with self.subTest(status=status):
                item = self.guidance(planned=[{
                    "planned_activity_id": status,
                    "customer_id": "store-1",
                    "sales_person": "Olle",
                    "contact_type": "phone",
                    "scheduled_at": "2026-08-11 09:00",
                    "status": status,
                }])
                self.assertNotEqual(item["status_key"], "planned")

    def test_workflow_pause_changes_guidance_without_changing_score(self):
        base = scored(customers=[customer(segment="A")])[0]
        for reason, expected_text in (
            ("snoozed", "snoozad"),
            ("dismissed", "Tidigare förslag dolt"),
        ):
            updated = apply_workflow_suppressions([base], {"store-1": reason})[0]
            self.assertEqual(updated["priority_score"], base["priority_score"])
            self.assertEqual(updated["customer_guidance"]["status_key"], "wait")
            self.assertIn(expected_text, updated["customer_guidance"]["reason_text"])

    def test_contract_is_complete_and_planning_does_not_change_score(self):
        future = {
            "planned_activity_id": "future",
            "customer_id": "store-1",
            "sales_person": "Olle",
            "contact_type": "phone",
            "scheduled_at": "2026-08-11 09:00",
            "status": "planned",
        }
        baseline = scored(customers=[customer(segment="A")])[0]
        planned = scored(customers=[customer(segment="A")], planned=[future])[0]
        self.assertEqual(planned["priority_score"], baseline["priority_score"])
        self.assertEqual(
            set(planned["customer_guidance"]),
            {
                "focus_key", "focus_label", "status_key", "status_label",
                "action_key", "action_label", "reason_code", "reason_text",
                "recommended_contact_type", "next_contact_at", "waiting_until",
                "overdue_activity_id", "planned_activity_id", "can_contact_now",
                "guidance_version",
            },
        )
