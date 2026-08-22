from copy import deepcopy
from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

from sales_coaching_rules import (  # noqa: E402
    add_seller_benchmarks,
    build_seller_signals,
    build_team_signals,
)


def rate(value, denominator=30, *, peer=None, peers=2, previous=None,
         status="sufficient"):
    numerator = round((value or 0) * denominator)
    return {
        "value": value,
        "numerator": numerator,
        "denominator": denominator,
        "status": status,
        "comparisons": {
            "peer_median": peer,
            "peer_count": peers,
            "delta_peer": value - peer if value is not None and peer is not None else None,
            "previous_period": previous,
            "previous_period_status": "sufficient" if previous is not None else "small_sample",
            "delta_previous": value - previous if value is not None and previous is not None else None,
        },
    }


def seller_metrics(**overrides):
    result = {
        "reach": rate(None, 0, status="not_computable", peer=None, peers=0),
        "positive_dialogue": rate(None, 0, status="not_computable", peer=None, peers=0),
        "positive_to_order_10d": rate(None, 0, status="not_computable", peer=None, peers=0),
        "order_10d": rate(None, 0, status="not_computable", peer=None, peers=0),
        "bom_ratio": rate(None, 0, status="not_computable", peer=None, peers=0),
        "priority_focus": rate(None, 0, status="not_computable", peer=None, peers=0),
        "priority_percentile_coverage": rate(1, 30, peer=None, peers=0),
        "positive_next_step_coverage": rate(None, 0, status="not_computable", peer=None, peers=0),
        "planned_completed_in_time": rate(None, 0, status="not_computable", peer=None, peers=0),
        "overdue_rate": rate(None, 0, status="not_computable", peer=None, peers=0),
        "human_activities_metric": rate(None, 0, status="not_computable", peer=None, peers=0),
    }
    result.update(overrides)
    return result


class BenchmarkTests(TestCase):
    def test_peer_median_excludes_the_selected_seller_and_previous_is_per_seller(self):
        current = [
            {"seller": "Sofia", "reach": rate(.9)},
            {"seller": "Olle", "reach": rate(.5)},
            {"seller": "Maja", "reach": rate(.7)},
        ]
        previous = [
            {"seller": "Sofia", "reach": rate(.8)},
            {"seller": "Olle", "reach": rate(.4)},
            {"seller": "Maja", "reach": rate(.6)},
        ]
        original = deepcopy(current)

        enriched = add_seller_benchmarks(current, previous)
        sofia = enriched[0]["reach"]["comparisons"]

        self.assertEqual(sofia["peer_median"], .6)
        self.assertEqual(sofia["peer_count"], 2)
        self.assertAlmostEqual(sofia["delta_peer"], .3)
        self.assertEqual(sofia["previous_period"], .8)
        self.assertAlmostEqual(sofia["delta_previous"], .1)
        self.assertEqual(current, original, "benchmarking must not mutate inputs")

    def test_at_least_two_sufficient_peers_are_required(self):
        current = [
            {"seller": "Sofia", "reach": rate(.9)},
            {"seller": "Olle", "reach": rate(.5)},
            {"seller": "Tiny", "reach": rate(.8, 3, status="small_sample")},
        ]

        comparison = add_seller_benchmarks(current, [])[0]["reach"]["comparisons"]

        self.assertIsNone(comparison["peer_median"])
        self.assertEqual(comparison["peer_count"], 1)


class SignalRuleTests(TestCase):
    def signals(self, metrics):
        return build_seller_signals(
            seller="Sofia", metrics=metrics, repeat_boms={},
            channel_effectiveness={},
        )

    def test_closing_gap_uses_positive_to_order_cohort(self):
        signals = self.signals(seller_metrics(
            positive_dialogue=rate(.70, peer=.65),
            positive_to_order_10d=rate(.30, peer=.50),
            order_10d=rate(.95, peer=.20),
        ))

        closing = next(item for item in signals if item["code"] == "closing_gap")
        self.assertEqual(closing["metric_key"], "positive_to_order_10d")
        self.assertEqual(closing["drilldown_metric"], "positive_to_order_10d")

    def test_priority_focus_requires_seventy_percent_v2_coverage_and_no_customer_list(self):
        metrics = seller_metrics(
            priority_focus=rate(.30, peer=.50),
            priority_percentile_coverage=rate(.69),
        )
        self.assertNotIn("priority_focus_low", {item["code"] for item in self.signals(metrics)})

        metrics["priority_percentile_coverage"] = rate(.70)
        signal = next(item for item in self.signals(metrics) if item["code"] == "priority_focus_low")
        self.assertNotIn("priority_gap", repr(signal))
        self.assertNotIn("customer", repr(signal).lower())

    def test_positive_and_low_bom_strengths_use_the_correct_direction(self):
        positive = self.signals(seller_metrics(
            positive_dialogue=rate(.80, peer=.60),
        ))
        bom = self.signals(seller_metrics(
            bom_ratio=rate(.10, peer=.30),
        ))

        self.assertIn("positive_dialogue_strength", {item["code"] for item in positive})
        self.assertIn("bom_ratio_strength", {item["code"] for item in bom})

    def test_absolute_follow_up_rule_does_not_depend_on_peer_level(self):
        signals = self.signals(seller_metrics(
            positive_next_step_coverage=rate(.50, peer=.40),
        ))
        self.assertIn("followup_gap", {item["code"] for item in signals})

    def test_small_samples_create_no_positive_or_negative_signal(self):
        metrics = seller_metrics(
            reach=rate(.01, 5, peer=.9, status="small_sample"),
            positive_dialogue=rate(.99, 5, peer=.1, status="small_sample"),
            bom_ratio=rate(.01, 5, peer=.9, status="small_sample"),
        )
        self.assertEqual(self.signals(metrics), [])

    def test_selection_is_deterministic_with_two_attention_and_one_strength(self):
        metrics = seller_metrics(
            reach=rate(.20, peer=.60),
            positive_dialogue=rate(.20, peer=.60),
            bom_ratio=rate(.70, peer=.20),
            planned_completed_in_time=rate(.95, peer=.70),
            overdue_rate=rate(.05, peer=.20),
        )
        first = self.signals(metrics)
        second = self.signals(metrics)

        self.assertEqual(first, second)
        self.assertLessEqual(len(first), 3)
        self.assertLessEqual(sum(item["polarity"] == "attention" for item in first), 2)
        self.assertLessEqual(sum(item["polarity"] == "strength" for item in first), 1)

    def test_team_mode_uses_separate_absolute_cards_without_peer_benchmark(self):
        cards = build_team_signals(
            metrics={
                "positive_next_step_coverage": rate(.40),
                "planned_completed_in_time": rate(.90),
                "overdue_rate": rate(.05),
            },
            previous_metrics={}, repeat_boms={},
        )

        self.assertEqual(cards[0]["code"], "team_followup_gap")
        self.assertNotIn("peer_median", cards[0]["benchmark"])


if __name__ == "__main__":
    main()
