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
         status="sufficient", waiting=None):
    numerator = round((value or 0) * denominator)
    result = {
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
    if waiting is not None:
        result["waiting_outcome_count"] = waiting
    return result


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

    def test_live_previous_is_suppressed_for_current_or_previous_pending_but_peer_remains(self):
        for metric_key in ("order_10d", "positive_to_order_10d"):
            for current_waiting, previous_waiting in ((2, 0), (0, 3)):
                with self.subTest(
                    metric_key=metric_key,
                    current_waiting=current_waiting,
                    previous_waiting=previous_waiting,
                ):
                    current = [
                        {"seller": "Sofia", metric_key: rate(.4, waiting=current_waiting)},
                        {"seller": "Olle", metric_key: rate(.5, waiting=0)},
                        {"seller": "Maja", metric_key: rate(.7, waiting=0)},
                    ]
                    previous = [
                        {"seller": "Sofia", metric_key: rate(.3, waiting=previous_waiting)},
                    ]

                    comparison = add_seller_benchmarks(
                        current, previous
                    )[0][metric_key]["comparisons"]

                    self.assertEqual(comparison["peer_median"], .6)
                    self.assertEqual(comparison["peer_count"], 2)
                    self.assertIsNone(comparison["previous_period"])
                    self.assertIsNone(comparison["delta_previous"])
                    self.assertEqual(
                        comparison["previous_period_suppressed_reason"],
                        "pending_10d_outcomes",
                    )

    def test_complete_live_periods_and_other_metrics_keep_previous_comparisons(self):
        for metric_key in (
            "order_10d", "positive_to_order_10d", "reach",
            "positive_dialogue", "bom_ratio", "positive_next_step_coverage",
            "planned_completed_in_time",
        ):
            with self.subTest(metric_key=metric_key):
                current_metric = rate(.6, waiting=0) if metric_key != "reach" else rate(.6)
                previous_metric = rate(.4, waiting=0) if metric_key != "reach" else rate(.4)
                comparison = add_seller_benchmarks(
                    [{"seller": "Sofia", metric_key: current_metric}],
                    [{"seller": "Sofia", metric_key: previous_metric}],
                )[0][metric_key]["comparisons"]

                self.assertEqual(comparison["previous_period"], .4)
                self.assertAlmostEqual(comparison["delta_previous"], .2)
                self.assertNotIn("previous_period_suppressed_reason", comparison)


class SignalRuleTests(TestCase):
    def signals(self, metrics):
        return build_seller_signals(
            seller="Sofia", metrics=metrics, repeat_boms={},
            channel_effectiveness={},
        )

    def test_closing_gap_uses_live_positive_to_order_cohort(self):
        signals = self.signals(seller_metrics(
            positive_dialogue=rate(.70, peer=.65),
            positive_to_order_10d=rate(.30, peer=.50, waiting=4),
            positive_to_order_10d_comparable=rate(.95, peer=.20),
        ))

        closing = next(item for item in signals if item["code"] == "closing_gap")
        self.assertEqual(closing["metric_key"], "positive_to_order_10d")
        self.assertEqual(closing["drilldown_metric"], "positive_to_order_10d")
        self.assertIn("hittills följts av order inom 10 dagar", closing["observation"])
        self.assertNotIn("fullständigt", closing["observation"])
        self.assertEqual(closing["evidence"]["waiting_outcome_count"], 4)

    def test_closing_strength_keeps_pending_evidence(self):
        signals = self.signals(seller_metrics(
            positive_dialogue=rate(.70, peer=.65),
            positive_to_order_10d=rate(.70, peer=.50, waiting=2),
        ))

        strength = next(
            item for item in signals
            if item["code"] == "positive_to_order_10d_strength"
        )
        self.assertEqual(strength["evidence"]["waiting_outcome_count"], 2)

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
        follow_up = next(item for item in signals if item["code"] == "followup_gap")
        self.assertEqual(follow_up["title"], "Positiva kontakter saknar nästa steg")
        self.assertNotIn("positiva dialoger", follow_up["next_action"].casefold())

    def test_follow_up_below_absolute_standard_cannot_be_peer_strength(self):
        signals = self.signals(seller_metrics(
            positive_next_step_coverage=rate(.55, peer=.40),
        ))
        follow_up = [item for item in signals if item["dimension"] == "follow_up"]

        self.assertEqual([item["code"] for item in follow_up], ["followup_gap"])
        self.assertTrue(all(item["polarity"] != "strength" for item in follow_up))

    def test_follow_up_strength_requires_absolute_standard(self):
        signals = self.signals(seller_metrics(
            positive_next_step_coverage=rate(.80, peer=.60),
        ))
        self.assertIn(
            "positive_next_step_coverage_strength",
            {item["code"] for item in signals},
        )

    def test_follow_up_at_absolute_boundary_is_not_attention(self):
        signals = self.signals(seller_metrics(
            positive_next_step_coverage=rate(.70, peer=.55),
        ))
        follow_up = [item for item in signals if item["dimension"] == "follow_up"]

        self.assertEqual(
            [item["code"] for item in follow_up],
            ["positive_next_step_coverage_strength"],
        )

    def test_activity_benchmark_labels_counts_as_activities(self):
        metric = rate(5, denominator=20, peer=10, previous=7)
        metric["metric_type"] = "count"
        metric["unit"] = "aktiviteter"
        signals = self.signals(seller_metrics(human_activities_metric=metric))
        activity = next(item for item in signals if item["code"] == "activity_low")

        self.assertEqual(activity["evidence"]["metric_type"], "count")
        self.assertEqual(activity["evidence"]["unit"], "aktiviteter")
        self.assertIn("10 aktiviteter", activity["benchmark"]["label"])
        self.assertIn("Median övriga säljare", activity["benchmark"]["label"])
        self.assertNotIn("Peer median", activity["benchmark"]["label"])
        self.assertIn("-", activity["benchmark"]["label"])
        self.assertNotIn("%", activity["benchmark"]["label"])
        self.assertNotIn("pp", activity["benchmark"]["label"])

    def test_rate_benchmark_uses_self_excluding_swedish_label(self):
        signal = self.signals(seller_metrics(
            positive_dialogue=rate(.80, peer=.60),
        ))[0]

        self.assertIn("Median övriga säljare 60.0%", signal["benchmark"]["label"])
        self.assertNotIn("Peer median", signal["benchmark"]["label"])

    def test_planning_below_absolute_standard_cannot_be_peer_strength(self):
        signals = self.signals(seller_metrics(
            planned_completed_in_time=rate(.55, peer=.40),
            overdue_rate=rate(.10, peer=.20),
        ))
        planning = [item for item in signals if item["dimension"] == "planning"]

        self.assertEqual([item["code"] for item in planning], ["planning_discipline"])
        self.assertTrue(all(item["polarity"] != "strength" for item in planning))

    def test_planning_strength_requires_both_absolute_standards(self):
        signals = self.signals(seller_metrics(
            planned_completed_in_time=rate(.80, peer=.60),
            overdue_rate=rate(.10, peer=.20),
        ))
        self.assertIn(
            "planned_completed_in_time_strength",
            {item["code"] for item in signals},
        )

    def test_channel_comparison_uses_one_metric_for_every_channel(self):
        metrics = seller_metrics()
        channels = {
            "visit": {
                "positive_to_order_10d": rate(.80, waiting=3),
                "order_10d": rate(.20),
                "positive_to_order_10d_comparable": rate(.05),
            },
            "phone": {
                "positive_to_order_10d": rate(.10, 5, status="small_sample"),
                "order_10d": rate(.10),
                "positive_to_order_10d_comparable": rate(.95),
            },
            "email": {
                "positive_to_order_10d": rate(.50),
                "order_10d": rate(.90),
            },
        }

        cards = build_seller_signals(
            seller="Sofia", metrics=metrics, repeat_boms={},
            channel_effectiveness=channels,
        )
        channel = next(item for item in cards if item["code"] == "channel_strength")

        self.assertEqual(channel["metric_key"], "positive_to_order_10d")
        self.assertEqual(channel["drilldown_metric"], "positive_to_order_10d")
        self.assertEqual(channel["drilldown_filters"], {"channel": "visit"})
        self.assertIn("Utfallet är preliminärt", channel["observation"])
        self.assertEqual(channel["evidence"]["waiting_outcome_count"], 3)

    def test_channel_comparison_falls_back_to_order_metric_as_a_whole(self):
        metrics = seller_metrics()
        channels = {
            "visit": {
                "positive_to_order_10d": rate(.80, 5, status="small_sample"),
                "order_10d": rate(.80),
            },
            "phone": {
                "positive_to_order_10d": rate(.10, 5, status="small_sample"),
                "order_10d": rate(.40),
            },
        }

        cards = build_seller_signals(
            seller="Sofia", metrics=metrics, repeat_boms={},
            channel_effectiveness=channels,
        )
        channel = next(item for item in cards if item["code"] == "channel_strength")

        self.assertEqual(channel["metric_key"], "order_10d")
        self.assertEqual(channel["drilldown_metric"], "order_10d")

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
        self.assertEqual(
            [item["code"] for item in first],
            [
                "bom_ratio_high",
                "positive_dialogue_low",
                "planned_completed_in_time_strength",
            ],
        )
        self.assertLessEqual(len(first), 3)
        self.assertLessEqual(sum(item["polarity"] == "attention" for item in first), 2)
        self.assertLessEqual(sum(item["polarity"] == "strength" for item in first), 1)

    def test_multiple_strengths_without_attention_return_at_most_one(self):
        signals = self.signals(seller_metrics(
            reach=rate(.80, peer=.60),
            positive_dialogue=rate(.85, peer=.60),
            bom_ratio=rate(.10, peer=.30),
        ))

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["polarity"], "strength")
        self.assertEqual(signals[0]["code"], "positive_dialogue_strength")

    def test_repeat_boms_attention_wins_over_bom_ratio_strength(self):
        signals = build_seller_signals(
            seller="Sofia",
            metrics=seller_metrics(bom_ratio=rate(.10, peer=.30)),
            repeat_boms={"customers": 3, "visits": 7},
            channel_effectiveness={},
        )
        bom = [item for item in signals if item["dimension"] == "bom"]

        self.assertEqual([item["code"] for item in bom], ["repeat_boms"])
        self.assertEqual(bom[0]["evidence"]["metric_type"], "count")
        self.assertEqual(bom[0]["evidence"]["unit"], "kunder")
        self.assertEqual(
            bom[0]["evidence"]["secondary_evidence"],
            {"value": 7, "unit": "bom-besök"},
        )

    def test_highest_ranked_attention_wins_within_bom_dimension(self):
        signals = build_seller_signals(
            seller="Sofia",
            metrics=seller_metrics(bom_ratio=rate(.80, peer=.20)),
            repeat_boms={"customers": 2, "visits": 4},
            channel_effectiveness={},
        )

        self.assertEqual(
            [item["code"] for item in signals if item["dimension"] == "bom"],
            ["bom_ratio_high"],
        )

    def test_team_repeat_boms_uses_count_evidence(self):
        cards = build_team_signals(
            metrics={}, previous_metrics={},
            repeat_boms={"customers": 2, "visits": 5},
        )
        repeat = next(item for item in cards if item["code"] == "team_repeat_boms")

        self.assertEqual(repeat["evidence"]["metric_type"], "count")
        self.assertEqual(repeat["evidence"]["unit"], "kunder")
        self.assertEqual(
            repeat["evidence"]["secondary_evidence"],
            {"value": 5, "unit": "bom-besök"},
        )

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
        self.assertEqual(cards[0]["title"], "Teamets positiva kontakter saknar nästa steg")
        self.assertNotIn("positiva dialoger", cards[0]["next_action"].casefold())
        self.assertNotIn("peer_median", cards[0]["benchmark"])


if __name__ == "__main__":
    main()
