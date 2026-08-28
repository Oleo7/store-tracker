"""Deterministic benchmark and coaching rules for sales coaching.

The module only accepts and returns plain data.  It deliberately has no Flask,
Google Sheets, or repository I/O dependencies.
"""

from __future__ import annotations

from copy import deepcopy
import statistics


MIN_SAMPLE = 10
MIN_PEERS = 2
RATE_GAP = 0.10
ACTIVITY_SHARE_GAP = 0.25
CHANNEL_GAP = 0.15
MIN_PRIORITY_COVERAGE = 0.70
FOLLOW_UP_GAP = 0.30
PLANNING_OVERDUE_RATE = 0.20
PLANNING_ON_TIME_RATE = 0.70
MAX_ATTENTION = 2
MAX_STRENGTH = 1
MAX_CARDS = 3

RATE_METRICS = (
    "reach",
    "positive_dialogue",
    "positive_to_order_10d_comparable",
    "order_10d_comparable",
    "bom_ratio",
    "priority_focus",
    "positive_next_step_coverage",
    "planned_completed_in_time",
)
BENCHMARK_METRICS = (*RATE_METRICS, "human_activities_metric")

BUSINESS_IMPACT_WEIGHTS = {
    "closing": 5,
    "follow_up": 4,
    "positive_dialogue": 4,
    "reach": 3,
    "planning": 3,
    "bom": 3,
    "activity": 2,
    "priority": 2,
    "channel": 1,
}


def _metric_value(metric):
    return metric.get("value") if isinstance(metric, dict) else None


def _is_sufficient(metric):
    return (
        isinstance(metric, dict)
        and metric.get("status") == "sufficient"
        and metric.get("value") is not None
    )


def add_seller_benchmarks(current_sellers, previous_sellers):
    """Return copied seller metrics with self-excluding peer comparisons."""
    result = deepcopy(list(current_sellers or ()))
    previous_by_seller = {
        item.get("seller"): item for item in (previous_sellers or ())
    }
    for seller_row in result:
        seller_name = seller_row.get("seller")
        previous_row = previous_by_seller.get(seller_name, {})
        for metric_key in BENCHMARK_METRICS:
            metric = seller_row.get(metric_key)
            if not isinstance(metric, dict):
                continue
            peers = [
                _metric_value(peer.get(metric_key))
                for peer in result
                if peer.get("seller") != seller_name
                and _is_sufficient(peer.get(metric_key))
            ]
            peer_median = statistics.median(peers) if len(peers) >= MIN_PEERS else None
            previous_metric = previous_row.get(metric_key)
            previous_value = (
                _metric_value(previous_metric) if _is_sufficient(previous_metric) else None
            )
            value = _metric_value(metric)
            comparisons = {
                "peer_median": peer_median,
                "peer_count": len(peers),
                "delta_peer": (
                    value - peer_median
                    if value is not None and peer_median is not None else None
                ),
                "previous_period": previous_value,
                "previous_period_status": (
                    previous_metric.get("status")
                    if isinstance(previous_metric, dict) else "not_computable"
                ),
                "delta_previous": (
                    value - previous_value
                    if value is not None and previous_value is not None else None
                ),
            }
            metric["comparisons"] = comparisons
    return result


def _confidence(evidence):
    denominator = evidence.get("denominator") if isinstance(evidence, dict) else 0
    return min(1.0, max(0, denominator or 0) / 30)


def _rank(weight_key, evidence, gap, threshold):
    excess_gap = max(0.0, abs(gap) - threshold)
    return round(
        BUSINESS_IMPACT_WEIGHTS[weight_key] * _confidence(evidence) * excess_gap,
        6,
    )


def _absolute_rank(weight_key, denominator, excess):
    confidence = min(1.0, max(0, denominator or 0) / 30)
    return round(BUSINESS_IMPACT_WEIGHTS[weight_key] * confidence * max(0, excess), 6)


def _benchmark(metric):
    comparisons = deepcopy(metric.get("comparisons") or {})
    peer = comparisons.get("peer_median")
    previous = comparisons.get("previous_period")
    labels = []
    count_metric = metric.get("metric_type") == "count"
    if peer is not None:
        labels.append((
            f"Median övriga säljare {peer:g} aktiviteter · {comparisons.get('delta_peer', 0):+g} aktiviteter"
            if count_metric else
            f"Median övriga säljare {peer:.1%} · {comparisons.get('delta_peer', 0) * 100:+.1f} pp"
        ))
    if previous is not None:
        labels.append((
            f"Föregående period {previous:g} aktiviteter · {comparisons.get('delta_previous', 0):+g} aktiviteter"
            if count_metric else
            f"Föregående period {previous:.1%} · {comparisons.get('delta_previous', 0) * 100:+.1f} pp"
        ))
    return {**comparisons, "label": "\n".join(labels)}


def _signal(*, code, dimension, polarity, metric_key, title, observation,
            evidence, benchmark, next_action, target, drilldown_metric,
            ranking_score, drilldown_filters=None):
    return {
        "code": code,
        "dimension": dimension,
        "polarity": polarity,
        "severity": polarity,
        "metric_key": metric_key,
        "title": title,
        "observation": observation,
        "evidence": deepcopy(evidence),
        "benchmark": deepcopy(benchmark),
        "next_action": next_action,
        # Compatibility for older clients during the contract transition.
        "recommendation": next_action,
        "target": target,
        "drilldown_metric": drilldown_metric,
        "drilldown_filters": deepcopy(drilldown_filters or {}),
        "ranking_score": ranking_score,
    }


def _peer_signal(*, seller, metric_key, metric, dimension, lower_is_better,
                 title_attention, title_strength, action_attention,
                 action_strength, target, drilldown_metric):
    if not _is_sufficient(metric):
        return []
    comparison = metric.get("comparisons") or {}
    peer = comparison.get("peer_median")
    if peer is None or comparison.get("peer_count", 0) < MIN_PEERS:
        return []
    gap = metric["value"] - peer
    attention = gap >= RATE_GAP if lower_is_better else gap <= -RATE_GAP
    strength = gap <= -RATE_GAP if lower_is_better else gap >= RATE_GAP
    if not (attention or strength):
        return []
    polarity = "attention" if attention else "strength"
    direction = "över" if gap > 0 else "under"
    observation = (
        f"{seller} ligger tydligt {direction} övriga säljare för {title_attention.lower()}."
        if attention else
        f"{seller} ligger tydligt {direction} övriga säljare och visar en styrka inom {title_strength.lower()}."
    )
    return [_signal(
        code=f"{metric_key}_{'high' if lower_is_better and attention else 'low' if attention else 'strength'}",
        dimension=dimension,
        polarity=polarity,
        metric_key=metric_key,
        title=title_attention if attention else title_strength,
        observation=observation,
        evidence=metric,
        benchmark=_benchmark(metric),
        next_action=action_attention if attention else action_strength,
        target=target,
        drilldown_metric=drilldown_metric,
        ranking_score=_rank(dimension, metric, gap, RATE_GAP),
    )]


def _select(signals):
    ordered = sorted(
        signals,
        key=lambda item: (
            0 if item["polarity"] == "attention" else 1,
            -item["ranking_score"],
            -(item.get("evidence", {}).get("denominator") or 0),
            item["code"],
        ),
    )
    deduplicated = []
    selected_dimensions = set()
    for item in ordered:
        dimension = item["dimension"]
        if dimension in selected_dimensions:
            continue
        selected_dimensions.add(dimension)
        deduplicated.append(item)
    attention = [
        item for item in deduplicated if item["polarity"] == "attention"
    ]
    strength = [
        item for item in deduplicated if item["polarity"] == "strength"
    ]
    return attention[:MAX_ATTENTION] + strength[:MAX_STRENGTH]


def build_seller_signals(*, seller, metrics, repeat_boms, channel_effectiveness):
    """Build at most three ranked coaching signals for one selected seller."""
    metrics = deepcopy(metrics or {})
    signals = []
    definitions = (
        ("reach", "reach", False, "Träffgraden kan förbättras", "Hög träffgrad", "Granska tidpunkt, kontaktperson och förberedelse för missade försök.", "Identifiera och återanvänd förberedelserna bakom den höga träffgraden.", "Behåll minst 10 analyserbara försök.", "reach"),
        ("positive_dialogue", "positive_dialogue", False, "Färre dialoger blir positiva", "Hög andel positiva dialoger", "Granska behovsfrågor, invändningar och hur nästa steg förankras.", "Identifiera vilka frågor och kundsituationer som driver de starka dialogerna.", "Behåll nivån med minst 30 nådda besök och telefonsamtal.", "positive_dialogue"),
        ("bom_ratio", "bom", True, "Många besök blir bom", "Låg bom-ratio", "Se över besökstid, bokning och rätt kontaktperson.", "Förstå och återanvänd bokningsarbetet bakom den låga bom-ration.", "Behåll minst 10 analyserbara besök.", "bom_ratio"),
    )
    for key, dimension, lower, attention_title, strength_title, attention_action, strength_action, target, drilldown in definitions:
        signals.extend(_peer_signal(
            seller=seller, metric_key=key, metric=metrics.get(key, {}),
            dimension=dimension, lower_is_better=lower,
            title_attention=attention_title, title_strength=strength_title,
            action_attention=attention_action, action_strength=strength_action,
            target=target, drilldown_metric=drilldown,
        ))

    activity = metrics.get("human_activities_metric", {})
    if _is_sufficient(activity):
        comparison = activity.get("comparisons") or {}
        peer = comparison.get("peer_median")
        if peer and comparison.get("peer_count", 0) >= MIN_PEERS:
            relative_gap = (activity["value"] - peer) / peer
            if relative_gap <= -ACTIVITY_SHARE_GAP:
                signals.append(_signal(
                    code="activity_low", dimension="activity", polarity="attention",
                    metric_key="human_activities", title="Aktivitetsnivån är låg",
                    observation=f"{seller} har tydligt färre mänskliga aktiviteter än övriga säljare.",
                    evidence=activity, benchmark=_benchmark(activity),
                    next_action="Utforska vad som begränsar planerad kontakttid.",
                    target="Närma aktivitetsnivån till övriga säljare utan att sänka kvaliteten.",
                    drilldown_metric="human_activities",
                    ranking_score=_rank("activity", activity, relative_gap, ACTIVITY_SHARE_GAP),
                ))

    positive = metrics.get("positive_dialogue", {})
    closing = metrics.get("positive_to_order_10d_comparable", {})
    if _is_sufficient(positive) and _is_sufficient(closing):
        pos_cmp, close_cmp = positive.get("comparisons", {}), closing.get("comparisons", {})
        if (
            pos_cmp.get("peer_median") is not None
            and close_cmp.get("peer_median") is not None
            and pos_cmp.get("peer_count", 0) >= MIN_PEERS
            and close_cmp.get("peer_count", 0) >= MIN_PEERS
            and positive["value"] >= pos_cmp["peer_median"]
            and closing["value"] <= close_cmp["peer_median"] - RATE_GAP
        ):
            gap = closing["value"] - close_cmp["peer_median"]
            signals.append(_signal(
                code="closing_gap", dimension="closing", polarity="attention",
                metric_key="positive_to_order_10d_comparable",
                title="Positiv dialog blir mer sällan order",
                observation=f"{seller} når minst nivån för övriga säljares positiva dialoger men färre positiva dialoger med fullständigt 10-dagarsutfall följs av order.",
                evidence=closing, benchmark=_benchmark(closing),
                next_action="Granska överenskommet nästa steg, erbjudande och uppföljning efter positiva dialoger.",
                target="Minska gapet till övriga säljare med minst 10 procentenheter.",
                drilldown_metric="positive_to_order_10d_comparable",
                ranking_score=_rank("closing", closing, gap, RATE_GAP),
            ))
        elif close_cmp.get("peer_median") is not None and close_cmp.get("peer_count", 0) >= MIN_PEERS and closing["value"] >= close_cmp["peer_median"] + RATE_GAP:
            gap = closing["value"] - close_cmp["peer_median"]
            signals.append(_signal(
                code="positive_to_order_10d_strength", dimension="closing", polarity="strength",
                metric_key="positive_to_order_10d_comparable", title="Stark positiv-till-order-konvertering",
                observation=f"{seller} ligger tydligt över övriga säljare för positiva dialoger med fullständigt 10-dagarsutfall.",
                evidence=closing, benchmark=_benchmark(closing),
                next_action="Identifiera vilka överenskommelser och uppföljningar som driver utfallet.",
                target="Behåll nivån med minst 30 positiva dialoger med fullständigt 10-dagarsutfall.",
                drilldown_metric="positive_to_order_10d_comparable",
                ranking_score=_rank("closing", closing, gap, RATE_GAP),
            ))

    priority = metrics.get("priority_focus", {})
    coverage = metrics.get("priority_percentile_coverage", {})
    if _is_sufficient(priority) and (coverage.get("value") or 0) >= MIN_PRIORITY_COVERAGE:
        signals.extend(_peer_signal(
            seller=seller, metric_key="priority_focus", metric=priority,
            dimension="priority", lower_is_better=False,
            title_attention="Lågt historiskt prioritetsfokus",
            title_strength="Högt historiskt prioritetsfokus",
            action_attention="Granska vilka signaler som styr kundvalen och om mer säljtidskapacitet bör fördelas till den historiska toppkvartilen.",
            action_strength="Förstå och återanvänd kundvalsprocessen bakom prioritetsfokuset.",
            target="Behåll minst 70 % jämförbar historisk täckning.",
            drilldown_metric="priority_focus",
        ))

    follow_up = metrics.get("positive_next_step_coverage", {})
    if _is_sufficient(follow_up):
        missing_rate = 1 - follow_up["value"]
        if follow_up["value"] < 1 - FOLLOW_UP_GAP:
            missing = follow_up["denominator"] - follow_up["numerator"]
            evidence = {"value": missing_rate, "numerator": missing,
                        "denominator": follow_up["denominator"], "status": "sufficient"}
            signals.append(_signal(
                code="followup_gap", dimension="follow_up", polarity="attention",
                metric_key="positive_next_step_coverage", title="Positiva kontakter saknar nästa steg",
                observation="Minst 30 procent av bedömningsbara positiva kontakter saknar order, uppföljningsdatum eller länkad aktivitet efter tre dagar.",
                evidence=evidence, benchmark=_benchmark(follow_up),
                next_action="Bestäm datum, ansvar och syfte för nästa steg redan vid den positiva kontakten.",
                target="Minst 70 % nästa-steg-täckning.", drilldown_metric="followup_gap",
                ranking_score=_absolute_rank("follow_up", evidence["denominator"], missing_rate - FOLLOW_UP_GAP),
            ))
        else:
            comparison = follow_up.get("comparisons") or {}
            peer = comparison.get("peer_median")
            if (
                follow_up["value"] >= 1 - FOLLOW_UP_GAP
                and peer is not None
                and comparison.get("peer_count", 0) >= MIN_PEERS
                and follow_up["value"] >= peer + RATE_GAP
            ):
                gap = follow_up["value"] - peer
                signals.append(_signal(
                    code="positive_next_step_coverage_strength",
                    dimension="follow_up", polarity="strength",
                    metric_key="positive_next_step_coverage",
                    title="Hög nästa-steg-täckning",
                    observation=f"{seller} uppfyller processstandarden och ligger tydligt över övriga säljare i nästa-steg-täckning.",
                    evidence=follow_up, benchmark=_benchmark(follow_up),
                    next_action="Återanvänd rutinen som säkrar tydliga nästa steg.",
                    target="Behåll minst 70 % nästa-steg-täckning.",
                    drilldown_metric="followup_success",
                    ranking_score=_rank("follow_up", follow_up, gap, RATE_GAP),
                ))

    planned = metrics.get("planned_completed_in_time", {})
    overdue = metrics.get("overdue_rate", {})
    if _is_sufficient(planned):
        overdue_value = overdue.get("value")
        planning_attention = (
            planned["value"] < PLANNING_ON_TIME_RATE
            or overdue_value is not None
            and overdue_value >= PLANNING_OVERDUE_RATE
        )
        if planning_attention:
            excess = max(
                PLANNING_ON_TIME_RATE - planned["value"],
                (overdue_value or 0) - PLANNING_OVERDUE_RATE,
            )
            signals.append(_signal(
                code="planning_discipline", dimension="planning", polarity="attention",
                metric_key="planned_completed_in_time", title="Planerade aktiviteter släpar efter",
                observation="Minst tio ansvariga planeringar visar försenade eller sena utfall.",
                evidence=planned, benchmark=_benchmark(planned),
                next_action="Gå igenom gamla planeringar, stäng irrelevanta och omplanera verkliga nästa steg.",
                target="Minst 70 % genomförda i tid och under 20 % försenade.",
                drilldown_metric="planned_overdue",
                ranking_score=_absolute_rank("planning", planned["denominator"], excess),
            ))
        else:
            comparison = planned.get("comparisons") or {}
            peer = comparison.get("peer_median")
            if (
                planned["value"] >= PLANNING_ON_TIME_RATE
                and overdue_value is not None
                and overdue_value < PLANNING_OVERDUE_RATE
                and peer is not None
                and comparison.get("peer_count", 0) >= MIN_PEERS
                and planned["value"] >= peer + RATE_GAP
            ):
                gap = planned["value"] - peer
                signals.append(_signal(
                    code="planned_completed_in_time_strength",
                    dimension="planning", polarity="strength",
                    metric_key="planned_completed_in_time",
                    title="Hög planeringsdisciplin",
                    observation=f"{seller} uppfyller processstandarden och ligger tydligt över övriga säljare i planeringar genomförda i tid.",
                    evidence=planned, benchmark=_benchmark(planned),
                    next_action="Förstå och återanvänd arbetssättet som håller planeringarna i tid.",
                    target="Behåll minst 70 % genomförda i tid och under 20 % försenade.",
                    drilldown_metric="planned_on_time",
                    ranking_score=_rank("planning", planned, gap, RATE_GAP),
                ))

    if (repeat_boms or {}).get("customers", 0) >= 2:
        evidence = {"value": repeat_boms["customers"], "numerator": repeat_boms["customers"],
                    "denominator": repeat_boms.get("visits", 0), "status": "sufficient",
                    "metric_type": "count", "unit": "kunder",
                    "secondary_evidence": {
                        "value": repeat_boms.get("visits", 0),
                        "unit": "bom-besök",
                    }}
        signals.append(_signal(
            code="repeat_boms", dimension="bom", polarity="attention",
            metric_key="repeat_boms", title="Återkommande bommar kräver nytt arbetssätt",
            observation="Minst två kunder har två eller fler bommar under perioden.",
            evidence=evidence, benchmark={},
            next_action="Byt tidpunkt eller kanal och bekräfta kontaktperson före nästa besök.",
            target="Inga återkommande bommönster.", drilldown_metric="repeat_boms",
            ranking_score=_absolute_rank("bom", evidence["denominator"], repeat_boms["customers"] / 2 - 1),
        ))

    channel_metric_key = None
    channel_candidates = []
    for candidate_key in (
        "positive_to_order_10d_comparable", "order_10d_comparable"
    ):
        candidates = [
            (channel, values.get(candidate_key))
            for channel, values in (channel_effectiveness or {}).items()
            if _is_sufficient(values.get(candidate_key))
        ]
        if len(candidates) >= 2:
            channel_metric_key = candidate_key
            channel_candidates = candidates
            break
    if len(channel_candidates) >= 2:
        strongest = max(channel_candidates, key=lambda item: (item[1]["value"], item[0]))
        weakest = min(channel_candidates, key=lambda item: (item[1]["value"], item[0]))
        gap = strongest[1]["value"] - weakest[1]["value"]
        if gap >= CHANNEL_GAP:
            signals.append(_signal(
                code="channel_strength", dimension="channel", polarity="strength",
                metric_key=channel_metric_key, title="En kanal visar ett starkare historiskt mönster",
                observation=f"{strongest[0]} ligger {gap:.1%} högre än {weakest[0]} bland kontakter med fullständigt 10-dagarsutfall; mönstret visar inte kausalitet.",
                evidence=strongest[1], benchmark={"comparison_channel": weakest[0], "comparison_value": weakest[1]["value"]},
                next_action="Identifiera vilka kundsituationer och arbetssätt från den starka kanalen som går att återanvända.",
                target="Bekräfta mönstret över fler analyserbara kontakter.",
                drilldown_metric=channel_metric_key,
                drilldown_filters={"channel": strongest[0]},
                ranking_score=_rank("channel", strongest[1], gap, CHANNEL_GAP),
            ))
    return _select(signals)


def build_team_signals(*, metrics, previous_metrics, repeat_boms):
    """Team rules use only previous team period and absolute process standards."""
    metrics = deepcopy(metrics or {})
    previous_metrics = deepcopy(previous_metrics or {})
    signals = []
    follow_up = metrics.get("positive_next_step_coverage", {})
    if _is_sufficient(follow_up) and follow_up["value"] < 1 - FOLLOW_UP_GAP:
        previous = previous_metrics.get("positive_next_step_coverage", {})
        benchmark = {"previous_period": _metric_value(previous) if _is_sufficient(previous) else None}
        missing = follow_up["denominator"] - follow_up["numerator"]
        evidence = {"value": 1 - follow_up["value"], "numerator": missing,
                    "denominator": follow_up["denominator"], "status": "sufficient"}
        signals.append(_signal(
            code="team_followup_gap", dimension="follow_up", polarity="attention",
            metric_key="positive_next_step_coverage", title="Teamets positiva kontakter saknar nästa steg",
            observation="Teamet ligger under den absoluta processen för nästa-steg-täckning.",
            evidence=evidence, benchmark=benchmark,
            next_action="Säkra datum, ansvar och syfte för nästa steg vid positiva kontakter.",
            target="Minst 70 % nästa-steg-täckning.", drilldown_metric="followup_gap",
            ranking_score=_absolute_rank("follow_up", evidence["denominator"], evidence["value"] - FOLLOW_UP_GAP),
        ))
    planned = metrics.get("planned_completed_in_time", {})
    overdue = metrics.get("overdue_rate", {})
    if _is_sufficient(planned) and (planned["value"] < PLANNING_ON_TIME_RATE or (overdue.get("value") or 0) >= PLANNING_OVERDUE_RATE):
        excess = max(PLANNING_ON_TIME_RATE - planned["value"], (overdue.get("value") or 0) - PLANNING_OVERDUE_RATE)
        signals.append(_signal(
            code="team_planning_discipline", dimension="planning", polarity="attention",
            metric_key="planned_completed_in_time", title="Teamets planeringar släpar efter",
            observation="Teamets ansvariga planeringar klarar inte den absoluta processstandarden.",
            evidence=planned, benchmark={},
            next_action="Rensa gamla planeringar och omplanera verkliga nästa steg.",
            target="Minst 70 % genomförda i tid och under 20 % försenade.",
            drilldown_metric="planned_overdue",
            ranking_score=_absolute_rank("planning", planned["denominator"], excess),
        ))
    if (repeat_boms or {}).get("customers", 0) >= 2:
        evidence = {"value": repeat_boms["customers"], "numerator": repeat_boms["customers"],
                    "denominator": repeat_boms.get("visits", 0), "status": "sufficient",
                    "metric_type": "count", "unit": "kunder",
                    "secondary_evidence": {
                        "value": repeat_boms.get("visits", 0),
                        "unit": "bom-besök",
                    }}
        signals.append(_signal(
            code="team_repeat_boms", dimension="bom", polarity="attention",
            metric_key="repeat_boms", title="Teamet har återkommande bommar",
            observation="Minst två kunder har två eller fler bommar under perioden.",
            evidence=evidence, benchmark={}, next_action="Byt tidpunkt eller kanal före nästa besök.",
            target="Inga återkommande bommönster.", drilldown_metric="repeat_boms",
            ranking_score=_absolute_rank("bom", evidence["denominator"], repeat_boms["customers"] / 2 - 1),
        ))
    return _select(signals)[:MAX_CARDS]
