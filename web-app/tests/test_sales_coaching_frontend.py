from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402
from sales_coaching import MAIN_KPI_KEYS, METRIC_DEFINITIONS  # noqa: E402


class SalesCoachingFrontendTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (WEB_APP_DIR / "static" / "sales_coaching.js").read_text(encoding="utf-8")
        cls.css = (WEB_APP_DIR / "static" / "sales_coaching.css").read_text(encoding="utf-8")
        cls.sales_coaching_source = (WEB_APP_DIR / "sales_coaching.py").read_text(encoding="utf-8")

    def test_index_integration_is_small_and_admin_controlled(self):
        self.assertIn('href="/static/sales_coaching.css"', self.html)
        self.assertIn('src="/static/sales_coaching.js"', self.html)
        self.assertIn('id="business-overview-tab"', self.html)
        self.assertIn('id="sales-coaching-tab"', self.html)
        self.assertIn('id="sales-coaching-dashboard"', self.html)
        self.assertIn("window.salesCoachingDashboard?.setAdmin(userIsAdmin())", self.html)
        self.assertIn(".sales-coaching-admin { display: none; }", self.css)
        self.assertIn("body.user-admin .sales-coaching-dashboard:not([hidden])", self.css)

    def test_dashboard_has_all_specified_sections_and_filters(self):
        for expected in (
            "Period", "Säljare", "Kanal", "Lifecycle", "Kundsegment",
            "Coachningsöversikt", "Coachningskort", "Teamjämförelse",
            "10-dagarskonvertering – trend",
            "Teamets prioriteringsmatris", "Aktivitetstratt", "10-dagarsutfall",
            "Konvertering", "Besök", "Kanaler", "Uppföljning", "Prioritering",
            "Datakvalitet och definitioner",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn('id="sc-comparison"', self.javascript)

    def test_exactly_five_main_kpis_are_rendered_in_requested_order(self):
        self.assertIn(
            'const order = ["human_activities", "reach", "positive_dialogue", "positive_to_order_10d", "order_10d"]',
            self.javascript,
        )
        self.assertEqual(
            MAIN_KPI_KEYS,
            (
                "human_activities", "reach", "positive_dialogue",
                "positive_to_order_10d", "order_10d",
            ),
        )
        self.assertIn("grid-template-columns: repeat(5, minmax(0, 1fr))", self.css)

    def test_main_kpis_explain_their_denominators_in_plain_swedish(self):
        self.assertEqual(
            METRIC_DEFINITIONS["reach"]["denominator_label"],
            "analyserbara besök/telefonsamtal",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_dialogue"]["denominator_label"],
            "nådda besök/telefonsamtal",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_to_order_10d"]["denominator_label"],
            "positiva dialoger har följts av order",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["order_10d"]["denominator_label"],
            "kontakter har följts av order",
        )
        self.assertIn("definition.denominator_label", self.javascript)
        self.assertIn("sc-kpi-denominator", self.javascript)
        self.assertIn("flex-wrap: wrap", self.css)

    def test_all_main_kpis_have_clickable_plain_language_explanations(self):
        for key in MAIN_KPI_KEYS:
            with self.subTest(key=key):
                self.assertTrue(METRIC_DEFINITIONS[key]["definition"])
                self.assertNotIn(
                    METRIC_DEFINITIONS[key]["definition"], self.javascript,
                    "definition copy must not be duplicated in the frontend",
                )
        self.assertNotIn("KPI_PRESENTATION", self.javascript)
        self.assertIn("state.data?.metric_definitions?.[key]", self.javascript)
        self.assertIn('data-sc-action="metric-info"', self.javascript)
        self.assertIn('aria-expanded="false"', self.javascript)
        self.assertIn("explanation.hidden = expanded", self.javascript)

    def test_live_10_day_definitions_explain_pending_comparisons(self):
        expected = {
            "positive_to_order_10d": "Andelen av alla berättigade positiva besök och telefonsamtal med säker kundidentitet i vald period som hittills har följts av en attribuerad order inom 0–10 dagar. Kontakter vars 10-dagarsfönster fortfarande är öppet ingår i nämnaren, därför är måttet preliminärt. Samma definition används i Coachningsöversikt, Teamjämförelse och övriga jämförelser. Jämförelser mellan säljare kan förändras medan utfall fortfarande väntar. Jämförelse med föregående period visas först när båda perioderna saknar väntande 10-dagarsutfall.",
            "order_10d": "Andelen av alla berättigade nådda mänskliga kontakter med säker kundidentitet i vald period som hittills har följts av en attribuerad order inom 0–10 dagar. Kontakter vars 10-dagarsfönster fortfarande är öppet ingår i nämnaren, därför är måttet preliminärt. Samma definition används i Coachningsöversikt, Teamjämförelse och övriga jämförelser. Jämförelser mellan säljare kan förändras medan utfall fortfarande väntar. Jämförelse med föregående period visas först när båda perioderna saknar väntande 10-dagarsutfall.",
        }

        for metric_key, definition in expected.items():
            with self.subTest(metric_key=metric_key):
                self.assertEqual(
                    METRIC_DEFINITIONS[metric_key]["definition"], definition
                )

    def test_frontend_uses_backend_rate_contract_without_recalculating_kpis(self):
        self.assertIn("percent(metric.value)", self.javascript)
        self.assertIn("rateEvidence(metric)", self.javascript)
        self.assertIn("metricDefinition(key, metric)", self.javascript)
        self.assertNotIn("attributed_orders /", self.javascript)
        self.assertNotIn("priority_percentile_at_contact >=", self.javascript)

    def test_main_kpi_labels_and_sample_status_are_plain_and_selective(self):
        self.assertEqual(METRIC_DEFINITIONS["human_activities"]["label"], "Aktiviteter")
        self.assertEqual(
            METRIC_DEFINITIONS["order_10d"]["label"],
            "Kontakt – order inom 10 dagar",
        )
        kpi_markup = self.javascript.split("function kpiMarkup", 1)[1].split(
            "function kpisMarkup", 1
        )[0]
        self.assertIn('metric.status === "small_sample"', kpi_markup)
        self.assertIn("Inte tillräckligt underlag", kpi_markup)
        self.assertNotIn("statusLabel(metric.status)", kpi_markup.split("const status", 1)[1])
        self.assertNotIn("Tillräckligt underlag</span>", kpi_markup)

    def test_matrix_uses_generic_backend_axes_and_does_not_invent_medians(self):
        self.assertIn("Otillräckligt jämförbart underlag", self.javascript)
        self.assertNotIn("matrix.medians?.priority_focus ?? 0.5", self.javascript)
        self.assertNotIn("matrix.medians?.order_10d ?? 0.5", self.javascript)
        self.assertIn("matrix.axes?.x?.key", self.javascript)
        self.assertIn("matrix.axes?.y?.key", self.javascript)
        self.assertNotIn('const xKey = sales ?', self.javascript)
        for metric in (
            "followup_success", "followup_gap", "followup_gap_10d",
            "planned_on_time", "planned_overdue", "planned_skipped",
        ):
            with self.subTest(metric=metric):
                self.assertIn(f'"{metric}"', self.javascript)

    def test_compact_data_quality_is_folded_and_separates_history(self):
        self.assertIn("order_attribution_identity_coverage", self.javascript)
        self.assertIn("core_flagged_activity_rows", self.javascript)
        self.assertIn("quality_issue_count", self.javascript)
        self.assertIn('details id="sc-quality-details"', self.javascript)
        self.assertIn("Ett operativt undantag är inte i sig ett kvalitetsfel", self.javascript)
        self.assertIn("Datakvalitet och täckning", self.javascript)
        self.assertIn("Ordlista över mått", self.javascript)
        self.assertNotIn("sc-quality-debug", self.javascript)

    def test_team_comparison_has_three_activity_bars_and_exact_table(self):
        for expected in (
            "teamComparisonMarkup", "human_activities_total",
            "visit_breakdown?.analysable", "visit_breakdown?.reached",
            "visit_breakdown?.boms", "channel_mix?.phone", "channel_mix?.email",
            "positive_to_order_10d", "positive_next_step_coverage",
            "is-visit-stack", "is-visit-reached", "is-visit-bom",
            "is-email", "sc-comparison-table", "Nästa-steg-täckning",
            "Kontakt – order inom 10 dagar",
            "waiting_positive_dialogues_count", "waiting_outcome_count",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn("item.attributed_orders /", self.javascript)
        self.assertNotIn('class="sc-team-secondary"', self.javascript)
        self.assertIn(
            "${rateCell(item.positive_to_order_10d, item.waiting_positive_dialogues_count)}</td><td>${rateCell(item.order_10d, item.waiting_outcome_count)}</td><td>${rateCell(item.positive_next_step_coverage)}",
            self.javascript,
        )
        self.assertIn(
            'metricLabel("order_10d", "Kontakt – order inom 10 dagar")',
            self.javascript,
        )

    def test_headline_outcomes_render_only_live_value_and_live_comparisons(self):
        self.assertNotIn("comparableOutcomeText", self.javascript)
        self.assertNotIn("Jämförbart 10-dagarsutfall", self.javascript)
        self.assertNotIn('class="sc-kpi-comparable"', self.javascript)
        self.assertNotIn(".sc-kpi-comparable", self.css)
        self.assertIn('${escapeHtml(comparisonText(metric))}', self.javascript)
        self.assertNotIn('metric.status === "sufficient"\n      ? \'<span class="sc-status"', self.javascript)

    def test_two_live_team_trends_are_accessible_and_independent(self):
        trend = self.javascript.split("function teamTrendPanelMarkup", 1)[1].split(
            "function matrixReasonLabel", 1
        )[0]
        render = self.javascript.split("function renderDashboard", 1)[1].split(
            "function handleDashboardClick", 1
        )[0]
        for expected in (
            "Varje punkt avser en kontaktvecka",
            "hela 10-dagarsfönstret har passerat",
            "veckopunkterna förändras inte enbart för att fler dagar passerar",
            "Diagrammen använder samma KPI-definitioner som Coachningsöversikten",
            "Period-, säljar- och kanalfilter begränsar inte grafen",
            "lifecycle och segment följer filtren",
            'metricKey: "order_10d"',
            'metricKey: "positive_to_order_10d"',
            'data-drilldown="${config.metricKey}"',
            'data-channel="all"',
            'data-seller="${escapeHtml(item.seller)}"',
            'tabindex="0" role="button"',
            "Litet underlag",
            "teamTrendMarker",
            "stroke-dasharray",
            "segment.length > 1",
            "point.value === null",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, trend)
        self.assertIn("const ticks = [0, 0.25, 0.5, 0.75, 1]", trend)
        self.assertIn("week_axis", trend)
        self.assertIn("teamTrendWeekLabel", trend)
        self.assertIn("previousYear !== match[1]", self.javascript)
        self.assertIn('const period = `${point.period?.start || "—"}–${point.period?.end || "—"}`', trend)
        self.assertIn("${teamTrendWeekLabel(point.week)} · ${period}", trend)
        self.assertIn("function teamTrendStyle(seller)", self.javascript)
        self.assertIn("TEAM_TREND_DASHES[styleIndex]", self.javascript)
        self.assertIn('.normalize("NFKC").trim().toLocaleLowerCase("sv-SE")', self.javascript)
        self.assertIn("hash = Math.imul(hash, 16777619)", self.javascript)
        self.assertIn("const style = teamTrendStyle(item.seller)", trend)
        self.assertIn("teamTrendMarker(style.marker", trend)
        self.assertIn('data-series-style="${style.key}"', trend)
        self.assertNotIn("TEAM_TREND_COLORS[seriesIndex", self.javascript)
        self.assertNotIn("TEAM_TREND_DASHES[seriesIndex", self.javascript)
        self.assertNotIn("rolling", trend.casefold())
        self.assertIn('role="tablist" aria-label="Välj 10-dagarstrend"', trend)
        self.assertIn('data-team-trend-view="${key}"', trend)
        self.assertIn('role="tabpanel"', trend)
        self.assertIn('id="sc-team-trend-panel-${view}"', trend)
        self.assertIn('aria-labelledby="sc-team-trend-tab-${view}"', trend)
        self.assertIn('${activeView === view ? "" : " hidden"}', trend)
        self.assertIn('aria-controls="sc-team-trend-panel-${key}"', trend)
        self.assertIn('aria-selected="${view === key}"', trend)
        self.assertIn('tabindex="${view === key ? "0" : "-1"}"', trend)
        self.assertIn(
            "tabs.map(([key, _label, config]) => teamTrendPanelMarkup(trends, key, config, view))",
            trend,
        )
        point_title = trend.split("const title =", 1)[1].split(";", 1)[0]
        self.assertNotIn("prelim", point_title.casefold())
        self.assertLess(render.index("teamComparisonMarkup"), render.index("teamTrendsMarkup"))
        self.assertLess(render.index("teamTrendsMarkup"), render.index("priorityMatrixMarkup"))
        self.assertIn(".sc-team-order-trend { display: block; width: 100%; min-width: 1040px", self.css)
        self.assertIn(".sc-team-order-trend-wrap { overflow-x: auto", self.css)
        self.assertIn(".sc-team-order-point.is-small-sample", self.css)
        self.assertIn(".sc-team-order-line.is-selected", self.css)
        self.assertIn(".sc-team-order-line.is-unselected", self.css)

    def test_team_trend_tabs_switch_client_side_with_full_keyboard_navigation(self):
        click_handler = self.javascript.split(
            'const teamTrendTab = event.target.closest("[data-team-trend-view]")',
            2,
        )[2].split('const diagnosticTab =', 1)[0]
        self.assertIn('teamTrendView: "order"', self.javascript)
        self.assertIn('const keys = ["order", "positive"]', self.javascript)
        for key in ("ArrowLeft", "ArrowRight", "Home", "End"):
            self.assertIn(key, self.javascript)
        self.assertIn("renderDashboard(state.data)", click_handler)
        self.assertNotIn("loadSummary", click_handler)

    def test_team_trend_point_drilldown_overrides_seller_period_and_channel(self):
        handler = self.javascript.split("function handleDashboardClick", 1)[1].split(
            "function drawerMarkup", 1
        )[0]
        self.assertIn("if (drilldown.dataset.seller) extra.seller", handler)
        self.assertIn("if (drilldown.dataset.channel) extra.channel", handler)
        self.assertIn("if (drilldown.dataset.start) extra.start", handler)
        self.assertIn("if (drilldown.dataset.end) extra.end", handler)
        self.assertLess(
            handler.index('event.target.closest("[data-drilldown]")'),
            handler.index('event.target.closest("[data-seller]")'),
        )
        self.assertIn('const value = extra[key] ?? state.filters[key]', self.javascript)

    def test_matrix_uses_fixed_points_offsets_ticks_and_separate_axes(self):
        for expected in (
            "MATRIX_TICKS = [0, 25, 50, 75, 100]",
            "sc-matrix-inner", "sc-matrix-gridline",
            "sc-matrix-x-axis-label", "sc-matrix-y-axis-label",
            "Math.max(0, Math.min(100",
            "const occupied = new Map()", "--offset-x",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn("Math.max(3, Math.min(97", self.javascript)
        self.assertIn("const collisionOffsets = [", self.javascript)
        self.assertIn("[0, 0]", self.javascript)
        self.assertIn("inset: 48px 48px 56px 62px", self.css)
        self.assertIn(".sc-matrix { position: relative; height: 410px; overflow: hidden", self.css)
        self.assertIn("writing-mode: vertical-rl", self.css)

    def test_only_priority_matrix_and_seller_highlight_are_present(self):
        self.assertIn("function priorityMatrixPanelMarkup", self.javascript)
        self.assertIn("Teamets prioriteringsmatris", self.javascript)
        self.assertIn("Kontakt – order inom 10 dagar", self.javascript)
        self.assertNotIn('data-matrix-view="sales"', self.javascript)
        self.assertNotIn('data-matrix-view="priority"', self.javascript)
        self.assertNotIn("matrixView", self.javascript)
        self.assertNotIn("Försäljning</button>", self.javascript)
        self.assertNotIn("sc-matrix-tabs", self.css)
        self.assertIn("sellerSelected(item.seller)", self.javascript)
        self.assertIn("is-selected", self.css)

    def test_funnel_outcome_and_aggregate_priority_are_separate(self):
        self.assertIn("Bom-ratio – planerade besök", self.javascript)
        self.assertIn("Bom-ratio – oplanerade besök", self.javascript)
        self.assertIn("funnel.steps", self.javascript)
        self.assertIn("när en attribuerad order redan finns inom fönstret", self.javascript)
        self.assertIn("Avgjorda kontakter", self.javascript)
        self.assertIn("Följdes av attribuerad order", self.javascript)
        self.assertIn("priorityDiagnosticsMarkup", self.javascript)
        self.assertNotIn("priority_gap", self.javascript)
        self.assertNotIn("Nästa bästa kunder", self.javascript)
        self.assertNotIn("Aktuella högprioriterade kunder", self.javascript)
        self.assertIn("Kan inte beräknas · jämförbar historisk prioritet saknas", self.javascript)
        self.assertNotIn("high_priority_score_fallback", self.javascript)

    def test_tabs_are_keyboard_accessible_and_only_outcomes_can_be_preliminary(self):
        self.assertIn('role="tablist" aria-label="Diagnostikflikar"', self.javascript)
        self.assertIn('role="tab"', self.javascript)
        self.assertIn("ArrowLeft", self.javascript)
        self.assertIn("ArrowRight", self.javascript)
        self.assertIn(
            'const keys = ["visits", "conversion", "channels", "followup", "priority"]',
            self.javascript,
        )
        self.assertIn("const preliminary = isOutcome && row.outcome_complete === false", self.javascript)
        self.assertIn('["resolved_converted_contacts", "Konverterade", "#b7791f", "order_10d_sync", true]', self.javascript)
        self.assertIn('["human_activities", "Aktiviteter", "#942a52", "human_activities", false]', self.javascript)
        self.assertIn("aktivitet, nådda och positiva är slutliga", self.javascript)
        self.assertIn('diagnosticTab: "visits"', self.javascript)
        self.assertLess(
            self.javascript.index('["visits", "Besök"]'),
            self.javascript.index('["conversion", "Konvertering"]'),
        )

    def test_visible_sales_coaching_copy_does_not_use_synchronous_wording(self):
        self.assertNotIn("synkrona", self.javascript.casefold())
        self.assertNotIn("synkrona", self.sales_coaching_source.casefold())

    def test_comparison_formatting_respects_count_metrics(self):
        self.assertIn('metric?.metric_type === "count"', self.javascript)
        self.assertIn('`${number(value, 1)} aktiviteter`', self.javascript)
        self.assertIn('`${value >= 0 ? "+" : ""}${number(value, 1)} aktiviteter`', self.javascript)
        self.assertIn("formatValue(comparisons.peer_median)", self.javascript)
        self.assertIn("formatValue(previousValue)", self.javascript)
        self.assertIn(
            'comparisons.previous_period_suppressed_reason === "pending_10d_outcomes"',
            self.javascript,
        )
        self.assertIn("!previousSuppressed && previousValue", self.javascript)
        self.assertIn("!previousSuppressed && comparisons.previous_period_status", self.javascript)

    def test_priority_matrix_has_swedish_denominator_zero_reason(self):
        self.assertIn(
            'order_denominator_zero: "inga berättigade kontakter för ordermåttet"',
            self.javascript,
        )
        self.assertNotIn("positive_order_denominator_zero", self.javascript)

    def test_pending_outcome_copy_is_preliminary_and_hidden_at_zero(self):
        kpi_markup = self.javascript.split("function kpiMarkup", 1)[1].split(
            "function kpisMarkup", 1
        )[0]
        self.assertIn("Number(metric.waiting_outcome_count) > 0", kpi_markup)
        self.assertIn(
            "Preliminärt · ${number(metric.waiting_outcome_count)} väntar på 10-dagarsutfall",
            kpi_markup,
        )
        self.assertNotIn("väntar fortfarande på fullt 10-dagarsutfall", self.javascript)
        coaching_markup = self.javascript.split(
            "function coachingMarkup", 1
        )[1].split("function conversionMarkup", 1)[0]
        self.assertIn(
            '["order_10d", "positive_to_order_10d"].includes(card.metric_key)',
            coaching_markup,
        )
        self.assertIn("Number(card.evidence?.waiting_outcome_count) > 0", coaching_markup)
        self.assertIn(
            "Preliminärt · ${number(card.evidence.waiting_outcome_count)} väntar på 10-dagarsutfall",
            coaching_markup,
        )
        matrix_markup = self.javascript.split(
            "function priorityMatrixPanelMarkup", 1
        )[1].split("function priorityMatrixMarkup", 1)[0]
        self.assertIn("Number(xRate.waiting_outcome_count) > 0", matrix_markup)
        self.assertIn(
            "preliminärt: ${number(xRate.waiting_outcome_count)} väntar på 10-dagarsutfall",
            matrix_markup,
        )
        self.assertIn('label: "Väntar på 10-dagarsutfall"', self.javascript)
        self.assertNotIn("väntar på slutligt 10-dagarsutfall", self.javascript)
        self.assertNotIn("Väntar på slutligt utfall", self.javascript)
        self.assertIn('drilldownMetric: "resolved_order_10d"', self.javascript)
        self.assertIn('drilldownMetric: "converted_order_10d"', self.javascript)

    def test_pr3_benchmark_and_signal_contract_is_presentational(self):
        self.assertIn("Median övriga säljare", self.javascript)
        self.assertNotIn("Peer median", self.javascript)
        self.assertIn("delta_peer", self.javascript)
        self.assertIn("Föregående period", self.javascript)
        self.assertIn("card.next_action", self.javascript)
        self.assertNotIn("card.code ===", self.javascript)
        self.assertNotIn("switch (card.code", self.javascript)

    def test_registry_drives_glossary_and_local_metric_information(self):
        required = {
            "bom_ratio", "high_priority_boms", "human_activities",
            "median_days_to_order", "positive_next_step_coverage",
            "order_10d", "planned_completed_in_time", "positive_dialogue",
            "positive_to_order_10d",
            "priority_focus", "strategic_coverage",
            "reach",
        }
        self.assertTrue(required.issubset(METRIC_DEFINITIONS))
        self.assertNotIn("positive_to_order_10d_comparable", METRIC_DEFINITIONS)
        self.assertNotIn("order_10d_comparable", METRIC_DEFINITIONS)
        self.assertIn('new Intl.Collator("sv-SE"', self.javascript)
        self.assertIn("collator.compare(left.label, right.label)", self.javascript)
        self.assertIn('data-definition-key="${escapeHtml(key)}"', self.javascript)
        self.assertIn("definitionParts(definitionKey, context)", self.javascript)
        self.assertIn("definitionParts(card.metric_key", self.javascript)
        self.assertIn("metricHeader(", self.javascript)
        self.assertIn("sc-metric-info", self.css)

    def test_synchronous_dialogue_metrics_are_not_applicable_for_email(self):
        self.assertEqual(
            METRIC_DEFINITIONS["positive_dialogue"]["not_computable_text"],
            "Positiv dialog mäts endast för Besök och Telefon.",
        )
        self.assertEqual(
            METRIC_DEFINITIONS["positive_to_order_10d"]["not_computable_text"],
            "Positiv → order mäts endast för Besök och Telefon.",
        )
        self.assertIn("channelRate(key, \"positive_dialogue\"", self.javascript)
        self.assertIn("channelRate(key, \"positive_to_order_10d\"", self.javascript)
        self.assertIn('? "Ej tillämpligt"', self.javascript)
        self.assertIn('data-channel-row="${key}"', self.javascript)
        self.assertIn('data-channel-metric="positive_to_order_10d"', self.javascript)
        self.assertNotIn("Fördjupa analysen utan operativa kundlistor.", self.javascript)

    def test_metric_information_is_separate_from_drilldown_and_accessible(self):
        info_branch = self.javascript.split(
            'const metricInfo = event.target.closest(\'[data-sc-action="metric-info"]\')', 1
        )[1].split('const retry =', 1)[0]
        self.assertIn("aria-controls", self.javascript)
        self.assertIn("aria-expanded", self.javascript)
        self.assertIn("return;", info_branch)
        self.assertNotIn("openDrilldown", info_branch)
        self.assertIn("buttonClass", self.javascript)
        self.assertIn(".sc-glossary dl { grid-template-columns: 1fr; }", self.css)
        self.assertIn("overflow-wrap: anywhere", self.css)

    def test_coaching_evidence_formats_counts_without_percentages(self):
        self.assertIn('item.metric_type === "count"', self.javascript)
        self.assertIn('item.unit || "st"', self.javascript)
        self.assertIn("item.secondary_evidence", self.javascript)
        self.assertIn("`${rateEvidence(item)} · ${percent(item.value)}`", self.javascript)
        count_branch = self.javascript.split(
            'if (item.metric_type === "count")', 1
        )[1].split("return item.denominator", 1)[0]
        self.assertNotIn("percent(", count_branch)
        self.assertNotIn("card.code", count_branch)

    def test_drilldown_explains_each_rows_cohort_role(self):
        self.assertIn("cohortLabels", self.javascript)
        self.assertIn('numerator: "Konverterad"', self.javascript)
        self.assertIn('resolved_without_order: "Avgjord utan order"', self.javascript)
        self.assertIn('pending: "Väntar på utfall"', self.javascript)
        self.assertIn('denominator_only: "Endast nämnare"', self.javascript)
        self.assertIn('missed_outcome: "Missat utfall"', self.javascript)

    def test_stale_response_and_transient_error_handling_are_explicit(self):
        self.assertIn("requestSerial", self.javascript)
        self.assertIn("new AbortController()", self.javascript)
        self.assertIn("serial !== state.requestSerial", self.javascript)
        self.assertIn("Senast lyckade data visas", self.javascript)
        self.assertIn('data-sc-action="retry"', self.javascript)

    def test_accessibility_and_drilldown_limit_are_explicit(self):
        self.assertIn('role="dialog"', self.javascript)
        self.assertIn('aria-modal="true"', self.javascript)
        self.assertIn('aria-live="polite"', self.javascript)
        self.assertIn("if (event.key === \"Escape\") closeDrawer()", self.javascript)
        self.assertIn("limit: 200", self.javascript)
        self.assertIn("state.lastFocus.focus()", self.javascript)

    def test_responsive_rules_avoid_page_level_horizontal_scroll(self):
        self.assertIn("@media (max-width: 900px)", self.css)
        self.assertIn("@media (max-width: 620px)", self.css)
        self.assertIn(".sc-matrix-wrap { overflow-x: auto", self.css)
        self.assertIn(".sc-trend-wrap,", self.css)
        self.assertIn("width: calc(100% - 20px)", self.css)
        self.assertIn(".sc-drawer { width: 100vw; }", self.css)
        self.assertIn(".sc-team-charts { grid-template-columns: 1fr; }", self.css)

    def test_static_assets_are_served_by_flask(self):
        app_module.app.config.update(TESTING=True)
        client = app_module.app.test_client()
        javascript = client.get("/static/sales_coaching.js")
        stylesheet = client.get("/static/sales_coaching.css")
        try:
            self.assertEqual(javascript.status_code, 200)
            self.assertEqual(stylesheet.status_code, 200)
        finally:
            javascript.close()
            stylesheet.close()


if __name__ == "__main__":
    main()
