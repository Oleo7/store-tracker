from pathlib import Path
from unittest import TestCase, main
import sys


WEB_APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WEB_APP_DIR))

import app as app_module  # noqa: E402


class SalesCoachingFrontendTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (WEB_APP_DIR / "index.html").read_text(encoding="utf-8")
        cls.javascript = (WEB_APP_DIR / "static" / "sales_coaching.js").read_text(encoding="utf-8")
        cls.css = (WEB_APP_DIR / "static" / "sales_coaching.css").read_text(encoding="utf-8")

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
            "Teamets coachningsmatriser", "Aktivitetstratt", "10-dagarsutfall",
            "Konvertering", "Besök", "Kanaler", "Uppföljning", "Prioritering",
            "Datakvalitet och definitioner",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn('id="sc-comparison"', self.javascript)

    def test_exactly_four_main_kpis_are_rendered(self):
        self.assertIn(
            'const order = ["human_activities", "reach", "positive_dialogue", "positive_to_order_10d"]',
            self.javascript,
        )
        self.assertNotIn(
            '"order_10d", "priority_focus", "bom_ratio"', self.javascript
        )

    def test_main_kpis_explain_their_denominators_in_plain_swedish(self):
        for expected in (
            'denominatorLabel: "analyserbara besök/telefonsamtal"',
            'denominatorLabel: "nådda kontakter"',
            'denominatorLabel: "mogna positiva kontakter"',
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertIn("sc-kpi-denominator", self.javascript)
        self.assertIn("flex-wrap: wrap", self.css)

    def test_all_main_kpis_have_clickable_plain_language_explanations(self):
        explanations = (
            "Alla mänskliga aktiviteter via besök, telefon och manuella mejl. Automatiska CRM-mejl räknas inte.",
            "Andelen analyserbara besök och telefonsamtal där säljaren faktiskt nådde kunden. Manuella mejl ingår inte i träffgraden.",
            "Andelen nådda mänskliga kontakter som slutade i positiv dialog eller order. Ej anträffbar/bom räknas inte i nämnaren.",
            "Andelen positiva kontakter som följdes av en attribuerad order inom 0–10 dagar. Endast kontakter som hunnit få ett fullständigt 10-dagarsutfall ingår.",
        )
        for explanation in explanations:
            with self.subTest(explanation=explanation):
                self.assertIn(explanation, self.javascript)
        self.assertIn('data-sc-action="kpi-info"', self.javascript)
        self.assertIn('aria-expanded="false"', self.javascript)
        self.assertIn("explanation.hidden = expanded", self.javascript)
        presentation = self.javascript.split(
            "const KPI_PRESENTATION", 1
        )[1].split("const root", 1)[0]
        for internal_term in (
            "mature cohort", "qualified dialogue", "attribution_eligible",
            "historical_snapshot", "current_customer_state",
        ):
            with self.subTest(internal_term=internal_term):
                self.assertNotIn(internal_term, presentation.lower())

    def test_frontend_uses_backend_rate_contract_without_recalculating_kpis(self):
        self.assertIn("percent(metric.value)", self.javascript)
        self.assertIn("rateEvidence(metric)", self.javascript)
        self.assertIn("metric.definition", self.javascript)
        self.assertNotIn("attributed_orders /", self.javascript)
        self.assertNotIn("priority_percentile_at_contact >=", self.javascript)

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
        self.assertIn("Suppression är inte ett kvalitetsfel", self.javascript)
        self.assertNotIn("sc-quality-debug", self.javascript)

    def test_team_comparison_has_three_activity_bars_and_exact_table(self):
        for expected in (
            "teamComparisonMarkup", "human_activities_total",
            "visit_breakdown?.analysable", "visit_breakdown?.reached",
            "visit_breakdown?.boms", "channel_mix?.phone", "channel_mix?.email",
            "positive_to_order_10d", "positive_next_step_coverage",
            "is-visit-stack", "is-visit-reached", "is-visit-bom",
            "is-email", "sc-comparison-table", "Nästa-steg-täckning",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn("item.attributed_orders /", self.javascript)
        self.assertNotIn('class="sc-team-secondary"', self.javascript)

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

    def test_both_matrix_views_small_samples_and_seller_highlight_are_present(self):
        self.assertIn('data-matrix-view="sales"', self.javascript)
        self.assertIn('data-matrix-view="priority"', self.javascript)
        self.assertIn('state.matrixView === "priority"', self.javascript)
        self.assertIn('item.sample_status === "small_sample"', self.javascript)
        self.assertIn("sellerSelected(item.seller)", self.javascript)
        self.assertIn("is-small-sample", self.css)
        self.assertIn("is-selected", self.css)

    def test_funnel_outcome_and_aggregate_priority_are_separate(self):
        self.assertIn("Bom-ratio – planerade besök", self.javascript)
        self.assertIn("Bom-ratio – oplanerade besök", self.javascript)
        self.assertIn("funnel.steps", self.javascript)
        self.assertIn("Mogen är en mätbar utfallskohort", self.javascript)
        self.assertIn("Följdes av attribuerad order", self.javascript)
        self.assertIn("priorityDiagnosticsMarkup", self.javascript)
        self.assertNotIn("priority_gap", self.javascript)
        self.assertNotIn("Nästa bästa kunder", self.javascript)
        self.assertNotIn("Aktuella högprioriterade kunder", self.javascript)
        self.assertIn("Kan inte beräknas · jämförbar historisk v2-percentil saknas", self.javascript)
        self.assertNotIn("high_priority_score_fallback", self.javascript)

    def test_tabs_are_keyboard_accessible_and_only_outcomes_can_be_preliminary(self):
        self.assertIn('role="tablist" aria-label="Diagnostikflikar"', self.javascript)
        self.assertIn('role="tab"', self.javascript)
        self.assertIn("ArrowLeft", self.javascript)
        self.assertIn("ArrowRight", self.javascript)
        self.assertIn("const preliminary = isOutcome && row.outcome_complete === false", self.javascript)
        self.assertIn('["mature_converted_contacts", "Konverterade", "#b7791f", "order_10d_sync", true]', self.javascript)
        self.assertIn('["human_activities", "Aktiviteter", "#942a52", "human_activities", false]', self.javascript)
        self.assertIn("aktivitet, nådda och positiva är slutliga", self.javascript)

    def test_comparison_formatting_respects_count_metrics(self):
        self.assertIn('metric?.metric_type === "count"', self.javascript)
        self.assertIn('`${number(value, 1)} aktiviteter`', self.javascript)
        self.assertIn('`${value >= 0 ? "+" : ""}${number(value, 1)} aktiviteter`', self.javascript)
        self.assertIn("formatValue(comparisons.peer_median)", self.javascript)
        self.assertIn("formatValue(previousValue)", self.javascript)

    def test_matrix_has_swedish_denominator_zero_reason(self):
        self.assertIn(
            'positive_order_denominator_zero: "inga mogna positiva kontakter för positiv-till-order-måttet"',
            self.javascript,
        )

    def test_pr3_benchmark_and_signal_contract_is_presentational(self):
        self.assertIn("Peer median", self.javascript)
        self.assertIn("delta_peer", self.javascript)
        self.assertIn("Föregående period", self.javascript)
        self.assertIn("card.next_action", self.javascript)
        self.assertNotIn("card.code ===", self.javascript)
        self.assertNotIn("switch (card.code", self.javascript)

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
        self.assertIn('numerator: "Täljare"', self.javascript)
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
