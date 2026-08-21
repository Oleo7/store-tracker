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
            "Period", "Säljare", "Kanal", "Lifecycle", "Kundsegment", "Jämförelse",
            "Coachningsöversikt", "Teamets coachningsmatris", "Säljtratt", "Veckotrend",
            "Besökseffektivitet", "Kanalernas effektivitet",
            "Prioritering och kundallokering", "Uppföljningsdisciplin", "Coachningskort",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)

    def test_frontend_uses_backend_rate_contract_without_recalculating_kpis(self):
        self.assertIn("percent(metric.value)", self.javascript)
        self.assertIn("rateEvidence(metric)", self.javascript)
        self.assertIn("metric.definition", self.javascript)
        self.assertNotIn("attributed_orders /", self.javascript)
        self.assertNotIn("priority_percentile_at_contact >=", self.javascript)

    def test_matrix_does_not_invent_medians_and_followup_cards_use_exact_metrics(self):
        self.assertIn("Otillräckligt jämförbart underlag", self.javascript)
        self.assertNotIn("matrix.medians?.priority_focus ?? 0.5", self.javascript)
        self.assertNotIn("matrix.medians?.order_10d ?? 0.5", self.javascript)
        for metric in (
            "followup_success", "followup_gap", "followup_gap_10d",
            "planned_on_time", "planned_overdue", "planned_skipped",
        ):
            with self.subTest(metric=metric):
                self.assertIn(f'"{metric}"', self.javascript)

    def test_data_quality_banner_distinguishes_rows_from_issue_count(self):
        self.assertIn("order_attribution_identity_coverage", self.javascript)
        self.assertIn("flagged_activity_rows", self.javascript)
        self.assertIn("quality_issue_count", self.javascript)
        self.assertIn("Kärndata för säljanalys", self.javascript)
        self.assertIn("Historisk prioriteringsdata", self.javascript)

    def test_team_comparison_uses_backend_counts_and_high_touch_grouped_bars(self):
        for expected in (
            "teamComparisonMarkup", "sc-team-charts", "human_activities_total",
            "channel_mix?.visit", "channel_mix?.phone", "positive_dialogues_count",
            "order_10d_converted_contacts", "waiting_outcome_count",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, self.javascript)
        self.assertNotIn("item.attributed_orders /", self.javascript)

    def test_both_matrix_views_small_samples_and_seller_highlight_are_present(self):
        self.assertIn('data-matrix-view="sales"', self.javascript)
        self.assertIn('data-matrix-view="priority"', self.javascript)
        self.assertIn('state.matrixView === "priority"', self.javascript)
        self.assertIn('item.sample_status === "small_sample"', self.javascript)
        self.assertIn("sellerSelected(item.seller)", self.javascript)
        self.assertIn("is-small-sample", self.css)
        self.assertIn("is-selected", self.css)

    def test_qa_labels_and_sequential_funnel_contract_are_rendered(self):
        self.assertIn("Bom-ratio – planerade besök", self.javascript)
        self.assertIn("Bom-ratio – oplanerade besök", self.javascript)
        self.assertIn("Aktuella högprioriterade kunder att bearbeta", self.javascript)
        self.assertIn("funnel.steps", self.javascript)
        self.assertIn("Kan inte beräknas · historisk prioritet saknas", self.javascript)

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
