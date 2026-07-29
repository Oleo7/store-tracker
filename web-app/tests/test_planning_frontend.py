from pathlib import Path
import re
from unittest import TestCase


INDEX_PATH = Path(__file__).resolve().parents[1] / "index.html"


class PlanningFrontendContractTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = INDEX_PATH.read_text(encoding="utf-8")

    def test_contact_log_uses_the_three_supported_channels(self):
        match = re.search(
            r'<select id="f-channel">(.*?)</select>',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        options = re.findall(r"<option(?: [^>]*)?>(.*?)</option>", match.group(1))
        self.assertEqual(options, ["Välj...", "Telefon", "Mejl", "Besök"])

    def test_partial_contact_save_keeps_a_retry_payload(self):
        self.assertIn('result?.status === "partial"', self.html)
        self.assertIn("contactRetryPayload = payload", self.html)
        self.assertIn("Försök slutföra sparningen", self.html)

    def test_planning_patch_flows_send_optimistic_version(self):
        self.assertIn(
            "payload.expected_updated_at = planningEditorActivity.updated_at",
            self.html,
        )
        self.assertIn("expected_updated_at: activity.updated_at", self.html)
        self.assertIn(
            "payload.expected_revision = Number(planningEditorActivity.revision || 1)",
            self.html,
        )
        self.assertIn("expected_revision: Number(activity.revision || 1)", self.html)

    def test_customer_selector_is_accessible_search_combobox_using_customer_id(self):
        self.assertIn('role="combobox"', self.html)
        self.assertIn('aria-controls="planning-editor-customer-list"', self.html)
        self.assertIn('role="listbox"', self.html)
        self.assertIn('"ArrowDown" || event.key === "ArrowUp"', self.html)
        self.assertIn('event.key === "Enter"', self.html)
        self.assertIn('.normalize("NFD")', self.html)
        self.assertIn("customer.address_google", self.html)
        self.assertIn("customer.customer_number", self.html)
        self.assertIn(
            "if (customer.customer_id) payload.customer_id = customer.customer_id",
            self.html,
        )

    def test_planning_customer_binding_never_uses_row_only(self):
        binding = re.search(
            r"function planningCustomerForItem\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(binding)
        body = binding.group(1)
        self.assertIn("customer_id", body)
        self.assertIn("customer_number", body)
        self.assertIn("byVerifiedSnapshot", body)
        self.assertNotIn("byRow", body)
        self.assertNotIn("customer_row", body)
        self.assertIn(
            "Kunden kunde inte bindas säkert. Ladda om eller kontakta administratör.",
            self.html,
        )

    def test_global_followup_queue_has_overdue_upcoming_and_show_all(self):
        self.assertIn("unscheduled_followups_overdue", self.html)
        self.assertIn("unscheduled_followups_upcoming", self.html)
        self.assertIn("Försenade uppföljningar", self.html)
        self.assertIn("Kommande uppföljningar", self.html)
        self.assertIn("Visa alla (${rows.length})", self.html)

    def test_planning_backlog_title_and_priority_sort(self):
        self.assertIn("Gamla uppföljningar att planera in", self.html)
        self.assertNotIn("Kontakter och uppföljningar utan tid.", self.html)
        self.assertIn("function planningBacklogPriorityScore(item)", self.html)
        self.assertIn("function planningSortBacklog(rows)", self.html)
        self.assertIn(
            "planningBacklogPriorityScore(right) - planningBacklogPriorityScore(left)",
            self.html,
        )

    def test_planning_header_map_uses_ordered_day_visits(self):
        self.assertNotIn('id="planning-new-btn"', self.html)
        self.assertIn('id="planning-day-map-btn"', self.html)
        self.assertIn("function planningVisitStopsForDate(dateKey)", self.html)
        self.assertIn('activity.contact_type === "visit"', self.html)
        self.assertIn(
            '!["cancelled", "skipped"].includes(activity.status)',
            self.html,
        )
        self.assertIn("routeInMapStops = [...visitStops]", self.html)
        self.assertIn('mapReturnView = "planning"', self.html)
        self.assertIn("showPlanningDayMap", self.html)

    def test_planning_error_preserves_admin_owner_and_backend_message(self):
        self.assertIn(
            "error?.details || error?.payload || {}",
            self.html,
        )
        owner_select = re.search(
            r"function renderPlanningOwnerSelect\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_select)
        self.assertIn("planningActiveUsersCache", owner_select.group(1))
        load_week = re.search(
            r"async function loadPlanningWeek\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(load_week)
        error_handler = load_week.group(1).split("} catch (error) {", 1)[1]
        self.assertNotIn("planningData = null", error_handler)

    def test_route_edit_explains_manual_conversion(self):
        self.assertIn(
            "När du sparar blir aktiviteten manuellt planerad och behålls vid nästa automatiska ruttberäkning.",
            self.html,
        )

    def test_legacy_followup_keeps_its_source_link(self):
        self.assertIn('payload.source = "follow_up"', self.html)
        self.assertIn(
            "payload.source_contact_id = planningEditorSeed.source_contact_id",
            self.html,
        )

    def test_unplanned_contacts_render_in_the_historical_agenda(self):
        self.assertIn("function planningUnplannedForDate", self.html)
        self.assertIn("function planningAgendaItemsForDate", self.html)
        self.assertIn("Uppföljningar utan bokad tid", self.html)
        self.assertNotIn("Oplanerade kontakter · ${unplanned.length}", self.html)

    def test_contact_types_are_three_touch_sized_radio_chips(self):
        self.assertIn('name="planning-editor-type"', self.html)
        self.assertIn("grid-template-columns: repeat(3, minmax(0, 1fr))", self.html)
        self.assertRegex(
            self.html,
            r"\.planning-type-choice span\s*\{[^}]*min-height:\s*52px",
        )

    def test_admin_owner_comes_from_active_seller_response(self):
        self.assertIn(
            "planningSelectedUserName = responseOwner.user_name",
            self.html,
        )
        owner_select = re.search(
            r"function renderPlanningOwnerSelect\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_select)
        self.assertNotIn("available.unshift(current)", owner_select.group(1))
        self.assertIn('id="f-followup-owner"', self.html)
        self.assertIn("followupEnabled && userIsAdmin()", self.html)

    def test_planning_entry_points_are_hidden_without_planning_role(self):
        self.assertIn("function currentUserCanPlan()", self.html)
        self.assertIn(
            'currentUserCanPlan() ? "" : "none"',
            self.html,
        )

    def test_frontend_accepts_every_backend_seller_role(self):
        seller_check = re.search(
            r"function currentUserIsSeller\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(seller_check)
        for role in ("säljare", "saljare", "account manager", "accountmanager"):
            self.assertIn(f'"{role}"', seller_check.group(1))

    def test_adjusted_map_route_cannot_silently_import_original_stops(self):
        self.assertIn("function routeMapSelectionDiffersFromProposal", self.html)
        self.assertIn("Kartans stopp har ändrats", self.html)
