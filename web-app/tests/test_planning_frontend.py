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

    def test_ambiguous_contact_activity_reuses_the_original_request(self):
        self.assertIn('result?.error === "ambiguous_planned_activity"', self.html)
        self.assertIn("function openAmbiguousContactActivityDialog(payload, candidates)", self.html)
        self.assertIn("...payload,", self.html)
        self.assertIn("planned_activity_id: activity.planned_activity_id", self.html)
        self.assertIn("contactRetryPayload = {", self.html)

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

    def test_planning_preview_uses_backend_queue_without_raw_score_backlog(self):
        self.assertIn("Fler kunder att planera", self.html)
        self.assertNotIn("Dagens fokus", self.html)
        self.assertNotIn("Gamla uppföljningar att planera in", self.html)
        self.assertNotIn("Kommande uppföljningar", self.html)
        renderer = re.search(
            r"function renderPlanningCandidates\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(renderer)
        body = renderer.group(1)
        self.assertIn("const visible = planningRecommendationPreview", body)
        self.assertIn("planning-backlog-load-more", body)
        self.assertIn("Ladda fler", body)
        self.assertIn(
            "const hasMore = visible.length < Math.max(",
            body,
        )
        self.assertIn(
            "0, planningRecommendationPendingCount - 1",
            body,
        )
        self.assertIn("${hasMore ?", body)
        self.assertNotIn("planningCandidateCustomers()", body)
        self.assertNotIn("priority_score", body)
        self.assertNotIn("expected_order_dfp", body)
        self.assertNotIn("Orderpotential", body)
        self.assertNotIn("Visa fler", body)

    def test_planning_preview_load_more_is_snapshot_based_and_resets_by_owner(self):
        self.assertIn("let planningRecommendationPreviewLimit = 10", self.html)
        self.assertIn("planningRecommendationPreviewLimit + 5", self.html)
        self.assertIn('params.set("preview_limit", String(previewLimit))', self.html)
        self.assertIn(
            "planningRecommendationPreview = Array.isArray(payload.queue_preview)",
            self.html,
        )
        owner_change = re.search(
            r'planning-owner-select"\)\.addEventListener\("change".*?\n  \}\);',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(owner_change)
        self.assertIn("planningRecommendationPreviewLimit = 10", owner_change.group(0))

    def test_phase1_renders_one_nonblocking_recommendation_card(self):
        self.assertIn('id="planning-recommendation"', self.html)
        self.assertIn('<div class="planning-recommendation-heading">NÄSTA ÅTGÄRD</div>', self.html)
        self.assertNotIn("planningRecommendationPendingCount} kvar", self.html)
        for label in ("Ring nu", "Planera", "Snooza", "Dölj detta förslag"):
            self.assertIn(f">{label}</button>", self.html)
        render = re.search(
            r"function renderPlanningRecommendation\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(render)
        self.assertNotIn(".map(", render.group(1))
        self.assertNotIn("Orderpotential", render.group(1))
        self.assertIn("planningRecommendation.recommended_contact_type", render.group(1))
        self.assertIn("!planningRecommendation.can_call", render.group(1))
        button_positions = [
            render.group(1).index(f">{label}</button>")
            for label in ("Ring nu", "Planera", "Snooza", "Dölj detta förslag")
        ]
        self.assertEqual(button_positions, sorted(button_positions))
        self.assertIn("Kalendern och övrig planering fungerar fortfarande", self.html)
        self.assertIn("loadPlanningRecommendation();", self.html)

    def test_phase1_actions_wait_for_success_and_lock_suggestion_customer(self):
        self.assertIn("Kunden är låst för den här rekommendationen", self.html)
        self.assertIn('contact_type: suggestion.recommended_contact_type || "visit"', self.html)
        self.assertIn("expected_suggestion_revision", self.html)
        self.assertIn("suggestionSeed.expected_suggestion_revision ?? 0", self.html)
        self.assertIn("source_suggestion_id", self.html)
        self.assertIn("replaceWithNextRecommendation(payload)", self.html)
        self.assertIn("replaceWithNextRecommendation(result)", self.html)
        self.assertIn("if (!suggestionLocked) setupPlanningCustomerCombobox", self.html)
        open_planner = re.search(
            r"function openRecommendationPlanner\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(open_planner)
        self.assertNotIn("planningFetchJson", open_planner.group(1))
        self.assertIn("loadPlanningRecommendation().finally", self.html)
        self.assertRegex(
            self.html,
            r"(?s)@media \(max-width: 620px\).*?planning-recommendation-actions.*?repeat\(2",
        )

    def test_recommendation_customer_binding_never_falls_back_to_customer_row(self):
        binding = re.search(
            r"function planningRecommendationCustomer\(.*?\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(binding)
        body = binding.group(1)
        self.assertIn("suggestion.customer_id", body)
        self.assertIn("if (!customerId) return null", body)
        self.assertNotIn("customer_row", body)
        self.assertNotIn("customer.row", body)

    def test_planning_calendar_has_two_compact_time_lanes(self):
        self.assertIn("Telefon/Email", self.html)
        self.assertIn('aria-label="Besök"', self.html)
        self.assertIn("function planningCalendarLayout(activities, startMinutes)", self.html)
        self.assertIn('class="planning-calendar-hour"', self.html)
        self.assertNotIn('class="planning-calendar-event-time"', self.html)
        self.assertIn("const PLANNING_CALENDAR_PX_PER_MINUTE = 1.5", self.html)
        self.assertIn("contact-phone", self.html)
        self.assertIn("contact-email", self.html)
        card = re.search(
            r"function renderPlanningCalendarActivity\(item\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(card)
        self.assertIn("planning-activity-customer", card.group(1))
        self.assertNotIn("planning-activity-type", card.group(1))
        self.assertNotIn("planning-activity-note", card.group(1))
        self.assertNotIn("planning-activity-time", card.group(1))

    def test_planning_week_omits_legacy_followup_payload(self):
        self.assertIn('include_followups: "0"', self.html)

    def test_drag_drop_uses_pointer_events_handle_and_half_hour_snapping(self):
        self.assertIn('addEventListener("pointerdown", planningDragPointerDown)', self.html)
        self.assertIn('document.addEventListener("pointermove", planningDragPointerMove', self.html)
        self.assertIn('handle ? "handle" : "longpress"', self.html)
        self.assertIn("}, 340)", self.html)
        self.assertNotIn('event.pointerType !== "mouse" && !handle', self.html)
        self.assertRegex(
            self.html,
            r"(?s)@media \(pointer: coarse\).*?\.planning-drag-handle\s*\{.*?width:\s*44px;.*?height:\s*44px;",
        )
        self.assertIn("Math.round(rawMinutes / 30) * 30", self.html)
        self.assertIn("planning-drop-indicator", self.html)
        self.assertIn("planning-drag-ghost", self.html)
        self.assertIn("planningStartDragAutoScroll", self.html)

    def test_mobile_whole_card_long_press_preserves_scroll_and_prevents_selection(self):
        self.assertIn("-webkit-touch-callout: none", self.html)
        self.assertIn("-webkit-user-select: none", self.html)
        self.assertIn("window.getSelection()?.removeAllRanges()", self.html)
        self.assertIn('state.activationMode = "scroll"', self.html)
        self.assertIn("window.scrollBy(0, state.lastClientY - event.clientY)", self.html)
        self.assertIn("state.active || state.scrolled", self.html)
        self.assertIn('button.addEventListener("contextmenu"', self.html)

    def test_drag_drop_allows_only_owned_planned_or_skipped_activities(self):
        can_drag = re.search(
            r"function planningCanDragActivity\(activity\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(can_drag)
        self.assertIn('["planned", "skipped"]', can_drag.group(1))
        self.assertIn("activity.unplanned", can_drag.group(1))
        self.assertIn("activityOwner === loadedOwner", can_drag.group(1))

    def test_drag_patch_is_minimal_idempotent_and_conflict_safe(self):
        commit = re.search(
            r"async function planningCommitDraggedActivity\(activity, targetMinutes\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(commit)
        body = commit.group(1)
        for field in (
            "scheduled_at",
            "client_request_id",
            "expected_revision",
            "expected_updated_at",
        ):
            self.assertIn(field, body)
        self.assertNotIn("customer_id", body)
        self.assertNotIn("contact_type:", body)
        self.assertIn("retry?.requestBody || JSON.stringify(payload)", body)
        self.assertIn('method: "PATCH"', body)
        self.assertIn('["revision_conflict", "planning_changed"]', body)
        self.assertIn("await loadPlanningWeek()", body)
        self.assertIn('activity.source === "route" ? "manual"', body)

    def test_drag_cleanup_covers_escape_reload_and_view_switch(self):
        self.assertIn('event.key === "Escape"', self.html)
        self.assertIn('if (name !== "planning") planningCancelDrag()', self.html)
        load_week = re.search(
            r"async function loadPlanningWeek\(\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(load_week)
        self.assertIn("planningCancelDrag()", load_week.group(1))
        self.assertIn('document.removeEventListener("pointermove", planningDragPointerMove)', self.html)
        pointer_move = re.search(
            r"function planningDragPointerMove\(event\) \{(.*?)\n  \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(pointer_move)
        self.assertNotIn("planningFetchJson", pointer_move.group(1))

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

    def test_list_view_hides_legacy_route_proposal_flow(self):
        list_view = re.search(
            r'<div class="view active" id="view-list">(.*?)<!-- .*?FOLLOW-UP INSIGHTS VIEW',
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(list_view)
        self.assertNotIn('id="chip-route-proposal"', list_view.group(1))
        self.assertNotIn('id="route-proposal-panel"', list_view.group(1))
        self.assertIn('id="route-mode-btn"', list_view.group(1))
        self.assertIn(
            'id="planning-route-preview-btn" type="button">Skapa ruttförslag</button>',
            self.html,
        )
        self.assertIn("LEGACY ROLLBACK SUPPORT", self.html)

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

    def test_route_preview_persists_and_replays_the_exact_pending_request(self):
        self.assertIn(
            '"store-tracker:route-preview-recovery:v1"',
            self.html,
        )
        self.assertIn("sessionStorage.setItem(", self.html)
        self.assertIn("sessionStorage.removeItem(", self.html)
        self.assertIn("PLANNING_ROUTE_RECOVERY_TTL_MS = 30 * 60 * 1000", self.html)
        self.assertIn("PLANNING_ROUTE_RECOVERY_POLL_MS = 15 * 1000", self.html)
        read = self.html.split("function readPlanningRouteRecoveryState", 1)[1].split(
            "function planningRouteRecoveryForCurrentContext", 1
        )[0]
        self.assertIn("if (!valid)", read)
        self.assertIn("const actorUserName = String(currentUser?.user_name || \"\").trim()", read)
        self.assertIn("if (!actorUserName) return null", read)
        self.assertIn("if (state.actor_user_name !== actorUserName)", read)
        self.assertEqual(read.count("clearPlanningRouteRecoveryState()"), 3)
        save = self.html.split("function savePlanningRouteRecoveryState", 1)[1].split(
            "function planningRouteError", 1
        )[0]
        self.assertIn("try {", save)
        self.assertIn("sessionStorage.setItem(", save)
        self.assertIn("state.storage_persisted = false", save)
        self.assertIn("return state", save)

        create = self.html.split("async function openPlanningRoutePreview()", 1)[1].split(
            "function renderPlanningRoutePreview", 1
        )[0]
        self.assertLess(create.index("getCurrentPositionForRoute()"), create.index("savePlanningRouteRecoveryState(payload)"))
        self.assertLess(create.index("savePlanningRouteRecoveryState(payload)"), create.index("postPendingPlanningRoutePreview(state"))

        recovery = self.html.split("function resumePlanningRoutePreviewRecovery", 1)[1].split(
            "async function openPlanningRoutePreview", 1
        )[0]
        self.assertIn("planningRoutePreviewStatus(state.payload.client_request_id)", recovery)
        self.assertIn('status.state === "completed"', recovery)
        self.assertIn("completedReplay: true", recovery)
        self.assertNotIn("getCurrentPositionForRoute", recovery)
        self.assertNotIn("planningClientRequestId", recovery)
        self.assertIn('window.addEventListener("online"', self.html)
        self.assertIn('document.addEventListener("visibilitychange"', self.html)

    def test_route_preview_recovery_clears_after_render_and_never_applies(self):
        recovery = self.html.split("function planningRouteCurrentOwnerUserName", 1)[1].split(
            "function renderPlanningRoutePreview", 1
        )[0]
        rendered = recovery.split("function renderRecoveredPlanningRoutePreview", 1)[1].split(
            "async function postPendingPlanningRoutePreview", 1
        )[0]
        self.assertLess(rendered.index("renderPlanningRoutePreview(payload)"), rendered.index("planningRouteApplyRequestId"))
        self.assertLess(rendered.index("planningRouteApplyRequestId"), rendered.index("clearPlanningRouteRecoveryState()"))
        self.assertIn('kind: "ambiguous_transport_or_body_failure"', recovery)
        self.assertIn('outcome.kind === "in_progress"', recovery)
        self.assertIn('outcome.kind === "terminal_backend_error"', recovery)
        self.assertIn("planningRoutePreviewFetch(state.payload)", recovery)
        self.assertNotIn("/planning/route-apply", recovery)

    def test_route_preview_context_switch_resets_ui_and_does_not_share_single_flight(self):
        recovery = self.html.split("function planningRouteRecoveryForCurrentContext", 1)[1].split(
            "async function openPlanningRoutePreview", 1
        )[0]
        resume = recovery.split("function resumePlanningRoutePreviewRecovery", 1)[1]
        self.assertIn("!planningRouteRecoveryStateMatchesCurrentContext(storedState)", resume)
        self.assertIn("window.clearTimeout(planningRouteRecoveryTimer)", resume)
        self.assertIn("planningRouteResetPreviewButton()", resume)
        self.assertIn("planningRouteRecoveryStateIsActive(state)", recovery)
        self.assertIn("planningRouteRecoveryPromiseKey === requestKey", recovery)
        self.assertIn("planningRouteRecoveryPromise === promise", recovery)
        self.assertIn(
            "runPlanningRouteRecoverySingleFlight(state.payload.client_request_id",
            recovery,
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
        self.assertIn("planningAgendaItemsForDate(planningSelectedDate)", self.html)

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
