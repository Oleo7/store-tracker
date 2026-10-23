from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch
from uuid import UUID

from gspread.exceptions import WorksheetNotFound

from customer_master_sync.sync import (
    CUSTOMER_REQUIRED_COLUMNS,
    GLN_COLUMN,
    REVIEW_SHEET_NAME,
    REVIEW_COLUMNS,
    build_order_masters,
    _result_payload,
    is_gln_like,
    is_organization_number_like,
    normalize_gln,
    plan_customer_sync,
    run_customer_sync,
)


ORDER_HEADERS = [
    "Reference",
    "Order date",
    "Customer",
    "Customer number",
    "Address",
    "Number",
    "Postal code",
    "City",
    "buyerEmail",
]
CUSTOMER_HEADERS = [
    "customer",
    "cancelled_flag",
    "sales_person",
    "customer_id",
    "customer_number",
    "email_last_order",
    "city_google",
    "address_google",
    "address_number_google",
    "postal_code_google",
]


def order(
    reference,
    order_date,
    customer,
    customer_number,
    *,
    address="Hantverkargatan",
    number="1",
    postal="11152",
    city="Stockholm",
    email="buyer@example.com",
    gln="",
    cid="",
):
    return {
        "Reference": reference,
        "Order date": order_date,
        "Customer": customer,
        "Customer number": customer_number,
        "Address": address,
        "Number": number,
        "Postal code": postal,
        "City": city,
        "buyerEmail": email,
        "Buyer number": gln,
        "customer_id": cid,
    }


def customer(
    name,
    customer_number="",
    *,
    email="",
    address="Hantverkargatan",
    number="1",
    postal="11152",
    city="Stockholm",
    sales_person="Sofia",
    gln="",
):
    return {
        "customer": name,
        "cancelled_flag": "",
        "sales_person": sales_person,
        "customer_id": (
            "11111111-1111-4111-8111-"
            f"{abs(hash(name)) % 10**12:012d}"
        ),
        "customer_number": customer_number,
        "email_last_order": email,
        "city_google": city,
        "address_google": address,
        "address_number_google": number,
        "postal_code_google": postal,
        GLN_COLUMN: gln,
    }


class CustomerMasterPlanningTests(TestCase):
    def test_latest_order_name_wins_for_shared_customer_number(self):
        rows = [
            order("OLD", "2026-01-01", "Old Store", "1001"),
            order("NEW", "2026-05-01", "New Store", "1001"),
            # A second product row from the older order must not affect the result.
            order("OLD", "2026-01-01", "Old Store", "1001"),
        ]

        masters = build_order_masters(rows)

        self.assertEqual(len(masters), 1)
        self.assertEqual(masters[0].master_name, "New Store")
        self.assertEqual(masters[0].reference, "NEW")
        self.assertEqual(masters[0].order_count, 2)

    def test_customer_number_match_plans_safe_name_change(self):
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "New Store", "1001")],
            [customer("Old Store", "1001", email="old@example.com")],
        )

        self.assertEqual(decisions[0].status, "safe_update_name")
        self.assertEqual(decisions[0].matched_by, "customer_number")
        self.assertTrue(decisions[0].changes_name)
        self.assertTrue(decisions[0].changes_email)

    def test_unique_name_match_backfills_blank_customer_number(self):
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "Same Store", "1001")],
            [customer("same store", "")],
        )

        self.assertEqual(decisions[0].status, "safe_update_case_spacing")
        self.assertTrue(decisions[0].backfills_customer_number)

    def test_gln_is_external_identity_and_never_backfills_customer_number(self):
        gln = "7300120154304"
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "Stora Coop Tumba", gln)],
            [customer("Stora Coop Tumba", "")],
        )

        self.assertTrue(is_gln_like(gln))
        self.assertEqual(decisions[0].status, "exact_match")
        self.assertEqual(decisions[0].customer_number, "")
        self.assertEqual(decisions[0].source_customer_identifier, gln)
        self.assertFalse(decisions[0].backfills_customer_number)
        self.assertTrue(decisions[0].backfills_gln)

    def test_organization_number_never_backfills_customer_number(self):
        organization_number = "559401-8045"
        decisions = plan_customer_sync(
            [
                order(
                    "NEW",
                    "2026-05-01",
                    "ICA Kvantum Brunnshög",
                    organization_number,
                )
            ],
            [customer("ICA Kvantum Brunnshög", "")],
        )

        self.assertTrue(
            is_organization_number_like(organization_number)
        )
        self.assertEqual(decisions[0].customer_number, "")
        self.assertEqual(
            decisions[0].source_customer_identifier,
            organization_number,
        )
        self.assertFalse(decisions[0].backfills_customer_number)

    def test_gln_orders_join_later_real_number_by_exact_identity(self):
        rows = [
            order(
                "OLD",
                "2026-01-01",
                "ICA Kvantum Sjöbo",
                "7301004000007",
                address="Gamla Torg",
                number="1",
                postal="27530",
                city="Sjöbo",
            ),
            order(
                "NEW",
                "2026-02-01",
                "ICA Kvantum Sjöbo",
                "1376",
                address="Gamla Torg",
                number="1",
                postal="27530",
                city="Sjöbo",
            ),
        ]

        masters = build_order_masters(rows)

        self.assertEqual(len(masters), 1)
        self.assertEqual(masters[0].customer_number, "1376")
        self.assertEqual(masters[0].order_count, 2)

    def test_exact_address_match_never_overwrites_conflicting_number(self):
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "New Store", "1001")],
            [customer("Old Store", "DIFFERENT")],
        )

        self.assertEqual(decisions[0].status, "needs_review")
        self.assertIn("conflicting", decisions[0].reason)

    def test_fuzzy_candidate_blocks_automatic_append(self):
        decisions = plan_customer_sync(
            [
                order(
                    "NEW",
                    "2026-05-01",
                    "ICA Nära Storgatan",
                    "1001",
                    address="Annan gata",
                    number="5",
                )
            ],
            [
                customer(
                    "ICA Nära Storg.",
                    "2002",
                    address="Tredje gatan",
                    number="8",
                )
            ],
        )

        self.assertEqual(decisions[0].status, "needs_review")
        self.assertEqual(decisions[0].matched_by, "possible_duplicate")

    def test_strong_name_similarity_blocks_malmborgs_duplicate(self):
        decisions = plan_customer_sync(
            [
                order(
                    "NEW",
                    "2026-05-01",
                    "Ica Malmborgs Limhamn",
                    "7301004027370",
                    address="Linnegatan",
                    number="40",
                    postal="21614",
                    city="Limhamn",
                )
            ],
            [
                customer(
                    "ICA Kvantum Malmborgs Limhamn",
                    "1076",
                    address="Annan gata",
                    number="8",
                    postal="21614",
                    city="Limhamn",
                )
            ],
        )

        self.assertEqual(decisions[0].status, "needs_review")
        self.assertEqual(decisions[0].matched_by, "possible_duplicate")

    def test_unmatched_stable_identity_is_new_customer(self):
        decisions = plan_customer_sync(
            [
                order(
                    "NEW",
                    "2026-05-01",
                    "Completely New Store",
                    "1001",
                    address="Nyvägen",
                    number="9",
                    postal="22222",
                    city="Malmö",
                )
            ],
            [customer("Existing Store", "2002")],
        )

        self.assertEqual(decisions[0].status, "new_customer")

    def test_duplicate_customer_number_is_review_only(self):
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "New Store", "1001")],
            [
                customer("Store One", "1001"),
                customer("Store Two", "1001", address="Other", number="2"),
            ],
        )

        self.assertEqual(decisions[0].status, "needs_review")
        self.assertIn("multiple customer rows", decisions[0].reason)

    def test_name_change_does_not_create_duplicate_master_name(self):
        decisions = plan_customer_sync(
            [order("NEW", "2026-05-01", "New Store", "1001")],
            [
                customer("Old Store", "1001"),
                customer("New Store", "2002", address="Other", number="2"),
            ],
        )

        self.assertEqual(decisions[0].status, "needs_review")
        self.assertIn("already belongs", decisions[0].reason)

    def test_missing_required_customer_column_fails_closed(self):
        incomplete = customer("Existing Store", "2002")
        del incomplete["postal_code_google"]

        with self.assertRaisesRegex(ValueError, "postal_code_google"):
            plan_customer_sync(
                [order("NEW", "2026-05-01", "New Store", "1001")],
                [incomplete],
            )

    def test_internal_customers_are_ignored(self):
        decisions = plan_customer_sync(
            [
                order("INTERNAL-1", "2026-05-01", "Polarbär - Inköp", ""),
                order("INTERNAL-2", "2026-05-02", "Polarbär - Inköp", ""),
            ],
            [customer("Existing Store", "2002")],
        )

        self.assertEqual(len(decisions), 1)
        self.assertEqual(decisions[0].status, "ignored_internal")


MASTHUGGET_GLN = "7300121361015"
ANDRA_GLN = "7300121360346"
MASTHUGGET_ID = "613ecf21-7e50-4c51-86d9-8f2d68aa59fc"


def masthugget_order(**changes):
    row = order("POR26AHGWQP8Q", "2026-10-20", "Coop Masthugget", "",
                gln=MASTHUGGET_GLN, address="Första Långgatan", number="11C",
                postal="", city="Göteborg")
    if "cid" in changes:
        changes["customer_id"] = changes.pop("cid")
    row.update(changes)
    return row


def masthugget_customer(**changes):
    row = customer("Coop Masthugget", address="Första Långgatan", number="11C",
                   postal="", city="Göteborg")
    row["customer_id"] = MASTHUGGET_ID
    row.update(changes)
    return row


class GlnIdentityTests(TestCase):
    def test_gln_format_checksum_and_external_number_exclusion(self):
        for value in (MASTHUGGET_GLN, " 7300 1213 6101 5 ", "730012136101-5",
                      int(MASTHUGGET_GLN), float(MASTHUGGET_GLN)):
            with self.subTest(value=value):
                self.assertEqual(normalize_gln(value), MASTHUGGET_GLN)
        for value in ("7300121361014", "0000000000000", "730012136101", "uuid", True):
            with self.subTest(value=value):
                self.assertEqual(normalize_gln(value), "")
        for row in (masthugget_order(**{"Buyer number": "7300121361014"}),
                    masthugget_order(**{"Buyer number": "", "Customer number": "730012136101-4"}),
                    masthugget_order(**{"Customer number": ANDRA_GLN})):
            self.assertEqual(plan_customer_sync([row], [])[0].status, "needs_review")
        for column in ("Buyer number", "Customer number"):
            row = masthugget_order(**{"Buyer number": "", column: float(MASTHUGGET_GLN)})
            decision = plan_customer_sync([row], [])[0]
            self.assertEqual(decision.stockfiller_gln, MASTHUGGET_GLN)
            self.assertEqual(decision.customer_number, "")

    def test_distinct_coop_locations_do_not_block_registration_without_postcode(self):
        other = customer("Coop Andra Långgatan", gln=ANDRA_GLN,
                         address="Andra Långgatan", number="20", postal="", city="Göteborg")
        decision = plan_customer_sync([masthugget_order()], [other])[0]
        self.assertEqual(decision.status, "new_customer")
        self.assertEqual(decision.stockfiller_gln, MASTHUGGET_GLN)
        self.assertEqual(decision.customer_number, "")

    def test_safe_complements_supply_distinct_gln_evidence_in_same_snapshot(self):
        other = customer("Coop Andra Långgatan", address="Andra Långgatan", number="20",
                         postal="", city="Göteborg")
        other_order = masthugget_order(Reference="OTHER", Customer=other["customer"],
                                      Address="Andra Långgatan", Number="20",
                                      **{"Buyer number": ANDRA_GLN})
        for rows in ([masthugget_order(), other_order], [other_order, masthugget_order()]):
            decisions = plan_customer_sync(rows, [other])
            self.assertEqual({d.master_customer: d.status for d in decisions},
                             {"Coop Masthugget": "new_customer", "Coop Andra Långgatan": "exact_match"})

    def test_manual_customer_matches_normalized_street_without_postcode(self):
        row = masthugget_order(Address=" FÖRSTA   LÅNGGATAN 11 c ", Number="", City="GÖTEBORG")
        decision = plan_customer_sync([row] * 3, [masthugget_customer()])[0]
        self.assertEqual(decision.status, "exact_match")
        self.assertEqual(decision.existing_customer_id, MASTHUGGET_ID)
        self.assertTrue(decision.backfills_gln)
        self.assertEqual(decision.order_row_numbers, (2, 3, 4))
        conflicting = masthugget_order(Number="11-13")
        self.assertEqual(plan_customer_sync([conflicting], [masthugget_customer()])[0].status, "needs_review")

    def test_known_gln_handles_later_number_and_safe_rename_without_replacing_uuid(self):
        existing = masthugget_customer(stockfiller_gln=MASTHUGGET_GLN)
        rows = [masthugget_order(), masthugget_order(Reference="LATER", **{
            "Order date": "2026-10-22", "Customer number": "1234", "Customer": "Coop Masthugget City"})]
        decision = plan_customer_sync(rows, [existing])[0]
        self.assertEqual(decision.matched_by, GLN_COLUMN)
        self.assertEqual(decision.status, "safe_update_name")
        self.assertTrue(decision.backfills_customer_number)
        self.assertEqual(decision.existing_customer_id, MASTHUGGET_ID)
        self.assertEqual(len(build_order_masters(rows)), 1)

    def test_internal_identity_and_gln_conflicts_require_review(self):
        first = masthugget_customer(stockfiller_gln=MASTHUGGET_GLN, customer_number="1")
        second = customer("Other shop", "2", gln=ANDRA_GLN, address="Elsewhere", city="Malmö")
        cases = [
            ([masthugget_order(**{"Customer number": "2"})], [first, second]),
            ([masthugget_order(cid=second["customer_id"])], [first, second]),
            ([masthugget_order(cid="unknown-uuid")], [first]),
            ([masthugget_order()], [first, {**second, GLN_COLUMN: MASTHUGGET_GLN}]),
            ([masthugget_order()], [{**first, GLN_COLUMN: "invalid"}]),
            ([masthugget_order()], [{**first, "customer_id": ""}]),
            ([masthugget_order()], [{**first, GLN_COLUMN: ANDRA_GLN}]),
            ([masthugget_order()], [first, {**second, "customer_id": MASTHUGGET_ID}]),
        ]
        for rows, customers in cases:
            with self.subTest(rows=rows, customers=customers):
                decisions = plan_customer_sync(rows, customers)
                self.assertTrue(all(d.status == "needs_review" for d in decisions))
                self.assertFalse(any(d.backfills_gln for d in decisions))

    def test_shared_gln_or_multiple_glns_per_internal_identity_require_review(self):
        cases = [
            [masthugget_order(), masthugget_order(Reference="OTHER", Customer="Different shop",
                                                 Address="Other street", Number="2")],
            [masthugget_order(cid=MASTHUGGET_ID),
             masthugget_order(Reference="OTHER", cid=MASTHUGGET_ID, **{"Buyer number": ANDRA_GLN})],
            [masthugget_order(), masthugget_order(Customer="Other shop")],
        ]
        for rows in cases:
            with self.subTest(rows=rows):
                decisions = plan_customer_sync(rows, [masthugget_customer()])
                self.assertTrue(all(d.status == "needs_review" for d in decisions))
        reused = masthugget_order(Customer="Different shop", Address="Other street", Number="2")
        self.assertEqual(plan_customer_sync([reused], [masthugget_customer(stockfiller_gln=MASTHUGGET_GLN)])[0].status,
                         "needs_review")

    def test_weak_evidence_never_coalesces_same_name_at_different_addresses(self):
        rows = [masthugget_order(**{"Buyer number": ""}),
                masthugget_order(Reference="OTHER", Address="Other street", **{"Buyer number": ""})]
        self.assertEqual(len(build_order_masters(rows)), 2)
        self.assertTrue(all(d.status == "needs_review" for d in plan_customer_sync(rows, [])))
        partial = masthugget_order(Address="", Number="")
        self.assertEqual(plan_customer_sync([partial], [])[0].status, "needs_review")

    def test_strong_name_similarity_still_blocks_append_despite_different_glns(self):
        other = customer("ICA Kvantum Malmborgs Limhamn", gln=ANDRA_GLN,
                         address="Erikslustvägen", number="20", city="Malmö", postal="")
        row = masthugget_order(Customer="Ica Malmborgs Limhamn", Address="Linnégatan", Number="5", City="Malmö")
        decision = plan_customer_sync([row], [other])[0]
        self.assertEqual(decision.status, "needs_review")
        self.assertEqual(decision.matched_by, "possible_duplicate")

    def test_real_solvedborg_stores_remain_separate_without_existing_gln_or_history(self):
        # CRM_DATABASE regression: ICA has neither GLN nor Stockfiller history.
        other = customer("ICA Supermarket Sölvesborg", address="Järnvägsgatan", number="1",
                         city="Sölvesborg", postal="")
        row = order("COOP-SOLVESBORG", "2026-10-23", "Coop Sölvesborg", "",
                    gln="7300120952603", address="Järnvägsgatan", number="6", city="Sölvesborg", postal="")
        decision = plan_customer_sync([row], [other])[0]
        self.assertEqual(decision.status, "new_customer")
        self.assertEqual(decision.stockfiller_gln, "7300120952603")
        self.assertIsNone(decision.matched_row)

    def test_distinct_chain_evidence_still_requires_known_separate_house_numbers(self):
        row = order("COOP-SOLVESBORG", "2026-10-23", "Coop Sölvesborg", "",
                    gln="7300120952603", address="Järnvägsgatan", number="6", city="Sölvesborg", postal="")
        for changes in ({"number": "6"}, {"number": "6A"}, {"number": "1-6"}, {"number": ""},
                        {"name": "Coop Supermarket Sölvesborg"}, {"name": "Supermarket Sölvesborg"}):
            with self.subTest(changes=changes):
                values = dict(name="ICA Supermarket Sölvesborg", address="Järnvägsgatan", number="1",
                              city="Sölvesborg", postal="")
                values.update(changes)
                decision = plan_customer_sync([row], [customer(**values)])[0]
                self.assertEqual(decision.status, "needs_review")

    def test_name_only_cannot_assign_new_gln_to_incomplete_customer_address(self):
        for missing in ("address_google", "address_number_google", "city_google"):
            with self.subTest(missing=missing):
                existing = masthugget_customer(**{missing: ""})
                decision = plan_customer_sync([masthugget_order()], [existing])[0]
                self.assertEqual(decision.status, "needs_review")
                self.assertFalse(decision.backfills_gln)
        existing = masthugget_customer(address_google="", address_number_google="", city_google="")
        row = masthugget_order(**{"Customer number": "new-unmatched-number"})
        self.assertEqual(plan_customer_sync([row], [existing])[0].status, "needs_review")

    def test_established_masthugget_uuid_allows_gln_complement_with_incomplete_address(self):
        existing = masthugget_customer(address_google="", address_number_google="", city_google="")
        rows = [masthugget_order(cid=MASTHUGGET_ID)] * 3
        decision = plan_customer_sync(rows, [existing])[0]
        self.assertEqual(decision.status, "exact_match")
        self.assertEqual(decision.matched_by, "customer_id")
        self.assertEqual(decision.existing_customer_id, MASTHUGGET_ID)
        self.assertTrue(decision.backfills_gln)
        self.assertEqual(decision.order_row_numbers, (2, 3, 4))

    def test_established_internal_number_allows_gln_complement_with_incomplete_address(self):
        existing = masthugget_customer(customer_number="1234", address_google="", address_number_google="", city_google="")
        row = masthugget_order(**{"Customer number": "1234"})
        decision = plan_customer_sync([row], [existing])[0]
        self.assertEqual(decision.status, "exact_match")
        self.assertEqual(decision.matched_by, "customer_number")
        self.assertTrue(decision.backfills_gln)

    def test_apply_gln_append_schema_report_and_review_resolution_are_idempotent(self):
        headers = CUSTOMER_HEADERS + ["manual_note", "lat", "lng"]
        existing = masthugget_customer(manual_note="keep", lat="57.7", lng="11.9")
        sheet = FakeWorksheet("customers_enriched", [headers, _row(headers, existing)])
        legacy = FakeWorksheet("customers", [["customer"], ["legacy unchanged"]])
        spreadsheet = FakeSpreadsheet({"customers_enriched": sheet, "customers": legacy,
                                       REVIEW_SHEET_NAME: FakeWorksheet(REVIEW_SHEET_NAME, [["old"], ["resolved"]])})
        review = spreadsheet.worksheet(REVIEW_SHEET_NAME)
        review.col_count = len(REVIEW_COLUMNS) - 2
        for title in ("sales_activities", "email_messages", "email_recipients"):
            spreadsheet.sheets[title] = FakeWorksheet(title, [["customer"]])
        rows = [masthugget_order()] * 3
        before = deepcopy(sheet.values)
        report = _result_payload(run_customer_sync(spreadsheet, mode="dry_run", order_rows=rows))
        self.assertEqual(sheet.values, before)
        self.assertEqual(report["backfilled_glns"], 1)
        self.assertEqual(report["proposed_changes"][0]["existing_stockfiller_gln"], "")
        self.assertEqual(report["proposed_changes"][0][GLN_COLUMN], MASTHUGGET_GLN)
        first = run_customer_sync(spreadsheet, mode="apply", order_rows=rows)
        self.assertEqual(first.appended_customers, 0)
        self.assertEqual(sheet.values[0], headers + [GLN_COLUMN])
        self.assertEqual(sheet.values[1][:-1], before[1][:headers.index("email_last_order")]
                         + ["buyer@example.com"] + before[1][headers.index("email_last_order") + 1:])
        self.assertEqual(sheet.values[1][-1], MASTHUGGET_GLN)
        self.assertEqual(len(spreadsheet.worksheet(REVIEW_SHEET_NAME).values), 1)
        self.assertEqual(review.col_count, len(REVIEW_COLUMNS))
        snapshot = deepcopy(sheet.values)
        again = run_customer_sync(spreadsheet, mode="apply", order_rows=rows)
        self.assertEqual(again.backfilled_glns, 0)
        self.assertEqual(sheet.values, snapshot)
        self.assertEqual(legacy.values, [["customer"], ["legacy unchanged"]])

    def test_retry_after_append_before_review_write_does_not_append_again(self):
        sheet = FakeWorksheet("customers_enriched", [CUSTOMER_HEADERS])
        review = FakeWorksheet(REVIEW_SHEET_NAME, [["status"]])
        spreadsheet = FakeSpreadsheet({"customers_enriched": sheet, REVIEW_SHEET_NAME: review})
        for title in ("sales_activities", "email_messages", "email_recipients"):
            spreadsheet.sheets[title] = FakeWorksheet(title, [["customer"]])
        with patch.object(review, "clear", side_effect=RuntimeError("review write failed")):
            with self.assertRaisesRegex(RuntimeError, "review write failed"):
                run_customer_sync(spreadsheet, mode="apply", order_rows=[masthugget_order()])
        self.assertEqual(len(sheet.values), 2)
        before = deepcopy(sheet.values)
        result = run_customer_sync(spreadsheet, mode="apply", order_rows=[masthugget_order()])
        self.assertEqual(result.appended_customers, 0)
        self.assertEqual(sheet.values, before)

    def test_new_gln_customer_has_uuid_raw_address_and_no_invented_enrichment(self):
        headers = CUSTOMER_HEADERS + ["lat", "lng", "manual_note"]
        sheet = FakeWorksheet("customers_enriched", [headers])
        spreadsheet = FakeSpreadsheet({"customers_enriched": sheet})
        for title in ("sales_activities", "email_messages", "email_recipients"):
            spreadsheet.sheets[title] = FakeWorksheet(title, [["customer"]])
        run_customer_sync(spreadsheet, mode="apply", order_rows=[masthugget_order()])
        row = dict(zip(sheet.values[0], sheet.values[1]))
        self.assertEqual(UUID(row["customer_id"]).version, 4)
        self.assertEqual(row[GLN_COLUMN], MASTHUGGET_GLN)
        self.assertEqual(row["address_google"], "Första Långgatan")
        self.assertEqual(row["address_number_google"], "11C")
        for column in ("customer_number", "postal_code_google", "lat", "lng", "sales_person", "manual_note"):
            self.assertEqual(row[column], "")
        second = run_customer_sync(spreadsheet, mode="apply", order_rows=[masthugget_order()])
        self.assertEqual(second.appended_customers, 0)
        self.assertEqual(len(sheet.values), 2)

    def test_gln_rename_preserves_history_planning_owner_and_comments(self):
        headers = CUSTOMER_HEADERS + [GLN_COLUMN, "comment"]
        existing = masthugget_customer(stockfiller_gln=MASTHUGGET_GLN, customer_number="1234", comment="keep")
        planning_headers = ["activity_id", "customer_id", "customer", "customer_number", "customer_key", "note", "sales_person"]
        sheets = {
            "customers_enriched": FakeWorksheet("customers_enriched", [headers, _row(headers, existing)]),
            "planned_activities": FakeWorksheet("planned_activities", [planning_headers,
                ["plan-1", MASTHUGGET_ID, existing["customer"], "1234", "1234", "call first", "Sofia"]]),
        }
        for title in ("sales_activities", "email_messages", "email_recipients"):
            sheets[title] = FakeWorksheet(title, [["customer", "customer_id", "note", "sales_person"],
                                                [existing["customer"], MASTHUGGET_ID, "keep", "Sofia"]])
        spreadsheet = FakeSpreadsheet(sheets)
        row = masthugget_order(Customer="Coop Masthugget City")
        result = run_customer_sync(spreadsheet, mode="apply", order_rows=[row])
        self.assertEqual(result.updated_names, 1)
        for title in ("sales_activities", "email_messages", "email_recipients"):
            self.assertEqual(sheets[title].values[1], ["Coop Masthugget City", MASTHUGGET_ID, "keep", "Sofia"])
        self.assertEqual(sheets["planned_activities"].values[1],
                         ["plan-1", MASTHUGGET_ID, "Coop Masthugget City", "1234", "1234", "call first", "Sofia"])
        actual = dict(zip(headers, sheets["customers_enriched"].values[1]))
        for key in ("customer_id", "customer_number", "comment", "sales_person"):
            self.assertEqual(actual[key], existing[key])


class CustomerMasterApplyTests(TestCase):
    def setUp(self):
        self.spreadsheet = FakeSpreadsheet(
            {
                "order_rows": FakeWorksheet(
                    "order_rows",
                    [
                        ORDER_HEADERS,
                        _row(
                            ORDER_HEADERS,
                            order("OLD", "2026-01-01", "Old Store", "1001"),
                        ),
                        _row(
                            ORDER_HEADERS,
                            order(
                                "NEW",
                                "2026-05-01",
                                "New Store",
                                "1001",
                                email="new@example.com",
                            ),
                        ),
                        _row(
                            ORDER_HEADERS,
                            order(
                                "ADDED",
                                "2026-05-02",
                                "Brand New Store",
                                "7301004000007",
                                address="Nyvägen",
                                number="9",
                                postal="22222",
                                city="Malmö",
                                email="brand-new@example.com",
                            ),
                        ),
                    ],
                ),
                "customers_enriched": FakeWorksheet(
                    "customers_enriched",
                    [
                        CUSTOMER_HEADERS,
                        _row(
                            CUSTOMER_HEADERS,
                            customer(
                                "Old Store",
                                "1001",
                                email="old@example.com",
                            ),
                        ),
                    ],
                ),
                "sales_activities": FakeWorksheet(
                    "sales_activities",
                    [
                        ["date_time", "customer", "comment"],
                        ["2026-01-02", "Old Store", "keep"],
                    ],
                ),
                "email_messages": FakeWorksheet(
                    "email_messages",
                    [
                        ["email_id", "customer", "customer_number"],
                        ["mail-1", "Old Store", "1001"],
                    ],
                ),
                "email_recipients": FakeWorksheet(
                    "email_recipients",
                    [
                        ["email_id", "customer", "actual_email"],
                        ["mail-1", "Old Store", "old@example.com"],
                    ],
                ),
            }
        )

    def test_apply_renames_history_then_customer_and_appends_once(self):
        original_sales_headers = list(
            self.spreadsheet.worksheet("sales_activities").values[0]
        )

        result = run_customer_sync(self.spreadsheet, mode="apply")

        self.assertEqual(result.updated_names, 1)
        self.assertEqual(result.appended_customers, 1)
        customers = self.spreadsheet.worksheet("customers_enriched").values
        self.assertEqual(customers[1][CUSTOMER_HEADERS.index("customer")], "New Store")
        self.assertEqual(
            customers[1][CUSTOMER_HEADERS.index("email_last_order")],
            "new@example.com",
        )
        self.assertEqual(
            customers[2][CUSTOMER_HEADERS.index("customer")],
            "Brand New Store",
        )
        self.assertEqual(
            customers[2][CUSTOMER_HEADERS.index("customer_number")],
            "",
        )
        self.assertEqual(result.ignored_gln_identifiers, 1)
        self.assertEqual(
            self.spreadsheet.worksheet("sales_activities").values[1][1],
            "New Store",
        )
        self.assertEqual(
            self.spreadsheet.worksheet("email_messages").values[1][1],
            "New Store",
        )
        self.assertEqual(
            self.spreadsheet.worksheet("email_recipients").values[1][1],
            "New Store",
        )
        self.assertEqual(
            self.spreadsheet.worksheet("sales_activities").values[0],
            original_sales_headers,
        )
        self.assertEqual(
            self.spreadsheet.worksheet("sales_activities").values[1][2],
            "keep",
        )
        review = self.spreadsheet.worksheet(REVIEW_SHEET_NAME)
        self.assertTrue(review.hidden)

        second = run_customer_sync(self.spreadsheet, mode="apply")

        self.assertEqual(second.updated_names, 0)
        self.assertEqual(second.appended_customers, 0)
        self.assertEqual(len(self.spreadsheet.worksheet("customers_enriched").values), 3)

    def test_dry_run_does_not_mutate_any_sheet(self):
        before = {
            name: deepcopy(sheet.values)
            for name, sheet in self.spreadsheet.sheets.items()
        }

        result = run_customer_sync(self.spreadsheet, mode="dry_run")

        self.assertEqual(result.updated_names, 1)
        self.assertEqual(result.appended_customers, 1)
        self.assertEqual(
            {
                name: sheet.values
                for name, sheet in self.spreadsheet.sheets.items()
            },
            before,
        )

    def test_missing_history_sheet_fails_before_customer_name_write(self):
        del self.spreadsheet.sheets["sales_activities"]

        with self.assertRaises(ValueError):
            run_customer_sync(self.spreadsheet, mode="apply")

        customers = self.spreadsheet.worksheet("customers_enriched").values
        self.assertEqual(customers[1][CUSTOMER_HEADERS.index("customer")], "Old Store")

    def test_retry_finishes_after_history_was_renamed_before_customer_failure(self):
        customer_sheet = self.spreadsheet.worksheet("customers_enriched")
        customer_sheet.fail_batch_once = True

        with self.assertRaises(RuntimeError):
            run_customer_sync(self.spreadsheet, mode="apply")

        self.assertEqual(
            self.spreadsheet.worksheet("sales_activities").values[1][1],
            "New Store",
        )
        self.assertEqual(
            customer_sheet.values[1][CUSTOMER_HEADERS.index("customer")],
            "Old Store",
        )

        result = run_customer_sync(self.spreadsheet, mode="apply")

        self.assertEqual(result.updated_names, 1)
        self.assertEqual(
            customer_sheet.values[1][CUSTOMER_HEADERS.index("customer")],
            "New Store",
        )


class FakeWorksheet:
    def __init__(self, title, values):
        self.title = title
        self.values = [list(row) for row in values]
        self.hidden = False
        self.fail_batch_once = False
        self.col_count = max((len(row) for row in values), default=0)

    def add_cols(self, count):
        self.col_count += count

    def get_all_values(self, **kwargs):
        return [list(row) for row in self.values]

    def batch_update(self, data, value_input_option=None):
        if self.fail_batch_once:
            self.fail_batch_once = False
            raise RuntimeError("simulated customer write failure")
        for item in data:
            match = __import__("re").fullmatch(
                r"([A-Z]+)(\d+):([A-Z]+)(\d+)",
                item["range"],
            )
            if not match:
                raise ValueError(f"Unsupported range: {item['range']}")
            column = _column_index(match.group(1))
            row_number = int(match.group(2))
            self._set_cell(row_number, column + 1, item["values"][0][0])

    def append_rows(self, rows, value_input_option=None):
        self.values.extend([list(row) for row in rows])

    def clear(self):
        self.values = []

    def update(self, values, range_name="A1", raw=True):
        if range_name != "A1":
            raise ValueError(f"Unsupported range: {range_name}")
        self.values = [list(row) for row in values]

    def hide(self):
        self.hidden = True

    def _set_cell(self, row_number, column_number, value):
        while len(self.values) < row_number:
            self.values.append([])
        row = self.values[row_number - 1]
        while len(row) < column_number:
            row.append("")
        row[column_number - 1] = value


class FakeSpreadsheet:
    def __init__(self, sheets):
        self.sheets = dict(sheets)

    def worksheet(self, title):
        try:
            return self.sheets[title]
        except KeyError as exc:
            raise WorksheetNotFound(title) from exc

    def add_worksheet(self, title, rows, cols):
        worksheet = FakeWorksheet(title, [])
        self.sheets[title] = worksheet
        return worksheet


def _row(headers, values):
    return [values.get(header, "") for header in headers]


def _column_index(name):
    number = 0
    for character in name:
        number = number * 26 + ord(character) - ord("A") + 1
    return number - 1
