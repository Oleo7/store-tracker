from copy import deepcopy
from unittest import TestCase

from gspread.exceptions import WorksheetNotFound

from customer_master_sync.sync import (
    CUSTOMER_REQUIRED_COLUMNS,
    REVIEW_SHEET_NAME,
    build_order_masters,
    is_gln_like,
    is_organization_number_like,
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
):
    return {
        "customer": name,
        "cancelled_flag": "",
        "sales_person": sales_person,
        "customer_number": customer_number,
        "email_last_order": email,
        "city_google": city,
        "address_google": address,
        "address_number_google": number,
        "postal_code_google": postal,
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

    def test_gln_is_temporary_identity_and_never_backfills_customer_number(self):
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
                "7301004000001",
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
                                "7301004000001",
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
