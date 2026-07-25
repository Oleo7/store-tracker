# CRM Customer Master Sync

The customer master sync keeps `CRM_DATABASE/customers_enriched.customer` aligned
with the latest customer name in `order_rows`. It is invoked by the production
Stockfiller order sync after `order_rows` has been written.

The feature is read-only by default:

```text
CRM_CUSTOMER_SYNC_MODE=dry_run
```

Supported modes:

- `off`: skip reconciliation.
- `dry_run`: classify and report changes without writing customer or review data.
- `apply`: apply safe changes, verify every touched customer, and write the
  current review queue.

Do not enable `apply` until a live dry-run has been reviewed.

## Matching and write rules

Product rows are collapsed by order `Reference`. Customer history is grouped by
`Customer number`, and the name from the latest `Order date` is the master
display name. The final physical row wins when dates are equal.

Stockfiller sometimes puts a 13-digit GLN or a Swedish organization number in
`Customer number` until a real Polarbär customer number has been assigned.
These values can help identify an order source, but they are treated as
temporary external identifiers: they are never written to
`customers_enriched.customer_number`. A new customer whose only source
identifier is a GLN or organization number is appended with a blank customer
number. When a real number appears later, exact name and full-address evidence
coalesces the older orders with that numbered customer.

The sync matches customers in this order:

1. unique `customer_number`
2. unique normalized customer name
3. one exact full address, postcode, and city match

Fuzzy similarity, including strong name-only similarity when enrichment is
missing, only blocks possible duplicates. It never writes a name.
Duplicate customer numbers, conflicting identifiers, ambiguous addresses, and
possible duplicates are sent to `_customer_sync_review`.

Safe existing matches can update:

- `customers_enriched.customer`
- a blank `customers_enriched.customer_number`
- `customers_enriched.email_last_order`

New safe customers are appended with those same three fields. Other fields,
including `cancelled_flag`, sales owner, segment, contact details, and enriched
address data, are preserved. Rows are never deleted.

`Polarbär - Inköp` and `Spakallarn` are ignored.

## Rename history

Before changing the customer master name, the sync updates only the `customer`
cell on matching rows in:

- `sales_activities`
- `email_messages`
- `email_recipients`

No column is added to `sales_activities`. Historical `order_rows` names are not
rewritten. Snowtracker uses `Customer number` first and the customer name as a
fallback when it connects historical orders and email activity.

Dependent history is written before `customers_enriched.customer`. This ordering
makes a retry safe if a run stops partway through.

## Standalone dry-run

The standalone command only needs `SHEET_KEY` and `GOOGLE_CREDENTIALS`:

```bash
python scripts/sync_crm_customers.py
```

It prints a JSON summary and the current review cases without modifying the
spreadsheet.

After reviewing the report and taking backups, apply safe changes explicitly:

```bash
python scripts/sync_crm_customers.py --apply
```

The apply command verifies the written customer values before it reports
success. It also updates customer-sync counters in `_stockfiller_sync_state`.

## Production rollout

1. Deploy with `CRM_CUSTOMER_SYNC_MODE=dry_run`.
2. Let at least one scheduled Stockfiller run complete.
3. Review the Render log and resolve duplicate customer numbers or ambiguous
   matches.
4. Export backups of `customers_enriched`, `sales_activities`,
   `email_messages`, and `email_recipients`.
5. Set `CRM_CUSTOMER_SYNC_MODE=apply`.
6. Confirm the next run succeeds and inspect `_customer_sync_review`.
7. Confirm a second run reports zero customer-name updates and zero appended
   customers.

If a structural requirement is missing, or a safe rename cannot preserve its
history, the job fails before changing the customer master name. The next
Stockfiller run can retry idempotently.
