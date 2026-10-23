# CRM Customer Master Sync

The sync reconciles Stockfiller `order_rows` with `customers_enriched` and runs
after the order import. Legacy `customers` is never changed.

## Identity fields and schema

| Field in `customers_enriched` | Meaning |
| --- | --- |
| `customer_id` | Stable internal identity. New rows get a UUID v4; existing IDs are never replaced. |
| `customer_number` | Internal Polarbär number. GLNs and Swedish organization numbers are excluded. Only blank values can be complemented. |
| `stockfiller_gln` | Optional external location identity: 13 digits, including a valid GS1 check digit. |

GLN comes from `order_rows["Buyer number"]`, falling back to `Customer number`
only when it contains a valid GLN. Whitespace, hyphen formatting and integral
numeric cells are normalized; invalid nonempty Buyer numbers, invalid GLN-shaped
fallbacks and conflicting source GLNs require review. An invalid GLN is never
promoted to an internal number.

Older customer sheets need not contain `stockfiller_gln`. A dry-run does not add
it. Explicit `apply` appends the column after all existing columns only when a
safe complement or new GLN customer requires it. Existing columns are not moved.
Existing valid GLNs are compared in normalized form and left as stored; a
nonempty invalid or different GLN is never overwritten automatically. Duplicate
column headers fail before customer changes.

This model deliberately allows one GLN per physical customer. Multiple GLNs for
one internal identity, or a GLN shared by conflicting stores, require review.
If these relationships are legitimate, introduce a separately reviewed external
identity mapping (`source`, `external_id`, `customer_id`, validity dates) rather
than replacing an existing GLN or merging customer UUIDs.

## Matching rules

All available strong identifiers are checked for conflicts before choosing a
match. Priority is:

1. Unique internal `customer_id`, then internal `customer_number`.
2. Unique validated `stockfiller_gln`.
3. Unique exact normalized customer name, without conflicting known addresses
   for a proposed GLN assignment or conflicting known postcodes.
4. Unique exact street, house number, postcode and city, with compatible names.
5. Exact name plus street, house number and city can join order history even
   without a postcode. Street/city alone is review evidence, not a safe rename.
6. Fuzzy name/address similarity is review-only; it never authorizes a merge.

Duplicate internal IDs, numbers or GLNs in the customer register block writes
to the affected customers. An unknown persisted order `customer_id` cannot be
rebound by name. Incompatible strong identifiers, ambiguous matches, unclear
GLN-based name changes and conflicting product-row metadata require review.

Street keys normalize case, extra spaces, Swedish diacritics and house suffixes
such as `11 C`/`11C`. A house number embedded in the street can match the separate
number column. Numeric ranges remain distinct (`11-13` is not `1113`). Known
postcodes add evidence; missing postcodes are not fabricated.

Different valid GLNs **and** different known street/house/city identities reduce
generic chain/city similarity. This allows Coop Masthugget and Coop Andra
Långgatan to remain separate. Strong name similarity still requires review,
even with distinct GLNs and addresses. Safe GLN complements from the same
snapshot can supply this distinction before any writes; conflicted proposals
cannot supply it.

A new customer needs a trusted internal number, a full physical address, or a
validated GLN together with name, street, house number and city. Plausible
duplicates still block automatic registration. Rows with no usable identity
remain in review. `Polarbär - Inköp` and `Spakallarn` are ignored.

## Order history, writes and retries

Product rows are grouped by `Reference`, retaining all identity evidence.
History joins on consistent internal IDs, internal numbers or validated GLNs.
A weaker bridge requires both exact name and street/house/city with no
conflicting identifiers or postcodes; name/city alone never joins stores.
The latest `Order date` supplies the display name and email; the last physical
row breaks ties. Multiple identities targeting the same customer are reviewed.

Safe existing matches can update the display name, latest-order email and blank
internal number/GLN. Other cells, including owner, notes, cancellation and
enriched address/coordinates, are preserved. New rows get a UUID and the supplied
raw order address/city, even without Google Maps enrichment. Postcode and
coordinates remain blank when absent; raw order addresses are not geocoding
results.

The existing rename flow updates only customer-name cells in `sales_activities`,
`email_messages`, `email_recipients` and optional `planned_activities` before the
master name. Planning snapshots are updated by UUID, preserving existing
internal numbers when the order supplies none. Activity IDs, notes, dates and
owners remain unchanged. Dependent history and customer values are verified
before success. A partial write can be retried without appending a duplicate.

Stockfiller uses the same identity planner before assigning order IDs/numbers
and again after registration, so every product row of a newly registered
customer receives the same UUID. A refreshed reference retains its persisted
UUID even if new external data needs review. Conflicting persisted UUIDs within
a refreshed reference abort the import before writes. A fallback GLN is copied
to `Buyer number` before projecting a later internal number, preserving the
external evidence. Review cases neither rebind IDs nor update customer emails.

After a successful customer `apply`, `_customer_sync_review` is replaced with
the current unresolved decisions. A manually registered customer such as
Masthugget keeps its UUID and disappears from the queue once safely matched.
No customer rows or history rows are deleted.

## Safe preview and rollout

Modes: `off` skips customer reconciliation; `dry_run` returns proposals without
customer/schema/review writes; `apply` performs and verifies safe changes.
Stockfiller's customer mode defaults to `off`; no schedule or production
configuration is changed by this implementation.

The standalone command defaults to a **read-only** dry-run. With separately
authorized access and the existing `SHEET_KEY`/`GOOGLE_CREDENTIALS`, capture the
JSON report before any production change:

```bash
python scripts/sync_crm_customers.py > customer-sync-preview.json
```

The report includes `backfilled_glns`, proposed/existing GLNs per decision,
internal numbers, match method, reasons and unresolved review cases. The legacy
`ignored_gln_identifiers` counter means GLNs excluded from the *internal number*
field, not GLNs discarded from matching. Review the complete proposal and
identity conflicts; no source row number should be treated as a stable ID.

After explicit production approval and backups of the master, order, history,
planning and review sheets, apply the separately reviewed plan:

```bash
python scripts/sync_crm_customers.py --apply
```

This explicit command can append the GLN column and complement unambiguous
identities; it is not an unconditional backfill. It also updates sync counters
in `_stockfiller_sync_state`. Verify preserved UUIDs, the review queue and a
second preview showing no repeated appends or complements. The standalone
command does not rewrite `order_rows`; the next approved Stockfiller run maps
the customer IDs after registration.

`CRM_CUSTOMER_SYNC_MODE=dry_run` previews only the **customer-master portion** of
the scheduled import: the surrounding Stockfiller job still writes order rows,
email updates and sync state. Use the standalone command for a read-only customer
preview, or the Stockfiller command's separate `--dry-run` for an import preview.
No production CRM access, backfill, configuration change or deployment is part
of development verification; tests use isolated in-memory sheets and API data.
