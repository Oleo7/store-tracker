# Stockfiller Order Sync

The Stockfiller sync fetches Supplier API orders and upserts them into the `order_rows` worksheet in `CRM_DATABASE`.
It replaces rows by Stockfiller order `Reference`, which makes repeated runs idempotent.

## Required Variables

Add these to `.env` locally and to the Render Cron Job environment:

```text
STOCKFILLER_API_TOKEN=<Stockfiller bearer token>
STOCKFILLER_ENVIRONMENT=production
STOCKFILLER_SUPPLIER_IDENTIFIER=supplierGln
STOCKFILLER_SUPPLIER_ID=7350179830001
```

`STOCKFILLER_SUPPLIER_IDENTIFIER` can be `supplierGln`, `supplierExternalId`, or `supplierInternalId`.
For Polarbar, Stockfiller has confirmed `supplierGln=7350179830001` and `supplierInternalId=690c7b28217614d491065c12`.

Optional variables:

```text
STOCKFILLER_SYNC_LOOKBACK_HOURS=48
STOCKFILLER_SYNC_OVERLAP_HOURS=2
STOCKFILLER_TIMEOUT_SECONDS=30
```

## Run Locally

Dry run, incremental window:

```bash
python scripts/sync_stockfiller_orders.py --dry-run
```

Initial backfill example:

```bash
python scripts/sync_stockfiller_orders.py --mode backfill --start 2025-01-01 --dry-run
python scripts/sync_stockfiller_orders.py --mode backfill --start 2025-01-01 --target-worksheet order_rows_stockfiller_preview
python scripts/sync_stockfiller_orders.py --mode backfill --start 2025-01-01
```

Scheduled incremental run:

```bash
python scripts/sync_stockfiller_orders.py
```

## Render Cron Job

Create a Render Cron Job using the same repo and environment variables as the web app, plus the Stockfiller variables above.

Suggested settings:

```text
Build Command: pip install -r web-app/requirements.txt
Command: python scripts/sync_stockfiller_orders.py
Schedule: twice per day, for example 05:30 and 13:30 UTC
```

The sync stores its last successful incremental stop time in `_stockfiller_sync_state`.
