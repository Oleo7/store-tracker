# Storepoint Customer Sync

The Storepoint sync reads recent customers from `CRM_DATABASE` sheet `order_rows` and writes the upload columns in the Storepoint template spreadsheet.

## Source and Target

Source spreadsheet:

```text
CRM_DATABASE
1SL7mYtrgMmUdtvt6eykg4OOuefRtpRrUurvwfu_Jdck
order_rows
```

Target spreadsheet:

```text
Polarbar_Storepoint
1pl0h9oiKOn0kUvrCrPk4ftiBpKKNBFkl1rOtCUuc5m4
storepoint_template_49b0fd29731a
```

## Mapping

The sync reads these source columns:

```text
Delivery date
Customer
Address
Number
Postal code
City
```

It writes these target columns, starting at row 2:

```text
Customer              -> name
Address + " " Number -> address
City                  -> city
Postal code           -> postcode
```

Only the target columns `name`, `address`, `city`, and `postcode` are cleared and rewritten. Template columns such as description, state, country, phone, website, hours, and image columns are left unchanged.

## Date Window

By default, the job includes rows whose `Delivery date` is between Europe/Stockholm today minus 3 calendar months and today, inclusive.

## Required Variables

Use the same Google service account setup as the web app and Stockfiller sync:

```text
GOOGLE_CREDENTIALS=<service account JSON as a single-line string>
SHEET_KEY=1SL7mYtrgMmUdtvt6eykg4OOuefRtpRrUurvwfu_Jdck
```

Make sure the service account has editor access to the Storepoint spreadsheet. Optional overrides:

```text
STOREPOINT_SOURCE_SHEET_KEY=1SL7mYtrgMmUdtvt6eykg4OOuefRtpRrUurvwfu_Jdck
STOREPOINT_TARGET_SHEET_KEY=1pl0h9oiKOn0kUvrCrPk4ftiBpKKNBFkl1rOtCUuc5m4
```

## Run Locally

Dry run:

```bash
python scripts/sync_storepoint_customers.py --dry-run
```

Run the sync:

```bash
python scripts/sync_storepoint_customers.py
```

To test a fixed date window:

```bash
python scripts/sync_storepoint_customers.py --dry-run --today 2026-06-11
```

## Render Cron Job

Create a Render Cron Job using the same repo and environment variables as the web app.
Render cron expressions use UTC.

Suggested settings:

```text
Build Command: pip install -r web-app/requirements.txt
Command: python scripts/sync_storepoint_customers.py
Schedule: Daily after the Stockfiller order sync has finished
Cron Expression: 45 7 * * *
```

If the Stockfiller order sync runs several times per day, schedule this Storepoint job shortly after the final daily order sync as well.
