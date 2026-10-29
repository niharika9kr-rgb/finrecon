# Implementation walkthrough

1. Generate deterministic payment, settlement, and merchant payout feeds in `src/finrecon.py`.
2. Validate and ingest them into SQLite, quarantining invalid and duplicate source records.
3. Build the union of transaction references across all three systems.
4. Compute independent bank and payout statuses and discrepancies in integer cents.
5. Store current reconciliation and run history; export CSV reports.
6. Ingest late settlement feed and rerun to demonstrate recovery.
7. Validate outcomes using pytest; inspect local charts and logs in `screenshots/` and `docs/evidence/`.
8. Optional cloud path: create a Unity Catalog volume, upload sample CSVs, and execute Databricks notebooks 01 → 02 → 03 → 02. The cloud path is not verified.
