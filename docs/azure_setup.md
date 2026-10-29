# Azure Databricks execution (not yet verified)

1. Choose a Unity Catalog catalog where you have permission to create a schema and managed Delta tables. The notebooks default to `main.finrecon`; adjust `CATALOG` and `SCHEMA` in all three notebooks if needed.
2. Create the schema and a managed volume with SQL (substitute your catalog):

```sql
CREATE SCHEMA IF NOT EXISTS main.finrecon;
CREATE VOLUME IF NOT EXISTS main.finrecon.landing;
```

3. Upload the four CSV files in `data/sample/` to `/Volumes/main/finrecon/landing/`.
4. Import the three `.py` notebooks into your Databricks workspace. Use a compatible Databricks Runtime with Delta Lake support.
5. Execute `01_ingest_delta.py`, then `02_reconcile_delta.py`. Record the output status counts.
6. Execute `03_late_settlement_recovery.py`, then rerun `02_reconcile_delta.py`. Confirm pending settlements decrease and no duplicate keys are introduced.
7. Capture real screenshots of the notebook results, `reconciliation_current` Delta table, and test queries. Never use the local evidence images as cloud execution proof.

**Limitations:** Cloud notebooks are supplied but have not been executed in a real Azure workspace. The cloud path is a demonstration, not a full operational deployment. The synthetic merchant fee is fixed at €1 per transaction; real payout reconciliation needs a contractual fee schedule and payout aggregation windows. The ingestion notebook quarantines invalid rows and every duplicate transaction key. The late-arrival notebook also quarantines conflicts with existing settlements. Review quarantine tables after each run.
