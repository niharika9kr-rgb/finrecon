# Engineering decisions

- **Integer cents:** avoids binary floating-point rounding for synthetic EUR payments.
- **Two independent status dimensions:** a valid settlement does not imply a valid merchant payout.
- **Fixed synthetic payout fee:** makes expected payouts auditable; production would require fee schedules and payout batching.
- **Stable primary keys and conflict handling:** repeated ingestion does not double-count accepted records. Duplicate input references are recorded in a rejected table.
- **SQLite local path:** allows genuine reproducible evidence without paid cloud compute.
- **Delta cloud path:** uses explicit schemas, presence flags, and Delta MERGE for late settlements. This code has not been executed in Azure.
- **No fake screenshots:** charts come from the local run; no Databricks execution is claimed.

## Known limitations
The local ingestion stores a single accepted record per source transaction ID and logs duplicates rather than reconciling conflicting versions. Cloud ingestion currently demonstrates raw Delta storage and should add a production-grade quarantine layer for conflicting duplicate records. Cloud reconciliation overwrites a current-state table; a separate append-only run audit and stronger transactional guarantees would be needed in production.
