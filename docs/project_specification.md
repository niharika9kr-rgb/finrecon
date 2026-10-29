# FinRecon project specification

## Business problem
A fictional European payment platform receives independent customer payment, bank settlement, and merchant payout feeds. Manual matching is slow and late settlements can change yesterday's reconciliation outcome.

## Scope
- Deterministic EUR synthetic transaction generation and CSV feeds.
- Validate and ingest three sources with duplicate and negative-amount rejection.
- Match bank settlements to payments, and merchant payouts to payments, independently.
- Identify missing, unexpected, mismatched amount/currency, and wrong-merchant records.
- Support late settlement arrival and idempotent reruns.
- Persist current reconciliation, rejected rows, and run-level status history in local SQLite.
- Supply Azure Databricks Delta notebooks with explicit schemas and three-way matching logic.

## Assumptions
- Transaction ID is a stable cross-system matching key.
- Financial amounts are stored in integer EUR cents.
- The synthetic merchant fee is exactly 100 cents per payment, for demonstration only.
- The local execution is authoritative evidence; cloud notebooks are not yet executed.

## Out of scope
Real payment data, live bank APIs, multi-currency FX, complex fee schedules, production orchestration, alerting, and actual cloud performance benchmarks.
