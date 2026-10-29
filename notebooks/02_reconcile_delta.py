# Databricks notebook source
# Three-way matching with explicit keys, presence flags, and source uniqueness checks.
from pyspark.sql import functions as F
CATALOG = "main"
SCHEMA = "finrecon"
FEE_CENTS = 100  # synthetic fixed-fee assumption

def load_unique(name):
    df = spark.table(f"{CATALOG}.{SCHEMA}.raw_{name}")
    if df.filter(F.col("transaction_id").isNull()).limit(1).count():
        raise ValueError(f"{name}: null transaction ID")
    if df.groupBy("transaction_id").count().filter("count > 1").limit(1).count():
        raise ValueError(f"{name}: duplicate IDs found; rerun ingestion/quarantine")
    return df

p = load_unique("payments").select("transaction_id", F.lit(1).alias("has_payment"),
    F.col("merchant_id").alias("payment_merchant"), F.col("amount_cents").alias("payment_cents"),
    F.col("currency").alias("payment_currency"))
s = load_unique("settlements").select("transaction_id", F.lit(1).alias("has_settlement"),
    F.col("amount_cents").alias("settled_cents"), F.col("currency").alias("settlement_currency"))
o = load_unique("payouts").select("transaction_id", F.lit(1).alias("has_payout"),
    F.col("merchant_id").alias("payout_merchant"), F.col("payout_cents"),
    F.col("currency").alias("payout_currency"))
joined = p.join(s, "transaction_id", "full_outer").join(o, "transaction_id", "full_outer")
bank_status = (F.when(F.col("has_payment").isNull() & F.col("has_settlement").isNotNull(), "UNEXPECTED_SETTLEMENT")
    .when(F.col("has_payment").isNull(), "PAYOUT_WITHOUT_PAYMENT")
    .when(F.col("has_settlement").isNull(), "PENDING_SETTLEMENT")
    .when(F.col("payment_currency") != F.col("settlement_currency"), "CURRENCY_MISMATCH")
    .when(F.col("payment_cents") != F.col("settled_cents"), "AMOUNT_MISMATCH")
    .otherwise("MATCHED"))
payout_status = (F.when(F.col("has_payment").isNull(), "PAYOUT_WITHOUT_PAYMENT")
    .when(F.col("has_payout").isNull(), "PENDING_PAYOUT")
    .when(F.col("payment_merchant") != F.col("payout_merchant"), "PAYOUT_MERCHANT_MISMATCH")
    .when(F.col("payment_currency") != F.col("payout_currency"), "PAYOUT_CURRENCY_MISMATCH")
    .when(F.col("payment_cents") - F.lit(FEE_CENTS) != F.col("payout_cents"), "PAYOUT_AMOUNT_MISMATCH")
    .otherwise("PAYOUT_MATCHED"))
result = (joined.withColumn("status", bank_status).withColumn("payout_status", payout_status)
    .withColumn("discrepancy_cents", F.when(F.col("has_payment").isNotNull() & F.col("has_settlement").isNotNull(),
        F.col("payment_cents") - F.col("settled_cents")))
    .withColumn("payout_discrepancy_cents", F.when(F.col("has_payment").isNotNull() & F.col("has_payout").isNotNull(),
        F.col("payment_cents") - F.lit(FEE_CENTS) - F.col("payout_cents")))
    .withColumn("checked_at", F.current_timestamp()))
result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.reconciliation_current")
display(result.groupBy("status", "payout_status").count().orderBy("status", "payout_status"))
