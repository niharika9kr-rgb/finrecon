# Databricks notebook source
# Insert new settlements idempotently; quarantine conflicting existing IDs and bad records.
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T
CATALOG = "main"
SCHEMA = "finrecon"
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
schema = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("amount_cents", T.LongType()),
    T.StructField("currency", T.StringType()),
    T.StructField("settled_date", T.StringType())])
late = (spark.read.option("header", "true").schema(schema)
        .csv(f"{ROOT}/settlements_late.csv").withColumn("source_file", F.lit("settlements_late.csv")))
duplicates = (late.filter(F.col("transaction_id").isNotNull()).groupBy("transaction_id")
              .count().filter("count > 1").select("transaction_id")
              .withColumn("duplicate_key", F.lit(True)))
late = late.join(duplicates, "transaction_id", "left")
invalid = (F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id")) == "") |
           F.col("amount_cents").isNull() | (F.col("amount_cents") < 0) |
           F.col("currency").isNull() | (F.col("currency") != "EUR"))
late = late.withColumn("reject_reason", F.when(invalid, "INVALID_RECORD")
                       .when(F.col("duplicate_key").isNotNull(), "DUPLICATE_IN_LATE_BATCH"))
target_name = f"{CATALOG}.{SCHEMA}.raw_settlements"
existing = spark.table(target_name).select("transaction_id", "amount_cents", "currency", "settled_date")
# Existing identical records are safe no-ops; conflicting records must not be silently overwritten.
comparison = (late.alias("l").join(existing.alias("e"), "transaction_id", "left")
    .withColumn("existing_id", F.col("e.transaction_id"))
    .withColumn("existing_amount", F.col("e.amount_cents"))
    .withColumn("existing_currency", F.col("e.currency"))
    .withColumn("existing_date", F.col("e.settled_date"))
    .select("l.*", "existing_id", "existing_amount", "existing_currency", "existing_date"))
conflict = (F.col("existing_id").isNotNull() &
    (~F.col("amount_cents").eqNullSafe(F.col("existing_amount")) |
     ~F.col("currency").eqNullSafe(F.col("existing_currency")) |
     ~F.col("settled_date").eqNullSafe(F.col("existing_date"))))
comparison = comparison.withColumn("reject_reason", F.when(F.col("reject_reason").isNotNull(), F.col("reject_reason"))
                                    .when(conflict, "CONFLICT_WITH_EXISTING"))
rejected = comparison.filter(F.col("reject_reason").isNotNull()).select(*late.columns)
accepted = (comparison.filter(F.col("reject_reason").isNull() & F.col("existing_id").isNull())
            .select("transaction_id", "amount_cents", "currency", "settled_date", "source_file"))
# Overwrite only the last late-batch quarantine; base-ingestion quarantine is retained separately.
rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_settlements_late")
DeltaTable.forName(spark, target_name).alias("t").merge(accepted.alias("s"), "t.transaction_id = s.transaction_id").whenNotMatchedInsertAll().execute()
print(f"new settlements={accepted.count()}, quarantined={rejected.count()}")
print("Rerun 02_reconcile_delta.py to refresh reconciliation_current")
