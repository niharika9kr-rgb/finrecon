# Databricks notebook source
# Ingest sample feeds with explicit schemas; quarantine every duplicate key and invalid row.
from pyspark.sql import functions as F, types as T

CATALOG = "main"  # change if you lack permissions
SCHEMA = "finrecon"
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SCHEMAS = {
    "payments": T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("amount_cents", T.LongType()),
        T.StructField("currency", T.StringType()),
        T.StructField("event_date", T.StringType())]),
    "settlements": T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("amount_cents", T.LongType()),
        T.StructField("currency", T.StringType()),
        T.StructField("settled_date", T.StringType())]),
    "payouts": T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("payout_cents", T.LongType()),
        T.StructField("currency", T.StringType())])
}
FILES = {"payments": "payments.csv", "settlements": "settlements_day1.csv", "payouts": "payouts.csv"}

for name, filename in FILES.items():
    source = (spark.read.option("header", "true").option("mode", "PERMISSIVE")
              .schema(SCHEMAS[name]).csv(f"{ROOT}/{filename}")
              .withColumn("source_file", F.lit(filename)))
    required = ["transaction_id", "currency", "amount_cents" if name != "payouts" else "payout_cents"]
    if name != "settlements":
        required.append("merchant_id")
    invalid = F.lit(False)
    for column in required:
        invalid = invalid | F.col(column).isNull()
    invalid = invalid | (F.trim(F.col("transaction_id")) == "")
    invalid = invalid | (F.col("amount_cents" if name != "payouts" else "payout_cents") < 0)
    invalid = invalid | (F.col("currency") != "EUR")  # sample feed contract
    duplicate_ids = (source.filter(F.col("transaction_id").isNotNull())
                     .groupBy("transaction_id").count().filter(F.col("count") > 1)
                     .select("transaction_id").withColumn("duplicate_key", F.lit(True)))
    checked = source.join(duplicate_ids, "transaction_id", "left")
    checked = checked.withColumn("reject_reason", F.when(invalid, F.lit("INVALID_RECORD"))
                                 .when(F.col("duplicate_key").isNotNull(), F.lit("DUPLICATE_TRANSACTION_ID")))
    rejected = checked.filter(F.col("reject_reason").isNotNull()).drop("duplicate_key")
    accepted = checked.filter(F.col("reject_reason").isNull()).drop("duplicate_key", "reject_reason")
    # This is a replaceable sample batch, not a production streaming ingestion pipeline.
    accepted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.raw_{name}")
    rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.quarantine_{name}")
    print(f"{name}: accepted={accepted.count()}, quarantined={rejected.count()}")
