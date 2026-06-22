from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/addresses/"
TARGET_TABLE = "ecom.silver.addresses"
QUARANTINE_TABLE = "ecom.silver.addresses_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_addresses"

INTERNAL_COLS = {
    "_fivetran_synced", "_fivetran_deleted", "_fivetran_index",
    "_silver_processed_at"
}

QUARANTINE_THRESHOLD = 0.05


def get_new_columns(batch_df, target_table_name):
    target_cols = set(spark.table(target_table_name).columns)
    batch_cols = set(batch_df.columns) - INTERNAL_COLS
    return list(batch_cols - target_cols)


def process_incremental_batch(microBatchDF, batchId):

    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("address_id", F.col("address_id").cast("integer")) \
        .withColumn("user_id", F.col("user_id").cast("integer")) \
        .withColumn("country", F.trim(F.col("country"))) \
        .withColumn("state", F.trim(F.col("state"))) \
        .withColumn("city", F.trim(F.col("city"))) \
        .withColumn("postal_code", F.trim(F.col("postal_code"))) \
        .withColumn("created_at", F.col("created_at").cast("timestamp")) \
        .withColumn("updated_at", F.col("updated_at").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("address_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))

    new_cols = get_new_columns(deduped_batch_df, TARGET_TABLE)

    if new_cols:
        (deduped_batch_df
         .withColumn("_rejection_reason", F.lit(f"Schema drift: new columns detected {new_cols}"))
         .withColumn("_rejected_at", F.current_timestamp())
         .write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(QUARANTINE_TABLE))

        raise Exception(
            f"[Batch {batchId}] Schema drift detected in {TARGET_TABLE}. "
            f"New columns: {new_cols}. "
            f"Entire batch quarantined. Update Silver schema manually and rerun."
        )

    rejection_conditions = [
        (F.col("address_id").isNull(), "null address_id"),
        (F.col("user_id").isNull(), "null user_id"),
        (F.col("country").isNull() | (F.col("country") == ""), "null/empty country"),
        (F.col("city").isNull() | (F.col("city") == ""), "null/empty city"),
        (F.col("postal_code").isNull() | (F.col("postal_code") == ""), "null/empty postal_code"),
    ]

    reason_col = F.concat_ws(", ", *[
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ])

    bad_rows_df = deduped_batch_df.filter(
        (F.col("address_id").isNull()) |
        (F.col("user_id").isNull()) |
        (F.col("country").isNull()) | (F.col("country") == "") |
        (F.col("city").isNull()) | (F.col("city") == "") |
        (F.col("postal_code").isNull()) | (F.col("postal_code") == "")
    ).withColumn("_rejection_reason", reason_col) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("address_id").isNotNull()) &
        (F.col("user_id").isNotNull()) &
        (F.col("country").isNotNull()) & (F.col("country") != "") &
        (F.col("city").isNotNull()) & (F.col("city") != "") &
        (F.col("postal_code").isNotNull()) & (F.col("postal_code") != "")
    )

    if not bad_rows_df.isEmpty():
        bad_count = bad_rows_df.count()
        total = deduped_batch_df.count()
        quarantine_rate = bad_count / total

        (bad_rows_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(QUARANTINE_TABLE))

        if quarantine_rate > QUARANTINE_THRESHOLD:
            raise Exception(
                f"[Batch {batchId}] Quarantine threshold breached: "
                f"{quarantine_rate:.1%} ({bad_count}/{total} rows) quarantined in {TARGET_TABLE}. "
                f"Investigate upstream data quality."
            )


    if not good_rows_df.isEmpty():
        deltaTargetTable = DeltaTable.forName(spark, TARGET_TABLE)

        (deltaTargetTable.alias("target")
         .merge(good_rows_df.alias("source"), "target.address_id = source.address_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())


bronze_stream_df = spark.readStream.format("delta").load(SOURCE_S3_PATH)

query = (bronze_stream_df.writeStream
         .format("delta")
         .foreachBatch(process_incremental_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(availableNow=True)
         .start())

query.awaitTermination()
print("Incremental Ingestion Completed Successfully for Addresses.")