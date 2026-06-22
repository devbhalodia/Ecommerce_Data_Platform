from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/shipments/"
TARGET_TABLE = "ecom.silver.shipments"
QUARANTINE_TABLE = "ecom.silver.shipments_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_shipments"

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
        .withColumn("shipment_id", F.col("shipment_id").cast("integer")) \
        .withColumn("order_id", F.col("order_id").cast("integer")) \
        .withColumn("carrier", F.trim(F.col("carrier"))) \
        .withColumn("shipment_status", F.trim(F.col("shipment_status"))) \
        .withColumn("shipped_date", F.col("shipped_date").cast("timestamp")) \
        .withColumn("delivered_date", F.col("delivered_date").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("shipment_id").orderBy(F.col("_fivetran_synced").desc())
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
        (F.col("shipment_id").isNull(), "null shipment_id"),
        (F.col("order_id").isNull(), "null order_id"),
        (F.col("carrier").isNull() | (F.col("carrier") == ""), "null/empty carrier"),
        (F.col("shipment_status").isNull() | (F.col("shipment_status") == ""), "null/empty shipment_status"),
        (F.col("shipped_date").isNull(), "null shipped_date"),
        (
            F.col("delivered_date").isNotNull() &
            F.col("shipped_date").isNotNull() &
            (F.col("delivered_date") < F.col("shipped_date")),
            "delivered_date before shipped_date"
        ),
    ]

    reason_col = F.concat_ws(", ", *[
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ])

    bad_rows_df = deduped_batch_df.filter(
        (F.col("shipment_id").isNull()) |
        (F.col("order_id").isNull()) |
        (F.col("carrier").isNull()) | (F.col("carrier") == "") |
        (F.col("shipment_status").isNull()) | (F.col("shipment_status") == "") |
        (F.col("shipped_date").isNull()) |
        (
            F.col("delivered_date").isNotNull() &
            F.col("shipped_date").isNotNull() &
            (F.col("delivered_date") < F.col("shipped_date"))
        )
    ).withColumn("_rejection_reason", reason_col) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("shipment_id").isNotNull()) &
        (F.col("order_id").isNotNull()) &
        (F.col("carrier").isNotNull()) & (F.col("carrier") != "") &
        (F.col("shipment_status").isNotNull()) & (F.col("shipment_status") != "") &
        (F.col("shipped_date").isNotNull()) &
        (
            F.col("delivered_date").isNull() |
            (F.col("delivered_date") >= F.col("shipped_date"))
        )
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
         .merge(
             good_rows_df.alias("source"),
             "target.shipment_id = source.shipment_id"
         )
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
print("Incremental Digestion Completed Successfully for Shipments Table.")