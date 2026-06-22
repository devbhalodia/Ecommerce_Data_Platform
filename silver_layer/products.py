from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/products/"
TARGET_TABLE = "ecom.silver.products"
QUARANTINE_TABLE = "ecom.silver.products_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_products"

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
        .withColumn("product_id", F.col("product_id").cast("integer")) \
        .withColumn("product_name", F.trim(F.col("product_name"))) \
        .withColumn("category", F.trim(F.col("category"))) \
        .withColumn("cost", F.col("cost").cast("decimal(10,2)")) \
        .withColumn("price", F.col("price").cast("decimal(10,2)")) \
        .withColumn("is_active", F.col("is_active").cast("boolean")) \
        .withColumn("created_at", F.col("created_at").cast("timestamp")) \
        .withColumn("updated_at", F.col("updated_at").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("product_id").orderBy(F.col("_fivetran_synced").desc())
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
        (F.col("product_id").isNull(), "null product_id"),
        (F.col("product_name").isNull() | (F.col("product_name") == ""), "null/empty product_name"),
        (F.col("category").isNull() | (F.col("category") == ""), "null/empty category"),
        (F.col("cost").isNull() | (F.col("cost") < 0), "null/negative cost"),
        (F.col("price").isNull() | (F.col("price") < 0), "null/negative price"),
    ]

    reason_col = F.concat_ws(", ", *[
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ])

    bad_rows_df = deduped_batch_df.filter(
        (F.col("product_id").isNull()) |
        (F.col("product_name").isNull()) | (F.col("product_name") == "") |
        (F.col("category").isNull()) | (F.col("category") == "") |
        (F.col("cost").isNull()) | (F.col("cost") < 0) |
        (F.col("price").isNull()) | (F.col("price") < 0)
    ).withColumn("_rejection_reason", reason_col) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("product_id").isNotNull()) &
        (F.col("product_name").isNotNull()) & (F.col("product_name") != "") &
        (F.col("category").isNotNull()) & (F.col("category") != "") &
        (F.col("cost").isNotNull()) & (F.col("cost") >= 0) &
        (F.col("price").isNotNull()) & (F.col("price") >= 0)
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
         .merge(good_rows_df.alias("source"), "target.product_id = source.product_id")
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
print("Incremental Ingestion Completed Successfully for Products.")