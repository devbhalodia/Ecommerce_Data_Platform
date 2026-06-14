from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/refunds/"
TARGET_TABLE = "ecom.silver.refunds"
QUARANTINE_TABLE = "ecom.silver.refunds_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_refunds"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("refund_id", F.col("refund_id").cast("integer")) \
        .withColumn("order_id", F.col("order_id").cast("integer")) \
        .withColumn("refund_amount", F.col("refund_amount").cast("decimal(10,2)")) \
        .withColumn("refund_reason", F.trim(F.col("refund_reason"))) \
        .withColumn("refund_timestamp", F.col("refund_timestamp").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("refund_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))

    bad_rows_df = deduped_batch_df.filter(
        (F.col("refund_id").isNull()) | 
        (F.col("order_id").isNull()) | 
        (F.col("refund_amount") <= 0) |
        (F.col("refund_reason").isNull()) | (F.col("refund_reason") == "")
    ).withColumn("_rejection_reason", F.lit("Missing Key, Invalid Refund Amount, OR Empty Refund Reason")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("refund_id").isNotNull()) & 
        (F.col("order_id").isNotNull()) & 
        (F.col("refund_amount") > 0) &
        (F.col("refund_reason").isNotNull()) & (F.col("refund_reason") != "")
    )

    if not bad_rows_df.isEmpty():
        (bad_rows_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(QUARANTINE_TABLE))

    if not good_rows_df.isEmpty():
        deltaTargetTable = DeltaTable.forName(spark, TARGET_TABLE)
        
        (deltaTargetTable.alias("target")
         .merge(
             good_rows_df.alias("source"), 
             "target.refund_id = source.refund_id" 
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
print("Incremental Ingestion Completed Successfully for Refunds Table.")