from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/orders/"
TARGET_TABLE = "ecom.silver.orders"
QUARANTINE_TABLE = "ecom.silver.orders_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_orders"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("order_id", F.col("order_id").cast("integer")) \
        .withColumn("user_id", F.col("user_id").cast("integer")) \
        .withColumn("order_status", F.trim(F.col("order_status"))) \
        .withColumn("order_timestamp", F.col("order_timestamp").cast("timestamp")) \
        .withColumn("updated_at", F.col("updated_at").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("order_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))

    bad_rows_df = deduped_batch_df.filter(
        (F.col("order_id").isNull()) | 
        (F.col("user_id").isNull()) | 
        (F.col("order_status").isNull()) | 
        (F.col("order_status") == "")
    ).withColumn("_rejection_reason", F.lit("Missing order_id, user_id, OR Blank order_status")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("order_id").isNotNull()) & 
        (F.col("user_id").isNotNull()) & 
        (F.col("order_status").isNotNull()) & 
        (F.col("order_status") != "")
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
         .merge(good_rows_df.alias("source"), "target.order_id = source.order_id")
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
print("Incremental Ingestion Completed Successfully for Orders.")