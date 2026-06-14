from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/order_items/"
TARGET_TABLE = "ecom.silver.order_items"
QUARANTINE_TABLE = "ecom.silver.order_items_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_order_items"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("order_item_id", F.col("order_item_id").cast("integer")) \
        .withColumn("order_id", F.col("order_id").cast("integer")) \
        .withColumn("product_id", F.col("product_id").cast("integer")) \
        .withColumn("quantity", F.col("quantity").cast("integer")) \
        .withColumn("item_price", F.col("item_price").cast("decimal(10,2)")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("order_item_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))

    bad_rows_df = deduped_batch_df.filter(
        (F.col("order_item_id").isNull()) | 
        (F.col("order_id").isNull()) | 
        (F.col("product_id").isNull()) | 
        (F.col("quantity") <= 0) | 
        (F.col("item_price") < 0)
    ).withColumn("_rejection_reason", F.lit("Missing Key links OR Invalid Quantity/Price bounds")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("order_item_id").isNotNull()) & 
        (F.col("order_id").isNotNull()) & 
        (F.col("product_id").isNotNull()) & 
        (F.col("quantity") > 0) & 
        (F.col("item_price") >= 0)
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
             "target.order_item_id = source.order_item_id"
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
print("Incremental Ingestion Completed Successfully for Order Items.")