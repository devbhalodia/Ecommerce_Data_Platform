from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/inventory/"
TARGET_TABLE = "ecom.silver.inventory"
QUARANTINE_TABLE = "ecom.silver.inventory_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_inventory"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("product_id", F.col("product_id").cast("integer")) \
        .withColumn("stock_quantity", F.col("stock_quantity").cast("integer")) \
        .withColumn("warehouse_location", F.trim(F.col("warehouse_location"))) \
        .withColumn("updated_at", F.col("updated_at").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("product_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))
        
    bad_rows_df = deduped_batch_df.filter(
        (F.col("product_id").isNull()) | 
        (F.col("stock_quantity") < 0)
    ).withColumn("_rejection_reason", F.lit("Missing Key (product_id) or Negative Stock Quantity")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("product_id").isNotNull()) & 
        (F.col("stock_quantity") >= 0)
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
             "target.product_id = source.product_id"
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
print("Incremental Ingestion Completed Successfully for Inventory.")