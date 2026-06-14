from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/shipments/"
TARGET_TABLE = "ecom.silver.shipments"
QUARANTINE_TABLE = "ecom.silver.shipments_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_shipments"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
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

    bad_rows_df = deduped_batch_df.filter(
        (F.col("shipment_id").isNull()) | 
        (F.col("order_id").isNull()) | 
        (F.col("carrier").isNull()) | (F.col("carrier") == "") | 
        (F.col("shipment_status").isNull()) | (F.col("shipment_status") == "") |
        ((F.col("delivered_date").isNotNull()) & (F.col("shipped_date").isNotNull()) & (F.col("delivered_date") < F.col("shipped_date")))
    ).withColumn("_rejection_reason", F.lit("Missing Key, Blank Meta-fields, OR Chronologically Invalid Dates")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(
        (F.col("shipment_id").isNotNull()) & 
        (F.col("order_id").isNotNull()) & 
        (F.col("carrier").isNotNull()) & (F.col("carrier") != "") & 
        (F.col("shipment_status").isNotNull()) & (F.col("shipment_status") != "") &
        ((F.col("delivered_date").isNull()) | (F.col("shipped_date").isNull()) | (F.col("delivered_date") >= F.col("shipped_date")))
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
print("Incremental Ingestion Completed Successfully for Shipments Table.")