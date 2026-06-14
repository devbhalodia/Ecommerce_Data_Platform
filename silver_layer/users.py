from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

SOURCE_S3_PATH = "s3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/users/"
TARGET_TABLE = "ecom.silver.users"
QUARANTINE_TABLE = "ecom.silver.users_quarantine"
CHECKPOINT_PATH = "/Volumes/ecom/silver/checkpoints/silver_users"


def process_incremental_batch(microBatchDF, batchId):
    #spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    if microBatchDF.isEmpty():
        return

    cleaned_df = microBatchDF \
        .withColumn("user_id", F.col("user_id").cast("integer")) \
        .withColumn("first_name", F.trim(F.col("first_name"))) \
        .withColumn("last_name", F.trim(F.col("last_name"))) \
        .withColumn("email", F.trim(F.col("email"))) \
        .withColumn("phone", F.trim(F.col("phone"))) \
        .withColumn("is_active", F.col("is_active").cast("boolean")) \
        .withColumn("created_at", F.col("created_at").cast("timestamp")) \
        .withColumn("updated_at", F.col("updated_at").cast("timestamp")) \
        .withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy("user_id").orderBy(F.col("_fivetran_synced").desc())
    deduped_batch_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter("row_num = 1")
                        .drop("row_num"))

    bad_rows_df = deduped_batch_df.filter(F.col("user_id").isNull()).withColumn("_rejection_reason", F.lit("Missing Key")) \
     .withColumn("_rejected_at", F.current_timestamp())

    good_rows_df = deduped_batch_df.filter(F.col("user_id").isNotNull())

    if not bad_rows_df.isEmpty():
        (bad_rows_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(QUARANTINE_TABLE))

    if not good_rows_df.isEmpty():
        deltaTargetTable = DeltaTable.forName(spark, TARGET_TABLE)
        
        (deltaTargetTable.alias("target")
         .merge(good_rows_df.alias("source"), "target.user_id = source.user_id")
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
print("Incremental Ingestion Completed Successfully For Users.")