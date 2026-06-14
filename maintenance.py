# Databricks notebook source
SILVER_TABLES = [
    "ecom.silver.addresses",
    "ecom.silver.inventory",
    "ecom.silver.order_items",
    "ecom.silver.orders",
    "ecom.silver.payments",
    "ecom.silver.products",
    "ecom.silver.refunds",
    "ecom.silver.shipments",
    "ecom.silver.users"
]

RETENTION_HOURS = 168

# COMMAND ----------

print(f"Starting Lakehouse Maintenance Routine for {len(SILVER_TABLES)} tables...\n")

for table in SILVER_TABLES:
    print(f"--------------------------------------------------")
    print(f"Processing: {table}")
    
    try:
        print(f"Running OPTIMIZE...")
        spark.sql(f"OPTIMIZE {table}")
        print(f"OPTIMIZE completed successfully.")
    except Exception as e:
        print(f"OPTIMIZE FAILED for {table}. Error: {str(e)}")
    
    try:
        print(f"Running VACUUM (Retaining last {RETENTION_HOURS} hours)...")
        spark.sql(f"VACUUM {table} RETAIN {RETENTION_HOURS} HOURS")
        print(f"VACUUM completed successfully.")
    except Exception as e:
        print(f"VACUUM FAILED for {table}. Error: {str(e)}")

print(f"\n--------------------------------------------------")
print("Maintenance Routine Finished.")