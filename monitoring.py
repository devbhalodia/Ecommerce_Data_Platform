# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecom.monitoring;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecom.monitoring.table_baseline (
# MAGIC     table_name STRING,
# MAGIC     expected_column_count INT
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ecom.monitoring.table_baseline
# MAGIC SELECT table_name, COUNT(column_name)
# MAGIC FROM ecom.information_schema.columns
# MAGIC WHERE table_schema = 'bronze'
# MAGIC GROUP BY table_name;