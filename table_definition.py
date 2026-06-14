# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.users (
# MAGIC     user_id INT, is_active BOOLEAN, updated_at TIMESTAMP, phone STRING, last_name STRING, created_at TIMESTAMP, first_name STRING, email STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.addresses (
# MAGIC     address_id INT, country STRING, updated_at TIMESTAMP, user_id INT, city STRING, created_at TIMESTAMP, state STRING, postal_code STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.products (
# MAGIC     product_id INT, cost DECIMAL(10,2), is_active BOOLEAN, updated_at TIMESTAMP, price DECIMAL(10,2), created_at TIMESTAMP, category STRING, product_name STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.inventory (
# MAGIC     product_id INT, warehouse_location STRING, stock_quantity INT, updated_at TIMESTAMP, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.orders (
# MAGIC     order_id INT, order_timestamp TIMESTAMP, order_status STRING, updated_at TIMESTAMP, user_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.order_items (
# MAGIC     order_item_id INT, quantity INT, order_id INT, item_price DECIMAL(10,2), product_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.payments (
# MAGIC     payment_id INT, payment_timestamp TIMESTAMP, amount DECIMAL(10,2), payment_status STRING, order_id INT, payment_method STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.shipments (
# MAGIC     shipment_id INT, carrier STRING, shipped_date TIMESTAMP, order_id INT, shipment_status STRING, delivered_date TIMESTAMP, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.refunds (
# MAGIC     refund_id INT, refund_amount DECIMAL(10,2), refund_reason STRING, refund_timestamp TIMESTAMP, order_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP
# MAGIC ) USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.users_quarantine (
# MAGIC     user_id INT, is_active BOOLEAN, updated_at TIMESTAMP, phone STRING, last_name STRING, created_at TIMESTAMP, first_name STRING, email STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.addresses_quarantine (
# MAGIC     address_id INT, country STRING, updated_at TIMESTAMP, user_id INT, city STRING, created_at TIMESTAMP, state STRING, postal_code STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.products_quarantine (
# MAGIC     product_id INT, cost DECIMAL(10,2), is_active BOOLEAN, updated_at TIMESTAMP, price DECIMAL(10,2), created_at TIMESTAMP, category STRING, product_name STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.inventory_quarantine (
# MAGIC     product_id INT, warehouse_location STRING, stock_quantity INT, updated_at TIMESTAMP, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.orders_quarantine (
# MAGIC     order_id INT, order_timestamp TIMESTAMP, order_status STRING, updated_at TIMESTAMP, user_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.order_items_quarantine (
# MAGIC     order_item_id INT, quantity INT, order_id INT, item_price DECIMAL(10,2), product_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.payments_quarantine (
# MAGIC     payment_id INT, payment_timestamp TIMESTAMP, amount DECIMAL(10,2), payment_status STRING, order_id INT, payment_method STRING, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.shipments_quarantine (
# MAGIC     shipment_id INT, carrier STRING, shipped_date TIMESTAMP, order_id INT, shipment_status STRING, delivered_date TIMESTAMP, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ecom.silver.refunds_quarantine (
# MAGIC     refund_id INT, refund_amount DECIMAL(10,2), refund_reason STRING, refund_timestamp TIMESTAMP, order_id INT, _fivetran_deleted BOOLEAN, _fivetran_synced TIMESTAMP, _rejection_reason STRING, _rejected_at TIMESTAMP
# MAGIC ) USING DELTA;