WITH current_s3_metadata AS (
  SELECT 
    table_name,
    COUNT(column_name) AS current_column_count
  FROM ecom.information_schema.columns  
  WHERE table_schema = 'bronze'
    AND table_name IN ('addresses', 'inventory', 'order_items', 'orders', 'payments', 'products', 'refunds', 'shipments', 'users')
  GROUP BY table_name
)
SELECT 
  c.table_name,
  c.current_column_count,
  b.expected_column_count,
  (c.current_column_count - b.expected_column_count) AS new_columns_detected
FROM current_s3_metadata c
JOIN ecom.monitoring.table_baseline b  
  ON c.table_name = b.table_name
WHERE c.current_column_count > b.expected_column_count;