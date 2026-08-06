# SQL to PySpark Comparison

This note compares common SQL operations with their PySpark DataFrame equivalents.

| Task | SQL | PySpark |
|---|---|---|
| Select columns | `SELECT order_id, status FROM orders` | `orders_df.select("order_id", "status")` |
| Filter rows | `WHERE status = 'delivered'` | `orders_df.filter(F.col("status") == "delivered")` |
| Null check | `WHERE order_id IS NULL` | `orders_df.filter(F.col("order_id").isNull())` |
| Rename column | `customer_state AS state` | `F.col("customer_state").alias("state")` |
| Add column | `price + freight AS total` | `df.withColumn("total", F.col("price") + F.col("freight"))` |
| Conditional logic | `CASE WHEN delay > 0 THEN true ELSE false END` | `F.when(F.col("delay") > 0, True).otherwise(False)` |
| Group and count | `GROUP BY state` with `COUNT(*)` | `df.groupBy("state").agg(F.count("*"))` |
| Sum values | `SUM(payment_value)` | `F.sum("payment_value")` |
| Left join | `LEFT JOIN customers USING (customer_id)` | `orders_df.join(customers_df, "customer_id", "left")` |
| Unmatched rows | `NOT EXISTS` | `orders_df.join(customers_df, "customer_id", "left_anti")` |
| Sort descending | `ORDER BY revenue DESC` | `df.orderBy(F.col("revenue").desc())` |
| Remove duplicates | `SELECT DISTINCT ...` | `df.distinct()` or `df.dropDuplicates(["key"])` |