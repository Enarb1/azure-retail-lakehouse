# Azure Retail Lakehouse

A six-week junior data engineering preparation project using the Olist Brazilian e-commerce dataset.

## Technology

- Python
- PySpark
- Parquet
- Databricks
- Delta Lake
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Power BI

## Project architecture

CSV source files  
→ Bronze raw layer  
→ Silver cleaned layer  
→ Gold dimensional model  
→ Power BI report

## Current progress

### Week 1

- Configured local PySpark
- Inspected the Olist orders dataset
- Verified the orders table grain and primary key
- Profiled order statuses and null values
- Added delivery data-quality checks
- Parsed timestamp columns
- Calculated delivery duration and delivery delay
- Wrote the cleaned orders dataset to Parquet
- Read and validated the saved Parquet dataset
- Inspected and cleaned the customers dataset
- Verified `customer_id` as the customer-record key
- Investigated `customer_unique_id` as the logical customer identifier
- Standardized customer city and state values
- Wrote and validated the cleaned customers dataset in Parquet
- Used a left-anti join to check for orders without matching customers
- Joined orders with customer details using a left join
- Verified that the join preserved one row per order
- Calculated order counts and late-delivery metrics by customer state
- Saved the enriched orders dataset to Parquet
- Inspected the Olist order-items dataset
- Identified the grain as one row per item within an order
- Verified the composite key of `order_id` and `order_item_id`
- Defined an explicit schema using decimal types for monetary values
- Calculated item totals from price and freight
- Checked for order items without matching orders
- Joined order items with enriched order and customer data
- Verified that the join preserved one row per order item
- Calculated order-level revenue summaries
- Inspected the Olist products dataset
- Verified `product_id` as the product-table primary key
- Defined an explicit product schema
- Standardized product category names
- Replaced missing categories with `unknown`
- Corrected misspelled source column names
- Checked for order items without matching products
- Joined products to the enriched order-items dataset
- Verified that the join preserved one row per order item
- Calculated product revenue, freight revenue, total revenue, item counts, and order counts by category
- Calculated average item price by product category
