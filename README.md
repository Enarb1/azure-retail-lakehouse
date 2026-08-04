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
