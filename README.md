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
