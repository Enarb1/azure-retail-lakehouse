# Azure Retail Lakehouse

A six-week junior data engineering preparation project using the Olist Brazilian e-commerce dataset.

The project currently demonstrates local PySpark development, Bronze ingestion, Silver data cleaning, data-quality validation, joins, aggregations, window functions, Parquet storage, and sales analysis.

## Project architecture

```text
CSV source files
        ↓
Bronze raw layer
        ↓
Silver cleaned layer
        ↓
Integrated sales analysis
```

## Technologies used

- Python
- PySpark
- Jupyter Notebook
- Parquet
- Git
- GitHub

## Dataset

This project uses the Olist Brazilian e-commerce dataset.

The source entities currently included are:

- Orders
- Customers
- Products
- Order items
- Payments

## Project structure

```text
azure-retail-lakehouse/
├── data/
│   ├── raw/
│   ├── bronze/
│   └── silver/
├── docs/
│   └── sql_to_pyspark.md
├── notebooks/
│   ├── 00_pyspark_basics.ipynb
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_orders.ipynb
│   ├── 03_silver_customers.ipynb
│   ├── 04_silver_products.ipynb
│   ├── 05_silver_order_items.ipynb
│   ├── 06_silver_payments.ipynb
│   └── 07_sales_analysis.ipynb
├── src/
│   ├── __init__.py
│   ├── config.py
│   └── spark_session.py
├── .gitignore
└── README.md
```

## Notebook responsibilities

### `00_pyspark_basics.ipynb`

Contains introductory PySpark exercises covering DataFrames, schemas, transformations, actions, lazy evaluation, filtering, calculated columns, null handling, deduplication, joins, and aggregation.

### `01_bronze_ingestion.ipynb`

Reads the original Olist CSV source files and writes them to the Bronze layer with minimal transformation.

### `02_silver_orders.ipynb`

Reads Bronze orders, validates source records, parses timestamps, standardizes order statuses, adds delivery metrics and data-quality flags, separates valid and rejected records, and writes cleaned orders to the Silver layer.

### `03_silver_customers.ipynb`

Reads Bronze customers, validates customer identifiers, standardizes city and state values, separates valid and rejected records, and writes cleaned customers to the Silver layer.

### `04_silver_products.ipynb`

Reads Bronze products, validates product identifiers, standardizes product categories, replaces missing categories with `unknown`, corrects misspelled source column names, separates valid and rejected records, and writes cleaned products to the Silver layer.

### `05_silver_order_items.ipynb`

Reads Bronze order items, validates the composite key, parses monetary values, calculates item totals, separates valid and rejected records, and writes cleaned order items to the Silver layer.

### `06_silver_payments.ipynb`

Reads Bronze payments, validates the composite key, standardizes payment types, checks payment values and installment counts, separates valid and rejected records, and writes cleaned payments to the Silver layer.

### `07_sales_analysis.ipynb`

Reads the cleaned Silver datasets, performs cross-table joins, validates referential integrity, creates enriched datasets, calculates business metrics, and performs sales analysis.

## Current progress

### Week 1 — PySpark foundations

- Configured a local PySpark development environment
- Created and inspected Spark DataFrames
- Used `show()`, `printSchema()`, `select()`, `filter()`, `withColumn()`, and `orderBy()`
- Defined explicit schemas
- Worked with string, integer, decimal, Boolean, and timestamp data types
- Parsed timestamp columns
- Used decimal types for monetary values
- Handled null values
- Standardized string columns
- Verified dataset grain
- Verified primary keys
- Verified composite keys
- Used `distinct()` and `dropDuplicates()`
- Learned the difference between transformations and actions
- Practised Spark lazy evaluation
- Read CSV source files
- Wrote Parquet datasets
- Read and validated saved Parquet datasets
- Checked output schemas and row counts
- Organized the project into Bronze, Silver, and analysis notebooks
- Created shared Python modules for project paths and Spark setup

### Practical PySpark ETL

- Built Bronze ingestion for orders, customers, products, order items, and payments
- Built separate Silver pipelines for each source entity
- Validated Bronze records before creating Silver outputs
- Added rejection reasons for invalid source records
- Split valid and rejected records
- Standardized order-status values
- Standardized customer city and state values
- Standardized product-category values
- Standardized payment-type values
- Replaced missing product categories with `unknown`
- Corrected misspelled product source columns
- Added delivery-duration and delivery-delay metrics
- Added delivery data-quality flags
- Calculated item totals from price and freight
- Checked for negative payment values
- Checked for invalid installment counts
- Used inner joins
- Used left joins
- Used left-semi joins
- Used left-anti joins
- Checked for unmatched records between related datasets
- Verified that joins preserved the expected grain
- Investigated and avoided row multiplication in one-to-many joins
- Aggregated order items to one row per order
- Aggregated payments to one row per order
- Joined orders with customer information
- Joined order items with order, customer, and product information
- Calculated order-level revenue summaries
- Calculated product revenue by category
- Calculated freight revenue by category
- Calculated total revenue by category
- Calculated item counts by category
- Calculated order counts by category
- Calculated average item prices by category
- Calculated order counts by customer state
- Calculated late-delivery metrics by customer state
- Compared payment totals with order-item totals
- Used a one-cent tolerance when comparing monetary totals
- Used window functions to identify the latest order per customer
- Used window functions to rank products by revenue within category
- Wrote cleaned Silver datasets to Parquet
- Read and validated the saved Silver datasets
- Reorganized notebooks by pipeline stage and entity

## Data model and grain

| Dataset | Grain | Key |
|---|---|---|
| Orders | One row per order | `order_id` |
| Customers | One row per customer record | `customer_id` |
| Products | One row per product | `product_id` |
| Order items | One row per item within an order | `order_id`, `order_item_id` |
| Payments | One row per payment sequence within an order | `order_id`, `payment_sequential` |

The `customer_unique_id` column represents a logical customer who may be associated with multiple customer records.

## Completed datasets

| Layer | Dataset |
|---|---|
| Bronze | Orders |
| Bronze | Customers |
| Bronze | Products |
| Bronze | Order items |
| Bronze | Payments |
| Silver | Orders |
| Silver | Customers |
| Silver | Products |
| Silver | Order items |
| Silver | Payments |
| Integrated analysis | Orders enriched with customer information |
| Integrated analysis | Order items enriched with order, customer, and product information |
| Integrated analysis | Order-level revenue summary |
| Integrated analysis | Order-level payment summary |

## Data-quality checks

The project currently includes checks for:

- Missing primary-key values
- Missing composite-key values
- Duplicate primary keys
- Duplicate composite keys
- Invalid timestamp values
- Missing customer relationships
- Missing order relationships
- Missing product relationships
- Negative payment values
- Invalid installment counts
- Delivered orders without delivery dates
- Non-delivered orders with delivery dates
- Unexpected row-count changes after joins
- Payment totals that differ from order-item totals

## Business metrics

The analysis currently includes:

- Delivery duration
- Delivery delay
- Late-delivery indicator
- Order counts by customer state
- Late-delivery metrics by customer state
- Product revenue by category
- Freight revenue by category
- Total revenue by category
- Item counts by category
- Order counts by category
- Average item price by category
- Order-level product revenue
- Order-level freight revenue
- Order-level total value
- Payment-record counts by order
- Distinct payment-method counts by order
- Maximum installments by order
- Payment-to-order-value reconciliation
- Latest order per customer
- Product revenue ranking within category

## Key engineering lessons

### Define the grain before joining

Each dataset has a specific row-level meaning. Understanding the grain helps prevent duplicate records and incorrect aggregations.

### One-to-many joins can multiply rows

Order items and payments can both contain multiple rows for the same order. Joining them directly on `order_id` can multiply records.

The project avoids this by aggregating order items and payments to one row per order before joining the summaries.

### Bronze and Silver have different responsibilities

The Bronze layer preserves source data with minimal transformation.

The Silver layer validates, parses, standardizes, deduplicates, and separates invalid records before producing trusted datasets.

### Validate both inputs and outputs

Bronze validation identifies problems in source records.

Silver validation confirms that cleaned outputs have the expected schema, row count, grain, and keys.

### Left-anti joins help find broken relationships

Left-anti joins are used to find records without matching parent records, such as payments without matching orders or order items without matching products.

### Parquet outputs are directories

Spark writes Parquet datasets as directories containing one or more part files rather than as a single file.

## Running the project

1. Create and activate a Python virtual environment.
2. Install PySpark and the required dependencies.
3. Place the Olist CSV source files in the configured raw-data directory.
4. Run the notebooks in order:

```text
00_pyspark_basics.ipynb
01_bronze_ingestion.ipynb
02_silver_orders.ipynb
03_silver_customers.ipynb
04_silver_products.ipynb
05_silver_order_items.ipynb
06_silver_payments.ipynb
07_sales_analysis.ipynb
```

The raw, Bronze, and Silver data directories are excluded from Git because they contain source or generated data files.