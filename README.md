# Azure Retail Lakehouse

A six-week junior data engineering preparation project using the Olist Brazilian e-commerce dataset.

The project currently demonstrates local PySpark development with Databricks Connect, managed Bronze and Silver Delta Lake pipelines, data-quality validation, rejected-record handling, Spark execution analysis, Unity Catalog, Delta history and time travel, parameterized notebooks, Databricks Jobs/Workflows, joins, aggregations, window functions, Parquet storage, and sales analysis.

## Current Architecture

The project currently runs PySpark code from PyCharm using Databricks Connect and Databricks serverless compute.

```text
Local Olist CSV files
        ↓
Databricks CLI upload
        ↓
Unity Catalog Volume
workspace.bronze.raw_files
        ↓
Databricks serverless compute
        ↓
PySpark / Databricks Connect
        ↓
Managed Delta Bronze tables
        ↓
Silver Delta tables
        ↓
Gold model
```


The Silver pipeline is also imported into the Databricks Workspace and can run as a parameterized Databricks Job, while local development continues in PyCharm through Databricks Connect.

## Technologies used

- Python 3.12
- PySpark DataFrame API
- Databricks Connect
- Databricks serverless compute
- Databricks CLI
- Databricks Jobs / Workflows
- Unity Catalog
- Delta Lake
- Parquet
- PyCharm
- Jupyter notebooks
- Git / GitHub

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
azure_retail_lakehouse/
├── architecture/
├── data/
│   └── raw/
│       └── olist/
├── docs/
│   ├── sql_to_pyspark.md
│   ├── pyspark_notes.md
│   ├── spark_execution_notes.md
│   ├── databricks_delta_notes.md
│   ├── spark_shuffles_note.md
│   └── silver_layer_delta_history_databricks_jobs.md
├── notebooks/
│   ├── 00_pyspark_basics.ipynb
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_orders.ipynb
│   ├── 03_silver_customers.ipynb
│   ├── 04_silver_order_items.ipynb
│   ├── 05_silver_products.ipynb
│   ├── 06_silver_payments.ipynb
│   ├── 07_sales_analysis.ipynb
│   ├── 08_spark_execution.ipynb
│   ├── 09_delta_bronze.ipynb
│   └── 10_delta_silver.ipynb
├── powerbi/
├── sql/
├── src/
│   ├── config.py
│   ├── schemas.py
│   └── spark_session.py
├── tests/
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


### `08_spark_execution.ipynb`

Explores Spark execution behaviour using `explain("formatted")`, including narrow and wide transformations, shuffle boundaries, `Exchange` nodes, partitioning, broadcast joins, sorting, caching, Adaptive Query Execution, and built-in functions versus Python UDFs.

### `09_delta_bronze.ipynb`

Uploads and reads the five Olist source datasets from a Unity Catalog Volume, applies explicit schemas, adds source-lineage metadata, and writes managed Bronze Delta tables. It also contains Delta Lake demonstrations covering schema enforcement and evolution, append/overwrite behaviour, `MERGE`, history, time travel, deletion vectors, and optimization.

### `10_delta_silver.ipynb`

Reads the managed Bronze Delta tables, applies cleaning and data-quality rules, validates keys and referential integrity, writes cleaned Silver Delta tables and rejected-record tables, and records rejection reasons. The notebook is parameterized with `dbutils.widgets` and can run as a Databricks Job using a `catalog` task parameter.

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
| Silver rejected | Orders |
| Silver rejected | Order items |
| Silver rejected | Payments |
| Integrated analysis | Orders enriched with customer information |
| Integrated analysis | Order items enriched with order, customer, and product information |
| Integrated analysis | Order-level revenue summary |
| Integrated analysis | Order-level payment summary |


## Current Silver Delta checkpoint

| Table | Valid rows | Rejected rows |
|---|---:|---:|
| `workspace.silver.orders` | 99,433 | 8 |
| `workspace.silver.customers` | 99,441 | 0 |
| `workspace.silver.order_items` | 112,642 | 8 |
| `workspace.silver.products` | 32,951 | 0 |
| `workspace.silver.payments` | 103,878 | 8 |

Current rejection reasons:

- `rejected_orders`: `delivered_status_missing_delivery_date` — 8 rows
- `rejected_order_items`: `parent_order_rejected` — 8 rows
- `rejected_payments`: `parent_order_rejected` — 8 rows

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
- Parent-order rejection propagation to dependent order-item and payment records

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

### Week 2 — Spark Execution 

- Used `explain("formatted")` to inspect Spark physical plans
- Identified narrow and wide transformations
- Identified `Exchange` nodes and shuffle boundaries
- Observed predicate pushdown and column pruning on Parquet reads
- Compared simple aggregation with `countDistinct`
- Observed hash partitioning during aggregations
- Observed range partitioning during global sorting
- Inspected a `BroadcastHashJoin`
- Compared `repartition()` with `coalesce()`
- Compared round-robin and hash repartitioning
- Investigated partition sizing and data skew
- Reviewed driver and executor responsibilities
- Learned why `collect()` is dangerous on large datasets
- Practiced caching and persistence concepts
- Compared Spark built-in functions with Python UDFs
- Reviewed Adaptive Query Execution and `isFinalPlan`

### Week 2 — Databricks + Delta Lake

- Switched the project environment to Python 3.12
- Configured Databricks Connect
- Connected PyCharm to Databricks serverless compute
- Configured Databricks CLI authentication
- Created the `workspace.bronze` and `workspace.silver` Unity Catalog schemas
- Created the `workspace.bronze.raw_files` managed Volume
- Uploaded the five raw Olist source CSV files to the Volume
- Refactored reusable Spark schemas into `src/schemas.py`
- Added source-lineage and ingestion metadata to Bronze records:
  - `_source_file`
  - `_source_file_modified_at`
  - `_ingested_at`
  - `_source_entity`
- Created managed Bronze Delta tables for:
  - `workspace.bronze.orders`
  - `workspace.bronze.customers`
  - `workspace.bronze.order_items`
  - `workspace.bronze.products`
  - `workspace.bronze.payments`
- Created cleaned managed Silver Delta tables for:
  - `workspace.silver.orders`
  - `workspace.silver.customers`
  - `workspace.silver.order_items`
  - `workspace.silver.products`
  - `workspace.silver.payments`
- Created rejected-record Delta tables with explicit reasons:
  - `workspace.silver.rejected_orders`
  - `workspace.silver.rejected_order_items`
  - `workspace.silver.rejected_payments`
- Validated Silver referential integrity:
  - orders → customers: 0 unmatched
  - order items → products: 0 unmatched
  - invalid child order items/payments traced to rejected parent orders
- Verified Delta tables using `DESCRIBE DETAIL`
- Inspected Delta transaction history with `DESCRIBE HISTORY`
- Demonstrated Delta table versioning
- Demonstrated time travel with `VERSION AS OF`
- Compared historical and current Silver orders using `exceptAll()`
- Demonstrated schema enforcement with an intentionally invalid append
- Performed explicit schema evolution using `ALTER TABLE ADD COLUMNS`
- Compared append and overwrite behaviour
- Demonstrated a Delta `MERGE` upsert
- Verified inserted and updated row metrics in Delta history
- Observed deletion-vector usage during `MERGE`
- Observed an automatic `OPTIMIZE` operation
- Confirmed predictive optimization was inherited from the Unity Catalog metastore
- Parameterized `10_delta_silver.ipynb` with `dbutils.widgets`
- Replaced hard-coded catalog references with the `catalog` notebook parameter
- Imported the Silver notebook into the Databricks Workspace
- Created a Databricks Job / Workflow with the Silver notebook as a task
- Passed `catalog=workspace` from the Job into the notebook
- Successfully executed the parameterized Silver pipeline as a Databricks Job


## Current status

Week 2 is complete. The project now has:

- managed Bronze Delta tables with source metadata
- cleaned Silver Delta tables
- rejected-record Delta tables with reasons
- Spark execution and shuffle documentation
- Delta history and time-travel demonstrations
- a parameterized Silver notebook
- a successful Databricks Job / Workflow run

The next project phase is **Week 3 — incremental processing and Gold dimensional modelling**, including dimensions, the order-item fact table, surrogate keys, incremental filtering, Delta `MERGE`, Type 1 and Type 2 changes, and idempotent reruns.

The raw, Bronze, and Silver data directories are excluded from Git because they contain source or generated data files.
