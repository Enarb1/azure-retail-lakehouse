# Azure Retail Lakehouse

A six-week junior data engineering preparation project using the Olist Brazilian e-commerce dataset.

The project currently demonstrates local PySpark development with Databricks Connect, managed Bronze/Silver/Gold Delta Lake pipelines, data-quality validation, rejected-record handling, Spark execution analysis, Unity Catalog, Delta history and time travel, parameterized notebooks, Databricks Jobs/Workflows, dimensional modelling, deterministic surrogate keys, incremental Delta `MERGE` processing, idempotent reruns, Slowly Changing Dimension examples, joins, aggregations, window functions, Parquet storage, and sales analysis.

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
Gold Delta star schema
(dim_customer / dim_product / dim_date / fact_order_item)
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
│   ├── silver_layer_delta_history_databricks_jobs.md
│   └── gold_data_dictionary.md
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
│   ├── 10_delta_silver.ipynb
│   └── 11_gold_dimensions.ipynb
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

### `11_gold_dimensions.ipynb`

Builds the Gold dimensional model from validated Silver Delta tables. It creates `dim_customer`, `dim_product`, `dim_date`, and `fact_order_item`; uses deterministic surrogate keys and unknown/default dimension members; validates fact grain and dimension joins; demonstrates incremental fact loading with a watermark and Delta `MERGE`; verifies idempotent reruns; and includes Type 1 and Type 2 Slowly Changing Dimension examples.

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
| Silver Orders | One row per order | `order_id` |
| Silver Customers | One row per customer record | `customer_id` |
| Silver Products | One row per product | `product_id` |
| Silver Order items | One row per item within an order | `order_id`, `order_item_id` |
| Silver Payments | One row per payment sequence within an order | `order_id`, `payment_sequential` |
| Gold `dim_customer` | One row per logical customer, plus unknown member | `customer_key` / `customer_unique_id` |
| Gold `dim_product` | One row per product, plus unknown member | `product_key` / `product_id` |
| Gold `dim_date` | One row per calendar date | `date_key` |
| Gold `fact_order_item` | One row per product line within one order | `order_id`, `order_item_id` |

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
| Gold | `dim_customer` |
| Gold | `dim_product` |
| Gold | `dim_date` |
| Gold | `fact_order_item` |
| Gold demo | Incremental fact `MERGE` |
| Gold demo | Product Type 1 SCD |
| Gold demo | Customer Type 2 SCD |
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


## Current Gold Delta checkpoint

| Table | Rows |
|---|---:|
| `workspace.gold.dim_customer` | 96,097 |
| `workspace.gold.dim_product` | 32,952 |
| `workspace.gold.dim_date` | 774 |
| `workspace.gold.fact_order_item` | 112,642 |

Gold model decisions and validation:

- `dim_customer` is built at one row per logical `customer_unique_id`, plus an unknown/default member.
- `dim_product` is built at one row per `product_id`, plus an unknown/default member.
- `dim_date` covers the valid order-date range from `2016-09-04` through `2018-10-17`.
- `fact_order_item` grain is one product line within one order.
- Deterministic `xxhash64()` surrogate keys are used for customer and product dimensions.
- Surrogate key `0` is reserved for unknown/default customer and product members.
- Duplicate customer surrogate keys: 0.
- Duplicate product surrogate keys: 0.
- Duplicate date keys: 0.
- Duplicate fact grain keys: 0.
- Facts using unknown customer key: 0.
- Facts using unknown product key: 0.
- Facts with missing date key: 0.

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
- Duplicate Gold surrogate keys
- Duplicate Gold fact-grain keys
- Null Gold foreign keys
- Negative Gold fact measures
- Incremental-load duplicate detection
- Type 2 current-row and effective-date validation

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



### Week 3 — Gold dimensional modelling and incremental processing (ahead of schedule)

Although these topics appear later in the six-week guide, they were completed during the project's actual Week 3.

- Created the `workspace.gold` Unity Catalog schema
- Profiled the relationship between `customer_id` and `customer_unique_id`
- Confirmed 2,997 logical customers have multiple `customer_id` values
- Identified logical customers with changing locations:
  - 122 with multiple cities
  - 39 with multiple states
- Used order timestamps to identify the latest observed customer record
- Built `workspace.gold.dim_customer`
  - grain: one row per `customer_unique_id`
  - 96,096 real logical customers
  - 1 unknown/default member
  - 96,097 total rows
- Built `workspace.gold.dim_product`
  - grain: one row per `product_id`
  - 32,951 real products
  - 1 unknown/default member
  - 32,952 total rows
- Generated `workspace.gold.dim_date`
  - date range: `2016-09-04` to `2018-10-17`
  - 774 calendar rows
- Built `workspace.gold.fact_order_item`
  - grain: one product line within one order
  - natural fact key: (`order_id`, `order_item_id`)
  - 112,642 rows
- Joined Gold facts to customer, product, and date dimensions without changing the fact grain
- Verified 0 unknown customer keys, 0 unknown product keys, and 0 missing date keys in the current fact data
- Validated Gold fact measures:
  - 0 negative prices
  - 0 negative freight values
  - 0 negative item totals
- Demonstrated a controlled incremental fact load using `order_purchase_timestamp` as a watermark
- Used `2018-01-01` as the demo watermark:
  - initial batch: 51,232 rows
  - incremental batch: 61,410 rows
- Loaded the incremental batch with Delta `MERGE`
- Verified the incremental target reached 112,642 rows
- Reran the same incremental batch and confirmed the row count remained 112,642
- Verified 0 duplicate fact keys after the rerun
- Compared full-load and incremental outputs with `exceptAll()` and confirmed both differences were 0
- Inspected Delta history and observed:
  - first `MERGE`: 61,410 inserts
  - rerun: 61,410 matched updates and 0 inserts
- Demonstrated SCD Type 1 on a product dimension copy
  - changed one product category from `perfumaria` to `fragrances`
  - preserved the same `product_key`
  - did not preserve the previous attribute value
- Demonstrated SCD Type 2 on a customer with a location change
  - created two historical rows for the same `customer_unique_id`
  - generated different surrogate keys per historical version
  - added `effective_from`, `effective_to`, and `is_current`
  - validated exactly one current row
  - validated 0 invalid effective-date ranges
- Reviewed late-arriving fact/dimension handling using unknown key `0`
- Created `docs/gold_data_dictionary.md`
- Documented Gold grains, keys, measures, unknown-member strategy, incremental loading, idempotency, and SCD demonstrations

## Current status

The project is currently in **actual Week 3**, but progress is ahead of the six-week guide and has already covered the guide's Gold/incremental-processing objectives.

The project now has:

- managed Bronze Delta tables with source metadata
- cleaned Silver Delta tables
- rejected-record Delta tables with reasons
- Spark execution and shuffle documentation
- Delta history and time-travel demonstrations
- a parameterized Silver notebook
- a successful Databricks Job / Workflow run
- a persisted Gold star schema
- deterministic customer and product surrogate keys
- unknown/default dimension members
- a generated date dimension
- a validated order-item fact table
- an incremental Delta `MERGE` demonstration
- an idempotent rerun test with no duplicate facts
- a Type 1 product-dimension demonstration
- a Type 2 customer-history demonstration
- Gold model and data-dictionary documentation

Raw and generated data files are excluded from Git. Bronze, Silver, and Gold Delta tables are stored in Databricks/Unity Catalog rather than committed to the repository.

````