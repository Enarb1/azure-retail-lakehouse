# Azure Retail Lakehouse

A hands-on junior data engineering project built with the Olist Brazilian e-commerce dataset.

The purpose of this project was to practise PySpark, Databricks, Delta Lake, Azure Data Factory, ADLS Gen2, dimensional modelling, orchestration, data quality, and troubleshooting through one learning project.

## What I built

The project contains two connected learning paths:

```text
Databricks / PySpark path

Olist CSV files
      ↓
Unity Catalog Volume
      ↓
Bronze Delta tables
      ↓
Silver Delta tables
      ↓
Gold dimensional model
      ↓
Gold validation
```

and:

```text
Azure ingestion / orchestration path

Olist CSV files
      ↓
ADLS Gen2 landing
      ↓
Azure Data Factory
      ↓
Lookup → ForEach → Copy
      ↓
ADLS Gen2 raw
      ↓
Azure Key Vault
      ↓
ADF Web Activity
      ↓
Databricks Jobs REST API
      ↓
Databricks Job
```

The Databricks Job runs:

```text
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

> Note: the ADF ingestion path and Databricks transformation path were connected for orchestration practice. The Databricks Free Edition workspace used in this project is AWS-hosted, so the Databricks Bronze layer is not directly reading the Azure ADLS `raw` files.

## Technologies used

- Python 3.12
- PySpark
- Databricks Connect
- Databricks Free Edition
- Databricks CLI
- Databricks Jobs / Workflows
- Unity Catalog
- Delta Lake
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Managed Identity
- Azure RBAC
- Azure Key Vault
- Databricks Jobs REST API
- Parquet
- PyCharm
- Jupyter notebooks
- Git / GitHub

## Dataset

The project uses the Olist Brazilian e-commerce dataset.

Source entities:

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
│   ├── gold_data_dictionary.md
│   ├── azure_adf_learning_notes_2026-08-25.md
│   └── azure_adf_databricks_progress_2026-08-26.md
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
│   ├── 11_gold_dimensions.ipynb
│   └── 12_gold_validation.ipynb
├── sql/
├── src/
│   ├── config.py
│   ├── schemas.py
│   └── spark_session.py
├── tests/
├── .gitignore
└── README.md
```

## PySpark and ETL work

The project started with local PySpark development and gradually moved into Databricks.

Completed work includes:

- creating DataFrames and explicit schemas
- filtering, selecting, ordering, and deriving columns
- null handling and standardisation
- parsing timestamps
- using decimal types for monetary values
- validating primary and composite keys
- deduplication
- joins: inner, left, left-semi, and left-anti
- aggregation
- window functions
- lazy evaluation
- reading CSV files
- writing and reading Parquet
- checking referential integrity
- preserving dataset grain across joins
- avoiding one-to-many row multiplication
- creating rejected-record outputs with rejection reasons

## Spark execution concepts practised

`08_spark_execution.ipynb` was used to inspect how Spark executes transformations.

Topics practised:

- `explain("formatted")`
- narrow vs wide transformations
- shuffle boundaries
- `Exchange` nodes
- hash partitioning
- range partitioning
- `repartition()` vs `coalesce()`
- broadcast joins
- partition sizing
- data skew
- driver and executor responsibilities
- why `collect()` can be dangerous
- caching and persistence
- built-in Spark functions vs Python UDFs
- Adaptive Query Execution

## Databricks and Delta Lake

Databricks Connect was configured so notebooks could be developed from PyCharm while using Databricks serverless Spark.

Unity Catalog objects created include:

```text
workspace.bronze
workspace.silver
workspace.gold
workspace.bronze.raw_files
```

The five Olist source files were uploaded to the managed Volume and used to build managed Delta tables.

### Bronze

Managed Bronze Delta tables:

- `workspace.bronze.orders`
- `workspace.bronze.customers`
- `workspace.bronze.order_items`
- `workspace.bronze.products`
- `workspace.bronze.payments`

Source metadata added to Bronze records:

- `_source_file`
- `_source_file_modified_at`
- `_ingested_at`
- `_source_entity`

### Silver

Managed Silver Delta tables:

| Table | Valid rows | Rejected rows |
|---|---:|---:|
| `workspace.silver.orders` | 99,433 | 8 |
| `workspace.silver.customers` | 99,441 | 0 |
| `workspace.silver.order_items` | 112,642 | 8 |
| `workspace.silver.products` | 32,951 | 0 |
| `workspace.silver.payments` | 103,878 | 8 |

Rejected-record tables:

- `workspace.silver.rejected_orders`
- `workspace.silver.rejected_order_items`
- `workspace.silver.rejected_payments`

Current rejection reasons include:

- `delivered_status_missing_delivery_date`
- `parent_order_rejected`

Silver processing includes:

- timestamp parsing
- status standardisation
- city/state standardisation
- product-category standardisation
- payment-type standardisation
- missing-category handling
- correction of misspelled source columns
- delivery metrics
- monetary validation
- parent/child referential-integrity checks

## Delta Lake features practised

The project includes practical Delta Lake exercises covering:

- managed Delta tables
- `DESCRIBE DETAIL`
- `DESCRIBE HISTORY`
- transaction history
- schema enforcement
- schema evolution
- append vs overwrite
- `MERGE`
- table versioning
- time travel
- deletion vectors
- optimization behaviour

A hard-coded `VERSION AS OF 0` demo eventually failed because the required old files had passed the Delta deleted-file retention period.

The notebook was changed to retrieve the latest version dynamically:

```python
history = spark.sql(f"""
    DESCRIBE HISTORY {catalog}.silver.orders
""")

latest_version = history.agg(F.max("version")).first()[0]
```

This was a useful example of how code that works as a one-time exercise can later fail when executed repeatedly inside a workflow.

## Gold dimensional model

The Gold layer contains:

| Table | Rows |
|---|---:|
| `workspace.gold.dim_customer` | 96,097 |
| `workspace.gold.dim_product` | 32,952 |
| `workspace.gold.dim_date` | 774 |
| `workspace.gold.fact_order_item` | 112,642 |

### `dim_customer`

Grain:

```text
one row per logical customer_unique_id
```

The dimension contains:

- 96,096 real logical customers
- 1 unknown/default member

A deterministic `xxhash64()` surrogate key is used.

### `dim_product`

Grain:

```text
one row per product_id
```

The dimension contains:

- 32,951 real products
- 1 unknown/default member

### `dim_date`

The date dimension covers:

```text
2016-09-04 → 2018-10-17
```

with 774 calendar rows.

### `fact_order_item`

Grain:

```text
one product line within one order
```

Natural fact key:

```text
(order_id, order_item_id)
```

The table contains 112,642 rows.

## Incremental loading

A controlled incremental-load demonstration was built using:

```text
order_purchase_timestamp
```

as a watermark.

Demo watermark:

```text
2018-01-01
```

Results:

```text
Initial batch:      51,232 rows
Incremental batch:  61,410 rows
Final target:      112,642 rows
```

The incremental batch was loaded with Delta `MERGE`.

The same batch was then rerun to test idempotency.

Results:

```text
Final target after rerun: 112,642 rows
Duplicate fact keys:      0
```

The full-load and incremental outputs were compared with `exceptAll()` and both differences were zero.

## Slowly Changing Dimensions

### Type 1

A product dimension copy was updated from:

```text
perfumaria
```

to:

```text
fragrances
```

The same surrogate key was preserved and the previous attribute value was overwritten.

### Type 2

A customer with a location change was used to create two historical dimension rows.

The example includes:

- different surrogate keys for each version
- `effective_from`
- `effective_to`
- `is_current`

Validation confirmed:

```text
Exactly one current row
0 invalid effective-date ranges
```

## Gold validation

`12_gold_validation.ipynb` contains 15 reusable validation checks.

Checks include:

- duplicate customer keys
- duplicate product keys
- duplicate date keys
- duplicate fact keys
- missing customer dimension keys
- missing product dimension keys
- missing date dimension keys
- null customer foreign keys
- null product foreign keys
- null date foreign keys
- negative price
- negative freight
- negative item total
- exactly one unknown customer member
- exactly one unknown product member

Current result:

```text
Gold validation passed: all checks succeeded.
```

The notebook raises an exception if any validation check fails.

## Databricks Job

The transformation workflow was imported into Databricks and configured as a three-task Job:

```text
silver_pipeline
      ↓
gold_dimensions
      ↓
gold_validation
```

The notebooks use the parameter:

```text
catalog=workspace
```

The complete workflow has been executed successfully.

## Azure Data Lake Storage Gen2

Azure resources used in the project include:

- Azure for Students subscription
- resource group `rg-azure-retail-lakehouse`
- ADLS Gen2 Storage Account
- Azure Data Factory `branimir01`
- Germany West Central deployment region

The ADLS layout is:

```text
landing/
├── olist_orders_dataset.csv
├── olist_customers_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
└── olist_order_payments_dataset.csv

raw/
├── metadata/
│   └── entities.json
├── olist_orders_dataset.csv
├── olist_customers_dataset.csv
├── olist_order_items_dataset.csv
├── olist_products_dataset.csv
└── olist_order_payments_dataset.csv
```

`landing` represents source-delivered files.

`raw` represents files copied successfully by Azure Data Factory.

## Azure Data Factory

The ADF pipeline is metadata-driven.

Linked service:

```text
ls_adls_retail
```

Authentication:

```text
System Assigned Managed Identity
```

The ADF managed identity was granted:

```text
Storage Blob Data Contributor
```

Datasets created:

- `ds_adls_landing_csv`
- `ds_adls_raw_csv`
- `ds_adls_entities_json`

The CSV datasets use a `file_name` parameter so the same dataset can be reused across all five source entities.

The metadata file contains entries such as:

```json
{
  "entity_name": "orders",
  "source_file": "olist_orders_dataset.csv"
}
```

The ADF ingestion flow is:

```text
lookup_entities
      ↓
foreach_entity
      ↓
copy_entity_to_raw
```

`Lookup` reads all metadata records.

`ForEach` uses:

```text
@activity('lookup_entities').output.value
```

The Copy activity uses:

```text
@item().source_file
```

for the dynamic source and sink file names.

All five files were successfully copied from `landing` to `raw`.

The pipeline was published with **Publish all**.

## Azure security practice

The project used Azure Managed Identity and RBAC instead of Storage Account keys for ADF access to ADLS.

An Azure Key Vault was also created to store the Databricks Personal Access Token.

The ADF managed identity was given:

```text
Key Vault Secrets User
```

ADF retrieves the token through a Web activity using:

```text
Authentication: System Assigned Managed Identity
Resource: https://vault.azure.net
```

Sensitive activity input/output was configured securely.

## ADF to Databricks orchestration

ADF was extended so that, after ingestion, it can trigger the existing Databricks Job.

The flow is:

```text
lookup_entities
      ↓
foreach_entity
      ↓
copy_entity_to_raw
      ↓
get_databricks_token
      ↓
trigger_databricks_job
```

The second Web activity calls:

```text
POST /api/2.2/jobs/run-now
```

using the token retrieved from Key Vault.

A new Databricks Job run successfully appeared when ADF called the API.

The Databricks workflow then completed successfully:

```text
silver_pipeline   → Succeeded
gold_dimensions   → Succeeded
gold_validation   → Succeeded
```

## Troubleshooting completed

This project included several real configuration and debugging issues.

### Azure Policy

Resource creation initially failed because the Azure subscription only allowed specific deployment regions.

The project was moved to:

```text
Germany West Central
```

### ADLS endpoint configuration

The first linked-service connection failed because incompatible Storage Account features were enabled.

The relevant settings were adjusted and the linked service was retested.

### ADLS authorization

ADF initially returned:

```text
ADLSGen2ForbiddenError
AuthorizationPermissionMismatch
```

The fix was to grant the Data Factory managed identity:

```text
Storage Blob Data Contributor
```

### Incorrect Copy sink

One ADF Debug run succeeded technically but copied files back into `landing` instead of `raw`.

The sink dataset was corrected and the output location was revalidated.

### ADF dependency

The Key Vault activity initially did not run because its dependency from the `ForEach` activity was not configured correctly.

The dependency was changed so the activity executes after successful ingestion.

### Databricks API URL

The first Databricks Web activity request contained a malformed URL.

After correcting the URL, ADF successfully triggered the Databricks Job.

### Delta time-travel retention

The Databricks Job later failed because `VERSION AS OF 0` referenced files older than the Delta deleted-file retention period.

The notebook was changed to use the latest available version dynamically, after which the workflow succeeded again.

## Business analysis completed

The notebooks calculate and explore:

- delivery duration
- delivery delay
- late-delivery indicators
- order counts by customer state
- late-delivery rates by state
- product revenue by category
- freight revenue by category
- total revenue by category
- item counts by category
- order counts by category
- average item price by category
- order-level product revenue
- order-level freight revenue
- order-level total value
- payment-record counts
- distinct payment methods
- maximum installments
- payment-to-order-value reconciliation
- latest order per customer
- product revenue ranking within category

## Key lessons

This project provided hands-on practice with:

- moving from local PySpark development into Databricks
- understanding Spark execution rather than only writing transformations
- building Bronze, Silver, and Gold layers
- using Delta Lake for versioned and incremental processing
- modelling facts and dimensions
- testing idempotent reruns
- implementing Type 1 and Type 2 dimension examples
- building reusable data-quality checks
- creating and running Databricks Jobs
- navigating Azure Data Factory
- building parameterized datasets and Copy activities
- using Lookup and ForEach
- working with ADLS Gen2
- using Managed Identity and Azure RBAC
- storing secrets in Azure Key Vault
- calling an external REST API from ADF
- reading activity output and troubleshooting failed runs
- validating the actual data result instead of relying only on a green activity status

## Project status

The project is complete for its intended learning scope.

It demonstrates practical experience with:

```text
PySpark
Databricks
Delta Lake
Unity Catalog
Bronze / Silver / Gold
Dimensional modelling
Incremental MERGE
SCD Type 1 and Type 2
Data-quality validation
Databricks Jobs
ADLS Gen2
Azure Data Factory
Managed Identity / RBAC
Azure Key Vault
REST API orchestration
Troubleshooting
```

Power BI and additional production-hardening work were intentionally left outside the scope of this project.

Raw and generated datasets are not committed to the repository. Delta tables are stored in Databricks/Unity Catalog, and Azure ingestion files are stored in ADLS Gen2.
