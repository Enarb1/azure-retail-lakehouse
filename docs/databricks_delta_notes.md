# Databricks and Delta Lake Notes

## Session Goal

Move from local PySpark/Parquet processing to a Databricks-based lakehouse workflow and understand the core Delta Lake features through hands-on experiments.

Today I worked with:

- Databricks Connect
- Databricks serverless compute
- Unity Catalog
- Catalogs, schemas, volumes, and managed tables
- Bronze ingestion
- Delta table history
- Time travel
- Schema enforcement
- Schema evolution
- Append and overwrite
- Delta `MERGE`
- Predictive optimization
- Delta maintenance behavior

---

## 1. Local PySpark vs Databricks Connect

During Weeks 1 and 2, Spark ran locally on my laptop:

```text
PyCharm
   ↓
Local Python
   ↓
Local PySpark
   ↓
Spark running on my Mac
```

For the Databricks stage, I changed to Databricks Connect:

```text
PyCharm
   ↓
Databricks Connect
   ↓
Databricks serverless compute
   ↓
Spark execution in Databricks
```

This means I can keep developing in PyCharm while Spark execution happens remotely in Databricks.

---

## 2. Python and Databricks Connect Environment

The project originally used Python 3.13. I changed it to Python 3.12 so it would be compatible with the Databricks Connect setup being used.

Installed versions:

```text
Python 3.12
databricks-connect 18.0.9
Databricks CLI v1.11.0
```

An important environment issue was discovered: standalone `pyspark` and `databricks-connect` cannot be installed together in the same virtual environment.

The conflict was fixed with:

```bash
pip uninstall -y databricks-connect pyspark pyspark-connect pyspark-client
pip install "databricks-connect==18.0.*"
```

### Key lesson

When using Databricks Connect, use the PySpark API supplied through Databricks Connect rather than maintaining a separate local `pyspark` installation in the same environment.

---

## 3. Databricks Authentication

The Databricks CLI was authenticated against the workspace using OAuth.

A profile called:

```text
DEFAULT
```

was created and verified successfully.

That same profile is used by Databricks Connect.

---

## 4. Creating a Databricks Spark Session from PyCharm

Because I am working from PyCharm rather than a Databricks-hosted notebook, the `spark` variable is not created automatically.

I create a remote Databricks Spark session with:

```python
from databricks.connect import DatabricksSession

spark = (
    DatabricksSession.builder
    .serverless()
    .profile("DEFAULT")
    .getOrCreate()
)
```

The returned Spark version was:

```text
4.1.0
```

A simple test confirmed remote Spark execution:

```python
spark.range(5).show()
```

Result:

```text
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+
```

---

## 5. Unity Catalog Namespace

The current namespace was checked with:

```sql
SELECT current_catalog(), current_schema()
```

The result was:

```text
catalog = workspace
schema  = default
```

Unity Catalog uses a three-level namespace:

```text
catalog.schema.table
```

For example:

```text
workspace.bronze.orders
```

means:

```text
workspace → catalog
bronze    → schema
orders    → table
```

---

## 6. Creating the Bronze Schema

A Bronze schema was created:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.bronze
```

The workspace then contained:

```text
bronze
default
information_schema
```

The Bronze schema will store source-level lakehouse objects.

---

## 7. Unity Catalog Volumes

A managed Volume was created:

```sql
CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_files
```

Its logical path is:

```text
/Volumes/workspace/bronze/raw_files/
```

The Volume is used for raw files rather than relational tables.

The raw orders CSV was uploaded from the local project:

```text
data/raw/olist/olist_orders_dataset.csv
```

into:

```text
/Volumes/workspace/bronze/raw_files/
```

Conceptually:

```text
Local project
      ↓
Databricks CLI upload
      ↓
Unity Catalog Volume
```

---

## 8. Reusable Schemas

The orders schema had originally been defined inside another notebook.

Instead of copying the schema into the new Bronze notebook, reusable schemas were moved into:

```text
src/schemas.py
```

Example:

```python
from src.schemas import orders_schema
```

### Key lesson

Reusable definitions such as schemas should live in normal Python modules rather than being duplicated between notebooks.

A notebook should not depend on another notebook only to access reusable Python definitions.

---

## 9. Reading the Raw Orders Dataset

The raw orders file was read from the Volume using the existing explicit schema:

```python
raw_orders_path = (
    "/Volumes/workspace/bronze/raw_files/"
    "olist_orders_dataset.csv"
)

raw_orders = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv(raw_orders_path)
)
```

The schema contained:

```text
order_id
customer_id
order_status
order_purchase_timestamp
order_approved_at
order_delivered_carrier_date
order_delivered_customer_date
order_estimated_delivery_date
```

The source contained:

```text
99,441 rows
```

---

## 10. Bronze Layer Philosophy

Bronze should remain close to the original source.

It should primarily preserve:

```text
source data
+
ingestion metadata
```

Derived business logic such as:

```text
delivery_days
delivery_delay_days
is_late
standardized business values
rejection rules
```

belongs in Silver.

---

## 11. Adding Source Metadata

Databricks file reads expose file metadata through the `_metadata` column.

The Bronze orders DataFrame was enriched with:

```text
_source_file
_source_file_modified_at
_ingested_at
_source_entity
```

Implementation:

```python
bronze_orders = (
    raw_orders
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time")
            .alias("_source_file_modified_at"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_entity", F.lit("orders"))
)
```

This allows Bronze records to retain source lineage.

Each row can answer:

```text
Where did this record come from?
When was the source file modified?
When was the record ingested?
Which source entity produced it?
```

---

## 12. Creating the First Managed Delta Table

The Bronze orders DataFrame was written as:

```python
(
    bronze_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.bronze.orders")
)
```

This created:

```text
workspace.bronze.orders
```

as a managed Delta table.

No explicit storage path was supplied, so Databricks and Unity Catalog manage the underlying table storage.

Downstream code can work with the logical table name:

```python
spark.table("workspace.bronze.orders")
```

rather than directly managing data-file paths.

---

## 13. Delta vs Plain Parquet

During Weeks 1 and 2, cleaned datasets were written directly as Parquet.

Conceptually:

```text
Parquet dataset
└── Parquet files
```

A Delta table adds transactional table management:

```text
Delta table
├── Parquet data files
└── Delta transaction information
```

This enables:

```text
table versions
history
time travel
schema enforcement
schema evolution
MERGE
transactional table operations
```

Delta still uses Parquet files for the actual data, but adds a transaction layer around them.

---

## 14. DESCRIBE DETAIL

The Bronze orders table was inspected with:

```sql
DESCRIBE DETAIL workspace.bronze.orders
```

Important observed values:

```text
format = delta
name = workspace.bronze.orders
numFiles = 1
sizeInBytes = 5719820
partitionColumns = []
clusteringColumns = []
```

The table was approximately 5.7 MB and stored in one data file.

The table was not explicitly partitioned or clustered.

---

## 15. Delta Transaction History

The table history was inspected with:

```sql
DESCRIBE HISTORY workspace.bronze.orders
```

The first write created:

```text
version = 0
```

Important operation information included:

```text
operation = CREATE OR REPLACE TABLE AS SELECT
numOutputRows = 99441
numFiles = 1
numOutputBytes = 5719820
```

### Key lesson

Delta records changes to the table as versions.

A Delta table therefore has an operational history rather than being only a directory of data files.

---

## 16. Delta History Demo Table

A small demo table was created so Delta features could be tested without modifying the real Bronze orders table.

Initial data:

```text
id | value
---|-------
1  | first
2  | second
3  | third
```

The table was saved as:

```text
workspace.bronze.delta_history_demo
```

The initial write created:

```text
version 0
```

---

## 17. Append

A fourth row was appended:

```text
4 | fourth
```

Using:

```python
new_row.write     .format("delta")     .mode("append")     .saveAsTable("workspace.bronze.delta_history_demo")
```

History then showed:

```text
version 0 → table creation
version 1 → append
```

### Append semantics

`append` preserves the current records and adds new records.

---

## 18. readVersion

Delta history showed:

```text
version 1
readVersion = 0
```

This means the transaction that created version 1 was based on version 0.

Later operations followed the same pattern:

```text
v1 read v0
v2 read v1
v3 read v2
```

Each table-changing transaction is applied against a previous valid Delta snapshot.

---

## 19. Time Travel

After appending row 4, the current table contained four rows.

Version 0 could still be queried:

```sql
SELECT *
FROM workspace.bronze.delta_history_demo
VERSION AS OF 0
ORDER BY id
```

This returned:

```text
1 first
2 second
3 third
```

while the current version returned four rows.

### Key lesson

Delta time travel allows previous table snapshots to be queried without manually creating backup tables after each write.

---

## 20. Schema Enforcement

A DataFrame was intentionally created with an extra column:

```text
id
value
extra_column
```

The target Delta table contained only:

```text
id
value
```

The append failed with:

```text
DELTA_METADATA_MISMATCH
```

Delta compared:

```text
Table schema:
id
value

Data schema:
id
value
extra_column
```

and rejected the write.

### Key lesson

Delta does not silently accept incompatible schemas.

Schema enforcement protects tables from accidental structural changes.

---

## 21. Schema Evolution

The new column was then added intentionally:

```sql
ALTER TABLE workspace.bronze.delta_history_demo
ADD COLUMNS (extra_column STRING)
```

The table schema became:

```text
id: long
value: string
extra_column: string
```

Delta history recorded:

```text
operation = ADD COLUMNS
```

### Schema enforcement vs schema evolution

Schema enforcement:

> Incoming data does not match the current schema, so reject the write.

Schema evolution:

> The schema change is intentional, so explicitly modify the table definition.

---

## 22. Existing Rows After Schema Evolution

After adding `extra_column`, the old records had:

```text
extra_column = NULL
```

Example:

```text
id | value  | extra_column
---|--------|-------------
1  | first  | NULL
2  | second | NULL
3  | third  | NULL
4  | fourth | NULL
5  | fifth  | unexpected
```

### Key lesson

Adding a nullable column changes the schema but does not invent values for existing rows.

Existing records naturally receive `NULL` for the new field unless they are explicitly updated.

---

## 23. Schema Evolution and Permissions

The schema-mismatch error also revealed that automatic schema migration was not allowed in this environment because Table ACLs were enabled.

Therefore the schema was evolved explicitly with:

```sql
ALTER TABLE ... ADD COLUMNS
```

This made the change deliberate and visible in Delta history.

---

## 24. Append vs Overwrite

A replacement DataFrame was created:

```text
100 | replacement_a | new
200 | replacement_b | new
```

It was written with:

```python
.mode("overwrite")
```

The current table then contained only those two rows.

### Append

```text
existing rows
+
new rows
```

### Overwrite

```text
replace current table state
```

---

## 25. Overwrite and Time Travel

Even after overwrite, the earlier five-row state could still be queried:

```sql
SELECT *
FROM workspace.bronze.delta_history_demo
VERSION AS OF 3
ORDER BY id
```

This returned the previous five rows.

### Key lesson

`overwrite` changes the current table state, but it does not immediately erase retained Delta history.

Older versions can still be available for time travel.

---

## 26. MERGE / Upsert

A source DataFrame was created:

```text
100 | replacement_a_updated | updated
300 | replacement_c         | new
```

The target table contained:

```text
100
200
```

The source was registered as a temporary view:

```python
updates_df.createOrReplaceTempView("updates")
```

Then a Delta `MERGE` was executed:

```sql
MERGE INTO workspace.bronze.delta_history_demo AS target
USING updates AS source
ON target.id = source.id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

The resulting table became:

```text
100 | replacement_a_updated | updated
200 | replacement_b         | new
300 | replacement_c         | new
```

---

## 27. MERGE Semantics

Three cases were demonstrated.

### Matched row

`id = 100` existed in both source and target.

It was updated.

### Target-only row

`id = 200` existed only in the target.

It remained unchanged.

### Source-only row

`id = 300` existed only in the source.

It was inserted.

This is an upsert:

```text
UPDATE existing records
+
INSERT new records
```

---

## 28. MERGE Operation Metrics

Delta history recorded the `MERGE`.

Important observed metrics included:

```text
numSourceRows = 2
numTargetRowsUpdated = 1
numTargetRowsInserted = 1
```

These metrics matched the actual result.

### Key lesson

Delta history can provide useful operational and audit information for a pipeline.

---

## 29. Why MERGE Matters

`MERGE` is important for incremental loads.

Instead of overwriting an entire table every run, a pipeline can process changed and new records:

```text
incoming records
       ↓
MERGE
       ↓
matched     → update
not matched → insert
```

This pattern will become important during Week 4 incremental processing.

---

## 30. Deletion Vectors

The `MERGE` history showed:

```text
numTargetDeletionVectorsAdded = 1
```

The Delta table had deletion vectors enabled.

Conceptually, deletion vectors allow Delta to represent row-level changes without always rewriting an entire data file immediately.

### Key lesson

Delta can optimize row-level updates and deletes internally instead of forcing every logical change to rewrite complete files.

---

## 31. Automatic OPTIMIZE

After the `MERGE`, Delta history contained:

```text
operation = OPTIMIZE
```

even though I did not manually execute `OPTIMIZE`.

Its metrics showed:

```text
numRemovedFiles = 3
numAddedFiles = 1
```

Conceptually:

```text
small file 1 ─┐
small file 2 ─┼── OPTIMIZE ──> compacted file
small file 3 ─┘
```

This reduces small-file overhead.

---

## 32. Predictive Optimization

The table configuration was checked with:

```sql
DESCRIBE TABLE EXTENDED workspace.bronze.delta_history_demo
```

The relevant property showed:

```text
Predictive Optimization
ENABLE
(inherited from METASTORE ...)
```

This explained why Databricks automatically ran `OPTIMIZE`.

### Key lesson

Managed Delta tables can receive automatic maintenance operations.

Delta history may therefore contain operations that were not explicitly issued by the developer.

---

## 33. Small File Problem

Repeated operations such as:

```text
append
append
merge
```

can create several small physical files.

A large number of tiny files can increase metadata and file-processing overhead.

`OPTIMIZE` compacts smaller files into fewer larger files.

In this experiment:

```text
3 files
   ↓
OPTIMIZE
   ↓
1 file
```

---

## 34. Managed Tables

The Bronze orders table and the demo table were managed Delta tables.

Observed table information included:

```text
Type = MANAGED
Provider = delta
```

With a managed table, Databricks and Unity Catalog manage:

```text
table metadata
+
underlying table storage
```

This is different from an external table, where the data storage location is managed separately from the table metadata.

---

## 35. Bronze Architecture So Far

The project now looks conceptually like:

```text
Local Olist CSV
      ↓
Databricks CLI upload
      ↓
Unity Catalog Volume
workspace.bronze.raw_files
      ↓
Spark read with explicit schema
      ↓
Add source metadata
      ↓
Managed Delta table
workspace.bronze.orders
```

The Bronze orders table contains:

```text
source order columns
+
_source_file
_source_file_modified_at
_ingested_at
_source_entity
```

---

## 36. Current Lakehouse Namespace

The current project structure is approximately:

```text
workspace
└── bronze
    ├── raw_files
    │   └── olist_orders_dataset.csv
    │
    ├── orders
    │   └── managed Delta table
    │
    └── delta_history_demo
        └── managed Delta learning table
```

Later the project will grow toward:

```text
workspace
├── bronze
├── silver
└── gold
```

---

## 37. Bronze vs Silver

### Bronze

Purpose:

```text
preserve source-level data
+
capture ingestion metadata
```

Typical work:

```text
read raw source
apply explicit schema
capture source metadata
store as Delta
```

### Silver

Purpose:

```text
clean
validate
standardize
derive
reject invalid records
enrich
```

Existing Weeks 1–2 logic such as:

```text
standardizing statuses
delivery metrics
invalid-record handling
join validation
product standardization
payment validation
```

belongs in Silver Delta processing.

---

## 38. Delta Version Timeline from the Demo

The demo table history became:

```text
Version 0
CREATE TABLE
3 rows

        ↓

Version 1
APPEND
row 4

        ↓

Version 2
ADD COLUMNS
extra_column

        ↓

Version 3
APPEND
row 5

        ↓

Version 4
OVERWRITE
rows 100 and 200

        ↓

Version 5
MERGE
update 100
insert 300

        ↓

Version 6
OPTIMIZE
automatic file compaction
```

This is a clear example of Delta table evolution over time.

---

## 39. Main Delta Concepts Learned

### Transaction history

Table-changing operations create Delta versions.

```sql
DESCRIBE HISTORY table_name
```

can be used to inspect them.

### Time travel

Previous snapshots can be queried using:

```sql
VERSION AS OF n
```

Useful for:

```text
debugging
audit investigation
comparing changes
recovering previous states
```

subject to retention rules.

### Schema enforcement

Delta rejects incompatible writes by default.

### Schema evolution

Intentional schema changes can be applied explicitly.

Example:

```sql
ALTER TABLE ... ADD COLUMNS
```

### Append

Adds records while preserving the current table contents.

### Overwrite

Replaces the current table contents.

### MERGE

Supports update-and-insert logic.

Typical pattern:

```text
matched     → update
not matched → insert
```

### OPTIMIZE

Compacts physical data files and reduces small-file overhead.

### Predictive optimization

Databricks can automatically perform maintenance on managed tables.

---

## 40. Important Engineering Lessons

### Reusable schemas belong in modules

Do not duplicate schemas between notebooks.

Use:

```text
src/schemas.py
```

as the single source of truth.

### Bronze should preserve lineage

Metadata such as:

```text
_source_file
_source_file_modified_at
_ingested_at
_source_entity
```

makes source lineage visible.

### Schema changes should be intentional

Unexpected incoming columns should cause investigation.

A production pipeline should not silently accept structural changes without a clear reason.

### Delta history is operational evidence

History can show information such as:

```text
operation
timestamp
readVersion
rows inserted
rows updated
files added
files removed
```

This makes it useful for debugging, auditing, and understanding pipeline behavior.

### Logical table state and physical files are different concepts

Delta exposes a logical transactional table.

Internal features such as:

```text
MERGE
OPTIMIZE
deletion vectors
```

can change how physical files are represented without changing the logical meaning of the table.

---

## 41. Questions I Should Now Be Able to Answer

### What is Delta Lake?

Delta Lake adds transactional table management on top of data stored in files such as Parquet.

It provides features such as table versions, history, schema enforcement, time travel, and `MERGE`.

### What is the difference between Parquet and Delta?

Parquet is primarily a columnar file format.

Delta uses Parquet data files while adding a transaction and table-management layer.

### What does `DESCRIBE HISTORY` show?

It shows operations performed against a Delta table, including versions and operation metadata.

### What is time travel?

Time travel allows a previous Delta table snapshot to be queried using a version or timestamp.

Example:

```sql
SELECT *
FROM some_table
VERSION AS OF 3
```

### What is schema enforcement?

Schema enforcement prevents an incompatible incoming schema from being written accidentally.

### What is schema evolution?

Schema evolution intentionally modifies the target table schema.

### What is the difference between append and overwrite?

`append` adds new records.

`overwrite` replaces the current table contents.

### What does MERGE do?

`MERGE` compares source records with a target using a matching condition.

It can then:

```text
matched     → UPDATE
not matched → INSERT
```

This makes it useful for incremental loads and upserts.

### Why did OPTIMIZE appear even though I did not run it?

The table inherited predictive optimization from the Unity Catalog metastore.

Databricks therefore automatically performed maintenance.

### Why use Bronze, Silver, and Gold?

Bronze preserves source data and lineage.

Silver cleans, validates, standardizes, and enriches data.

Gold presents business-ready dimensional and analytical structures.

---

## 42. Main Takeaway

The biggest conceptual change today was moving from thinking about data as:

```text
files that I read and overwrite
```

to thinking about data as:

```text
versioned transactional tables with history and controlled state changes
```

With plain Parquet, I mainly managed files and paths.

With Delta Lake, I can reason about:

```text
table versions
transactions
schema changes
historical states
upserts
operational history
automatic table maintenance
```

This is the foundation for implementing incremental Bronze → Silver → Gold lakehouse pipelines.
