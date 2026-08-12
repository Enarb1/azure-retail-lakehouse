#  Silver Layer, Delta History, and Databricks Jobs

## 1. Silver Products

Loaded the Bronze products table:

```python
bronze_products = spark.table("workspace.bronze.products")
```

Bronze row count:

```text
32,951
```

### Cleaning

Corrected the misspelled source column names:

```python
silver_products_base = (
    bronze_products
    .withColumnRenamed(
        "product_name_lenght",
        "product_name_length"
    )
    .withColumnRenamed(
        "product_description_lenght",
        "product_description_length"
    )
    .withColumn(
        "product_category_name",
        F.when(
            F.col("product_category_name").isNull(),
            F.lit("unknown")
        ).otherwise(
            F.lower(F.trim(F.col("product_category_name")))
        )
    )
)
```

### Data-quality checks

Validated:

- `product_id` is not null
- no negative product weight
- no negative length
- no negative height
- no negative width
- no negative photo quantity
- no duplicate `product_id`

Results:

```text
total_rows:           32,951
null_product_id:      0
negative_weight:      0
negative_length:      0
negative_height:      0
negative_width:       0
negative_photos_qty:  0
duplicate_product_id: 0
```

### Referential integrity

Checked that every valid Silver order item references an existing product.

```python
unmatched_products = (
    silver_order_items_ref
    .join(
        F.broadcast(
            silver_products_base.select("product_id")
        ),
        on="product_id",
        how="left_anti"
    )
)
```

Result:

```text
Silver order items with no matching product: 0
```

The explicit `F.broadcast()` was used after the initial automatic join execution was unexpectedly slow.

### Persisted table

```text
workspace.silver.products
Rows: 32,951
Format: Delta
Files: 1
```

---

## 2. Silver Payments

Loaded the Bronze payments table.

```python
bronze_payments = spark.table("workspace.bronze.payments")
```

Bronze row count:

```text
103,886
```

### Cleaning

Standardized `payment_type`:

```python
silver_payments_base = (
    bronze_payments
    .withColumn(
        "payment_type",
        F.lower(F.trim(F.col("payment_type")))
    )
)
```

### Data-quality checks

Validated:

- `order_id` is not null
- `payment_sequential` is not null
- `payment_type` is not null
- no negative `payment_value`
- no negative `payment_installments`
- composite key `(order_id, payment_sequential)` is unique

Results:

```text
total_rows:               103,886
null_order_id:            0
null_payment_sequential:  0
null_payment_type:        0
negative_payment_value:   0
negative_installments:    0
duplicate_payment_keys:   0
```

### Referential integrity

Checked payments against valid Silver orders.

```text
Payment rows with no matching Silver order: 8
```

All eight unmatched payment rows belonged to orders already rejected because of:

```text
delivered_status_missing_delivery_date
```

### Valid/rejected split

```python
silver_payments = (
    silver_payments_base
    .join(
        silver_orders_ref.select("order_id"),
        on="order_id",
        how="left_semi"
    )
)

rejected_payments = (
    silver_payments_base
    .join(
        silver_orders_ref.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    .withColumn(
        "rejection_reason",
        F.lit("parent_order_rejected")
    )
)
```

Counts:

```text
Silver payments:   103,878
Rejected payments:       8
Total:             103,886
```

### Persisted tables

```text
workspace.silver.payments           103,878 rows
workspace.silver.rejected_payments        8 rows
```

---

## 3. Final Silver Referential-Integrity Check

Validated Silver orders against Silver customers.

```text
Silver orders with no matching customer: 0
```

This confirmed that the valid Silver layer is internally consistent across the core relationships checked:

- orders → customers
- order_items → orders
- order_items → products
- payments → orders

---

## 4. Silver Layer Validation Checkpoint

Final row counts:

| Table | Rows | Rejected |
|---|---:|---:|
| `orders` | 99,433 | 8 |
| `customers` | 99,441 | 0 |
| `order_items` | 112,642 | 8 |
| `products` | 32,951 | 0 |
| `payments` | 103,878 | 8 |

Persisted Silver tables:

```text
workspace.silver.customers
workspace.silver.order_items
workspace.silver.orders
workspace.silver.payments
workspace.silver.products
workspace.silver.rejected_order_items
workspace.silver.rejected_orders
workspace.silver.rejected_payments
```

### Rejection reasons

```text
rejected_orders
  delivered_status_missing_delivery_date: 8

rejected_order_items
  parent_order_rejected: 8

rejected_payments
  parent_order_rejected: 8
```

This documents both the direct order-level DQ failure and the downstream parent-child rejection logic.

---

## 5. Delta Table History

Used `DESCRIBE HISTORY` across the five main Silver tables:

```python
silver_tables = [
    "workspace.silver.orders",
    "workspace.silver.customers",
    "workspace.silver.order_items",
    "workspace.silver.products",
    "workspace.silver.payments"
]

for table in silver_tables:
    print(f"\n{table}")

    spark.sql(f"""
        DESCRIBE HISTORY {table}
    """).select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    ).show(truncate=False)
```

Observed:

```text
orders      versions 0, 1, 2
customers   versions 0, 1, 2
order_items versions 0, 1
products    version 0
payments    version 0
```

Repeated overwrites created new Delta versions even when the final business data remained unchanged.

---

## 6. Delta Time Travel

Queried version 0 of the Silver orders table:

```python
orders_v0 = spark.sql("""
    SELECT *
    FROM workspace.silver.orders VERSION AS OF 0
""")

orders_current = spark.table("workspace.silver.orders")
```

Counts:

```text
Orders version 0: 99,433
Orders current:   99,433
```

Then compared both versions using `exceptAll()`:

```python
v0_not_current = orders_v0.exceptAll(orders_current).count()
current_not_v0 = orders_current.exceptAll(orders_v0).count()
```

Results:

```text
Rows in v0 but not current: 0
Rows in current but not v0: 0
```

This demonstrated that Delta history can contain multiple versions even when later overwrites produce logically identical data.

---

## 7. Parameterized Silver Notebook

Updated `10_delta_silver.ipynb` to avoid hard-coded catalog references.

Added a Databricks widget:

```python
dbutils.widgets.text("catalog", "workspace")

catalog = dbutils.widgets.get("catalog")

print(f"Using catalog: {catalog}")
```

Table references were changed from hard-coded values such as:

```python
spark.table("workspace.bronze.orders")
```

to parameterized references:

```python
spark.table(f"{catalog}.bronze.orders")
```

The notebook was rerun successfully after updating all catalog references.

---

## 8. Databricks Job / Workflow

Because the notebook originally existed only locally in PyCharm, it was imported into the Databricks Workspace under the project folder.

A Databricks Job was then created with `10_delta_silver.ipynb` as a notebook task.

Task parameter:

```text
catalog = workspace
```

The Job completed successfully.

The notebook run output confirmed that the Job parameter was passed into the widget:

```text
Using catalog: workspace
```

This demonstrated:

- Databricks Jobs / Workflows
- notebook task execution
- notebook parameters
- `dbutils.widgets`
- parameterized catalog references
- successful end-to-end Silver notebook execution

---

## 9. Deliverables Status

Completed:

- Bronze Delta tables with source metadata
- Silver Delta tables with cleaned records
- rejected-record Delta tables with rejection reasons
- Spark shuffle/execution note
- Delta history demonstration
- Delta time-travel query
- Databricks Jobs / Workflows
- notebook parameterization

