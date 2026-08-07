# Weeks 1–2 Notes — PySpark Foundations and Practical ETL

These notes summarize the concepts and engineering patterns practiced during Weeks 1 and 2 of the **Azure Retail Lakehouse** project.

The focus was not only on learning PySpark syntax, but on building habits that matter in real data-engineering work: understanding table grain, validating keys, checking joins, handling invalid data, using explicit schemas, and validating written outputs.

---

# PySpark Foundations

## 1. SparkSession and DataFrames

A `SparkSession` is the main entry point for working with Spark SQL and PySpark DataFrames.

Typical workflow:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("azure-retail-lakehouse")
    .getOrCreate()
)
```

A Spark DataFrame is a distributed table-like structure made of rows and typed columns.

Useful inspection methods:

```python
df.show()
df.printSchema()
df.count()
df.columns
```

### Key idea

A DataFrame should not be treated as a Python list of rows. Spark distributes the data into partitions and evaluates transformations lazily.

---

## 2. Transformations vs. Actions

### Transformations

Transformations describe work that Spark should perform.

Examples:

```python
df.select(...)
df.filter(...)
df.withColumn(...)
df.drop(...)
df.join(...)
df.groupBy(...)
df.orderBy(...)
```

They are **lazy**: Spark builds a logical execution plan rather than immediately processing all the data.

### Actions

Actions require Spark to produce a result.

Examples:

```python
df.show()
df.count()
df.collect()
df.write.parquet(...)
```

An action causes Spark to execute the required lineage of transformations.

### Lazy evaluation

Example:

```python
clean_orders = (
    orders
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_status", F.lower(F.trim("order_status")))
)
```

Creating `clean_orders` does not necessarily execute the transformation immediately.

Execution starts when an action needs the result.

---

## 3. Explicit Schemas

Where the source structure is known, explicit schemas are preferable to relying entirely on schema inference.

Example pattern:

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_item_id", IntegerType(), False),
    StructField("price", DecimalType(12, 2), True),
])
```

### Why explicit schemas matter

They:

- make expected data types visible;
- prevent accidental type inference;
- make data-quality assumptions explicit;
- help catch malformed source data earlier;
- avoid using floating-point types for financial values.

For monetary fields such as `price`, `freight_value`, and `payment_value`, decimal types were used instead of floating-point types.

---

## 4. Data Types and Casting

Source CSV columns may initially be strings even when they represent dates, timestamps, integers, or monetary values.

Typical conversions:

```python
F.col("price").cast("decimal(12,2)")
F.to_timestamp("order_purchase_timestamp")
F.to_date("some_date")
```

Correct data types are important because later calculations, comparisons, aggregations, and validation rules depend on them.

---

## 5. Basic Column Operations

Frequently used DataFrame operations included:

```python
df.select(...)
df.filter(...)
df.withColumn(...)
df.alias(...)
df.orderBy(...)
```

Examples:

```python
orders.select(
    "order_id",
    "customer_id",
    "order_status"
)
```

```python
orders.filter(
    F.col("order_id").isNotNull()
)
```

```python
orders.withColumn(
    "order_status",
    F.lower(F.trim(F.col("order_status")))
)
```

---

## 6. Null Handling

Nulls were inspected before deciding how to handle them.

Important principle:

> A null is not automatically an error. Whether it is valid depends on the meaning of the column and the business process.

For example, some delivery timestamps can legitimately be null for orders that were not delivered.

Data-quality checks should therefore consider both the value and the business status of the record.

---

## 7. Deduplication and Key Validation

Before using a column as a key, its uniqueness should be tested.

General pattern:

```python
df.groupBy("key_column").count().filter(
    F.col("count") > 1
)
```

A key should not simply be assumed from its name.

The project explicitly investigated the grain and key structure of each dataset before joining it to other data.

---

# Orders Dataset

## Grain

**One row per order.**

## Primary key

```text
order_id
```

The uniqueness of `order_id` was verified before treating it as the order-table key.

---

## Order Status Profiling

Order statuses were inspected to understand the source data before applying transformations.

Typical profiling pattern:

```python
orders.groupBy("order_status").count().orderBy(
    F.col("count").desc()
)
```

This helps identify:

- expected statuses;
- rare statuses;
- unexpected values;
- values that may need standardization.

---

## Timestamp Parsing

Order timestamp fields were converted into timestamp values so that date calculations could be performed correctly.

Examples include:

```text
order_purchase_timestamp
order_delivered_customer_date
order_estimated_delivery_date
```

---

## Delivery Metrics

Delivery-related fields were derived from the source timestamps.

### Delivery duration

Conceptually:

```text
actual delivery date - purchase date
```

This measures the time between the customer placing the order and receiving it.

### Delivery delay

Conceptually:

```text
actual delivery date - estimated delivery date
```

Interpretation:

```text
delay > 0  -> delivered late
delay = 0  -> delivered on estimated date
delay < 0  -> delivered early
```

An `is_late` indicator was also derived.

These calculations were accompanied by data-quality checks so that invalid timestamp combinations were not silently accepted.

---

## Writing Clean Orders

The cleaned orders dataset was written to Parquet.

After writing it, it was read again and validated.

This is an important engineering habit:

> A successful write operation does not by itself prove that the persisted dataset has the expected schema, row count, or content.

Validation after persistence helps detect path, schema, and write-mode mistakes.

---

# Customers Dataset

## Grain

**One row per customer record.**

## Customer identifiers

Two customer identifiers required different interpretations.

### `customer_id`

Used as the key for the customer record and as the identifier referenced by orders.

### `customer_unique_id`

Represents the logical customer across potentially multiple customer records.

This distinction is important because a technical record key and a business-level customer identity do not always have the same grain.

---

## Customer Standardization

Customer location fields were standardized, including city and state values.

Common string-cleaning operations include:

```python
F.trim(...)
F.lower(...)
F.upper(...)
```

Standardization prevents logically equivalent values from being treated as different categories during grouping or analysis.

---

## Customer Output Validation

The cleaned customers dataset was written to Parquet and read back for validation.

The same read-after-write validation approach used for orders was applied to customers.

---

# Week 2 — Practical PySpark ETL

Week 2 moved from individual DataFrame cleaning into multi-table ETL and analytical transformations.

Key themes were:

- aggregations;
- joins;
- referential-integrity checks;
- join-grain validation;
- date/string logic;
- conditional columns;
- window functions;
- invalid-record handling;
- revenue and payment reconciliation.

---

# 1. Join Types

## Inner join

Returns records that exist on both sides of the join.

```python
left.join(right, "key", "inner")
```

---

## Left join

Preserves all records from the left DataFrame.

```python
left.join(right, "key", "left")
```

This was important for enrichment because dropping transaction rows simply because reference data is missing can hide data-quality problems.

---

## Left-anti join

Returns rows from the left DataFrame that have **no matching record** on the right.

Example:

```python
orders.join(
    customers,
    on="customer_id",
    how="left_anti"
)
```

This is very useful for foreign-key validation.

It answers questions such as:

> Are there orders whose `customer_id` does not exist in the customer dataset?

Similar checks were performed for:

- orders → customers;
- order items → orders;
- order items → products;
- payments → orders.

---

## Left-semi join

A left-semi join keeps left-side rows that have a match on the right, but does not add the right-side columns.

Conceptually it behaves like an existence check.

---

# 2. Join Correctness and Grain

A join that runs successfully is not necessarily correct.

Before a join, the grain of both sides must be understood.

After the join, the row count and key uniqueness should be validated.

General pattern:

```python
before = left_df.count()

joined = left_df.join(
    right_df,
    on="key",
    how="left"
)

after = joined.count()
```

If the expected grain is one row per left-side entity, unexpected row growth can indicate duplicate keys on the right side.

---

## Join fan-out

Suppose the left table contains:

```text
order_id = A
```

once, while the right table contains two matching rows:

```text
A
A
```

A join produces:

```text
A
A
```

The original row has multiplied.

This is why key validation must occur before assuming a one-to-one or many-to-one relationship.

---

# Orders + Customers

Orders were checked for unmatched customer records using a left-anti join.

The cleaned orders were then enriched with customer information using a left join.

The resulting DataFrame was validated to ensure the join preserved the expected grain:

```text
one row per order
```

Customer-state metrics were then calculated, including:

- order counts;
- late-delivery metrics.

---

# Order Items Dataset

## Grain

**One row per item within an order.**

A single order can therefore contain multiple rows.

Example:

```text
order_id    order_item_id
A           1
A           2
A           3
```

## Composite key

```text
(order_id, order_item_id)
```

The combination was verified as unique.

This is a good example of why table grain must be defined before choosing a key.

---

## Explicit Monetary Schema

`price` and `freight_value` were read using decimal types.

A derived item total was calculated from item monetary values.

The project retained financial precision rather than relying on binary floating-point values.

---

## Referential Integrity

Order items with no matching order were checked using a left-anti join.

This type of check prevents orphan transaction records from passing unnoticed into later analytical layers.

---

## Enrichment

Order items were joined with enriched order and customer data.

The expected output grain remained:

```text
one row per order item
```

The join was validated to ensure that enrichment did not multiply rows.

---

# Products Dataset

## Grain

**One row per product.**

## Primary key

```text
product_id
```

The uniqueness of `product_id` was validated.

---

## Product Cleaning

An explicit schema was defined.

Product category values were standardized.

Missing categories were replaced with:

```text
unknown
```

Misspelled source column names were corrected during cleaning.

This separates source-system naming problems from the curated dataset consumed downstream.

---

## Product Referential Integrity

Order items without a matching product were checked using a left-anti join.

Products were then joined to enriched order-item data.

The result was validated to ensure that the grain still remained:

```text
one row per order item
```

---

# Product and Category Metrics

After product enrichment, analytical metrics were calculated by product category.

These included:

- product revenue;
- freight revenue;
- total revenue;
- item counts;
- distinct order counts;
- average item price.

This demonstrates a common analytical pattern:

```python
df.groupBy("dimension_column").agg(...)
```

Example:

```python
(
    df
    .groupBy("product_category_name")
    .agg(
        F.sum("price").alias("product_revenue"),
        F.sum("freight_value").alias("freight_revenue"),
        F.count("*").alias("item_count"),
        F.countDistinct("order_id").alias("order_count"),
        F.avg("price").alias("average_item_price"),
    )
)
```

---

# Payments Dataset

## Grain

**One row per payment sequence within an order.**

An order can therefore have more than one payment record.

## Composite key

```text
(order_id, payment_sequential)
```

This was verified before aggregation.

---

## Payment Cleaning

An explicit schema was defined.

Payment types were standardized.

Data-quality checks included:

- negative payment values;
- invalid installment counts;
- payments without matching orders.

---

# Aggregating Payments Before Joining

The payment dataset has a different grain from the order-items dataset.

Payments:

```text
one row per payment sequence within an order
```

Order items:

```text
one row per item within an order
```

Joining these tables directly can create a many-to-many multiplication.

Example:

```text
Order A:
3 order items
2 payment rows
```

A direct join can produce:

```text
3 × 2 = 6 rows
```

This would duplicate both item and payment amounts.

---

## Correct solution

Aggregate each dataset to the required common grain before joining.

For payments:

```text
payment rows
    ↓
aggregate by order_id
    ↓
one row per order
```

For order-item reconciliation:

```text
order-item rows
    ↓
aggregate by order_id
    ↓
one row per order
```

Then:

```text
one order row
JOIN
one payment summary row
```

This avoids row multiplication.

---

# Payment Metrics

Payment records were aggregated to one row per order.

Metrics included:

- total payment value;
- number of payment rows;
- number of payment methods;
- maximum installment count.

The payment totals were then compared with order-item totals.

---

## Monetary Reconciliation

Order-item totals and payment totals were compared to identify discrepancies.

A one-cent tolerance was used rather than assuming decimal values must always match exactly at every intermediate step.

Conceptually:

```python
F.abs(
    F.col("payment_total") - F.col("order_total")
) <= 0.01
```

This creates a controlled rule for deciding whether small monetary differences are acceptable.

---

# 3. Aggregations

PySpark aggregations commonly use:

```python
df.groupBy(...).agg(...)
```

Frequently used functions include:

```python
F.sum(...)
F.avg(...)
F.count(...)
F.countDistinct(...)
F.min(...)
F.max(...)
```

Important grain rule:

> Before aggregating, state what one output row should represent.

For example:

```text
one row per product category
```

or:

```text
one row per order
```

This prevents mixing measures with incompatible grains.

---

# 4. Date and String Functions

Built-in Spark functions were used for cleaning and derivation.

Examples:

```python
F.trim(...)
F.lower(...)
F.upper(...)
F.regexp_replace(...)
F.to_timestamp(...)
F.to_date(...)
F.datediff(...)
```

Built-in functions are preferred because Spark understands them as part of the query plan.

---

# 5. Conditional Logic

Business rules can be expressed using:

```python
F.when(...).otherwise(...)
```

Example:

```python
.withColumn(
    "is_late",
    F.when(
        F.col("delivery_delay_days") > 0,
        True
    ).otherwise(False)
)
```

Conditional columns make business logic explicit and reusable downstream.

---

# 6. Window Functions

Window functions perform calculations across related rows without collapsing the DataFrame like a `groupBy`.

Typical structure:

```python
from pyspark.sql.window import Window

window = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("order_purchase_timestamp").desc())
)
```

Example: latest order per customer.

```python
latest_order = (
    orders
    .withColumn(
        "row_number",
        F.row_number().over(window)
    )
    .filter(F.col("row_number") == 1)
)
```

Another useful pattern is ranking products by revenue within a category.

### Difference from `groupBy`

`groupBy` changes the grain by collapsing rows.

A window calculation preserves the original rows and adds information based on related rows.

---

# 7. Invalid Records and Rejection Reasons

Invalid rows should not simply disappear from a pipeline without explanation.

A useful pattern is to derive a `rejection_reason` column.

Conceptually:

```python
validated = (
    df
    .withColumn(
        "rejection_reason",
        F.when(
            F.col("required_key").isNull(),
            "missing_required_key"
        )
        .when(
            F.col("amount") < 0,
            "negative_amount"
        )
    )
)
```

Then records can be separated into:

```text
valid records
rejected records
```

This improves:

- traceability;
- debugging;
- auditability;
- rerun safety;
- communication with source-system owners.

---

# 8. Avoiding Ambiguous Columns After Joins

When DataFrames contain columns with the same names, aliases and explicit selections make joins easier to reason about.

Example:

```python
o = orders.alias("o")
c = customers.alias("c")

joined = (
    o.join(
        c,
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("c.customer_state")
    )
)
```

Explicit selection also prevents unnecessary duplicate columns from propagating through the pipeline.

---

# 9. SQL-to-PySpark Mental Mapping

Because many DataFrame operations correspond directly to SQL concepts, it is useful to translate between them.

| SQL | PySpark |
|---|---|
| `SELECT col1, col2` | `df.select("col1", "col2")` |
| `WHERE condition` | `df.filter(condition)` |
| `CASE WHEN` | `F.when(...).otherwise(...)` |
| `GROUP BY` | `df.groupBy(...)` |
| `SUM()` | `F.sum(...)` |
| `COUNT(DISTINCT ...)` | `F.countDistinct(...)` |
| `LEFT JOIN` | `df.join(..., "left")` |
| `INNER JOIN` | `df.join(..., "inner")` |
| `NOT EXISTS` / anti-match | `left_anti` join |
| `ORDER BY` | `df.orderBy(...)` |
| `ROW_NUMBER() OVER (...)` | `F.row_number().over(window)` |
| `CAST()` | `.cast(...)` |

The main difference is not the analytical concept; it is that Spark executes these operations in a distributed environment.

---

# 10. Engineering Lessons from Weeks 1–2

## Always define the grain

Before transforming a dataset, answer:

> What does one row represent?

Examples from the project:

| Dataset | Grain |
|---|---|
| Orders | One row per order |
| Customers | One row per customer record |
| Products | One row per product |
| Order items | One row per item within an order |
| Payments | One row per payment sequence within an order |

Grain determines:

- valid keys;
- join relationships;
- aggregation logic;
- duplicate detection;
- whether a join can multiply rows.

---

## Validate keys instead of assuming them

A column named `_id` is not automatically unique.

Primary or composite keys were checked explicitly before relying on them.

---

## Use anti joins for integrity checks

Before enriching transaction data, verify that referenced entities exist.

Examples:

```text
orders without customers
order items without orders
order items without products
payments without orders
```

A left-anti join makes these problems visible.

---

## Validate row counts after joins

A successful Spark join can still produce an incorrect dataset.

If the expected left-side grain should be preserved, validate:

- row count;
- key uniqueness;
- unmatched keys;
- unexpected duplication.

---

## Never combine incompatible grains carelessly

The payments/order-items example is especially important.

Two child tables of the same parent should not usually be joined directly when both contain multiple rows per parent.

Instead:

```text
aggregate child A
aggregate child B
join at common grain
```

This prevents many-to-many row multiplication.

---

## Persisted output should be validated

After writing Parquet:

1. read the dataset again;
2. inspect the schema;
3. validate row count;
4. validate keys;
5. inspect important derived columns.

Writing is part of the pipeline, not the end of validation.

---

## Prefer explicit, inspectable transformations

Cleaning and business logic should be visible in the DataFrame pipeline.

Examples:

```text
standardized category
delivery delay
is_late
rejection_reason
item total
payment reconciliation
```

This makes transformations easier to test and explain.

---



By the end of these two weeks, I should be able to explain the following without looking at code.

### Spark basics

- What is a SparkSession?
- What is a DataFrame?
- What is the difference between a transformation and an action?
- What is lazy evaluation?
- Why use an explicit schema?
- Why use decimal types for financial data?
- Why validate data after writing it?

### Data modelling and grain

- What is the grain of each project dataset?
- What is the difference between `customer_id` and `customer_unique_id`?
- Why does `order_items` need a composite key?
- Why does `payments` need a composite key?

### Joins

- What is the difference between inner, left, left-semi, and left-anti joins?
- Why use a left-anti join for foreign-key checks?
- How can a join accidentally multiply rows?
- How do I verify that a join preserved the expected grain?

### Transformations

- How do `groupBy` and `agg` work?
- When would I use a window function instead of `groupBy`?
- How can I calculate the latest order per customer?
- How can I rank products within category?
- How do I represent invalid records and rejection reasons?

### Project-specific engineering

- How were late deliveries calculated?
- Why were missing product categories converted to `unknown`?
- Why were payments aggregated before comparison with order-item totals?
- Why is a one-cent tolerance useful for monetary reconciliation?
- Why should order-items and payments not be joined directly at their raw grains?

---

# 12. Concise Week 1 Summary

Week 1 established the PySpark foundation:

- configured local PySpark;
- created and inspected DataFrames;
- learned transformations, actions, and lazy evaluation;
- used explicit schemas;
- cast timestamps and monetary columns;
- cleaned and validated orders;
- derived delivery metrics;
- cleaned customer data;
- distinguished record keys from logical customer identity;
- wrote cleaned datasets to Parquet;
- read the written data back and validated it.

---

# 13. Concise Week 2 Summary

Week 2 extended the project into practical ETL:

- used aggregations and several join types;
- performed left-anti referential-integrity checks;
- enriched orders with customer information;
- validated join grain and row counts;
- cleaned order-item, product, and payment datasets;
- used composite keys where required;
- calculated revenue and delivery metrics;
- used window functions;
- handled invalid records with rejection logic;
- joined products to order items without changing order-item grain;
- aggregated payments to one row per order;
- reconciled payment totals against order-item totals;
- avoided many-to-many row multiplication by aggregating before joining.

---

# 14. Main Takeaway

The most important lesson from Weeks 1–2 is that correct data engineering is not just about producing a DataFrame that runs successfully.

For every pipeline step, ask:

1. **What is the grain?**
2. **What is the key?**
3. **What records are invalid?**
4. **What does the join relationship look like?**
5. **Did the join change the grain unexpectedly?**
6. **Are financial and timestamp columns using appropriate types?**
7. **Can the written output be read back and validated?**

These checks make the pipeline explainable, testable, and much safer to extend into Spark execution tuning, Delta Lake, incremental processing, and dimensional modelling.
