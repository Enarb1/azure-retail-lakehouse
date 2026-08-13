# Gold Data Dictionary

**Project:** Azure Retail Lakehouse  
**Layer:** Gold  
**Purpose:** Dimensional model for retail analytics built from validated Silver Delta tables.

---

## Gold Model Overview

The Gold layer contains three core dimensions and one fact table:

- `workspace.gold.dim_customer`
- `workspace.gold.dim_product`
- `workspace.gold.dim_date`
- `workspace.gold.fact_order_item`

Optional/demo tables created during Week 4:

- `workspace.gold.fact_order_item_incremental_demo`
- `workspace.gold.dim_product_type1_demo`
- `workspace.gold.dim_customer_type2_demo`

The core fact grain is:

> **One row per product line within one order.**

The natural fact key is:

```text
(order_id, order_item_id)
```

---

# 1. `dim_customer`

## Purpose

Stores the current Gold representation of a logical customer.

The Silver source contains:

- `customer_id` — order-facing customer record identifier
- `customer_unique_id` — logical customer identifier across purchases

Because one `customer_unique_id` can map to multiple `customer_id` values, the Gold customer dimension is built at the logical-customer grain.

## Grain

> **One row per `customer_unique_id`, plus one unknown/default customer row.**

## Source

```text
workspace.silver.customers
workspace.silver.orders
```

Order timestamps are used to determine the most recently observed customer record/location.

## Keys

| Column | Type | Role |
|---|---|---|
| `customer_key` | long | Gold surrogate key |
| `customer_unique_id` | string | Source/business key |

The surrogate key is generated deterministically:

```python
F.xxhash64("customer_unique_id")
```

This produces a stable key for the same customer across reruns.

## Unknown Member

The dimension reserves:

```text
customer_key = 0
```

for the default/unknown customer.

Example:

```text
customer_key = 0
customer_unique_id = UNKNOWN
customer_zip_code_prefix = 00000
customer_city = unknown
customer_state = UN
```

This allows a valid fact to retain referential integrity if its real customer dimension record is not yet available.

## Columns

| Column | Description |
|---|---|
| `customer_key` | Warehouse surrogate key |
| `customer_unique_id` | Logical customer business key |
| `customer_zip_code_prefix` | ZIP-code prefix from the latest observed customer record |
| `customer_city` | Latest observed customer city |
| `customer_state` | Latest observed customer state |

## Current Row Count

```text
96,097
```

This consists of:

```text
96,096 real logical customers
1 unknown/default customer
```

---

# 2. `dim_product`

## Purpose

Stores descriptive product attributes used by the fact table and analytical queries.

## Grain

> **One row per `product_id`, plus one unknown/default product row.**

## Source

```text
workspace.silver.products
```

## Keys

| Column | Type | Role |
|---|---|---|
| `product_key` | long | Gold surrogate key |
| `product_id` | string | Source/business key |

The surrogate key is generated deterministically:

```python
F.xxhash64("product_id")
```

## Unknown Member

The dimension reserves:

```text
product_key = 0
```

for an unknown/default product.

## Columns

| Column | Description |
|---|---|
| `product_key` | Warehouse surrogate key |
| `product_id` | Source product business key |
| `product_category_name` | Standardized product category |
| `product_name_length` | Length of the product name |
| `product_description_length` | Length of the product description |
| `product_photos_qty` | Number of product photos |
| `product_weight_g` | Product weight in grams |
| `product_length_cm` | Product length in centimetres |
| `product_height_cm` | Product height in centimetres |
| `product_width_cm` | Product width in centimetres |

## Current Row Count

```text
32,952
```

This consists of:

```text
32,951 real products
1 unknown/default product
```

---

# 3. `dim_date`

## Purpose

Provides reusable calendar attributes for reporting and analytical grouping.

## Grain

> **One row per calendar date.**

## Source

Generated from the valid Silver order purchase-date range.

Observed date range:

```text
2016-09-04
to
2018-10-17
```

## Key

| Column | Type | Role |
|---|---|---|
| `date_key` | integer | Date key in `YYYYMMDD` format |

Example:

```text
2018-07-28 -> 20180728
```

## Columns

| Column | Description |
|---|---|
| `date_key` | Integer date key in `YYYYMMDD` format |
| `full_date` | Calendar date |
| `day_of_month` | Day number within the month |
| `day_name` | Full weekday name |
| `week_of_year` | Week number within the year |
| `month_number` | Month number |
| `month_name` | Full month name |
| `quarter` | Calendar quarter |
| `year` | Calendar year |
| `is_weekend` | Boolean indicator for Saturday/Sunday |

## Current Row Count

```text
774
```

---

# 4. `fact_order_item`

## Purpose

Stores order-line measures and foreign keys to the Gold dimensions.

## Grain

> **One row per product line within one order.**

This grain is preserved from the Silver order-items table.

## Natural Fact Key

```text
(order_id, order_item_id)
```

This composite key was validated as unique.

## Sources

```text
workspace.silver.order_items
workspace.silver.orders
workspace.silver.customers
workspace.gold.dim_customer
workspace.gold.dim_product
workspace.gold.dim_date
```

## Dimension Foreign Keys

| Column | References |
|---|---|
| `customer_key` | `dim_customer.customer_key` |
| `product_key` | `dim_product.product_key` |
| `date_key` | `dim_date.date_key` |

During the Gold build:

```text
Facts using unknown customer_key: 0
Facts using unknown product_key: 0
Facts with missing date_key: 0
```

## Columns

| Column | Description |
|---|---|
| `order_id` | Source order identifier |
| `order_item_id` | Item sequence within an order |
| `customer_key` | Foreign key to `dim_customer` |
| `product_key` | Foreign key to `dim_product` |
| `date_key` | Foreign key to `dim_date` |
| `seller_id` | Source seller identifier |
| `price` | Product-line price |
| `freight_value` | Product-line freight charge |
| `item_total` | `price + freight_value` |
| `order_status` | Standardized order status |
| `delivery_days` | Days from purchase to customer delivery |
| `delivery_delay_days` | Difference between actual and estimated delivery date |
| `is_late` | Boolean late-delivery indicator |

## Measures

The main line-level measures are:

```text
price
freight_value
item_total
```

Delivery metrics are also retained for analytical use:

```text
delivery_days
delivery_delay_days
is_late
```

## Current Row Count

```text
112,642
```

## Fact Quality Checks

Validated:

```text
duplicate fact keys: 0
null customer_key: 0
null product_key: 0
null date_key: 0
negative price: 0
negative freight: 0
negative item_total: 0
```

---

# 5. Gold Integrity Checks

The following uniqueness checks were performed:

```text
duplicate_customer_keys: 0
duplicate_product_keys: 0
duplicate_date_keys: 0
duplicate_fact_keys: 0
```

The fact joins preserved the original Silver fact grain:

```text
Silver order items:   112,642
Gold fact_order_item: 112,642
```

---

# 6. Surrogate-Key Strategy

The project uses deterministic 64-bit hashes for customer and product surrogate keys:

```python
F.xxhash64(...)
```

## Why this strategy was used

- same business key produces the same warehouse key on reruns
- simple to implement in Spark
- avoids unstable IDs from `row_number()` or `monotonically_increasing_id()`
- useful for demonstrating repeatable Gold builds

## Trade-off

Hash collisions are theoretically possible, although no collisions were found in this dataset.

Validated:

```text
duplicate generated customer keys: 0
duplicate generated product keys: 0
```

The value `0` was confirmed unused and reserved for unknown/default members.

---

# 7. Incremental Fact-Load Strategy

The Olist source does not provide a true `updated_at` field, so the project uses:

```text
order_purchase_timestamp
```

as a controlled watermark for the Week 4 incremental-loading demonstration.

Demo watermark:

```text
2018-01-01
```

The fact data was split into:

```text
Initial batch:      51,232 rows
Incremental batch:  61,410 rows
Total:             112,642 rows
```

The initial batch was written to:

```text
workspace.gold.fact_order_item_incremental_demo
```

The later batch was applied with Delta `MERGE` using the fact grain:

```sql
ON target.order_id = source.order_id
AND target.order_item_id = source.order_item_id
```

## MERGE Result

After the first incremental MERGE:

```text
Target rows: 112,642
```

Delta history showed:

```text
61,410 rows inserted
0 rows updated
```

The same incremental batch was then rerun.

Result:

```text
Target rows after rerun: 112,642
Duplicate fact keys after rerun: 0
```

Delta history showed that the second MERGE matched and updated the existing 61,410 rows rather than inserting duplicates.

## Full-Load Comparison

The incremental-demo table was compared with the full Gold fact table using `exceptAll()`.

Results:

```text
Rows in full fact but not incremental: 0
Rows in incremental but not full fact: 0
```

Therefore the controlled incremental process produced exactly the same logical result as the full load.

---

# 8. Idempotency

The incremental fact load was explicitly rerun with the same input batch.

The second run did not create additional fact rows.

This demonstrates data-level idempotency:

> Processing the same logical input again produces the same final target dataset.

A useful implementation note is that:

```sql
WHEN MATCHED THEN UPDATE SET *
```

still rewrites matched rows even when the values have not changed.

Therefore:

> Idempotent final data does not necessarily mean zero processing work.

A future optimization could update only when tracked values actually differ.

---

# 9. SCD Type 1 Demonstration

Demo table:

```text
workspace.gold.dim_product_type1_demo
```

A real product was selected:

```text
product_id = 00066f42aeeb9f3007548bb9d3f33c38
```

Original category:

```text
perfumaria
```

Simulated corrected category:

```text
fragrances
```

A Delta `MERGE` updated the existing dimension row in place.

The following remained unchanged:

```text
product_id
product_key
```

The descriptive attribute changed:

```text
product_category_name:
perfumaria -> fragrances
```

The previous value was not preserved.

This demonstrates **Slowly Changing Dimension Type 1**.

---

# 10. SCD Type 2 Customer Demonstration

Demo table:

```text
workspace.gold.dim_customer_type2_demo
```

A logical customer with two observed locations was used:

```text
customer_unique_id = 0178b244a5c281fb2ade54038dd4b161
```

Observed history:

```text
2017-05-10 -> guaratingueta, SP
2018-07-28 -> novo horizonte, SP
```

The Type 2 table stores two rows for the same business key with different surrogate keys.

## Type 2 Columns

| Column | Description |
|---|---|
| `customer_key` | Surrogate key for the specific historical version |
| `customer_unique_id` | Logical customer business key |
| `customer_city` | City for that historical version |
| `customer_state` | State for that historical version |
| `effective_from` | Timestamp from which the version is valid |
| `effective_to` | Exclusive end timestamp; null for the current row |
| `is_current` | Indicates the current active version |

Example:

```text
Version 1:
guaratingueta, SP
effective_from = 2017-05-10 20:04:09
effective_to   = 2018-07-28 13:13:00
is_current     = false

Version 2:
novo horizonte, SP
effective_from = 2018-07-28 13:13:00
effective_to   = NULL
is_current     = true
```

Validation:

```text
current rows for customer: 1
invalid Type 2 date ranges: 0
```

This demonstrates **Slowly Changing Dimension Type 2**.

---

# 11. Late-Arriving Data Strategy

The Gold dimensions reserve key `0` for unknown members.

If a valid fact arrives before the matching customer or product dimension row exists, the fact can be loaded temporarily using:

```text
customer_key = 0
```

or:

```text
product_key = 0
```

When the missing dimension record arrives later, the affected fact can be reprocessed or updated to point to the real surrogate key.

The current Olist Gold build does not require this fallback because all valid facts matched existing customer and product dimensions.

---

# 12. Gold Deliverables Covered

The current Gold implementation demonstrates:

- Gold customer dimension
- Gold product dimension
- generated date dimension
- order-item fact table
- documented fact grain
- business-key and surrogate-key strategy
- unknown/default dimension members
- dimension/fact joins
- fact-grain validation
- null-key and invalid-measure tests
- incremental filtering using a watermark
- Delta `MERGE`
- rerun/idempotency validation
- Type 1 dimension update
- Type 2 customer-history example
- late-arriving dimension/fact handling conceptually
- comparison of incremental output with the full-load result
