# Likely Spark Shuffles in the Retail Lakehouse

A **shuffle** occurs when Spark must redistribute data between partitions, usually because rows with the same key need to be brought together. Shuffles are relatively expensive because they can involve network transfer, serialization, disk I/O, and additional stages.

## Aggregations

Operations such as:

```python
df.groupBy("product_category_name").agg(
    F.sum("price")
)
```

are likely to cause a shuffle. Spark must move rows so that all records for the same category are processed together. In the physical plan this appears as an **Exchange**, usually with `hashpartitioning`.

`countDistinct()` may require additional redistribution compared with a normal aggregation because Spark must deduplicate values before producing the final result.

## Global sorting

Operations such as:

```python
df.orderBy(F.col("price").desc())
```

cause a shuffle because Spark needs a globally ordered result. In the execution plan this appeared as a range-partitioning **Exchange** followed by a `Sort`.

## Joins

Joins can cause large shuffles when both sides must be repartitioned on the join key.

For small lookup tables, Spark can instead use a **broadcast join**. For example, when checking `order_items → products`, the products table was small enough to broadcast:

```text
PhotonBroadcastHashJoin LeftAnti
```

This avoids shuffling the larger `order_items` side. I also used `F.broadcast()` explicitly when the automatic execution was unexpectedly slow.

The same principle applies to referential-integrity checks such as:

```python
payments.join(
    F.broadcast(orders.select("order_id")),
    "order_id",
    "left_anti"
)
```

## Window functions

Window functions such as:

```python
Window.partitionBy("customer_id").orderBy(...)
```

are likely to cause a shuffle because all rows belonging to the same customer must be placed in the same partition. Spark may also need to sort rows inside each partition.

Examples in this project include finding the latest order per customer and ranking products within categories.

## `repartition()` and `coalesce()`

`repartition()` explicitly causes redistribution:

```python
df.repartition(4, "product_id")
```

The execution plan showed an **Exchange** with hash partitioning.

`coalesce()` normally reduces the number of partitions without a full shuffle, so it is cheaper when reducing partitions. However, if it follows a previous `repartition()`, the earlier shuffle still remains in the plan.

## Narrow transformations

Operations such as:

```python
select()
filter()
withColumn()
```

usually do **not** require a shuffle by themselves. For example, filtering `price > 100` produced a scan, filter and projection without an Exchange node.

Spark was also able to apply **predicate pushdown** and **column pruning** when reading Parquet/Delta data.

## How I identify shuffles

I use:

```python
df.explain("formatted")
```

and look for nodes such as:

```text
Exchange
ShuffleExchange
PhotonShuffleExchange
```

The main places where I expect shuffles in this retail lakehouse are therefore **aggregations, distinct calculations, global sorts, non-broadcast joins, window functions and explicit repartitioning**.

The goal is not to eliminate every shuffle—many are required for correct distributed processing—but to recognize them and avoid unnecessary ones.
