# Spark Execution Notes

## Filter and select

Filtering order items by price and selecting a subset of columns did
not introduce an Exchange in the physical plan.

Spark pushed the price predicate into the Parquet scan and pruned
unused columns.

Therefore these transformations did not require data redistribution
between partitions.

## Aggregation by product category

Grouping order items by product category introduced an Exchange using:

hashpartitioning(product_category_name, 200)

Rows had to be redistributed so that records belonging to the same
category could be aggregated together.

Spark performed partial aggregation before the shuffle and final
aggregation afterwards.

## Count distinct

Adding countDistinct(order_id) resulted in two Exchanges.

The first repartitioned by product_category_name and order_id to support
distinct order counting. The second repartitioned by product category
for the final category-level aggregation.

## Global sorting

orderBy(price) introduced an Exchange using range partitioning.

Spark needed to redistribute rows across partitions to create a global
ordering.

## Product join

Joining order_items to products produced a BroadcastHashJoin with
products as BuildRight.

Because products was small enough to broadcast, Spark distributed the
products relation rather than repartitioning both DataFrames by
product_id.

## Partition sizing and skew

Spark partitions determine how data can be processed in parallel.

Too few partitions can limit parallelism and create very large tasks.
Too many small partitions can introduce task scheduling and file overhead.

repartition(n) redistributes rows across a requested number of partitions
and usually produces relatively balanced partitions.

repartition(n, key) hash-partitions by the key. Rows with the same key
are assigned to the same partition, but uneven key frequencies can cause
data skew.

Data skew occurs when some partitions contain significantly more rows or
data than others, causing a small number of slow tasks to delay the
entire Spark stage.

spark.sql.shuffle.partitions controls the default number of partitions
used by many shuffle operations. In my local Spark session it is 200.

Manual repartitioning should have a reason; Spark automatically inserts
required Exchanges for joins, aggregations and sorts.

## Caching and driver-side collection

The Spark driver coordinates execution, while executors process
partitions and perform distributed transformations.

collect() returns every DataFrame row to the driver. This can exhaust
driver memory for large datasets and should only be used when the
result is known to be small.

count() is different because Spark performs the aggregation
distributed and only returns the final count.

Caching is useful when an expensive intermediate DataFrame will be
reused by multiple actions.

cache() is lazy. The cache is populated when an action causes the
DataFrame to be evaluated.

Cached data still requires downstream shuffles when operations such as
groupBy require redistribution.

Caching has a resource cost, so DataFrames should not be cached
automatically. Cached datasets should be unpersisted when no longer
needed.

## Built-in functions versus Python UDFs

Spark built-in functions should normally be preferred over Python UDFs.

Built-in expressions are visible to Spark's Catalyst optimizer and can
be executed efficiently inside Spark's execution engine.

Traditional Python UDFs require data to cross between the JVM and
Python execution environment and Spark has less visibility into the
function's internal logic.

I should first check whether a transformation can be implemented with
Spark SQL or DataFrame functions before creating a Python UDF.

Python UDFs are appropriate when genuinely custom logic cannot
reasonably be expressed using Spark's built-in functions.