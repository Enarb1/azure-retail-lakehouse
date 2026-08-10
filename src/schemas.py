from pyspark.sql.types import StructType, StructField, TimestampType, StringType

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_status", StringType(), nullable=True),
    StructField("order_purchase_timestamp", TimestampType(), nullable=True),
    StructField("order_approved_at", TimestampType(), nullable=True),
    StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
    StructField("order_delivered_customer_date", TimestampType(), nullable=True),
    StructField("order_estimated_delivery_date", TimestampType(), nullable=True),
])