from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType, DecimalType

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


CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("customer_unique_id", StringType(), nullable=False),
    StructField("customer_zip_code_prefix", StringType(), nullable=True),
    StructField("customer_city", StringType(), nullable=True),
    StructField("customer_state", StringType(), nullable=True)
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_item_id", IntegerType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("seller_id", StringType(), nullable=False),
    StructField("shipping_limit_date", TimestampType(), nullable=True),
    StructField("price", DecimalType(12, 2), nullable=True),
    StructField("freight_value", DecimalType(12, 2), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_category_name", StringType(), nullable=True),
    StructField("product_name_lenght", IntegerType(), nullable=True),
    StructField("product_description_lenght", IntegerType(), nullable=True),
    StructField("product_photos_qty", IntegerType(), nullable=True),
    StructField("product_weight_g", IntegerType(), nullable=True),
    StructField("product_length_cm", IntegerType(), nullable=True),
    StructField("product_height_cm", IntegerType(), nullable=True),
    StructField("product_width_cm", IntegerType(), nullable=True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("payment_sequential", IntegerType(), nullable=False),
    StructField("payment_type", StringType(), nullable=True),
    StructField("payment_installments", IntegerType(), nullable=True),
    StructField("payment_value", DecimalType(10, 2), nullable=True),
])