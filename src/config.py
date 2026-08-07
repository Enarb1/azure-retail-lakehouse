from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent


RAW_DIR = PROJECT_ROOT / "data" / "raw" / "olist"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

ORDERS_RAW_PATH = RAW_DIR / "olist_orders_dataset.csv"
CUSTOMERS_RAW_PATH = RAW_DIR / "olist_customers_dataset.csv"
PRODUCTS_RAW_PATH = RAW_DIR / "olist_products_dataset.csv"
ORDER_ITEMS_RAW_PATH = RAW_DIR / "olist_order_items_dataset.csv"
PAYMENTS_RAW_PATH = RAW_DIR / "olist_order_payments_dataset.csv"

ORDERS_SILVER_PATH = SILVER_DIR / "orders"
CUSTOMERS_SILVER_PATH = SILVER_DIR / "customers"
PRODUCTS_SILVER_PATH = SILVER_DIR / "products"
ORDER_ITEMS_SILVER_PATH = SILVER_DIR / "order_items"
PAYMENTS_SILVER_PATH = SILVER_DIR / "payments"

ORDERS_ENRICHED_PATH = SILVER_DIR / "orders_enriched"
ORDER_ITEMS_WITH_PRODUCTS_PATH = SILVER_DIR / "order_items_with_products"