# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: dimension snapshots
# MAGIC
# MAGIC Dimensions (customers, products, warehouses, promotions) are loaded as full
# MAGIC batch snapshots on every run (`CREATE OR REPLACE`, not append), because the
# MAGIC landing zone holds the complete current file each time, not an incremental
# MAGIC diff. This differs from orders/order_items/returns, which are a true
# MAGIC incremental Autoloader stream.
# MAGIC
# MAGIC Runs as a Databricks Workflow task, or locally via Databricks Connect.

# COMMAND ----------

# Spark session: injected in notebooks, built explicitly under Databricks Connect
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("urban_cart").serverless(True).getOrCreate()

# COMMAND ----------

# Config: catalog/schema arrive as job parameters (via widgets) when deployed.
# The fallback is for local Connect runs and deliberately points at DEV, never prod.
try:
    dbutils.widgets.text("catalog", "urban_cart_dev")
    dbutils.widgets.text("schema", "asikpodavid2")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
except NameError:
    CATALOG = "urban_cart_dev"
    SCHEMA = "asikpodavid2"

VOLUME_BASE = "/Volumes/urban_cart/urban_cart_bronze/landing_zone/dimensions/"

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Dimension definitions: one entry per landing file / bronze table
DIMENSIONS = [
    {
        "table_name": "customers_raw",
        "file_name": "customers.csv",
        "schema": (
            "customer_id string, first_name string, last_name string, "
            "email string, signup_date date, country string, "
            "marketing_opt_in boolean"
        ),
    },
    {
        "table_name": "products_raw",
        "file_name": "products.csv",
        "schema": (
            "product_id string, product_name string, category_raw string, "
            "unit_price double, cost_price double, active boolean"
        ),
    },
    {
        "table_name": "warehouses_raw",
        "file_name": "warehouses.csv",
        "schema": "warehouse_id string, warehouse_name string, region string",
    },
    {
        "table_name": "promotions_raw",
        "file_name": "promotions.csv",
        "schema": (
            "promo_code string, description string, discount_pct int, "
            "valid_from date, valid_to date"
        ),
    },
]

# COMMAND ----------

def ingest_dimension(table_name: str, file_name: str, schema: str) -> int:
    """Batch-load one dimension CSV into its bronze table. Returns row count."""
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    file_path = f"{VOLUME_BASE}/{file_name}"

    spark.sql(f"""
        CREATE OR REPLACE TABLE {full_table_name} AS
        SELECT
            *,
            current_timestamp() AS _ingested_at,
            '{file_path}' AS _source_file
        FROM read_files(
            '{file_path}',
            format => 'csv',
            header => true,
            schema => '{schema}'
        )
    """)

    row_count = spark.table(full_table_name).count()
    print(f"  {full_table_name}: {row_count} rows")
    return row_count

# COMMAND ----------

# Run: ensure the schema exists, then load every dimension
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Ingesting dimensions into {CATALOG}.{SCHEMA} ...")
total = 0
for dim in DIMENSIONS:
    total += ingest_dimension(dim["table_name"], dim["file_name"], dim["schema"])
print(f"Done. {total} total rows across {len(DIMENSIONS)} dimension tables.")