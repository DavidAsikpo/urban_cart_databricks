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
from pyspark.sql import functions as F

# COMMAND ----------

# Config: catalog/schema arrive as job parameters (via widgets) when deployed.
try:
    dbutils.widgets.text("catalog", "urban_cart_dev")
    dbutils.widgets.text("schema", "asikpodavid2")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
except NameError:
    CATALOG = "urban_cart_dev"
    SCHEMA = "asikpodavid2"

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Dimension definitions: one entry per landing file / bronze table
DIMENSIONS = [
    {
        "table_name": "customers_raw",
        "key_column": "customer_id",
        "file_type": "json",
        "is_cdc": True,
        "volume_path": "/Volumes/urban_cart/bronze/landing_zone/dimensions/customers/*.json",
        "schema": (
            "customer_id string, first_name string, last_name string, "
            "email string, signup_date date, country string, "
            "marketing_opt_in boolean, op string, seq int, event_ts timestamp"
        ),
    },
    {
        "table_name": "products_raw",
        "key_column": "product_id",
        "file_type": "csv",
        "is_cdc": False,
        "volume_path": "/Volumes/urban_cart/bronze/landing_zone/dimensions/products.csv",
        "schema": (
            "product_id string, product_name string, category_raw string, "
            "unit_price double, cost_price double, active boolean"
        ),
    },
    {
        "table_name": "warehouses_raw",
        "key_column": "warehouse_id",
        "file_type": "csv",
        "is_cdc": False,
        "volume_path": "/Volumes/urban_cart/bronze/landing_zone/dimensions/warehouses.csv",
        "schema": "warehouse_id string, warehouse_name string, region string",
    },
    {
        "table_name": "promotions_raw",
        "key_column": "promo_code",
        "file_type": "csv",
        "is_cdc": False,
        "volume_path": "/Volumes/urban_cart/bronze/landing_zone/dimensions/promotions.csv",
        "schema": (
            "promo_code string, description string, discount_pct int, "
            "valid_from date, valid_to date"
        ),
    },
]

# COMMAND ----------

def ingest_dimension(table_name: str, schema: str, file_type: str, volume_path: str) -> int:
    """Batch-load one dimension file or batch directory into its bronze table."""
    full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    headers_opt = ", header => true" if file_type.lower() == "csv" else ""

    spark.sql(f"""
        CREATE OR REPLACE TABLE {full_table_name} AS
        SELECT
            *,
            current_timestamp() AS _ingested_at,
            _metadata.file_path AS _source_file
        FROM read_files(
            '{volume_path}',
            format => '{file_type}',
            schema => '{schema}'
            {headers_opt}
        )
    """)

    row_count = spark.table(full_table_name).count()
    print(f"  {full_table_name}: {row_count} rows")
    return row_count

# COMMAND ----------

def validate_tables(table_name: str, schema_name: str, key_column: str) -> None:
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"

    dupes = (
        spark.table(full_table_name)
        .groupBy(key_column)
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
    )
    dupe_count = dupes.count()

    if dupe_count > 0:
        sample = [row[key_column] for row in dupes.limit(5).collect()]
        raise ValueError(
            f"Uniqueness check failed on {full_table_name}.{key_column}: "
            f"{dupe_count} duplicate key(s) found. Sample: {sample}"
        )
    print(f"  {full_table_name}.{key_column}: unique, 0 duplicate keys")

# COMMAND ----------

def check_nulls(table_name: str, schema_name: str, key_column: str) -> None:
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"

    nulls = spark.table(full_table_name).filter(F.col(key_column).isNull())
    nulls_count = nulls.count()

    if nulls_count > 0:
        try:
            display(nulls.limit(5))
        except NameError:
            nulls.limit(5).show(truncate=False)
            
        raise ValueError(
            f"Null check failed on {full_table_name}.{key_column}: "
            f"{nulls_count} null key(s) found."
        )
    print(f"  {full_table_name}.{key_column}: 0 nulls")

# COMMAND ----------

# Run: ensure the schema exists, then load every dimension
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Ingesting dimensions into {CATALOG}.{SCHEMA} ...")
total = 0
for dim in DIMENSIONS:
    total += ingest_dimension(
        table_name=dim["table_name"],
        schema=dim["schema"],
        file_type=dim["file_type"],
        volume_path=dim["volume_path"]
    )
print(f"Done. {total} total rows across {len(DIMENSIONS)} dimension tables.\n")

print("Validating tables...")
for dim in DIMENSIONS:
    # 1. Null checks apply to all tables (even CDC streams must have non-null keys)
    check_nulls(dim["table_name"], SCHEMA, dim["key_column"])
    
    # 2. Skip uniqueness check for CDC batch feeds
    if dim.get("is_cdc", False):
        print(f"  {dim['table_name']}.{dim['key_column']}: skipped uniqueness check (CDC multi-event batch)")
        continue

    validate_tables(dim["table_name"], SCHEMA, dim["key_column"])

print(f"Done validating {len(DIMENSIONS)} tables.")