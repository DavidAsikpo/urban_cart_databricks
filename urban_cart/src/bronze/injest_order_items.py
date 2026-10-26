# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: order_items (streaming)
# MAGIC
# MAGIC Same Autoloader pattern as orders: incremental, checkpointed, append-only,
# MAGIC no cleaning or deduplication at this layer.
# MAGIC
# MAGIC `unit_price` is read as **string**, not double. For a 3-week window
# MAGIC (2025-02-01 to 2025-02-21, warehouse WH02 only), the source emits prices
# MAGIC as `"$1,234.56"`-formatted strings instead of plain numbers. Declaring the
# MAGIC bronze schema as `double` would silently null out every row in that
# MAGIC window during ingestion — a real production incident waiting to happen.
# MAGIC Bronze's job is to store what arrived, untouched; the `$`-stripping and
# MAGIC cast to a real number happens in silver, not here.
# MAGIC
# MAGIC Because order events are at-least-once (the same weekly order batch can be
# MAGIC re-emitted), their line items inherit the same duplication: a repeated
# MAGIC `order_id` brings repeated `order_item_id`s with it. Bronze stores those
# MAGIC duplicates as-is; dedup happens in silver, not here.
# MAGIC
# MAGIC Runs as a Databricks Workflow task, or locally via Databricks Connect.
 
# COMMAND ----------
 
# Spark session: injected in notebooks, built explicitly under Databricks Connect.
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
 
    spark = (
        DatabricksSession.builder
        .profile("urban_cart")
        .serverless(True)
        .getOrCreate()
    )
 
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
 
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
 
LANDING_PATH = "/Volumes/urban_cart/bronze/landing_zone/order_items"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/checkpoint/order_items"
SCHEMA_PATH = F"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/schema/order_items"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.order_items_raw"
KEY_COLUMN = "order_item_id"
 
# unit_price stays a string here on purpose -- see module docstring.
ORDER_ITEMS_SCHEMA = (
    "order_item_id string, order_id string, product_id string, "
    "quantity int, unit_price string"
)
 
# COMMAND ----------
 
 
def ingest_order_items() -> int:
    """Stream new order-item batches from the landing zone into order_items_raw.
 
    Append-only: no filtering, deduplication, or price normalization happens
    here. Returns the table's total row count after ingestion.
    """
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema(ORDER_ITEMS_SCHEMA)
        .load(LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
 
    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )
    query.awaitTermination()
 
    row_count = spark.table(TARGET_TABLE).count()
    print(f"Ingestion complete. {TARGET_TABLE}: {row_count} total rows")
    return row_count
 
 
# COMMAND ----------
 
 
def check_duplicates(
    table_name: str,
    schema_name: str,
    key_column: str,
    *,
    fail_on_violation: bool = False,
) -> int:
    """Report duplicate key_column values in table_name.
 
    By default this only warns (fail_on_violation=False): order_items_raw is
    expected to carry the same planted at-least-once duplicates as orders_raw,
    resolved in silver. Pass fail_on_violation=True for tables where
    duplicates indicate a real data integrity problem.
 
    Returns the number of distinct duplicated keys found.
    """
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"
 
    dupes: DataFrame = (
        spark.table(full_table_name)
        .groupBy(key_column)
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
    )
    dupe_count = dupes.count()
 
    if dupe_count == 0:
        print(f"  {full_table_name}.{key_column}: unique, 0 violations")
        return 0
 
    sample = [row[key_column] for row in dupes.limit(5).collect()]
 
    if fail_on_violation:
        raise ValueError(
            f"Uniqueness check failed on {full_table_name}.{key_column}: "
            f"{dupe_count} duplicate key(s) found. Sample: {sample}"
        )
 
    print(
        f"  WARNING: {full_table_name}.{key_column} has {dupe_count} "
        f"duplicate key(s) — expected at bronze, resolved in silver dedup. "
        f"Sample: {sample}"
    )
    return dupe_count
 
 
# COMMAND ----------
 
 
def check_nulls(table_name: str, schema_name: str, key_column: str) -> int:
    """Fail loudly if key_column has any null values in table_name.
 
    A null order_item_id is never expected at any layer -- this always
    raises on a violation.
    """
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"
 
    nulls = spark.table(full_table_name).filter(F.col(key_column).isNull())
    null_count = nulls.count()
 
    if null_count > 0:
        display(nulls.limit(5))
        raise ValueError(
            f"Not-null check failed on {full_table_name}.{key_column}: "
            f"{null_count} null key(s) found. See sample above."
        )
 
    print(f"  {full_table_name}.{key_column}: not null, 0 violations")
    return null_count
 
 
# COMMAND ----------
 
if __name__ == "__main__":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
 
    print(f"Target: {TARGET_TABLE}")
    print(f"Reading from: {LANDING_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
 
    ingest_order_items()
 
    print(f"Validating {TARGET_TABLE} ...")
    check_nulls("order_items_raw", SCHEMA, KEY_COLUMN)
    check_duplicates("order_items_raw", SCHEMA, KEY_COLUMN, fail_on_violation=False)
 