# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: orders (streaming)
# MAGIC
# MAGIC Orders are ingested as a true **incremental stream** via Autoloader
# MAGIC (`cloudFiles`), unlike the dimension snapshots, which are full batch
# MAGIC overwrites. The landing zone holds one weekly batch file per drop, not a
# MAGIC complete history each time, so this reads only files it hasn't already
# MAGIC processed, tracked via a checkpoint.
# MAGIC
# MAGIC Bronze stores exactly what arrived, duplicates and all — including the
# MAGIC ~1.5% of order events that are deliberately re-emitted a week after their
# MAGIC real batch (a planted at-least-once-delivery problem). Deduplication on
# MAGIC `order_id` happens downstream, in silver, not here. This table is
# MAGIC append-only and is never filtered, cleaned, or deduplicated at this layer.
# MAGIC Duplicate `order_id`s are therefore expected here and only logged as a
# MAGIC warning, not treated as a failure.
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

LANDING_PATH = "/Volumes/urban_cart/bronze/landing_zone/orders"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/checkpoint/orders"
SCHEMA_PATH = F"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/schema/orders"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.orders_raw"
KEY_COLUMN = "order_id"

ORDER_SCHEMA = (
    "order_id string, customer_id string, order_ts string, "
    "warehouse_id string, promo_code string, order_status string, "
    "is_test boolean"
)

# COMMAND ----------


def ingest_orders() -> int:
    """Stream new order-event batches from the landing zone into orders_raw.

    Append-only: no filtering, deduplication, or type coercion happens here.
    Returns the table's total row count after ingestion.
    """
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema(ORDER_SCHEMA)
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

    By default this only warns (fail_on_violation=False), since orders_raw
    is expected to contain planted duplicates that get resolved in silver.
    Pass fail_on_violation=True for tables where duplicates indicate a real
    data integrity problem (e.g. dimension tables).

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

    Unlike duplicates, a null order_id is never expected at any layer —
    this always raises on a violation.
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

    ingest_orders()

    print(f"Validating {TARGET_TABLE} ...")
    check_nulls("orders_raw", SCHEMA, KEY_COLUMN)
    check_duplicates("orders_raw", SCHEMA, KEY_COLUMN, fail_on_violation=False)