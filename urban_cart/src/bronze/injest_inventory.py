# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion: inventory snapshots (batch, checkpointed)
# MAGIC
# MAGIC One file lands per month (`inventory_<month>.csv`), each a complete stock
# MAGIC count per warehouse x product *for that month only* -- unlike the
# MAGIC dimension snapshots, a new file here is not a full replacement of history,
# MAGIC it's one more period to append. So this is a batch read (not a true
# MAGIC streaming source), but it still goes through Autoloader (`cloudFiles`) so
# MAGIC that re-running the job doesn't re-append a month that's already landed.
# MAGIC
# MAGIC The natural key is the composite (`snapshot_month`, `warehouse_id`,
# MAGIC `product_id`), not a single id column.
# MAGIC
# MAGIC `recorded_ending_stock` is known to drift from fulfillment-implied stock
# MAGIC for one warehouse (the planted reconciliation mismatch) -- that's a
# MAGIC silver-layer reconciliation check, not something bronze filters or fixes.
# MAGIC Bronze stores exactly what was recorded, append-only.
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
 
LANDING_PATH = "/Volumes/urban_cart/bronze/landing_zone/inventory"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/checkpoint/inventory"
SCHEMA_PATH = F"/Volumes/{CATALOG}/{SCHEMA}/landing_zone/metadata/schema/inventory"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.inventory_snapshots_raw"
KEY_COLUMNS = ["snapshot_month", "warehouse_id", "product_id"]
 
INVENTORY_SCHEMA = (
    "snapshot_month string, warehouse_id string, product_id string, "
    "recorded_ending_stock int"
)
 
# COMMAND ----------
 
 
def ingest_inventory() -> int:
    """Batch-load new monthly inventory snapshot files into inventory_snapshots_raw.
 
    Uses Autoloader in trigger(availableNow=True) mode rather than a plain
    batch read: each run picks up only months that haven't landed yet (via
    the checkpoint), so re-running the job is safe and doesn't duplicate a
    month's snapshot. Append-only: no reconciliation or correction happens
    here. Returns the table's total row count after ingestion.
    """
    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema(INVENTORY_SCHEMA)
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
    key_columns: list[str],
    *,
    fail_on_violation: bool = True,
) -> int:
    """Report duplicate (key_columns) combinations in table_name.
 
    Each (snapshot_month, warehouse_id, product_id) combination is expected
    to appear exactly once -- unlike orders/order_items, no at-least-once
    duplication is planted here, so this fails loudly by default.
 
    Returns the number of distinct duplicated key combinations found.
    """
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"
    key_label = ", ".join(key_columns)
 
    dupes: DataFrame = (
        spark.table(full_table_name)
        .groupBy(*key_columns)
        .agg(F.count("*").alias("n"))
        .filter(F.col("n") > 1)
    )
    dupe_count = dupes.count()
 
    if dupe_count == 0:
        print(f"  {full_table_name}.({key_label}): unique, 0 violations")
        return 0
 
    sample = [row.asDict() for row in dupes.limit(5).collect()]
 
    if fail_on_violation:
        raise ValueError(
            f"Uniqueness check failed on {full_table_name}.({key_label}): "
            f"{dupe_count} duplicate key(s) found. Sample: {sample}"
        )
 
    print(
        f"  WARNING: {full_table_name}.({key_label}) has {dupe_count} "
        f"duplicate key(s). Sample: {sample}"
    )
    return dupe_count
 
 
# COMMAND ----------
 
 
def check_nulls(table_name: str, schema_name: str, key_columns: list[str]) -> int:
    """Fail loudly if any key_columns have null values in table_name.
 
    A null snapshot_month, warehouse_id, or product_id is never expected --
    this always raises on a violation.
    """
    full_table_name = f"{CATALOG}.{schema_name}.{table_name}"
 
    null_condition = F.lit(False)
    for col in key_columns:
        null_condition = null_condition | F.col(col).isNull()
 
    nulls = spark.table(full_table_name).filter(null_condition)
    null_count = nulls.count()
 
    if null_count > 0:
        display(nulls.limit(5))
        raise ValueError(
            f"Not-null check failed on {full_table_name}.({', '.join(key_columns)}): "
            f"{null_count} row(s) with a null key column found. See sample above."
        )
 
    print(f"  {full_table_name}.({', '.join(key_columns)}): not null, 0 violations")
    return null_count
 
 
# COMMAND ----------
 
if __name__ == "__main__":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
 
    print(f"Target: {TARGET_TABLE}")
    print(f"Reading from: {LANDING_PATH}")
    print(f"Checkpoint: {CHECKPOINT_PATH}")
 
    ingest_inventory()
 
    print(f"Validating {TARGET_TABLE} ...")
    check_nulls("inventory_snapshots_raw", SCHEMA, KEY_COLUMNS)
    check_duplicates("inventory_snapshots_raw", SCHEMA, KEY_COLUMNS, fail_on_violation=True)