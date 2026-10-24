"""
Bronze ingestion: dimension snapshots.
 
Dimensions (customers, products, warehouses, promotions) are loaded as full
batch snapshots on every run -- CREATE OR REPLACE, not append -- because the
landing zone holds the complete current file each time, not an incremental
diff. This is deliberately different from the orders/order_items/returns
ingestion, which is a true incremental Autoloader stream (see
ingest_orders.py and friends).
 
Run locally via Databricks Connect for testing, or as a Databricks Workflow
task in production -- the `try/except` below makes both work unmodified.
"""
 
try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("urban_cart").serverless(True).getOrCreate()
 
# ---------------------------------------------------------------------------
# Config -- catalog is fixed, schema is overridable via a job/task parameter
# so the same script lands in your personal dev schema vs. the real bronze
# schema depending on which bundle target deployed it.
# ---------------------------------------------------------------------------
 
try:
    dbutils.widgets.text("catalog", "urban_cart")
    dbutils.widgets.text("schema", "bronze")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
except NameError:
    # dbutils isn't available when running plain Python locally via Connect --
    # fall back to hardcoded defaults for local testing.
    CATALOG = "urban_cart"
    SCHEMA = "bronze"
 
VOLUME_BASE = "/Volumes/urban_cart/bronze/landing_volume/dimensions"
 
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

"""
    row_count = spark.table(full_table_name).count()
    print(f"  {full_table_name}: {row_count} rows")
    return row_count
"""
 
 
if __name__ == "__main__":
    print(f"Ingesting dimensions into {CATALOG}.{SCHEMA} ...")
    total = 0
    for dim in DIMENSIONS:
        total += ingest_dimension(dim["table_name"], dim["file_name"], dim["schema"])
    print(f"Done. {total} total rows across {len(DIMENSIONS)} dimension tables.")
 