try:
    spark
except NameError:
    from databricks.connect import DatabricksSession
    spark = DatabricksSession.builder.profile("urban_cart").serverless(True).getOrCreate()

customer_schema = "customer_id string, first_name string, last_name string, email string, signup_date date, country string, marketing_opt_in boolean"
spark.sql(f"""CREATE OR REPLACE TABLE urban_cart.urban_cart_bronze.customer_raw AS SELECT *,
           current_timestamp() AS _ingestion_timestamp,
           _metadata.file_path AS _source_file FROM
           read_files('/Volumes/urban_cart/urban_cart_bronze/landing_zone/dimensions/customers.csv',
           format => 'csv',
           header => true,
           Schema => '{customer_schema}')""")


