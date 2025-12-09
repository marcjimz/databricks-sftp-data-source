# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Delta Live Tables Pipeline with SFTP
# MAGIC
# MAGIC This DLT pipeline implements:
# MAGIC 1. Reads data from source SFTP using AutoLoader
# MAGIC 2. Processes and transforms data through bronze → silver → gold layers
# MAGIC 3. Writes processed data back to target SFTP using custom SFTP data source
# MAGIC
# MAGIC **Note:** This should be run as a DLT pipeline in Databricks.

# COMMAND ----------

import sys
import os
import tempfile
import dlt
from pyspark.sql import functions as F, SparkSession

# Get spark session
spark = SparkSession.builder.getOrCreate()

# Add src folder to Python path for DLT
# For DLT, we need to calculate the path relative to the pipeline file location
pipeline_file_path = os.path.abspath(__file__)
notebooks_dir = os.path.dirname(pipeline_file_path)
repo_root = os.path.dirname(notebooks_dir)
src_path = os.path.join(repo_root, 'src')

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# Import custom SFTP package
from ingest import SFTPDataSource

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Configuration is loaded from DLT pipeline parameters and Unity Catalog.

# COMMAND ----------

# Get configuration from DLT pipeline parameters
CATALOG_NAME = spark.conf.get("catalog_name", "sftp_demo")
SCHEMA_NAME = spark.conf.get("schema_name", "default")
SOURCE_CONNECTION_NAME = spark.conf.get("source_connection_name", "source_sftp_connection")
TARGET_CONNECTION_NAME = spark.conf.get("target_connection_name", "target_sftp_connection")

# Load configuration from config table
config_df = spark.table(f"{CATALOG_NAME}.config.connection_params")
config_dict = {row.key: row.value for row in config_df.collect()}

# Source SFTP configuration
SOURCE_HOST = config_dict["source_host"]
SOURCE_USERNAME = config_dict["source_username"]

# Target SFTP configuration
TARGET_HOST = config_dict["target_host"]
TARGET_USERNAME = config_dict["target_username"]
SECRET_SCOPE = config_dict["secret_scope"]
SSH_KEY_SECRET = config_dict["ssh_key_secret"]

print(f"DLT Pipeline Configuration:")
print(f"  Catalog: {CATALOG_NAME}")
print(f"  Schema: {SCHEMA_NAME}")
print(f"  Source: {SOURCE_USERNAME}@{SOURCE_HOST}")
print(f"  Target: {TARGET_USERNAME}@{TARGET_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Use AutoLoader to read CSV files from source SFTP.

# COMMAND ----------

@dlt.table(
    name="bronze_customers",
    comment="Raw customer data from source SFTP",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    """Ingest raw customer data from SFTP using AutoLoader"""
    source_uri = f"sftp://{SOURCE_USERNAME}@{SOURCE_HOST}:22/customers.csv"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/tmp/{CATALOG_NAME}/dlt/schema/customers")
        .option("header", "true")
        .load(source_uri)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

# COMMAND ----------

@dlt.table(
    name="bronze_orders",
    comment="Raw order data from source SFTP",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    """Ingest raw order data from SFTP using AutoLoader"""
    source_uri = f"sftp://{SOURCE_USERNAME}@{SOURCE_HOST}:22/orders.csv"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"/tmp/{CATALOG_NAME}/dlt/schema/orders")
        .option("header", "true")
        .load(source_uri)
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Cleaned and Validated Data
# MAGIC
# MAGIC Apply data quality rules and transformations.

# COMMAND ----------

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("valid_signup_date", "signup_date IS NOT NULL")
def silver_customers():
    """Clean and validate customer data"""
    return (
        dlt.read_stream("bronze_customers")
        .select(
            "customer_id",
            F.trim(F.col("name")).alias("name"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.upper(F.trim(F.col("country"))).alias("country"),
            F.to_date(F.col("signup_date")).alias("signup_date"),
            F.current_timestamp().alias("processed_timestamp")
        )
        .dropDuplicates(["customer_id"])
    )

# COMMAND ----------

@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated order data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_drop("valid_quantity", "quantity > 0")
def silver_orders():
    """Clean and validate order data"""
    return (
        dlt.read_stream("bronze_orders")
        .select(
            "order_id",
            "customer_id",
            F.trim(F.col("product")).alias("product"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("amount").cast("decimal(10,2)").alias("amount"),
            F.to_date(F.col("order_date")).alias("order_date"),
            F.current_timestamp().alias("processed_timestamp")
        )
        .dropDuplicates(["order_id"])
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Business-Level Aggregations
# MAGIC
# MAGIC Create enriched datasets for analytics.

# COMMAND ----------

@dlt.table(
    name="gold_customer_orders",
    comment="Enriched customer order data with aggregations",
    table_properties={"quality": "gold"}
)
def gold_customer_orders():
    """Create enriched customer order dataset"""
    customers = dlt.read_stream("silver_customers")
    orders = dlt.read_stream("silver_orders")

    return (
        orders
        .join(customers, "customer_id", "left")
        .select(
            "order_id",
            "customer_id",
            "name",
            "email",
            "country",
            "product",
            "quantity",
            "amount",
            "order_date",
            F.current_timestamp().alias("processed_timestamp")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_customer_summary",
    comment="Customer summary metrics",
    table_properties={"quality": "gold"}
)
def gold_customer_summary():
    """Calculate customer summary metrics"""
    return (
        dlt.read_stream("gold_customer_orders")
        .groupBy("customer_id", "name", "email", "country")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_order_amount"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date")
        )
        .withColumn("processed_timestamp", F.current_timestamp())
    )
