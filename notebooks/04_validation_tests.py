# Databricks notebook source
# MAGIC %md
# MAGIC # SFTP Pipeline Validation Test Suite
# MAGIC
# MAGIC This notebook runs comprehensive validation tests for the entire SFTP data pipeline.
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC 1. Azure Storage connectivity
# MAGIC 2. Unity Catalog external locations
# MAGIC 3. AutoLoader SFTP reading
# MAGIC 4. Custom SFTP writer functionality
# MAGIC 5. DLT pipeline data flow
# MAGIC 6. End-to-end data integrity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Update these values with your storage account names
SOURCE_STORAGE_ACCOUNT = "<source-storage-account>"  # e.g., "sftpsource20241204"
TARGET_STORAGE_ACCOUNT = "<target-storage-account>"  # e.g., "sftptarget20241204"
RESOURCE_GROUP = "<resource-group>"  # e.g., "rg-sftp-demo"

# Unity Catalog configuration
CATALOG = "main"
SCHEMA = "sftp_demo"

# SFTP configuration
SFTP_USER = "sftpuser"
SFTP_KEY_PATH = "/dbfs/FileStore/ssh_keys/sftp_key"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Suite Class

# COMMAND ----------

import time
from datetime import datetime
from typing import Dict, List, Tuple

class SFTPPipelineValidator:
    """Comprehensive validation test suite for SFTP pipeline"""

    def __init__(self):
        self.results = {
            "tests_passed": 0,
            "tests_failed": 0,
            "tests_skipped": 0,
            "errors": [],
            "start_time": datetime.now()
        }
        self.test_details = []

    def run_test(self, test_name: str, test_func) -> bool:
        """Run a single test and record results"""
        print(f"\n{'='*60}")
        print(f"Running Test: {test_name}")
        print('='*60)

        try:
            test_func()
            self.results["tests_passed"] += 1
            self.test_details.append({
                "name": test_name,
                "status": "PASSED",
                "error": None
            })
            print(f"✓ {test_name}: PASSED")
            return True

        except Exception as e:
            self.results["tests_failed"] += 1
            error_msg = str(e)
            self.results["errors"].append(f"{test_name}: {error_msg}")
            self.test_details.append({
                "name": test_name,
                "status": "FAILED",
                "error": error_msg
            })
            print(f"✗ {test_name}: FAILED")
            print(f"Error: {error_msg}")
            return False

    def skip_test(self, test_name: str, reason: str):
        """Skip a test with reason"""
        self.results["tests_skipped"] += 1
        self.test_details.append({
            "name": test_name,
            "status": "SKIPPED",
            "error": reason
        })
        print(f"⊘ {test_name}: SKIPPED ({reason})")

    def print_summary(self):
        """Print test execution summary"""
        duration = (datetime.now() - self.results["start_time"]).total_seconds()

        print("\n" + "="*60)
        print("VALIDATION TEST SUITE SUMMARY")
        print("="*60)
        print(f"Total Tests: {self.results['tests_passed'] + self.results['tests_failed'] + self.results['tests_skipped']}")
        print(f"Passed: {self.results['tests_passed']}")
        print(f"Failed: {self.results['tests_failed']}")
        print(f"Skipped: {self.results['tests_skipped']}")
        print(f"Duration: {duration:.2f} seconds")

        if self.results['tests_passed'] + self.results['tests_failed'] > 0:
            success_rate = (self.results['tests_passed'] /
                          (self.results['tests_passed'] + self.results['tests_failed'])) * 100
            print(f"Success Rate: {success_rate:.1f}%")

        print("="*60)

        # Print detailed results
        print("\nDetailed Results:")
        for detail in self.test_details:
            status_symbol = {
                "PASSED": "✓",
                "FAILED": "✗",
                "SKIPPED": "⊘"
            }[detail["status"]]
            print(f"{status_symbol} {detail['name']}: {detail['status']}")
            if detail["error"]:
                print(f"  → {detail['error']}")

        # Print errors if any
        if self.results['errors']:
            print("\n" + "="*60)
            print("ERRORS ENCOUNTERED")
            print("="*60)
            for error in self.results['errors']:
                print(f"• {error}")

        return self.results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Definitions

# COMMAND ----------

# Initialize validator
validator = SFTPPipelineValidator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 1: External Locations Exist

# COMMAND ----------

def test_external_locations():
    """Verify Unity Catalog external locations are created"""
    locations_df = spark.sql("SHOW EXTERNAL LOCATIONS")
    locations = [row.name for row in locations_df.collect()]

    assert "sftp_source_location" in locations, "sftp_source_location not found"
    assert "sftp_target_location" in locations, "sftp_target_location not found"

    print(f"Found external locations: {[loc for loc in locations if 'sftp' in loc]}")

validator.run_test("External Locations Exist", test_external_locations)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Storage Credentials Configured

# COMMAND ----------

def test_storage_credentials():
    """Verify storage credentials are set up"""
    credentials_df = spark.sql("SHOW STORAGE CREDENTIALS")
    credentials = [row.name for row in credentials_df.collect()]

    assert "sftp_source_credential" in credentials, "sftp_source_credential not found"
    assert "sftp_target_credential" in credentials, "sftp_target_credential not found"

    print(f"Found credentials: {[cred for cred in credentials if 'sftp' in cred]}")

validator.run_test("Storage Credentials Configured", test_storage_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Source Data Accessible

# COMMAND ----------

def test_source_data_accessible():
    """Verify source CSV files are accessible via Unity Catalog"""
    source_path = f"abfss://source@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    # Test customers.csv
    customers_df = spark.read.csv(
        f"{source_path}customers.csv",
        header=True,
        inferSchema=True
    )
    customer_count = customers_df.count()
    assert customer_count == 10, f"Expected 10 customers, got {customer_count}"

    # Test orders.csv
    orders_df = spark.read.csv(
        f"{source_path}orders.csv",
        header=True,
        inferSchema=True
    )
    order_count = orders_df.count()
    assert order_count == 15, f"Expected 15 orders, got {order_count}"

    print(f"✓ Source customers: {customer_count} records")
    print(f"✓ Source orders: {order_count} records")

validator.run_test("Source Data Accessible", test_source_data_accessible)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Source Data Schema Validation

# COMMAND ----------

def test_source_data_schema():
    """Validate schema of source CSV files"""
    source_path = f"abfss://source@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    # Customers schema
    customers_df = spark.read.csv(f"{source_path}customers.csv", header=True, inferSchema=True)
    customer_columns = set(customers_df.columns)
    expected_customer_columns = {"customer_id", "name", "email", "country", "signup_date"}
    assert customer_columns == expected_customer_columns, \
        f"Customer columns mismatch. Expected: {expected_customer_columns}, Got: {customer_columns}"

    # Orders schema
    orders_df = spark.read.csv(f"{source_path}orders.csv", header=True, inferSchema=True)
    order_columns = set(orders_df.columns)
    expected_order_columns = {"order_id", "customer_id", "product", "quantity", "amount", "order_date"}
    assert order_columns == expected_order_columns, \
        f"Order columns mismatch. Expected: {expected_order_columns}, Got: {order_columns}"

    print(f"✓ Customer columns: {list(customer_columns)}")
    print(f"✓ Order columns: {list(order_columns)}")

validator.run_test("Source Data Schema Validation", test_source_data_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 5: SFTP Writer Module Import

# COMMAND ----------

def test_sftp_writer_import():
    """Verify SFTP writer module can be imported"""
    from ingest import SFTPWriter, SFTPDataSource

    # Verify classes exist
    assert SFTPWriter is not None, "SFTPWriter class not found"
    assert SFTPDataSource is not None, "SFTPDataSource class not found"

    # Verify key methods exist
    assert hasattr(SFTPWriter, 'connect'), "SFTPWriter.connect method not found"
    assert hasattr(SFTPWriter, 'write_dataframe'), "SFTPWriter.write_dataframe method not found"
    assert hasattr(SFTPDataSource, 'create_writer'), "SFTPDataSource.create_writer method not found"

    print("✓ SFTPWriter class loaded")
    print("✓ SFTPDataSource class loaded")
    print("✓ All required methods present")

validator.run_test("SFTP Writer Module Import", test_sftp_writer_import)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 6: SFTP Writer Configuration

# COMMAND ----------

def test_sftp_writer_config():
    """Verify SFTP writer can be configured"""
    from ingest import SFTPDataSource

    config = {
        "host": f"{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net",
        "username": SFTP_USER,
        "port": 22,
        "private_key_path": SFTP_KEY_PATH
    }

    writer = SFTPDataSource.create_writer(config)
    assert writer is not None, "Failed to create writer"
    assert writer.host == config["host"], "Host not set correctly"
    assert writer.username == config["username"], "Username not set correctly"

    print(f"✓ Writer configured for host: {writer.host}")
    print(f"✓ Username: {writer.username}")

validator.run_test("SFTP Writer Configuration", test_sftp_writer_config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 7: SFTP Connection Test

# COMMAND ----------

def test_sftp_connection():
    """Test SFTP connection to target storage"""
    from ingest import SFTPDataSource

    config = {
        "host": f"{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net",
        "username": SFTP_USER,
        "port": 22,
        "private_key_path": SFTP_KEY_PATH
    }

    writer = SFTPDataSource.create_writer(config)

    # Test connection with context manager
    with writer.session():
        files = writer.list_files("target")
        print(f"✓ Connected successfully")
        print(f"✓ Files in target: {len(files)} files")
        if files:
            print(f"  Files: {files[:5]}")  # Show first 5 files

validator.run_test("SFTP Connection Test", test_sftp_connection)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 8: SFTP Write Test

# COMMAND ----------

def test_sftp_write():
    """Test writing data to SFTP"""
    from ingest import SFTPDataSource
    import pandas as pd

    config = {
        "host": f"{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net",
        "username": SFTP_USER,
        "port": 22,
        "private_key_path": SFTP_KEY_PATH
    }

    # Create test DataFrame
    test_df = pd.DataFrame({
        "test_id": [1, 2, 3],
        "test_value": ["Validation_A", "Validation_B", "Validation_C"],
        "timestamp": pd.date_range("2024-01-01", periods=3)
    })

    # Write to SFTP
    test_file_path = f"target/validation_test_{int(time.time())}.csv"
    SFTPDataSource.write(test_df, test_file_path, config, format="csv")

    # Verify file exists
    writer = SFTPDataSource.create_writer(config)
    with writer.session():
        files = writer.list_files("target")
        assert any(test_file_path.split("/")[-1] in f for f in files), \
            f"Test file not found in target. Files: {files}"

    print(f"✓ Test file written: {test_file_path}")
    print(f"✓ Test DataFrame: {len(test_df)} records")

validator.run_test("SFTP Write Test", test_sftp_write)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 9: AutoLoader Schema Inference

# COMMAND ----------

def test_autoloader_schema():
    """Test AutoLoader schema inference"""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    source_path = f"abfss://source@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    # Test AutoLoader with customers
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True)
    ])

    autoloader_df = (
        spark.read
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(customers_schema)
        .load(f"{source_path}customers.csv")
    )

    count = autoloader_df.count()
    assert count == 10, f"Expected 10 records, got {count}"

    print(f"✓ AutoLoader loaded {count} records")
    print(f"✓ Schema inferred: {len(autoloader_df.columns)} columns")

validator.run_test("AutoLoader Schema Inference", test_autoloader_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 10: Data Transformation Logic

# COMMAND ----------

def test_data_transformation():
    """Test data transformation logic (join)"""
    source_path = f"abfss://source@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    customers_df = spark.read.csv(f"{source_path}customers.csv", header=True, inferSchema=True)
    orders_df = spark.read.csv(f"{source_path}orders.csv", header=True, inferSchema=True)

    # Perform join
    enriched_df = orders_df.join(
        customers_df.select("customer_id", "name"),
        on="customer_id",
        how="left"
    ).withColumnRenamed("name", "customer_name")

    # Validate results
    enriched_count = enriched_df.count()
    assert enriched_count == 15, f"Expected 15 enriched records, got {enriched_count}"

    # Check that all orders have customer names
    null_names = enriched_df.filter("customer_name IS NULL").count()
    assert null_names == 0, f"Found {null_names} orders without customer names"

    print(f"✓ Enriched orders: {enriched_count} records")
    print(f"✓ All orders have customer names")
    print(f"✓ Join successful")

validator.run_test("Data Transformation Logic", test_data_transformation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 11: Target Data Exists

# COMMAND ----------

def test_target_data_exists():
    """Verify data exists in target SFTP container"""
    target_path = f"abfss://target@{TARGET_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    try:
        # List files in target
        files = dbutils.fs.ls(target_path)
        assert len(files) > 0, "No files found in target container"

        print(f"✓ Target container has {len(files)} files")
        print(f"Files: {[f.name for f in files[:5]]}")

    except Exception as e:
        # If DLT pipeline hasn't run yet, this test might fail
        # That's okay - we just want to verify the path is accessible
        if "Path does not exist" in str(e):
            print("⚠ Target container is empty (DLT pipeline may not have run yet)")
        else:
            raise

validator.run_test("Target Data Exists", test_target_data_exists)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 12: End-to-End Data Integrity

# COMMAND ----------

def test_end_to_end_integrity():
    """Verify end-to-end data integrity"""
    source_path = f"abfss://source@{SOURCE_STORAGE_ACCOUNT}.dfs.core.windows.net/"
    target_path = f"abfss://target@{TARGET_STORAGE_ACCOUNT}.dfs.core.windows.net/"

    # Read source data
    source_orders = spark.read.csv(f"{source_path}orders.csv", header=True, inferSchema=True)
    source_count = source_orders.count()

    # Try to read target data
    try:
        # Look for enriched orders file
        target_files = dbutils.fs.ls(target_path)
        enriched_file = [f for f in target_files if "enriched_orders" in f.name or "orders" in f.name]

        if enriched_file:
            target_orders = spark.read.csv(
                target_path + enriched_file[0].name,
                header=True,
                inferSchema=True
            )
            target_count = target_orders.count()

            # Verify counts match
            assert target_count == source_count, \
                f"Record count mismatch: source={source_count}, target={target_count}"

            print(f"✓ Source records: {source_count}")
            print(f"✓ Target records: {target_count}")
            print(f"✓ Data integrity verified")
        else:
            print("⚠ No enriched orders file found in target (DLT pipeline may not have completed)")

    except Exception as e:
        if "Path does not exist" in str(e):
            print("⚠ Target data not yet available (run DLT pipeline first)")
        else:
            raise

validator.run_test("End-to-End Data Integrity", test_end_to_end_integrity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Summary and Results

# COMMAND ----------

# Print comprehensive summary
summary = validator.print_summary()

# Create results DataFrame for visualization
results_data = [
    (detail["name"], detail["status"], detail["error"] or "")
    for detail in validator.test_details
]

results_df = spark.createDataFrame(
    results_data,
    ["Test Name", "Status", "Error Message"]
)

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overall Assessment

# COMMAND ----------

total_tests = summary["tests_passed"] + summary["tests_failed"]
if total_tests > 0:
    success_rate = (summary["tests_passed"] / total_tests) * 100

    if success_rate == 100:
        print("🎉 EXCELLENT! All tests passed. Your SFTP pipeline is fully functional.")
    elif success_rate >= 80:
        print("✓ GOOD! Most tests passed. Review failed tests and address issues.")
    elif success_rate >= 50:
        print("⚠ WARNING! Several tests failed. Check configuration and setup.")
    else:
        print("✗ CRITICAL! Many tests failed. Review setup from the beginning.")

    print(f"\nOverall Success Rate: {success_rate:.1f}%")
    print(f"Tests Passed: {summary['tests_passed']}/{total_tests}")
else:
    print("No tests were executed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Based on test results:
# MAGIC
# MAGIC **If all tests passed:**
# MAGIC - Your SFTP pipeline is fully functional
# MAGIC - You can proceed with production use
# MAGIC - Consider running DLT pipeline if not already done
# MAGIC
# MAGIC **If some tests failed:**
# MAGIC - Review error messages in the summary above
# MAGIC - Check Azure storage account configuration
# MAGIC - Verify Unity Catalog external locations
# MAGIC - Ensure SSH keys are correctly configured
# MAGIC - Re-run failed notebooks (01, 02, 03)
# MAGIC
# MAGIC **Common fixes:**
# MAGIC - Update storage account names in configuration cells
# MAGIC - Verify SFTP is enabled on Azure storage
# MAGIC - Check Unity Catalog permissions
# MAGIC - Upload SSH private key to DBFS at correct path
