# Databricks SFTP Data Source

A complete example demonstrating how to build a data pipeline that reads from SFTP using Databricks AutoLoader and writes back to SFTP using a custom data source built with Paramiko.

## Overview

This repository contains:

1. **Custom SFTP Data Source** - Python package using Paramiko SSHv2 for writing data to SFTP
2. **Sample Data** - Example CSV files (customers, orders)
3. **Infrastructure Notebooks** - Step-by-step setup for Azure Storage with SFTP enabled
4. **DLT Pipeline** - Complete Delta Live Tables pipeline with Bronze → Silver → Gold layers

## Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Source SFTP    │      │  Bronze Layer   │      │  Silver Layer   │      │   Gold Layer    │      │  Target SFTP    │
│ (Azure Storage) │ ───> │   (Raw Data)    │ ───> │ (Validated Data)│ ───> │(Business Metrics)│ ───> │ (Azure Storage) │
└─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
   AutoLoader             Cleaning &               Aggregation &            Custom SFTP
   Ingestion              Validation               Enrichment               Data Source
```

## Features

- **AutoLoader Integration**: Leverage Databricks AutoLoader to incrementally read from SFTP
- **Custom Data Source**: Write data back to SFTP using Paramiko SSHv2
- **Unity Catalog**: Manage SFTP connections through Unity Catalog
- **Delta Live Tables**: Full DLT pipeline with data quality expectations
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Azure Integration**: Complete setup using Azure Storage with SFTP enabled

## Prerequisites

- Azure subscription with permissions to create storage accounts
- Databricks workspace with Unity Catalog enabled
- Azure CLI installed locally
- Python 3.9+

## Quick Start - End-to-End Setup

Follow these steps in order to set up and run the complete SFTP data pipeline:

### Step 1: Configure Azure Settings (Local Machine)

Before running any notebooks, configure your Azure environment locally:

```bash
# Copy the configuration template
cp config/azure_config.template.sh config/azure_config.sh

# Edit azure_config.sh with your Azure subscription and resource details
# Update: SUBSCRIPTION_ID, RESOURCE_GROUP, LOCATION, etc.

# Load the configuration
source config/azure_config.sh
```

### Step 2: Run Prerequisites Notebook

**Notebook**: `notebooks/00_prerequisites.ipynb`

This step sets up your Azure infrastructure. Run the Azure CLI commands in this notebook to:
- Generate SSH key pair (`~/.ssh/sftp_key`)
- Create two Azure Storage accounts with SFTP enabled (source and target)
- Configure SFTP users with SSH key authentication
- Upload sample CSV files (customers.csv, orders.csv) to source SFTP

**What you'll have after this step:**
- Two Azure Storage accounts with SFTP enabled
- SSH keys for authentication
- Sample data files in source SFTP

### Step 3: Install Package and Configure Secrets

**Notebook**: `notebooks/01_infrastructure_setup.ipynb`

Run this notebook in your Databricks workspace to:

1. **Install the SFTP package:**
   ```python
   %pip install paramiko
   %pip install -e /Workspace/Repos/<your-repo>/databricks-sftp-data-source
   ```

2. **Create Databricks secrets** (run these commands in your terminal or notebook):
   ```bash
   databricks secrets create-scope --scope sftp-credentials

   databricks secrets put --scope sftp-credentials --key source-host
   databricks secrets put --scope sftp-credentials --key source-username
   databricks secrets put --scope sftp-credentials --key source-private-key

   databricks secrets put --scope sftp-credentials --key target-host
   databricks secrets put --scope sftp-credentials --key target-username
   databricks secrets put --scope sftp-credentials --key target-private-key
   ```

3. **Upload SSH private key to DBFS:**
   ```bash
   databricks fs cp ~/.ssh/sftp_key dbfs:/FileStore/ssh-keys/sftp_key
   ```

4. **Test SFTP connections** using the notebook validation cells

**What you'll have after this step:**
- SFTP package installed in Databricks
- Credentials securely stored in Databricks secrets
- SSH keys available in DBFS
- Verified SFTP connectivity

### Step 4: Set Up Unity Catalog Connections

**Notebook**: `notebooks/02_uc_connection_setup.ipynb`

Run this notebook to configure Unity Catalog for SFTP access:

1. **Create catalog and schema:**
   ```sql
   CREATE CATALOG IF NOT EXISTS sftp_demo;
   CREATE SCHEMA IF NOT EXISTS sftp_demo.connections;
   ```

2. **Create Unity Catalog SFTP connections:**
   - Source SFTP connection for reading data
   - Target SFTP connection for writing data

3. **Grant permissions** to users/groups who need access

4. **Test AutoLoader** with SFTP to ensure data can be read

**What you'll have after this step:**
- Unity Catalog connections configured for source and target SFTP
- Permissions set up for pipeline execution
- Verified AutoLoader can read from SFTP

### Step 5: Create and Run DLT Pipeline

**Notebook**: `notebooks/03_dlt_pipeline.ipynb`

1. **Create the DLT Pipeline:**
   - In Databricks UI: Go to **Delta Live Tables** → **Create Pipeline**
   - **Pipeline Name**: `sftp_ingestion_pipeline`
   - **Notebook Path**: `/Workspace/Repos/<your-repo>/databricks-sftp-data-source/notebooks/03_dlt_pipeline.ipynb`
   - **Target**: `sftp_demo.processed_data`
   - **Configuration**: Use settings from `config/dlt_pipeline_config.json`

2. **Start the Pipeline:**
   - Click **Start** in the DLT UI
   - Monitor the pipeline execution

3. **Pipeline stages:**
   - **Bronze Layer**: Ingest raw CSV files from source SFTP via AutoLoader
   - **Silver Layer**: Clean and validate data (email validation, data type checks)
   - **Gold Layer**: Create aggregated business metrics (customer summaries, order analytics)
   - **Export**: Write Gold layer tables back to target SFTP using custom data source

**What you'll have after this step:**
- Complete medallion architecture (Bronze → Silver → Gold)
- Data quality expectations enforced
- Processed data exported to target SFTP
- Monitoring and lineage visualization in DLT UI

### Step 6: Verify Results

After the pipeline completes, verify the output:

```python
# Check files written to target SFTP
from ingest import SFTPWriter

config = {
    "host": dbutils.secrets.get("sftp-credentials", "target-host"),
    "username": dbutils.secrets.get("sftp-credentials", "target-username"),
    "private_key_path": "/dbfs/FileStore/ssh-keys/sftp_key",
    "port": 22
}

writer = SFTPWriter.create_writer(config)
with writer.session():
    files = writer.list_files("/data/output")
    print(f"Files in target SFTP: {files}")
```

**Expected output files:**
- `customer_summary.csv` - Aggregated customer metrics
- `order_analytics.csv` - Order statistics and trends

## Project Structure

```
databricks-sftp-data-source/
├── src/                       # Source code
│   └── ingest/               # Custom SFTP data source package
│       ├── __init__.py
│       └── SFTPWriter.py     # Paramiko-based SFTP writer
├── notebooks/                 # Databricks notebooks
│   ├── 00_prerequisites.ipynb
│   ├── 01_infrastructure_setup.ipynb
│   ├── 02_uc_connection_setup.ipynb
│   └── 03_dlt_pipeline.ipynb
├── data/                      # Sample CSV files
│   ├── customers.csv
│   └── orders.csv
├── config/                    # Configuration templates
│   ├── sftp_config.template.json
│   ├── azure_config.template.sh
│   └── dlt_pipeline_config.json
├── requirements.txt
├── setup.py
└── README.md
```

## API Usage

### Custom SFTP Data Source API

```python
from ingest import SFTPDataSource, SFTPWriter

# Configuration
config = {
    "host": "myaccount.blob.core.windows.net",
    "username": "myaccount.sftpuser",
    "private_key_path": "/dbfs/FileStore/ssh-keys/sftp_key",
    "port": 22
}

# Write DataFrame to SFTP
SFTPDataSource.write(
    df=my_dataframe,
    remote_path="/data/output.csv",
    config=config,
    format="csv",
    header=True
)

# Using context manager
writer = SFTPDataSource.create_writer(config)
with writer.session():
    writer.write_dataframe(df, "/data/output.csv", format="csv")
    files = writer.list_files(".")
```

### DLT Pipeline

The DLT pipeline implements a medallion architecture:

**Bronze Layer**: Raw data ingestion from source SFTP
```python
@dlt.table(name="bronze_customers")
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.connectionName", "sftp_demo.source_sftp_connection")
        .load("sftp://host/customers.csv")
    )
```

**Silver Layer**: Data cleaning and validation
```python
@dlt.table(name="silver_customers")
@dlt.expect_or_drop("valid_email", "email LIKE '%@%'")
def silver_customers():
    return dlt.read_stream("bronze_customers").select(...)
```

**Gold Layer**: Business metrics and aggregations
```python
@dlt.table(name="gold_customer_summary")
def gold_customer_summary():
    return (
        dlt.read_stream("silver_customers")
        .groupBy("customer_id")
        .agg(...)
    )
```

## Data Flow Example

### Input Data (Source SFTP)

**customers.csv**
```csv
customer_id,name,email,country,signup_date
1,John Smith,john.smith@example.com,USA,2024-01-15
2,Emma Johnson,emma.j@example.com,UK,2024-02-20
```

**orders.csv**
```csv
order_id,customer_id,product,quantity,amount,order_date
1001,1,Laptop,1,999.99,2024-01-20
1002,2,Mouse,2,25.50,2024-02-22
```

### Output Data (Target SFTP)

**customer_summary.csv**
```csv
customer_id,name,email,country,total_orders,total_amount,avg_order_amount
1,John Smith,john.smith@example.com,USA,2,1399.97,699.985
2,Emma Johnson,emma.j@example.com,UK,1,25.50,25.50
```

## API Reference

### SFTPWriter

Main class for SFTP operations.

**Methods:**
- `connect()` - Establish SFTP connection
- `disconnect()` - Close SFTP connection
- `session()` - Context manager for automatic connection handling
- `write_dataframe(df, remote_path, format, **kwargs)` - Write DataFrame to SFTP
- `write_file(local_path, remote_path)` - Upload file to SFTP
- `list_files(remote_dir)` - List files in remote directory

### SFTPDataSource

Factory class for creating SFTP writers.

**Methods:**
- `create_writer(config)` - Create SFTPWriter from configuration
- `write(df, remote_path, config, **kwargs)` - Convenience method to write DataFrame

## Testing

Run unit tests:

```bash
pytest tests/
```

Run with coverage:

```bash
pytest --cov=ingest tests/
```

## Monitoring

Monitor your DLT pipeline:
- **Data Quality**: View expectation metrics in DLT UI
- **Data Lineage**: Track data flow through bronze → silver → gold
- **Performance**: Check processing times and record counts
- **Errors**: Review failed records and validation issues

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to SFTP
```
Solution: Verify SSH key permissions and host configuration
- Check SSH key: ls -la ~/.ssh/sftp_key
- Test connection: sftp -i ~/.ssh/sftp_key user@host
```

### AutoLoader Issues

**Problem**: AutoLoader not detecting files
```
Solution: Verify Unity Catalog connection
- Check connection: SHOW CONNECTIONS IN sftp_demo
- Test manually: spark.read.format("csv").load("sftp://...")
```

### Permission Issues

**Problem**: Access denied to Unity Catalog connection
```
Solution: Grant appropriate permissions
GRANT USAGE ON CONNECTION sftp_demo.source_sftp_connection TO `user@example.com`
```

## Acknowledgments

- Built with [Paramiko](https://www.paramiko.org/) for SFTP operations
- Uses [Databricks AutoLoader](https://docs.databricks.com/ingestion/auto-loader/index.html) for incremental ingestion
- Implements [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html) for data pipelines
