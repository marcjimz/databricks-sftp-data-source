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

### For Demo Setup (Optional - Creates Azure SFTP Infrastructure):
- Azure subscription with permissions to create storage accounts
- Azure CLI installed locally ([installation guide](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli))
- Databricks CLI installed locally ([installation guide](https://docs.databricks.com/en/dev-tools/cli/install.html))

### For Production Use (Using Existing SFTP Servers):
- Databricks workspace with Unity Catalog enabled
- Access to existing SFTP servers (host, username, SSH keys)
- Python 3.9+

## Quick Start - End-to-End Setup

### Option A: Demo Setup with Azure Storage (Creates SFTP Infrastructure)

Follow these steps to set up a complete demo environment with Azure Storage as SFTP servers.

#### Step 0: Configure Azure and Databricks CLI (Local Machine)

Before running the setup scripts, ensure your CLI tools are configured:

**Install and Configure Azure CLI:**
```bash
# Install Azure CLI (if not already installed)
# macOS
brew install azure-cli

# Windows
# Download from: https://aka.ms/installazurecliwindows

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login to Azure
az login

# Set your subscription (if you have multiple)
az account list --output table
az account set --subscription "<subscription-id>"

# Verify login
az account show
```

**Install and Configure Databricks CLI:**
```bash
# Install Databricks CLI (latest version)
# macOS/Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Or via Homebrew on macOS
brew tap databricks/tap
brew install databricks

# Verify installation
databricks --version
```

**Configure Authentication:**

```bash
# If you're already logged in to Azure CLI (az login),
# the Databricks CLI can automatically use those credentials

# Just configure the workspace host to DEFAULT
databricks configure --host https://adb-123456789.azuredatabricks.net

# The CLI will automatically use your Azure CLI credentials
# Verify it works
databricks workspace list / --profile DEFAULT
```

**Multiple Workspaces (Using Profiles):**
```bash
# Configure different workspaces with profiles
databricks configure --host https://prod.azuredatabricks.net --profile production
databricks configure --host https://dev.azuredatabricks.net --profile development

# View all configured profiles
cat ~/.databrickscfg

# Use a specific profile
databricks workspace ls / --profile production
```

**Important**: The `setup_databricks_secrets.sh` script will use your **DEFAULT** profile. If you have multiple workspaces:
```bash
# Use default profile
./scripts/setup_databricks_secrets.sh

# Use a specific profile
DATABRICKS_CONFIG_PROFILE=production ./scripts/setup_databricks_secrets.sh
```

**Configure Environment Variables:**
```bash
# Copy the example configuration file
cp config/.env.example config/.env

# Edit config/.env with your values
# Update storage account names to be globally unique
nano config/.env  # or use your preferred editor
```

#### Step 1: Setup Azure Infrastructure (Local Machine)

Run the Azure setup script from the repository root:

```bash
./scripts/setup_azure_infrastructure.sh
```

**What this script does:**
- Generates SSH key pair (`~/.ssh/sftp_key`)
- Creates Azure resource group
- Creates two Azure Storage accounts with SFTP enabled (source and target)
- Creates containers for data
- Configures SFTP users with SSH key authentication
- Uploads sample CSV files (customers.csv, orders.csv) to source SFTP
- Displays connection details

**What you'll have after this step:**
- Two Azure Storage accounts with SFTP enabled
- SSH keys for authentication
- Sample data files in source SFTP
- SFTP connection details

### Step 2: Configure Databricks Secrets (Local Machine)

Run the Databricks secrets setup script from the repository root:

```bash
./scripts/setup_databricks_secrets.sh
```

**Prerequisites:**
- Databricks CLI installed and configured (see Step 0 for authentication options - Azure CLI auth is recommended)
- Azure CLI configured

**What this script does:**
- Displays the Databricks workspace you're connected to and prompts for confirmation
- Creates Databricks secret scope (`sftp-credentials`)
- Stores SFTP connection details in Databricks secrets
- Stores SSH private key content in Databricks secrets

**What you'll have after this step:**
- All credentials securely stored in Databricks secrets (host, username, SSH private key)
- Ready to run Databricks notebooks

### Step 3: Install Package and Verify Setup (Databricks)

**Notebook**: `notebooks/01_infrastructure_setup.ipynb`

Run this notebook in your Databricks workspace to:
- Install the SFTP package from requirements.txt using relative paths (`%pip install -r ../requirements.txt`)
- Verify secrets are configured correctly (host, username, SSH private key)
- Retrieve SSH private key from secrets and create temporary file for authentication
- Test SFTP connections to source and target
- Save configuration to Unity Catalog

**What you'll have after this step:**
- SFTP package installed in Databricks
- Verified SFTP connectivity using credentials from secrets
- Configuration saved to Unity Catalog

### Step 4: Set Up Unity Catalog Connections (Databricks)

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
import tempfile
import os

# Get SSH private key from secrets and write to temporary file
ssh_key_content = dbutils.secrets.get("sftp-credentials", "ssh-private-key")
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_sftp_key') as tmp_key:
    tmp_key.write(ssh_key_content)
    tmp_key_path = tmp_key.name

# Set proper permissions on the key file
os.chmod(tmp_key_path, 0o600)

config = {
    "host": dbutils.secrets.get("sftp-credentials", "target-host"),
    "username": dbutils.secrets.get("sftp-credentials", "target-username"),
    "private_key_path": tmp_key_path,
    "port": 22
}

writer = SFTPWriter.create_writer(config)
with writer.session():
    files = writer.list_files("/data/output")
    print(f"Files in target SFTP: {files}")

# Clean up temporary key file
os.remove(tmp_key_path)
```

**Expected output files:**
- `customer_summary.csv` - Aggregated customer metrics
- `order_analytics.csv` - Order statistics and trends

### Option B: Production Setup with Existing SFTP Servers

If you have existing SFTP servers and don't need the demo Azure infrastructure:

#### Step 1: Prepare SSH Keys and Credentials (Local Machine)

Ensure you have:
- SSH private key for SFTP authentication
- SFTP connection details (host, username, port)

#### Step 2: Configure Databricks Secrets (Local Machine)

First, configure Databricks CLI if not already done:
```bash
# Option 1: Azure CLI authentication (recommended, no PAT needed)
databricks configure --host https://adb-XXXXX.azuredatabricks.net

# Option 2: OAuth authentication (no PAT needed)
databricks auth login --host https://adb-XXXXX.azuredatabricks.net

# Option 3: Personal Access Token
databricks configure --host https://adb-XXXXX.azuredatabricks.net --token
```

Manually create Databricks secrets:
```bash
# Create secret scope
databricks secrets create-scope sftp-credentials

# Store source SFTP credentials
echo "source.sftp.hostname.com" | databricks secrets put-secret sftp-credentials source-host
echo "sourceuser" | databricks secrets put-secret sftp-credentials source-username

# Store target SFTP credentials
echo "target.sftp.hostname.com" | databricks secrets put-secret sftp-credentials target-host
echo "targetuser" | databricks secrets put-secret sftp-credentials target-username

# Store SSH private key content in secrets
cat ~/.ssh/your_sftp_key | databricks secrets put-secret sftp-credentials ssh-private-key
```

#### Step 3: Continue with Databricks Setup

Proceed directly to Step 3 (Install Package and Verify Setup) in the Databricks notebooks.

## Project Structure

```
databricks-sftp-data-source/
├── src/                       # Source code
│   └── ingest/               # Custom SFTP data source package
│       ├── __init__.py
│       └── SFTPWriter.py     # Paramiko-based SFTP writer
├── scripts/                   # Local machine setup scripts
│   ├── setup_azure_infrastructure.sh    # Azure resources setup
│   └── setup_databricks_secrets.sh      # Databricks secrets configuration
├── notebooks/                 # Databricks notebooks (run in Databricks)
│   ├── 01_infrastructure_setup.ipynb    # Package install & verification
│   ├── 02_uc_connection_setup.ipynb     # Unity Catalog setup
│   └── 03_dlt_pipeline.ipynb            # DLT pipeline
├── data/                      # Sample CSV files
│   ├── customers.csv
│   └── orders.csv
├── config/                    # Configuration files
│   ├── .env.example                     # Environment variables template
│   └── dlt_pipeline_config.json         # DLT pipeline configuration
├── requirements.txt           # Python dependencies (pinned versions)
├── setup.py                   # Package setup
└── README.md
```

## API Usage

### Custom SFTP Data Source API

```python
from ingest import SFTPDataSource, SFTPWriter
import tempfile
import os

# Get SSH private key from secrets and write to temporary file
ssh_key_content = dbutils.secrets.get("sftp-credentials", "ssh-private-key")
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_sftp_key') as tmp_key:
    tmp_key.write(ssh_key_content)
    tmp_key_path = tmp_key.name

# Set proper permissions
os.chmod(tmp_key_path, 0o600)

# Configuration
config = {
    "host": dbutils.secrets.get("sftp-credentials", "target-host"),
    "username": dbutils.secrets.get("sftp-credentials", "target-username"),
    "private_key_path": tmp_key_path,
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

# Clean up temporary key file
os.remove(tmp_key_path)
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
