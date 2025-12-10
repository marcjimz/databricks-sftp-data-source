# Databricks SFTP Data Source

A custom Databricks data source demonstrating how to read from SFTP using AutoLoader and write to SFTP using Paramiko SSHv2 library with the Python Data Source API.

## Overview

This repository contains:

1. **Custom SFTP Data Source** - Python package using Paramiko SSHv2 for writing data to SFTP via Databricks Data Source API
2. **Sample Data** - Example CSV files (customers.csv)
3. **Infrastructure Setup** - Complete Azure Storage with SFTP setup scripts
4. **Demo Notebooks** - Step-by-step demonstration of reading and writing with SFTP

## Architecture

```
┌─────────────────┐                          ┌─────────────────┐
│  Source SFTP    │      AutoLoader          │  Databricks     │
│ (Azure Storage) │ ──────────────────────>  │    Streaming    │
└─────────────────┘      (Read)              │    DataFrame    │
                                              └─────────────────┘
                                                      │
                                                      │ Custom
                                                      │ Data Source
                                                      │ (Paramiko)
                                                      ▼
                                              ┌─────────────────┐
                                              │  Target SFTP    │
                                              │ (Azure Storage) │
                                              └─────────────────┘
```

## Features

- **AutoLoader Integration**: Read from SFTP using Databricks built-in AutoLoader
- **Custom Data Source API**: Write to SFTP using Databricks Python Data Source API with Paramiko 3.4.0
- **Unity Catalog**: Manage SFTP connections and credentials securely
- **Distributed Writes**: Each Spark executor handles its own partition writes
- **Managed Volumes**: Checkpoint storage in Unity Catalog managed volumes
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

## Quick Start

### Option A: Demo with Azure Storage (Full Setup)

**Prerequisites:** Azure CLI and Databricks CLI installed and configured ([Azure CLI](https://docs.microsoft.com/cli/azure/install-azure-cli), [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/install.html))

```bash
# Configure environment
cp config/.env.example config/.env
# Edit config/.env with your Azure subscription and storage account names
```

**Step 1: Create Azure Infrastructure**
```bash
./scripts/setup_azure_infrastructure.sh
# Creates Azure Storage accounts with SFTP, uploads sample data
```

**Step 2: Configure Databricks Secrets**
```bash
./scripts/setup_databricks_secrets.sh
# Stores SFTP credentials and SSH keys in Databricks secrets
```

**Step 3-5: Run Databricks Notebooks**
1. `01_infrastructure_setup.ipynb` - Install package, test connections, save config
2. `02_uc_connection_setup.ipynb` - Create Unity Catalog SFTP connections
3. `03_sftp_structured_streaming.ipynb` - Demo read/write with AutoLoader and custom data source

### Option B: Use Existing SFTP Servers

If you already have SFTP servers, skip Azure setup and manually configure secrets:

```bash
# Create secret scope and store credentials
databricks secrets create-scope sftp-credentials
echo -n "sftp.hostname.com" | databricks secrets put-secret sftp-credentials source-host
echo -n "username" | databricks secrets put-secret sftp-credentials source-username
cat ~/.ssh/sftp_key | databricks secrets put-secret sftp-credentials ssh-private-key
# Repeat for target-host, target-username
```

Then run notebooks 01-03 as described above.

## Project Structure

```
databricks-sftp-data-source/
├── CustomDataSource/          # Custom SFTP data source package
│   ├── __init__.py
│   └── SFTPDataSource.py     # Databricks Data Source API + Paramiko
├── scripts/                   # Local machine setup scripts
│   ├── setup_azure_infrastructure.sh    # Azure resources setup
│   └── setup_databricks_secrets.sh      # Databricks secrets configuration
├── notebooks/                 # Databricks notebooks (run in Databricks)
│   ├── 01_infrastructure_setup.ipynb    # Infrastructure setup & verification
│   ├── 02_uc_connection_setup.ipynb     # Unity Catalog connection setup
│   └── 03_sftp_structured_streaming.ipynb  # SFTP read/write demo
├── data/                      # Sample CSV files
│   └── customers.csv          # Sample customer data
├── config/                    # Configuration files
│   └── .env.example           # Environment variables template
├── requirements.txt           # Python dependencies (Paramiko 3.4.0)
├── setup.py                   # Package setup for editable install
├── pyproject.toml             # Modern Python build configuration
└── README.md
```

## Advanced: Configure for Serverless Base Environment

For production use, configure the SFTP data source in the **base environment** for serverless compute, making it automatically available in all serverless notebooks without `%pip install`.

**Full Documentation:** [Add a base environment to your workspace](https://docs.databricks.com/aws/en/admin/workspace-settings/base-environment#add-a-base-environment-to-your-workspace)

### Example Configuration Spec

```yaml
# Base environment configuration for SFTP data source
# Add via: Admin Console → Workspace Settings → Serverless → Base environment

packages:
  - type: "volume"
    path: "/Volumes/main/default/libraries/databricks_sftp_datasource-0.1.0-py3-none-any.whl"
  # Or use workspace file:
  # - type: "workspace_file"
  #   path: "/Workspace/Shared/libraries/databricks_sftp_datasource-0.1.0-py3-none-any.whl"
```

### Build and Upload Package

```bash
# Build the wheel (required - base environment doesn't support source directories)
python -m build

# This creates: dist/databricks_sftp_datasource-0.1.0-py3-none-any.whl

# Upload to Unity Catalog Volume (recommended)
# Via UI: Upload to /Volumes/main/default/libraries/
# Or via CLI (coming soon in Databricks CLI)
```

**Note:** Base environment requires pre-built wheels. For development/testing in individual notebooks, you can use editable installs: `%pip install -e /path/to/source`

### Usage in Serverless Notebooks

```python
# No %pip install needed - package is pre-loaded!
from CustomDataSource import SFTPDataSource

spark.dataSource.register(SFTPDataSource)

# Use immediately
df.write.format("sftp").option(...).save()
```

**Note:** Requires workspace admin privileges. Applies only to serverless compute (notebooks, jobs, SQL warehouses).

## API Usage

### Custom SFTP Data Source API

**Writing to SFTP with Databricks Data Source API:**

```python
from CustomDataSource import SFTPDataSource

# Register the data source with Spark (once per session)
spark.dataSource.register(SFTPDataSource)

# Get SSH key from secrets
ssh_key_content = dbutils.secrets.get("sftp-credentials", "ssh-private-key")

# Write DataFrame to SFTP using native Spark API
df.write \
    .format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("private_key_content", ssh_key_content) \
    .option("port", "22") \
    .option("path", "/remote/path/output.csv") \
    .option("format", "csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save()
```

**Reading from SFTP with AutoLoader:**

```python
# Read from SFTP using AutoLoader (Unity Catalog connection auto-matched by host)
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/catalog/schema/_checkpoints/schema")
    .option("header", "true")
    .load("sftp://user@sftp.example.com:22/data.csv")
)

# Write to table with checkpoint
query = (
    df.writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", "/Volumes/catalog/schema/_checkpoints/data")
    .toTable("catalog.schema.table")
)

query.awaitTermination()
```

**Connection Testing (Non-Distributed):**

```python
from CustomDataSource import SFTPConnectionTester

# For testing connections on driver only (not for Spark distributed operations)
tester = SFTPConnectionTester(
    host="sftp.example.com",
    username="user",
    private_key_path="/path/to/key",
    port=22
)

with tester as conn:
    files = conn.list_files(".")
    print(f"Files: {files}")
```

## API Reference

### SFTPDataSource

Databricks Python Data Source API implementation for SFTP writes.

**Class Methods:**
- `name()` - Returns "sftp" (format name for Spark)
- `writer(schema, overwrite)` - Creates SFTPWriter instance for distributed writes

**Usage:**
```python
spark.dataSource.register(SFTPDataSource)
df.write.format("sftp").option(...).save()
```

### SFTPWriter

DataSourceWriter implementation that handles partition writes.

**Methods:**
- `write(iterator)` - Write partition data to SFTP (called by Spark executors)
- `commit(messages)` - Called when all partition writes succeed
- `abort(messages)` - Called when partition writes fail

### SFTPConnectionTester

Utility class for testing SFTP connections (non-distributed).

**Methods:**
- `connect()` - Establish SFTP connection
- `disconnect()` - Close SFTP connection
- `list_files(remote_dir)` - List files in remote directory
- `__enter__/__exit__` - Context manager support

## Acknowledgments

- Built with [Paramiko 3.4.0](https://www.paramiko.org/) for SFTP operations
- Uses [Databricks Python Data Source API](https://www.databricks.com/blog/announcing-general-availability-python-data-source-api) for custom data source integration
- Leverages [Databricks AutoLoader](https://docs.databricks.com/ingestion/auto-loader/index.html) for incremental SFTP ingestion
- Unity Catalog for secure credential and connection management
