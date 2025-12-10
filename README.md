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

Run this notebook to configure Unity Catalog SFTP connections:

1. **Load configuration** saved from notebook 01
2. **Create Unity Catalog SFTP connections:**
   - Source SFTP connection for reading data (used by AutoLoader)
   - Target SFTP connection for writing data (used by AutoLoader)

**What you'll have after this step:**
- Unity Catalog connections configured for source and target SFTP
- Connections ready for AutoLoader and structured streaming

### Step 5: Run SFTP Structured Streaming Demo (Databricks)

**Notebook**: `notebooks/03_sftp_structured_streaming.ipynb`

Run this notebook to demonstrate end-to-end SFTP reading and writing:

1. **Read from Source SFTP:**
   - Uses AutoLoader to read `customers.csv` from source SFTP
   - Displays the data and schema
   - Writes to Unity Catalog table

2. **Write to Target SFTP:**
   - Creates demo DataFrame
   - Uses custom SFTP data source to write to target SFTP
   - Each Spark executor writes its partition using Paramiko

3. **Verify Results:**
   - Lists files in target SFTP directory
   - Confirms `demo_customers.csv` was written successfully

**What you'll have after this step:**
- Complete demonstration of SFTP read/write operations
- Data written to target SFTP server
- Verification that the custom data source works correctly

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

# Store source SFTP credentials (use -n to avoid adding newline)
echo -n "source.sftp.hostname.com" | databricks secrets put-secret sftp-credentials source-host
echo -n "sourceuser" | databricks secrets put-secret sftp-credentials source-username

# Store target SFTP credentials
echo -n "target.sftp.hostname.com" | databricks secrets put-secret sftp-credentials target-host
echo -n "targetuser" | databricks secrets put-secret sftp-credentials target-username

# Store SSH private key content in secrets
cat ~/.ssh/your_sftp_key | databricks secrets put-secret sftp-credentials ssh-private-key
```

#### Step 3: Continue with Databricks Setup

Proceed directly to Step 3 (Install Package and Verify Setup) in the Databricks notebooks.

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

## Data Flow Example

### Input Data (Source SFTP)

**customers.csv**
```csv
customer_id,name,email,country,signup_date
1,John Smith,john.smith@example.com,USA,2024-01-15
2,Emma Johnson,emma.j@example.com,UK,2024-02-20
```

### Output Data (Target SFTP)

**demo_customers.csv** (written by custom data source)
```csv
customer_id,name,email,country,signup_date
1,Demo Customer 1,demo1@example.com,USA,2025-12-09
2,Demo Customer 2,demo2@example.com,UK,2025-12-09
3,Demo Customer 3,demo3@example.com,Canada,2025-12-09
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

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to SFTP
```
Solution: Verify SSH key and credentials
- Check SSH key in secrets: databricks secrets list-secrets sftp-credentials
- Test connection from local machine: sftp -i ~/.ssh/sftp_key user@host
- Verify ECDSA fingerprint: ssh-keyscan -t ecdsa host | ssh-keygen -lf -
```

### AutoLoader Issues

**Problem**: AutoLoader not detecting files or connection errors
```
Solution: Verify Unity Catalog SFTP connection
- List connections: SHOW CONNECTIONS IN catalog_name
- Check connection config: DESCRIBE CONNECTION connection_name
- Verify host in SFTP URI matches connection host (auto-matching)
```

### Custom Data Source Write Issues

**Problem**: ModuleNotFoundError when writing to SFTP
```
Solution: Ensure package is installed with pip install -e
- Run: %pip install -e .. (from notebooks directory)
- Restart Python: dbutils.library.restartPython()
- Verify import works: from CustomDataSource import SFTPDataSource
```

**Problem**: SSH key file not found on executors
```
Solution: Use private_key_content option instead of private_key_path
- Pass key content as string: .option("private_key_content", ssh_key_content)
- Each executor creates its own temp file from the content
- Avoids file path issues in distributed environments
```

## Acknowledgments

- Built with [Paramiko 3.4.0](https://www.paramiko.org/) for SFTP operations
- Uses [Databricks Python Data Source API](https://www.databricks.com/blog/announcing-general-availability-python-data-source-api) for custom data source integration
- Leverages [Databricks AutoLoader](https://docs.databricks.com/ingestion/auto-loader/index.html) for incremental SFTP ingestion
- Unity Catalog for secure credential and connection management
