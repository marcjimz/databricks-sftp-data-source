#!/bin/bash
# Setup Azure Storage accounts with SFTP enabled for Databricks integration
# Run this script on your local machine with Azure CLI installed and logged in

set -e  # Exit on error

# Load configuration from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../config/.env" ]; then
    echo "Loading configuration from config/.env..."
    source "$SCRIPT_DIR/../config/.env"
fi

# Configuration variables (can be overridden by environment variables)
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-databricks-sftp-demo}"
LOCATION="${LOCATION:-eastus}"
SOURCE_STORAGE="${SOURCE_STORAGE:-sftpsourcestorage001}"
TARGET_STORAGE="${TARGET_STORAGE:-sftptargetstorage001}"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-source-data}"
TARGET_CONTAINER="${TARGET_CONTAINER:-target-data}"
SFTP_USER="${SFTP_USER:-sftpuser}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/sftp_key}"

# Check Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "Error: Azure CLI is not installed"
    echo "Install from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
    exit 1
fi

# Check Azure CLI is logged in
if ! az account show &> /dev/null; then
    echo "Error: Not logged in to Azure"
    echo "Please run: az login"
    exit 1
fi

echo "========================================="
echo "Azure SFTP Infrastructure Setup"
echo "========================================="
echo "Subscription: $(az account show --query name -o tsv)"
echo "Resource Group: $RESOURCE_GROUP"
echo "Location: $LOCATION"
echo "Source Storage: $SOURCE_STORAGE"
echo "Target Storage: $TARGET_STORAGE"
echo "========================================="
echo ""

# Step 1: Generate SSH Key Pair
echo "Step 1: Generating SSH key pair..."
if [ -f "$SSH_KEY_PATH" ]; then
    echo "SSH key already exists at $SSH_KEY_PATH"
    read -p "Overwrite? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N ""
        echo "✓ SSH key pair generated"
    fi
else
    ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PATH" -N ""
    echo "✓ SSH key pair generated"
fi
echo ""

# Step 2: Create Resource Group
echo "Step 2: Creating resource group..."
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION"
echo "✓ Resource group created: $RESOURCE_GROUP"
echo ""

# Step 3: Create Source Storage Account
echo "Step 3: Creating source storage account with SFTP..."
az storage account create \
  --name "$SOURCE_STORAGE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true \
  --enable-sftp true
echo "✓ Source storage account created: $SOURCE_STORAGE"
echo ""

# Step 4: Create Target Storage Account
echo "Step 4: Creating target storage account with SFTP..."
az storage account create \
  --name "$TARGET_STORAGE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true \
  --enable-sftp true
echo "✓ Target storage account created: $TARGET_STORAGE"
echo ""

# Step 5: Create Containers
echo "Step 5: Creating containers..."
az storage fs create \
  --name "$SOURCE_CONTAINER" \
  --account-name "$SOURCE_STORAGE" \
  --auth-mode login

az storage fs create \
  --name "$TARGET_CONTAINER" \
  --account-name "$TARGET_STORAGE" \
  --auth-mode login
echo "✓ Containers created successfully"
echo ""

# Step 6: Create SFTP Users
echo "Step 6: Creating SFTP users with SSH key authentication..."
PUBLIC_KEY=$(cat "${SSH_KEY_PATH}.pub")

# Source storage SFTP user
az storage account local-user create \
  --account-name "$SOURCE_STORAGE" \
  --name "$SFTP_USER" \
  --home-directory "$SOURCE_CONTAINER" \
  --permission-scope permissions=rwdlc service=blob resource-name="$SOURCE_CONTAINER" \
  --ssh-authorized-key key="$PUBLIC_KEY"
echo "✓ SFTP user created for source storage"

# Target storage SFTP user
az storage account local-user create \
  --account-name "$TARGET_STORAGE" \
  --name "$SFTP_USER" \
  --home-directory "$TARGET_CONTAINER" \
  --permission-scope permissions=rwdlc service=blob resource-name="$TARGET_CONTAINER" \
  --ssh-authorized-key key="$PUBLIC_KEY"
echo "✓ SFTP user created for target storage"
echo ""

# Step 7: Upload Sample CSV Files
echo "Step 7: Uploading sample CSV files to source storage..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../data"

if [ -f "$DATA_DIR/customers.csv" ]; then
    az storage fs file upload \
      --file-system "$SOURCE_CONTAINER" \
      --account-name "$SOURCE_STORAGE" \
      --source "$DATA_DIR/customers.csv" \
      --path customers.csv \
      --auth-mode login
    echo "✓ Uploaded customers.csv"
else
    echo "⚠ Warning: customers.csv not found at $DATA_DIR/customers.csv"
fi

if [ -f "$DATA_DIR/orders.csv" ]; then
    az storage fs file upload \
      --file-system "$SOURCE_CONTAINER" \
      --account-name "$SOURCE_STORAGE" \
      --source "$DATA_DIR/orders.csv" \
      --path orders.csv \
      --auth-mode login
    echo "✓ Uploaded orders.csv"
else
    echo "⚠ Warning: orders.csv not found at $DATA_DIR/orders.csv"
fi
echo ""

# Step 8: Get SFTP Connection Details
echo "Step 8: Retrieving SFTP connection details..."
SOURCE_ENDPOINT=$(az storage account show \
  --name "$SOURCE_STORAGE" \
  --query 'primaryEndpoints.dfs' -o tsv | sed 's|https://||' | sed 's|/||')

TARGET_ENDPOINT=$(az storage account show \
  --name "$TARGET_STORAGE" \
  --query 'primaryEndpoints.dfs' -o tsv | sed 's|https://||' | sed 's|/||')

echo ""
echo "========================================="
echo "SFTP Connection Details"
echo "========================================="
echo ""
echo "Source SFTP:"
echo "  Host: $SOURCE_ENDPOINT"
echo "  Username: ${SOURCE_STORAGE}.${SFTP_USER}"
echo "  Port: 22"
echo ""
echo "Target SFTP:"
echo "  Host: $TARGET_ENDPOINT"
echo "  Username: ${TARGET_STORAGE}.${SFTP_USER}"
echo "  Port: 22"
echo ""
echo "SSH Private Key: $SSH_KEY_PATH"
echo "SSH Public Key: ${SSH_KEY_PATH}.pub"
echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Save these connection details"
echo "2. Run: ./scripts/setup_databricks_secrets.sh"
echo "3. Open Databricks and run notebook: 01_infrastructure_setup.ipynb"
echo ""

# Optional: Test SFTP Connection
read -p "Test SFTP connection now? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Testing SFTP connection to source storage..."
    sftp -i "$SSH_KEY_PATH" -P 22 "${SOURCE_STORAGE}.${SFTP_USER}@${SOURCE_ENDPOINT}" <<EOF
ls
bye
EOF
    echo "✓ SFTP connection test completed"
fi
