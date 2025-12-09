#!/bin/bash
# Setup Databricks secrets for SFTP connections
# Run this script on your local machine with Databricks CLI configured

set -e  # Exit on error

# Ensure proper SSL verification (disable insecure mode if set)
if [ ! -z "$AZURE_CLI_DISABLE_CONNECTION_VERIFICATION" ]; then
    echo "Warning: Disabling AZURE_CLI_DISABLE_CONNECTION_VERIFICATION for security"
    unset AZURE_CLI_DISABLE_CONNECTION_VERIFICATION
fi

# Load configuration from .env file if it exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../config/.env" ]; then
    echo "Loading configuration from config/.env..."
    source "$SCRIPT_DIR/../config/.env"
fi

# Configuration variables (can be overridden by environment variables)
SOURCE_STORAGE="${SOURCE_STORAGE:-sftpsourcestorage001}"
TARGET_STORAGE="${TARGET_STORAGE:-sftptargetstorage001}"
SFTP_USER="${SFTP_USER:-sftpuser}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/sftp_key}"
SECRET_SCOPE="${SECRET_SCOPE:-sftp-credentials}"

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo "Error: Databricks CLI is not installed"
    echo "Install from: https://docs.databricks.com/en/dev-tools/cli/install.html"
    exit 1
fi

# Get Databricks workspace information
WORKSPACE_PROFILE="${DATABRICKS_CONFIG_PROFILE:-DEFAULT}"
WORKSPACE_URL=$(grep -A 2 "\[$WORKSPACE_PROFILE\]" ~/.databrickscfg 2>/dev/null | grep "host" | cut -d'=' -f2 | tr -d ' ' || echo "Unknown")

# Build profile flag for databricks commands
PROFILE_FLAG="--profile $WORKSPACE_PROFILE"

# Check if Databricks CLI is configured
# Try to get current user to verify authentication works
if ! databricks current-user me $PROFILE_FLAG > /dev/null 2>&1; then
    echo "Error: Databricks CLI is not configured or authentication failed"
    echo ""
    echo "Configure using one of these methods:"
    echo ""
    echo "Option 1 - Azure CLI authentication (recommended, no PAT needed):"
    echo "  databricks configure --host https://adb-XXXXX.azuredatabricks.net --profile $WORKSPACE_PROFILE"
    echo ""
    echo "Option 2 - OAuth authentication (no PAT needed):"
    echo "  databricks auth login --host https://adb-XXXXX.azuredatabricks.net --profile $WORKSPACE_PROFILE"
    echo ""
    echo "Option 3 - Personal Access Token:"
    echo "  databricks configure --host https://adb-XXXXX.azuredatabricks.net --token --profile $WORKSPACE_PROFILE"
    exit 1
fi

# Check if Azure CLI is logged in (needed to get storage account details)
if ! az account show &> /dev/null; then
    echo "Error: Not logged in to Azure"
    echo "Please run: az login"
    exit 1
fi

echo "========================================="
echo "Databricks Secrets Setup"
echo "========================================="
echo "Databricks Workspace: $WORKSPACE_URL"
echo "Databricks Profile: $WORKSPACE_PROFILE"
echo "Secret Scope: $SECRET_SCOPE"
echo "SSH Key Path: $SSH_KEY_PATH"
echo "Source Storage: $SOURCE_STORAGE"
echo "Target Storage: $TARGET_STORAGE"
echo "========================================="
echo ""

# Confirmation prompt for workspace
read -p "Continue with this Databricks workspace? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled. To use a different workspace:"
    echo "  1. Configure a new profile:"
    echo "     databricks configure --host https://adb-XXXXX.azuredatabricks.net --profile <name>"
    echo "  2. Run with that profile:"
    echo "     DATABRICKS_CONFIG_PROFILE=<name> ./scripts/setup_databricks_secrets.sh"
    exit 0
fi
echo ""

# Step 1: Create Secret Scope
echo "Step 1: Creating secret scope..."
if databricks secrets list-scopes $PROFILE_FLAG | grep -q "$SECRET_SCOPE"; then
    echo "Secret scope '$SECRET_SCOPE' already exists"
else
    databricks secrets create-scope $PROFILE_FLAG "$SECRET_SCOPE"
    echo "✓ Secret scope created: $SECRET_SCOPE"
fi
echo ""

# Step 2: Get SFTP Connection Details
echo "Step 2: Retrieving SFTP connection details from Azure..."
# Use blob endpoint for SFTP, not dfs endpoint
SOURCE_ENDPOINT=$(az storage account show \
  --name "$SOURCE_STORAGE" \
  --query 'primaryEndpoints.blob' -o tsv | sed 's|https://||' | sed 's|/||')

TARGET_ENDPOINT=$(az storage account show \
  --name "$TARGET_STORAGE" \
  --query 'primaryEndpoints.blob' -o tsv | sed 's|https://||' | sed 's|/||')

SOURCE_USERNAME="${SOURCE_STORAGE}.${SFTP_USER}"
TARGET_USERNAME="${TARGET_STORAGE}.${SFTP_USER}"

echo "✓ Connection details retrieved"
echo ""

# Step 3: Store Source SFTP Credentials
echo "Step 3: Storing source SFTP credentials..."
echo -n "$SOURCE_ENDPOINT" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" source-host
echo -n "$SOURCE_USERNAME" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" source-username
echo "✓ Source SFTP credentials stored"
echo ""

# Step 4: Store Target SFTP Credentials
echo "Step 4: Storing target SFTP credentials..."
echo -n "$TARGET_ENDPOINT" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" target-host
echo -n "$TARGET_USERNAME" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" target-username
echo "✓ Target SFTP credentials stored"
echo ""

# Step 5: Store SSH Private Key in Secrets
echo "Step 5: Storing SSH private key in secrets..."
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Error: SSH private key not found at $SSH_KEY_PATH"
    exit 1
fi

# Store the private key content as a secret
cat "$SSH_KEY_PATH" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" ssh-private-key
echo "✓ SSH private key stored in secrets"
echo ""

# Step 6: Get and Store SSH Host Key Fingerprint
echo "Step 6: Retrieving SSH host key fingerprint..."

# Create temporary file for host key
TMP_HOST_KEY=$(mktemp)
trap "rm -f $TMP_HOST_KEY" EXIT

# Get the SSH host key
ssh-keyscan -p 22 -t rsa "$SOURCE_ENDPOINT" 2>/dev/null | grep "^$SOURCE_ENDPOINT" | head -n 1 > "$TMP_HOST_KEY"

if [ ! -s "$TMP_HOST_KEY" ]; then
    echo "Error: Could not retrieve SSH host key from $SOURCE_ENDPOINT"
    exit 1
fi

# Extract just the key part and save to temp file
TMP_PUB_KEY=$(mktemp)
trap "rm -f $TMP_HOST_KEY $TMP_PUB_KEY" EXIT

awk '{print $2, $3}' "$TMP_HOST_KEY" > "$TMP_PUB_KEY"

# Get fingerprint
SSH_FINGERPRINT=$(ssh-keygen -lf "$TMP_PUB_KEY" 2>/dev/null | awk '{print $2}')

if [ -z "$SSH_FINGERPRINT" ]; then
    echo "Error: Could not generate SSH fingerprint"
    cat "$TMP_HOST_KEY"
    exit 1
fi

echo "✓ SSH fingerprint retrieved: $SSH_FINGERPRINT"
echo -n "$SSH_FINGERPRINT" | databricks secrets put-secret $PROFILE_FLAG "$SECRET_SCOPE" ssh-key-fingerprint
echo "✓ SSH key fingerprint stored in secrets"

# Cleanup
rm -f "$TMP_HOST_KEY" "$TMP_PUB_KEY"
echo ""

# Step 7: Verify Setup
echo "Step 7: Verifying setup..."
echo "Secrets in scope '$SECRET_SCOPE':"
databricks secrets list-secrets $PROFILE_FLAG "$SECRET_SCOPE"
echo ""

echo "========================================="
echo "Databricks Secrets Setup Complete!"
echo "========================================="
echo ""
echo "Stored secrets in scope '$SECRET_SCOPE':"
echo "  - source-host: $SOURCE_ENDPOINT"
echo "  - source-username: $SOURCE_USERNAME"
echo "  - target-host: $TARGET_ENDPOINT"
echo "  - target-username: $TARGET_USERNAME"
echo "  - ssh-private-key: (SSH private key content)"
echo "  - ssh-key-fingerprint: $SSH_FINGERPRINT"
echo ""
echo "Next step:"
echo "  Open Databricks and run notebook: 01_infrastructure_setup.ipynb"
echo ""
