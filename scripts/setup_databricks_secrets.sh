#!/bin/bash
# Setup Databricks secrets for SFTP connections
# Run this script on your local machine with Databricks CLI configured

set -e  # Exit on error

# Configuration variables
SOURCE_STORAGE="${SOURCE_STORAGE:-sftpsourcestorage001}"
TARGET_STORAGE="${TARGET_STORAGE:-sftptargetstorage001}"
SFTP_USER="${SFTP_USER:-sftpuser}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/sftp_key}"
SECRET_SCOPE="${SECRET_SCOPE:-sftp-credentials}"

echo "========================================="
echo "Databricks Secrets Setup"
echo "========================================="
echo "Secret Scope: $SECRET_SCOPE"
echo "SSH Key Path: $SSH_KEY_PATH"
echo "========================================="
echo ""

# Check if Databricks CLI is configured
if ! databricks workspace ls / > /dev/null 2>&1; then
    echo "Error: Databricks CLI is not configured"
    echo "Please run: databricks configure --token"
    exit 1
fi

# Step 1: Create Secret Scope
echo "Step 1: Creating secret scope..."
if databricks secrets list-scopes | grep -q "$SECRET_SCOPE"; then
    echo "Secret scope '$SECRET_SCOPE' already exists"
else
    databricks secrets create-scope --scope "$SECRET_SCOPE"
    echo "✓ Secret scope created: $SECRET_SCOPE"
fi
echo ""

# Step 2: Get SFTP Connection Details
echo "Step 2: Retrieving SFTP connection details from Azure..."
SOURCE_ENDPOINT=$(az storage account show \
  --name "$SOURCE_STORAGE" \
  --query 'primaryEndpoints.dfs' -o tsv | sed 's|https://||' | sed 's|/||')

TARGET_ENDPOINT=$(az storage account show \
  --name "$TARGET_STORAGE" \
  --query 'primaryEndpoints.dfs' -o tsv | sed 's|https://||' | sed 's|/||')

SOURCE_USERNAME="${SOURCE_STORAGE}.${SFTP_USER}"
TARGET_USERNAME="${TARGET_STORAGE}.${SFTP_USER}"

echo "✓ Connection details retrieved"
echo ""

# Step 3: Store Source SFTP Credentials
echo "Step 3: Storing source SFTP credentials..."
echo "$SOURCE_ENDPOINT" | databricks secrets put-secret --scope "$SECRET_SCOPE" --key source-host
echo "$SOURCE_USERNAME" | databricks secrets put-secret --scope "$SECRET_SCOPE" --key source-username
echo "✓ Source SFTP credentials stored"
echo ""

# Step 4: Store Target SFTP Credentials
echo "Step 4: Storing target SFTP credentials..."
echo "$TARGET_ENDPOINT" | databricks secrets put-secret --scope "$SECRET_SCOPE" --key target-host
echo "$TARGET_USERNAME" | databricks secrets put-secret --scope "$SECRET_SCOPE" --key target-username
echo "✓ Target SFTP credentials stored"
echo ""

# Step 5: Upload SSH Private Key to DBFS
echo "Step 5: Uploading SSH private key to DBFS..."
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Error: SSH private key not found at $SSH_KEY_PATH"
    exit 1
fi

databricks fs cp --overwrite "$SSH_KEY_PATH" dbfs:/FileStore/ssh-keys/sftp_key
echo "✓ SSH private key uploaded to: dbfs:/FileStore/ssh-keys/sftp_key"
echo ""

# Step 6: Verify Setup
echo "Step 6: Verifying setup..."
echo "Secrets in scope '$SECRET_SCOPE':"
databricks secrets list-secrets --scope "$SECRET_SCOPE"
echo ""
echo "Files in DBFS:"
databricks fs ls dbfs:/FileStore/ssh-keys/
echo ""

echo "========================================="
echo "Databricks Secrets Setup Complete!"
echo "========================================="
echo ""
echo "Stored secrets:"
echo "  - source-host: $SOURCE_ENDPOINT"
echo "  - source-username: $SOURCE_USERNAME"
echo "  - target-host: $TARGET_ENDPOINT"
echo "  - target-username: $TARGET_USERNAME"
echo ""
echo "Uploaded to DBFS:"
echo "  - SSH private key: dbfs:/FileStore/ssh-keys/sftp_key"
echo ""
echo "Next step:"
echo "  Open Databricks and run notebook: 01_infrastructure_setup.ipynb"
echo ""
