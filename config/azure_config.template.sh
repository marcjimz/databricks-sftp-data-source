#!/bin/bash
# Azure Configuration Template
# Copy this file and fill in your values

# Resource Group
export RESOURCE_GROUP="rg-databricks-sftp-demo"
export LOCATION="eastus"

# Storage Accounts
export SOURCE_STORAGE="sftpsourcestorage001"
export TARGET_STORAGE="sftptargetstorage001"

# Containers
export SOURCE_CONTAINER="source-data"
export TARGET_CONTAINER="target-data"

# SFTP User
export SFTP_USER="sftpuser"

# SSH Key
export SSH_KEY_PATH="~/.ssh/sftp_key"

# Databricks
export DATABRICKS_WORKSPACE_URL="https://<workspace-id>.azuredatabricks.net"
export DATABRICKS_TOKEN="<your-databricks-token>"
