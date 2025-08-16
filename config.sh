#!/bin/bash

# Load .env.local if it exists
if [[ -f "$(dirname "$0")/.env.local" ]]; then
    echo "Loading configuration from .env.local..."
    export $(grep -v '^#' "$(dirname "$0")/.env.local" | xargs)
fi

# Map .env.local variables to our config format (no hardcoded defaults)
if [[ -n "$DATABRICKS_SERVER_HOSTNAME" ]]; then
    DEFAULT_DATABRICKS_HOST="https://$DATABRICKS_SERVER_HOSTNAME"
else
    DEFAULT_DATABRICKS_HOST=""
fi

DEFAULT_DATABRICKS_TOKEN="$DATABRICKS_PAT_TOKEN"
# Extract warehouse ID from HTTP path (format: /sql/1.0/warehouses/warehouse-id)
if [[ -n "$DATABRICKS_HTTP_PATH" ]]; then
    DEFAULT_WAREHOUSE_ID=$(echo "$DATABRICKS_HTTP_PATH" | sed 's/.*warehouses\///')
else
    DEFAULT_WAREHOUSE_ID=""
fi

# Skyflow settings from .env.local (no hardcoded defaults)
DEFAULT_SKYFLOW_VAULT_URL="$SKYFLOW_VAULT_URL"
DEFAULT_SKYFLOW_VAULT_ID="$SKYFLOW_VAULT_ID"
DEFAULT_SKYFLOW_PAT_TOKEN="$SKYFLOW_PAT_TOKEN"
DEFAULT_SKYFLOW_TABLE="$SKYFLOW_TABLE"

# Group mappings for detokenization
DEFAULT_PLAIN_TEXT_GROUPS="auditor"
DEFAULT_MASKED_GROUPS="customer_service"
DEFAULT_REDACTED_GROUPS="marketing"

# Apply any provided values, otherwise use defaults
export DATABRICKS_HOST=${DATABRICKS_HOST:-$DEFAULT_DATABRICKS_HOST}
export DATABRICKS_TOKEN=${DATABRICKS_TOKEN:-$DEFAULT_DATABRICKS_TOKEN}
export WAREHOUSE_ID=${WAREHOUSE_ID:-$DEFAULT_WAREHOUSE_ID}

export SKYFLOW_VAULT_URL=${SKYFLOW_VAULT_URL:-$DEFAULT_SKYFLOW_VAULT_URL}
export SKYFLOW_VAULT_ID=${SKYFLOW_VAULT_ID:-$DEFAULT_SKYFLOW_VAULT_ID}
export SKYFLOW_PAT_TOKEN=${SKYFLOW_PAT_TOKEN:-$DEFAULT_SKYFLOW_PAT_TOKEN}
export SKYFLOW_TABLE=${SKYFLOW_TABLE:-$DEFAULT_SKYFLOW_TABLE}

export PLAIN_TEXT_GROUPS=${PLAIN_TEXT_GROUPS:-$DEFAULT_PLAIN_TEXT_GROUPS}
export MASKED_GROUPS=${MASKED_GROUPS:-$DEFAULT_MASKED_GROUPS}
export REDACTED_GROUPS=${REDACTED_GROUPS:-$DEFAULT_REDACTED_GROUPS}
