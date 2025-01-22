#!/bin/bash

# Check if action and prefix are provided correctly
if [[ "$1" != "create" && "$1" != "destroy" && "$1" != "recreate" ]]; then
    echo "Invalid action. Use 'create', 'destroy', or 'recreate'."
    echo "Usage: ./setup.sh <action> <prefix>"
    echo "Example: ./setup.sh create demo"
    exit 1
fi

if [[ "$1" == "create" && -z "$2" ]]; then
    echo "Error: Prefix is required for create action"
    echo "Usage: ./setup.sh create <prefix>"
    exit 1
fi

# Set prefix if provided
if [[ -n "$2" ]]; then
    # Convert to lowercase and replace any non-alphanumeric chars with underscore
    export PREFIX=$(echo "$2" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g')
    echo "Using prefix: $PREFIX"
fi

# Function to prompt for configuration values
prompt_for_config() {
    # Source config.sh to get default values
    source "$(dirname "$0")/config.sh"

    echo
    echo "Enter values for configuration (press Enter to use default values):"
    
    # Databricks settings
    echo -e "\nDatabricks Configuration:"
    read -p "Databricks Host URL [${DEFAULT_DATABRICKS_HOST}]: " input
    export DATABRICKS_HOST=${input:-$DEFAULT_DATABRICKS_HOST}
    
    read -p "Databricks Access Token [${DEFAULT_DATABRICKS_TOKEN}]: " input
    export DATABRICKS_TOKEN=${input:-$DEFAULT_DATABRICKS_TOKEN}
    
    read -p "SQL Warehouse ID [${DEFAULT_WAREHOUSE_ID}]: " input
    export WAREHOUSE_ID=${input:-$DEFAULT_WAREHOUSE_ID}
    
    # Skyflow settings
    echo -e "\nSkyflow Configuration:"
    read -p "Skyflow Vault URL [${DEFAULT_SKYFLOW_VAULT_URL}]: " input
    export SKYFLOW_VAULT_URL=${input:-$DEFAULT_SKYFLOW_VAULT_URL}
    
    read -p "Skyflow Vault ID [${DEFAULT_SKYFLOW_VAULT_ID}]: " input
    export SKYFLOW_VAULT_ID=${input:-$DEFAULT_SKYFLOW_VAULT_ID}
    
    read -p "Skyflow Account ID [${DEFAULT_SKYFLOW_ACCOUNT_ID}]: " input
    export SKYFLOW_ACCOUNT_ID=${input:-$DEFAULT_SKYFLOW_ACCOUNT_ID}
    
    read -p "Skyflow Bearer Token [${DEFAULT_SKYFLOW_BEARER_TOKEN}]: " input
    export SKYFLOW_BEARER_TOKEN=${input:-$DEFAULT_SKYFLOW_BEARER_TOKEN}
    
    # Group mappings
    echo -e "\nGroup Mappings Configuration:"
    read -p "PLAIN_TEXT Groups (comma-separated) [${DEFAULT_PLAIN_TEXT_GROUPS}]: " input
    export PLAIN_TEXT_GROUPS=${input:-$DEFAULT_PLAIN_TEXT_GROUPS}
    
    read -p "MASKED Groups (comma-separated) [${DEFAULT_MASKED_GROUPS}]: " input
    export MASKED_GROUPS=${input:-$DEFAULT_MASKED_GROUPS}
    
    read -p "REDACTED Groups (comma-separated) [${DEFAULT_REDACTED_GROUPS}]: " input
    export REDACTED_GROUPS=${input:-$DEFAULT_REDACTED_GROUPS}

    echo -e "\nConfiguration values set."
    
    # Source config.sh again to apply any other dependent variables
    source "$(dirname "$0")/config.sh"
}

# Function to replace placeholders in content
replace_placeholders() {
    local content=$1
    echo "$content" | sed \
        -e "s|<TODO: SKYFLOW_ACCOUNT_ID>|${SKYFLOW_ACCOUNT_ID}|g" \
        -e "s|<TODO: SKYFLOW_VAULT_URL>|${SKYFLOW_VAULT_URL}|g" \
        -e "s|<TODO: SKYFLOW_VAULT_ID>|${SKYFLOW_VAULT_ID}|g" \
        -e "s|<TODO: SKYFLOW_BEARER_TOKEN>|${SKYFLOW_BEARER_TOKEN}|g" \
        -e "s|<TODO: DATABRICKS_HOST>|${DATABRICKS_HOST}|g" \
        -e "s|<TODO: DATABRICKS_ACCESS_TOKEN>|${DATABRICKS_TOKEN}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_1>|${PLAIN_TEXT_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_2>|${PLAIN_TEXT_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_3>|${PLAIN_TEXT_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_4>|${MASKED_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_5>|${MASKED_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_6>|${MASKED_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_7>|${REDACTED_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_8>|${REDACTED_GROUPS}|g" \
        -e "s|<TODO: DATABRICKS_GROUP_9>|${REDACTED_GROUPS}|g" \
        -e "s|<TODO: PREFIX>|${PREFIX}|g"
}

# Function to create a notebook using Databricks REST API
create_notebook() {
    local path=$1
    local source_file=$2
    
    # Check if source file exists
    if [[ ! -f "$source_file" ]]; then
        echo "Error: Source file not found: $source_file"
        return 1
    fi

    # Read file content and replace placeholders
    local content=$(cat "$source_file")
    local processed_content=$(replace_placeholders "$content")

    # Base64 encode the processed content
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        encoded_content=$(echo "$processed_content" | base64)
    else
        # Linux - try -w 0 first, fall back to plain base64
        encoded_content=$(echo "$processed_content" | base64 -w 0 2>/dev/null || echo "$processed_content" | base64 | tr -d '\n')
    fi

    local response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/import" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"path\": \"${path}\",
            \"format\": \"JUPYTER\",
            \"content\": \"${encoded_content}\",
            \"overwrite\": true
        }")
    
    # Check for errors
    if echo "$response" | grep -q "error"; then
        echo "Error creating notebook:"
        echo "$response"
        return 1
    else
        echo "Notebook created successfully at ${path}"
    fi
}

# Function to execute SQL using Databricks REST API
execute_sql() {
    local sql_file=$1
    
    local statements=()
    
    # Handle direct SQL statements vs SQL files
    if [[ "$sql_file" == DROP* || "$sql_file" == DESCRIBE* ]]; then
        # For DROP and DESCRIBE commands, use the command directly
        statements+=("$sql_file")
    else
        # For SQL files, read and process content
        if [[ ! -f "$sql_file" ]]; then
            echo "Error: SQL file not found: $sql_file"
            return 1
        fi
        
        # Read and process SQL file
        local sql_content=$(cat "$sql_file")
        local processed_content=$(replace_placeholders "$sql_content")
        local current_statement=""
        
        # Split into statements, handling multi-line SQL properly
        while IFS= read -r line || [[ -n "$line" ]]; do
            # Skip empty lines and comments
            if [[ -z "${line// }" ]] || [[ "$line" =~ ^[[:space:]]*-- ]]; then
                continue
            fi
            
            # Add line to current statement with exact whitespace preservation
            if [[ -z "$current_statement" ]]; then
                current_statement="${line}"
            else
                # Preserve exact line including all whitespace
                current_statement+=$'\n'"${line}"
            fi
            
            # If line ends with semicolon, it's end of statement
            if [[ "$line" =~ \;[[:space:]]*$ ]]; then
                if [[ -n "$current_statement" ]]; then
                    statements+=("$current_statement")
                fi
                current_statement=""
            fi
        done <<< "$processed_content"
    fi
    
    # Execute each statement
    for statement in "${statements[@]}"; do
        # Properly escape for JSON while preserving newlines
        # Write statement to temp file to avoid shell interpretation
        local temp_file=$(mktemp)
        printf "%s" "$statement" > "$temp_file"
        
        # Use Python to properly escape while preserving exact formatting
        local json_statement=$(python3 -c '
import json, sys
with open("'"$temp_file"'", "r") as f:
    content = f.read()
print(json.dumps(content))
')
        rm "$temp_file"
        
        local response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/sql/statements" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"statement\":${json_statement},\"catalog\":\"hive_metastore\",\"schema\":\"default\",\"warehouse_id\":\"${WAREHOUSE_ID}\"}")
        
        # Check for errors in response
        if echo "$response" | grep -q "error"; then
            echo "Error executing SQL:"
            echo "$response"
            return 1
        fi
    done
}

# Function to import dashboard using Databricks Lakeview API
create_dashboard() {
    local path=$1
    local source_file=$2
    
    # Check if source file exists
    if [[ ! -f "$source_file" ]]; then
        echo "Error: Dashboard file not found: $source_file"
        return 1
    fi

    # Read file content and replace placeholders
    local content=$(cat "$source_file")
    
    # Replace placeholders and stringify dashboard content
    local processed_content=$(replace_placeholders "$content" | python3 -c 'import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))')

    # Check if dashboard already exists and delete it
    dashboards=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json")
    
    existing_id=$(echo "$dashboards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
prefix = '${PREFIX}'
ids = [d['dashboard_id'] for d in data.get('dashboards', []) 
       if d.get('display_name', '') == prefix + '_customer_insights']
print(ids[0] if ids else '')
")

    if [[ -n "$existing_id" ]]; then
        echo "Deleting existing dashboard..."
        curl -s -X DELETE "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards/${existing_id}" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}"
        
        # Wait for deletion to complete
        echo "Waiting for dashboard deletion to complete..."
        sleep 5
        
        # Verify deletion
        local max_retries=3
        local retry_count=0
        while [[ $retry_count -lt $max_retries ]]; do
            dashboards=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards" \
                -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
                -H "Content-Type: application/json")
            
            still_exists=$(echo "$dashboards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
prefix = '${PREFIX}'
ids = [d['dashboard_id'] for d in data.get('dashboards', []) 
       if d.get('display_name', '') == prefix + '_customer_insights']
print('true' if ids else 'false')
")
            
            if [[ "$still_exists" == "false" ]]; then
                break
            fi
            
            echo "Dashboard still exists, waiting..."
            sleep 5
            ((retry_count++))
        done
    fi

    # Create dashboard using Lakeview API
    local payload=$(echo "{
        \"display_name\": \"${PREFIX}_customer_insights\",
        \"warehouse_id\": \"${WAREHOUSE_ID}\",
        \"serialized_dashboard\": ${processed_content},
        \"parent_path\": \"/Shared\"
    }")

    local response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "$payload")
    
    # Check for errors
    if echo "$response" | grep -q "error\|Error\|ERROR"; then
        echo "Error creating dashboard:"
        echo "$response"
        return 1
    fi
    
    # Extract dashboard ID from response, handling both stdout and stderr from curl
    local dashboard_id=$(echo "$response" | grep -o '"dashboard_id":"[^"]*"' | cut -d'"' -f4)
    if [[ -z "$dashboard_id" ]]; then
        echo "Error: Could not extract dashboard ID from response"
        return 1
    fi
    echo "$dashboard_id"
}

# Function to check directory existence
check_directories() {
    local dirs=("notebooks" "sql" "dashboards")
    local missing=0
    
    for dir in "${dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            echo "Error: Required directory not found: $dir"
            missing=1
        fi
    done
    
    if [[ $missing -eq 1 ]]; then
        echo "Please ensure all required directories exist before running setup."
        exit 1
    fi
}

create_components() {
    echo "Creating resources with prefix: ${PREFIX}"
    
    # Verify required directories exist
    check_directories || exit 1

    # Create Shared directory if it doesn't exist
    echo "Creating Shared directory..."
    curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/mkdirs" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"path\": \"/Workspace/Shared\"}"

    # Create sample table
    echo "Creating sample table..."
    execute_sql "sql/create_sample_table.sql" || exit 1

    # Create notebooks
    echo "Creating notebooks..."
    for notebook in tokenize_table call_tokenize_table; do
        echo "Creating ${notebook} notebook..."
        create_notebook "/Workspace/Shared/${PREFIX}_${notebook}" "notebooks/notebook_${notebook}.ipynb" || exit 1
    done

    # Create detokenization function
    echo "Creating detokenization function..."
    execute_sql "sql/create_detokenize_function.sql" || exit 1
    
    # Verify function creation
    # echo "Verifying function creation..."
    # execute_sql "DESCRIBE FUNCTION EXTENDED ${PREFIX}_skyflow_bulk_detokenize" || exit 1

    # Create dashboard
    echo "Creating dashboard..."
    dashboard_id=$(create_dashboard "${PREFIX}_customer_insights" "dashboards/customer_insights_dashboard.lvdash.json" | tail -n 1) || exit 1
    echo "Dashboard created successfully"
    echo "Dashboard URL: ${DATABRICKS_HOST}/sql/dashboardsv3/${dashboard_id}"

    echo "Setup complete! Created resources with prefix: ${PREFIX}"
    echo "
Resources created:
1. Sample table: ${PREFIX}_customer_data
2. Tokenization notebooks:
   - /Workspace/Shared/${PREFIX}_tokenize_table
   - /Workspace/Shared/${PREFIX}_call_tokenize_table
3. Detokenization function: ${PREFIX}_skyflow_bulk_detokenize
4. Dashboard: ${PREFIX}_customer_insights

Usage:
1. To tokenize data:
   Open and run the /Workspace/Shared/${PREFIX}_call_tokenize_table notebook

2. To detokenize data:
   Run: SELECT ${PREFIX}_skyflow_bulk_detokenize(array('token1', 'token2'), current_user())

3. To view data:
   Dashboard URL: ${DATABRICKS_HOST}/sql/dashboardsv3/${dashboard_id:-"<dashboard_id_not_found>"}
"
}

destroy_components() {
    echo "Destroying resources with prefix: ${PREFIX}"
    local failed_deletions=()
    local successful_deletions=()

    # Drop the function
    echo "Dropping detokenization function..."
    execute_sql "DROP FUNCTION IF EXISTS ${PREFIX}_skyflow_bulk_detokenize"
    # Verify function deletion
    if execute_sql "DESCRIBE FUNCTION ${PREFIX}_skyflow_bulk_detokenize" &>/dev/null; then
        failed_deletions+=("Function: ${PREFIX}_skyflow_bulk_detokenize")
    else
        successful_deletions+=("Function: ${PREFIX}_skyflow_bulk_detokenize")
    fi

    # Drop the table
    echo "Dropping sample table..."
    execute_sql "DROP TABLE IF EXISTS ${PREFIX}_customer_data"
    # Verify table deletion
    if execute_sql "DESCRIBE TABLE ${PREFIX}_customer_data" &>/dev/null; then
        failed_deletions+=("Table: ${PREFIX}_customer_data")
    else
        successful_deletions+=("Table: ${PREFIX}_customer_data")
    fi

    # Delete notebooks
    echo "Deleting notebooks..."
    local notebook_paths=("/Workspace/Shared/${PREFIX}_tokenize_table" "/Workspace/Shared/${PREFIX}_call_tokenize_table")
    for notebook_path in "${notebook_paths[@]}"; do
        echo "Deleting notebook: ${notebook_path}"
        local response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/delete" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"path\": \"${notebook_path}\", \"recursive\": true}")
        
        # Verify notebook deletion by trying to get its info
        local verify_response=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.0/workspace/get-status" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"path\": \"${notebook_path}\"}")
        
        if echo "$verify_response" | grep -q "error_code.*RESOURCE_DOES_NOT_EXIST"; then
            successful_deletions+=("Notebook: ${notebook_path}")
        else
            failed_deletions+=("Notebook: ${notebook_path}")
        fi
    done

    # Delete dashboard
    echo "Deleting dashboard..."
    # Get all dashboards and find matching ones
    dashboards=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json")
    
    # Use Python to handle the JSON parsing and find all matching dashboards
    matching_ids=$(echo "$dashboards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
prefix = '${PREFIX}'
ids = [d['dashboard_id'] for d in data.get('dashboards', []) 
       if d.get('display_name', '').startswith(prefix)]
print('\n'.join(ids))
")
    
    # Delete each matching dashboard
    while IFS= read -r dashboard_id; do
        if [[ -n "$dashboard_id" ]]; then
            echo "Deleting dashboard with ID: ${dashboard_id}"
            curl -s -X DELETE "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards/${dashboard_id}" \
                -H "Authorization: Bearer ${DATABRICKS_TOKEN}"
            
            # Verify dashboard deletion
            sleep 2  # Brief pause to allow deletion to propagate
            local verify_dashboards=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.0/lakeview/dashboards" \
                -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
                -H "Content-Type: application/json")
            
            local still_exists=$(echo "$verify_dashboards" | python3 -c "
import sys, json
data = json.load(sys.stdin)
dashboard_id = '${dashboard_id}'
exists = any(d['dashboard_id'] == dashboard_id for d in data.get('dashboards', []))
print('true' if exists else 'false')
")
            
            if [[ "$still_exists" == "false" ]]; then
                successful_deletions+=("Dashboard: ID ${dashboard_id}")
            else
                failed_deletions+=("Dashboard: ID ${dashboard_id}")
            fi
        fi
    done <<< "$matching_ids"

    # Print summary
    echo -e "\nDestroy Summary:"
    if [[ ${#successful_deletions[@]} -gt 0 ]]; then
        echo -e "\nSuccessfully deleted:"
        printf '%s\n' "${successful_deletions[@]}"
    fi
    
    if [[ ${#failed_deletions[@]} -gt 0 ]]; then
        echo -e "\nFailed to delete:"
        printf '%s\n' "${failed_deletions[@]}"
        echo -e "\nWarning: Some resources could not be verified as deleted. They may require manual cleanup."
        exit 1
    else
        echo -e "\nAll resources successfully deleted!"
    fi
}

# Main logic
prompt_for_config

if [[ "$1" == "create" ]]; then
    create_components
elif [[ "$1" == "destroy" ]]; then
    destroy_components
elif [[ "$1" == "recreate" ]]; then
    destroy_components
    create_components
fi
