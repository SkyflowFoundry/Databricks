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

# Function to load configuration values automatically from .env.local
load_config() {
    # Source config.sh to get default values and load from .env.local
    source "$(dirname "$0")/config.sh"

    echo
    
    # Set variables from .env.local or defaults, and report what was loaded
    export DATABRICKS_HOST=${DEFAULT_DATABRICKS_HOST}
    if [[ -n "$DATABRICKS_HOST" ]]; then
        echo "✓ DATABRICKS_HOST loaded from .env.local"
    fi
    
    export DATABRICKS_TOKEN=${DEFAULT_DATABRICKS_TOKEN}
    if [[ -n "$DATABRICKS_TOKEN" ]]; then
        echo "✓ DATABRICKS_TOKEN loaded from .env.local"
    fi
    
    export WAREHOUSE_ID=${DEFAULT_WAREHOUSE_ID}
    if [[ -n "$WAREHOUSE_ID" ]]; then
        echo "✓ WAREHOUSE_ID loaded from .env.local"
    fi
    
    export SKYFLOW_VAULT_URL=${DEFAULT_SKYFLOW_VAULT_URL}
    if [[ -n "$SKYFLOW_VAULT_URL" ]]; then
        echo "✓ SKYFLOW_VAULT_URL loaded from .env.local"
    fi
    
    export SKYFLOW_VAULT_ID=${DEFAULT_SKYFLOW_VAULT_ID}
    if [[ -n "$SKYFLOW_VAULT_ID" ]]; then
        echo "✓ SKYFLOW_VAULT_ID loaded from .env.local"
    fi
    
    export SKYFLOW_ACCOUNT_ID=${DEFAULT_SKYFLOW_ACCOUNT_ID}
    if [[ -n "$SKYFLOW_ACCOUNT_ID" ]]; then
        echo "✓ SKYFLOW_ACCOUNT_ID loaded from .env.local"
    fi
    
    export SKYFLOW_PAT_TOKEN=${DEFAULT_SKYFLOW_PAT_TOKEN}
    if [[ -n "$SKYFLOW_PAT_TOKEN" ]]; then
        echo "✓ SKYFLOW_PAT_TOKEN loaded from .env.local"
    fi
    
    export SKYFLOW_TABLE=${DEFAULT_SKYFLOW_TABLE}
    if [[ -n "$SKYFLOW_TABLE" ]]; then
        echo "✓ SKYFLOW_TABLE loaded from .env.local"
    fi
    
    # Group mappings with defaults
    export PLAIN_TEXT_GROUPS=${DEFAULT_PLAIN_TEXT_GROUPS:-"auditor"}
    export MASKED_GROUPS=${DEFAULT_MASKED_GROUPS:-"customer_service"}  
    export REDACTED_GROUPS=${DEFAULT_REDACTED_GROUPS:-"marketing"}
    echo "✓ Group mappings set: PLAIN_TEXT=${PLAIN_TEXT_GROUPS}, MASKED=${MASKED_GROUPS}, REDACTED=${REDACTED_GROUPS}"

    echo -e "\nConfiguration loaded successfully."
}

# Function to substitute environment variables in content
substitute_variables() {
    local content=$1
    echo "$content" | envsubst
}

# Function to setup Unity Catalog connections via SQL file
setup_uc_connections() {
    echo "Creating Unity Catalog connections via SQL..."
    
    # Read SQL file and process each connection separately (direct API call to avoid catalog context)
    local sql_content=$(cat "sql/setup/create_uc_connections.sql")
    local processed_content=$(substitute_variables "$sql_content")
    
    # Split into tokenization and detokenization connection statements
    local tokenize_sql=$(echo "$processed_content" | sed -n '/CREATE CONNECTION.*skyflow_tokenize_conn/,/);/p')
    local detokenize_sql=$(echo "$processed_content" | sed -n '/CREATE CONNECTION.*skyflow_detokenize_conn/,/);$/p')
    
    # Execute tokenization connection creation with detailed logging (direct API call to avoid catalog context)
    echo "Executing tokenization connection SQL without catalog context..."
    local tokenize_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/sql/statements" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"statement\":$(echo "$tokenize_sql" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),\"warehouse_id\":\"${WAREHOUSE_ID}\"}")
    
    if echo "$tokenize_response" | grep -q '"state":"SUCCEEDED"'; then
        echo "✓ Created UC tokenization connection: skyflow_tokenize_conn"
        local tokenize_success=true
    else
        echo "❌ ERROR: Tokenization connection creation failed"
        echo "SQL statement was: $tokenize_sql"
        echo "Response: $tokenize_response"
        local tokenize_success=false
    fi
    
    # Execute detokenization connection creation with detailed logging (direct API call to avoid catalog context)
    echo "Executing detokenization connection SQL without catalog context..."
    local detokenize_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/sql/statements" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"statement\":$(echo "$detokenize_sql" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))'),\"warehouse_id\":\"${WAREHOUSE_ID}\"}")
    
    if echo "$detokenize_response" | grep -q '"state":"SUCCEEDED"'; then
        echo "✓ Created UC detokenization connection: skyflow_detokenize_conn"  
        local detokenize_success=true
    else
        echo "❌ ERROR: Detokenization connection creation failed"
        echo "SQL statement was: $detokenize_sql"
        echo "Response: $detokenize_response"
        local detokenize_success=false
    fi
    
    # Verify connections actually exist after creation
    echo "Verifying UC connections were actually created..."
    local actual_connections=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.1/unity-catalog/connections" | \
        python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    skyflow_conns = [c['name'] for c in data.get('connections', []) if 'skyflow' in c['name'].lower()]
    print(' '.join(skyflow_conns))
except:
    print('')
")
    
    if echo "$actual_connections" | grep -q "skyflow_tokenize_conn"; then
        echo "✓ Verified skyflow_tokenize_conn exists"
    else
        echo "❌ skyflow_tokenize_conn NOT FOUND after creation"
        tokenize_success=false
    fi
    
    if echo "$actual_connections" | grep -q "skyflow_detokenize_conn"; then
        echo "✓ Verified skyflow_detokenize_conn exists"
    else
        echo "❌ skyflow_detokenize_conn NOT FOUND after creation"
        detokenize_success=false
    fi
    
    # Return success only if both connections succeeded
    if [ "$tokenize_success" = true ] && [ "$detokenize_success" = true ]; then
        echo "✓ Both UC connections created successfully via SQL"
        return 0
    else
        echo "❌ Failed to create required UC connections"
        echo "Both connections must be created successfully for setup to proceed"
        exit 1
    fi
}

# Function to setup Unity Catalog secrets via Databricks REST API
setup_uc_secrets() {
    echo "Creating Unity Catalog secrets scope..."
    
    # Create UC-backed secrets scope
    local scope_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/secrets/scopes/create" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d '{
            "scope": "skyflow-secrets",
            "scope_backend_type": "UC"
        }')
    
    # Check if scope creation failed (ignore if already exists)
    if echo "$scope_response" | grep -q '"error_code":"RESOURCE_ALREADY_EXISTS"'; then
        echo "✓ Secrets scope already exists"
    elif echo "$scope_response" | grep -q '"error_code"'; then
        echo "Error creating secrets scope: $scope_response"
        return 1
    else
        echo "✓ Created secrets scope successfully"
    fi
    
    # Create individual secrets
    local secrets=(
        "skyflow_pat_token:${SKYFLOW_PAT_TOKEN}"
        "skyflow_account_id:${SKYFLOW_ACCOUNT_ID}"
        "skyflow_vault_url:${SKYFLOW_VAULT_URL}"
        "skyflow_vault_id:${SKYFLOW_VAULT_ID}"
        "skyflow_table:${SKYFLOW_TABLE}"
    )
    
    for secret_pair in "${secrets[@]}"; do
        IFS=':' read -r key value <<< "$secret_pair"
        echo "Creating secret: $key"
        
        local secret_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/secrets/put" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{
                \"scope\": \"skyflow-secrets\",
                \"key\": \"$key\",
                \"string_value\": \"$value\"
            }")
        
        if echo "$secret_response" | grep -q '"error_code"'; then
            echo "Warning: Error creating secret $key: $secret_response"
        else
            echo "✓ Created secret: $key"
        fi
    done
    
    return 0
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

    # Read file content and substitute variables
    local content=$(cat "$source_file")
    local processed_content=$(substitute_variables "$content")

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
        local processed_content=$(substitute_variables "$sql_content")
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
        
        # Use dedicated catalog if available, otherwise main
        local catalog_context="${CATALOG_NAME:-main}"
        local response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/sql/statements" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "{\"statement\":${json_statement},\"catalog\":\"${catalog_context}\",\"schema\":\"default\",\"warehouse_id\":\"${WAREHOUSE_ID}\"}")
        
        # Check for errors in response
        if echo "$response" | grep -q "error"; then
            echo "Error executing SQL:"
            echo "Statement was: $statement"
            echo "Response: $response"
            return 1
        fi
    done
}

# Function to run a notebook using Databricks Runs API
run_notebook() {
    local notebook_path=$1
    local table_name=$2
    local pii_columns=$3
    local batch_size=${4:-${SKYFLOW_BATCH_SIZE}}  # Use provided batch size or env default
    
    echo "Running notebook: ${notebook_path}"
    echo "Batch size: ${batch_size}"
    
    # Create job run with notebook parameters using serverless compute
    # Note: For serverless, we must use multi-task format with tasks array
    local run_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.1/jobs/runs/submit" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"run_name\": \"Serverless_Tokenize_${table_name}_$(date +%s)\",
            \"tasks\": [
                {
                    \"task_key\": \"tokenize_task\",
                    \"notebook_task\": {
                        \"notebook_path\": \"${notebook_path}\",
                        \"source\": \"WORKSPACE\",
                        \"base_parameters\": {
                            \"table_name\": \"${table_name}\",
                            \"pii_columns\": \"${pii_columns}\",
                            \"batch_size\": \"${batch_size}\"
                        }
                    },
                    \"timeout_seconds\": 1800
                }
            ]
        }")
    
    # Extract run ID
    local run_id=$(echo "$run_response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    if 'run_id' in data:
        print(data['run_id'])
    else:
        print('ERROR: ' + str(data))
        sys.exit(1)
except Exception as e:
    print('ERROR: ' + str(e))
    sys.exit(1)
")
    
    if [[ "$run_id" == ERROR* ]]; then
        echo "Failed to start notebook run: $run_id"
        return 1
    fi
    
    echo "Started notebook run with ID: $run_id"
    
    # Extract workspace ID from hostname for task URL  
    local workspace_id=$(echo "$DATABRICKS_HOST" | sed 's/https:\/\/dbc-//' | sed 's/-.*\.cloud\.databricks\.com.*//')
    echo "View live logs: ${DATABRICKS_HOST}/jobs/runs/${run_id}?o=${workspace_id}"
    
    # Wait for run to complete
    echo "Waiting for tokenization to complete..."
    local max_wait=300  # 5 minutes
    local wait_time=0
    
    while [[ $wait_time -lt $max_wait ]]; do
        local status_response=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id=${run_id}" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}")
        
        local run_state=$(echo "$status_response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    state = data.get('state', {}).get('life_cycle_state', 'UNKNOWN')
    print(state)
except:
    print('UNKNOWN')
")
        
        case $run_state in
            "TERMINATED")
                local result_state=$(echo "$status_response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    result = data.get('state', {}).get('result_state', 'UNKNOWN')
    print(result)
except:
    print('UNKNOWN')
")
                if [[ "$result_state" == "SUCCESS" ]]; then
                    echo "✅ Tokenization completed successfully"
                    return 0
                else
                    echo "❌ Tokenization failed with result: $result_state"
                    return 1
                fi
                ;;
            "INTERNAL_ERROR"|"FAILED"|"TIMEDOUT"|"CANCELED"|"SKIPPED")
                echo "❌ Tokenization run failed with state: $run_state"
                return 1
                ;;
            *)
                echo "Tokenization in progress... (state: $run_state)"
                sleep 30
                wait_time=$((wait_time + 30))
                ;;
        esac
    done
    
    echo "❌ Tokenization timed out after ${max_wait} seconds"
    return 1
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

    # Read file content and substitute variables
    local content=$(cat "$source_file")
    
    # Substitute variables and stringify dashboard content
    local processed_content=$(substitute_variables "$content" | python3 -c 'import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))')

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

# Function to create metastore
create_metastore() {
    echo "Creating dedicated metastore..."
    local metastore_name="${PREFIX}_metastore"
    
    # Create metastore with only required fields (no S3 bucket or IAM role)
    local metastore_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.1/unity-catalog/metastores" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"${metastore_name}\",
            \"region\": \"${DATABRICKS_METASTORE_REGION:-us-west-1}\"
        }")
    
    # Extract metastore ID
    local metastore_id=$(echo "$metastore_response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    if 'metastore_id' in data:
        print(data['metastore_id'])
    else:
        print('ERROR: ' + str(data))
        sys.exit(1)
except Exception as e:
    print('ERROR: ' + str(e))
    sys.exit(1)
")
    
    if [[ "$metastore_id" == ERROR* ]]; then
        echo "Failed to create metastore: $metastore_id"
        return 1
    fi
    
    echo "Metastore created with ID: $metastore_id"
    export METASTORE_ID="$metastore_id"
    
    # Assign metastore to current workspace
    echo "Assigning metastore to workspace..."
    local assignment_response=$(curl -s -X PUT "${DATABRICKS_HOST}/api/2.1/unity-catalog/current-metastore-assignment" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"metastore_id\": \"${metastore_id}\"}")
    
    # Wait for assignment to propagate
    echo "Waiting for metastore assignment..."
    sleep 10
    
    return 0
}

# Function to destroy metastore
destroy_metastore() {
    echo "Finding and destroying metastore..."
    local metastore_name="${PREFIX}_metastore"
    
    # Get metastore ID by name
    local metastores=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.1/unity-catalog/metastores")
    
    local metastore_id=$(echo "$metastores" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    for ms in data.get('metastores', []):
        if ms.get('name') == '${metastore_name}':
            print(ms.get('metastore_id', ''))
            break
    else:
        print('')
except:
    print('')
")
    
    if [[ -n "$metastore_id" ]]; then
        echo "Found metastore ID: $metastore_id"
        
        # Delete metastore
        echo "Deleting metastore..."
        curl -s -X DELETE "${DATABRICKS_HOST}/api/2.1/unity-catalog/metastores/${metastore_id}" \
            -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
            -d '{"force": true}'
        
        echo "Metastore deletion initiated"
        return 0
    else
        echo "Metastore ${metastore_name} not found"
        return 0
    fi
}

# Function to check directory existence
check_directories() {
    local dirs=("notebooks" "sql" "sql/setup" "sql/destroy" "sql/verify" "dashboards")
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

    # Create dedicated catalog for this instance (instead of metastore)
    echo "Creating dedicated catalog: ${PREFIX}_catalog"
    execute_sql "sql/setup/create_catalog.sql" || exit 1
    
    # Use our dedicated catalog instead of main
    export CATALOG_NAME="${PREFIX}_catalog"

    # Create Shared directory if it doesn't exist
    echo "Creating Shared directory..."
    curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/mkdirs" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d "{\"path\": \"/Workspace/Shared\"}"

    # Catalog and schema setup complete
    
    # Create sample table
    echo "Creating sample table..."
    execute_sql "sql/setup/create_sample_table.sql" || exit 1

    # Create tokenization notebook
    echo "Creating tokenization notebook..."
    create_notebook "/Workspace/Shared/${PREFIX}_tokenize_table" "notebooks/notebook_tokenize_table.ipynb" || exit 1

    # Setup Unity Catalog secrets via REST API
    echo "Setting up Unity Catalog secrets via Databricks API..."
    setup_uc_secrets || exit 1
    
    # Create UC connections (required)
    echo "Creating Unity Catalog connections..."
    setup_uc_connections || exit 1
    
    echo "Creating Pure SQL detokenization functions with UC connections..."
    execute_sql "sql/setup/setup_uc_connections_api.sql" || exit 1
    echo "✓ Using UC connections approach (pure SQL, highest performance)"
    
    # Brief pause to ensure function is fully created
    echo "Verifying function creation..."
    sleep 5
    
    # Verify Unity Catalog functions are created
    echo "Verifying Unity Catalog detokenization functions..."
    execute_sql "sql/verify/verify_functions.sql" || exit 1
    
    # Check if table exists before applying masks
    echo "Verifying table exists..."
    execute_sql "sql/verify/verify_table.sql" || exit 1
    
    # The conditional detokenization function is already created by create_uc_detokenize_functions.sql
    echo "✓ Unity Catalog conditional detokenization functions created"
    
    # Tokenize the sample data FIRST (before applying masks)
    echo "Tokenizing PII data in sample table..."
    local pii_columns="first_name"
    local table_name="${PREFIX}_catalog.default.${PREFIX}_customer_data"
    run_notebook "/Workspace/Shared/${PREFIX}_tokenize_table" "${table_name}" "${pii_columns}" "${SKYFLOW_BATCH_SIZE}" || exit 1
    
    # Apply column masks to PII columns AFTER tokenization
    echo "Applying column masks to tokenized PII columns..."
    execute_sql "sql/setup/apply_column_masks.sql" || exit 1

    # Create dashboard
    echo "Creating dashboard..."
    dashboard_id=$(create_dashboard "${PREFIX}_customer_insights" "dashboards/customer_insights_dashboard.lvdash.json" | tail -n 1) || exit 1
    echo "Dashboard created successfully"
    echo "Dashboard URL: ${DATABRICKS_HOST}/sql/dashboardsv3/${dashboard_id}"

    echo "Setup complete! Created resources with prefix: ${PREFIX}"
    echo "
Resources created:
1. Dedicated Unity Catalog: ${PREFIX}_catalog
2. Sample table: ${PREFIX}_catalog.default.${PREFIX}_customer_data (with tokenized PII and column masks applied)
3. Tokenization notebook:
   - /Workspace/Shared/${PREFIX}_tokenize_table (serverless-optimized)
4. Unity Catalog Infrastructure:
   - SQL-created HTTP connections: skyflow_tokenize_conn, skyflow_detokenize_conn
   - UC-backed secrets scope: skyflow-secrets (contains PAT token, account ID, vault details)
   - Bearer token authentication with proper secret() references
5. Pure SQL Functions:
   - ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize (direct Skyflow API via UC connections)
   - ${PREFIX}_catalog.default.${PREFIX}_skyflow_conditional_detokenize (role-based access control)
   - ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize (column mask wrapper)
6. Column masks applied to ALL PII columns - only 'auditor' group sees detokenized data
7. Dashboard: ${PREFIX}_customer_insights (catalog-qualified queries)
8. ✅ PII data automatically tokenized during setup

Usage:
1. Data is already tokenized and ready to use!

2. To query data with automatic detokenization:
   Run: SELECT * FROM ${PREFIX}_catalog.default.${PREFIX}_customer_data
   (PII columns automatically detokenized based on user role)

3. For bulk detokenization:
   Run: SELECT ${PREFIX}_catalog.default.${PREFIX}_skyflow_bulk_detokenize(array('token1', 'token2'), current_user())

3. To view data:
   Dashboard URL: ${DATABRICKS_HOST}/sql/dashboardsv3/${dashboard_id:-"<dashboard_id_not_found>"}
"
}

destroy_components() {
    echo "Destroying resources with prefix: ${PREFIX}"
    local failed_deletions=()
    local successful_deletions=()

    # Drop Unity Catalog functions (including test functions)
    echo "Dropping Unity Catalog detokenization functions..."
    
    # Set catalog context to dedicated catalog if it exists, otherwise skip function drops
    local catalog_name="${PREFIX}_catalog"
    local catalog_check_response=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs/${catalog_name}" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}")
    
    if echo "$catalog_check_response" | grep -q '"name"'; then
        echo "Using catalog: ${catalog_name}"
        CATALOG_NAME="${catalog_name}" execute_sql "sql/destroy/drop_functions.sql"
    else
        echo "Dedicated catalog ${catalog_name} not found, skipping function drops"
    fi
    
    # Clean up UC secrets scope
    echo "Cleaning up Unity Catalog secrets..."
    local delete_scope_response=$(curl -s -X POST "${DATABRICKS_HOST}/api/2.0/secrets/scopes/delete" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        -H "Content-Type: application/json" \
        -d '{"scope": "skyflow-secrets"}')
    
    if echo "$delete_scope_response" | grep -q '"error_code"'; then
        echo "Warning: Could not delete secrets scope: $delete_scope_response"
    else
        echo "✓ Deleted secrets scope: skyflow-secrets"
    fi
    
    # Drop Unity Catalog connections (both SQL-created and REST-created)
    echo "Cleaning up Unity Catalog connections..."
    local connections_response=$(curl -s -X GET "${DATABRICKS_HOST}/api/2.1/unity-catalog/connections" \
        -H "Authorization: Bearer ${DATABRICKS_TOKEN}")
    
    # Extract connection names that are Skyflow-related
    local connection_names=$(echo "$connections_response" | python3 -c "
import json, sys
try:
    data = json.load(sys.stdin)
    names = [conn['name'] for conn in data.get('connections', []) 
             if conn['name'] in ['skyflow_tokenize_conn', 'skyflow_detokenize_conn']]
    print('\n'.join(names))
except Exception as e:
    print(f'Error extracting names: {e}')
    print('')
")
    
    # Delete each matching connection
    if [[ -n "$connection_names" ]]; then
        while IFS= read -r conn_name; do
            if [[ -n "$conn_name" ]]; then
                echo "Deleting UC connection: ${conn_name}"
                local delete_response=$(curl -s -X DELETE "${DATABRICKS_HOST}/api/2.1/unity-catalog/connections/${conn_name}" \
                    -H "Authorization: Bearer ${DATABRICKS_TOKEN}")
                if echo "$delete_response" | grep -q '"error_code"'; then
                    echo "  Warning: Error deleting $conn_name: $delete_response"
                else
                    echo "  ✓ Deleted connection: $conn_name"
                fi
            fi
        done <<< "$connection_names"
    else
        echo "No connections found to delete"
    fi
    
    # Verify UC function deletions
    if execute_sql "sql/verify/check_functions_exist.sql" &>/dev/null; then
        failed_deletions+=("Function: ${PREFIX}_skyflow_uc_detokenize")
        failed_deletions+=("Function: ${PREFIX}_skyflow_mask_detokenize")
    else
        successful_deletions+=("Function: ${PREFIX}_skyflow_uc_detokenize")
        successful_deletions+=("Function: ${PREFIX}_skyflow_mask_detokenize")
    fi

    # Remove column masks before dropping table
    echo "Removing column masks..."
    execute_sql "sql/destroy/remove_column_masks.sql" &>/dev/null || true

    # Drop the table
    echo "Dropping sample table..."
    execute_sql "sql/destroy/drop_table.sql"
    # Verify table deletion
    if execute_sql "sql/verify/check_table_exists.sql" &>/dev/null; then
        failed_deletions+=("Table: ${PREFIX}_customer_data")
    else
        successful_deletions+=("Table: ${PREFIX}_customer_data")
    fi

    # Delete notebook
    echo "Deleting tokenization notebook..."
    local notebook_paths=("/Workspace/Shared/${PREFIX}_tokenize_table")
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

    # Destroy dedicated catalog
    echo "Destroying dedicated catalog..."
    execute_sql "sql/destroy/cleanup_catalog.sql" || echo "Failed to drop catalog, continuing..."

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

# Function to check if resources already exist
check_existing_resources() {
    local has_existing=false
    local existing_resources=()
    
    echo "Checking for existing resources with prefix: ${PREFIX}"
    
    # Check for catalog
    local catalog_check=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs" | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    catalogs = [c['name'] for c in data.get('catalogs', [])]
    exists = '${PREFIX}_catalog' in catalogs
    print('true' if exists else 'false')
except:
    print('false')
")
    
    if [[ "$catalog_check" == "true" ]]; then
        has_existing=true
        existing_resources+=("Catalog: ${PREFIX}_catalog")
    fi
    
    # Check for secrets scope
    local secrets_check=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/secrets/scopes/list" | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    scopes = [s['name'] for s in data.get('scopes', [])]
    exists = 'skyflow-secrets' in scopes
    print('true' if exists else 'false')
except:
    print('false')
")
    
    if [[ "$secrets_check" == "true" ]]; then
        has_existing=true
        existing_resources+=("Secrets scope: skyflow-secrets")
    fi
    
    # Check for UC connections
    local connections_check=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.1/unity-catalog/connections" | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    connections = [c['name'] for c in data.get('connections', [])]
    tokenize_exists = 'skyflow_tokenize_conn' in connections
    detokenize_exists = 'skyflow_detokenize_conn' in connections
    print('true' if (tokenize_exists or detokenize_exists) else 'false')
except:
    print('false')
")
    
    if [[ "$connections_check" == "true" ]]; then
        has_existing=true
        existing_resources+=("UC connections: skyflow_*_conn")
    fi
    
    # Check for notebooks
    local notebook_path="/Workspace/Shared/${PREFIX}_tokenize_table"
    local notebook_check=$(curl -s -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
        "${DATABRICKS_HOST}/api/2.0/workspace/get-status" \
        -d "{\"path\": \"${notebook_path}\"}" | \
        python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    exists = 'path' in data
    print('true' if exists else 'false')
except:
    print('false')
")
    
    if [[ "$notebook_check" == "true" ]]; then
        has_existing=true
        existing_resources+=("Notebook: ${notebook_path}")
    fi
    
    if [[ "$has_existing" == "true" ]]; then
        echo ""
        echo "❌ ERROR: Existing resources found that would conflict with setup:"
        printf '  - %s\n' "${existing_resources[@]}"
        echo ""
        echo "Please run one of the following commands first:"
        echo "  ./setup.sh destroy        # Remove existing resources"
        echo "  ./setup.sh recreate ${PREFIX}  # Remove and recreate all resources"
        echo ""
        exit 1
    else
        echo "✅ No conflicting resources found. Proceeding with setup..."
    fi
}

# Main logic
load_config

if [[ "$1" == "create" ]]; then
    check_existing_resources
    create_components
elif [[ "$1" == "destroy" ]]; then
    destroy_components
elif [[ "$1" == "recreate" ]]; then
    destroy_components
    create_components
fi
