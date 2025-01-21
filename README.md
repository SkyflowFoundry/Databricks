# Skyflow for Databricks: Bulk Detokenize UDF
This repository contains a user-defined function that can be deployed within any Databricks instance in order to bulk detokenize Skyflow tokens in Databricks. It receives the full tokenized column in bulk, breaks it down into chunks of 25 tokens each, then runs it in a multi-threaded processing before it finally combines back the data before displaying the query result to the user.

![databricks_dashboard](https://github.com/user-attachments/assets/f81227c5-fbbf-481c-b7dc-516f64ad6114)

Note: these examples are not an officially-supported product or recommended for production deployment without further review, testing, and hardening. Use with caution, this is sample code only.

## Create and register the Skyflow UDF
Start a new Notebook in Databricks, set the Notebook to SQL and copy/paste the below code after completing the TODOs.

### Prerequisites

**In Databricks**
- Configure and select the proper resource/cluster in Databricks that allows the execution of a python-wrapped SQL function
- Generate a Databricks access token for SCIM API access
- Configure user groups in Databricks that align with your data access policies

**In Skyflow**
- Create or log into your account at skyflow.com and generate an API key: docs.skyflow.com
- Create a vault and relevant schema to hold your data
- Copy your API key, Vault URL, and Vault ID

#### SQL code
To use this code in your own account, complete the TODOs in the sample below:

```sql
CREATE OR REPLACE FUNCTION default.skyflow_bulk_detokenize(tokens ARRAY<STRING>, user_email STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
AS $$
import requests
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

# Skyflow API details
SKYFLOW_API_URL = "https://<TODO: SKYFLOW_VAULT_URL>/v1/vaults/<TODO: SKYFLOW_VAULT_ID>/detokenize"
BEARER_TOKEN = "<TODO: SKYFLOW_API_KEY>"

# Databricks SCIM API details
DATABRICKS_INSTANCE = "https://<TODO: DATABRICKS_INSTNANCE_ID>.cloud.databricks.com" # e.g. xyz-abc12d34-567e
SCIM_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Users"
DATABRICKS_TOKEN = "<TODO: DATABRICKS_ACCESS_TOKEN>" # e.g. dapi0123456789abcdef0123456789abcdef

# Mapping roles to redaction styles with multiple groups
ROLE_TO_REDACTION = {
    "PLAIN_TEXT": [
        "<TODO: DATABRICKS_GROUP_1>",
        "<TODO: DATABRICKS_GROUP_2>",
        "<TODO: DATABRICKS_GROUP_3>"
    ],
    "MASKED": [
        "<TODO: DATABRICKS_GROUP_4>",
        "<TODO: DATABRICKS_GROUP_5>",
        "<TODO: DATABRICKS_GROUP_6>"
    ],
    "REDACTED": [
        "<TODO: DATABRICKS_GROUP_7>",
        "<TODO: DATABRICKS_GROUP_8>",
        "<TODO: DATABRICKS_GROUP_9>"
    ]
}

# Define the redaction priority order
REDACTION_PRIORITY = ["PLAIN_TEXT", "MASKED", "REDACTED"] # if user has multiple roles across redaction levels, the highest privileges will be used

def get_redaction_style(user_email):
    """
    Function to get the highest redaction style based on databricks user group membership.
    Defaults to REDACTED if no relevant roles are found.
    """
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        # API call to fetch user details
        response = requests.get(f"{SCIM_API_URL}?filter=userName%20eq%20%22{user_email}%22", headers=headers)
        response.raise_for_status()
        user_info = response.json()

        if user_info["totalResults"] == 0:
            print(f"No user found for email: {user_email}")
            return "REDACTED"

        # Extract user groups from the SCIM response
        user_groups = [group.get('display').lower() for group in user_info["Resources"][0].get("groups", [])]

        found_redactions = []

        # Determine redaction levels based on user groups
        for redaction_level, groups in ROLE_TO_REDACTION.items():
            if any(group.lower() in user_groups for group in groups):
                found_redactions.append(redaction_level)

        # If the user has no mapped groups, default to the lowest level (REDACTED)
        if not found_redactions:
            print(f"User {user_email} has no relevant roles, defaulting to REDACTED.")
            return "REDACTED"

        # Determine the highest priority redaction level based on predefined order
        for level in REDACTION_PRIORITY:
            if level in found_redactions:
                return level

        return "REDACTED"  # Default fallback

    except requests.exceptions.RequestException as e:
        print(f"Error fetching user role: {e}")
        return "REDACTED"  # Default on error

def detokenize_chunk(chunk, redaction_style):
    """
    Function to detokenize a chunk of tokens using the Skyflow API.
    """
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "detokenizationParameters": [
            {"token": token, "redaction": redaction_style} for token in chunk
        ]
    }

    try:
        response = requests.post(SKYFLOW_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        return [record["value"] for record in result["records"]]
    except requests.exceptions.RequestException as e:
        print(f"Error detokenizing chunk: {e}")
        return [f"Error: {str(e)}" for _ in chunk]

def bulk_detokenize(tokens, user_email):
    """
    Multi-threaded bulk detokenization with user-specific redaction style.
    """
    if not tokens:
        return []

    # Get the highest redaction style based on user group memberships
    redaction_style = get_redaction_style(user_email)

    MAX_TOKENS_PER_REQUEST = 25
    results = []

    # Split tokens into chunks
    chunks = [tokens[i:i + MAX_TOKENS_PER_REQUEST] for i in range(0, len(tokens), MAX_TOKENS_PER_REQUEST)]

    # Use ThreadPoolExecutor for multi-threading
    with ThreadPoolExecutor(max_workers=math.ceil(len(tokens) / MAX_TOKENS_PER_REQUEST)) as executor:
        future_to_chunk = {executor.submit(detokenize_chunk, chunk, redaction_style): chunk for chunk in chunks}

        for future in as_completed(future_to_chunk):
            results.extend(future.result())

    return results

return bulk_detokenize(tokens, user_email)
$$;
```

## Trigger the Skyflow UDF
Start a new Query or Dashboard in Databricks and copy/paste the below code after completing the TODOs.

### Prerequisites

- Insert a couple records in your Skyflow vault ensuring tokenization is enabled to receive back Skyflow tokens
- Insert the resulting Skyflow tokens in your Databricks instance
- Ensure your Databricks users are assigned to appropriate groups that match your data access policies

#### SQL code
The query below uses current_user() to automatically determine the appropriate redaction level based on the user's Databricks group memberships:

```sql
USE hive_metastore.default;

WITH grouped_data AS (
    SELECT
        1 AS group_id,
        COLLECT_LIST(name) AS names,
        COLLECT_LIST(employee_id) AS employee_ids,
        COLLECT_LIST(role) AS roles,
        COLLECT_LIST(department) AS departments,
        COLLECT_LIST(date_joined) AS date_joineds,
        COLLECT_LIST(age) AS ages,
        COLLECT_LIST(business_unit) AS business_units,
        COLLECT_LIST(security_clearance) AS security_clearances
    FROM employee_data
    GROUP BY group_id
),
detokenized_batches AS (
    SELECT
        skyflow_bulk_detokenize(names, current_user()) AS detokenized_names,
        employee_ids,
        roles,
        departments,
        date_joineds,
        ages,
        business_units,
        security_clearances
    FROM grouped_data
),
exploded_data AS (
    SELECT
        employee_ids[pos] AS employee_id,
        detokenized_names[pos] AS detokenized_name,
        roles[pos] AS role,
        departments[pos] AS department,
        date_joineds[pos] AS date_joined,
        ages[pos] AS age,
        business_units[pos] AS business_unit,
        security_clearances[pos] AS security_clearance
    FROM detokenized_batches
    LATERAL VIEW POSEXPLODE(detokenized_names) AS pos, detokenized_name
)
SELECT
    detokenized_name as name,
    role,
    department,
    date_joined,
    age,
    business_unit,
    security_clearance
FROM exploded_data;
```

# Learn more
To learn more about Skyflow Detokenization APIs visit docs.skyflow.com.
