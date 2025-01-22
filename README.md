# Skyflow for Databricks: Bulk Detokenize UDF
This repository contains a user-defined function that can be deployed within any Databricks instance in order to bulk detokenize Skyflow tokens in Databricks. It receives the full tokenized column in bulk, breaks it down into chunks of 25 tokens each, then runs it in a multi-threaded processing before it finally combines back the data before displaying the query result to the user.

![databricks_dashboard](https://github.com/user-attachments/assets/f81227c5-fbbf-481c-b7dc-516f64ad6114)

Note: these examples are not an officially-supported product or recommended for production deployment without further review, testing, and hardening. Use with caution, this is sample code only.

## Quick Start
The repository includes a setup script that automates the deployment process. Simply run:

```bash
./setup.sh create <prefix>
```

This will:
1. Create the detokenization function with your specified prefix
2. Create a sample customer data table
3. Create tokenization notebooks in your Databricks workspace
4. Deploy a customer insights dashboard

To remove all created resources:
```bash
./setup.sh destroy <prefix>
```

## Manual Setup

### Prerequisites

**In Databricks**
- Configure and select the proper resource/cluster in Databricks that allows the execution of a python-wrapped SQL function
- Generate a Databricks access token for SCIM API access
- Configure user groups in Databricks that align with your data access policies

**In Skyflow**
- Create or log into your account at skyflow.com and generate an API key: docs.skyflow.com
- Create a vault and relevant schema to hold your data
- Copy your API key, Vault URL, and Vault ID

### Configuration
Copy config.sh.example to config.sh and fill in your configuration values:
```bash
cp config.sh.example config.sh
```

Edit config.sh to set:
- Databricks host, token, and warehouse ID
- Skyflow vault URL, ID, and bearer token
- Group mappings for redaction levels

### SQL Function
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
BEARER_TOKEN = "<TODO: SKYFLOW_BEARER_TOKEN>"

# Databricks SCIM API details
DATABRICKS_INSTANCE = "https://<TODO: DATABRICKS_INSTNANCE_ID>.cloud.databricks.com" # e.g. xyz-abc12d34-567e
SCIM_API_URL = f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Users"
DATABRICKS_TOKEN = "<TODO: DATABRICKS_ACCESS_TOKEN>" # e.g. dapi0123456789ab...

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

## Example Usage
The query below uses current_user() to automatically determine the appropriate redaction level based on the user's Databricks group memberships:

```sql
USE hive_metastore.default;

WITH grouped_data AS (
    SELECT
        1 AS group_id,
        COLLECT_LIST(first_name) AS first_names,
        COLLECT_LIST(last_name) AS last_names,
        COLLECT_LIST(email) AS emails,
        COLLECT_LIST(phone_number) AS phones,
        COLLECT_LIST(address) AS addresses,
        COLLECT_LIST(date_of_birth) AS dobs
    FROM customer_data
    GROUP BY group_id
),
detokenized_batches AS (
    SELECT
        skyflow_bulk_detokenize(first_names, current_user()) AS detokenized_first_names,
        skyflow_bulk_detokenize(last_names, current_user()) AS detokenized_last_names,
        skyflow_bulk_detokenize(emails, current_user()) AS detokenized_emails,
        skyflow_bulk_detokenize(phones, current_user()) AS detokenized_phones,
        skyflow_bulk_detokenize(addresses, current_user()) AS detokenized_addresses,
        skyflow_bulk_detokenize(dobs, current_user()) AS detokenized_dobs
    FROM grouped_data
),
exploded_data AS (
    SELECT
        pos AS idx,
        detokenized_first_names[pos] AS first_name,
        detokenized_last_names[pos] AS last_name,
        detokenized_emails[pos] AS email,
        detokenized_phones[pos] AS phone_number,
        detokenized_addresses[pos] AS address,
        detokenized_dobs[pos] AS date_of_birth
    FROM detokenized_batches
    LATERAL VIEW POSEXPLODE(detokenized_first_names) AS pos, val
)
SELECT
    c.customer_id,
    e.first_name,
    e.last_name,
    e.email,
    e.phone_number,
    e.address,
    e.date_of_birth,
    c.signup_date,
    c.last_login,
    c.total_purchases,
    c.total_spent,
    c.loyalty_status,
    c.preferred_language,
    c.consent_marketing,
    c.consent_data_sharing
FROM customer_data c
JOIN exploded_data e ON e.idx = CAST(REGEXP_EXTRACT(c.customer_id, '(\\d+)', 0) AS INT) - 1;
```

## Customer Insights Dashboard
The repository includes a pre-built dashboard that showcases the detokenization function in action. The dashboard provides:

- Customer overview with detokenized PII fields
- Spending distribution by loyalty status
- Language preferences
- Consent metrics
- Customer acquisition trends

The dashboard is automatically deployed when running `setup.sh create <prefix>`. You can access it at:
```
https://<your-databricks-host>/sql/dashboards/v3/<dashboard-id>
```

The dashboard URL will be displayed after running the setup script.

# Learn more
To learn more about Skyflow Detokenization APIs visit docs.skyflow.com.
