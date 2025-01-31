CREATE OR REPLACE FUNCTION default.<TODO: PREFIX>_skyflow_bulk_detokenize(tokens ARRAY<STRING>, user_email STRING)
RETURNS ARRAY<STRING>
LANGUAGE PYTHON
AS $$
import requests
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

# Skyflow API details
SKYFLOW_API_URL = "<TODO: SKYFLOW_VAULT_URL>/v1/vaults/<TODO: SKYFLOW_VAULT_ID>/detokenize"
BEARER_TOKEN = "<TODO: SKYFLOW_BEARER_TOKEN>"

# Databricks SCIM API details
DATABRICKS_INSTANCE = "<TODO: DATABRICKS_HOST>" # e.g. xyz-abc12d34-567e
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
    Function to get the highest redaction style based on the user's Databricks SCIM groups.
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

        # Extract user's groups from the SCIM response
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

    # Get the highest redaction style based on user's group memberships
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
