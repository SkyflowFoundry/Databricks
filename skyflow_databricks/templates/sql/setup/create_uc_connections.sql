-- Unity Catalog HTTP connections for Skyflow API integration
-- These must be created without catalog context (global metastore resources)

-- Single Skyflow connection with base_path ending in vault_id
-- Tokenization adds /{table_name}, detokenization adds /detokenize
CREATE CONNECTION IF NOT EXISTS skyflow_conn TYPE HTTP
OPTIONS (
  host '${SKYFLOW_VAULT_URL}',
  port 443,
  base_path '/v1/vaults/${SKYFLOW_VAULT_ID}',
  bearer_token secret('skyflow-secrets', 'skyflow_pat_token')
);