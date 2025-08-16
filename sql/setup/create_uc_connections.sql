-- Unity Catalog HTTP connections for Skyflow API integration
-- These must be created without catalog context (global metastore resources)

-- Tokenization connection
CREATE CONNECTION IF NOT EXISTS skyflow_tokenize_conn TYPE HTTP
OPTIONS (
  host '${SKYFLOW_VAULT_URL}',
  port 443,
  base_path '/v1/vaults/${SKYFLOW_VAULT_ID}/${SKYFLOW_TABLE}',
  bearer_token secret('skyflow-secrets', 'skyflow_pat_token')
);

-- Detokenization connection  
CREATE CONNECTION IF NOT EXISTS skyflow_detokenize_conn TYPE HTTP  
OPTIONS (
  host '${SKYFLOW_VAULT_URL}',
  port 443,
  base_path '/v1/vaults/${SKYFLOW_VAULT_ID}',
  bearer_token secret('skyflow-secrets', 'skyflow_pat_token')
);