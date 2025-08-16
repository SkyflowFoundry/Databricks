-- Unity Catalog HTTP connections for Skyflow API integration
-- These must be created without catalog context (global metastore resources)

-- Single consolidated Skyflow connection for both tokenization and detokenization
CREATE CONNECTION IF NOT EXISTS skyflow_conn TYPE HTTP
OPTIONS (
  host '${SKYFLOW_VAULT_URL}',
  port 443,
  base_path '/v1/vaults',
  bearer_token secret('skyflow-secrets', 'skyflow_pat_token')
);