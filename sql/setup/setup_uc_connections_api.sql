-- Unity Catalog Connections Setup via REST API
-- Complete pure SQL implementation matching Python UDF functionality
-- This provides zero Python overhead with native Spark SQL performance

-- Connections are created via REST API in setup.sh:
-- 
-- Tokenization connection:
-- {
--   "name": "skyflow_tokenize_conn",
--   "connection_type": "HTTP", 
--   "options": {
--     "host": "${SKYFLOW_VAULT_URL}",
--     "port": "443",
--     "base_path": "/v1/vaults/${SKYFLOW_VAULT_ID}/${SKYFLOW_TABLE}",
--     "bearer_token": "{{secrets.skyflow-secrets.skyflow_pat_token}}"
--   }
-- }
--
-- Detokenization connection:
-- {
--   "name": "skyflow_detokenize_conn",
--   "connection_type": "HTTP", 
--   "options": {
--     "host": "${SKYFLOW_VAULT_URL}",
--     "port": "443",
--     "base_path": "/v1/vaults/${SKYFLOW_VAULT_ID}",
--     "bearer_token": "{{secrets.skyflow-secrets.skyflow_pat_token}}"
--   }
-- }

-- Core detokenization function with configurable redaction level
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_uc_detokenize(token STRING, redaction_level STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC
RETURN
  CASE 
    -- Handle null/empty tokens
    WHEN token IS NULL OR trim(token) = '' THEN token
    ELSE
      COALESCE(
        -- Extract detokenized value from Skyflow API response via UC connection
        get_json_object(
          get_json_object(
            http_request(
              conn => 'skyflow_conn',
              method => 'POST', 
              path => '${SKYFLOW_VAULT_ID}/detokenize',
              headers => map(
                'Content-Type', 'application/json',
                'Accept', 'application/json'
              ),
              json => concat(
                '{"detokenizationParameters":[{"token":"',
                token,
                '","redaction":"',
                redaction_level,
                '"}]}'
              )
            ).text,
            '$.records[0]'
          ),
          '$.value'
        ),
        -- Fallback to token if API call fails
        token
      )
  END;

-- Multi-level conditional detokenization function with role-based redaction
-- Supports PLAIN_TEXT, MASKED, and token-only based on user group membership
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_conditional_detokenize(token STRING)
RETURNS STRING
LANGUAGE SQL  
DETERMINISTIC
RETURN
  CASE
    -- Auditors get plain text (full detokenization)
    WHEN is_account_group_member('auditor') OR is_member('auditor') THEN 
      ${PREFIX}_skyflow_uc_detokenize(token, 'PLAIN_TEXT')
    -- Customer service gets masked data (partial redaction)
    WHEN is_account_group_member('customer_service') OR is_member('customer_service') THEN 
      ${PREFIX}_skyflow_uc_detokenize(token, 'MASKED')
    -- Marketing and all other users get tokens without API overhead
    ELSE token
  END;

-- Convenience function for column masks (uses conditional logic)
CREATE OR REPLACE FUNCTION ${PREFIX}_skyflow_mask_detokenize(token STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC  
RETURN ${PREFIX}_skyflow_conditional_detokenize(token);