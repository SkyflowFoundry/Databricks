-- Unity Catalog Connections Setup via SQL Functions
-- Complete pure SQL implementation with zero Python overhead
-- Uses single skyflow_conn connection with dynamic paths

-- Set catalog context for functions
USE CATALOG ${PREFIX}_catalog;

-- Connection is created via SQL in create_uc_connections.sql:
-- Single connection with base_path '/v1/vaults/${SKYFLOW_VAULT_ID}'
-- Tokenization uses path '/${SKYFLOW_TABLE}'
-- Detokenization uses path '/detokenize'

-- Core detokenization function with configurable redaction level
CREATE OR REPLACE FUNCTION ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize(token STRING, redaction_level STRING)
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
              path => '/detokenize',
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
CREATE OR REPLACE FUNCTION ${PREFIX}_catalog.default.${PREFIX}_skyflow_conditional_detokenize(token STRING)
RETURNS STRING
LANGUAGE SQL  
DETERMINISTIC
RETURN
  CASE
    -- Auditors get plain text (full detokenization)
    WHEN is_account_group_member('auditor') OR is_member('auditor') THEN 
      ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize(token, 'PLAIN_TEXT')
    -- Customer service gets masked data (partial redaction)
    WHEN is_account_group_member('customer_service') OR is_member('customer_service') THEN 
      ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize(token, 'MASKED')
    -- Marketing and all other users get tokens without API overhead
    ELSE token
  END;

-- Convenience function for column masks (uses conditional logic)
CREATE OR REPLACE FUNCTION ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize(token STRING)
RETURNS STRING
LANGUAGE SQL
DETERMINISTIC  
RETURN ${PREFIX}_catalog.default.${PREFIX}_skyflow_conditional_detokenize(token);