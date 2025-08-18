-- Verify Unity Catalog detokenization functions exist
-- Set catalog context first
USE CATALOG ${PREFIX}_catalog;

DESCRIBE FUNCTION ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize;
DESCRIBE FUNCTION ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;