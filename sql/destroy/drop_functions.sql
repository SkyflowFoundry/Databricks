-- Drop Unity Catalog detokenization functions during cleanup
-- Note: Must be run with proper catalog context
DROP FUNCTION IF EXISTS ${PREFIX}_catalog.default.${PREFIX}_skyflow_uc_detokenize;
DROP FUNCTION IF EXISTS ${PREFIX}_catalog.default.${PREFIX}_skyflow_conditional_detokenize;
DROP FUNCTION IF EXISTS ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;