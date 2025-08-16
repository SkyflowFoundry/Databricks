-- Verify Unity Catalog detokenization functions exist
DESCRIBE FUNCTION ${PREFIX}_skyflow_uc_detokenize;
DESCRIBE FUNCTION ${PREFIX}_skyflow_mask_detokenize;