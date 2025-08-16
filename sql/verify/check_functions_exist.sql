-- Check if functions still exist (used for destroy verification)
-- These will error if functions don't exist, which is expected for verification
DESCRIBE FUNCTION ${PREFIX}_skyflow_uc_detokenize;
DESCRIBE FUNCTION ${PREFIX}_skyflow_mask_detokenize;