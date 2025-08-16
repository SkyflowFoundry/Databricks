-- Apply column masks to PII columns using Unity Catalog SQL-only functions  
-- Pure SQL performance with UC connections - zero Python UDF overhead
-- Only auditors get detokenized data, others see raw tokens (optimized for performance)
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN first_name SET MASK ${PREFIX}_skyflow_mask_detokenize;