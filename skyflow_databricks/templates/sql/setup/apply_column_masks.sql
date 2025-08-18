-- Apply column masks to PII columns using Unity Catalog SQL-only functions  
-- Pure SQL performance with UC connections - zero Python UDF overhead
-- Role-based access: auditors see detokenized, customer_service sees masked, others see tokens

-- Set catalog context
USE CATALOG ${PREFIX}_catalog;

-- Apply masks to all tokenized PII columns
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN first_name SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN last_name SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN email SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN phone_number SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN address SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN date_of_birth SET MASK ${PREFIX}_catalog.default.${PREFIX}_skyflow_mask_detokenize;