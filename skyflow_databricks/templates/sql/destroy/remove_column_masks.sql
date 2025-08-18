-- Remove column masks before dropping table during cleanup
-- Remove masks from all tokenized PII columns during cleanup

-- Set catalog context
USE CATALOG ${PREFIX}_catalog;

ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN first_name DROP MASK;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN last_name DROP MASK;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN email DROP MASK;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN phone_number DROP MASK;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN address DROP MASK;
ALTER TABLE ${PREFIX}_catalog.default.${PREFIX}_customer_data ALTER COLUMN date_of_birth DROP MASK;