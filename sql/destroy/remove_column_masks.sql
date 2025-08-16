-- Remove column masks before dropping table during cleanup
-- Note: Only first_name should have a mask in current setup, but including all for safety
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN first_name DROP MASK;
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN last_name DROP MASK;
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN email DROP MASK;
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN phone_number DROP MASK;
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN address DROP MASK;
ALTER TABLE ${PREFIX}_customer_data ALTER COLUMN date_of_birth DROP MASK;