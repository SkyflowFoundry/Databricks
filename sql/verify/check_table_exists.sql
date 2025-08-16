-- Check if table still exists (used for destroy verification)  
-- Will error if table doesn't exist, which is expected for verification
DESCRIBE TABLE ${PREFIX}_customer_data;