-- Drop sample customer data table during cleanup
-- Set catalog context
USE CATALOG ${PREFIX}_catalog;

DROP TABLE IF EXISTS ${PREFIX}_catalog.default.${PREFIX}_customer_data;