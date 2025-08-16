-- Create dedicated catalog and schema for this instance
CREATE CATALOG IF NOT EXISTS ${PREFIX}_catalog;
CREATE SCHEMA IF NOT EXISTS ${PREFIX}_catalog.default;