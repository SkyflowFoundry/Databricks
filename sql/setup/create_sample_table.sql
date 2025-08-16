CREATE TABLE IF NOT EXISTS ${PREFIX}_customer_data (
    customer_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    date_of_birth STRING,
    signup_date TIMESTAMP,
    last_login TIMESTAMP,
    total_purchases INT,
    total_spent DOUBLE,
    loyalty_status STRING,
    preferred_language STRING,
    consent_marketing BOOLEAN,
    consent_data_sharing BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

INSERT INTO ${PREFIX}_customer_data (
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    address,
    date_of_birth,
    signup_date,
    last_login,
    total_purchases,
    total_spent,
    loyalty_status,
    preferred_language,
    consent_marketing,
    consent_data_sharing,
    created_at,
    updated_at
)
WITH numbered_rows AS (
  SELECT 
    posexplode(array_repeat(1, 50)) AS (id, _)
),
base_data AS (
  SELECT
    id + 1 AS id,  -- Ensure IDs start from 1
    CASE MOD(id, 4)
      WHEN 0 THEN 'Jonathan'
      WHEN 1 THEN 'Jessica'
      WHEN 2 THEN 'Michael'
      WHEN 3 THEN 'Stephanie'
    END AS first_name,
    CASE MOD(id, 4)
      WHEN 0 THEN 'Anderson'
      WHEN 1 THEN 'Williams'
      WHEN 2 THEN 'Johnson'
      WHEN 3 THEN 'Rodgers'
    END AS last_name,
    CASE MOD(id, 10)
      WHEN 0 THEN 'London, England, SW1A 1AA'
      WHEN 1 THEN 'Paris, France, 75001'
      WHEN 2 THEN 'Berlin, Germany, 10115'
      WHEN 3 THEN 'Tokyo, Japan, 100-0001'
      WHEN 4 THEN 'Sydney, Australia, 2000'
      WHEN 5 THEN 'Toronto, Canada, M5H 2N2'
      WHEN 6 THEN 'Singapore, 238859'
      WHEN 7 THEN 'Dubai, UAE, 12345'
      WHEN 8 THEN 'São Paulo, Brazil, 01310-000'
      WHEN 9 THEN 'Mumbai, India, 400001'
    END AS city
  FROM numbered_rows
)
SELECT
  CONCAT('CUST', LPAD(CAST(id AS STRING), 5, '0')) AS customer_id,
  -- Real PII data that will be tokenized
  first_name AS first_name,
  last_name AS last_name,
  CONCAT(LOWER(first_name), '.', LOWER(last_name), '@example.com') AS email,
  CASE MOD(id, 10)
    WHEN 0 THEN '+1-555-0100'
    WHEN 1 THEN '+1-555-0101' 
    WHEN 2 THEN '+1-555-0102'
    WHEN 3 THEN '+1-555-0103'
    WHEN 4 THEN '+1-555-0104'
    WHEN 5 THEN '+1-555-0105'
    WHEN 6 THEN '+1-555-0106'
    WHEN 7 THEN '+1-555-0107'
    WHEN 8 THEN '+1-555-0108'
    WHEN 9 THEN '+1-555-0109'
  END AS phone_number,
  city AS address,
  CASE MOD(id, 10)
    WHEN 0 THEN '1985-03-15'
    WHEN 1 THEN '1990-07-22'
    WHEN 2 THEN '1988-11-08'
    WHEN 3 THEN '1992-01-30'
    WHEN 4 THEN '1987-09-14'
    WHEN 5 THEN '1991-05-03'
    WHEN 6 THEN '1989-12-18'
    WHEN 7 THEN '1993-04-25'
    WHEN 8 THEN '1986-08-11'
    WHEN 9 THEN '1994-06-07'
  END AS date_of_birth,
  DATE_ADD('2018-01-01', id - 1) AS signup_date,
  DATE_ADD('2023-01-01', id - 1) AS last_login,
  id * 5 AS total_purchases,
  CAST(id * 50.00 AS DOUBLE) AS total_spent,
  CASE MOD(id, 4)
    WHEN 0 THEN 'Silver'
    WHEN 1 THEN 'Gold'
    WHEN 2 THEN 'Platinum'
    WHEN 3 THEN 'Diamond'
  END AS loyalty_status,
  CASE MOD(id, 2) WHEN 0 THEN 'English' ELSE 'Spanish' END AS preferred_language,
  CASE MOD(id, 2) WHEN 0 THEN TRUE ELSE FALSE END AS consent_marketing,
  CASE MOD(id, 3) WHEN 0 THEN TRUE ELSE FALSE END AS consent_data_sharing,
  current_timestamp() AS created_at,
  current_timestamp() AS updated_at
FROM base_data;