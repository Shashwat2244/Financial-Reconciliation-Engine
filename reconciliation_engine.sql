USE DATABASE FINANCE_DB;
USE SCHEMA RECONCILIATION_SCHEMA;
USE WAREHOUSE COMPUTE_WH;

-- Create Storage Integration (mocked)
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/myrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://s3-fintech-recon-silver-ss/');

-- Create External Stage
CREATE OR REPLACE STAGE silver_stage
  URL = 's3://s3-fintech-recon-silver-ss/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (TYPE = PARQUET);

-- Create Tables from Parquet
CREATE OR REPLACE TABLE internal_orders AS
SELECT 
  $1:order_id::VARCHAR AS order_id,
  $1:user_id::VARCHAR AS user_id,
  $1:amount::NUMBER(10,2) AS amount,
  $1:timestamp_utc::TIMESTAMP AS timestamp_utc
FROM @silver_stage/silver_orders.parquet;

CREATE OR REPLACE TABLE gateway_settlements AS
SELECT 
  $1:settlement_id::VARCHAR AS settlement_id,
  $1:order_id::VARCHAR AS order_id,
  $1:settled_amount::NUMBER(10,2) AS settled_amount,
  $1:fee::NUMBER(10,2) AS fee,
  $1:timestamp_utc::TIMESTAMP AS timestamp_utc
FROM @silver_stage/silver_settlements.parquet;

-- Core Reconciliation Logic
CREATE OR REPLACE TABLE daily_reconciliation_breaks AS
SELECT 
    COALESCE(o.order_id, s.order_id) AS order_id,
    o.amount AS internal_amount,
    s.settled_amount AS gateway_amount,
    s.fee AS gateway_fee,
    CASE
        WHEN o.order_id IS NULL THEN 'Missing in Internal'
        WHEN s.order_id IS NULL THEN 'Missing in Gateway'
        WHEN o.amount != s.settled_amount THEN 'Amount Mismatch'
        ELSE 'Other Discrepancy'
    END as discrepancy_reason,
    CURRENT_TIMESTAMP() AS reconciled_at
FROM internal_orders o
FULL OUTER JOIN gateway_settlements s ON o.order_id = s.order_id
WHERE 
    o.order_id IS NULL 
    OR s.order_id IS NULL 
    OR o.amount != s.settled_amount;
