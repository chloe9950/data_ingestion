-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Tables for Edge Draft Pipeline
-- MAGIC This notebook sets up all required tables in Unity Catalog

-- COMMAND ----------

-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS main.bronze_schema;
CREATE SCHEMA IF NOT EXISTS main.silver_schema;
CREATE SCHEMA IF NOT EXISTS main.logs_schema;
CREATE SCHEMA IF NOT EXISTS main.reference_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Table - Raw ingested data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.bronze_schema.bronze_edge_draft (
    date_submitted TIMESTAMP COMMENT 'Submission date converted to AU timezone',
    account_name STRING COMMENT 'Account name from source',
    account_type STRING COMMENT 'Type of account',
    practice STRING COMMENT 'Practice name',
    adviser STRING COMMENT 'Adviser name',
    source_filename STRING COMMENT 'Source file name',
    ingestion_datetime TIMESTAMP COMMENT 'When record was ingested (AU time)',
    download_date DATE COMMENT 'Date extracted from filename'
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/main.db/bronze_schema/bronze_edge_draft'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Table - Transformed data with adviser codes

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.silver_schema.silver_edge_draft (
    date_submitted TIMESTAMP COMMENT 'Submission date converted to AU timezone',
    account_name STRING COMMENT 'Account name from source',
    account_type STRING COMMENT 'Type of account',
    practice STRING COMMENT 'Practice name',
    adviser STRING COMMENT 'Adviser name',
    matched_adviser_code STRING COMMENT 'Matched adviser code from organization table',
    source_filename STRING COMMENT 'Source file name',
    ingestion_datetime TIMESTAMP COMMENT 'When record was ingested (AU time)',
    download_date DATE COMMENT 'Date extracted from filename',
    silver_processed_datetime TIMESTAMP COMMENT 'When record was processed to silver',
    data_quality_flags STRING COMMENT 'Data quality flags'
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/main.db/silver_schema/silver_edge_draft'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion Logs Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.logs_schema.ingestion_logs (
    ingestion_id STRING COMMENT 'Unique ingestion identifier',
    source_file STRING COMMENT 'Source file processed',
    ingestion_datetime TIMESTAMP COMMENT 'Ingestion timestamp',
    records_processed BIGINT COMMENT 'Number of records processed',
    status STRING COMMENT 'Processing status (SUCCESS/FAILED)',
    error_message STRING COMMENT 'Error message if failed'
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/main.db/logs_schema/ingestion_logs'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Organization Reference Table
-- MAGIC This table should already exist in your environment. If not, create it with this structure:

-- COMMAND ----------

-- Example organization table structure (modify as needed)
CREATE TABLE IF NOT EXISTS main.reference_schema.organization (
    adviser_name STRING COMMENT 'Adviser name in organization system',
    company_name STRING COMMENT 'Company/practice name',
    adviser_code STRING COMMENT 'Unique adviser code',
    created_date TIMESTAMP COMMENT 'When the adviser record was created',
    updated_date TIMESTAMP COMMENT 'Last updated timestamp',
    is_active BOOLEAN COMMENT 'Whether the adviser is active'
)
USING DELTA
LOCATION 'dbfs:/user/hive/warehouse/main.db/reference_schema/organization'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Views for Easy Access

-- COMMAND ----------

-- Create view for latest ingestion logs
CREATE OR REPLACE VIEW main.logs_schema.v_latest_ingestion_logs AS
SELECT 
    ingestion_id,
    source_file,
    ingestion_datetime,
    records_processed,
    status,
    error_message,
    ROW_NUMBER() OVER (PARTITION BY source_file ORDER BY ingestion_datetime DESC) as rn
FROM main.logs_schema.ingestion_logs
WHERE rn = 1;

-- COMMAND ----------

-- Create view for data quality monitoring
CREATE OR REPLACE VIEW main.silver_schema.v_data_quality_summary AS
SELECT 
    download_date,
    COUNT(*) as total_records,
    COUNT(matched_adviser_code) as matched_records,
    ROUND((COUNT(matched_adviser_code) * 100.0 / COUNT(*)), 2) as match_rate_percent,
    COUNT(CASE WHEN data_quality_flags IS NOT NULL THEN 1 END) as records_with_flags
FROM main.silver_schema.silver_edge_draft
GROUP BY download_date
ORDER BY download_date DESC;

-- COMMAND ----------

-- Grant permissions (adjust as needed for your environment)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON main.bronze_schema.bronze_edge_draft TO `your_service_principal`;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON main.silver_schema.silver_edge_draft TO `your_service_principal`;
-- GRANT SELECT, INSERT ON main.logs_schema.ingestion_logs TO `your_service_principal`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Table Creation

-- COMMAND ----------

-- Verify all tables exist
SHOW TABLES IN main.bronze_schema;
SHOW TABLES IN main.silver_schema; 
SHOW TABLES IN main.logs_schema;
SHOW TABLES IN main.reference_schema;