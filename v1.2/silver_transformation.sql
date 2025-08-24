-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer Transformation
-- MAGIC Creates silver_edge_draft table with adviser code enrichment

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration and Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Load configuration
-- MAGIC import json
-- MAGIC with open('/Workspace/Shared/config/pipeline_config.json', 'r') as f:
-- MAGIC     config = json.load(f)
-- MAGIC 
-- MAGIC catalog = config['target']['catalog']
-- MAGIC schema = config['target']['schema']
-- MAGIC bronze_table = f"{catalog}.{schema}.{config['target']['bronze_table']}"
-- MAGIC silver_table = f"{catalog}.{schema}.{config['target']['silver_table']}"
-- MAGIC org_table = f"{catalog}.{schema}.{config['reference_tables']['organization_table']}"
-- MAGIC dynamic_table = f"{catalog}.{schema}.{config['reference_tables']['dynamic_mapping_table']}"
-- MAGIC 
-- MAGIC # Create SQL variables
-- MAGIC spark.sql(f"SET bronze_table = '{bronze_table}'")
-- MAGIC spark.sql(f"SET silver_table = '{silver_table}'") 
-- MAGIC spark.sql(f"SET org_table = '{org_table}'")
-- MAGIC spark.sql(f"SET dynamic_table = '{dynamic_table}'")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Helper Functions for Adviser Name Matching

-- COMMAND ----------

CREATE OR REPLACE FUNCTION clean_adviser_name(name STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import re
if not name:
    return ""

# Remove special characters and extra spaces  
cleaned = re.sub(r'[*\-_]+', '', name)
cleaned = re.sub(r'\s+', ' ', cleaned).strip()
cleaned = re.sub(r'_inactive$', '', cleaned, flags=re.IGNORECASE)

return cleaned.lower()
$$;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adviser Name Standardization

-- COMMAND ----------

-- Create standardized adviser lookup
CREATE OR REPLACE TEMPORARY VIEW adviser_standardization AS
WITH organization_clean AS (
  SELECT 
    advisorname,
    advisorcode,
    companyname,
    clean_adviser_name(advisorname) as clean_advisor_name,
    ROW_NUMBER() OVER (
      PARTITION BY advisorname, companyname 
      ORDER BY advisorcode DESC
    ) as rn
  FROM ${org_table}
  WHERE advisorname IS NOT NULL 
    AND advisorcode IS NOT NULL
),
latest_advisor_codes AS (
  SELECT 
    advisorname,
    advisorcode,
    companyname,
    clean_advisor_name
  FROM organization_clean 
  WHERE rn = 1
),
bronze_clean AS (
  SELECT 
    *,
    clean_adviser_name(adviser) as clean_adviser_name
  FROM ${bronze_table}
),
adviser_matches AS (
  SELECT DISTINCT
    b.adviser as original_adviser,
    b.practice as original_practice,
    b.clean_adviser_name,
    o.advisorname,
    o.advisorcode,
    o.companyname,
    CASE 
      -- Exact match
      WHEN b.clean_adviser_name = o.clean_advisor_name THEN 100
      -- Bronze contains organization name
      WHEN b.clean_adviser_name LIKE CONCAT('%', o.clean_advisor_name, '%') THEN 90
      -- Organization contains bronze name  
      WHEN o.clean_advisor_name LIKE CONCAT('%', b.clean_adviser_name, '%') THEN 80
      ELSE 0
    END as match_score
  FROM bronze_clean b
  CROSS JOIN latest_advisor_codes o
  WHERE (b.clean_adviser_name = o.clean_advisor_name
         OR b.clean_adviser_name LIKE CONCAT('%', o.clean_advisor_name, '%')
         OR o.clean_advisor_name LIKE CONCAT('%', b.clean_adviser_name, '%'))
    AND LOWER(b.practice) = LOWER(o.companyname)
),
best_matches AS (
  SELECT 
    original_adviser,
    original_practice,
    advisorcode,
    advisorname,
    ROW_NUMBER() OVER (
      PARTITION BY original_adviser, original_practice 
      ORDER BY match_score DESC, advisorcode DESC
    ) as match_rank
  FROM adviser_matches
)
SELECT 
  original_adviser,
  original_practice, 
  advisorcode,
  advisorname
FROM best_matches 
WHERE match_rank = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Table Creation

-- COMMAND ----------

-- Create or replace silver table
CREATE OR REPLACE TABLE ${silver_table}
USING DELTA
PARTITIONED BY (download_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH bronze_with_codes AS (
  SELECT 
    b.*,
    COALESCE(a.advisorcode, 'UNKNOWN') as advisor_code,
    COALESCE(a.advisorname, b.adviser) as standardized_adviser_name
  FROM ${bronze_table} b
  LEFT JOIN adviser_standardization a 
    ON b.adviser = a.original_adviser 
    AND b.practice = a.original_practice
),
final_with_dynamic AS (
  SELECT 
    bwc.*,
    dm.dynamic_id,
    dm.business_date
  FROM bronze_with_codes bwc
  LEFT JOIN ${dynamic_table} dm
    ON bwc.advisor_code = dm.advisercode
)
SELECT * FROM final_with_dynamic;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Data Quality Checks

-- COMMAND ----------

-- Silver table statistics
SELECT 
  'Total Records' as metric,
  COUNT(*) as value
FROM ${silver_table}

UNION ALL

SELECT 
  'Records with Advisor Codes' as metric,
  COUNT(*) as value
FROM ${silver_table}
WHERE advisor_code != 'UNKNOWN'

UNION ALL

SELECT 
  'Records with Dynamic Mapping' as metric, 
  COUNT(*) as value
FROM ${silver_table}
WHERE dynamic_id IS NOT NULL

UNION ALL

SELECT 
  'Unique Practices' as metric,
  COUNT(DISTINCT practice) as value
FROM ${silver_table}

UNION ALL

SELECT 
  'Unique Advisers' as metric,
  COUNT(DISTINCT adviser) as value
FROM ${silver_table}

UNION ALL

SELECT 
  'Date Range (Days)' as metric,
  DATEDIFF(MAX(download_date), MIN(download_date)) as value
FROM ${silver_table};

-- COMMAND ----------

-- Adviser matching success rate
SELECT 
  'Matched' as status,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM ${silver_table}
WHERE advisor_code != 'UNKNOWN'

UNION ALL

SELECT 
  'Unmatched' as status,
  COUNT(*) as count, 
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM ${silver_table}
WHERE advisor_code = 'UNKNOWN';

-- COMMAND ----------

-- Sample of unmatched advisers
SELECT DISTINCT 
  adviser,
  practice,
  COUNT(*) as record_count
FROM ${silver_table}
WHERE advisor_code = 'UNKNOWN'
GROUP BY adviser, practice
ORDER BY record_count DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental Update Logic (for scheduled runs)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # For incremental updates, only process new data
-- MAGIC # This would be called in incremental mode
-- MAGIC 
-- MAGIC def incremental_silver_update():
-- MAGIC     """Update silver table incrementally"""
-- MAGIC     
-- MAGIC     # Get the latest ingestion datetime from silver table
-- MAGIC     latest_silver = spark.sql(f"""
-- MAGIC         SELECT COALESCE(MAX(ingestion_datetime), '1900-01-01') as max_ingestion
-- MAGIC         FROM {silver_table}
-- MAGIC     """).collect()[0]['max_ingestion']
-- MAGIC     
-- MAGIC     # Create temp view with only new bronze records
-- MAGIC     spark.sql(f"""
-- MAGIC         CREATE OR REPLACE TEMPORARY VIEW new_bronze_records AS
-- MAGIC         SELECT * FROM {bronze_table}
-- MAGIC         WHERE ingestion_datetime > '{latest_silver}'
-- MAGIC     """)
-- MAGIC     
-- MAGIC     # Only proceed if there are new records
-- MAGIC     new_count = spark.sql("SELECT COUNT(*) as cnt FROM new_bronze_records").collect()[0]['cnt']
-- MAGIC     
-- MAGIC     if new_count > 0:
-- MAGIC         print(f"Processing {new_count} new records")
-- MAGIC         
-- MAGIC         # Run the transformation for new records only
-- MAGIC         spark.sql(f"""
-- MAGIC             INSERT INTO {silver_table}
-- MAGIC             WITH bronze_with_codes AS (
-- MAGIC               SELECT 
-- MAGIC                 b.*,
-- MAGIC                 COALESCE(a.advisorcode, 'UNKNOWN') as advisor_code,
-- MAGIC                 COALESCE(a.advisorname, b.adviser) as standardized_adviser_name
-- MAGIC               FROM new_bronze_records b
-- MAGIC               LEFT JOIN adviser_standardization a 
-- MAGIC                 ON b.adviser = a.original_adviser 
-- MAGIC                 AND b.practice = a.original_practice
-- MAGIC             ),
-- MAGIC             final_with_dynamic AS (
-- MAGIC               SELECT 
-- MAGIC                 bwc.*,
-- MAGIC                 dm.dynamic_id,
-- MAGIC                 dm.business_date
-- MAGIC               FROM bronze_with_codes bwc
-- MAGIC               LEFT JOIN {dynamic_table} dm
-- MAGIC                 ON bwc.advisor_code = dm.advisercode
-- MAGIC             )
-- MAGIC             SELECT * FROM final_with_dynamic
-- MAGIC         """)
-- MAGIC         
-- MAGIC         print("Incremental update completed")
-- MAGIC     else:
-- MAGIC         print("No new records to process")
-- MAGIC 
-- MAGIC # Uncomment to run incremental update
-- MAGIC # incremental_silver_update()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup

-- COMMAND ----------

-- Clean up temporary views and functions
DROP VIEW IF EXISTS adviser_standardization;
DROP FUNCTION IF EXISTS clean_adviser_name;