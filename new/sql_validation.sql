-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL Validation and Testing Queries
-- MAGIC This notebook contains SQL queries to validate and test the transformation logic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Data Quality Validation Queries

-- COMMAND ----------

-- Check bronze data availability
SELECT 
    download_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT adviser) as unique_advisers,
    COUNT(DISTINCT practice) as unique_practices,
    COUNT(DISTINCT source_filename) as unique_files
FROM main.bronze_schema.bronze_edge_draft
WHERE download_date >= date_sub(current_date(), 7)
GROUP BY download_date
ORDER BY download_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Performance and Processing Time Analysis

-- COMMAND ----------

-- Check processing times and performance metrics
SELECT 
    DATE(silver_processed_datetime) as processing_date,
    COUNT(*) as records_processed,
    COUNT(DISTINCT download_date) as unique_dates_processed,
    MIN(silver_processed_datetime) as batch_start,
    MAX(silver_processed_datetime) as batch_end,
    ROUND((UNIX_TIMESTAMP(MAX(silver_processed_datetime)) - UNIX_TIMESTAMP(MIN(silver_processed_datetime))) / 60, 2) as processing_minutes
FROM main.silver_schema.silver_edge_draft
WHERE silver_processed_datetime >= date_sub(current_timestamp(), 7)
GROUP BY DATE(silver_processed_datetime)
ORDER BY processing_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Adviser Code Distribution Analysis

-- COMMAND ----------

-- Analyze adviser code distribution
SELECT 
    matched_adviser_code,
    COUNT(*) as record_count,
    COUNT(DISTINCT adviser) as unique_bronze_advisers,
    COUNT(DISTINCT practice) as unique_practices,
    MIN(date_submitted) as earliest_submission,
    MAX(date_submitted) as latest_submission
FROM main.silver_schema.silver_edge_draft
WHERE matched_adviser_code IS NOT NULL
    AND download_date >= date_sub(current_date(), 30)
GROUP BY matched_adviser_code
ORDER BY record_count DESC
LIMIT 20;

-- COMMAND ----------

-- Check for potential duplicate adviser codes (same adviser mapping to multiple codes)
WITH adviser_code_mapping AS (
    SELECT 
        adviser,
        practice,
        matched_adviser_code,
        COUNT(*) as record_count
    FROM main.silver_schema.silver_edge_draft
    WHERE matched_adviser_code IS NOT NULL
        AND download_date >= date_sub(current_date(), 30)
    GROUP BY adviser, practice, matched_adviser_code
),
adviser_multiple_codes AS (
    SELECT 
        adviser,
        practice,
        COUNT(DISTINCT matched_adviser_code) as code_count,
        COLLECT_LIST(matched_adviser_code) as all_codes
    FROM adviser_code_mapping
    GROUP BY adviser, practice
    HAVING COUNT(DISTINCT matched_adviser_code) > 1
)
SELECT 
    adviser,
    practice,
    code_count,
    all_codes
FROM adviser_multiple_codes
ORDER BY code_count DESC, adviser;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Data Completeness and Quality Checks

-- COMMAND ----------

-- Check for null or empty values in key fields
SELECT 
    'date_submitted' as field_name,
    COUNT(*) as total_records,
    COUNT(date_submitted) as non_null_count,
    COUNT(*) - COUNT(date_submitted) as null_count,
    ROUND((COUNT(date_submitted) * 100.0 / COUNT(*)), 2) as completeness_percent
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 7)

UNION ALL

SELECT 
    'account_name' as field_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN account_name IS NOT NULL AND TRIM(account_name) != '' THEN 1 END) as non_null_count,
    COUNT(*) - COUNT(CASE WHEN account_name IS NOT NULL AND TRIM(account_name) != '' THEN 1 END) as null_count,
    ROUND((COUNT(CASE WHEN account_name IS NOT NULL AND TRIM(account_name) != '' THEN 1 END) * 100.0 / COUNT(*)), 2) as completeness_percent
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 7)

UNION ALL

SELECT 
    'adviser' as field_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN adviser IS NOT NULL AND TRIM(adviser) != '' THEN 1 END) as non_null_count,
    COUNT(*) - COUNT(CASE WHEN adviser IS NOT NULL AND TRIM(adviser) != '' THEN 1 END) as null_count,
    ROUND((COUNT(CASE WHEN adviser IS NOT NULL AND TRIM(adviser) != '' THEN 1 END) * 100.0 / COUNT(*)), 2) as completeness_percent
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 7)

UNION ALL

SELECT 
    'practice' as field_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN practice IS NOT NULL AND TRIM(practice) != '' THEN 1 END) as non_null_count,
    COUNT(*) - COUNT(CASE WHEN practice IS NOT NULL AND TRIM(practice) != '' THEN 1 END) as null_count,
    ROUND((COUNT(CASE WHEN practice IS NOT NULL AND TRIM(practice) != '' THEN 1 END) * 100.0 / COUNT(*)), 2) as completeness_percent
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 7);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 9. Historical Trend Analysis

-- COMMAND ----------

-- Track match rate trends over time
SELECT 
    download_date,
    COUNT(*) as total_records,
    COUNT(matched_adviser_code) as matched_records,
    ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2) as match_rate_percent,
    LAG(ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2)) OVER (ORDER BY download_date) as previous_match_rate,
    ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2) - 
        LAG(ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2)) OVER (ORDER BY download_date) as match_rate_change
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 30)
GROUP BY download_date
ORDER BY download_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 10. Error and Alert Detection Queries

-- COMMAND ----------

-- Check for potential data quality issues that should trigger alerts
WITH quality_checks AS (
    SELECT 
        download_date,
        COUNT(*) as total_records,
        COUNT(matched_adviser_code) as matched_records,
        ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2) as match_rate,
        COUNT(CASE WHEN data_quality_flags IS NOT NULL THEN 1 END) as flagged_records,
        COUNT(CASE WHEN adviser IS NULL OR TRIM(adviser) = '' THEN 1 END) as missing_adviser,
        COUNT(CASE WHEN practice IS NULL OR TRIM(practice) = '' THEN 1 END) as missing_practice,
        COUNT(CASE WHEN account_name IS NULL OR TRIM(account_name) = '' THEN 1 END) as missing_account
    FROM main.silver_schema.silver_edge_draft
    WHERE download_date >= date_sub(current_date(), 7)
    GROUP BY download_date
)
SELECT 
    download_date,
    total_records,
    match_rate,
    CASE 
        WHEN match_rate < 50 THEN '🚨 CRITICAL: Very Low Match Rate'
        WHEN match_rate < 70 THEN '⚠️ WARNING: Low Match Rate'
        WHEN match_rate >= 90 THEN '✅ EXCELLENT: High Match Rate'
        ELSE '👍 GOOD: Normal Match Rate'
    END as match_rate_status,
    CASE 
        WHEN missing_adviser > 0 THEN '⚠️ Missing Adviser Data'
        WHEN missing_practice > 0 THEN '⚠️ Missing Practice Data'  
        WHEN missing_account > 0 THEN '⚠️ Missing Account Data'
        ELSE '✅ Complete Data'
    END as data_completeness_status,
    flagged_records
FROM quality_checks
ORDER BY download_date DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 11. Sample Data Validation

-- COMMAND ----------

-- Show sample of matched vs unmatched records for manual validation
WITH sample_data AS (
    SELECT 
        adviser,
        practice,
        matched_adviser_code,
        data_quality_flags,
        download_date,
        account_name,
        ROW_NUMBER() OVER (PARTITION BY (matched_adviser_code IS NULL) ORDER BY RANDOM()) as rn
    FROM main.silver_schema.silver_edge_draft
    WHERE download_date >= date_sub(current_date(), 3)
)
SELECT 
    CASE WHEN matched_adviser_code IS NOT NULL THEN '✅ MATCHED' ELSE '❌ UNMATCHED' END as status,
    adviser,
    practice,
    matched_adviser_code,
    data_quality_flags,
    download_date,
    account_name
FROM sample_data
WHERE rn <= 5
ORDER BY status DESC, adviser;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 12. Organization Table Analysis for Matching Improvement

-- COMMAND ----------

-- Analyze organization table for potential matching improvements
SELECT 
    company_name,
    COUNT(*) as adviser_count,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_adviser_count,
    COLLECT_LIST(CASE WHEN is_active = true THEN adviser_name END) as active_advisers
FROM main.reference_schema.organization
GROUP BY company_name
HAVING COUNT(CASE WHEN is_active = true THEN 1 END) > 0
ORDER BY active_adviser_count DESC, company_name;

-- COMMAND ----------

-- Find potential duplicate advisers in organization table
WITH org_analysis AS (
    SELECT 
        adviser_name,
        company_name,
        adviser_code,
        is_active,
        created_date,
        ROW_NUMBER() OVER (PARTITION BY TRIM(LOWER(adviser_name)), TRIM(LOWER(company_name)) ORDER BY created_date DESC) as rn
    FROM main.reference_schema.organization
    WHERE is_active = true
)
SELECT 
    adviser_name,
    company_name,
    COUNT(*) as duplicate_count,
    COLLECT_LIST(adviser_code) as all_codes,
    COLLECT_LIST(created_date) as all_dates
FROM org_analysis
WHERE rn = 1
GROUP BY adviser_name, company_name
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC; BY download_date DESC;

-- COMMAND ----------

-- Check organization reference data
SELECT 
    COUNT(*) as total_advisers,
    COUNT(CASE WHEN is_active = true THEN 1 END) as active_advisers,
    COUNT(DISTINCT company_name) as unique_companies,
    MIN(created_date) as earliest_record,
    MAX(created_date) as latest_record
FROM main.reference_schema.organization;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Matching Logic Test Queries

-- COMMAND ----------

-- Test exact matching logic
WITH exact_match_test AS (
    SELECT 
        b.adviser as bronze_adviser,
        b.practice as bronze_practice,
        o.adviser_name as org_adviser,
        o.company_name as org_company,
        o.adviser_code,
        'EXACT_MATCH' as match_type
    FROM main.bronze_schema.bronze_edge_draft b
    INNER JOIN main.reference_schema.organization o 
        ON LOWER(TRIM(b.adviser)) = LOWER(TRIM(o.adviser_name))
        AND LOWER(TRIM(b.practice)) = LOWER(TRIM(o.company_name))
        AND o.is_active = true
    WHERE b.download_date >= date_sub(current_date(), 7)
)
SELECT 
    COUNT(*) as exact_matches,
    COUNT(DISTINCT bronze_adviser) as unique_bronze_advisers,
    COUNT(DISTINCT org_adviser) as unique_org_advisers
FROM exact_match_test;

-- COMMAND ----------

-- Test fuzzy matching - bronze contains org
WITH bronze_contains_org_test AS (
    SELECT 
        b.adviser as bronze_adviser,
        b.practice as bronze_practice,
        o.adviser_name as org_adviser,
        o.company_name as org_company,
        o.adviser_code,
        'BRONZE_CONTAINS_ORG' as match_type
    FROM main.bronze_schema.bronze_edge_draft b
    INNER JOIN main.reference_schema.organization o 
        ON LOWER(TRIM(b.adviser)) LIKE CONCAT('%', LOWER(TRIM(o.adviser_name)), '%')
        AND LOWER(TRIM(b.practice)) = LOWER(TRIM(o.company_name))
        AND LENGTH(TRIM(o.adviser_name)) >= 3
        AND o.is_active = true
    WHERE b.download_date >= date_sub(current_date(), 7)
    -- Exclude exact matches
    AND NOT (LOWER(TRIM(b.adviser)) = LOWER(TRIM(o.adviser_name)))
)
SELECT 
    COUNT(*) as fuzzy_matches_bronze_contains,
    COUNT(DISTINCT bronze_adviser) as unique_bronze_advisers,
    COUNT(DISTINCT org_adviser) as unique_org_advisers
FROM bronze_contains_org_test;

-- COMMAND ----------

-- Test fuzzy matching - org contains bronze
WITH org_contains_bronze_test AS (
    SELECT 
        b.adviser as bronze_adviser,
        b.practice as bronze_practice,
        o.adviser_name as org_adviser,
        o.company_name as org_company,
        o.adviser_code,
        'ORG_CONTAINS_BRONZE' as match_type
    FROM main.bronze_schema.bronze_edge_draft b
    INNER JOIN main.reference_schema.organization o 
        ON LOWER(TRIM(o.adviser_name)) LIKE CONCAT('%', LOWER(TRIM(b.adviser)), '%')
        AND LOWER(TRIM(b.practice)) = LOWER(TRIM(o.company_name))
        AND LENGTH(TRIM(b.adviser)) >= 3
        AND o.is_active = true
    WHERE b.download_date >= date_sub(current_date(), 7)
    -- Exclude exact matches and bronze_contains_org matches
    AND NOT (LOWER(TRIM(b.adviser)) = LOWER(TRIM(o.adviser_name)))
    AND NOT (LOWER(TRIM(b.adviser)) LIKE CONCAT('%', LOWER(TRIM(o.adviser_name)), '%'))
)
SELECT 
    COUNT(*) as fuzzy_matches_org_contains,
    COUNT(DISTINCT bronze_adviser) as unique_bronze_advisers,
    COUNT(DISTINCT org_adviser) as unique_org_advisers
FROM org_contains_bronze_test;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Comprehensive Match Analysis

-- COMMAND ----------

-- Complete matching analysis with all strategies
WITH bronze_recent AS (
    SELECT DISTINCT
        TRIM(adviser) as adviser,
        TRIM(practice) as practice
    FROM main.bronze_schema.bronze_edge_draft
    WHERE download_date >= date_sub(current_date(), 7)
),

org_active AS (
    SELECT 
        TRIM(adviser_name) as adviser_name,
        TRIM(company_name) as company_name,
        adviser_code,
        created_date
    FROM main.reference_schema.organization
    WHERE is_active = true
),

match_analysis AS (
    SELECT 
        b.adviser as bronze_adviser,
        b.practice as bronze_practice,
        o.adviser_name as org_adviser,
        o.company_name as org_company,
        o.adviser_code,
        o.created_date,
        CASE 
            WHEN LOWER(b.adviser) = LOWER(o.adviser_name) 
                 AND LOWER(b.practice) = LOWER(o.company_name) 
            THEN 'EXACT_MATCH'
            WHEN LOWER(b.adviser) LIKE CONCAT('%', LOWER(o.adviser_name), '%')
                 AND LOWER(b.practice) = LOWER(o.company_name)
                 AND LENGTH(o.adviser_name) >= 3
            THEN 'BRONZE_CONTAINS_ORG'
            WHEN LOWER(o.adviser_name) LIKE CONCAT('%', LOWER(b.adviser), '%')
                 AND LOWER(b.practice) = LOWER(o.company_name)
                 AND LENGTH(b.adviser) >= 3
            THEN 'ORG_CONTAINS_BRONZE'
            ELSE 'NO_MATCH'
        END as match_type,
        CASE 
            WHEN LOWER(b.adviser) = LOWER(o.adviser_name) 
                 AND LOWER(b.practice) = LOWER(o.company_name) 
            THEN 1
            WHEN LOWER(b.adviser) LIKE CONCAT('%', LOWER(o.adviser_name), '%')
                 AND LOWER(b.practice) = LOWER(o.company_name)
                 AND LENGTH(o.adviser_name) >= 3
            THEN 2
            WHEN LOWER(o.adviser_name) LIKE CONCAT('%', LOWER(b.adviser), '%')
                 AND LOWER(b.practice) = LOWER(o.company_name)
                 AND LENGTH(b.adviser) >= 3
            THEN 3
            ELSE 4
        END as match_priority
    FROM bronze_recent b
    CROSS JOIN org_active o
),

best_matches AS (
    SELECT 
        bronze_adviser,
        bronze_practice,
        org_adviser,
        org_company,
        adviser_code,
        match_type,
        ROW_NUMBER() OVER (
            PARTITION BY bronze_adviser, bronze_practice 
            ORDER BY match_priority ASC, created_date DESC, adviser_code DESC
        ) as match_rank
    FROM match_analysis
    WHERE match_type != 'NO_MATCH'
)

SELECT 
    match_type,
    COUNT(*) as match_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM best_matches
WHERE match_rank = 1
GROUP BY match_type
ORDER BY match_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Unmatched Records Analysis

-- COMMAND ----------

-- Find unmatched advisers and their patterns
WITH bronze_recent AS (
    SELECT DISTINCT
        TRIM(adviser) as adviser,
        TRIM(practice) as practice,
        COUNT(*) as record_count
    FROM main.bronze_schema.bronze_edge_draft
    WHERE download_date >= date_sub(current_date(), 7)
    GROUP BY TRIM(adviser), TRIM(practice)
),

matched_advisers AS (
    SELECT DISTINCT
        b.adviser,
        b.practice
    FROM main.bronze_schema.bronze_edge_draft b
    INNER JOIN main.reference_schema.organization o 
        ON (
            (LOWER(TRIM(b.adviser)) = LOWER(TRIM(o.adviser_name))) OR
            (LOWER(TRIM(b.adviser)) LIKE CONCAT('%', LOWER(TRIM(o.adviser_name)), '%') AND LENGTH(TRIM(o.adviser_name)) >= 3) OR
            (LOWER(TRIM(o.adviser_name)) LIKE CONCAT('%', LOWER(TRIM(b.adviser)), '%') AND LENGTH(TRIM(b.adviser)) >= 3)
        )
        AND LOWER(TRIM(b.practice)) = LOWER(TRIM(o.company_name))
        AND o.is_active = true
    WHERE b.download_date >= date_sub(current_date(), 7)
),

unmatched AS (
    SELECT 
        b.adviser,
        b.practice,
        b.record_count,
        LENGTH(b.adviser) as adviser_name_length
    FROM bronze_recent b
    LEFT JOIN matched_advisers m 
        ON b.adviser = m.adviser AND b.practice = m.practice
    WHERE m.adviser IS NULL
)

SELECT 
    adviser,
    practice,
    record_count,
    adviser_name_length,
    CASE 
        WHEN adviser_name_length < 3 THEN 'Too Short'
        WHEN adviser LIKE '%&%' OR adviser LIKE '%,%' THEN 'Multiple Names'
        WHEN adviser RLIKE '[0-9]' THEN 'Contains Numbers'
        ELSE 'Other'
    END as unmatched_reason
FROM unmatched
ORDER BY record_count DESC, adviser;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Silver Table Validation Queries

-- COMMAND ----------

-- Check silver table data quality
SELECT 
    download_date,
    COUNT(*) as total_records,
    COUNT(matched_adviser_code) as matched_records,
    ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2) as match_rate_percent,
    COUNT(CASE WHEN data_quality_flags IS NOT NULL THEN 1 END) as flagged_records,
    COUNT(DISTINCT adviser) as unique_advisers,
    COUNT(DISTINCT matched_adviser_code) as unique_adviser_codes
FROM main.silver_schema.silver_edge_draft
WHERE download_date >= date_sub(current_date(), 7)
GROUP BY download_date
ORDER BY download_date DESC;

-- COMMAND ----------

-- Compare bronze vs silver record counts
WITH bronze_counts AS (
    SELECT 
        download_date,
        COUNT(*) as bronze_count
    FROM main.bronze_schema.bronze_edge_draft
    WHERE download_date >= date_sub(current_date(), 7)
    GROUP BY download_date
),
silver_counts AS (
    SELECT 
        download_date,
        COUNT(*) as silver_count
    FROM main.silver_schema.silver_edge_draft
    WHERE download_date >= date_sub(current_date(), 7)
    GROUP BY download_date
)
SELECT 
    COALESCE(b.download_date, s.download_date) as download_date,
    COALESCE(b.bronze_count, 0) as bronze_count,
    COALESCE(s.silver_count, 0) as silver_count,
    CASE 
        WHEN b.bronze_count = s.silver_count THEN '✓ Match'
        WHEN b.bronze_count > s.silver_count THEN '⚠ Silver Missing'
        WHEN b.bronze_count < s.silver_count THEN '⚠ Silver Extra'
        ELSE '✗ Error'
    END as status
FROM bronze_counts b
FULL OUTER JOIN silver_counts s ON b.download_date = s.download_date
ORDER