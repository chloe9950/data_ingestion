# Edge Draft Data Pipeline - Deployment Guide

## Overview
This production-ready data pipeline ingests CSV files from Azure Blob Storage, processes them through Bronze and Silver layers in Databricks, and provides comprehensive monitoring and alerting capabilities.

## Architecture
- **Bronze Layer**: Raw data ingestion from blob storage
- **Silver Layer**: Cleansed data with adviser code matching
- **Monitoring**: Schema validation, data quality checks, and alerting
- **Logging**: Comprehensive audit trail of all operations

## Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure Blob Storage access (read permissions)
- Service Principal with appropriate Databricks permissions
- Python libraries: `pytz` (should be pre-installed in Databricks)

## Deployment Steps

### 1. Setup Directory Structure
Create the following directory structure in your Databricks workspace:
```
/Workspace/edge_draft_pipeline/
├── config.json
├── utils.py
├── data_ingestion.py
├── silver_transformation.py
├── setup_tables.sql
├── monitoring_and_alerts.py
└── DEPLOYMENT_GUIDE.md
```

### 2. Configure Azure Blob Storage Access
You'll need to set up authentication to your Azure storage account. Choose one method:

#### Option A: Service Principal (Recommended for Production)
```python
# Add to your Databricks cluster spark config or init script
spark.conf.set("fs.azure.account.auth.type.yourstorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.yourstorageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.yourstorageaccount.dfs.core.windows.net", "<your-client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.yourstorageaccount.dfs.core.windows.net", "<your-client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.yourstorageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

#### Option B: Access Key
```python
spark.conf.set("fs.azure.account.key.yourstorageaccount.dfs.core.windows.net", "<your-access-key>")
```

### 3. Update Configuration File
Modify `config.json` with your specific values:
```json
{
  "pipeline_config": {
    "source": {
      "storage_account": "yourstorageaccount",
      "container": "yourcontainer",
      "folder_path": "path/to/your/files/",
      "file_pattern": "*.csv",
      "filename_regex": "^.*\\((\\d{1,2})_(\\d{1,2})_(\\d{4})\\)\\.csv$"
    },
    "destinations": {
      "bronze_table": "main.bronze_schema.bronze_edge_draft",
      "silver_table": "main.silver_schema.silver_edge_draft",
      "log_table": "main.logs_schema.ingestion_logs"
    },
    "reference_tables": {
      "organization_table": "main.reference_schema.organization"
    }
  }
}
```

### 4. Create Database Tables
1. Run the `setup_tables.sql` notebook to create all required tables
2. Populate the `organization` table with your reference data:
```sql
-- Example insert for organization table
INSERT INTO main.reference_schema.organization VALUES
('John Smith', 'ABC Financial', 'ADV001', '2024-01-01 00:00:00', '2024-01-01 00:00:00', true),
('Jane Doe', 'XYZ Advisors', 'ADV002', '2024-01-01 00:00:00', '2024-01-01 00:00:00', true);
```

### 5. Create Databricks Job
#### Method 1: Using Databricks UI
1. Go to Workflows → Jobs → Create Job
2. Configure two tasks:
   - **Task 1**: Bronze Ingestion (notebook: `data_ingestion.py`)
   - **Task 2**: Silver Transformation (notebook: `silver_transformation.py`, depends on Task 1)
3. Set up file arrival trigger:
   - Source: `abfss://yourcontainer@yourstorageaccount.dfs.core.windows.net/path/to/files/`
   - File pattern: `*.csv`

#### Method 2: Using Databricks CLI/API
Use the provided `databricks_job_config.json` template:
```bash
databricks jobs create --json-file databricks_job_config.json
```

### 6. Configure File Trigger
Since you only have read access to blob storage, you'll need to use Databricks' file arrival trigger:
1. In the job configuration, enable "File arrival" trigger
2. Set the path to your blob storage location
3. Configure file pattern to match your CSV files

### 7. Set Up Notifications
Configure email and webhook notifications in the job settings:
- On failure: Send alerts to operations team
- On success: Optional success notifications
- Schema change alerts: Will be logged and can trigger custom notifications

## Usage

### Running the Pipeline

#### Full Refresh
To perform a complete reload of data:
```python
# Set job parameter
{"full_refresh": "true"}
```

#### Incremental Load (Default)
Normal daily processing will run incrementally, only processing new dates:
```python
# Default parameter
{"full_refresh": "false"}
```

### Monitoring
Use the monitoring notebook to check pipeline health:
```python
%run ./monitoring_and_alerts

# Check pipeline health
monitor = PipelineMonitor("/Workspace/edge_draft_pipeline/config.json")
health = monitor.get_pipeline_health_summary()
display(health)
```

# Edge Draft Data Pipeline - Deployment Guide

## Overview
This production-ready data pipeline ingests CSV files from Azure Blob Storage, processes them through Bronze and Silver layers in Databricks using **SQL-based transformations**, and provides comprehensive monitoring and alerting capabilities.

## Architecture
- **Bronze Layer**: Raw data ingestion from blob storage
- **Silver Layer**: **SQL-based** cleansed data with adviser code matching using fuzzy logic
- **Monitoring**: Schema validation, data quality checks, and alerting
- **Logging**: Comprehensive audit trail of all operations

## Key Features of SQL-based Transformation
- **Pure SQL Logic**: All transformation logic written in SQL for better performance and maintainability
- **Advanced Fuzzy Matching**: Three-tier matching strategy (exact, contains, partial)
- **Conflict Resolution**: Prioritizes matches by accuracy and recency
- **Data Quality Flags**: Automatic flagging of unmatched records
- **Performance Optimized**: Uses CTEs and window functions for efficient processing

## Prerequisites
- Databricks workspace with Unity Catalog enabled
- Azure Blob Storage access (read permissions)
- Service Principal with appropriate Databricks permissions
- Python libraries: `pytz` (should be pre-installed in Databricks)

## Deployment Steps

### 1. Setup Directory Structure
Create the following directory structure in your Databricks workspace:
```
/Workspace/edge_draft_pipeline/
├── config.json
├── utils.py
├── data_ingestion.py
├── silver_transformation_sql.py  # ← SQL-based transformation
├── setup_tables.sql
├── sql_validation_queries.sql    # ← New validation queries
├── monitoring_and_alerts.py
└── DEPLOYMENT_GUIDE.md
```

### 2. Configure Azure Blob Storage Access
You'll need to set up authentication to your Azure storage account. Choose one method:

#### Option A: Service Principal (Recommended for Production)
```python
# Add to your Databricks cluster spark config or init script
spark.conf.set("fs.azure.account.auth.type.yourstorageaccount.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.yourstorageaccount.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.yourstorageaccount.dfs.core.windows.net", "<your-client-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.yourstorageaccount.dfs.core.windows.net", "<your-client-secret>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.yourstorageaccount.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
```

#### Option B: Access Key
```python
spark.conf.set("fs.azure.account.key.yourstorageaccount.dfs.core.windows.net", "<your-access-key>")
```

### 3. Update Configuration File
Modify `config.json` with your specific values:
```json
{
  "pipeline_config": {
    "source": {
      "storage_account": "yourstorageaccount",
      "container": "yourcontainer",
      "folder_path": "path/to/your/files/",
      "file_pattern": "*.csv",
      "filename_regex": "^.*\\((\\d{1,2})_(\\d{1,2})_(\\d{4})\\)\\.csv$"
    },
    "destinations": {
      "bronze_table": "main.bronze_schema.bronze_edge_draft",
      "silver_table": "main.silver_schema.silver_edge_draft",
      "log_table": "main.logs_schema.ingestion_logs"
    },
    "reference_tables": {
      "organization_table": "main.reference_schema.organization"
    }
  }
}
```

### 4. Create Database Tables
1. Run the `setup_tables.sql` notebook to create all required tables
2. Populate the `organization` table with your reference data:
```sql
-- Example insert for organization table
INSERT INTO main.reference_schema.organization VALUES
('John Smith', 'ABC Financial', 'ADV001', '2024-01-01 00:00:00', '2024-01-01 00:00:00', true),
('Jane Doe', 'XYZ Advisors', 'ADV002', '2024-01-01 00:00:00', '2024-01-01 00:00:00', true);
```

### 5. Create Databricks Job
#### Method 1: Using Databricks UI
1. Go to Workflows → Jobs → Create Job
2. Configure two tasks:
   - **Task 1**: Bronze Ingestion (notebook: `data_ingestion.py`)
   - **Task 2**: Silver Transformation (notebook: `silver_transformation_sql.py`, depends on Task 1)
3. Set up file arrival trigger:
   - Source: `abfss://yourcontainer@yourstorageaccount.dfs.core.windows.net/path/to/files/`
   - File pattern: `*.csv`

#### Method 2: Using Databricks CLI/API
Update the job config to use the SQL-based transformation:
```json
{
  "task_key": "silver_transformation", 
  "description": "SQL-based transformation from bronze to silver with adviser matching",
  "depends_on": [{"task_key": "bronze_ingestion", "outcome": "success"}],
  "notebook_task": {
    "notebook_path": "/Workspace/edge_draft_pipeline/silver_transformation_sql",
    "base_parameters": {
      "full_refresh": "false",
      "config_path": "/Workspace/edge_draft_pipeline/config.json"
    }
  }
}
```

### 6. Configure File Trigger
Since you only have read access to blob storage, you'll need to use Databricks' file arrival trigger:
1. In the job configuration, enable "File arrival" trigger
2. Set the path to your blob storage location
3. Configure file pattern to match your CSV files

### 7. Set Up Notifications
Configure email and webhook notifications in the job settings:
- On failure: Send alerts to operations team
- On success: Optional success notifications
- Schema change alerts: Will be logged and can trigger custom notifications

## Usage

### Running the Pipeline

#### Full Refresh
To perform a complete reload of data:
```python
# Set job parameter
{"full_refresh": "true"}
```

#### Incremental Load (Default)
Normal daily processing will run incrementally, only processing new dates:
```python
# Default parameter
{"full_refresh": "false"}
```

### Testing the SQL Transformation
Use the validation notebook to test transformation logic:
```python
%run ./silver_transformation_sql

# Test transformation logic
test_transformation_logic("/Workspace/edge_draft_pipeline/config.json")
```

### Monitoring
Use the monitoring notebook to check pipeline health:
```python
%run ./monitoring_and_alerts

# Check pipeline health
monitor = PipelineMonitor("/Workspace/edge_draft_pipeline/config.json")
health = monitor.get_pipeline_health_summary()
display(health)
```

## Key Features

### 1. SQL-Based Fuzzy Matching Algorithm
The silver transformation uses a sophisticated three-tier matching strategy:

#### Tier 1: Exact Match
```sql
LOWER(TRIM(bronze.adviser)) = LOWER(TRIM(org.adviser_name))
AND LOWER(TRIM(bronze.practice)) = LOWER(TRIM(org.company_name))
```

#### Tier 2: Bronze Contains Organization
```sql
LOWER(TRIM(bronze.adviser)) LIKE CONCAT('%', LOWER(TRIM(org.adviser_name)), '%')
AND LOWER(TRIM(bronze.practice)) = LOWER(TRIM(org.company_name))
AND LENGTH(TRIM(org.adviser_name)) >= 3
```

#### Tier 3: Organization Contains Bronze
```sql
LOWER(TRIM(org.adviser_name)) LIKE CONCAT('%', LOWER(TRIM(bronze.adviser)), '%')
AND LOWER(TRIM(bronze.practice)) = LOWER(TRIM(org.company_name))
AND LENGTH(TRIM(bronze.adviser)) >= 3
```

### 2. Advanced Conflict Resolution
- Prioritizes exact matches over fuzzy matches
- For multiple matches, selects the most recently created adviser code
- Uses window functions for efficient deduplication

### 3. File Deduplication
- Automatically identifies and processes only the latest file per date
- Handles multiple uploads per day by selecting the most recent version

### 4. Schema Validation
- Enforces strict column schema
- Alerts when source schema changes
- Prevents bad data from entering the pipeline

### 5. Timezone Conversion
- Converts date_submitted from UTC to Australia/Sydney time
- Maintains audit trail with ingestion timestamps in local time

### 6. Data Quality Monitoring
- Tracks adviser match rates with detailed breakdowns by match type
- Alerts when match rates fall below thresholds
- Comprehensive logging of all operations

### 7. Error Handling
- Graceful handling of file processing errors
- Detailed error logging
- Retry logic for transient failures
- Continues processing other files when one fails

## SQL Validation and Testing

### Run Comprehensive Data Quality Checks
```sql
%run ./sql_validation_queries

-- Check recent match rates
SELECT * FROM main.silver_schema.v_data_quality_summary
ORDER BY download_date DESC LIMIT 7;

-- Analyze unmatched records
-- (Use queries from sql_validation_queries.sql)
```

### Key Validation Queries Available
1. **Data Quality Validation**: Record counts, completeness checks
2. **Matching Logic Tests**: Test each tier of fuzzy matching
3. **Comprehensive Match Analysis**: Full matching breakdown with percentages
4. **Unmatched Records Analysis**: Identify patterns in unmatched data
5. **Performance Analysis**: Processing times and batch metrics
6. **Historical Trends**: Match rate trends over time
7. **Error Detection**: Automated quality issue detection

## Troubleshooting

### Common Issues

#### 1. Low Match Rates
**Issue**: Adviser matching rate below 70%
**Investigation Steps**:
```sql
-- Run unmatched analysis
%run ./sql_validation_queries
-- Look at "Unmatched Records Analysis" section

-- Check organization data quality
SELECT COUNT(*) as active_advisers FROM main.reference_schema.organization WHERE is_active = true;
```

#### 2. SQL Performance Issues
**Issue**: Transformation taking too long
**Solutions**:
- Ensure tables have proper partitioning by `download_date`
- Enable Delta optimizations (already configured)
- Consider increasing cluster size for large datasets

#### 3. Schema Mismatch
**Error**: SQL transformation fails due to column mismatch
**Solution**: 
```sql
-- Check table schemas
DESCRIBE main.bronze_schema.bronze_edge_draft;
DESCRIBE main.reference_schema.organization;
```

#### 4. Duplicate Adviser Codes
**Issue**: Same adviser mapping to multiple codes
**Investigation**:
```sql
-- Use validation query to find duplicates
-- (See sql_validation_queries.sql - section 7)
```

### Debug Commands
```sql
-- Check recent ingestion logs
SELECT * FROM main.logs_schema.ingestion_logs 
ORDER BY ingestion_datetime DESC LIMIT 10;

-- Check transformation performance
SELECT 
    DATE(silver_processed_datetime) as processing_date,
    COUNT(*) as records_processed,
    ROUND(AVG(UNIX_TIMESTAMP(silver_processed_datetime) - UNIX_TIMESTAMP(ingestion_datetime))/60, 2) as avg_processing_minutes
FROM main.silver_schema.silver_edge_draft
WHERE silver_processed_datetime >= date_sub(current_timestamp(), 7)
GROUP BY DATE(silver_processed_datetime)
ORDER BY processing_date DESC;

-- Detailed match rate breakdown
WITH match_summary AS (
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN matched_adviser_code IS NOT NULL THEN 1 END) as matched,
        COUNT(CASE WHEN data_quality_flags = 'ADVISER_NOT_MATCHED' THEN 1 END) as unmatched
    FROM main.silver_schema.silver_edge_draft
    WHERE download_date >= date_sub(current_date(), 1)
)
SELECT 
    total,
    matched,
    unmatched,
    ROUND(matched * 100.0 / total, 2) as match_rate_percent
FROM match_summary;
```

## Performance Optimization

### SQL Query Optimization
- Uses efficient CTEs instead of nested subqueries
- Leverages window functions for deduplication
- Implements proper join strategies with broadcast hints where applicable
- Minimizes cross joins by filtering early in the process

### Table Optimization
```sql
-- Optimize tables for better performance
OPTIMIZE main.bronze_schema.bronze_edge_draft ZORDER BY (download_date, practice);
OPTIMIZE main.silver_schema.silver_edge_draft ZORDER BY (download_date, matched_adviser_code);
OPTIMIZE main.reference_schema.organization ZORDER BY (company_name, adviser_name);
```

### Cluster Configuration
For optimal SQL performance, configure your cluster with:
- Photon engine enabled
- Adaptive query execution enabled
- Appropriate worker node count based on data volume

## Scaling and Maintenance

### Adding New Data Sources
1. Create new configuration file
2. The SQL transformation logic is reusable - just change table references
3. Deploy as separate job or add as new task

### Performance Optimization
- Adjust cluster size based on file volume
- Enable Delta optimizations (already configured)
- Consider partitioning large tables by date
- Use table statistics for better query optimization

### Maintenance Tasks
- Monitor ingestion logs weekly using validation queries
- Review match rates monthly with trend analysis queries
- Update organization reference data as needed
- Archive old log data quarterly
- Run OPTIMIZE and VACUUM commands on Delta tables monthly

## Security Considerations
- Use service principals for authentication
- Store secrets in Azure Key Vault or Databricks secrets
- Implement row-level security if needed
- Regular audit of access permissions
- SQL injection protection through parameterized queries

## Benefits of SQL-based Approach

### Advantages
1. **Performance**: SQL executions are optimized by Spark's Catalyst optimizer
2. **Maintainability**: Business logic is clearly expressed in SQL
3. **Testability**: Easy to test individual CTE components
4. **Debugging**: Can run individual SQL components for troubleshooting
5. **Transparency**: Logic is visible and auditable by business users
6. **Scalability**: Leverages Spark's distributed SQL engine

### When to Use
- ✅ Complex data transformations with multiple join conditions
- ✅ Heavy aggregation and window function operations
- ✅ When business users need to understand/validate the logic
- ✅ Performance-critical transformations
- ✅ When you need to easily test and debug transformation logic

The SQL-based transformation approach provides better performance, maintainability, and transparency compared to the original Python-based approach while maintaining all the sophisticated fuzzy matching capabilities.