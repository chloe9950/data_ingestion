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

## Key Features

### 1. File Deduplication
- Automatically identifies and processes only the latest file per date
- Handles multiple uploads per day by selecting the most recent version

### 2. Schema Validation
- Enforces strict column schema
- Alerts when source schema changes
- Prevents bad data from entering the pipeline

### 3. Timezone Conversion
- Converts date_submitted from UTC to Australia/Sydney time
- Maintains audit trail with ingestion timestamps in local time

### 4. Fuzzy Matching
The silver transformation performs intelligent adviser matching using three strategies:
- Exact name and practice match
- Bronze adviser name contains organization adviser name
- Organization adviser name contains bronze adviser name
- Returns the latest adviser code when multiple matches found

### 5. Data Quality Monitoring
- Tracks adviser match rates
- Alerts when match rates fall below thresholds
- Comprehensive logging of all operations

### 6. Error Handling
- Graceful handling of file processing errors
- Detailed error logging
- Retry logic for transient failures
- Continues processing other files when one fails

## Troubleshooting

### Common Issues

#### 1. File Access Issues
**Error**: `java.io.FileNotFoundException`
**Solution**: Check storage account authentication and permissions

#### 2. Schema Mismatch
**Error**: Schema validation failed
**Solution**: Check source file format matches expected columns

#### 3. Low Match Rates
**Issue**: Adviser matching rate below threshold
**Solution**: Review organization reference data and matching logic

#### 4. Job Trigger Not Working
**Issue**: Pipeline not triggering on new files
**Solution**: Verify file arrival trigger path and permissions

### Debug Commands
```sql
-- Check recent ingestion logs
SELECT * FROM main.logs_schema.ingestion_logs 
ORDER BY ingestion_datetime DESC LIMIT 10;

-- Check data quality summary
SELECT * FROM main.silver_schema.v_data_quality_summary
ORDER BY download_date DESC LIMIT 7;

-- Check bronze data counts
SELECT download_date, COUNT(*) as record_count
FROM main.bronze_schema.bronze_edge_draft
GROUP BY download_date
ORDER BY download_date DESC;
```

## Scaling and Maintenance

### Adding New Data Sources
1. Create new configuration file
2. Modify pipeline code to use different config path
3. Deploy as separate job or add as new task

### Performance Optimization
- Adjust cluster size based on file volume
- Enable Delta optimizations (already configured)
- Consider partitioning large tables by date

### Maintenance Tasks
- Monitor ingestion logs weekly
- Review match rates monthly
- Update organization reference data as needed
- Archive old log data quarterly

## Security Considerations
- Use service principals for authentication
- Store secrets in Azure Key Vault or Databricks secrets
- Implement row-level security if needed
- Regular audit of access permissions

##