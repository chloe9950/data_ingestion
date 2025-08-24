# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Data Ingestion
# MAGIC Ingests data from ADLS Gen2 to bronze table with file triggering

# COMMAND ----------

# MAGIC %run ./utils_module

# COMMAND ----------

import os
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

# Initialize components
spark = SparkSession.builder.getOrCreate()
config = ConfigManager("/Workspace/Shared/config/pipeline_config.json")
log_manager = LogManager(spark, config)
file_utils = FileUtils()
processor = DataProcessor()
validator = SchemaValidator(config.get("schema"))

# Parameters
dbutils.widgets.text("mode", "incremental", "Processing Mode (incremental/full)")
dbutils.widgets.text("triggered_file", "", "Triggered File Path")

mode = dbutils.widgets.get("mode")
triggered_file = dbutils.widgets.get("triggered_file")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Discovery and Processing

# COMMAND ----------

class BronzeIngestion:
    """Bronze layer data ingestion"""
    
    def __init__(self, config: ConfigManager, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.bronze_table = f"{config.get('target.catalog')}.{config.get('target.schema')}.{config.get('target.bronze_table')}"
        self.storage_path = f"abfss://{config.get('source.container')}@{config.get('source.storage_account')}.dfs.core.windows.net/{config.get('source.path')}"
    
    def discover_files(self) -> List[Tuple[str, datetime]]:
        """Discover files in storage with modification times"""
        try:
            files = dbutils.fs.ls(self.storage_path)
            file_list = []
            
            for file_info in files:
                if (file_info.name.endswith('.csv') and 
                    'EDGE Reporting - Edge Draft Apps Manual Update' in file_info.name):
                    # Get modification time (simplified - in production use file system APIs)
                    mod_time = datetime.now()  # Placeholder - replace with actual modification time
                    file_list.append((file_info.path, mod_time))
            
            return file_list
        except Exception as e:
            print(f"Error discovering files: {str(e)}")
            return []
    
    def get_files_to_process(self, mode: str, triggered_file: str = None) -> List[str]:
        """Determine which files to process based on mode"""
        if triggered_file:
            # File-triggered mode - process specific file
            return [triggered_file]
        
        if mode == "full":
            # Full refresh - process all files (latest per date)
            all_files = self.discover_files()
            return file_utils.get_latest_files_per_date(all_files)
        else:
            # Incremental - process today's files only
            all_files = self.discover_files()
            today = date.today()
            today_files = []
            
            for filepath, mod_time in all_files:
                filename = filepath.split('/')[-1]
                file_date = file_utils.extract_date_from_filename(filename)
                if file_date == today:
                    today_files.append((filepath, mod_time))
            
            return file_utils.get_latest_files_per_date(today_files)
    
    def process_file(self, file_path: str) -> bool:
        """Process individual file"""
        try:
            filename = file_path.split('/')[-1]
            download_date = file_utils.extract_date_from_filename(filename)
            
            if not download_date:
                log_manager.log_ingestion(filename, 0, "FAILED", "Could not extract date from filename")
                return False
            
            # Read CSV file
            df = self.spark.read.option("header", "true").csv(file_path)
            
            # Validate schema
            is_valid, issues = validator.validate_schema(df)
            if not is_valid:
                error_msg = f"Schema validation failed: {'; '.join(issues)}"
                log_manager.log_ingestion(filename, 0, "FAILED", error_msg)
                raise Exception(error_msg)
            
            # Apply transformations
            df = processor.apply_schema_transformations(df, self.config)
            
            # Add metadata columns
            df = df.withColumn("source_filename", lit(filename))
            df = df.withColumn("download_date", lit(download_date))
            
            # Cast columns to expected types
            df = self._apply_schema_types(df)
            
            # Write to bronze table
            if mode == "full":
                self._write_full_refresh(df, download_date)
            else:
                self._write_incremental(df, download_date)
            
            record_count = df.count()
            log_manager.log_ingestion(filename, record_count, "SUCCESS", f"Processed {record_count} records")
            
            return True
            
        except Exception as e:
            log_manager.log_ingestion(filename, 0, "FAILED", str(e))
            raise e
    
    def _apply_schema_types(self, df: DataFrame) -> DataFrame:
        """Apply proper data types to DataFrame"""
        return df.select(
            col("date_submitted").cast(TimestampType()),
            col("account_name").cast(StringType()),
            col("account_type").cast(StringType()),
            col("practice").cast(StringType()),
            col("adviser").cast(StringType()),
            col("source_filename").cast(StringType()),
            col("ingestion_datetime").cast(TimestampType()),
            col("download_date").cast(DateType())
        )
    
    def _write_full_refresh(self, df: DataFrame, download_date: date):
        """Write data in full refresh mode"""
        # Delete existing data for the same download_date
        self.spark.sql(f"""
            DELETE FROM {self.bronze_table} 
            WHERE download_date = '{download_date}'
        """)
        
        # Insert new data
        df.write.mode("append").option("mergeSchema", "false").saveAsTable(self.bronze_table)
    
    def _write_incremental(self, df: DataFrame, download_date: date):
        """Write data in incremental mode with deduplication"""
        # Create temporary view
        df.createOrReplaceTempView("new_data")
        
        # Merge logic to handle duplicates (latest ingestion wins)
        merge_sql = f"""
        MERGE INTO {self.bronze_table} target
        USING (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY download_date, account_name, practice, adviser 
                ORDER BY ingestion_datetime DESC
            ) as rn
            FROM new_data
        ) source
        ON target.download_date = source.download_date 
           AND target.account_name = source.account_name 
           AND target.practice = source.practice 
           AND target.adviser = source.adviser
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED AND source.rn = 1 THEN INSERT *
        """
        
        self.spark.sql(merge_sql)
    
    def run_ingestion(self):
        """Main ingestion process"""
        try:
            files_to_process = self.get_files_to_process(mode, triggered_file)
            
            if not files_to_process:
                print("No files to process")
                return
            
            print(f"Processing {len(files_to_process)} files in {mode} mode")
            
            for file_path in files_to_process:
                print(f"Processing file: {file_path}")
                self.process_file(file_path)
            
            print("Ingestion completed successfully")
            
        except Exception as e:
            print(f"Ingestion failed: {str(e)}")
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion

# COMMAND ----------

# Initialize and run ingestion
ingestion = BronzeIngestion(config, spark)
ingestion.run_ingestion()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Basic data quality checks
bronze_table = f"{config.get('target.catalog')}.{config.get('target.schema')}.{config.get('target.bronze_table')}"

print("=== Data Quality Summary ===")
df_bronze = spark.table(bronze_table)

print(f"Total records: {df_bronze.count()}")
print(f"Date range: {df_bronze.select(min('download_date'), max('download_date')).collect()[0]}")
print(f"Unique practices: {df_bronze.select('practice').distinct().count()}")
print(f"Unique advisers: {df_bronze.select('adviser').distinct().count()}")

# Check for nulls in critical columns
null_checks = df_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls") 
    for c in ['account_name', 'practice', 'adviser', 'download_date']
]).collect()[0].asDict()

print(f"Null counts: {null_checks}")

# Display recent ingestion logs
log_table = f"{config.get('target.catalog')}.{config.get('target.schema')}.{config.get('target.log_table')}"
recent_logs = spark.sql(f"""
    SELECT filename, records_processed, status, message, log_datetime 
    FROM {log_table} 
    ORDER BY log_datetime DESC 
    LIMIT 10
""")
recent_logs.show(truncate=False)