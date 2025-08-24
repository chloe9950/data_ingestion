# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import os

class BaseIngestionStrategy(ABC):
    """Abstract base class for ingestion strategies"""
    
    @abstractmethod
    def ingest(self, spark: SparkSession, config: ConfigManager) -> bool:
        pass

class FileIngestionStrategy(BaseIngestionStrategy):
    """File-based ingestion strategy"""
    
    def __init__(self, full_refresh: bool = False):
        self.full_refresh = full_refresh
    
    def _get_file_list(self, spark: SparkSession, config: ConfigManager) -> List[Dict]:
        """Get list of files from blob storage with metadata"""
        storage_account = config.get("source.storage_account")
        container = config.get("source.container")
        folder_path = config.get("source.folder_path")
        
        # Mount or access ADLS Gen2
        mount_path = f"/mnt/{container}"
        file_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{folder_path}"
        
        try:
            files = dbutils.fs.ls(file_path)
            file_info_list = []
            
            for file in files:
                if file.name.endswith('.csv'):
                    # Extract date from filename
                    download_date = FileUtils.extract_date_from_filename(
                        file.name, config.get("source.filename_regex")
                    )
                    
                    if download_date:
                        file_info_list.append({
                            "path": file.path,
                            "name": file.name,
                            "size": file.size,
                            "modification_time": file.modificationTime,
                            "download_date": download_date
                        })
            
            return file_info_list
            
        except Exception as e:
            raise Exception(f"Failed to list files: {str(e)}")
    
    def _process_files(self, spark: SparkSession, config: ConfigManager, 
                      files_to_process: List[Dict]) -> DataFrame:
        """Process multiple CSV files and return combined DataFrame"""
        all_dfs = []
        
        for file_info in files_to_process:
            try:
                # Read CSV with explicit schema
                df = self._read_csv_with_schema(spark, file_info["path"], config)
                
                # Add metadata columns
                df = df.withColumn("source_filename", lit(file_info["name"])) \
                      .withColumn("ingestion_datetime", 
                                lit(TimeUtils.get_current_au_time().isoformat()).cast(TimestampType())) \
                      .withColumn("download_date", lit(file_info["download_date"]).cast(DateType()))
                
                # Convert timezone for date_submitted
                df = self._convert_date_submitted_timezone(df, config)
                
                all_dfs.append(df)
                
            except Exception as e:
                Logger.log_ingestion(spark, config, file_info["name"], 0, "FAILED", str(e))
                print(f"Failed to process file {file_info['name']}: {str(e)}")
                continue
        
        if not all_dfs:
            raise Exception("No files were successfully processed")
        
        # Union all DataFrames
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.union(df)
        
        return combined_df
    
    def _read_csv_with_schema(self, spark: SparkSession, file_path: str, 
                             config: ConfigManager) -> DataFrame:
        """Read CSV with predefined schema"""
        schema_config = config.get("schema")
        
        # Build schema
        fields = []
        for col_name, col_type in schema_config.items():
            if col_name in ['source_filename', 'ingestion_datetime', 'download_date']:
                continue  # These are added later
            
            if col_type == "string":
                fields.append(StructField(col_name, StringType(), True))
            elif col_type == "timestamp":
                fields.append(StructField(col_name, StringType(), True))  # Read as string first
            elif col_type == "date":
                fields.append(StructField(col_name, StringType(), True))
        
        schema = StructType(fields)
        
        # Read CSV
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(file_path)
        
        # Validate schema
        validator = SchemaValidator({k: v for k, v in schema_config.items() 
                                   if k not in ['source_filename', 'ingestion_datetime', 'download_date']})
        is_valid, errors = validator.validate(df)
        
        if not is_valid:
            raise Exception(f"Schema validation failed: {errors}")
        
        return df
    
    def _convert_date_submitted_timezone(self, df: DataFrame, config: ConfigManager) -> DataFrame:
        """Convert date_submitted from UTC to AU timezone"""
        # Convert string to timestamp with timezone conversion
        df = df.withColumn(
            "date_submitted",
            from_utc_timestamp(
                to_timestamp(col("date_submitted"), "d/M/yyyy H:mm"),
                config.get("timezone.target", "Australia/Sydney")
            )
        )
        return df
    
    def _get_existing_dates(self, spark: SparkSession, config: ConfigManager) -> set:
        """Get existing dates in bronze table for incremental processing"""
        if self.full_refresh:
            return set()
        
        try:
            bronze_table = config.get("destinations.bronze_table")
            existing_df = spark.sql(f"SELECT DISTINCT download_date FROM {bronze_table}")
            return set([row.download_date for row in existing_df.collect()])
        except Exception:
            # Table doesn't exist or other error, treat as full refresh
            return set()
    
    def _save_to_bronze(self, df: DataFrame, config: ConfigManager):
        """Save DataFrame to bronze table"""
        bronze_table = config.get("destinations.bronze_table")
        
        if self.full_refresh:
            # Full refresh - overwrite table
            df.write.mode("overwrite").saveAsTable(bronze_table)
        else:
            # Incremental - merge logic for handling duplicates per date
            temp_view = "temp_new_data"
            df.createOrReplaceTempView(temp_view)
            
            # Create table if not exists
            try:
                spark.sql(f"DESCRIBE TABLE {bronze_table}")
            except Exception:
                df.write.mode("overwrite").saveAsTable(bronze_table)
                return
            
            # Merge strategy: delete existing records for the same download_date, then insert new ones
            dates_to_replace = [row.download_date for row in 
                              spark.sql(f"SELECT DISTINCT download_date FROM {temp_view}").collect()]
            
            if dates_to_replace:
                date_filter = ", ".join([f"'{date}'" for date in dates_to_replace])
                spark.sql(f"""
                    DELETE FROM {bronze_table} 
                    WHERE download_date IN ({date_filter})
                """)
            
            # Insert new data
            df.write.mode("append").saveAsTable(bronze_table)
    
    def ingest(self, spark: SparkSession, config: ConfigManager) -> bool:
        """Main ingestion method"""
        try:
            print("Starting file ingestion...")
            
            # Get file list
            all_files = self._get_file_list(spark, config)
            if not all_files:
                print("No files found to process")
                return True
            
            print(f"Found {len(all_files)} files")
            
            # Get latest file per date
            files_to_process = FileUtils.get_latest_file_per_date(all_files)
            print(f"Processing {len(files_to_process)} latest files")
            
            # Filter for incremental processing
            if not self.full_refresh:
                existing_dates = self._get_existing_dates(spark, config)
                files_to_process = [f for f in files_to_process 
                                  if f["download_date"] not in existing_dates]
                print(f"After filtering existing dates: {len(files_to_process)} files to process")
            
            if not files_to_process:
                print("No new files to process")
                return True
            
            # Process files
            combined_df = self._process_files(spark, config, files_to_process)
            total_records = combined_df.count()
            print(f"Total records processed: {total_records}")
            
            # Save to bronze table
            self._save_to_bronze(combined_df, config)
            
            # Log successful ingestion
            for file_info in files_to_process:
                Logger.log_ingestion(spark, config, file_info["name"], 
                                   total_records, "SUCCESS")
            
            print("Ingestion completed successfully")
            return True
            
        except Exception as e:
            print(f"Ingestion failed: {str(e)}")
            Logger.log_ingestion(spark, config, "MULTIPLE_FILES", 0, "FAILED", str(e))
            return False

class IngestionOrchestrator:
    """Orchestrates the ingestion process"""
    
    def __init__(self, config_path: str):
        self.config = ConfigManager(config_path)
        self.spark = SparkSession.getActiveSession()
    
    def run_ingestion(self, full_refresh: bool = False) -> bool:
        """Run the ingestion process"""
        strategy = FileIngestionStrategy(full_refresh=full_refresh)
        return strategy.ingest(self.spark, self.config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Cell

# COMMAND ----------

# Main execution
def main():
    # Get parameters
    full_refresh = dbutils.widgets.get("full_refresh") == "true" if dbutils.widgets.get("full_refresh") else False
    config_path = dbutils.widgets.get("config_path") if dbutils.widgets.get("config_path") else "/Workspace/config.json"
    
    print(f"Starting ingestion - Full Refresh: {full_refresh}")
    
    # Run ingestion
    orchestrator = IngestionOrchestrator(config_path)
    success = orchestrator.run_ingestion(full_refresh=full_refresh)
    
    if success:
        print("Ingestion completed successfully")
        dbutils.notebook.exit("SUCCESS")
    else:
        print("Ingestion failed")
        dbutils.notebook.exit("FAILED")

# Set up widgets for parameters
dbutils.widgets.text("full_refresh", "false", "Full Refresh")
dbutils.widgets.text("config_path", "/Workspace/config.json", "Config Path")

# Run main function
if __name__ == "__main__":
    main()