# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Logger
# MAGIC This notebook handles logging of ingestion activities to the Unity Catalog

# COMMAND ----------

# MAGIC %run ./01_BasePipeline

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
from datetime import datetime

# COMMAND ----------

class IngestionLogger:
    """Manages ingestion logging to Unity Catalog table"""
    
    def __init__(self, config_manager: ConfigManager, logger: LoggingManager):
        self.config = config_manager
        self.logger = logger
        self.spark = SparkSession.getActiveSession()
        
        self.log_table_config = self.config.get('destinations.log_table')
        self.log_table_name = f"{self.log_table_config['catalog']}.{self.log_table_config['schema']}.{self.log_table_config['table']}"
        
        self._ensure_log_table_exists()
    
    def log_ingestion_start(self, pipeline_run_id: str, files_to_process: List[Dict[str, Any]]) -> None:
        """Log the start of an ingestion run"""
        for file_info in files_to_process:
            self._insert_log_record(
                pipeline_run_id=pipeline_run_id,
                source_filename=file_info['name'],
                source_file_path=file_info['path'],
                file_size_bytes=file_info['size'],
                file_modification_time=file_info['modification_time'],
                status='STARTED',
                records_processed=0
            )
        
        self.logger.info(f"Logged start of ingestion for {len(files_to_process)} files")
    
    def log_ingestion_success(self, pipeline_run_id: str, source_filename: str, 
                            records_processed: int, processing_duration_seconds: float) -> None:
        """Log successful completion of file ingestion"""
        self._update_log_record(
            pipeline_run_id=pipeline_run_id,
            source_filename=source_filename,
            status='SUCCESS',
            records_processed=records_processed,
            processing_duration_seconds=processing_duration_seconds,
            end_time=datetime.now()
        )
        
        self.logger.info(f"Logged successful ingestion: {source_filename} - {records_processed} records")
    
    def log_ingestion_failure(self, pipeline_run_id: str, source_filename: str, 
                            error_message: str, processing_duration_seconds: float = None) -> None:
        """Log failed ingestion attempt"""
        self._update_log_record(
            pipeline_run_id=pipeline_run_id,
            source_filename=source_filename,
            status='FAILED',
            error_message=error_message,
            processing_duration_seconds=processing_duration_seconds,
            end_time=datetime.now()
        )
        
        self.logger.error(f"Logged ingestion failure: {source_filename} - {error_message}")
    
    def log_schema_validation(self, pipeline_run_id: str, source_filename: str, 
                            is_valid: bool, missing_columns: List[str] = None) -> None:
        """Log schema validation results"""
        validation_message = "Schema validation passed"
        if not is_valid:
            validation_message = f"Schema validation failed. Missing columns: {', '.join(missing_columns or [])}"
        
        self._update_log_record(
            pipeline_run_id=pipeline_run_id,
            source_filename=source_filename,
            schema_validation_passed=is_valid,
            validation_message=validation_message
        )
        
        self.logger.info(f"Logged schema validation: {source_filename} - {'PASSED' if is_valid else 'FAILED'}")
    
    def get_pipeline_run_history(self, days: int = 7) -> DataFrame:
        """Get ingestion run history for the last N days"""
        return (self.spark.table(self.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .orderBy(F.desc("start_time")))
    
    def _ensure_log_table_exists(self) -> None:
        """Create the log table if it doesn't exist"""
        if not self._log_table_exists():
            self._create_log_table()
    
    def _log_table_exists(self) -> bool:
        """Check if the log table exists"""
        try:
            self.spark.table(self.log_table_name)
            return True
        except:
            return False
    
    def _create_log_table(self) -> None:
        """Create the ingestion log table"""
        schema = StructType([
            StructField("pipeline_run_id", StringType(), False),
            StructField("source_filename", StringType(), False),
            StructField("source_file_path", StringType(), True),
            StructField("file_size_bytes", IntegerType(), True),
            StructField("file_modification_time", TimestampType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("records_processed", IntegerType(), True),
            StructField("processing_duration_seconds", StringType(), True),
            StructField("schema_validation_passed", BooleanType(), True),
            StructField("validation_message", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False)
        ])
        
        # Create empty DataFrame with schema
        empty_df = self.spark.createDataFrame([], schema)
        
        (empty_df.write
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(self.log_table_name))
        
        self.logger.info(f"Created ingestion log table: {self.log_table_name}")
    
    def _insert_log_record(self, **kwargs) -> None:
        """Insert a new log record"""
        current_time = F.from_utc_timestamp(F.current_timestamp(), self.config.get('timezone.target'))
        
        log_data = {
            'pipeline_run_id': kwargs.get('pipeline_run_id'),
            'source_filename': kwargs.get('source_filename'),
            'source_file_path': kwargs.get('source_file_path'),
            'file_size_bytes': kwargs.get('file_size_bytes'),
            'file_modification_time': kwargs.get('file_modification_time'),
            'start_time': current_time,
            'end_time': None,
            'status': kwargs.get('status', 'STARTED'),
            'records_processed': kwargs.get('records_processed', 0),
            'processing_duration_seconds': None,
            'schema_validation_passed': None,
            'validation_message': None,
            'error_message': None,
            'created_at': current_time,
            'updated_at': current_time
        }
        
        log_df = self.spark.createDataFrame([log_data])
        
        (log_df.write
         .mode("append")
         .saveAsTable(self.log_table_name))
    
    def _update_log_record(self, pipeline_run_id: str, source_filename: str, **updates) -> None:
        """Update an existing log record"""
        from delta.tables import DeltaTable
        
        log_table = DeltaTable.forName(self.spark, self.log_table_name)
        current_time = F.from_utc_timestamp(F.current_timestamp(), self.config.get('timezone.target'))
        
        # Build the set clause for updates
        set_clause = {'updated_at': current_time}
        
        for key, value in updates.items():
            if value is not None:
                if key == 'end_time' and isinstance(value, datetime):
                    # Convert datetime to appropriate format
                    set_clause[key] = F.lit(value)
                else:
                    set_clause[key] = F.lit(value)
        
        # Perform the update
        (log_table
         .update(
             condition=(F.col("pipeline_run_id") == pipeline_run_id) & 
                      (F.col("source_filename") == source_filename),
             set=set_clause
         ))

# COMMAND ----------

class PipelineMetrics:
    """Provides metrics and monitoring for the pipeline"""
    
    def __init__(self, config_manager: ConfigManager, ingestion_logger: IngestionLogger):
        self.config = config_manager
        self.ingestion_logger = ingestion_logger
        self.spark = SparkSession.getActiveSession()
    
    def get_daily_ingestion_summary(self, days: int = 7) -> DataFrame:
        """Get daily summary of ingestion activities"""
        return (self.ingestion_logger.spark.table(self.ingestion_logger.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .groupBy(F.date(F.col("start_time")).alias("ingestion_date"))
                .agg(
                    F.count("*").alias("total_files"),
                    F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_files"),
                    F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_files"),
                    F.sum("records_processed").alias("total_records_processed"),
                    F.avg("processing_duration_seconds").alias("avg_processing_duration")
                )
                .orderBy(F.desc("ingestion_date")))
    
    def get_file_processing_trends(self, days: int = 30) -> DataFrame:
        """Get processing trends over time"""
        return (self.ingestion_logger.spark.table(self.ingestion_logger.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .filter(F.col("status") == "SUCCESS")
                .select(
                    F.date(F.col("start_time")).alias("processing_date"),
                    "source_filename",
                    "records_processed",
                    "processing_duration_seconds",
                    "file_size_bytes"
                )
                .orderBy("processing_date", "source_filename"))
    
    def get_schema_validation_failures(self, days: int = 7) -> DataFrame:
        """Get schema validation failures"""
        return (self.ingestion_logger.spark.table(self.ingestion_logger.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .filter(F.col("schema_validation_passed") == False)
                .select(
                    "pipeline_run_id",
                    "source_filename",
                    "start_time",
                    "validation_message",
                    "status"
                )
                .orderBy(F.desc("start_time")))