# Databricks notebook source
# MAGIC %md
# MAGIC # Main Pipeline Orchestrator - Clean & Optimized
# MAGIC Enhanced main pipeline with better error handling and performance optimizations

# COMMAND ----------

# MAGIC %run ./01_BasePipeline

# COMMAND ----------

# MAGIC %run ./02_EdgeDataProcessor

# COMMAND ----------

# MAGIC %run ./03_IngestionLogger

# COMMAND ----------

import uuid
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

# COMMAND ----------

@dataclass
class PipelineExecutionResult:
    """Enhanced result class for pipeline execution"""
    pipeline_run_id: str
    mode: str
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    files_processed: int = 0
    records_processed: int = 0
    bronze_records: int = 0
    silver_records: int = 0
    errors: List[str] = None
    performance_metrics: Dict[str, float] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.performance_metrics is None:
            self.performance_metrics = {}
    
    @property
    def duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

# COMMAND ----------

class OptimizedEdgeDataPipeline:
    """Clean and optimized main pipeline orchestrator"""
    
    def __init__(self, config_path: str = "/Workspace/config/config.json"):
        """Initialize pipeline with optimized components"""
        self.config = ConfigManager(config_path)
        self.logger = OptimizedLogger("EdgeDataPipeline")
        
        # Initialize optimized components
        self.file_manager = OptimizedFileManager(self.config, self.logger)
        self.schema_validator = SchemaValidator(self.config.get('schema'))
        self.data_processor = OptimizedEdgeProcessor(self.config, self.logger)
        self.bronze_loader = OptimizedBronzeLoader(self.config, self.logger)
        self.silver_transformer = EnhancedSilverTransformer(self.config, self.logger)
        self.ingestion_logger = IngestionLogger(self.config, self.logger)
        self.alert_manager = AlertManager(self.logger)
        
        # Performance components
        self.cache_manager = CacheManager(SparkSession.getActiveSession(), self.logger)
        self.optimizer = DeltaTableOptimizer(SparkSession.getActiveSession(), self.logger)
        
        self.spark = SparkSession.getActiveSession()
    
    @timing_decorator
    def run_pipeline(self, mode: str = "incremental", force_process_all: bool = False) -> PipelineExecutionResult:
        """
        Enhanced pipeline execution with better tracking and error handling
        
        Args:
            mode: 'incremental' or 'full_refresh'
            force_process_all: Process all files regardless of modification time
            
        Returns:
            PipelineExecutionResult with comprehensive metrics
        """
        pipeline_run_id = str(uuid.uuid4())
        result = PipelineExecutionResult(
            pipeline_run_id=pipeline_run_id,
            mode=mode,
            start_time=datetime.now()
        )
        
        self.logger.info("Starting optimized Edge data pipeline", 
                        run_id=pipeline_run_id, mode=mode)
        
        try:
            # Phase 1: File Discovery
            files_to_process = self._discover_and_validate_files(force_process_all)
            if not files_to_process:
                self.logger.info("No files to process - pipeline completed")
                result.success = True
                return result
            
            # Phase 2: Log pipeline start
            self.ingestion_logger.log_ingestion_start(
                pipeline_run_id, list(files_to_process.values())
            )
            
            # Phase 3: Process files to bronze
            bronze_result = self._process_files_to_bronze(pipeline_run_id, files_to_process, mode)
            result.files_processed = bronze_result['files_processed']
            result.bronze_records = bronze_result['records_processed']
            result.errors.extend(bronze_result['errors'])
            
            # Phase 4: Transform to silver if bronze processing succeeded
            if result.bronze_records > 0:
                silver_result = self._transform_to_silver(mode)
                result.silver_records = silver_result.records_processed
                if not silver_result.success:
                    result.errors.append(f"Silver transformation failed: {silver_result.error_message}")
            
            # Phase 5: Performance optimization
            self._optimize_tables_if_needed(mode)
            
            # Phase 6: Calculate final metrics
            result.records_processed = result.bronze_records
            result.success = result.files_processed > 0 and len(result.errors) == 0
            
            # Performance metrics
            result.performance_metrics = {
                'files_per_second': result.files_processed / max(result.duration_seconds, 1),
                'records_per_second': result.records_processed / max(result.duration_seconds, 1),
                'bronze_to_silver_ratio': result.silver_records / max(result.bronze_records, 1)
            }
            
            self._log_pipeline_completion(result)
            
        except Exception as e:
            result.errors.append(f"Pipeline execution failed: {str(e)}")
            result.success = False
            self.logger.error("Pipeline execution failed", error=str(e))
        
        finally:
            result.end_time = datetime.now()
            self.cache_manager.clear_cache()
        
        return result
    
    @timing_decorator
    def _discover_and_validate_files(self, force_process_all: bool) -> Dict[str, Dict[str, Any]]:
        """Enhanced file discovery with validation"""
        source_config = self.config.get('source')
        
        # Build storage path
        storage_path = (f"abfss://{source_config['container']}@"
                       f"{source_config['storage_account']}.dfs.core.windows.net/"
                       f"{source_config['path']}")
        
        self.logger.info("Discovering files", path=storage_path)
        
        # List and filter files
        all_files = self.file_manager.list_files(storage_path, source_config.get('file_pattern'))
        
        if not all_files:
            self.logger.info("No files found matching pattern")
            return {}
        
        # Get latest files by date
        latest_files = self.file_manager.get_latest_files_by_date(all_files)
        
        # Apply time-based filtering if not forcing all files
        if not force_process_all:
            latest_files = self._filter_recent_files(latest_files)
        
        self.logger.info(f"Files selected for processing", count=len(latest_files))
        return latest_files
    
    def _filter_recent_files(self, files_by_date: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Filter files modified in the last 24 hours"""
        cutoff_time = datetime.now() - timedelta(hours=24)
        return {
            date_key: file_info 
            for date_key, file_info in files_by_date.items()
            if file_info['modification_time'] >= cutoff_time
        }
    
    @timing_decorator
    def _process_files_to_bronze(self, pipeline_run_id: str, files_to_process: Dict[str, Dict[str, Any]], 
                                mode: str) -> Dict[str, Any]:
        """Process multiple files to bronze with batch optimization"""
        result = {
            'files_processed': 0,
            'records_processed': 0,
            'errors': []
        }
        
        for date_key, file_info in files_to_process.items():
            try:
                file_result = self._process_single_file(pipeline_run_id, file_info, mode)
                if file_result.success:
                    result['files_processed'] += 1
                    result['records_processed'] += file_result.records_processed
                else:
                    result['errors'].append(f"File {file_info['name']}: {file_result.error_message}")
                    
            except Exception as e:
                error_msg = f"Failed to process {file_info['name']}: {str(e)}"
                result['errors'].append(error_msg)
                self.ingestion_logger.log_ingestion_failure(pipeline_run_id, file_info['name'], error_msg)
        
        return result
    
    @timing_decorator
    def _process_single_file(self, pipeline_run_id: str, file_info: Dict[str, Any], 
                           mode: str) -> ProcessingResult:
        """Process a single file with comprehensive error handling"""
        filename = file_info['name']
        file_path = file_info['path']
        
        self.logger.info("Processing file", filename=filename)
        start_time = time.time()
        
        try:
            # Read file with schema validation
            df = self._read_and_validate_file(file_path, filename, pipeline_run_id)
            
            # Process data
            processed_df = self.data_processor.process(
                df, filename, cache_manager=self.cache_manager
            )
            
            # Load to bronze
            if mode == "full_refresh":
                load_result = self.bronze_loader.load_full_refresh(processed_df)
            else:
                load_result = self.bronze_loader.load_incremental(processed_df)
            
            # Log success
            processing_duration = time.time() - start_time
            self.ingestion_logger.log_ingestion_success(
                pipeline_run_id, filename, load_result.records_processed, processing_duration
            )
            
            return load_result
            
        except Exception as e:
            processing_duration = time.time() - start_time
            error_msg = str(e)
            self.ingestion_logger.log_ingestion_failure(
                pipeline_run_id, filename, error_msg, processing_duration
            )
            return ProcessingResult(False, 0, processing_duration, error_msg)
    
    def _read_and_validate_file(self, file_path: str, filename: str, pipeline_run_id: str) -> DataFrame:
        """Read file and validate schema with proper error handling"""
        # Read with expected schema
        expected_schema = self.schema_validator.create_expected_schema()
        
        df = (self.spark.read
              .option("header", "true")
              .option("inferSchema", "false")
              .schema(expected_schema)
              .csv(file_path))
        
        # Validate schema
        is_valid, missing_cols, extra_cols = self.schema_validator.validate_schema(df)
        
        # Log validation results
        self.ingestion_logger.log_schema_validation(pipeline_run_id, filename, is_valid, missing_cols)
        
        if not is_valid:
            self.alert_manager.send_schema_change_alert(missing_cols, extra_cols)
            raise ValueError(f"Schema validation failed: missing columns {missing_cols}")
        
        return df
    
    @timing_decorator
    def _transform_to_silver(self, mode: str) -> ProcessingResult:
        """Transform bronze data to silver with enhanced processing"""
        try:
            return self.silver_transformer.transform_and_load(mode)
        except Exception as e:
            self.logger.error("Silver transformation failed", error=str(e))
            return ProcessingResult(False, 0, 0, str(e))
    
    def _optimize_tables_if_needed(self, mode: str):
        """Optimize tables based on configuration and mode"""
        opt_config = self.config.get('optimization', {})
        
        # Optimize after full refresh or based on frequency
        should_optimize = (
            mode == "full_refresh" or 
            opt_config.get('auto_optimize', False)
        )
        
        if should_optimize:
            try:
                z_order_cols = opt_config.get('z_order_columns', [])
                
                # Optimize bronze table
                bronze_table = f"{self.config.get('destinations.bronze_table.catalog')}.{self.config.get('destinations.bronze_table.schema')}.{self.config.get('destinations.bronze_table.table')}"
                self.optimizer.optimize_table(bronze_table, z_order_cols)
                
                # Optimize silver table
                silver_table = f"{self.config.get('destinations.silver_table.catalog')}.{self.config.get('destinations.silver_table.schema')}.{self.config.get('destinations.silver_table.table')}"
                self.optimizer.optimize_table(silver_table, z_order_cols)
                
            except Exception as e:
                self.logger.warning("Table optimization failed", error=str(e))
    
    def _log_pipeline_completion(self, result: PipelineExecutionResult):
        """Log comprehensive pipeline completion metrics"""
        status = "SUCCESS" if result.success else "FAILED"
        
        self.logger.info(
            f"Pipeline completed with status: {status}",
            run_id=result.pipeline_run_id,
            mode=result.mode,
            files_processed=result.files_processed,
            bronze_records=result.bronze_records,
            silver_records=result.silver_records,
            duration=result.duration_seconds,
            errors_count=len(result.errors),
            **result.performance_metrics
        )
    
    # Monitoring and debugging methods
    def get_pipeline_health(self, days: int = 7) -> DataFrame:
        """Get comprehensive pipeline health metrics"""
        return (self.ingestion_logger.spark.table(self.ingestion_logger.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .groupBy(F.date(F.col("start_time")).alias("date"))
                .agg(
                    F.count("*").alias("total_runs"),
                    F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
                    F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
                    F.avg("records_processed").alias("avg_records_processed"),
                    F.avg("processing_duration_seconds").alias("avg_processing_time"),
                    F.sum("records_processed").alias("total_records_processed")
                )
                .withColumn("success_rate", F.round(F.col("successful_runs") / F.col("total_runs") * 100, 2))
                .orderBy(F.desc("date")))
    
    def get_performance_trends(self, days: int = 30) -> DataFrame:
        """Get performance trends over time"""
        return (self.ingestion_logger.spark.table(self.ingestion_logger.log_table_name)
                .filter(F.col("start_time") >= F.date_sub(F.current_date(), days))
                .filter(F.col("status") == "SUCCESS")
                .select(
                    F.date(F.col("start_time")).alias("date"),
                    F.hour(F.col("start_time")).alias("hour"),
                    "source_filename",
                    "records_processed",
                    F.col("processing_duration_seconds").cast("double").alias("processing_duration"),
                    "file_size_bytes"
                )
                .withColumn("records_per_second", 
                           F.col("records_processed") / F.greatest(F.col("processing_duration"), F.lit(1)))
                .withColumn("mb_processed", F.col("file_size_bytes") / 1024 / 1024)
                .orderBy("date", "hour"))

# COMMAND ----------

# Enhanced widget configuration
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full_refresh"], "Pipeline Mode")
dbutils.widgets.dropdown("force_process_all", "false", ["true", "false"], "Force Process All Files") 
dbutils.widgets.dropdown("enable_monitoring", "true", ["true", "false"], "Enable Monitoring Output")
dbutils.widgets.text("config_path", "/Workspace/config/config.json", "Configuration Path")

# COMMAND ----------

def display_pipeline_results(result: PipelineExecutionResult):
    """Display comprehensive pipeline results"""
    print("="*60)
    print("PIPELINE EXECUTION RESULTS")
    print("="*60)
    
    print(f"🔍 Run ID: {result.pipeline_run_id}")
    print(f"⚙️  Mode: {result.mode}")
    print(f"✅ Success: {'YES' if result.success else 'NO'}")
    print(f"📁 Files Processed: {result.files_processed}")
    print(f"📊 Bronze Records: {result.bronze_records:,}")
    print(f"🥈 Silver Records: {result.silver_records:,}")
    print(f"⏱️  Duration: {result.duration_seconds:.2f} seconds")
    
    if result.performance_metrics:
        print("\n📈 PERFORMANCE METRICS:")
        for metric, value in result.performance_metrics.items():
            print(f"   • {metric.replace('_', ' ').title()}: {value:.2f}")
    
    if result.errors:
        print(f"\n❌ ERRORS ({len(result.errors)}):")
        for i, error in enumerate(result.errors, 1):
            print(f"   {i}. {error}")
    
    print("="*60)

def display_monitoring_dashboard(pipeline: OptimizedEdgeDataPipeline):
    """Display monitoring dashboard"""
    print("\n" + "="*60)
    print("PIPELINE MONITORING DASHBOARD")
    print("="*60)
    
    try:
        # Health metrics for last 7 days
        print("\n📊 PIPELINE HEALTH (Last 7 days):")
        health_df = pipeline.get_pipeline_health(7)
        if health_df.count() > 0:
            health_df.show(10, truncate=False)
        else:
            print("   No health data available")
        
        # Performance trends for last 7 days
        print("\n⚡ PERFORMANCE TRENDS (Last 7 days):")
        trends_df = pipeline.get_performance_trends(7)
        if trends_df.count() > 0:
            # Show summary statistics
            summary_df = (trends_df
                         .groupBy("date")
                         .agg(
                             F.sum("records_processed").alias("daily_records"),
                             F.avg("records_per_second").alias("avg_throughput"),
                             F.avg("mb_processed").alias("avg_mb_processed")
                         )
                         .orderBy(F.desc("date")))
            summary_df.show(7, truncate=False)
        else:
            print("   No performance data available")
            
    except Exception as e:
        print(f"   ⚠️  Monitoring data unavailable: {str(e)}")

# COMMAND ----------

# Main execution block
if __name__ == "__main__":
    # Get parameters from widgets
    mode = dbutils.widgets.get("mode")
    force_process_all = dbutils.widgets.get("force_process_all").lower() == "true"
    enable_monitoring = dbutils.widgets.get("enable_monitoring").lower() == "true"
    config_path = dbutils.widgets.get("config_path")
    
    pipeline_start_time = datetime.now()
    
    try:
        # Initialize pipeline
        print("🚀 Initializing Edge Data Pipeline...")
        pipeline = OptimizedEdgeDataPipeline(config_path)
        
        # Execute pipeline
        print(f"▶️  Starting pipeline execution in {mode} mode...")
        result = pipeline.run_pipeline(mode=mode, force_process_all=force_process_all)
        
        # Display results
        display_pipeline_results(result)
        
        # Show monitoring dashboard if enabled
        if enable_monitoring:
            display_monitoring_dashboard(pipeline)
        
        # Additional insights for failed executions
        if not result.success:
            print("\n🔍 FAILURE ANALYSIS:")
            recent_failures = (pipeline.ingestion_logger.spark.table(pipeline.ingestion_logger.log_table_name)
                             .filter(F.col("status") == "FAILED")
                             .filter(F.col("start_time") >= F.current_date())
                             .select("source_filename", "error_message", "start_time")
                             .orderBy(F.desc("start_time")))
            
            if recent_failures.count() > 0:
                print("Recent failures today:")
                recent_failures.show(5, truncate=False)
        
        # Set appropriate exit status
        exit_status = "SUCCESS" if result.success else "FAILED"
        
        # Final summary
        total_execution_time = (datetime.now() - pipeline_start_time).total_seconds()
        print(f"\n🏁 Pipeline execution completed in {total_execution_time:.2f} seconds")
        print(f"📤 Exiting with status: {exit_status}")
        
        dbutils.notebook.exit(exit_status)
        
    except Exception as e:
        error_msg = f"💥 Fatal pipeline error: {str(e)}"
        print(error_msg)
        
        # Try to log the fatal error if possible
        try:
            pipeline = OptimizedEdgeDataPipeline(config_path)
            pipeline.logger.error("Fatal pipeline error", error=str(e))
        except:
            pass  # If logging fails, just continue to exit
        
        dbutils.notebook.exit("FATAL_ERROR")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Monitoring and Debugging
# MAGIC 
# MAGIC Use these commands for advanced monitoring and troubleshooting

# COMMAND ----------

# Uncomment and run for advanced monitoring

# # Get detailed pipeline metrics
# pipeline = OptimizedEdgeDataPipeline()
# 
# # Health check with alerts
# health_df = pipeline.get_pipeline_health(30)
# failed_days = health_df.filter(F.col("success_rate") < 100).count()
# if failed_days > 0:
#     print("⚠️  ALERT: Pipeline failures detected!")
#     health_df.filter(F.col("success_rate") < 100).show()

# COMMAND ----------

# # Schema evolution tracking
# schema_issues = (pipeline.ingestion_logger.spark.table(pipeline.ingestion_logger.log_table_name)
#                  .filter(F.col("schema_validation_passed") == False)
#                  .filter(F.col("start_time") >= F.current_date() - 30)
#                  .select("source_filename", "validation_message", "start_time")
#                  .orderBy(F.desc("start_time")))
# 
# if schema_issues.count() > 0:
#     print("🚨 SCHEMA EVOLUTION DETECTED:")
#     schema_issues.show(20, truncate=False)

# COMMAND ----------

# # Performance bottleneck analysis
# bottlenecks = (pipeline.get_performance_trends(7)
#                .filter(F.col("records_per_second") < 1000)  # Threshold for slow processing
#                .select("date", "source_filename", "records_processed", 
#                       "processing_duration", "records_per_second")
#                .orderBy("records_per_second"))
# 
# if bottlenecks.count() > 0:
#     print("🐌 PERFORMANCE BOTTLENECKS DETECTED:")
#     bottlenecks.show(10, truncate=False)

# COMMAND ----------

# # Data quality metrics
# def analyze_data_quality():
#     bronze_table = f"{pipeline.config.get('destinations.bronze_table.catalog')}.{pipeline.config.get('destinations.bronze_table.schema')}.{pipeline.config.get('destinations.bronze_table.table')}"
#     
#     quality_metrics = (pipeline.spark.table(bronze_table)
#                       .filter(F.date(F.col("ingestion_datetime")) >= F.current_date() - 7)
#                       .agg(
#                           F.count("*").alias("total_records"),
#                           F.sum(F.when(F.col("has_valid_date"), 1).otherwise(0)).alias("valid_dates"),
#                           F.avg("record_completeness").alias("avg_completeness"),
#                           F.countDistinct("practice").alias("unique_practices"),
#                           F.countDistinct("adviser").alias("unique_advisers")
#                       )
#                       .withColumn("date_validity_rate", 
#                                  F.round(F.col("valid_dates") / F.col("total_records") * 100, 2)))
#     
#     print("📊 DATA QUALITY METRICS (Last 7 days):")
#     quality_metrics.show(truncate=False)
# 
# # analyze_data_quality()

# COMMAND ----------

# # Silver table join success rates
# def analyze_join_success():
#     silver_table = f"{pipeline.config.get('destinations.silver_table.catalog')}.{pipeline.config.get('destinations.silver_table.schema')}.{pipeline.config.get('destinations.silver_table.table')}"
#     
#     try:
#         join_metrics = (pipeline.spark.table(silver_table)
#                        .filter(F.date(F.col("ingestion_datetime")) >= F.current_date() - 7)
#                        .groupBy("match_type")
#                        .agg(
#                            F.count("*").alias("record_count"),
#                            F.avg("match_confidence").alias("avg_confidence")
#                        )
#                        .orderBy(F.desc("record_count")))
#         
#         print("🔗 ORGANIZATION JOIN ANALYSIS (Last 7 days):")
#         join_metrics.show(truncate=False)
#         
#         # Dynamic mapping success
#         mapping_success = (pipeline.spark.table(silver_table)
#                           .filter(F.date(F.col("ingestion_datetime")) >= F.current_date() - 7)
#                           .agg(
#                               F.count("*").alias("total_records"),
#                               F.sum(F.when(F.col("dynamic_id").isNotNull(), 1).otherwise(0)).alias("mapped_records")
#                           )
#                           .withColumn("mapping_success_rate",
#                                      F.round(F.col("mapped_records") / F.col("total_records") * 100, 2)))
#         
#         print("🎯 DYNAMIC MAPPING SUCCESS:")
#         mapping_success.show(truncate=False)
#         
#     except Exception as e:
#         print(f"⚠️  Silver table analysis unavailable: {str(e)}")
# 
# # analyze_join_success()