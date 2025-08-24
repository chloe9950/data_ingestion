# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json

class SchemaMonitor:
    """Monitors for schema changes and alerts"""
    
    def __init__(self, config: ConfigManager, spark: SparkSession):
        self.config = config
        self.spark = spark
    
    def check_schema_drift(self, df, expected_schema: dict) -> dict:
        """Check if DataFrame schema matches expected schema"""
        current_columns = set(df.columns)
        expected_columns = set(expected_schema.keys())
        
        missing_columns = expected_columns - current_columns
        extra_columns = current_columns - expected_columns
        
        schema_issues = {
            "has_issues": len(missing_columns) > 0 or len(extra_columns) > 0,
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "check_timestamp": datetime.now().isoformat()
        }
        
        return schema_issues
    
    def send_schema_alert(self, schema_issues: dict, source_file: str):
        """Send alert for schema issues"""
        if schema_issues["has_issues"]:
            alert_message = {
                "alert_type": "SCHEMA_DRIFT",
                "severity": "HIGH",
                "source_file": source_file,
                "timestamp": schema_issues["check_timestamp"],
                "issues": {
                    "missing_columns": schema_issues["missing_columns"],
                    "extra_columns": schema_issues["extra_columns"]
                }
            }
            
            # Log to ingestion logs table
            Logger.log_ingestion(
                self.spark, self.config, source_file, 0, 
                "SCHEMA_ALERT", json.dumps(alert_message)
            )
            
            print(f"🚨 SCHEMA ALERT: {alert_message}")
            return True
        
        return False

class DataQualityMonitor:
    """Monitors data quality metrics"""
    
    def __init__(self, config: ConfigManager, spark: SparkSession):
        self.config = config
        self.spark = spark
    
    def check_data_quality(self) -> dict:
        """Run data quality checks on silver table"""
        silver_table = self.config.get("destinations.silver_table")
        
        # Get recent data (last 7 days)
        cutoff_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        df = self.spark.sql(f"""
            SELECT * FROM {silver_table} 
            WHERE download_date >= '{cutoff_date}'
        """)
        
        if df.count() == 0:
            return {"has_issues": False, "message": "No recent data to check"}
        
        # Calculate metrics
        total_records = df.count()
        matched_records = df.filter(col("matched_adviser_code").isNotNull()).count()
        match_rate = (matched_records / total_records) * 100
        
        # Define thresholds
        min_match_rate = 70  # Minimum acceptable match rate
        
        quality_issues = {
            "has_issues": match_rate < min_match_rate,
            "total_records": total_records,
            "matched_records": matched_records,
            "match_rate": match_rate,
            "min_threshold": min_match_rate,
            "check_timestamp": datetime.now().isoformat()
        }
        
        return quality_issues
    
    def send_quality_alert(self, quality_issues: dict):
        """Send alert for data quality issues"""
        if quality_issues.get("has_issues"):
            alert_message = {
                "alert_type": "DATA_QUALITY",
                "severity": "MEDIUM",
                "timestamp": quality_issues["check_timestamp"],
                "metrics": {
                    "match_rate": quality_issues["match_rate"],
                    "threshold": quality_issues["min_threshold"],
                    "total_records": quality_issues["total_records"]
                }
            }
            
            Logger.log_ingestion(
                self.spark, self.config, "DATA_QUALITY_CHECK", 
                quality_issues["total_records"], "QUALITY_ALERT", 
                json.dumps(alert_message)
            )
            
            print(f"📊 DATA QUALITY ALERT: {alert_message}")
            return True
        
        return False

class PipelineMonitor:
    """Main pipeline monitoring orchestrator"""
    
    def __init__(self, config_path: str):
        self.config = ConfigManager(config_path)
        self.spark = SparkSession.getActiveSession()
        self.schema_monitor = SchemaMonitor(self.config, self.spark)
        self.quality_monitor = DataQualityMonitor(self.config, self.spark)
    
    def run_monitoring_checks(self) -> dict:
        """Run all monitoring checks"""
        results = {
            "monitoring_timestamp": datetime.now().isoformat(),
            "checks_performed": [],
            "alerts_sent": 0
        }
        
        # Schema monitoring (run during ingestion)
        # This would typically be called from the ingestion pipeline
        
        # Data quality monitoring
        try:
            quality_results = self.quality_monitor.check_data_quality()
            results["checks_performed"].append("data_quality")
            
            if self.quality_monitor.send_quality_alert(quality_results):
                results["alerts_sent"] += 1
                
        except Exception as e:
            print(f"Data quality check failed: {str(e)}")
        
        return results
    
    def get_pipeline_health_summary(self) -> dict:
        """Get overall pipeline health summary"""
        log_table = self.config.get("destinations.log_table")
        
        # Get recent ingestion stats
        recent_logs = self.spark.sql(f"""
            SELECT 
                status,
                COUNT(*) as count,
                MAX(ingestion_datetime) as latest_run
            FROM {log_table}
            WHERE ingestion_datetime >= date_sub(current_date(), 7)
            GROUP BY status
        """).collect()
        
        # Get silver table stats
        silver_table = self.config.get("destinations.silver_table")
        try:
            silver_stats = self.spark.sql(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT download_date) as unique_dates,
                    MAX(silver_processed_datetime) as latest_processing
                FROM {silver_table}
                WHERE download_date >= date_sub(current_date(), 7)
            """).collect()[0]
        except:
            silver_stats = None
        
        health_summary = {
            "pipeline_status": "HEALTHY" if any(log.status == "SUCCESS" for log in recent_logs) else "UNHEALTHY",
            "recent_ingestion_logs": [{"status": log.status, "count": log.count, "latest_run": str(log.latest_run)} for log in recent_logs],
            "silver_table_stats": {
                "total_records": silver_stats.total_records if silver_stats else 0,
                "unique_dates": silver_stats.unique_dates if silver_stats else 0,
                "latest_processing": str(silver_stats.latest_processing) if silver_stats else None
            } if silver_stats else None,
            "summary_timestamp": datetime.now().isoformat()
        }
        
        return health_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Dashboard Queries

# COMMAND ----------

def create_monitoring_dashboard():
    """Create monitoring dashboard queries"""
    
    dashboard_queries = {
        "ingestion_success_rate": """
            SELECT 
                DATE(ingestion_datetime) as date,
                COUNT(*) as total_runs,
                SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
                ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
            FROM main.logs_schema.ingestion_logs
            WHERE ingestion_datetime >= date_sub(current_date(), 30)
            GROUP BY DATE(ingestion_datetime)
            ORDER BY date DESC
        """,
        
        "daily_record_counts": """
            SELECT 
                download_date,
                COUNT(*) as record_count,
                COUNT(DISTINCT source_filename) as file_count
            FROM main.bronze_schema.bronze_edge_draft
            WHERE download_date >= date_sub(current_date(), 30)
            GROUP BY download_date
            ORDER BY download_date DESC
        """,
        
        "adviser_match_rates": """
            SELECT 
                download_date,
                COUNT(*) as total_records,
                COUNT(matched_adviser_code) as matched_records,
                ROUND(COUNT(matched_adviser_code) * 100.0 / COUNT(*), 2) as match_rate
            FROM main.silver_schema.silver_edge_draft
            WHERE download_date >= date_sub(current_date(), 30)
            GROUP BY download_date
            ORDER BY download_date DESC
        """,
        
        "processing_times": """
            SELECT 
                source_file,
                ingestion_datetime,
                records_processed,
                CASE 
                    WHEN records_processed > 0 THEN 'Normal'
                    WHEN status = 'FAILED' THEN 'Failed'
                    ELSE 'No Data'
                END as processing_status
            FROM main.logs_schema.ingestion_logs
            WHERE ingestion_datetime >= date_sub(current_date(), 7)
            ORDER BY ingestion_datetime DESC
        """
    }
    
    return dashboard_queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main():
    """Main monitoring execution"""
    config_path = dbutils.widgets.get("config_path") if dbutils.widgets.get("config_path") else "/Workspace/config.json"
    
    print("🔍 Starting pipeline monitoring...")
    
    # Run monitoring checks
    monitor = PipelineMonitor(config_path)
    monitoring_results = monitor.run_monitoring_checks()
    
    print(f"✅ Monitoring completed: {monitoring_results}")
    
    # Get health summary
    health_summary = monitor.get_pipeline_health_summary()
    print(f"🏥 Pipeline Health: {health_summary}")
    
    # Return results
    dbutils.notebook.exit(json.dumps({
        "monitoring_results": monitoring_results,
        "health_summary": health_summary
    }))

# Set up widgets
dbutils.widgets.text("config_path", "/Workspace/config.json", "Config Path")

# Run main function
if __name__ == "__main__":
    main()