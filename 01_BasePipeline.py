# Databricks notebook source
# MAGIC %md
# MAGIC # Base Pipeline Classes - Clean & Optimized
# MAGIC This notebook contains the cleaned base classes with optimizations

# COMMAND ----------

import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, date
import re
from pathlib import Path
from functools import lru_cache, wraps
from dataclasses import dataclass

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

@dataclass
class ProcessingResult:
    """Data class for processing results"""
    success: bool
    records_processed: int
    duration_seconds: float
    error_message: Optional[str] = None

# COMMAND ----------

def timing_decorator(func):
    """Decorator to measure function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        try:
            result = func(*args, **kwargs)
            duration = (datetime.now() - start).total_seconds()
            if hasattr(args[0], 'logger'):
                args[0].logger.info(f"{func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = (datetime.now() - start).total_seconds()
            if hasattr(args[0], 'logger'):
                args[0].logger.error(f"{func.__name__} failed after {duration:.2f}s: {str(e)}")
            raise
    return wrapper

# COMMAND ----------

class ConfigManager:
    """Singleton configuration manager with caching"""
    
    _instance = None
    _config_cache = {}
    
    def __new__(cls, config_path: str):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: str):
        if not hasattr(self, 'initialized'):
            self.config_path = config_path
            self._config = None
            self.initialized = True
    
    @lru_cache(maxsize=1)
    def load_config(self) -> Dict[str, Any]:
        """Load and cache configuration"""
        try:
            with open(self.config_path, 'r') as f:
                self._config = json.load(f)
            self._validate_config()
            return self._config
        except Exception as e:
            raise ValueError(f"Configuration error: {e}")
    
    def _validate_config(self):
        """Validate required configuration keys"""
        required_keys = ['pipeline', 'source', 'schema', 'destinations', 'timezone', 'processing']
        missing = [k for k in required_keys if k not in self._config]
        if missing:
            raise ValueError(f"Missing required configuration keys: {missing}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation support"""
        config = self.load_config()
        value = config
        
        for k in key.split('.'):
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

# COMMAND ----------

class OptimizedLogger:
    """Optimized logger with structured logging"""
    
    def __init__(self, name: str = __name__, level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _log(self, level: str, message: str, **kwargs):
        """Internal log method with context"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()]) if kwargs else ""
        full_message = f"{message} | {context}" if context else message
        getattr(self.logger, level.lower())(full_message)
    
    def info(self, message: str, **kwargs):
        self._log("INFO", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, **kwargs)

# COMMAND ----------

class SchemaValidator:
    """Optimized schema validator with caching"""
    
    def __init__(self, expected_schema: Dict[str, Any]):
        self.expected_columns = {col['name']: col['type'] for col in expected_schema['columns']}
        self.logger = OptimizedLogger("SchemaValidator")
    
    @timing_decorator
    def validate_schema(self, df: DataFrame) -> Tuple[bool, List[str], List[str]]:
        """
        Validate DataFrame schema
        Returns: (is_valid, missing_columns, extra_columns)
        """
        actual_columns = set(df.columns)
        expected_columns = set(self.expected_columns.keys())
        
        missing = list(expected_columns - actual_columns)
        extra = list(actual_columns - expected_columns)
        
        is_valid = len(missing) == 0
        
        if missing:
            self.logger.error("Schema validation failed", missing_columns=missing)
        if extra:
            self.logger.warning("Extra columns detected", extra_columns=extra)
        
        return is_valid, missing, extra
    
    @lru_cache(maxsize=1)
    def create_expected_schema(self) -> StructType:
        """Create cached PySpark StructType"""
        fields = [
            StructField(name, StringType(), True) 
            for name in self.expected_columns.keys()
        ]
        return StructType(fields)

# COMMAND ----------

class DataProcessor(ABC):
    """Abstract base class for optimized data processing"""
    
    def __init__(self, config: ConfigManager, logger: OptimizedLogger):
        self.config = config
        self.logger = logger
        self.spark = SparkSession.getActiveSession()
        self._setup_spark_optimization()
    
    def _setup_spark_optimization(self):
        """Configure Spark optimizations"""
        opt_config = self.config.get('optimization', {})
        
        if opt_config.get('adaptive_query_execution', True):
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
        if opt_config.get('auto_optimize', True):
            self.spark.conf.set("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
            self.spark.conf.set("spark.databricks.delta.autoOptimize.autoCompact", "true")
    
    @abstractmethod
    def process(self, df: DataFrame, **kwargs) -> DataFrame:
        """Process DataFrame - to be implemented by subclasses"""
        pass
    
    def add_audit_columns(self, df: DataFrame, source_filename: str) -> DataFrame:
        """Add optimized audit columns"""
        current_time_au = F.from_utc_timestamp(
            F.current_timestamp(), 
            self.config.get('timezone.target')
        )
        
        return (df
                .withColumn("source_filename", F.lit(source_filename))
                .withColumn("ingestion_datetime", current_time_au)
                .withColumn("download_date", self._extract_date_from_filename(source_filename)))
    
    @lru_cache(maxsize=100)
    def _extract_date_from_filename(self, filename: str) -> F.Column:
        """Cached date extraction from filename"""
        pattern = self.config.get('processing.filename_date_pattern')
        
        day = F.regexp_extract(F.lit(filename), pattern, 1).cast("int")
        month = F.regexp_extract(F.lit(filename), pattern, 2).cast("int")
        year = F.regexp_extract(F.lit(filename), pattern, 3).cast("int")
        
        return F.when(
            day > 0,
            F.to_date(F.concat(year, F.lit("-"), month, F.lit("-"), day))
        ).otherwise(F.current_date())

# COMMAND ----------

class OptimizedFileManager:
    """File manager with improved performance"""
    
    def __init__(self, config: ConfigManager, logger: OptimizedLogger):
        self.config = config
        self.logger = logger
        self.spark = SparkSession.getActiveSession()
    
    @timing_decorator
    def list_files(self, path: str, pattern: str = None) -> List[Dict[str, Any]]:
        """List files with optimized filtering"""
        try:
            # Use DataFrame operations for better performance
            files_df = (self.spark.sql(f"LIST '{path}'")
                       .filter(F.col("path").endswith(".csv"))
                       .filter(F.col("size") > 0))
            
            if pattern:
                files_df = files_df.filter(F.col("name").rlike(self._convert_pattern_to_regex(pattern)))
            
            files = []
            for row in files_df.collect():
                files.append({
                    'path': row.path,
                    'name': Path(row.path).name,
                    'size': row.size,
                    'modification_time': row.modificationTime
                })
            
            self.logger.info(f"Found {len(files)} files", path=path)
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing files", path=path, error=str(e))
            return []
    
    def _convert_pattern_to_regex(self, pattern: str) -> str:
        """Convert glob pattern to regex"""
        return pattern.replace("*", ".*")
    
    @timing_decorator
    def get_latest_files_by_date(self, files: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Optimized file grouping by date"""
        pattern = self.config.get('processing.filename_date_pattern')
        files_by_date = {}
        
        for file_info in files:
            file_date = self._extract_date_from_filename_string(file_info['name'], pattern)
            if not file_date:
                continue
                
            date_key = file_date.strftime('%Y-%m-%d')
            
            # Keep latest modification time for each date
            if (date_key not in files_by_date or 
                file_info['modification_time'] > files_by_date[date_key]['modification_time']):
                files_by_date[date_key] = file_info
        
        self.logger.info(f"Grouped into {len(files_by_date)} unique dates")
        return files_by_date
    
    @lru_cache(maxsize=1000)
    def _extract_date_from_filename_string(self, filename: str, pattern: str) -> Optional[date]:
        """Cached date extraction from filename string"""
        match = re.search(pattern, filename)
        if match:
            try:
                day, month, year = map(int, match.groups())
                return date(year, month, day)
            except (ValueError, TypeError):
                self.logger.warning(f"Invalid date in filename", filename=filename)
        return None

# COMMAND ----------

class AlertManager:
    """Optimized alert manager with different alert types"""
    
    def __init__(self, logger: OptimizedLogger):
        self.logger = logger
    
    def send_schema_change_alert(self, missing_columns: List[str], extra_columns: List[str] = None):
        """Send structured schema change alert"""
        alert_data = {
            "alert_type": "SCHEMA_CHANGE",
            "missing_columns": missing_columns,
            "extra_columns": extra_columns or [],
            "timestamp": datetime.now().isoformat()
        }
        
        self.logger.error("SCHEMA CHANGE DETECTED", **alert_data)
        # Future: Integrate with external alerting systems
    
    def send_processing_alert(self, message: str, severity: str = "ERROR", **context):
        """Send generic processing alert with context"""
        alert_data = {
            "alert_type": "PROCESSING",
            "severity": severity,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            **context
        }
        
        log_method = getattr(self.logger, severity.lower(), self.logger.error)
        log_method("PROCESSING ALERT", **alert_data)

# COMMAND ----------

class DeltaTableOptimizer:
    """Handles Delta table optimizations"""
    
    def __init__(self, spark: SparkSession, logger: OptimizedLogger):
        self.spark = spark
        self.logger = logger
    
    @timing_decorator
    def optimize_table(self, table_name: str, z_order_columns: List[str] = None):
        """Optimize Delta table with optional Z-ordering"""
        try:
            if z_order_columns:
                z_order_clause = f"ZORDER BY ({', '.join(z_order_columns)})"
                self.spark.sql(f"OPTIMIZE {table_name} {z_order_clause}")
                self.logger.info(f"Optimized table with Z-ordering", 
                               table=table_name, z_order_columns=z_order_columns)
            else:
                self.spark.sql(f"OPTIMIZE {table_name}")
                self.logger.info(f"Optimized table", table=table_name)
        except Exception as e:
            self.logger.error(f"Table optimization failed", table=table_name, error=str(e))
    
    @timing_decorator
    def vacuum_table(self, table_name: str, retention_hours: int = 168):
        """Vacuum Delta table to remove old files"""
        try:
            self.spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
            self.logger.info(f"Vacuumed table", table=table_name, retention_hours=retention_hours)
        except Exception as e:
            self.logger.error(f"Table vacuum failed", table=table_name, error=str(e))

# COMMAND ----------

class CacheManager:
    """Manages DataFrame caching for optimization"""
    
    def __init__(self, spark: SparkSession, logger: OptimizedLogger):
        self.spark = spark
        self.logger = logger
        self._cached_dfs = set()
    
    def cache_if_beneficial(self, df: DataFrame, threshold_actions: int = 2) -> DataFrame:
        """Cache DataFrame if it will be used multiple times"""
        # Simple heuristic: cache if DataFrame is likely to be reused
        if df.isStreaming or df.count() < 1000:  # Don't cache small or streaming DataFrames
            return df
            
        cached_df = df.cache()
        self._cached_dfs.add(id(cached_df))
        self.logger.debug(f"Cached DataFrame", df_id=id(cached_df))
        return cached_df
    
    def clear_cache(self):
        """Clear all managed caches"""
        self.spark.catalog.clearCache()
        self._cached_dfs.clear()
        self.logger.debug("Cleared all caches")