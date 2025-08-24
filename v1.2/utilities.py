# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Functions Module
# MAGIC Production-ready utility functions for the data pipeline

# COMMAND ----------

import json
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pytz

# COMMAND ----------

class ConfigManager:
    """Manages pipeline configuration"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
    
    def _load_config(self, path: str) -> Dict:
        """Load configuration from JSON file"""
        with open(path, 'r') as f:
            return json.load(f)
    
    def get(self, key_path: str, default=None):
        """Get config value using dot notation (e.g., 'source.container')"""
        keys = key_path.split('.')
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default

# COMMAND ----------

class FileUtils:
    """File processing utilities"""
    
    @staticmethod
    def extract_date_from_filename(filename: str) -> Optional[date]:
        """Extract date from filename format: (6_8_2025) or (19_08_2025)"""
        pattern = r'\((\d{1,2})_(\d{1,2})_(\d{4})\)'
        match = re.search(pattern, filename)
        if match:
            day, month, year = map(int, match.groups())
            try:
                return date(year, month, day)
            except ValueError:
                return None
        return None
    
    @staticmethod
    def get_latest_files_per_date(file_list: List[Tuple[str, datetime]]) -> List[str]:
        """Get latest file for each date based on modification time"""
        date_files = {}
        
        for filepath, mod_time in file_list:
            filename = filepath.split('/')[-1]
            file_date = FileUtils.extract_date_from_filename(filename)
            
            if file_date:
                if file_date not in date_files or mod_time > date_files[file_date][1]:
                    date_files[file_date] = (filepath, mod_time)
        
        return [filepath for filepath, _ in date_files.values()]

# COMMAND ----------

class TimezoneConverter:
    """Timezone conversion utilities"""
    
    @staticmethod
    def convert_utc_to_sydney(utc_timestamp: str) -> datetime:
        """Convert UTC timestamp to Sydney timezone"""
        try:
            # Parse various datetime formats
            formats = ['%d/%m/%Y %H:%M', '%m/%d/%Y %H:%M', '%Y-%m-%d %H:%M:%S']
            dt = None
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(utc_timestamp, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                raise ValueError(f"Unable to parse datetime: {utc_timestamp}")
            
            # Assume UTC and convert to Sydney
            utc_tz = pytz.UTC
            sydney_tz = pytz.timezone('Australia/Sydney')
            
            dt_utc = utc_tz.localize(dt)
            dt_sydney = dt_utc.astimezone(sydney_tz)
            
            return dt_sydney.replace(tzinfo=None)  # Remove timezone info for Spark
            
        except Exception as e:
            print(f"Error converting timestamp {utc_timestamp}: {str(e)}")
            return None

# COMMAND ----------

class SchemaValidator:
    """Schema validation utilities"""
    
    def __init__(self, expected_schema: Dict[str, str]):
        self.expected_schema = expected_schema
    
    def validate_schema(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame schema against expected schema"""
        issues = []
        df_columns = set(df.columns)
        expected_columns = set(self.expected_schema.keys()) - {'source_filename', 'ingestion_datetime', 'download_date'}
        
        # Check for missing columns
        missing_cols = expected_columns - df_columns
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check for extra columns
        extra_cols = df_columns - expected_columns
        if extra_cols:
            issues.append(f"Extra columns: {extra_cols}")
        
        return len(issues) == 0, issues

# COMMAND ----------

class DataProcessor:
    """Data processing utilities"""
    
    @staticmethod
    def clean_adviser_name(adviser_name: str) -> str:
        """Clean adviser name by removing special characters and standardizing format"""
        if not adviser_name:
            return ""
        
        # Remove special characters and extra spaces
        cleaned = re.sub(r'[*\-_]+', '', adviser_name)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        cleaned = re.sub(r'_inactive$', '', cleaned, flags=re.IGNORECASE)
        
        return cleaned.lower()
    
    @staticmethod
    def apply_schema_transformations(df: DataFrame, config: ConfigManager) -> DataFrame:
        """Apply schema transformations to raw DataFrame"""
        sydney_tz = pytz.timezone('Australia/Sydney')
        current_time = datetime.now(sydney_tz).replace(tzinfo=None)
        
        # Convert date_submitted to Sydney timezone
        df = df.withColumn(
            "date_submitted_sydney",
            when(col("date_submitted").isNotNull(), 
                 from_utc_timestamp(
                     to_timestamp(col("date_submitted"), "d/M/yyyy H:mm"),
                     "Australia/Sydney"
                 )
            )
        ).drop("date_submitted").withColumnRenamed("date_submitted_sydney", "date_submitted")
        
        # Add ingestion datetime (current Sydney time)
        df = df.withColumn("ingestion_datetime", lit(current_time))
        
        return df

# COMMAND ----------

class LogManager:
    """Logging utilities"""
    
    def __init__(self, spark: SparkSession, config: ConfigManager):
        self.spark = spark
        self.config = config
        self.log_table = f"{config.get('target.catalog')}.{config.get('target.schema')}.{config.get('target.log_table')}"
    
    def log_ingestion(self, filename: str, records_processed: int, status: str, message: str = ""):
        """Log ingestion details"""
        sydney_tz = pytz.timezone('Australia/Sydney')
        log_time = datetime.now(sydney_tz).replace(tzinfo=None)
        
        log_data = [
            (filename, records_processed, status, message, log_time)
        ]
        
        log_df = self.spark.createDataFrame(
            log_data,
            ["filename", "records_processed", "status", "message", "log_datetime"]
        )
        
        log_df.write.mode("append").saveAsTable(self.log_table)