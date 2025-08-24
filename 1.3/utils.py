# Databricks notebook source
# MAGIC %python
# MAGIC import json
# MAGIC from datetime import datetime, timezone
# MAGIC from typing import Dict, List, Optional, Tuple
# MAGIC import re
# MAGIC from pyspark.sql import SparkSession, DataFrame
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC import pytz

class ConfigManager:
    """Manages configuration loading and validation"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise Exception(f"Failed to load config: {str(e)}")
    
    def get(self, key_path: str, default=None):
        """Get configuration value using dot notation"""
        keys = key_path.split('.')
        value = self.config
        for key in keys:
            value = value.get(key, default)
            if value is None:
                return default
        return value

class TimeUtils:
    """Utility functions for time operations"""
    
    @staticmethod
    def convert_timezone(dt_str: str, source_tz: str = "UTC", target_tz: str = "Australia/Sydney") -> datetime:
        """Convert datetime string from source timezone to target timezone"""
        try:
            # Parse various date formats
            formats = ["%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"]
            dt = None
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(dt_str, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                raise ValueError(f"Unable to parse datetime: {dt_str}")
            
            # Localize to source timezone
            source_tz_obj = pytz.timezone(source_tz)
            target_tz_obj = pytz.timezone(target_tz)
            
            dt_localized = source_tz_obj.localize(dt)
            dt_converted = dt_localized.astimezone(target_tz_obj)
            
            return dt_converted
        except Exception as e:
            raise Exception(f"Timezone conversion failed: {str(e)}")
    
    @staticmethod
    def get_current_au_time() -> datetime:
        """Get current time in Australia/Sydney timezone"""
        au_tz = pytz.timezone("Australia/Sydney")
        return datetime.now(au_tz)

class FileUtils:
    """Utility functions for file operations"""
    
    @staticmethod
    def extract_date_from_filename(filename: str, pattern: str) -> Optional[str]:
        """Extract date from filename using regex pattern"""
        try:
            match = re.search(pattern, filename)
            if match:
                day, month, year = match.groups()
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            return None
        except Exception:
            return None
    
    @staticmethod
    def get_latest_file_per_date(files_info: List[Dict]) -> List[Dict]:
        """Get the latest file for each date based on modification time"""
        from collections import defaultdict
        
        files_by_date = defaultdict(list)
        
        # Group files by date
        for file_info in files_info:
            date_key = file_info.get('download_date')
            if date_key:
                files_by_date[date_key].append(file_info)
        
        # Get latest file for each date
        latest_files = []
        for date_key, files in files_by_date.items():
            latest_file = max(files, key=lambda x: x.get('modification_time', 0))
            latest_files.append(latest_file)
        
        return latest_files

class SchemaValidator:
    """Validates DataFrame schemas"""
    
    def __init__(self, expected_schema: Dict[str, str]):
        self.expected_schema = expected_schema
    
    def validate(self, df: DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame schema against expected schema"""
        errors = []
        df_columns = set(df.columns)
        expected_columns = set(self.expected_schema.keys())
        
        # Check for missing columns
        missing = expected_columns - df_columns
        if missing:
            errors.append(f"Missing columns: {missing}")
        
        # Check for extra columns
        extra = df_columns - expected_columns
        if extra:
            errors.append(f"Extra columns: {extra}")
        
        return len(errors) == 0, errors

class Logger:
    """Simple logging utility for Databricks"""
    
    @staticmethod
    def log_ingestion(spark: SparkSession, config: ConfigManager, 
                     source_file: str, records_count: int, status: str, 
                     error_message: str = None):
        """Log ingestion details to log table"""
        try:
            log_data = [{
                "ingestion_id": f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(source_file)}",
                "source_file": source_file,
                "ingestion_datetime": TimeUtils.get_current_au_time(),
                "records_processed": records_count,
                "status": status,
                "error_message": error_message
            }]
            
            log_df = spark.createDataFrame(log_data)
            log_table = config.get("destinations.log_table")
            
            log_df.write.mode("append").saveAsTable(log_table)
            
        except Exception as e:
            print(f"Failed to log ingestion: {str(e)}")

# COMMAND ----------

# Test configuration loading
def test_config():
    """Test function for configuration"""
    try:
        config = ConfigManager("/Workspace/path/to/config.json")
        print("Config loaded successfully")
        print(f"Bronze table: {config.get('destinations.bronze_table')}")
        return True
    except Exception as e:
        print(f"Config test failed: {str(e)}")
        return False