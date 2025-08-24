# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

class SilverTransformation:
    """Handles transformation from bronze to silver layer"""
    
    def __init__(self, config: ConfigManager, spark: SparkSession):
        self.config = config
        self.spark = spark
    
    def _get_bronze_data(self) -> DataFrame:
        """Get data from bronze table"""
        bronze_table = self.config.get("destinations.bronze_table")
        return self.spark.table(bronze_table)
    
    def _get_organization_data(self) -> DataFrame:
        """Get organization reference data"""
        org_table = self.config.get("reference_tables.organization_table")
        return self.spark.table(org_table)
    
    def _fuzzy_match_adviser(self, bronze_df: DataFrame, org_df: DataFrame) -> DataFrame:
        """
        Perform fuzzy matching between adviser names in bronze and organization tables
        Returns bronze data with matched adviser codes
        """
        # Create broadcast for smaller organization table
        org_broadcast = broadcast(org_df.select("adviser_name", "company_name", "adviser_code", "created_date"))
        
        # Join conditions with fuzzy matching logic
        # Case 1: Exact match on adviser name
        exact_match = bronze_df.alias("b").join(
            org_broadcast.alias("o"),
            (lower(trim(col("b.adviser"))) == lower(trim(col("o.adviser_name")))) &
            (lower(trim(col("b.practice"))) == lower(trim(col("o.company_name")))),
            "left"
        ).select("b.*", col("o.adviser_code").alias("matched_adviser_code"), 
                col("o.created_date").alias("org_created_date"))
        
        # Case 2: Bronze adviser name contains organization adviser name
        bronze_contains_org = bronze_df.alias("b").join(
            org_broadcast.alias("o"),
            (lower(trim(col("b.adviser"))).contains(lower(trim(col("o.adviser_name"))))) &
            (lower(trim(col("b.practice"))) == lower(trim(col("o.company_name")))) &
            (length(trim(col("o.adviser_name"))) >= 3),  # Avoid short name matches
            "left"
        ).select("b.*", col("o.adviser_code").alias("matched_adviser_code"),
                col("o.created_date").alias("org_created_date"))
        
        # Case 3: Organization adviser name contains bronze adviser name
        org_contains_bronze = bronze_df.alias("b").join(
            org_broadcast.alias("o"),
            (lower(trim(col("o.adviser_name"))).contains(lower(trim(col("b.adviser"))))) &
            (lower(trim(col("b.practice"))) == lower(trim(col("o.company_name")))) &
            (length(trim(col("b.adviser"))) >= 3),  # Avoid short name matches
            "left"
        ).select("b.*", col("o.adviser_code").alias("matched_adviser_code"),
                col("o.created_date").alias("org_created_date"))
        
        # Union all matches and get the best match per record
        all_matches = exact_match.union(bronze_contains_org).union(org_contains_bronze)
        
        # Remove duplicates and get latest adviser code for each record
        window_spec = Window.partitionBy("date_submitted", "account_name", "adviser", "practice") \
                           .orderBy(desc("org_created_date"), desc("matched_adviser_code"))
        
        result = all_matches.withColumn("row_num", row_number().over(window_spec)) \
                           .filter(col("row_num") == 1) \
                           .drop("row_num", "org_created_date")
        
        return result
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business rules and data quality checks"""
        # Add transformation timestamp
        df = df.withColumn("silver_processed_datetime", 
                          lit(TimeUtils.get_current_au_time().isoformat()).cast(TimestampType()))
        
        # Data quality flags
        df = df.withColumn("data_quality_flags", 
                          when(col("matched_adviser_code").isNull(), "ADVISER_NOT_MATCHED")
                          .otherwise(lit(None)))
        
        # Clean and standardize data
        df = df.withColumn("adviser", trim(col("adviser"))) \
              .withColumn("practice", trim(col("practice"))) \
              .withColumn("account_name", trim(col("account_name"))) \
              .withColumn("account_type", trim(col("account_type")))
        
        return df
    
    def _save_to_silver(self, df: DataFrame, full_refresh: bool = False):
        """Save DataFrame to silver table"""
        silver_table = self.config.get("destinations.silver_table")
        
        if full_refresh:
            # Full refresh - overwrite table
            df.write.mode("overwrite").saveAsTable(silver_table)
        else:
            # Incremental - merge logic
            temp_view = "temp_silver_data"
            df.createOrReplaceTempView(temp_view)
            
            # Create table if not exists
            try:
                self.spark.sql(f"DESCRIBE TABLE {silver_table}")
            except Exception:
                df.write.mode("overwrite").saveAsTable(silver_table)
                return
            
            # Merge strategy: delete existing records for the same download_date, then insert new ones
            dates_to_replace = [row.download_date for row in 
                              self.spark.sql(f"SELECT DISTINCT download_date FROM {temp_view}").collect()]
            
            if dates_to_replace:
                date_filter = ", ".join([f"'{date}'" for date in dates_to_replace])
                self.spark.sql(f"""
                    DELETE FROM {silver_table} 
                    WHERE download_date IN ({date_filter})
                """)
            
            # Insert new data
            df.write.mode("append").saveAsTable(silver_table)
    
    def transform_to_silver(self, full_refresh: bool = False) -> bool:
        """Main transformation method"""
        try:
            print("Starting silver transformation...")
            
            # Get bronze data
            bronze_df = self._get_bronze_data()
            bronze_count = bronze_df.count()
            print(f"Bronze records to process: {bronze_count}")
            
            if bronze_count == 0:
                print("No bronze data to process")
                return True
            
            # Get organization data
            org_df = self._get_organization_data()
            org_count = org_df.count()
            print(f"Organization reference records: {org_count}")
            
            # Perform fuzzy matching
            matched_df = self._fuzzy_match_adviser(bronze_df, org_df)
            
            # Apply business rules
            silver_df = self._apply_business_rules(matched_df)
            
            # Check match rate
            total_records = silver_df.count()
            matched_records = silver_df.filter(col("matched_adviser_code").isNotNull()).count()
            match_rate = (matched_records / total_records) * 100 if total_records > 0 else 0
            
            print(f"Total records: {total_records}")
            print(f"Matched records: {matched_records}")
            print(f"Match rate: {match_rate:.2f}%")
            
            # Save to silver table
            self._save_to_silver(silver_df, full_refresh)
            
            # Log transformation
            Logger.log_ingestion(
                self.spark, self.config, 
                "SILVER_TRANSFORMATION", 
                total_records, 
                "SUCCESS", 
                f"Match rate: {match_rate:.2f}%"
            )
            
            print("Silver transformation completed successfully")
            return True
            
        except Exception as e:
            print(f"Silver transformation failed: {str(e)}")
            Logger.log_ingestion(
                self.spark, self.config, 
                "SILVER_TRANSFORMATION", 
                0, "FAILED", str(e)
            )
            return False

class SilverOrchestrator:
    """Orchestrates the silver transformation process"""
    
    def __init__(self, config_path: str):
        self.config = ConfigManager(config_path)
        self.spark = SparkSession.getActiveSession()
        self.transformer = SilverTransformation(self.config, self.spark)
    
    def run_transformation(self, full_refresh: bool = False) -> bool:
        """Run the silver transformation"""
        return self.transformer.transform_to_silver(full_refresh)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution Cell

# COMMAND ----------

# Main execution
def main():
    # Get parameters
    full_refresh = dbutils.widgets.get("full_refresh") == "true" if dbutils.widgets.get("full_refresh") else False
    config_path = dbutils.widgets.get("config_path") if dbutils.widgets.get("config_path") else "/Workspace/config.json"
    
    print(f"Starting silver transformation - Full Refresh: {full_refresh}")
    
    # Run transformation
    orchestrator = SilverOrchestrator(config_path)
    success = orchestrator.run_transformation(full_refresh=full_refresh)
    
    if success:
        print("Silver transformation completed successfully")
        dbutils.notebook.exit("SUCCESS")
    else:
        print("Silver transformation failed")
        dbutils.notebook.exit("FAILED")

# Set up widgets for parameters
dbutils.widgets.text("full_refresh", "false", "Full Refresh")
dbutils.widgets.text("config_path", "/Workspace/config.json", "Config Path")

# Run main function
if __name__ == "__main__":
    main()