# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation (PySpark)
# MAGIC Creates silver_edge_draft table with adviser code enrichment using PySpark

# COMMAND ----------

# MAGIC %run ./utils_module

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import re
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Setup

# COMMAND ----------

# Initialize components
spark = SparkSession.builder.getOrCreate()
config = ConfigManager("/Workspace/Shared/config/pipeline_config.json")
log_manager = LogManager(spark, config)

# Get table names from config
catalog = config.get('target.catalog')
schema = config.get('target.schema')
bronze_table = f"{catalog}.{schema}.{config.get('target.bronze_table')}"
silver_table = f"{catalog}.{schema}.{config.get('target.silver_table')}"
org_table = f"{catalog}.{schema}.{config.get('reference_tables.organization_table')}"
dynamic_table = f"{catalog}.{schema}.{config.get('reference_tables.dynamic_mapping_table')}"

print(f"Processing: {bronze_table} -> {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adviser Name Matching Functions

# COMMAND ----------

class AdviserMatcher:
    """Advanced adviser name matching with fuzzy logic"""
    
    @staticmethod
    def clean_adviser_name(name: str) -> str:
        """Clean and standardize adviser name"""
        if not name:
            return ""
        
        # Convert to lowercase for comparison
        name = name.lower()
        
        # Remove common prefixes and suffixes
        name = re.sub(r'^(mr\.?|mrs\.?|ms\.?|dr\.?)\s+', '', name)
        name = re.sub(r'\s+(jr\.?|sr\.?|ii|iii)$', '', name)
        
        # Remove special characters and formatting
        name = re.sub(r'[*\-_]+', '', name)
        name = re.sub(r'\s+', ' ', name)
        
        # Remove inactive markers
        name = re.sub(r'[_\-\s]*inactive\s*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'[_\-\s]*\(inactive\)\s*$', '', name, flags=re.IGNORECASE)
        
        return name.strip()
    
    @staticmethod
    def calculate_similarity(name1: str, name2: str) -> float:
        """Calculate similarity score between two names"""
        if not name1 or not name2:
            return 0.0
            
        # Clean both names
        clean1 = AdviserMatcher.clean_adviser_name(name1)
        clean2 = AdviserMatcher.clean_adviser_name(name2)
        
        if clean1 == clean2:
            return 1.0
        
        # Check if one contains the other
        if clean1 in clean2 or clean2 in clean1:
            return 0.9
        
        # Use sequence matcher for fuzzy matching
        return SequenceMatcher(None, clean1, clean2).ratio()
    
    @staticmethod
    def find_best_matches(bronze_advisers: List[Tuple], org_advisers: List[Tuple]) -> Dict[Tuple, Tuple]:
        """Find best matches between bronze and organization advisers"""
        matches = {}
        
        for bronze_adviser, bronze_practice in bronze_advisers:
            best_match = None
            best_score = 0.0
            best_advisor_code = None
            
            # Find matching practice first, then match adviser within that practice
            for org_adviser, org_practice, advisor_code in org_advisers:
                # Practice must match (case-insensitive)
                if bronze_practice.lower() != org_practice.lower():
                    continue
                
                # Calculate adviser name similarity
                similarity = AdviserMatcher.calculate_similarity(bronze_adviser, org_adviser)
                
                # Use threshold of 0.7 for matching
                if similarity >= 0.7 and similarity > best_score:
                    best_score = similarity
                    best_match = (org_adviser, org_practice, advisor_code)
                    best_advisor_code = advisor_code
            
            if best_match:
                matches[(bronze_adviser, bronze_practice)] = {
                    'advisor_code': best_advisor_code,
                    'matched_adviser': best_match[0],
                    'similarity_score': best_score
                }
        
        return matches

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Transformation Pipeline

# COMMAND ----------

class SilverTransformation:
    """Silver layer transformation pipeline"""
    
    def __init__(self, config: ConfigManager, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.matcher = AdviserMatcher()
    
    def load_bronze_data(self) -> DataFrame:
        """Load bronze data"""
        df = spark.table(bronze_table)
        print(f"Loaded {df.count()} records from bronze table")
        return df
    
    def load_organization_data(self) -> DataFrame:
        """Load organization reference data with latest advisor codes"""
        # Get latest advisor code for each adviser-practice combination
        org_df = spark.table(org_table)
        
        # Window to get latest advisor code per adviser
        window_spec = Window.partitionBy("advisorname", "companyname").orderBy(desc("advisorcode"))
        
        latest_org = org_df.withColumn(
            "rn", row_number().over(window_spec)
        ).filter(
            col("rn") == 1
        ).select(
            "advisorname", 
            "companyname", 
            "advisorcode"
        ).filter(
            col("advisorname").isNotNull() & 
            col("advisorcode").isNotNull() &
            col("companyname").isNotNull()
        )
        
        print(f"Loaded {latest_org.count()} unique advisor records from organization table")
        return latest_org
    
    def load_dynamic_mapping(self) -> DataFrame:
        """Load dynamic mapping data"""
        dynamic_df = spark.table(dynamic_table)
        print(f"Loaded {dynamic_df.count()} records from dynamic mapping table")
        return dynamic_df
    
    def perform_adviser_matching(self, bronze_df: DataFrame, org_df: DataFrame) -> DataFrame:
        """Perform advanced adviser matching using Python logic"""
        
        # Collect unique bronze adviser-practice combinations
        bronze_advisers = bronze_df.select("adviser", "practice").distinct().collect()
        bronze_list = [(row.adviser, row.practice) for row in bronze_advisers]
        
        # Collect organization data
        org_advisers = org_df.collect()
        org_list = [(row.advisorname, row.companyname, row.advisorcode) for row in org_advisers]
        
        print(f"Matching {len(bronze_list)} bronze advisers against {len(org_list)} organization advisers")
        
        # Perform matching using Python logic
        matches = self.matcher.find_best_matches(bronze_list, org_list)
        
        print(f"Found {len(matches)} matches out of {len(bronze_list)} bronze advisers")
        
        # Convert matches to DataFrame for joining
        match_data = []
        for (bronze_adviser, bronze_practice), match_info in matches.items():
            match_data.append((
                bronze_adviser,
                bronze_practice, 
                match_info['advisor_code'],
                match_info['matched_adviser'],
                match_info['similarity_score']
            ))
        
        # Create matches DataFrame
        match_schema = StructType([
            StructField("bronze_adviser", StringType(), True),
            StructField("bronze_practice", StringType(), True),
            StructField("advisor_code", StringType(), True),
            StructField("matched_adviser_name", StringType(), True),
            StructField("similarity_score", DoubleType(), True)
        ])
        
        matches_df = spark.createDataFrame(match_data, match_schema)
        
        # Join with bronze data
        enriched_df = bronze_df.join(
            matches_df,
            (bronze_df.adviser == matches_df.bronze_adviser) & 
            (bronze_df.practice == matches_df.bronze_practice),
            "left"
        ).select(
            bronze_df["*"],
            coalesce(matches_df.advisor_code, lit("UNKNOWN")).alias("advisor_code"),
            coalesce(matches_df.matched_adviser_name, bronze_df.adviser).alias("standardized_adviser_name"),
            coalesce(matches_df.similarity_score, lit(0.0)).alias("match_confidence")
        )
        
        return enriched_df
    
    def add_dynamic_mapping(self, enriched_df: DataFrame, dynamic_df: DataFrame) -> DataFrame:
        """Add dynamic mapping information"""
        
        # Join with dynamic mapping on advisor_code
        final_df = enriched_df.join(
            dynamic_df,
            enriched_df.advisor_code == dynamic_df.advisercode,
            "left"
        ).select(
            enriched_df["*"],
            dynamic_df.dynamic_id,
            dynamic_df.business_date
        )
        
        return final_df
    
    def create_silver_table(self, final_df: DataFrame) -> None:
        """Create or replace silver table"""
        
        # Add processing metadata
        sydney_tz = pytz.timezone('Australia/Sydney')
        processing_time = datetime.now(sydney_tz).replace(tzinfo=None)
        
        final_df = final_df.withColumn("silver_processing_datetime", lit(processing_time))
        
        # Write to silver table with partitioning
        final_df.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("download_date") \
            .saveAsTable(silver_table)
        
        # Optimize table
        spark.sql(f"OPTIMIZE {silver_table} ZORDER BY (practice, adviser)")
        
        # Set table properties
        spark.sql(f"""
            ALTER TABLE {silver_table} SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'delta.enableChangeDataFeed' = 'true'
            )
        """)
        
        print(f"Silver table created successfully with {final_df.count()} records")
    
    def incremental_update(self) -> None:
        """Perform incremental update for new bronze data"""
        
        # Get latest processing time from silver table
        try:
            latest_processing = spark.sql(f"""
                SELECT COALESCE(MAX(ingestion_datetime), '1900-01-01') as max_ingestion
                FROM {silver_table}
            """).collect()[0]['max_ingestion']
        except Exception:
            # Table doesn't exist, treat as full refresh
            latest_processing = datetime(1900, 1, 1)
        
        # Get new bronze records
        new_bronze = spark.table(bronze_table).filter(
            col("ingestion_datetime") > lit(latest_processing)
        )
        
        new_count = new_bronze.count()
        if new_count == 0:
            print("No new records to process")
            return
        
        print(f"Processing {new_count} new bronze records incrementally")
        
        # Load reference data
        org_df = self.load_organization_data()
        dynamic_df = self.load_dynamic_mapping()
        
        # Perform transformations
        enriched_df = self.perform_adviser_matching(new_bronze, org_df)
        final_df = self.add_dynamic_mapping(enriched_df, dynamic_df)
        
        # Add processing metadata
        sydney_tz = pytz.timezone('Australia/Sydney')
        processing_time = datetime.now(sydney_tz).replace(tzinfo=None)
        final_df = final_df.withColumn("silver_processing_datetime", lit(processing_time))
        
        # Merge with existing silver data
        final_df.createOrReplaceTempView("new_silver_data")
        
        spark.sql(f"""
            MERGE INTO {silver_table} target
            USING (
                SELECT *, 
                       ROW_NUMBER() OVER (
                           PARTITION BY download_date, account_name, practice, adviser 
                           ORDER BY ingestion_datetime DESC
                       ) as rn
                FROM new_silver_data
            ) source
            ON target.download_date = source.download_date 
               AND target.account_name = source.account_name 
               AND target.practice = source.practice 
               AND target.adviser = source.adviser
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED AND source.rn = 1 THEN INSERT *
        """)
        
        print(f"Incremental update completed for {new_count} records")
    
    def run_full_transformation(self) -> None:
        """Run complete silver transformation"""
        try:
            print("Starting silver layer transformation...")
            
            # Load data
            bronze_df = self.load_bronze_data()
            org_df = self.load_organization_data()
            dynamic_df = self.load_dynamic_mapping()
            
            # Perform transformations
            enriched_df = self.perform_adviser_matching(bronze_df, org_df)
            final_df = self.add_dynamic_mapping(enriched_df, dynamic_df)
            
            # Create silver table
            self.create_silver_table(final_df)
            
            # Log success
            record_count = final_df.count()
            log_manager.log_ingestion(
                "silver_transformation", 
                record_count, 
                "SUCCESS", 
                f"Silver transformation completed with {record_count} records"
            )
            
            print("Silver layer transformation completed successfully!")
            
        except Exception as e:
            error_msg = f"Silver transformation failed: {str(e)}"
            print(error_msg)
            log_manager.log_ingestion("silver_transformation", 0, "FAILED", error_msg)
            raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Parameters

# COMMAND ----------

# Parameters
dbutils.widgets.text("mode", "full", "Processing Mode (full/incremental)")
mode = dbutils.widgets.get("mode")

print(f"Running silver transformation in {mode} mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Transformation

# COMMAND ----------

# Initialize and run transformation
transformation = SilverTransformation(config, spark)

if mode == "incremental":
    transformation.incremental_update()
else:
    transformation.run_full_transformation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation

# COMMAND ----------

class SilverDataQuality:
    """Data quality validation for silver layer"""
    
    def __init__(self, spark: SparkSession, silver_table: str):
        self.spark = spark
        self.silver_table = silver_table
    
    def run_quality_checks(self):
        """Run comprehensive data quality checks"""
        
        print("=== SILVER TABLE DATA QUALITY REPORT ===")
        
        df = spark.table(self.silver_table)
        
        # Basic statistics
        total_records = df.count()
        print(f"\n📊 Basic Statistics:")
        print(f"   Total records: {total_records:,}")
        
        # Date range
        date_stats = df.select(
            min("download_date").alias("min_date"),
            max("download_date").alias("max_date"),
            countDistinct("download_date").alias("unique_dates")
        ).collect()[0]
        
        print(f"   Date range: {date_stats.min_date} to {date_stats.max_date}")
        print(f"   Unique dates: {date_stats.unique_dates}")
        
        # Adviser matching statistics
        matching_stats = df.select(
            sum(when(col("advisor_code") != "UNKNOWN", 1).otherwise(0)).alias("matched"),
            sum(when(col("advisor_code") == "UNKNOWN", 1).otherwise(0)).alias("unmatched"),
            avg("match_confidence").alias("avg_confidence")
        ).collect()[0]
        
        match_rate = (matching_stats.matched / total_records) * 100 if total_records > 0 else 0
        
        print(f"\n🎯 Adviser Matching Results:")
        print(f"   Matched advisers: {matching_stats.matched:,} ({match_rate:.1f}%)")
        print(f"   Unmatched advisers: {matching_stats.unmatched:,}")
        print(f"   Average confidence: {matching_stats.avg_confidence:.3f}")
        
        # Dynamic mapping statistics
        dynamic_stats = df.select(
            sum(when(col("dynamic_id").isNotNull(), 1).otherwise(0)).alias("with_dynamic"),
            sum(when(col("dynamic_id").isNull(), 1).otherwise(0)).alias("without_dynamic")
        ).collect()[0]
        
        dynamic_rate = (dynamic_stats.with_dynamic / total_records) * 100 if total_records > 0 else 0
        
        print(f"\n🔗 Dynamic Mapping Results:")
        print(f"   With dynamic mapping: {dynamic_stats.with_dynamic:,} ({dynamic_rate:.1f}%)")
        print(f"   Without dynamic mapping: {dynamic_stats.without_dynamic:,}")
        
        # Top practices and advisers
        print(f"\n🏢 Top 10 Practices:")
        df.groupBy("practice").count().orderBy(desc("count")).limit(10).show()
        
        print(f"\n👤 Top 10 Advisers:")
        df.groupBy("adviser", "advisor_code").count().orderBy(desc("count")).limit(10).show()
        
        # Data quality issues
        print(f"\n⚠️  Data Quality Issues:")
        
        null_checks = df.select([
            sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_nulls") 
            for c in ["account_name", "practice", "adviser", "download_date"]
        ]).collect()[0].asDict()
        
        for col_name, null_count in null_checks.items():
            if null_count > 0:
                print(f"   {col_name}: {null_count} null values")
        
        # Sample unmatched advisers
        unmatched_sample = df.filter(col("advisor_code") == "UNKNOWN") \
            .select("adviser", "practice") \
            .distinct() \
            .limit(10)
        
        if unmatched_sample.count() > 0:
            print(f"\n🔍 Sample Unmatched Advisers:")
            unmatched_sample.show(truncate=False)
        
        print("=== END DATA QUALITY REPORT ===\n")

# COMMAND ----------

# Run data quality validation
quality_checker = SilverDataQuality(spark, silver_table)
quality_checker.run_quality_checks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Metrics

# COMMAND ----------

# Show recent processing performance
print("=== RECENT PROCESSING PERFORMANCE ===")

log_table = f"{config.get('target.catalog')}.{config.get('target.schema')}.{config.get('target.log_table')}"

# Recent logs
recent_performance = spark.sql(f"""
    SELECT 
        filename,
        records_processed,
        status,
        log_datetime,
        message
    FROM {log_table}
    WHERE log_datetime >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    ORDER BY log_datetime DESC
    LIMIT 20
""")

recent_performance.show(truncate=False)

# Processing summary
processing_summary = spark.sql(f"""
    SELECT 
        status,
        COUNT(*) as run_count,
        AVG(records_processed) as avg_records,
        SUM(records_processed) as total_records
    FROM {log_table}
    WHERE log_datetime >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
    GROUP BY status
    ORDER BY status
""")

print("\n📈 7-Day Processing Summary:")
processing_summary.show()

print("Transformation complete! ✅")