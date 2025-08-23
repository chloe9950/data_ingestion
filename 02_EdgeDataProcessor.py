# Databricks notebook source
# MAGIC %md
# MAGIC # Edge Data Processor - Clean & Optimized
# MAGIC Enhanced processor with advanced organization matching and dynamic mapping

# COMMAND ----------

# MAGIC %run ./01_BasePipeline

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import *
from delta.tables import DeltaTable
import re

# COMMAND ----------

class OptimizedEdgeProcessor(DataProcessor):
    """Optimized Edge data processor with enhanced functionality"""
    
    @timing_decorator
    def process(self, df: DataFrame, source_filename: str, **kwargs) -> DataFrame:
        """Process Edge data with optimizations"""
        self.logger.info(f"Processing Edge data", filename=source_filename, input_rows=df.count())
        
        # Cache input if beneficial
        cache_manager = kwargs.get('cache_manager')
        if cache_manager:
            df = cache_manager.cache_if_beneficial(df)
        
        # Process in pipeline
        processed_df = (df
                       .transform(lambda x: self.add_audit_columns(x, source_filename))
                       .transform(self._convert_date_submitted)
                       .transform(self._clean_and_standardize)
                       .transform(self._add_data_quality_metrics))
        
        self.logger.info(f"Processing complete", output_rows=processed_df.count())
        return processed_df
    
    def _convert_date_submitted(self, df: DataFrame) -> DataFrame:
        """Optimized date conversion with better error handling"""
        date_formats = self.config.get('processing.date_formats', ['d/MM/yyyy HH:mm'])
        target_tz = self.config.get('timezone.target', 'Australia/Sydney')
        
        # Try each format in sequence
        converted_df = df
        for i, fmt in enumerate(date_formats):
            col_name = f"date_attempt_{i}"
            converted_df = converted_df.withColumn(col_name, F.to_timestamp(F.col("date_submitted"), fmt))
        
        # Coalesce all attempts
        attempt_cols = [F.col(f"date_attempt_{i}") for i in range(len(date_formats))]
        converted_df = (converted_df
                       .withColumn("date_submitted_utc", F.coalesce(*attempt_cols))
                       .withColumn("date_submitted", 
                                 F.from_utc_timestamp(F.col("date_submitted_utc"), target_tz))
                       .drop(*[f"date_attempt_{i}" for i in range(len(date_formats))], "date_submitted_utc"))
        
        return converted_df
    
    def _clean_and_standardize(self, df: DataFrame) -> DataFrame:
        """Enhanced data cleaning and standardization"""
        # Define cleaning rules
        string_cols = ['account_name', 'account_type', 'practice', 'adviser']
        
        cleaned_df = df
        for col in string_cols:
            if col in df.columns:
                cleaned_df = (cleaned_df
                             .withColumn(col, F.trim(F.upper(F.col(col))))  # Normalize case
                             .withColumn(col, F.regexp_replace(F.col(col), r'\s+', ' ')))  # Multiple spaces
        
        # Filter out invalid records
        cleaned_df = (cleaned_df
                     .filter(F.col("account_name").isNotNull() & (F.col("account_name") != ""))
                     .filter(F.col("practice").isNotNull() & (F.col("practice") != ""))
                     .filter(F.col("adviser").isNotNull() & (F.col("adviser") != "")))
        
        return cleaned_df
    
    def _add_data_quality_metrics(self, df: DataFrame) -> DataFrame:
        """Add data quality indicators"""
        return (df
               .withColumn("has_valid_date", F.col("date_submitted").isNotNull())
               .withColumn("record_completeness", 
                          F.when(F.col("account_name").isNotNull() & 
                                F.col("account_type").isNotNull() & 
                                F.col("practice").isNotNull() & 
                                F.col("adviser").isNotNull(), 1.0)
                           .otherwise(0.8)))

# COMMAND ----------

class OptimizedBronzeLoader:
    """Enhanced bronze loader with batch processing"""
    
    def __init__(self, config: ConfigManager, logger: OptimizedLogger):
        self.config = config
        self.logger = logger
        self.spark = SparkSession.getActiveSession()
        self.table_config = config.get('destinations.bronze_table')
        self.table_name = f"{self.table_config['catalog']}.{self.table_config['schema']}.{self.table_config['table']}"
        self.optimizer = DeltaTableOptimizer(self.spark, logger)
    
    @timing_decorator
    def load_incremental(self, df: DataFrame) -> ProcessingResult:
        """Optimized incremental load with batch processing"""
        try:
            if not self._table_exists():
                return self._create_table(df)
            
            return self._upsert_data(df)
        except Exception as e:
            self.logger.error("Incremental load failed", error=str(e))
            return ProcessingResult(False, 0, 0, str(e))
    
    @timing_decorator
    def load_full_refresh(self, df: DataFrame) -> ProcessingResult:
        """Optimized full refresh with table optimization"""
        start_time = datetime.now()
        
        try:
            # Use optimized write
            (df.write
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("optimizeWrite", "true")
             .saveAsTable(self.table_name))
            
            # Optimize after full refresh
            z_order_cols = self.config.get('optimization.z_order_columns', [])
            if z_order_cols:
                self.optimizer.optimize_table(self.table_name, z_order_cols)
            
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()
            
            self.logger.info("Full refresh completed", 
                           records=record_count, duration=duration)
            
            return ProcessingResult(True, record_count, duration)
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ProcessingResult(False, 0, duration, str(e))
    
    def _table_exists(self) -> bool:
        """Check table existence efficiently"""
        try:
            self.spark.catalog.tableExists(self.table_name)
            return True
        except:
            return False
    
    def _create_table(self, df: DataFrame) -> ProcessingResult:
        """Create table with optimizations enabled"""
        start_time = datetime.now()
        
        try:
            (df.write
             .mode("overwrite")
             .option("optimizeWrite", "true")
             .saveAsTable(self.table_name))
            
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()
            
            return ProcessingResult(True, record_count, duration)
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ProcessingResult(False, 0, duration, str(e))
    
    @timing_decorator
    def _upsert_data(self, new_df: DataFrame) -> ProcessingResult:
        """Optimized upsert with better merge conditions"""
        start_time = datetime.now()
        
        try:
            bronze_table = DeltaTable.forName(self.spark, self.table_name)
            dedup_cols = self.config.get('processing.deduplication_columns')
            
            # Build merge condition
            merge_conditions = [f"bronze.{col} = updates.{col}" for col in dedup_cols]
            merge_conditions.append("date(bronze.download_date) = date(updates.download_date)")
            merge_condition = " AND ".join(merge_conditions)
            
            # Execute merge
            (bronze_table.alias("bronze")
             .merge(new_df.alias("updates"), merge_condition)
             .whenMatchedUpdate(
                 condition="updates.ingestion_datetime > bronze.ingestion_datetime",
                 set={col: f"updates.{col}" for col in new_df.columns}
             )
             .whenNotMatchedInsert(
                 values={col: f"updates.{col}" for col in new_df.columns}
             )
             .execute())
            
            duration = (datetime.now() - start_time).total_seconds()
            record_count = new_df.count()
            
            return ProcessingResult(True, record_count, duration)
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ProcessingResult(False, 0, duration, str(e))

# COMMAND ----------

class EnhancedSilverTransformer:
    """Advanced silver transformer with complex organization and dynamic mapping"""
    
    def __init__(self, config: ConfigManager, logger: OptimizedLogger):
        self.config = config
        self.logger = logger
        self.spark = SparkSession.getActiveSession()
        
        # Table configurations
        self.bronze_config = config.get('destinations.bronze_table')
        self.silver_config = config.get('destinations.silver_table')
        self.org_config = config.get('destinations.organization_table')
        self.mapping_config = config.get('destinations.dynamic_mapping_table')
        
        # Table names
        self.bronze_table = f"{self.bronze_config['catalog']}.{self.bronze_config['schema']}.{self.bronze_config['table']}"
        self.silver_table = f"{self.silver_config['catalog']}.{self.silver_config['schema']}.{self.silver_config['table']}"
        self.org_table = f"{self.org_config['catalog']}.{self.org_config['schema']}.{self.org_config['table']}"
        self.mapping_table = f"{self.mapping_config['catalog']}.{self.mapping_config['schema']}.{self.mapping_config['table']}"
        
        # Configuration for organization matching
        self.org_matching = config.get('organization_matching', {})
        self.dynamic_mapping = config.get('dynamic_mapping', {})
        
        self.cache_manager = CacheManager(self.spark, logger)
        self.optimizer = DeltaTableOptimizer(self.spark, logger)
    
    @timing_decorator
    def transform_and_load(self, mode: str = "incremental") -> ProcessingResult:
        """Enhanced transformation with complex matching logic"""
        start_time = datetime.now()
        
        try:
            # Step 1: Get bronze data
            bronze_df = self._get_bronze_data(mode)
            if bronze_df is None:
                return ProcessingResult(True, 0, 0, "No bronze data to process")
            
            # Step 2: Prepare organization data with latest records
            org_df = self._prepare_organization_data()
            
            # Step 3: Prepare dynamic mapping data
            mapping_df = self._prepare_dynamic_mapping_data()
            
            # Step 4: Join with organization (complex matching)
            silver_df = self._join_with_organization(bronze_df, org_df)
            
            # Step 5: Join with dynamic mapping
            final_df = self._join_with_dynamic_mapping(silver_df, mapping_df)
            
            # Step 6: Load to silver table
            result = self._load_silver_table(final_df, mode)
            
            # Step 7: Optimize table if configured
            if result.success and self.config.get('optimization.auto_optimize', False):
                z_order_cols = self.config.get('optimization.z_order_columns', [])
                if z_order_cols:
                    self.optimizer.optimize_table(self.silver_table, z_order_cols)
            
            # Clean up cache
            self.cache_manager.clear_cache()
            
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info("Silver transformation completed", 
                           records=result.records_processed, duration=duration)
            
            return ProcessingResult(result.success, result.records_processed, duration, result.error_message)
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error("Silver transformation failed", error=str(e), duration=duration)
            return ProcessingResult(False, 0, duration, str(e))
    
    def _get_bronze_data(self, mode: str) -> DataFrame:
        """Get bronze data with optimized filtering"""
        try:
            base_df = self.spark.table(self.bronze_table)
            
            if mode == "incremental":
                # Get today's ingested data
                filtered_df = base_df.filter(F.date(F.col("ingestion_datetime")) == F.current_date())
                
                if filtered_df.count() == 0:
                    self.logger.info("No incremental data found for today")
                    return None
                    
                return self.cache_manager.cache_if_beneficial(filtered_df)
            else:
                return self.cache_manager.cache_if_beneficial(base_df)
                
        except Exception as e:
            self.logger.error("Failed to read bronze data", error=str(e))
            raise
    
    def _prepare_organization_data(self) -> DataFrame:
        """Prepare organization data with latest records per adviser"""
        try:
            org_df = self.spark.table(self.org_table)
            
            company_col = self.org_matching.get('company_name_column', 'companyname')
            advisor_col = self.org_matching.get('advisor_name_column', 'advisorname')
            load_date_col = self.org_matching.get('load_date_column', 'load_date')
            adviser_code_col = self.org_matching.get('adviser_code_column', 'adviser_code')
            
            # Clean adviser names (remove inactive patterns)
            cleaned_df = self._clean_adviser_names(org_df, advisor_col)
            
            # Get latest record for each adviser
            window_spec = (Window.partitionBy(F.col(advisor_col), F.col(company_col))
                          .orderBy(F.desc(F.col(load_date_col))))
            
            latest_df = (cleaned_df
                        .withColumn("row_num", F.row_number().over(window_spec))
                        .filter(F.col("row_num") == 1)
                        .drop("row_num")
                        .select(
                            F.col(company_col).alias("org_company_name"),
                            F.col(advisor_col).alias("org_advisor_name"), 
                            F.col(f"{advisor_col}_cleaned").alias("org_advisor_name_cleaned"),
                            F.col(adviser_code_col).alias("org_adviser_code"),
                            F.col(load_date_col).alias("org_load_date")
                        ))
            
            self.logger.info("Organization data prepared", records=latest_df.count())
            return self.cache_manager.cache_if_beneficial(latest_df)
            
        except Exception as e:
            self.logger.error("Failed to prepare organization data", error=str(e))
            raise
    
    def _clean_adviser_names(self, df: DataFrame, advisor_col: str) -> DataFrame:
        """Clean adviser names using configured patterns"""
        cleaning_patterns = self.org_matching.get('name_cleaning_patterns', [])
        
        cleaned_df = df.withColumn(f"{advisor_col}_original", F.col(advisor_col))
        cleaned_col = F.col(advisor_col)
        
        # Apply each cleaning pattern
        for pattern in cleaning_patterns:
            cleaned_col = F.regexp_replace(cleaned_col, pattern, "")
        
        # Final cleanup: trim and normalize
        cleaned_col = F.trim(F.upper(cleaned_col))
        
        return cleaned_df.withColumn(f"{advisor_col}_cleaned", cleaned_col)
    
    def _prepare_dynamic_mapping_data(self) -> DataFrame:
        """Prepare dynamic mapping data"""
        try:
            mapping_df = self.spark.table(self.mapping_table)
            
            adviser_code_col = self.dynamic_mapping.get('adviser_code_column', 'adviser_code')
            dynamic_id_col = self.dynamic_mapping.get('dynamic_id_column', 'dynamic_id')
            
            # Select and rename columns for clarity
            prepared_df = mapping_df.select(
                F.col(adviser_code_col).alias("mapping_adviser_code"),
                F.col(dynamic_id_col).alias("dynamic_id")
            ).distinct()  # Remove duplicates
            
            self.logger.info("Dynamic mapping data prepared", records=prepared_df.count())
            return self.cache_manager.cache_if_beneficial(prepared_df)
            
        except Exception as e:
            self.logger.error("Failed to prepare dynamic mapping data", error=str(e))
            # Return empty DataFrame to allow processing to continue
            from pyspark.sql.types import StructType, StructField, StringType
            empty_schema = StructType([
                StructField("mapping_adviser_code", StringType(), True),
                StructField("dynamic_id", StringType(), True)
            ])
            return self.spark.createDataFrame([], empty_schema)
    
    def _join_with_organization(self, bronze_df: DataFrame, org_df: DataFrame) -> DataFrame:
        """Complex organization join with fuzzy matching"""
        try:
            # Normalize bronze data for matching
            normalized_bronze = (bronze_df
                               .withColumn("bronze_practice_clean", F.trim(F.upper(F.col("practice"))))
                               .withColumn("bronze_adviser_clean", F.trim(F.upper(F.col("adviser")))))
            
            # Strategy 1: Exact match
            exact_match_df = (normalized_bronze.alias("b")
                            .join(org_df.alias("o"),
                                  (F.col("b.bronze_practice_clean") == F.col("o.org_company_name")) &
                                  (F.col("b.bronze_adviser_clean") == F.col("o.org_advisor_name_cleaned")),
                                  "left")
                            .withColumn("match_type", F.lit("exact"))
                            .withColumn("match_confidence", F.lit(1.0)))
            
            # Strategy 2: Fuzzy match for unmatched records
            unmatched_df = exact_match_df.filter(F.col("org_adviser_code").isNull())
            matched_df = exact_match_df.filter(F.col("org_adviser_code").isNotNull())
            
            if unmatched_df.count() > 0:
                fuzzy_matched_df = self._apply_fuzzy_matching(unmatched_df, org_df)
                # Combine exact and fuzzy matches
                final_df = matched_df.union(fuzzy_matched_df)
            else:
                final_df = matched_df
            
            # Add organization join statistics
            total_records = bronze_df.count()
            matched_records = final_df.filter(F.col("org_adviser_code").isNotNull()).count()
            match_rate = (matched_records / total_records * 100) if total_records > 0 else 0
            
            self.logger.info("Organization join completed", 
                           total_records=total_records, 
                           matched_records=matched_records,
                           match_rate=f"{match_rate:.2f}%")
            
            return final_df
            
        except Exception as e:
            self.logger.error("Organization join failed", error=str(e))
            # Return bronze data with null organization fields
            return (bronze_df
                   .withColumn("org_adviser_code", F.lit(None).cast("string"))
                   .withColumn("org_company_name", F.lit(None).cast("string"))
                   .withColumn("match_type", F.lit("failed"))
                   .withColumn("match_confidence", F.lit(0.0)))
    
    def _apply_fuzzy_matching(self, unmatched_df: DataFrame, org_df: DataFrame) -> DataFrame:
        """Apply fuzzy matching for unmatched records"""
        # Simple fuzzy matching using soundex and levenshtein distance
        # For production, consider using more sophisticated fuzzy matching libraries
        
        fuzzy_df = (unmatched_df.alias("u")
                   .crossJoin(org_df.alias("o"))
                   .withColumn("name_similarity", 
                             F.when(F.levenshtein(F.col("u.bronze_adviser_clean"), 
                                                 F.col("o.org_advisor_name_cleaned")) <= 3, 0.8)
                              .when(F.soundex(F.col("u.bronze_adviser_clean")) == 
                                   F.soundex(F.col("o.org_advisor_name_cleaned")), 0.6)
                              .otherwise(0.0))
                   .withColumn("company_similarity",
                             F.when(F.levenshtein(F.col("u.bronze_practice_clean"), 
                                                 F.col("o.org_company_name")) <= 5, 0.8)
                              .otherwise(0.0))
                   .withColumn("total_similarity", 
                             (F.col("name_similarity") + F.col("company_similarity")) / 2)
                   .filter(F.col("total_similarity") > 0.5))  # Minimum similarity threshold
        
        # Get best match for each bronze record
        window_spec = (Window.partitionBy([col for col in unmatched_df.columns if col.startswith("bronze_") or not col.startswith("org_")])
                      .orderBy(F.desc("total_similarity")))
        
        best_matches_df = (fuzzy_df
                          .withColumn("rank", F.row_number().over(window_spec))
                          .filter(F.col("rank") == 1)
                          .drop("rank", "name_similarity", "company_similarity")
                          .withColumn("match_type", F.lit("fuzzy"))
                          .withColumn("match_confidence", F.col("total_similarity"))
                          .drop("total_similarity"))
        
        # For records with no fuzzy matches, return with nulls
        no_match_df = (unmatched_df
                      .join(best_matches_df.select([col for col in unmatched_df.columns]), 
                           [col for col in unmatched_df.columns if col.startswith("bronze_") or not col.startswith("org_")], 
                           "left_anti")
                      .withColumn("org_adviser_code", F.lit(None).cast("string"))
                      .withColumn("org_company_name", F.lit(None).cast("string"))
                      .withColumn("match_type", F.lit("no_match"))
                      .withColumn("match_confidence", F.lit(0.0)))
        
        return best_matches_df.union(no_match_df)
    
    def _join_with_dynamic_mapping(self, silver_df: DataFrame, mapping_df: DataFrame) -> DataFrame:
        """Join with dynamic mapping table to get dynamic IDs"""
        try:
            # Join on adviser code
            final_df = (silver_df.alias("s")
                       .join(mapping_df.alias("m"),
                             F.col("s.org_adviser_code") == F.col("m.mapping_adviser_code"),
                             "left")
                       .drop("mapping_adviser_code"))
            
            # Add mapping statistics
            total_records = silver_df.count()
            mapped_records = final_df.filter(F.col("dynamic_id").isNotNull()).count()
            mapping_rate = (mapped_records / total_records * 100) if total_records > 0 else 0
            
            self.logger.info("Dynamic mapping completed",
                           total_records=total_records,
                           mapped_records=mapped_records,
                           mapping_rate=f"{mapping_rate:.2f}%")
            
            return final_df
            
        except Exception as e:
            self.logger.error("Dynamic mapping failed", error=str(e))
            # Return silver data with null dynamic_id
            return silver_df.withColumn("dynamic_id", F.lit(None).cast("string"))
    
    def _load_silver_table(self, df: DataFrame, mode: str) -> ProcessingResult:
        """Load data to silver table with optimizations"""
        start_time = datetime.now()
        
        try:
            if mode == "full_refresh":
                (df.write
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .option("optimizeWrite", "true")
                 .saveAsTable(self.silver_table))
                
                record_count = df.count()
            else:
                # Incremental load with merge
                if not self._silver_table_exists():
                    (df.write
                     .mode("overwrite")
                     .option("optimizeWrite", "true")
                     .saveAsTable(self.silver_table))
                    record_count = df.count()
                else:
                    record_count = self._merge_silver_data(df)
            
            duration = (datetime.now() - start_time).total_seconds()
            return ProcessingResult(True, record_count, duration)
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ProcessingResult(False, 0, duration, str(e))
    
    def _merge_silver_data(self, new_df: DataFrame) -> int:
        """Merge new data into silver table"""
        silver_table = DeltaTable.forName(self.spark, self.silver_table)
        dedup_cols = self.config.get('processing.deduplication_columns')
        
        # Build merge condition
        merge_conditions = [f"silver.{col} = updates.{col}" for col in dedup_cols]
        merge_conditions.append("date(silver.download_date) = date(updates.download_date)")
        merge_condition = " AND ".join(merge_conditions)
        
        # Execute merge
        (silver_table.alias("silver")
         .merge(new_df.alias("updates"), merge_condition)
         .whenMatchedUpdate(
             condition="updates.ingestion_datetime > silver.ingestion_datetime",
             set={col: f"updates.{col}" for col in new_df.columns}
         )
         .whenNotMatchedInsert(
             values={col: f"updates.{col}" for col in new_df.columns}
         )
         .execute())
        
        return new_df.count()
    
    def _silver_table_exists(self) -> bool:
        """Check if silver table exists"""
        try:
            self.spark.catalog.tableExists(self.silver_table)
            return True
        except:
            return False