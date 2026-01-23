"""
Silver Layer - Curated Data Transformation

This module handles the transformation of raw data from Bronze layer to
curated data in Silver layer using PySpark. Data is transformed to columnar
format (Parquet/Delta), partitioned by location, and validated for quality.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType
)

from utils.delta_spark import initialize_spark, stop_spark
from src.config.settings import Settings
from src.enrichment.geocoding import GeocodeEnricher


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverLayer:
    """
    Silver Layer handler for curated data transformation.
    
    Responsibilities:
    - Read raw JSON data from Bronze layer
    - Apply schema validation and enforcement
    - Perform data cleaning and normalization
    - Transform to columnar format (Parquet/Delta)
    - Partition by brewery location (country, state)
    - Add metadata columns (processing timestamp, etc.)
    - Validate data quality
    
    Example:
        >>> silver = SilverLayer()
        >>> metadata = silver.transform_breweries()
        >>> print(f"Transformed {metadata['total_records']} records")
    """
    
    # Define schema for breweries data
    BREWERY_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("brewery_type", StringType(), True),
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state_province", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("phone", StringType(), True),
        StructField("website_url", StringType(), True),
        StructField("street", StringType(), True)
    ])
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        bronze_path: Optional[str] = None,
        silver_path: Optional[str] = None
    ):
        """
        Initialize the Silver Layer processor.
        
        Args:
            spark (SparkSession, optional): Spark session instance
            bronze_path (str, optional): Base path for Bronze layer
            silver_path (str, optional): Base path for Silver layer
        """
        self.spark = spark if spark else initialize_spark()
        self.bronze_path = bronze_path or Settings.BRONZE_PATH
        self.silver_path = silver_path or Settings.SILVER_PATH
        
        # Ensure silver path exists
        Path(self.silver_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized SilverLayer with paths:")
        logger.info(f"  Bronze: {self.bronze_path}")
        logger.info(f"  Silver: {self.silver_path}")
    
    def _read_bronze_data(
        self,
        dataset_name: str = "breweries",
        partition_date: Optional[Dict[str, int]] = None,
        ingestion_path: Optional[str] = None
    ) -> DataFrame:
        """
        Read raw JSON data from Bronze layer.
        
        Args:
            dataset_name (str): Name of the dataset
            partition_date (dict, optional): Partition filter
            ingestion_path (str, optional): Specific file path to read (for incremental processing)
            
        Returns:
            DataFrame: Raw data from Bronze layer
        """
        # If specific ingestion path provided, read only that file
        if ingestion_path:
            path = ingestion_path
            logger.info(f"Reading specific Bronze ingestion from: {path}")
        # Build path with optional partition filter
        elif partition_date:
            year = partition_date.get('year', '*')
            month = partition_date.get('month', '*')
            day = partition_date.get('day', '*')
            path = f"{self.bronze_path}/{dataset_name}/year={year}/month={month:02d}/day={day:02d}/*.json"
            logger.info(f"Reading Bronze data with partition filter from: {path}")
        else:
            path = f"{self.bronze_path}/{dataset_name}/year=*/month=*/day=*/*.json"
            logger.info(f"Reading ALL Bronze data from: {path}")
        
        # Read JSON files with schema
        # Use multiLine=True because Bronze saves as JSON array (pretty-printed)
        df = self.spark.read.schema(self.BREWERY_SCHEMA).option("multiLine", "true").json(path)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count:,} records from Bronze layer")
        
        return df
    
    def _clean_and_normalize(self, df: DataFrame) -> DataFrame:
        """
        Clean and normalize brewery data.
        
        Args:
            df (DataFrame): Raw DataFrame
            
        Returns:
            DataFrame: Cleaned and normalized DataFrame
        """
        logger.info("Applying data cleaning and normalization...")
        
        # 1. Trim whitespace from string columns
        string_cols = [field.name for field in df.schema.fields 
                      if isinstance(field.dataType, StringType)]
        
        for col in string_cols:
            df = df.withColumn(col, F.trim(F.col(col)))
        
        # 2. Replace empty strings with null
        for col in string_cols:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", None).otherwise(F.col(col))
            )
        
        # 3. Normalize country names
        df = df.withColumn(
            "country_normalized",
            F.when(F.col("country").isin(["United States", "USA", "US"]), "United States")
            .when(F.col("country") == "England", "United Kingdom")
            .otherwise(F.col("country"))
        )
        
        # 4. Normalize brewery type
        df = df.withColumn(
            "brewery_type_normalized",
            F.lower(F.regexp_replace(F.col("brewery_type"), "[^a-zA-Z0-9]", "_"))
        )
        
        # 5. Create full address field
        df = df.withColumn(
            "full_address",
            F.concat_ws(", ",
                F.col("address_1"),
                F.col("city"),
                F.col("state"),
                F.col("postal_code"),
                F.col("country")
            )
        )
        
        # 6. Create location key for partitioning
        df = df.withColumn(
            "location_key",
            F.concat_ws("_",
                F.lower(F.regexp_replace(F.coalesce(F.col("country"), F.lit("unknown")), "[^a-zA-Z0-9]", "_")),
                F.lower(F.regexp_replace(F.coalesce(F.col("state"), F.lit("unknown")), "[^a-zA-Z0-9]", "_"))
            )
        )
        
        # 7. Add data quality flags
        df = df.withColumn(
            "has_coordinates",
            F.when(
                (F.col("longitude").isNotNull()) & (F.col("latitude").isNotNull()),
                True
            ).otherwise(False)
        )
        
        df = df.withColumn(
            "has_contact",
            F.when(
                (F.col("phone").isNotNull()) | (F.col("website_url").isNotNull()),
                True
            ).otherwise(False)
        )
        
        df = df.withColumn(
            "is_complete",
            F.when(
                (F.col("has_coordinates")) & (F.col("has_contact")) & 
                (F.col("city").isNotNull()) & (F.col("state").isNotNull()),
                True
            ).otherwise(False)
        )
        
        logger.info("Data cleaning and normalization completed")
        
        return df
    
    def _add_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add metadata columns to the DataFrame.
        
        Args:
            df (DataFrame): DataFrame to add metadata to
            
        Returns:
            DataFrame: DataFrame with metadata columns
        """
        logger.info("Adding metadata columns...")
        
        # Add processing timestamp
        df = df.withColumn(
            "silver_processed_at",
            F.lit(datetime.now().isoformat())
        )
        
        # Add processing date (for partitioning)
        df = df.withColumn(
            "processing_date",
            F.current_date()
        )
        
        # Add year/month for time-based partitioning (optional)
        df = df.withColumn("processing_year", F.year(F.col("processing_date")))
        df = df.withColumn("processing_month", F.month(F.col("processing_date")))
        
        return df
    
    def _enrich_with_geocoding(
        self,
        df: DataFrame,
        max_records: Optional[int] = None
    ) -> tuple[DataFrame, Dict[str, Any]]:
        """
        Enrich data with geocoding for missing coordinates.
        
        Args:
            df: Input DataFrame
            max_records: Maximum number of records to geocode (for control)
            
        Returns:
            Tuple of (enriched DataFrame, geocoding metrics)
        """
        logger.info("=" * 80)
        logger.info("GEOCODING ENRICHMENT")
        logger.info("=" * 80)
        
        geocoding_start = datetime.now()
        
        # Count before enrichment
        total_records = df.count()
        before_coords = df.filter(
            (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull())
        ).count()
        before_missing = total_records - before_coords
        before_pct = (before_coords / total_records * 100) if total_records > 0 else 0
        
        logger.info(f"BEFORE Geocoding:")
        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  With coordinates: {before_coords:,} ({before_pct:.2f}%)")
        logger.info(f"  Missing coordinates: {before_missing:,} ({100-before_pct:.2f}%)")
        
        # Perform geocoding
        enricher = GeocodeEnricher(self.spark, rate_limit_delay=1.1)
        df_enriched = enricher.enrich_coordinates(df, max_records=max_records)
        
        # Count after enrichment
        after_coords = df_enriched.filter(
            (F.col("latitude").isNotNull()) & (F.col("longitude").isNotNull())
        ).count()
        after_missing = total_records - after_coords
        after_pct = (after_coords / total_records * 100) if total_records > 0 else 0
        
        # Calculate improvement
        new_coords = after_coords - before_coords
        improvement_pct = (new_coords / before_missing * 100) if before_missing > 0 else 0
        
        geocoding_time = (datetime.now() - geocoding_start).total_seconds()
        
        # Get enricher stats
        enricher_stats = enricher.get_stats()
        enricher.close()
        
        # Create comprehensive metrics
        geocoding_metrics = {
            "enabled": True,
            "geocoding_time_seconds": round(geocoding_time, 2),
            "total_records": total_records,
            "before_geocoding": {
                "with_coordinates": before_coords,
                "missing_coordinates": before_missing,
                "coverage_percentage": round(before_pct, 2)
            },
            "after_geocoding": {
                "with_coordinates": after_coords,
                "missing_coordinates": after_missing,
                "coverage_percentage": round(after_pct, 2)
            },
            "enrichment_results": {
                "new_coordinates_added": new_coords,
                "attempted_geocoding": enricher_stats.get('total_missing', 0),
                "successful_geocoding": enricher_stats.get('geocoded_count', 0),
                "failed_geocoding": enricher_stats.get('failed_count', 0),
                "success_rate_percentage": round(
                    (enricher_stats.get('geocoded_count', 0) / enricher_stats.get('total_missing', 1) * 100), 2
                ) if enricher_stats.get('total_missing', 0) > 0 else 0,
                "improvement_percentage": round(improvement_pct, 2)
            },
            "performance": {
                "records_per_second": round(
                    enricher_stats.get('geocoded_count', 0) / geocoding_time, 2
                ) if geocoding_time > 0 else 0,
                "avg_time_per_record_seconds": round(
                    geocoding_time / enricher_stats.get('geocoded_count', 1), 2
                ) if enricher_stats.get('geocoded_count', 0) > 0 else 0
            }
        }
        
        # Log results
        logger.info("=" * 80)
        logger.info("GEOCODING RESULTS")
        logger.info("=" * 80)
        logger.info(f"AFTER Geocoding:")
        logger.info(f"  With coordinates: {after_coords:,} ({after_pct:.2f}%)")
        logger.info(f"  Missing coordinates: {after_missing:,} ({100-after_pct:.2f}%)")
        logger.info(f"")
        logger.info(f"📊 ENRICHMENT SUMMARY:")
        logger.info(f"  ✅ New coordinates added: {new_coords:,}")
        logger.info(f"  🎯 Success rate: {geocoding_metrics['enrichment_results']['success_rate_percentage']:.2f}%")
        logger.info(f"  📈 Coverage improvement: +{improvement_pct:.2f}%")
        logger.info(f"  ⏱️  Processing time: {geocoding_time:.2f}s")
        logger.info(f"  ⚡ Speed: {geocoding_metrics['performance']['records_per_second']:.2f} records/sec")
        logger.info("=" * 80)
        
        return df_enriched, geocoding_metrics
    
    def _validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate data quality and generate quality metrics.
        
        Args:
            df (DataFrame): DataFrame to validate
            
        Returns:
            dict: Data quality metrics
        """
        logger.info("Validating data quality...")
        
        total_records = df.count()
        
        # Calculate quality metrics
        metrics = {
            "total_records": total_records,
            "unique_ids": df.select("id").distinct().count(),
            "null_names": df.filter(F.col("name").isNull()).count(),
            "null_types": df.filter(F.col("brewery_type").isNull()).count(),
            "null_countries": df.filter(F.col("country").isNull()).count(),
            "null_states": df.filter(F.col("state").isNull()).count(),
            "with_coordinates": df.filter(F.col("has_coordinates") == True).count(),
            "with_contact": df.filter(F.col("has_contact") == True).count(),
            "complete_records": df.filter(F.col("is_complete") == True).count(),
        }
        
        # Calculate percentages
        if total_records > 0:
            metrics["completeness_rate"] = round(
                (metrics["complete_records"] / total_records) * 100, 2
            )
            metrics["coordinate_coverage"] = round(
                (metrics["with_coordinates"] / total_records) * 100, 2
            )
            metrics["contact_coverage"] = round(
                (metrics["with_contact"] / total_records) * 100, 2
            )
        
        # Count by type and country
        metrics["by_type"] = {
            row["brewery_type_normalized"]: row["count"] 
            for row in df.groupBy("brewery_type_normalized").count().collect()
        }
        
        metrics["by_country"] = {
            row["country_normalized"]: row["count"] 
            for row in df.groupBy("country_normalized").count().collect()
        }
        
        # Log quality metrics
        logger.info("=" * 80)
        logger.info("DATA QUALITY METRICS")
        logger.info("=" * 80)
        logger.info(f"Total records: {metrics['total_records']:,}")
        logger.info(f"Unique IDs: {metrics['unique_ids']:,}")
        logger.info(f"Completeness rate: {metrics.get('completeness_rate', 0):.2f}%")
        logger.info(f"Coordinate coverage: {metrics.get('coordinate_coverage', 0):.2f}%")
        logger.info(f"Contact coverage: {metrics.get('contact_coverage', 0):.2f}%")
        logger.info("=" * 80)
        
        return metrics
    
    def _write_to_silver(
        self,
        df: DataFrame,
        dataset_name: str = "breweries",
        partition_by: Optional[List[str]] = None,
        mode: str = "overwrite"
    ) -> str:
        """
        Write DataFrame to Silver layer in Delta Lake format.
        
        Args:
            df (DataFrame): DataFrame to write
            dataset_name (str): Name of the dataset
            partition_by (list, optional): Columns to partition by
            mode (str): Write mode (overwrite, append, etc.)
            
        Returns:
            str: Path where data was written
        """
        output_path = f"{self.silver_path}/{dataset_name}"
        
        logger.info(f"Writing to Silver layer: {output_path}")
        logger.info(f"Format: Delta Lake")
        logger.info(f"Mode: {mode}")
        
        if partition_by:
            logger.info(f"Partitioning by: {partition_by}")
        
        # Write in Delta Lake format
        writer = df.write.mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.format("delta").save(output_path)
        
        logger.info(f"Successfully wrote data to {output_path}")
        
        return output_path
    
    def transform_breweries(
        self,
        partition_date: Optional[Dict[str, int]] = None,
        partition_by: Optional[List[str]] = None,
        ingestion_path: Optional[str] = None,
        enable_geocoding: bool = True,
        max_geocoding_records: Optional[int] = 1000
    ) -> Dict[str, Any]:
        """
        Complete transformation pipeline from Bronze to Silver layer.
        Data is always saved in Delta Lake format for ACID transactions and time travel.
        
        Args:
            partition_date (dict, optional): Filter Bronze data by date
            partition_by (list, optional): Columns to partition Silver data by
            ingestion_path (str, optional): Specific Bronze file to process (for incremental loads)
            enable_geocoding (bool): Enable geocoding enrichment for missing coordinates
            max_geocoding_records (int, optional): Max records to geocode (None = unlimited)
            
        Returns:
            dict: Transformation metadata and quality metrics
            
        Example:
            >>> silver = SilverLayer()
            >>> metadata = silver.transform_breweries(
            ...     ingestion_path="/path/to/specific/file.json",
            ...     enable_geocoding=True,
            ...     max_geocoding_records=100
            ... )
        """
        logger.info("=" * 80)
        logger.info("STARTING SILVER LAYER TRANSFORMATION")
        logger.info("=" * 80)
        
        transformation_start = datetime.now()
        
        try:
            # 1. Read Bronze data
            df_bronze = self._read_bronze_data(
                dataset_name="breweries",
                partition_date=partition_date,
                ingestion_path=ingestion_path
            )
            
            # 2. Clean and normalize
            df_cleaned = self._clean_and_normalize(df_bronze)
            
            # 3. Add metadata
            df_enriched = self._add_metadata(df_cleaned)
            
            # 4. GEOCODING ENRICHMENT (NEW!)
            geocoding_metrics = None
            if enable_geocoding:
                df_enriched, geocoding_metrics = self._enrich_with_geocoding(
                    df_enriched,
                    max_records=max_geocoding_records
                )
            else:
                logger.info("⏭️  Geocoding enrichment DISABLED")
                geocoding_metrics = {"enabled": False}
            
            # 5. Validate quality
            quality_metrics = self._validate_data_quality(df_enriched)
            
            # 6. Write to Silver layer
            default_partitions = partition_by or ["country_normalized", "state"]
            output_path = self._write_to_silver(
                df_enriched,
                dataset_name="breweries",
                partition_by=default_partitions
            )
            
            # Calculate transformation time
            transformation_time = (datetime.now() - transformation_start).total_seconds()
            
            # Create metadata with geocoding metrics
            metadata = {
                "status": "success",
                "transformation_timestamp": transformation_start.isoformat(),
                "transformation_time_seconds": round(transformation_time, 2),
                "output_path": output_path,
                "output_format": "delta",
                "partition_columns": default_partitions,
                "quality_metrics": quality_metrics,
                "geocoding_metrics": geocoding_metrics  # NEW!
            }
            
            logger.info("=" * 80)
            logger.info("SILVER LAYER TRANSFORMATION COMPLETED SUCCESSFULLY")
            logger.info(f"Total records: {quality_metrics['total_records']:,}")
            logger.info(f"Output path: {output_path}")
            logger.info(f"Transformation time: {transformation_time:.2f}s")
            if geocoding_metrics and geocoding_metrics.get('enabled'):
                logger.info(f"Geocoding: +{geocoding_metrics['enrichment_results']['new_coordinates_added']} coordinates")
            logger.info("=" * 80)
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error during Silver layer transformation: {e}", exc_info=True)
            
            return {
                "status": "failed",
                "error": str(e),
                "transformation_timestamp": transformation_start.isoformat()
            }
    
    def read_silver_data(
        self,
        dataset_name: str = "breweries",
        filters: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Read data from Silver layer with optional filters.
        
        Args:
            dataset_name (str): Name of the dataset
            filters (dict, optional): Filters to apply
            
        Returns:
            DataFrame: Silver layer data
            
        Example:
            >>> silver = SilverLayer()
            >>> df = silver.read_silver_data(
            ...     filters={"country_normalized": "United States", "state": "California"}
            ... )
        """
        path = f"{self.silver_path}/{dataset_name}"
        
        logger.info(f"Reading Silver data from: {path}")
        
        df = self.spark.read.parquet(path)
        
        if filters:
            logger.info(f"Applying filters: {filters}")
            for column, value in filters.items():
                df = df.filter(F.col(column) == value)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count:,} records from Silver layer")
        
        return df
    
    def get_statistics(self, dataset_name: str = "breweries") -> Dict[str, Any]:
        """
        Get statistics about Silver layer data.
        
        Args:
            dataset_name (str): Name of the dataset
            
        Returns:
            dict: Statistics about the data
        """
        df = self.read_silver_data(dataset_name)
        
        stats = {
            "total_records": df.count(),
            "columns": df.columns,
            "partitions": [
                col for col in ["country_normalized", "state"] 
                if col in df.columns
            ],
            "by_country": {
                row["country_normalized"]: row["count"]
                for row in df.groupBy("country_normalized").count()
                .orderBy(F.desc("count")).limit(10).collect()
            },
            "by_state": {
                row["state"]: row["count"]
                for row in df.filter(F.col("state").isNotNull())
                .groupBy("state").count()
                .orderBy(F.desc("count")).limit(10).collect()
            },
            "by_type": {
                row["brewery_type_normalized"]: row["count"]
                for row in df.groupBy("brewery_type_normalized").count()
                .orderBy(F.desc("count")).collect()
            }
        }
        
        return stats
    
    def close(self):
        """Stop the Spark session."""
        if self.spark:
            stop_spark(self.spark)
            logger.info("Spark session stopped")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
