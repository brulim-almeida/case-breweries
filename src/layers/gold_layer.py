"""
Gold Layer - Business Aggregations

This module handles business-level aggregations for analytics and reporting.
Creates aggregated views from Silver layer data with focus on brewery counts
by type and location for business intelligence.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from utils.delta_spark import initialize_spark, stop_spark
from src.config.settings import Settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoldLayer:
    """
    Gold Layer handler for business aggregations and analytics.
    
    Responsibilities:
    - Read curated data from Silver layer
    - Create aggregations by brewery type and location
    - Generate business-level KPIs and metrics
    - Store aggregated views in Delta format
    - Provide analytics-ready datasets
    
    Aggregations:
    - Breweries count by type
    - Breweries count by country
    - Breweries count by state
    - Breweries count by type and country
    - Breweries count by type and state
    - Summary statistics
    
    Example:
        >>> gold = GoldLayer()
        >>> metadata = gold.create_aggregations()
        >>> print(f"Created {len(metadata['aggregations'])} aggregation tables")
    """
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        silver_path: Optional[str] = None,
        gold_path: Optional[str] = None
    ):
        """
        Initialize the Gold Layer processor.
        
        Args:
            spark (SparkSession, optional): Spark session instance
            silver_path (str, optional): Base path for Silver layer
            gold_path (str, optional): Base path for Gold layer
        """
        self.spark = spark if spark else initialize_spark()
        self.silver_path = silver_path or Settings.SILVER_PATH
        self.gold_path = gold_path or Settings.GOLD_PATH
        
        # Ensure gold path exists
        Path(self.gold_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized GoldLayer with paths:")
        logger.info(f"  Silver: {self.silver_path}")
        logger.info(f"  Gold: {self.gold_path}")
    
    def _read_silver_data(self, dataset_name: str = "breweries") -> DataFrame:
        """
        Read curated data from Silver layer.
        
        Args:
            dataset_name (str): Name of the dataset
            
        Returns:
            DataFrame: Silver layer data
        """
        path = f"{self.silver_path}/{dataset_name}"
        
        logger.info(f"Reading Silver data from: {path}")
        
        df = self.spark.read.format("delta").load(path)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count:,} records from Silver layer")
        
        return df
    
    def _aggregate_by_type(self, df: DataFrame) -> DataFrame:
        """
        Aggregate breweries count by type.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Aggregated by brewery type
        """
        logger.info("Creating aggregation: breweries by type")
        
        agg_df = (df
                  .groupBy("brewery_type_normalized")
                  .agg(
                      F.count("*").alias("brewery_count"),
                      F.countDistinct("id").alias("unique_breweries"),
                      F.sum(F.when(F.col("has_coordinates") == True, 1).otherwise(0)).alias("with_coordinates"),
                      F.sum(F.when(F.col("has_contact") == True, 1).otherwise(0)).alias("with_contact"),
                      F.sum(F.when(F.col("is_complete") == True, 1).otherwise(0)).alias("complete_records")
                  )
                  .withColumn("completeness_rate", 
                             F.round((F.col("complete_records") / F.col("brewery_count")) * 100, 2))
                  .withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
                  .orderBy(F.desc("brewery_count")))
        
        return agg_df
    
    def _aggregate_by_country(self, df: DataFrame) -> DataFrame:
        """
        Aggregate breweries count by country.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Aggregated by country
        """
        logger.info("Creating aggregation: breweries by country")
        
        agg_df = (df
                  .groupBy("country_normalized")
                  .agg(
                      F.count("*").alias("brewery_count"),
                      F.countDistinct("id").alias("unique_breweries"),
                      F.countDistinct("brewery_type_normalized").alias("distinct_types"),
                      F.sum(F.when(F.col("has_coordinates") == True, 1).otherwise(0)).alias("with_coordinates"),
                      F.sum(F.when(F.col("has_contact") == True, 1).otherwise(0)).alias("with_contact")
                  )
                  .withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
                  .orderBy(F.desc("brewery_count")))
        
        return agg_df
    
    def _aggregate_by_state(self, df: DataFrame) -> DataFrame:
        """
        Aggregate breweries count by state.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Aggregated by state
        """
        logger.info("Creating aggregation: breweries by state")
        
        agg_df = (df
                  .filter(F.col("state").isNotNull())
                  .groupBy("country_normalized", "state")
                  .agg(
                      F.count("*").alias("brewery_count"),
                      F.countDistinct("id").alias("unique_breweries"),
                      F.countDistinct("brewery_type_normalized").alias("distinct_types"),
                      F.countDistinct("city").alias("distinct_cities")
                  )
                  .withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
                  .orderBy(F.desc("brewery_count")))
        
        return agg_df
    
    def _aggregate_by_type_and_country(self, df: DataFrame) -> DataFrame:
        """
        Aggregate breweries count by type and country.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Aggregated by type and country
        """
        logger.info("Creating aggregation: breweries by type and country")
        
        agg_df = (df
                  .groupBy("brewery_type_normalized", "country_normalized")
                  .agg(
                      F.count("*").alias("brewery_count"),
                      F.countDistinct("id").alias("unique_breweries")
                  )
                  .withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
                  .orderBy("brewery_type_normalized", F.desc("brewery_count")))
        
        return agg_df
    
    def _aggregate_by_type_and_state(self, df: DataFrame) -> DataFrame:
        """
        Aggregate breweries count by type and state.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Aggregated by type and state
        """
        logger.info("Creating aggregation: breweries by type and state")
        
        agg_df = (df
                  .filter(F.col("state").isNotNull())
                  .groupBy("brewery_type_normalized", "country_normalized", "state")
                  .agg(
                      F.count("*").alias("brewery_count"),
                      F.countDistinct("id").alias("unique_breweries")
                  )
                  .withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
                  .orderBy("brewery_type_normalized", "country_normalized", F.desc("brewery_count")))
        
        return agg_df
    
    def _create_summary_statistics(self, df: DataFrame) -> DataFrame:
        """
        Create overall summary statistics.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            DataFrame: Summary statistics
        """
        logger.info("Creating summary statistics")
        
        # Overall statistics
        stats = df.agg(
            F.count("*").alias("total_breweries"),
            F.countDistinct("id").alias("unique_breweries"),
            F.countDistinct("brewery_type_normalized").alias("distinct_types"),
            F.countDistinct("country_normalized").alias("distinct_countries"),
            F.countDistinct("state").alias("distinct_states"),
            F.countDistinct("city").alias("distinct_cities"),
            F.sum(F.when(F.col("has_coordinates") == True, 1).otherwise(0)).alias("with_coordinates"),
            F.sum(F.when(F.col("has_contact") == True, 1).otherwise(0)).alias("with_contact"),
            F.sum(F.when(F.col("is_complete") == True, 1).otherwise(0)).alias("complete_records")
        ).withColumn("aggregation_timestamp", F.lit(datetime.now().isoformat()))
        
        return stats
    
    def _create_complete_breweries_table(self, df: DataFrame) -> DataFrame:
        """
        Create a complete, optimized breweries table for analytics and dashboards.
        This table is denormalized and ready for consumption.
        
        Args:
            df (DataFrame): Input DataFrame from Silver
            
        Returns:
            DataFrame: Complete breweries data optimized for Gold layer
        """
        logger.info("Creating complete breweries table for Gold layer")
        
        # Select and order columns for better readability
        complete_df = df.select(
            "id",
            "name",
            "brewery_type_normalized",
            "street",
            "address_1",
            "address_2",
            "address_3",
            "city",
            "state",
            "postal_code",
            "country_normalized",
            "full_address",
            "longitude",
            "latitude",
            "coordinates_valid",
            "has_coordinates",
            "phone",
            "website_url",
            "has_contact",
            "is_complete",
            "silver_processed_at"
        )
        
        logger.info(f"Complete breweries table prepared with {complete_df.count():,} records")
        
        return complete_df
    
    def _write_to_gold(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite"
    ) -> str:
        """
        Write aggregated DataFrame to Gold layer.
        
        Args:
            df (DataFrame): DataFrame to write
            table_name (str): Name of the aggregation table
            mode (str): Write mode (overwrite, append)
            
        Returns:
            str: Path where data was written
        """
        output_path = f"{self.gold_path}/{table_name}"
        
        logger.info(f"Writing to Gold layer: {output_path}")
        logger.info(f"Mode: {mode}")
        logger.info(f"Records: {df.count():,}")
        
        # Write in Delta Lake format with schema evolution support
        # overwriteSchema=True allows adding/removing columns when mode=overwrite
        df.write.mode(mode).format("delta").option("overwriteSchema", "true").save(output_path)
        
        logger.info(f"Successfully wrote aggregation to {output_path}")
        
        return output_path
    
    def create_aggregations(self) -> Dict[str, Any]:
        """
        Create all Gold layer aggregations.
        
        Returns:
            dict: Metadata about created aggregations
            
        Example:
            >>> gold = GoldLayer()
            >>> metadata = gold.create_aggregations()
            >>> print(f"Created {len(metadata['aggregations'])} tables")
        """
        logger.info("=" * 80)
        logger.info("STARTING GOLD LAYER AGGREGATIONS")
        logger.info("=" * 80)
        
        aggregation_start = datetime.now()
        aggregations_created = []
        
        try:
            # 1. Read Silver data
            df_silver = self._read_silver_data("breweries")
            
            # 2. Create complete breweries table (denormalized for consumption)
            complete_breweries = self._create_complete_breweries_table(df_silver)
            complete_path = self._write_to_gold(complete_breweries, "breweries", mode="overwrite")
            aggregations_created.append({
                "table_name": "breweries",
                "output_path": complete_path,
                "record_count": complete_breweries.count()
            })
            
            # 3. Create aggregations
            aggregations = {
                "breweries_by_type": self._aggregate_by_type(df_silver),
                "breweries_by_country": self._aggregate_by_country(df_silver),
                "breweries_by_state": self._aggregate_by_state(df_silver),
                "breweries_by_type_and_country": self._aggregate_by_type_and_country(df_silver),
                "breweries_by_type_and_state": self._aggregate_by_type_and_state(df_silver),
                "brewery_summary_statistics": self._create_summary_statistics(df_silver)
            }
            
            # 4. Write aggregations to Gold layer
            for table_name, agg_df in aggregations.items():
                output_path = self._write_to_gold(agg_df, table_name)
                aggregations_created.append({
                    "table_name": table_name,
                    "output_path": output_path,
                    "record_count": agg_df.count()
                })
            
            # Calculate processing time
            aggregation_time = (datetime.now() - aggregation_start).total_seconds()
            
            # Create metadata
            metadata = {
                "status": "success",
                "aggregation_timestamp": aggregation_start.isoformat(),
                "aggregation_time_seconds": round(aggregation_time, 2),
                "aggregations": aggregations_created,
                "total_aggregations": len(aggregations_created)
            }
            
            logger.info("=" * 80)
            logger.info("GOLD LAYER AGGREGATIONS COMPLETED SUCCESSFULLY")
            logger.info(f"Total aggregations: {len(aggregations_created)}")
            logger.info(f"Processing time: {aggregation_time:.2f}s")
            logger.info("=" * 80)
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error during Gold layer aggregation: {e}", exc_info=True)
            
            return {
                "status": "failed",
                "error": str(e),
                "aggregation_timestamp": aggregation_start.isoformat()
            }
    
    def read_aggregation(
        self,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None
    ) -> DataFrame:
        """
        Read a specific aggregation table from Gold layer.
        
        Args:
            table_name (str): Name of the aggregation table
            filters (dict, optional): Filters to apply
            
        Returns:
            DataFrame: Aggregation data
            
        Example:
            >>> gold = GoldLayer()
            >>> df = gold.read_aggregation("breweries_by_type")
            >>> df.show()
        """
        path = f"{self.gold_path}/{table_name}"
        
        # Check if table exists
        if not Path(path).exists():
            raise ValueError(f"Aggregation table '{table_name}' does not exist at path: {path}")
        
        logger.info(f"Reading Gold aggregation from: {path}")
        
        df = self.spark.read.format("delta").load(path)
        
        if filters:
            logger.info(f"Applying filters: {filters}")
            for column, value in filters.items():
                df = df.filter(F.col(column) == value)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count:,} records from aggregation")
        
        return df
    
    def list_aggregations(self) -> List[str]:
        """
        List all available aggregation tables in Gold layer.
        
        Returns:
            list: Names of available aggregation tables
        """
        gold_path = Path(self.gold_path)
        
        if not gold_path.exists():
            logger.warning("Gold path does not exist")
            return []
        
        # List directories (each is a table)
        tables = [d.name for d in gold_path.iterdir() if d.is_dir() and not d.name.startswith('.')]
        
        logger.info(f"Found {len(tables)} aggregation tables in Gold layer")
        
        return sorted(tables)
    
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
