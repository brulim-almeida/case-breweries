"""
Delta Spark Utilities Module

This module provides utilities for initializing and configuring Apache Spark
with Delta Lake support for the Breweries Data Engineering case.
"""

import pyspark
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

# Get the version of PySpark
pyspark_version = pyspark.__version__


def initialize_spark(app_name: str = 'Breweries-DataLake') -> SparkSession:
    """
    Initialize a Spark session with Delta Lake support.
    
    This function creates and configures a SparkSession with the necessary
    settings for Delta Lake operations, including catalog configurations
    and Hive support for the Medallion architecture (Bronze/Silver/Gold).

    Args:
        app_name (str): Name of the Spark application. 
                       Defaults to 'Breweries-DataLake'.

    Returns:
        SparkSession: Configured SparkSession object with Delta Lake support.
        
    Example:
        >>> spark = initialize_spark('MyBreweriesApp')
        >>> df = spark.read.format('delta').load('/path/to/delta/table')
    """

    # Display initialization message
    print('=' * 120)
    print(f'{"STARTING SPARK SESSION".center(120, "=")}')
    print('=' * 120)

    # Spark session configuration with Delta Lake support
    spark = (SparkSession
             .builder
             .appName(app_name)
             .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
             .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
             .enableHiveSupport()
             )

    # Configure Delta Lake with Spark using pip utilities
    spark = configure_spark_with_delta_pip(spark).getOrCreate()

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel('ERROR')
    
    # Configure shuffle partitions for optimized performance
    spark.conf.set("spark.sql.shuffle.partitions", 8)

    # Display version information
    delta_version_info = 'Delta Lake Version = 3.1.0'
    pyspark_version_info = f'PySpark Version = {pyspark_version}'
    
    print('=' * 120)
    print('SPARK | DELTA | INITIALIZED SUCCESSFULLY')
    print(f'{pyspark_version_info} | {delta_version_info}')
    print('=' * 120)

    return spark


def stop_spark(spark: SparkSession) -> None:
    """
    Stop the Spark session gracefully.
    
    Args:
        spark (SparkSession): The SparkSession instance to stop.
        
    Example:
        >>> spark = initialize_spark()
        >>> # ... do some work ...
        >>> stop_spark(spark)
    """
    if spark:
        print('=' * 120)
        print(f'{"STOPPING SPARK SESSION".center(120, "=")}')
        print('=' * 120)
        spark.stop()
        print('SPARK SESSION STOPPED SUCCESSFULLY')
