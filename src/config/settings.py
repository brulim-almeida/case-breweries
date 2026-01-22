"""
Settings module for Breweries Data Lake

This module centralizes all configuration settings for the data pipeline,
including API endpoints, paths, and runtime parameters.
"""

import os
from typing import Optional
from pathlib import Path


class Settings:
    """
    Centralized configuration settings for the Breweries Data Lake.
    
    All settings can be overridden via environment variables.
    """
    
    # ========================================================================
    # Open Brewery DB API Configuration
    # ========================================================================
    BREWERY_API_BASE_URL: str = os.getenv(
        "BREWERY_API_BASE_URL", 
        "https://api.openbrewerydb.org/v1"
    )
    
    BREWERY_API_TIMEOUT: int = int(os.getenv("BREWERY_API_TIMEOUT", "30"))
    BREWERY_API_RETRIES: int = int(os.getenv("BREWERY_API_RETRIES", "3"))
    BREWERY_API_RETRY_DELAY: int = int(os.getenv("BREWERY_API_RETRY_DELAY", "5"))
    BREWERY_API_PER_PAGE: int = int(os.getenv("BREWERY_API_PER_PAGE", "200"))
    
    # ========================================================================
    # Data Lake Paths
    # ========================================================================
    # Default to local paths for development, Docker paths in containers
    _default_base = "./lakehouse" if not os.path.exists("/opt/airflow") else "/opt/airflow/lakehouse"
    
    DATALAKE_BASE_PATH: str = os.getenv(
        "DATALAKE_BASE_PATH", 
        _default_base
    )
    
    BRONZE_PATH: str = os.getenv(
        "BRONZE_PATH",
        f"{DATALAKE_BASE_PATH}/bronze"
    )
    
    SILVER_PATH: str = os.getenv(
        "SILVER_PATH",
        f"{DATALAKE_BASE_PATH}/silver"
    )
    
    GOLD_PATH: str = os.getenv(
        "GOLD_PATH",
        f"{DATALAKE_BASE_PATH}/gold"
    )
    
    # ========================================================================
    # Spark Configuration
    # ========================================================================
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "Breweries-DataLake")
    SPARK_SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
    SPARK_LOG_LEVEL: str = os.getenv("SPARK_LOG_LEVEL", "ERROR")
    
    # ========================================================================
    # Data Quality Configuration
    # ========================================================================
    ENABLE_DATA_QUALITY_CHECKS: bool = os.getenv(
        "ENABLE_DATA_QUALITY_CHECKS", 
        "true"
    ).lower() == "true"
    
    DATA_QUALITY_LEVEL: str = os.getenv("DATA_QUALITY_LEVEL", "strict")
    
    # ========================================================================
    # Monitoring Configuration
    # ========================================================================
    ENABLE_PROMETHEUS_METRICS: bool = os.getenv(
        "ENABLE_PROMETHEUS_METRICS", 
        "true"
    ).lower() == "true"
    
    PROMETHEUS_PORT: int = int(os.getenv("PROMETHEUS_PORT", "9090"))
    
    # ========================================================================
    # Execution Configuration
    # ========================================================================
    MAX_ACTIVE_RUNS: int = int(os.getenv("MAX_ACTIVE_RUNS", "1"))
    TASK_EXECUTION_TIMEOUT: int = int(os.getenv("TASK_EXECUTION_TIMEOUT", "3600"))
    TASK_RETRIES: int = int(os.getenv("TASK_RETRIES", "3"))
    TASK_RETRY_DELAY: int = int(os.getenv("TASK_RETRY_DELAY", "300"))
    
    @classmethod
    def get_bronze_path(cls, dataset: str) -> str:
        """
        Get the full path for a Bronze layer dataset.
        
        Args:
            dataset (str): Name of the dataset (e.g., 'breweries')
            
        Returns:
            str: Full path to the Bronze layer dataset
        """
        return f"{cls.BRONZE_PATH}/{dataset}"
    
    @classmethod
    def get_silver_path(cls, dataset: str) -> str:
        """
        Get the full path for a Silver layer dataset.
        
        Args:
            dataset (str): Name of the dataset (e.g., 'breweries')
            
        Returns:
            str: Full path to the Silver layer dataset
        """
        return f"{cls.SILVER_PATH}/{dataset}"
    
    @classmethod
    def get_gold_path(cls, dataset: str) -> str:
        """
        Get the full path for a Gold layer dataset.
        
        Args:
            dataset (str): Name of the dataset (e.g., 'breweries_aggregated')
            
        Returns:
            str: Full path to the Gold layer dataset
        """
        return f"{cls.GOLD_PATH}/{dataset}"
    
    @classmethod
    def ensure_paths_exist(cls) -> None:
        """
        Ensure that all necessary Data Lake paths exist.
        Creates directories if they don't exist.
        """
        paths = [
            cls.DATALAKE_BASE_PATH,
            cls.BRONZE_PATH,
            cls.SILVER_PATH,
            cls.GOLD_PATH
        ]
        
        for path in paths:
            Path(path).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def display_settings(cls) -> None:
        """Display current settings (useful for debugging)."""
        print("=" * 80)
        print("BREWERIES DATA LAKE - CURRENT SETTINGS")
        print("=" * 80)
        print(f"API Base URL: {cls.BREWERY_API_BASE_URL}")
        print(f"API Timeout: {cls.BREWERY_API_TIMEOUT}s")
        print(f"API Retries: {cls.BREWERY_API_RETRIES}")
        print(f"Bronze Path: {cls.BRONZE_PATH}")
        print(f"Silver Path: {cls.SILVER_PATH}")
        print(f"Gold Path: {cls.GOLD_PATH}")
        print(f"Spark App Name: {cls.SPARK_APP_NAME}")
        print(f"Data Quality Checks: {cls.ENABLE_DATA_QUALITY_CHECKS}")
        print("=" * 80)
