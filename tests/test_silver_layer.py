"""
Unit tests for Silver Layer transformation.

Tests cover:
- Reading Bronze data
- Data cleaning and normalization
- Schema validation
- Metadata addition
- Data quality validation
- Writing to Silver layer
- Querying Silver data
"""

import pytest
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.layers.silver_layer import SilverLayer
from src.config.settings import Settings


@pytest.fixture(scope="module")
def spark_session():
    """
    Create a Spark session for testing.
    Uses utils.delta_spark for proper Delta Lake configuration.
    """
    from utils.delta_spark import initialize_spark
    spark = initialize_spark()
    
    yield spark
    
    from utils.delta_spark import stop_spark
    stop_spark(spark)


@pytest.fixture
def sample_brewery_data():
    """
    Sample brewery data for testing.
    """
    return [
        {
            "id": "brewery-1",
            "name": "Test Brewery",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "address_2": None,
            "address_3": None,
            "city": "San Francisco",
            "state_province": "California",
            "state": "California",
            "postal_code": "94102",
            "country": "United States",
            "longitude": -122.4194,
            "latitude": 37.7749,
            "phone": "5551234567",
            "website_url": "http://testbrewery.com",
            "street": "123 Main St"
        },
        {
            "id": "brewery-2",
            "name": "  Another Brewery  ",  # Test trimming
            "brewery_type": "brewpub",
            "address_1": "",  # Test empty string
            "address_2": None,
            "address_3": None,
            "city": "Los Angeles",
            "state_province": "California",
            "state": "California",
            "postal_code": "90001",
            "country": "USA",  # Test normalization
            "longitude": None,  # Test missing coordinates
            "latitude": None,
            "phone": None,
            "website_url": None,
            "street": ""
        },
        {
            "id": "brewery-3",
            "name": "UK Brewery",
            "brewery_type": "regional",
            "address_1": "456 High St",
            "address_2": None,
            "address_3": None,
            "city": "London",
            "state_province": None,
            "state": None,
            "postal_code": "SW1A 1AA",
            "country": "England",  # Test country normalization
            "longitude": -0.1276,
            "latitude": 51.5074,
            "phone": "4420123456",
            "website_url": "http://ukbrewery.com",
            "street": "456 High St"
        }
    ]


@pytest.fixture
def temp_bronze_path(sample_brewery_data):
    """
    Create temporary Bronze layer with sample data.
    """
    temp_dir = tempfile.mkdtemp()
    bronze_path = Path(temp_dir) / "bronze"
    
    # Create Bronze data structure
    today = datetime.now()
    data_path = (bronze_path / "breweries" / 
                 f"year={today.year}" / 
                 f"month={today.month:02d}" / 
                 f"day={today.day:02d}")
    data_path.mkdir(parents=True, exist_ok=True)
    
    # Write sample data as JSON array (same format as Bronze layer)
    data_file = data_path / "test_data.json"
    with open(data_file, 'w') as f:
        json.dump(sample_brewery_data, f, indent=2, ensure_ascii=False)
    
    yield str(bronze_path)
    
    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def temp_silver_path():
    """
    Create temporary Silver layer path.
    """
    temp_dir = tempfile.mkdtemp()
    silver_path = Path(temp_dir) / "silver"
    silver_path.mkdir(parents=True, exist_ok=True)
    
    yield str(silver_path)
    
    # Cleanup
    shutil.rmtree(temp_dir)


class TestSilverLayerInitialization:
    """Test Silver Layer initialization."""
    
    def test_initialization_with_defaults(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test Silver Layer initialization with default parameters."""
        with patch.object(Settings, 'BRONZE_PATH', temp_bronze_path), \
             patch.object(Settings, 'SILVER_PATH', temp_silver_path):
            
            silver = SilverLayer(spark=spark_session)
            
            assert silver.spark is not None
            assert silver.bronze_path == temp_bronze_path
            assert silver.silver_path == temp_silver_path
    
    def test_initialization_with_custom_paths(self, spark_session):
        """Test Silver Layer initialization with custom paths."""
        with tempfile.TemporaryDirectory() as temp_dir:
            custom_bronze = f"{temp_dir}/custom/bronze"
            custom_silver = f"{temp_dir}/custom/silver"
            
            silver = SilverLayer(
                spark=spark_session,
                bronze_path=custom_bronze,
                silver_path=custom_silver
            )
            
            assert silver.bronze_path == custom_bronze
            assert silver.silver_path == custom_silver
            assert Path(custom_silver).exists()  # Verify path was created


class TestBronzeDataReading:
    """Test reading data from Bronze layer."""
    
    def test_read_bronze_data(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test reading Bronze data successfully."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        
        assert df is not None
        assert df.count() == 3
        assert "id" in df.columns
        assert "name" in df.columns
    
    def test_read_bronze_data_with_date_filter(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test reading Bronze data with date partition filter."""
        today = datetime.now()
        
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data(
            "breweries",
            partition_date={
                "year": today.year,
                "month": today.month,
                "day": today.day
            }
        )
        
        assert df is not None
        assert df.count() == 3


class TestDataCleaning:
    """Test data cleaning and normalization."""
    
    def test_trim_whitespace(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test that whitespace is trimmed from strings."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        # Check that "  Another Brewery  " was trimmed
        names = [row.name for row in df_cleaned.collect()]
        assert "Another Brewery" in names
        assert "  Another Brewery  " not in names
    
    def test_empty_strings_to_null(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test that empty strings are converted to null."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        # Check that empty address_1 was converted to null
        brewery_2 = df_cleaned.filter(df_cleaned.id == "brewery-2").first()
        assert brewery_2.address_1 is None
    
    def test_country_normalization(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test country name normalization."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        # Check USA normalization
        brewery_2 = df_cleaned.filter(df_cleaned.id == "brewery-2").first()
        assert brewery_2.country_normalized == "United States"
        
        # Check England -> United Kingdom
        brewery_3 = df_cleaned.filter(df_cleaned.id == "brewery-3").first()
        assert brewery_3.country_normalized == "United Kingdom"
    
    def test_data_quality_flags(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test data quality flag creation."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        # Brewery 1: has coordinates and contact
        brewery_1 = df_cleaned.filter(df_cleaned.id == "brewery-1").first()
        assert brewery_1.has_coordinates == True
        assert brewery_1.has_contact == True
        assert brewery_1.is_complete == True
        
        # Brewery 2: no coordinates, no contact
        brewery_2 = df_cleaned.filter(df_cleaned.id == "brewery-2").first()
        assert brewery_2.has_coordinates == False
        assert brewery_2.has_contact == False
        assert brewery_2.is_complete == False


class TestMetadataAddition:
    """Test metadata column addition."""
    
    def test_add_metadata(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test adding metadata columns."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_with_meta = silver._add_metadata(df)
        
        # Check metadata columns exist
        assert "silver_processed_at" in df_with_meta.columns
        assert "processing_date" in df_with_meta.columns
        assert "processing_year" in df_with_meta.columns
        assert "processing_month" in df_with_meta.columns
        
        # Check values are not null
        first_row = df_with_meta.first()
        assert first_row.silver_processed_at is not None
        assert first_row.processing_date is not None


class TestDataQualityValidation:
    """Test data quality validation."""
    
    def test_validate_data_quality(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test data quality metrics calculation."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        metrics = silver._validate_data_quality(df_cleaned)
        
        assert metrics["total_records"] == 3
        assert metrics["unique_ids"] == 3
        assert "completeness_rate" in metrics
        assert "coordinate_coverage" in metrics
        assert "by_type" in metrics
        assert "by_country" in metrics


class TestSilverWriting:
    """Test writing data to Silver layer."""
    
    def test_write_to_silver_delta(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test writing to Silver layer in Delta Lake format."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        output_path = silver._write_to_silver(
            df_cleaned,
            dataset_name="test_breweries"
        )
        
        assert Path(output_path).exists()
        
        # Check that Delta log was created
        delta_log = Path(output_path) / "_delta_log"
        assert delta_log.exists()
        
        # Verify data can be read back via Delta format
        df_read = spark_session.read.format("delta").load(output_path)
        assert df_read.count() == 3
    
    def test_write_to_silver_with_partitions(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test writing with partitions."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        df = silver._read_bronze_data("breweries")
        df_cleaned = silver._clean_and_normalize(df)
        
        output_path = silver._write_to_silver(
            df_cleaned,
            dataset_name="test_breweries_partitioned",
            partition_by=["country_normalized"]
        )
        
        assert Path(output_path).exists()
        
        # Check that Delta log was created
        delta_log = Path(output_path) / "_delta_log"
        assert delta_log.exists()


class TestCompleteTransformation:
    """Test complete transformation pipeline."""
    
    def test_transform_breweries_success(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test complete transformation pipeline."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        metadata = silver.transform_breweries(
            partition_by=["country_normalized"]
        )
        
        assert metadata["status"] == "success"
        assert "quality_metrics" in metadata
        assert metadata["quality_metrics"]["total_records"] == 3
        assert metadata["output_format"] == "delta"
        assert Path(metadata["output_path"]).exists()
    
    def test_read_silver_data(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test reading data from Silver layer."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        # First transform
        silver.transform_breweries(
            partition_by=["country_normalized"]
        )
        
        # Then read
        df = silver.read_silver_data("breweries")
        
        assert df is not None
        assert df.count() == 3
    
    def test_read_silver_data_with_filters(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test reading Silver data with filters."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        # First transform
        silver.transform_breweries(
            partition_by=["country_normalized"]
        )
        
        # Read with filter
        df = silver.read_silver_data(
            "breweries",
            filters={"country_normalized": "United States"}
        )
        
        assert df is not None
        assert df.count() == 2  # brewery-1 and brewery-2


class TestStatistics:
    """Test statistics generation."""
    
    def test_get_statistics(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test getting statistics from Silver layer."""
        silver = SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        )
        
        # First transform
        silver.transform_breweries(
            partition_by=["country_normalized"]
        )
        
        # Get stats
        stats = silver.get_statistics("breweries")
        
        assert stats["total_records"] == 3
        assert "by_country" in stats
        assert "by_type" in stats
        assert stats["by_country"]["United States"] == 2


class TestContextManager:
    """Test context manager functionality."""
    
    def test_context_manager(self, spark_session, temp_bronze_path, temp_silver_path):
        """Test using Silver Layer as context manager."""
        session_stopped = False
        
        with SilverLayer(
            spark=spark_session,
            bronze_path=temp_bronze_path,
            silver_path=temp_silver_path
        ) as silver:
            assert silver is not None
            df = silver._read_bronze_data("breweries")
            assert df.count() == 3


@pytest.mark.integration
class TestIntegrationTransformation:
    """
    Integration test for complete transformation.
    
    Requires actual Bronze data to be present.
    """
    
    def test_full_transformation_pipeline(self, sample_brewery_data):
        """
        Test complete transformation with simulated Bronze data.
        
        This test is skipped by default. Run with: pytest -m integration
        """
        # Create separate temp paths for this test
        with tempfile.TemporaryDirectory() as temp_dir:
            bronze_path = Path(temp_dir) / "bronze"
            silver_path = Path(temp_dir) / "silver"
            
            # Create Bronze data structure
            today = datetime.now()
            data_path = (bronze_path / "breweries" / 
                        f"year={today.year}" / 
                        f"month={today.month:02d}" / 
                        f"day={today.day:02d}")
            data_path.mkdir(parents=True, exist_ok=True)
            
            # Write sample data as JSON array (same format as Bronze layer)
            data_file = data_path / "test_data.json"
            with open(data_file, 'w') as f:
                json.dump(sample_brewery_data, f, indent=2, ensure_ascii=False)
            
            # Create separate Spark session for this test with Delta support
            from utils.delta_spark import initialize_spark, stop_spark
            test_spark = initialize_spark()
            
            try:
                # Use temporary test data
                silver = SilverLayer(
                    spark=test_spark,
                    bronze_path=str(bronze_path),
                    silver_path=str(silver_path)
                )
                
                metadata = silver.transform_breweries()
                
                assert metadata["status"] == "success"
                assert metadata["quality_metrics"]["total_records"] == 3
                assert Path(metadata["output_path"]).exists()
            finally:
                stop_spark(test_spark)
