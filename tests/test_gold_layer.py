"""
Tests for Gold Layer aggregations.
"""

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)
from datetime import datetime
import shutil

from src.layers.gold_layer import GoldLayer
from src.config.settings import Settings
from utils.delta_spark import initialize_spark


@pytest.fixture(scope="function")
def spark_for_data():
    """Create a separate Spark session for data creation."""
    spark = initialize_spark(app_name="test_gold_data_prep")
    yield spark
    # Don't stop this session - let it be reused


@pytest.fixture(scope="session")
def spark_session():
    """Create a shared Spark session for testing."""
    spark = initialize_spark(app_name="test_gold_layer")
    yield spark
    spark.stop()


@pytest.fixture
def temp_silver_path(tmp_path):
    """Create a temporary Silver layer path."""
    silver_path = tmp_path / "silver" / "breweries"
    silver_path.mkdir(parents=True, exist_ok=True)
    return str(silver_path)


@pytest.fixture
def temp_gold_path(tmp_path):
    """Create a temporary Gold layer path."""
    gold_path = tmp_path / "gold"
    gold_path.mkdir(parents=True, exist_ok=True)
    return str(gold_path)


@pytest.fixture
def sample_silver_data(spark_for_data, temp_silver_path):
    """Create sample Silver data for testing."""
    schema = StructType([
        StructField("id", StringType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("brewery_type", StringType(), nullable=True),
        StructField("street", StringType(), nullable=True),
        StructField("address_1", StringType(), nullable=True),
        StructField("address_2", StringType(), nullable=True),
        StructField("address_3", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("website_url", StringType(), nullable=True),
        StructField("brewery_type_normalized", StringType(), nullable=True),
        StructField("country_normalized", StringType(), nullable=True),
        StructField("full_address", StringType(), nullable=True),
        StructField("coordinates_valid", IntegerType(), nullable=True),
        StructField("has_coordinates", IntegerType(), nullable=True),
        StructField("has_contact", IntegerType(), nullable=True),
        StructField("is_complete", IntegerType(), nullable=True),
        StructField("silver_processed_at", TimestampType(), nullable=True),
    ])

    # Create diverse sample data with different types and locations
    now = datetime.now()
    data = [
        # US - California breweries
        ("cal-1", "SF Brewery", "micro", "123 Main St", "123 Main St", None, None,
         "San Francisco", "California", "94102", "United States", -122.4194, 37.7749,
         "415-555-0001", "http://sfbrewery.com", "micro", "united states",
         "123 Main St, San Francisco, CA 94102", 1, 1, 1, 1, now),
        ("cal-2", "LA Brewpub", "brewpub", "456 Ocean Ave", "456 Ocean Ave", None, None,
         "Los Angeles", "California", "90001", "United States", -118.2437, 34.0522,
         None, "http://labrewpub.com", "brewpub", "united states",
         "456 Ocean Ave, Los Angeles, CA 90001", 1, 1, 1, 1, now),
        ("cal-3", "San Diego Large", "large", "789 Bay Rd", "789 Bay Rd", None, None,
         "San Diego", "California", "92101", "United States", -117.1611, 32.7157,
         "619-555-0003", "http://sdlarge.com", "large", "united states",
         "789 Bay Rd, San Diego, CA 92101", 1, 1, 1, 1, now),

        # US - Texas breweries
        ("tx-1", "Austin Micro", "micro", "101 Congress Ave", "101 Congress Ave", None, None,
         "Austin", "Texas", "78701", "United States", -97.7431, 30.2672,
         "512-555-0004", None, "micro", "united states",
         "101 Congress Ave, Austin, TX 78701", 1, 1, 1, 1, now),
        ("tx-2", "Houston Regional", "regional", "202 Main St", "202 Main St", None, None,
         "Houston", "Texas", "77002", "United States", -95.3698, 29.7604,
         None, None, "regional", "united states",
         "202 Main St, Houston, TX 77002", 1, 1, 0, 0, now),

        # US - New York breweries
        ("ny-1", "NYC Brewpub", "brewpub", "303 Broadway", "303 Broadway", None, None,
         "New York", "New York", "10007", "United States", -74.0060, 40.7128,
         "212-555-0006", "http://nycbrewpub.com", "brewpub", "united states",
         "303 Broadway, New York, NY 10007", 1, 1, 1, 1, now),

        # Canada - Ontario breweries
        ("on-1", "Toronto Micro", "micro", "404 King St", "404 King St", None, None,
         "Toronto", "Ontario", "M5H 1A1", "Canada", -79.3832, 43.6532,
         "+1-416-555-0007", "http://torontomicro.ca", "micro", "canada",
         "404 King St, Toronto, ON M5H 1A1", 1, 1, 1, 1, now),
        ("on-2", "Ottawa Planning", "planning", "505 Wellington St", "505 Wellington St", None, None,
         "Ottawa", "Ontario", "K1A 0A9", "Canada", None, None,
         None, None, "planning", "canada",
         "505 Wellington St, Ottawa, ON K1A 0A9", None, 0, 0, 0, now),

        # UK brewery
        ("uk-1", "London Brewpub", "brewpub", "606 Thames St", "606 Thames St", None, None,
         "London", "England", "SW1A 1AA", "United Kingdom", -0.1278, 51.5074,
         "+44-20-555-0009", "http://londonbrewpub.co.uk", "brewpub", "united kingdom",
         "606 Thames St, London, SW1A 1AA", 1, 1, 1, 1, now),

        # Incomplete data
        ("incomplete-1", "Unknown Brewery", None, None, None, None, None,
         None, None, None, None, None, None,
         None, None, None, None,
         None, None, 0, 0, 0, now),
    ]

    df = spark_for_data.createDataFrame(data, schema)

    # Write as Delta table
    df.write.format("delta").mode("overwrite").save(temp_silver_path)

    return temp_silver_path


@pytest.fixture
def gold_layer(temp_silver_path, temp_gold_path, monkeypatch):
    """Create a GoldLayer instance with temporary paths."""
    # Override settings
    monkeypatch.setattr(Settings, "SILVER_PATH", str(Path(temp_silver_path).parent))
    monkeypatch.setattr(Settings, "GOLD_PATH", temp_gold_path)
    
    gold = GoldLayer()
    yield gold
    gold.close()


class TestGoldLayerInit:
    """Tests for GoldLayer initialization."""
    
    def test_init_creates_spark_session(self, gold_layer):
        """Test that initialization creates a Spark session."""
        assert gold_layer.spark is not None
        assert isinstance(gold_layer.spark, SparkSession)
    
    def test_context_manager(self, temp_silver_path, temp_gold_path, monkeypatch):
        """Test that GoldLayer works as a context manager."""
        monkeypatch.setattr(Settings, "SILVER_PATH", str(Path(temp_silver_path).parent))
        monkeypatch.setattr(Settings, "GOLD_PATH", temp_gold_path)
        
        with GoldLayer() as gold:
            assert gold.spark is not None
        
        # Spark session should be stopped after context exit
        # (we can't easily test this without breaking other tests)


class TestGoldLayerReadSilver:
    """Tests for reading Silver data."""
    
    def test_read_silver_data(self, gold_layer, sample_silver_data):
        """Test reading Silver layer data."""
        df = gold_layer._read_silver_data()
        
        assert df is not None
        assert df.count() == 10
        
        # Check schema
        expected_columns = ["id", "brewery_type_normalized", "country_normalized", 
                          "state", "city", "has_coordinates", "has_contact"]
        for col in expected_columns:
            assert col in df.columns


class TestGoldLayerAggregations:
    """Tests for individual aggregation methods."""
    
    def test_aggregate_by_type(self, gold_layer, sample_silver_data):
        """Test aggregation by brewery type."""
        df = gold_layer._read_silver_data()
        result = gold_layer._aggregate_by_type(df)
        
        assert result is not None
        assert result.count() > 0
        
        # Check schema
        assert "brewery_type_normalized" in result.columns
        assert "brewery_count" in result.columns
        assert "completeness_rate" in result.columns
        
        # Verify data
        types = [row['brewery_type_normalized'] for row in result.collect()]
        assert "micro" in types
        assert "brewpub" in types
        assert "large" in types
        
        # Check micro breweries count (should be 3: cal-1, tx-1, on-1)
        micro_count = result.filter(result.brewery_type_normalized == "micro").first()
        assert micro_count['brewery_count'] == 3
    
    def test_aggregate_by_country(self, gold_layer, sample_silver_data):
        """Test aggregation by country."""
        df = gold_layer._read_silver_data()
        result = gold_layer._aggregate_by_country(df)
        
        assert result is not None
        assert result.count() > 0
        
        # Check schema
        assert "country_normalized" in result.columns
        assert "brewery_count" in result.columns
        assert "distinct_types" in result.columns
        
        # Verify data - US should have most breweries (6)
        us_data = result.filter(result.country_normalized == "united states").first()
        assert us_data is not None
        assert us_data['brewery_count'] == 6
        assert us_data['distinct_types'] >= 4  # micro, brewpub, large, regional
    
    def test_aggregate_by_state(self, gold_layer, sample_silver_data):
        """Test aggregation by state."""
        df = gold_layer._read_silver_data()
        result = gold_layer._aggregate_by_state(df)
        
        assert result is not None
        assert result.count() > 0
        
        # Check schema
        assert "country_normalized" in result.columns
        assert "state" in result.columns
        assert "brewery_count" in result.columns
        assert "distinct_cities" in result.columns
        
        # Verify California data (should have 3 breweries in 3 cities)
        ca_data = result.filter(
            (result.country_normalized == "united states") & 
            (result.state == "California")
        ).first()
        assert ca_data is not None
        assert ca_data['brewery_count'] == 3
        assert ca_data['distinct_cities'] == 3
    
    def test_aggregate_by_type_and_country(self, gold_layer, sample_silver_data):
        """Test aggregation by type and country."""
        df = gold_layer._read_silver_data()
        result = gold_layer._aggregate_by_type_and_country(df)
        
        assert result is not None
        assert result.count() > 0
        
        # Check schema
        assert "brewery_type_normalized" in result.columns
        assert "country_normalized" in result.columns
        assert "brewery_count" in result.columns
        
        # Verify US micro breweries (should be 2: cal-1, tx-1)
        us_micro = result.filter(
            (result.brewery_type_normalized == "micro") & 
            (result.country_normalized == "united states")
        ).first()
        assert us_micro is not None
        assert us_micro['brewery_count'] == 2
    
    def test_aggregate_by_type_and_state(self, gold_layer, sample_silver_data):
        """Test aggregation by type and state."""
        df = gold_layer._read_silver_data()
        result = gold_layer._aggregate_by_type_and_state(df)
        
        assert result is not None
        assert result.count() > 0
        
        # Check schema
        assert "brewery_type_normalized" in result.columns
        assert "country_normalized" in result.columns
        assert "state" in result.columns
        assert "brewery_count" in result.columns
        
        # Verify California micro breweries (should be 1: cal-1)
        ca_micro = result.filter(
            (result.brewery_type_normalized == "micro") & 
            (result.country_normalized == "united states") &
            (result.state == "California")
        ).first()
        assert ca_micro is not None
        assert ca_micro['brewery_count'] == 1
    
    def test_summary_statistics(self, gold_layer, sample_silver_data):
        """Test summary statistics generation."""
        df = gold_layer._read_silver_data()
        result = gold_layer._create_summary_statistics(df)
        
        assert result is not None
        assert result.count() == 1
        
        # Check schema
        expected_columns = [
            "total_breweries", "unique_breweries", "distinct_types",
            "distinct_countries", "distinct_states", "distinct_cities",
            "with_coordinates", "with_contact", "complete_records"
        ]
        for col in expected_columns:
            assert col in result.columns
        
        # Verify statistics
        stats = result.first()
        assert stats['total_breweries'] == 10
        assert stats['distinct_types'] >= 5
        assert stats['distinct_countries'] >= 3
        assert stats['with_coordinates'] > 0
        assert stats['with_contact'] > 0


class TestGoldLayerCreateAggregations:
    """Tests for the full aggregation pipeline."""
    
    def test_create_aggregations_success(self, gold_layer, sample_silver_data):
        """Test successful creation of all aggregations."""
        metadata = gold_layer.create_aggregations()

        assert metadata is not None
        assert metadata["status"] == "success"
        assert metadata["total_aggregations"] == 7
        assert metadata["aggregation_time_seconds"] > 0

        # Check that all aggregations were created
        expected_tables = [
            "breweries",
            "breweries_by_type",
            "breweries_by_country",
            "breweries_by_state",
            "breweries_by_type_and_country",
            "breweries_by_type_and_state",
            "brewery_summary_statistics"
        ]
        
        aggregation_names = [agg["table_name"] for agg in metadata["aggregations"]]
        for table in expected_tables:
            assert table in aggregation_names
        
        # Verify files exist
        gold_path = Path(Settings.GOLD_PATH)
        for table in expected_tables:
            table_path = gold_path / table
            assert table_path.exists(), f"Table {table} not found at {table_path}"
    
    def test_create_aggregations_writes_delta_format(self, gold_layer, sample_silver_data):
        """Test that aggregations are written in Delta format."""
        gold_layer.create_aggregations()
        
        gold_path = Path(Settings.GOLD_PATH)
        
        # Check that _delta_log exists for each table
        tables = ["breweries_by_type", "breweries_by_country"]
        for table in tables:
            delta_log = gold_path / table / "_delta_log"
            assert delta_log.exists(), f"Delta log not found for {table}"


class TestGoldLayerReadAggregation:
    """Tests for reading aggregations."""
    
    def test_read_aggregation_success(self, gold_layer, sample_silver_data):
        """Test reading an aggregation."""
        # First create aggregations
        gold_layer.create_aggregations()
        
        # Read aggregation
        df = gold_layer.read_aggregation("breweries_by_type")
        
        assert df is not None
        assert df.count() > 0
        assert "brewery_type_normalized" in df.columns
    
    def test_read_aggregation_with_filters(self, gold_layer, sample_silver_data):
        """Test reading an aggregation with filters."""
        # First create aggregations
        gold_layer.create_aggregations()
        
        # Read with filter
        filters = {"brewery_type_normalized": "micro"}
        df = gold_layer.read_aggregation("breweries_by_type", filters=filters)
        
        assert df is not None
        assert df.count() == 1
        
        row = df.first()
        assert row['brewery_type_normalized'] == "micro"
        assert row['brewery_count'] == 3
    
    def test_read_aggregation_not_found(self, gold_layer, sample_silver_data):
        """Test reading a non-existent aggregation."""
        with pytest.raises(ValueError, match="Aggregation table .* does not exist"):
            gold_layer.read_aggregation("non_existent_table")


class TestGoldLayerListAggregations:
    """Tests for listing aggregations."""
    
    def test_list_aggregations_empty(self, gold_layer):
        """Test listing aggregations when none exist."""
        tables = gold_layer.list_aggregations()
        assert tables == []
    
    def test_list_aggregations_with_data(self, gold_layer, sample_silver_data):
        """Test listing aggregations after creation."""
        # Create aggregations
        gold_layer.create_aggregations()

        # List aggregations
        tables = gold_layer.list_aggregations()

        assert len(tables) == 7
        assert "breweries" in tables
        assert "breweries_by_type" in tables
        assert "breweries_by_country" in tables
        assert "brewery_summary_statistics" in tables


class TestGoldLayerIntegration:
    """Integration tests with real data flow."""
    
    def test_end_to_end_aggregation_pipeline(self, temp_silver_path, temp_gold_path, monkeypatch):
        """Test complete pipeline from Silver to Gold."""
        # Override settings
        monkeypatch.setattr(Settings, "SILVER_PATH", str(Path(temp_silver_path).parent))
        monkeypatch.setattr(Settings, "GOLD_PATH", temp_gold_path)

        # Create fresh Spark session for this test
        spark = initialize_spark(app_name="test_integration")

        try:
            # Prepare Silver data with full schema
            schema = StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("brewery_type_normalized", StringType(), nullable=True),
                StructField("street", StringType(), nullable=True),
                StructField("address_1", StringType(), nullable=True),
                StructField("address_2", StringType(), nullable=True),
                StructField("address_3", StringType(), nullable=True),
                StructField("city", StringType(), nullable=True),
                StructField("state", StringType(), nullable=True),
                StructField("postal_code", StringType(), nullable=True),
                StructField("country_normalized", StringType(), nullable=True),
                StructField("full_address", StringType(), nullable=True),
                StructField("longitude", DoubleType(), nullable=True),
                StructField("latitude", DoubleType(), nullable=True),
                StructField("coordinates_valid", IntegerType(), nullable=True),
                StructField("has_coordinates", IntegerType(), nullable=True),
                StructField("phone", StringType(), nullable=True),
                StructField("website_url", StringType(), nullable=True),
                StructField("has_contact", IntegerType(), nullable=True),
                StructField("is_complete", IntegerType(), nullable=True),
                StructField("silver_processed_at", TimestampType(), nullable=True),
            ])

            now = datetime.now()
            data = [
                ("1", "Brewery A", "micro", "123 Main St", "123 Main St", None, None,
                 "SF", "California", "94102", "united states", "123 Main St, SF, CA",
                 -122.4194, 37.7749, 1, 1, "415-555-0001", "http://a.com", 1, 1, now),
                ("2", "Brewery B", "micro", "456 Oak Ave", "456 Oak Ave", None, None,
                 "LA", "California", "90001", "united states", "456 Oak Ave, LA, CA",
                 -118.2437, 34.0522, 1, 1, "213-555-0002", "http://b.com", 1, 1, now),
                ("3", "Brewery C", "brewpub", "789 King St", "789 King St", None, None,
                 "Toronto", "Ontario", "M5H 1A1", "canada", "789 King St, Toronto, ON",
                 -79.3832, 43.6532, 1, 1, None, None, 0, 0, now),
            ]

            df = spark.createDataFrame(data, schema)
            df.write.format("delta").mode("overwrite").save(temp_silver_path)

            # Run Gold layer
            with GoldLayer() as gold:
                metadata = gold.create_aggregations()

                # Verify results
                assert metadata["status"] == "success"

                # Check type aggregation
                type_df = gold.read_aggregation("breweries_by_type")
                assert type_df.count() == 2

                micro_row = type_df.filter(type_df.brewery_type_normalized == "micro").first()
                assert micro_row['brewery_count'] == 2

                # Check country aggregation
                country_df = gold.read_aggregation("breweries_by_country")
                assert country_df.count() == 2

                us_row = country_df.filter(country_df.country_normalized == "united states").first()
                assert us_row['brewery_count'] == 2

        finally:
            spark.stop()
