"""
Unit tests for Bronze Layer

This module contains comprehensive tests for the Bronze Layer functionality,
including API client tests and data ingestion tests.
"""

import json
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

from src.api.brewery_client import BreweryAPIClient
from src.layers.bronze_layer import BronzeLayer


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_brewery_data():
    """Sample brewery data for testing."""
    return [
        {
            "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
            "name": "Sample Brewing Company",
            "brewery_type": "micro",
            "address_1": "123 Main St",
            "city": "San Francisco",
            "state": "California",
            "postal_code": "94102",
            "country": "United States",
            "longitude": "-122.419906",
            "latitude": "37.774929",
            "phone": "4155551234",
            "website_url": "http://www.samplebrewery.com"
        },
        {
            "id": "9c5a66c8-cc13-416f-a5d9-0a769c87d318",
            "name": "Another Brewery",
            "brewery_type": "brewpub",
            "address_1": "456 Oak Ave",
            "city": "Austin",
            "state": "Texas",
            "postal_code": "78701",
            "country": "United States",
            "longitude": "-97.743061",
            "latitude": "30.267153",
            "phone": "5125555678",
            "website_url": "http://www.anotherbrewery.com"
        }
    ]


@pytest.fixture
def temp_bronze_path():
    """Create a temporary directory for Bronze layer testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def mock_api_client(sample_brewery_data):
    """Mock API client with sample data."""
    client = Mock(spec=BreweryAPIClient)
    client.get_all_breweries.return_value = sample_brewery_data
    client.get_breweries_page.return_value = sample_brewery_data
    return client


# =============================================================================
# API Client Tests
# =============================================================================

class TestBreweryAPIClient:
    """Tests for the Brewery API Client."""
    
    def test_client_initialization(self):
        """Test API client initialization."""
        client = BreweryAPIClient()
        
        assert client.base_url is not None
        assert client.timeout > 0
        assert client.retries >= 0
        assert client.session is not None
    
    @patch('src.api.brewery_client.requests.Session.get')
    def test_get_breweries_page_success(self, mock_get, sample_brewery_data):
        """Test successful API request for a single page."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = sample_brewery_data
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        client = BreweryAPIClient()
        breweries = client.get_breweries_page(page=1)
        
        assert len(breweries) == 2
        assert breweries[0]["name"] == "Sample Brewing Company"
        mock_get.assert_called_once()
    
    @patch('src.api.brewery_client.requests.Session.get')
    def test_get_breweries_page_timeout(self, mock_get):
        """Test API request timeout handling."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()
        
        client = BreweryAPIClient()
        
        with pytest.raises(requests.exceptions.Timeout):
            client.get_breweries_page(page=1)
    
    @patch('src.api.brewery_client.requests.Session.get')
    def test_get_all_breweries_pagination(self, mock_get, sample_brewery_data):
        """Test pagination through multiple pages."""
        # First page returns data, second page returns empty list
        mock_response_1 = Mock()
        mock_response_1.json.return_value = sample_brewery_data
        mock_response_1.raise_for_status = Mock()
        
        mock_response_2 = Mock()
        mock_response_2.json.return_value = []
        mock_response_2.raise_for_status = Mock()
        
        mock_get.side_effect = [mock_response_1, mock_response_2]
        
        client = BreweryAPIClient()
        all_breweries = client.get_all_breweries(delay_between_pages=0)
        
        assert len(all_breweries) == 2
        assert mock_get.call_count == 2
    
    def test_context_manager(self, mock_api_client):
        """Test API client as context manager."""
        with BreweryAPIClient() as client:
            assert client is not None
        
        # Session should be closed after exiting context


# =============================================================================
# Bronze Layer Tests
# =============================================================================

class TestBronzeLayer:
    """Tests for the Bronze Layer."""
    
    def test_bronze_layer_initialization(self, temp_bronze_path, mock_api_client):
        """Test Bronze layer initialization."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        assert bronze.bronze_path == temp_bronze_path
        assert Path(temp_bronze_path).exists()
    
    def test_get_ingestion_date_path(self, temp_bronze_path, mock_api_client):
        """Test partition path generation."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        test_date = datetime(2026, 1, 21, 10, 30, 0)
        partition_path = bronze._get_ingestion_date_path(test_date)
        
        assert partition_path == "year=2026/month=01/day=21"
    
    def test_ingest_breweries_success(
        self,
        temp_bronze_path,
        mock_api_client,
        sample_brewery_data
    ):
        """Test successful brewery data ingestion."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        metadata = bronze.ingest_breweries()
        
        # Verify metadata
        assert metadata["status"] == "success"
        assert metadata["total_records"] == 2
        assert "ingestion_id" in metadata
        assert "file_path" in metadata
        
        # Verify file was created
        assert Path(metadata["file_path"]).exists()
        
        # Verify data content
        with open(metadata["file_path"], 'r') as f:
            saved_data = json.load(f)
        
        assert len(saved_data) == 2
        assert saved_data[0]["name"] == "Sample Brewing Company"
    
    def test_ingest_breweries_no_data(self, temp_bronze_path):
        """Test ingestion when API returns no data."""
        mock_client = Mock(spec=BreweryAPIClient)
        mock_client.get_all_breweries.return_value = []
        
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_client
        )
        
        metadata = bronze.ingest_breweries()
        
        assert metadata["status"] == "warning"
        assert metadata["total_records"] == 0
    
    def test_ingest_breweries_api_error(self, temp_bronze_path):
        """Test ingestion when API raises an error."""
        mock_client = Mock(spec=BreweryAPIClient)
        mock_client.get_all_breweries.side_effect = Exception("API Error")
        
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_client
        )
        
        metadata = bronze.ingest_breweries()
        
        assert metadata["status"] == "failed"
        assert "error" in metadata
        assert metadata["total_records"] == 0
    
    def test_save_and_read_metadata(
        self,
        temp_bronze_path,
        mock_api_client,
        sample_brewery_data
    ):
        """Test metadata saving and retrieval."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        # Ingest data
        metadata = bronze.ingest_breweries()
        
        # Retrieve latest ingestion metadata
        latest = bronze.get_latest_ingestion()
        
        assert latest is not None
        assert latest["ingestion_id"] == metadata["ingestion_id"]
        assert latest["total_records"] == 2
    
    def test_list_ingestions(
        self,
        temp_bronze_path,
        mock_api_client,
        sample_brewery_data
    ):
        """Test listing all ingestions."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        # Perform multiple ingestions
        bronze.ingest_breweries()
        bronze.ingest_breweries()
        
        # List all ingestions
        ingestions = bronze.list_ingestions()
        
        assert len(ingestions) == 2
        assert all("ingestion_id" in ing for ing in ingestions)
    
    def test_read_bronze_data(
        self,
        temp_bronze_path,
        mock_api_client,
        sample_brewery_data
    ):
        """Test reading data from Bronze layer."""
        bronze = BronzeLayer(
            bronze_path=temp_bronze_path,
            api_client=mock_api_client
        )
        
        # Ingest data
        bronze.ingest_breweries()
        
        # Read data back
        data = bronze.read_bronze_data()
        
        assert len(data) == 2
        assert data[0]["name"] == "Sample Brewing Company"
    
    def test_context_manager(self, temp_bronze_path, mock_api_client):
        """Test Bronze layer as context manager."""
        with BronzeLayer(bronze_path=temp_bronze_path, api_client=mock_api_client) as bronze:
            assert bronze is not None
            metadata = bronze.ingest_breweries()
            assert metadata["status"] == "success"


# =============================================================================
# Integration Tests
# =============================================================================

class TestBronzeLayerIntegration:
    """Integration tests for Bronze Layer (requires API access)."""
    
    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires actual API access")
    def test_real_api_ingestion(self, temp_bronze_path):
        """Test ingestion with real API (integration test)."""
        bronze = BronzeLayer(bronze_path=temp_bronze_path)
        
        # Fetch only 1 page for testing
        metadata = bronze.ingest_breweries(max_pages=1)
        
        assert metadata["status"] == "success"
        assert metadata["total_records"] > 0
        assert Path(metadata["file_path"]).exists()


# =============================================================================
# Run tests
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
