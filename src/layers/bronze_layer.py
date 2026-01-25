"""
Bronze Layer - Raw Data Ingestion

This module handles the ingestion of raw data from the Open Brewery DB API
into the Bronze layer of the Data Lake. Data is stored in JSON format with
partitioning by ingestion date for efficient organization and retrieval.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import hashlib

from src.api.brewery_client import BreweryAPIClient
from src.config.settings import Settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BronzeLayer:
    """
    Bronze Layer handler for raw data ingestion.
    
    Responsibilities:
    - Fetch raw data from the Open Brewery DB API
    - Store data in native JSON format
    - Partition data by ingestion date (year/month/day)
    - Maintain metadata about ingestion runs
    - Ensure data persistence and recoverability
    
    Example:
        >>> bronze = BronzeLayer()
        >>> metadata = bronze.ingest_breweries()
        >>> print(f"Ingested {metadata['total_records']} breweries")
    """
    
    def __init__(
        self,
        bronze_path: Optional[str] = None,
        api_client: Optional[BreweryAPIClient] = None
    ):
        """
        Initialize the Bronze Layer processor.
        
        Args:
            bronze_path (str, optional): Base path for Bronze layer storage
            api_client (BreweryAPIClient, optional): API client instance
        """
        self.bronze_path = bronze_path or Settings.BRONZE_PATH
        self.api_client = api_client or BreweryAPIClient()
        
        # Ensure bronze path exists
        Path(self.bronze_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized BronzeLayer with path: {self.bronze_path}")
    
    def _get_ingestion_date_path(self, base_date: Optional[datetime] = None) -> str:
        """
        Generate partition path based on ingestion date (year/month/day).
        
        Args:
            base_date (datetime, optional): Base date for partitioning. Defaults to now.
            
        Returns:
            str: Formatted partition path (e.g., 'year=2026/month=01/day=21')
        """
        date = base_date or datetime.now()
        
        return (
            f"year={date.year}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}"
        )
    
    def _generate_file_hash(self, data: List[Dict[str, Any]]) -> str:
        """
        Generate a hash for the data to create unique filenames.
        
        Args:
            data (list): Data to hash
            
        Returns:
            str: MD5 hash of the data
        """
        data_string = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_string.encode()).hexdigest()[:8]
    
    def _save_json_file(
        self,
        data: List[Dict[str, Any]],
        file_path: str
    ) -> None:
        """
        Save data to a JSON file with proper formatting.
        
        Args:
            data (list): Data to save
            file_path (str): Destination file path
        """
        # Ensure directory exists
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save data with indentation for readability
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(data)} records to {file_path}")
    
    def _create_metadata(
        self,
        total_records: int,
        ingestion_timestamp: datetime,
        file_path: str,
        source: str = "open_brewery_db_api"
    ) -> Dict[str, Any]:
        """
        Create metadata for the ingestion run.
        
        Args:
            total_records (int): Total number of records ingested
            ingestion_timestamp (datetime): Timestamp of ingestion
            file_path (str): Path where data was saved
            source (str): Data source identifier
            
        Returns:
            dict: Metadata dictionary
        """
        return {
            "ingestion_id": hashlib.md5(
                f"{ingestion_timestamp.isoformat()}{file_path}".encode()
            ).hexdigest(),
            "ingestion_timestamp": ingestion_timestamp.isoformat(),
            "source": source,
            "total_records": total_records,
            "file_path": file_path,
            "file_size_bytes": Path(file_path).stat().st_size if Path(file_path).exists() else 0,
            "partition_date": {
                "year": ingestion_timestamp.year,
                "month": ingestion_timestamp.month,
                "day": ingestion_timestamp.day
            },
            "status": "success"
        }
    
    def _save_metadata(
        self,
        metadata: Dict[str, Any],
        dataset_name: str = "breweries"
    ) -> None:
        """
        Save ingestion metadata to a separate JSON file.
        
        Args:
            metadata (dict): Metadata to save
            dataset_name (str): Name of the dataset
        """
        metadata_path = (
            f"{self.bronze_path}/{dataset_name}/"
            f"_metadata/ingestion_id={metadata['ingestion_id']}.json"
        )
        
        Path(metadata_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved metadata to {metadata_path}")
    
    def ingest_breweries(
        self,
        max_pages: Optional[int] = None,
        save_metadata: bool = True
    ) -> Dict[str, Any]:
        """
        Ingest all brewery data from the API and save to Bronze layer.
        
        Args:
            max_pages (int, optional): Maximum pages to fetch (for testing)
            save_metadata (bool): Whether to save ingestion metadata
            
        Returns:
            dict: Metadata about the ingestion run
            
        Example:
            >>> bronze = BronzeLayer()
            >>> metadata = bronze.ingest_breweries()
            >>> print(f"Ingestion ID: {metadata['ingestion_id']}")
            >>> print(f"Total records: {metadata['total_records']}")
        """
        logger.info("=" * 80)
        logger.info("STARTING BRONZE LAYER INGESTION")
        logger.info("=" * 80)
        
        ingestion_timestamp = datetime.now()
        pages_processed = 0
        
        try:
            # Fetch all breweries from API
            logger.info("Fetching breweries from Open Brewery DB API...")
            breweries = self.api_client.get_all_breweries(max_pages=max_pages)
            
            # Calculate pages processed (assuming per_page from settings)
            if breweries:
                pages_processed = (len(breweries) + Settings.BREWERY_API_PER_PAGE - 1) // Settings.BREWERY_API_PER_PAGE
            
            if not breweries:
                logger.warning("No breweries fetched from API")
                return {
                    "status": "warning",
                    "message": "No data fetched",
                    "total_records": 0
                }
            
            # Generate partition path
            partition_path = self._get_ingestion_date_path(ingestion_timestamp)
            
            # Generate unique filename
            timestamp_str = ingestion_timestamp.strftime("%Y%m%d_%H%M%S")
            data_hash = self._generate_file_hash(breweries)
            filename = f"breweries_{timestamp_str}_{data_hash}.json"
            
            # Full file path
            file_path = (
                f"{self.bronze_path}/breweries/"
                f"{partition_path}/"
                f"{filename}"
            )
            
            # Save raw data
            logger.info(f"Saving {len(breweries)} breweries to Bronze layer...")
            self._save_json_file(breweries, file_path)
            
            # Create metadata
            metadata = self._create_metadata(
                total_records=len(breweries),
                ingestion_timestamp=ingestion_timestamp,
                file_path=file_path
            )
            
            # Add pages_processed to metadata
            metadata['pages_processed'] = pages_processed
            
            # Save metadata
            if save_metadata:
                self._save_metadata(metadata)
            
            logger.info("=" * 80)
            logger.info("BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY")
            logger.info(f"Total records ingested: {metadata['total_records']}")
            logger.info(f"Pages processed: {pages_processed}")
            logger.info(f"File path: {metadata['file_path']}")
            logger.info(f"File size: {metadata['file_size_bytes']:,} bytes")
            logger.info("=" * 80)
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error during Bronze layer ingestion: {e}", exc_info=True)
            
            error_metadata = {
                "status": "failed",
                "error": str(e),
                "ingestion_timestamp": ingestion_timestamp.isoformat(),
                "total_records": 0
            }
            
            return error_metadata
    
    def get_latest_ingestion(self, dataset_name: str = "breweries") -> Optional[Dict[str, Any]]:
        """
        Get metadata for the most recent ingestion.
        
        Args:
            dataset_name (str): Name of the dataset
            
        Returns:
            dict: Latest ingestion metadata, or None if no ingestions found
        """
        metadata_dir = f"{self.bronze_path}/{dataset_name}/_metadata"
        
        if not Path(metadata_dir).exists():
            logger.warning(f"No metadata directory found: {metadata_dir}")
            return None
        
        # Get all metadata files
        metadata_files = list(Path(metadata_dir).glob("*.json"))
        
        if not metadata_files:
            logger.warning(f"No metadata files found in {metadata_dir}")
            return None
        
        # Sort by modification time (most recent first)
        latest_file = max(metadata_files, key=lambda p: p.stat().st_mtime)
        
        # Load and return metadata
        with open(latest_file, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        logger.info(f"Latest ingestion: {metadata['ingestion_id']}")
        
        return metadata
    
    def list_ingestions(
        self,
        dataset_name: str = "breweries",
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        List all ingestion metadata, sorted by timestamp (newest first).
        
        Args:
            dataset_name (str): Name of the dataset
            limit (int, optional): Maximum number of ingestions to return
            
        Returns:
            list: List of ingestion metadata dictionaries
        """
        metadata_dir = f"{self.bronze_path}/{dataset_name}/_metadata"
        
        if not Path(metadata_dir).exists():
            logger.warning(f"No metadata directory found: {metadata_dir}")
            return []
        
        # Get all metadata files
        metadata_files = sorted(
            Path(metadata_dir).glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        
        if limit:
            metadata_files = metadata_files[:limit]
        
        # Load all metadata
        ingestions = []
        for file in metadata_files:
            with open(file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
                ingestions.append(metadata)
        
        logger.info(f"Found {len(ingestions)} ingestion(s)")
        
        return ingestions
    
    def clean_all_data(self, dataset_name: str = "breweries") -> None:
        """
        Clean all data from Bronze layer for a specific dataset.
        Useful for testing to avoid data duplication.
        
        Args:
            dataset_name (str): Name of the dataset to clean
        """
        import shutil
        
        base_path = f"{self.bronze_path}/{dataset_name}"
        
        if Path(base_path).exists():
            logger.info(f"🧹 Cleaning Bronze layer data at: {base_path}")
            shutil.rmtree(base_path)
            logger.info(f"✅ Successfully removed all data for dataset: {dataset_name}")
        else:
            logger.info(f"ℹ️  No data found to clean at: {base_path}")
    
    def read_bronze_data(
        self,
        dataset_name: str = "breweries",
        partition_date: Optional[Dict[str, int]] = None
    ) -> List[Dict[str, Any]]:
        """
        Read raw data from Bronze layer.
        
        Args:
            dataset_name (str): Name of the dataset
            partition_date (dict, optional): Partition filter (year, month, day)
            
        Returns:
            list: List of records from Bronze layer
        """
        base_path = f"{self.bronze_path}/{dataset_name}"
        
        if partition_date:
            # Build partition path
            partition_path = (
                f"year={partition_date.get('year', '*')}/"
                f"month={partition_date.get('month', '*'):02d}/"
                f"day={partition_date.get('day', '*'):02d}"
            )
            search_path = f"{base_path}/{partition_path}"
        else:
            search_path = base_path
        
        # Find all JSON files (excluding metadata)
        json_files = [
            f for f in Path(search_path).rglob("*.json")
            if "_metadata" not in str(f)
        ]
        
        logger.info(f"Found {len(json_files)} file(s) in Bronze layer")
        
        # Read and combine all data
        all_data = []
        for file in json_files:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                all_data.extend(data)
        
        logger.info(f"Read {len(all_data)} total records from Bronze layer")
        
        return all_data
    
    def close(self):
        """Close the API client."""
        if self.api_client:
            self.api_client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
