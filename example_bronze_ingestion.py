"""
Example script to demonstrate Bronze Layer ingestion

This script shows how to use the Bronze Layer to ingest brewery data
from the Open Brewery DB API.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.layers.bronze_layer import BronzeLayer
from src.config.settings import Settings


def main():
    """Main function to run Bronze layer ingestion."""
    
    print("\n" + "=" * 80)
    print("BREWERIES DATA LAKE - BRONZE LAYER INGESTION EXAMPLE")
    print("=" * 80 + "\n")
    
    # Display current settings
    Settings.display_settings()
    
    # Ensure paths exist
    Settings.ensure_paths_exist()
    
    # Create Bronze layer instance
    print("\nInitializing Bronze Layer...")
    with BronzeLayer() as bronze:
        
        # Option 1: Ingest all breweries (full load)
        print("\n" + "-" * 80)
        print("Option 1: Full data ingestion")
        print("-" * 80)
        response = input("Do you want to ingest ALL breweries? (y/n): ")
        
        if response.lower() == 'y':
            print("\nStarting full ingestion (this may take a few minutes)...")
            metadata = bronze.ingest_breweries()
        else:
            # Option 2: Ingest only first page (for testing)
            print("\n" + "-" * 80)
            print("Option 2: Test ingestion (1 page only)")
            print("-" * 80)
            print("\nStarting test ingestion (1 page only)...")
            metadata = bronze.ingest_breweries(max_pages=1)
        
        # Display results
        print("\n" + "=" * 80)
        print("INGESTION RESULTS")
        print("=" * 80)
        print(f"Status: {metadata.get('status', 'unknown')}")
        print(f"Total records: {metadata.get('total_records', 0):,}")
        print(f"Ingestion ID: {metadata.get('ingestion_id', 'N/A')}")
        print(f"File path: {metadata.get('file_path', 'N/A')}")
        
        if metadata.get('file_size_bytes'):
            size_mb = metadata['file_size_bytes'] / (1024 * 1024)
            print(f"File size: {size_mb:.2f} MB")
        
        print("=" * 80)
        
        # Show latest ingestion info
        print("\n" + "-" * 80)
        print("LATEST INGESTION INFO")
        print("-" * 80)
        latest = bronze.get_latest_ingestion()
        if latest:
            print(f"Ingestion timestamp: {latest.get('ingestion_timestamp')}")
            print(f"Total records: {latest.get('total_records'):,}")
        else:
            print("No previous ingestions found")
        
        # List all ingestions
        print("\n" + "-" * 80)
        print("INGESTION HISTORY (last 5)")
        print("-" * 80)
        ingestions = bronze.list_ingestions(limit=5)
        for i, ing in enumerate(ingestions, 1):
            print(f"\n{i}. Ingestion ID: {ing['ingestion_id']}")
            print(f"   Timestamp: {ing['ingestion_timestamp']}")
            print(f"   Records: {ing['total_records']:,}")
            print(f"   Status: {ing['status']}")
    
    print("\n" + "=" * 80)
    print("BRONZE LAYER INGESTION COMPLETED")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nIngestion interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        sys.exit(1)
