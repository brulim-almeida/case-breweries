#!/usr/bin/env python3
"""
Example script to demonstrate Silver Layer transformation.

This script shows how to:
1. Transform Bronze data to Silver layer
2. Read and query Silver data
3. Get statistics and quality metrics
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.layers.silver_layer import SilverLayer
from src.config.settings import Settings


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(title.center(80))
    print("=" * 80 + "\n")


def main():
    """Main demonstration function."""
    print_section("SILVER LAYER DEMONSTRATION")
    
    # Show current settings
    print("Current Settings:")
    print(f"  Bronze Path: {Settings.BRONZE_PATH}")
    print(f"  Silver Path: {Settings.SILVER_PATH}")
    print()
    
    # Check if Bronze data exists
    bronze_path = Path(Settings.BRONZE_PATH) / "breweries"
    if not bronze_path.exists():
        print("❌ ERROR: No Bronze data found!")
        print(f"   Expected path: {bronze_path}")
        print("\nPlease run Bronze layer ingestion first:")
        print("  python3 example_bronze_ingestion.py")
        return
    
    print("✓ Bronze data found\n")
    
    # Interactive menu
    while True:
        print_section("OPTIONS")
        print("1. Transform Bronze to Silver (Full transformation)")
        print("2. Read and query Silver data")
        print("3. Get Silver layer statistics")
        print("4. Exit")
        print()
        
        choice = input("Choose an option (1-4): ").strip()
        
        if choice == "1":
            # Full transformation
            print_section("TRANSFORMING BRONZE TO SILVER")
            
            with SilverLayer() as silver:
                print("Starting transformation pipeline...")
                print("This will:")
                print("  • Read JSON data from Bronze layer")
                print("  • Clean and normalize brewery data")
                print("  • Add metadata columns")
                print("  • Validate data quality")
                print("  • Write to Delta Lake format")
                print("  • Partition by country and state")
                print()
                
                input("Press Enter to continue...")
                
                # Perform transformation
                metadata = silver.transform_breweries(
                    partition_by=["country_normalized", "state"]
                )
                
                if metadata["status"] == "success":
                    print("\n✓ Transformation completed successfully!\n")
                    print("Transformation Summary:")
                    print(f"  Total records: {metadata['quality_metrics']['total_records']:,}")
                    print(f"  Output format: {metadata['output_format']}")
                    print(f"  Output path: {metadata['output_path']}")
                    print(f"  Processing time: {metadata['transformation_time_seconds']:.2f}s")
                    print()
                    print("Quality Metrics:")
                    qm = metadata['quality_metrics']
                    print(f"  Completeness rate: {qm.get('completeness_rate', 0):.2f}%")
                    print(f"  Coordinate coverage: {qm.get('coordinate_coverage', 0):.2f}%")
                    print(f"  Contact coverage: {qm.get('contact_coverage', 0):.2f}%")
                else:
                    print(f"\n❌ Transformation failed: {metadata.get('error')}")
        
        elif choice == "2":
            # Query Silver data
            print_section("QUERYING SILVER DATA")
            
            silver_path = Path(Settings.SILVER_PATH) / "breweries"
            if not silver_path.exists():
                print("❌ No Silver data found. Please run transformation first (option 1).")
                continue
            
            with SilverLayer() as silver:
                print("Available filters:")
                print("1. No filter (all data)")
                print("2. Filter by United States")
                print("3. Filter by California, USA")
                print("4. Filter by micro breweries")
                print()
                
                filter_choice = input("Choose filter (1-4): ").strip()
                
                filters = None
                if filter_choice == "2":
                    filters = {"country_normalized": "United States"}
                elif filter_choice == "3":
                    filters = {"country_normalized": "United States", "state": "California"}
                
                print("\nReading data...")
                df = silver.read_silver_data(filters=filters)
                
                print(f"\n✓ Loaded {df.count():,} records\n")
                
                # Show sample
                print("Sample records (first 5):")
                df.select(
                    "id", "name", "brewery_type_normalized", 
                    "city", "state", "country_normalized",
                    "has_coordinates", "has_contact"
                ).show(5, truncate=False)
                
                # Show schema
                print("\nSchema:")
                df.printSchema()
        
        elif choice == "3":
            # Get statistics
            print_section("SILVER LAYER STATISTICS")
            
            silver_path = Path(Settings.SILVER_PATH) / "breweries"
            if not silver_path.exists():
                print("❌ No Silver data found. Please run transformation first (option 1).")
                continue
            
            with SilverLayer() as silver:
                print("Calculating statistics...\n")
                stats = silver.get_statistics()
                
                print(f"Total records: {stats['total_records']:,}")
                print(f"Total columns: {len(stats['columns'])}")
                print(f"Partition columns: {', '.join(stats['partitions'])}")
                print()
                
                # Top countries
                print("Top 10 Countries:")
                for i, (country, count) in enumerate(list(stats['by_country'].items())[:10], 1):
                    pct = (count / stats['total_records']) * 100
                    print(f"  {i:2d}. {country:30s}: {count:5,} ({pct:5.2f}%)")
                
                print()
                
                # Top states
                print("Top 10 States:")
                for i, (state, count) in enumerate(list(stats['by_state'].items())[:10], 1):
                    pct = (count / stats['total_records']) * 100
                    print(f"  {i:2d}. {state:30s}: {count:5,} ({pct:5.2f}%)")
                
                print()
                
                # Brewery types
                print("Brewery Types:")
                for brewery_type, count in stats['by_type'].items():
                    pct = (count / stats['total_records']) * 100
                    print(f"  • {brewery_type:30s}: {count:5,} ({pct:5.2f}%)")
        
        elif choice == "4":
            print("\nExiting...")
            break
        
        else:
            print("\n❌ Invalid option. Please choose 1-4.")
        
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
