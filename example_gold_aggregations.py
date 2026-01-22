#!/usr/bin/env python3
"""
Example script to demonstrate Gold Layer aggregations.

This script shows how to:
1. Create business aggregations from Silver data
2. Query aggregation tables
3. View brewery statistics by type and location
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.layers.gold_layer import GoldLayer
from src.config.settings import Settings


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(title.center(80))
    print("=" * 80 + "\n")


def main():
    """Main demonstration function."""
    print_section("GOLD LAYER DEMONSTRATION")
    
    # Show current settings
    print("Current Settings:")
    print(f"  Silver Path: {Settings.SILVER_PATH}")
    print(f"  Gold Path: {Settings.GOLD_PATH}")
    print()
    
    # Check if Silver data exists
    silver_path = Path(Settings.SILVER_PATH) / "breweries"
    if not silver_path.exists():
        print("❌ ERROR: No Silver data found!")
        print(f"   Expected path: {silver_path}")
        print("\nPlease run Silver layer transformation first:")
        print("  python3 example_silver_transformation.py")
        return
    
    print("✓ Silver data found\n")
    
    # Interactive menu
    while True:
        print_section("OPTIONS")
        print("1. Create Gold Layer Aggregations")
        print("2. View Aggregation: Breweries by Type")
        print("3. View Aggregation: Breweries by Country")
        print("4. View Aggregation: Breweries by State (Top 20)")
        print("5. View Aggregation: Breweries by Type and Country")
        print("6. View Summary Statistics")
        print("7. List All Aggregations")
        print("8. Exit")
        print()
        
        choice = input("Choose an option (1-8): ").strip()
        
        if choice == "1":
            # Create aggregations
            print_section("CREATING GOLD LAYER AGGREGATIONS")
            
            with GoldLayer() as gold:
                print("Starting aggregation pipeline...")
                print("This will create:")
                print("  • Breweries by Type")
                print("  • Breweries by Country")
                print("  • Breweries by State")
                print("  • Breweries by Type and Country")
                print("  • Breweries by Type and State")
                print("  • Summary Statistics")
                print()
                
                input("Press Enter to continue...")
                
                # Perform aggregations
                metadata = gold.create_aggregations()
                
                if metadata["status"] == "success":
                    print("\n✓ Aggregations completed successfully!\n")
                    print("Aggregation Summary:")
                    print(f"  Total tables created: {metadata['total_aggregations']}")
                    print(f"  Processing time: {metadata['aggregation_time_seconds']:.2f}s")
                    print()
                    print("Created Tables:")
                    for agg in metadata['aggregations']:
                        print(f"  • {agg['table_name']:40s}: {agg['record_count']:5,} records")
                else:
                    print(f"\n❌ Aggregation failed: {metadata.get('error')}")
        
        elif choice == "2":
            # View by type
            print_section("BREWERIES BY TYPE")
            
            gold_path = Path(Settings.GOLD_PATH) / "breweries_by_type"
            if not gold_path.exists():
                print("❌ Aggregation not found. Please create aggregations first (option 1).")
                continue
            
            with GoldLayer() as gold:
                print("Loading aggregation...\n")
                df = gold.read_aggregation("breweries_by_type")
                
                print(f"Total brewery types: {df.count()}\n")
                print("Brewery Counts by Type:")
                print("-" * 100)
                
                rows = df.orderBy("brewery_count", ascending=False).collect()
                for row in rows:
                    brewery_type = row['brewery_type_normalized'] or 'unknown'
                    count = row['brewery_count']
                    completeness = row['completeness_rate']
                    
                    print(f"  {brewery_type:30s}: {count:5,} breweries (completeness: {completeness:5.1f}%)")
        
        elif choice == "3":
            # View by country
            print_section("BREWERIES BY COUNTRY")
            
            gold_path = Path(Settings.GOLD_PATH) / "breweries_by_country"
            if not gold_path.exists():
                print("❌ Aggregation not found. Please create aggregations first (option 1).")
                continue
            
            with GoldLayer() as gold:
                print("Loading aggregation...\n")
                df = gold.read_aggregation("breweries_by_country")
                
                print(f"Total countries: {df.count()}\n")
                print("Top 20 Countries by Brewery Count:")
                print("-" * 100)
                
                rows = df.orderBy("brewery_count", ascending=False).limit(20).collect()
                for i, row in enumerate(rows, 1):
                    country = row['country_normalized'] or 'unknown'
                    count = row['brewery_count']
                    types = row['distinct_types']
                    
                    print(f"  {i:2d}. {country:30s}: {count:5,} breweries ({types} types)")
        
        elif choice == "4":
            # View by state
            print_section("BREWERIES BY STATE (TOP 20)")
            
            gold_path = Path(Settings.GOLD_PATH) / "breweries_by_state"
            if not gold_path.exists():
                print("❌ Aggregation not found. Please create aggregations first (option 1).")
                continue
            
            with GoldLayer() as gold:
                print("Loading aggregation...\n")
                df = gold.read_aggregation("breweries_by_state")
                
                print(f"Total states: {df.count()}\n")
                print("Top 20 States by Brewery Count:")
                print("-" * 100)
                
                rows = df.orderBy("brewery_count", ascending=False).limit(20).collect()
                for i, row in enumerate(rows, 1):
                    country = row['country_normalized'] or 'unknown'
                    state = row['state'] or 'unknown'
                    count = row['brewery_count']
                    cities = row['distinct_cities']
                    
                    print(f"  {i:2d}. {state:25s} ({country:20s}): {count:5,} breweries in {cities} cities")
        
        elif choice == "5":
            # View by type and country
            print_section("BREWERIES BY TYPE AND COUNTRY")
            
            gold_path = Path(Settings.GOLD_PATH) / "breweries_by_type_and_country"
            if not gold_path.exists():
                print("❌ Aggregation not found. Please create aggregations first (option 1).")
                continue
            
            with GoldLayer() as gold:
                print("Select a brewery type to filter:")
                print("  1. micro")
                print("  2. brewpub")
                print("  3. large")
                print("  4. regional")
                print("  5. planning")
                print("  6. Show all")
                print()
                
                type_choice = input("Choose an option (1-6): ").strip()
                
                filters = None
                if type_choice == "1":
                    filters = {"brewery_type_normalized": "micro"}
                elif type_choice == "2":
                    filters = {"brewery_type_normalized": "brewpub"}
                elif type_choice == "3":
                    filters = {"brewery_type_normalized": "large"}
                elif type_choice == "4":
                    filters = {"brewery_type_normalized": "regional"}
                elif type_choice == "5":
                    filters = {"brewery_type_normalized": "planning"}
                
                print("\nLoading aggregation...\n")
                df = gold.read_aggregation("breweries_by_type_and_country", filters=filters)
                
                if filters:
                    print(f"Showing results for: {filters['brewery_type_normalized']}\n")
                
                print(f"Total combinations: {df.count()}\n")
                print("Brewery Counts by Type and Country:")
                print("-" * 100)
                
                rows = df.orderBy("brewery_type_normalized", "brewery_count", ascending=[True, False]).limit(30).collect()
                for row in rows:
                    brewery_type = row['brewery_type_normalized'] or 'unknown'
                    country = row['country_normalized'] or 'unknown'
                    count = row['brewery_count']
                    
                    print(f"  {brewery_type:20s} | {country:30s}: {count:5,} breweries")
        
        elif choice == "6":
            # View summary statistics
            print_section("SUMMARY STATISTICS")
            
            gold_path = Path(Settings.GOLD_PATH) / "brewery_summary_statistics"
            if not gold_path.exists():
                print("❌ Aggregation not found. Please create aggregations first (option 1).")
                continue
            
            with GoldLayer() as gold:
                print("Loading summary statistics...\n")
                df = gold.read_aggregation("brewery_summary_statistics")
                
                stats = df.collect()[0]
                
                print("Overall Statistics:")
                print("-" * 80)
                print(f"  Total Breweries:        {stats['total_breweries']:,}")
                print(f"  Unique Breweries:       {stats['unique_breweries']:,}")
                print(f"  Distinct Types:         {stats['distinct_types']:,}")
                print(f"  Distinct Countries:     {stats['distinct_countries']:,}")
                print(f"  Distinct States:        {stats['distinct_states']:,}")
                print(f"  Distinct Cities:        {stats['distinct_cities']:,}")
                print()
                print("Data Quality:")
                print("-" * 80)
                print(f"  With Coordinates:       {stats['with_coordinates']:,} ({(stats['with_coordinates']/stats['total_breweries']*100):.1f}%)")
                print(f"  With Contact Info:      {stats['with_contact']:,} ({(stats['with_contact']/stats['total_breweries']*100):.1f}%)")
                print(f"  Complete Records:       {stats['complete_records']:,} ({(stats['complete_records']/stats['total_breweries']*100):.1f}%)")
        
        elif choice == "7":
            # List all aggregations
            print_section("ALL AGGREGATIONS")
            
            with GoldLayer() as gold:
                tables = gold.list_aggregations()
                
                if tables:
                    print(f"Found {len(tables)} aggregation tables:\n")
                    for i, table in enumerate(tables, 1):
                        # Get record count
                        df = gold.read_aggregation(table)
                        count = df.count()
                        print(f"  {i}. {table:40s}: {count:,} records")
                else:
                    print("No aggregations found. Please create aggregations first (option 1).")
        
        elif choice == "8":
            print("\nExiting...")
            break
        
        else:
            print("\n❌ Invalid option. Please choose 1-8.")
        
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
