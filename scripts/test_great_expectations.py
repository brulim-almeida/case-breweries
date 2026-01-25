"""
Test script for Great Expectations validation

This script demonstrates how to use the BreweriesDataValidator
to validate data quality across all layers.

Usage:
    python scripts/test_great_expectations.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from src.validation import BreweriesDataValidator
from src.config.settings import Settings


def test_bronze_validation():
    """Test Bronze layer validation."""
    print("\n" + "=" * 80)
    print("Testing Bronze Layer Validation")
    print("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TestGreatExpectations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Read Bronze data
        bronze_df = spark.read.format("delta").load(Settings.BRONZE_PATH)
        print(f"✓ Loaded Bronze data: {bronze_df.count():,} records")
        
        # Initialize validator
        validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
        
        # Validate
        result = validator.validate_bronze_layer(
            df=bronze_df,
            execution_date="2026-01-24"
        )
        
        # Print results
        print(f"\n{'✅' if result['success'] else '❌'} Bronze Validation:")
        print(f"   Success Rate: {result['success_rate']:.1f}%")
        print(f"   Passed: {result['passed_expectations']}/{result['total_expectations']}")
        
        if not result['success']:
            print("\n   Failed Expectations:")
            for failure in result['failed_details']:
                print(f"   - {failure['expectation']}")
                print(f"     {failure['description']}")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Error testing Bronze validation: {e}")
        return False
    finally:
        spark.stop()


def test_silver_validation():
    """Test Silver layer validation."""
    print("\n" + "=" * 80)
    print("Testing Silver Layer Validation")
    print("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TestGreatExpectations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Read Silver data
        silver_df = spark.read.format("delta").load(Settings.SILVER_PATH)
        print(f"✓ Loaded Silver data: {silver_df.count():,} records")
        
        # Read Bronze count for comparison
        bronze_df = spark.read.format("delta").load(Settings.BRONZE_PATH)
        bronze_count = bronze_df.count()
        
        # Initialize validator
        validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
        
        # Validate
        result = validator.validate_silver_layer(
            df=silver_df,
            execution_date="2026-01-24",
            bronze_count=bronze_count
        )
        
        # Print results
        print(f"\n{'✅' if result['success'] else '❌'} Silver Validation:")
        print(f"   Success Rate: {result['success_rate']:.1f}%")
        print(f"   Passed: {result['passed_expectations']}/{result['total_expectations']}")
        
        # Print enrichment stats
        enrichment = result.get('enrichment_stats', {})
        print(f"\n   Enrichment Statistics:")
        print(f"   - Coordinate Coverage: {enrichment.get('coordinate_coverage', 0):.1%}")
        print(f"   - Valid Coordinates: {enrichment.get('valid_coordinates_rate', 0):.1%}")
        print(f"   - Geocoded: {enrichment.get('geocoded_rate', 0):.1%}")
        
        if not result['success']:
            print("\n   Failed Expectations:")
            for failure in result['failed_details']:
                print(f"   - {failure['expectation']}")
                print(f"     {failure['description']}")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Error testing Silver validation: {e}")
        return False
    finally:
        spark.stop()


def test_gold_validation():
    """Test Gold layer validation."""
    print("\n" + "=" * 80)
    print("Testing Gold Layer Validation")
    print("=" * 80)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("TestGreatExpectations") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Read Gold aggregations
        aggregations = {}
        gold_base = Settings.GOLD_PATH
        
        agg_names = [
            'breweries_by_country',
            'breweries_by_type',
            'breweries_by_state',
            'brewery_summary_statistics'
        ]
        
        for agg_name in agg_names:
            agg_path = f"{gold_base}/{agg_name}"
            try:
                agg_df = spark.read.format("delta").load(agg_path)
                aggregations[agg_name] = agg_df
                print(f"✓ Loaded {agg_name}: {agg_df.count():,} records")
            except Exception as e:
                print(f"⚠️  Could not load {agg_name}: {e}")
        
        # Get Silver count for validation
        silver_df = spark.read.format("delta").load(Settings.SILVER_PATH)
        silver_count = silver_df.count()
        
        # Initialize validator
        validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
        
        # Validate
        result = validator.validate_gold_layer(
            aggregations=aggregations,
            execution_date="2026-01-24",
            silver_count=silver_count
        )
        
        # Print results
        print(f"\n{'✅' if result['success'] else '❌'} Gold Validation:")
        print(f"   Total Aggregations: {result['summary']['total_aggregations']}")
        print(f"   Passed: {result['summary']['passed_aggregations']}")
        print(f"   Failed: {result['summary']['failed_aggregations']}")
        
        if not result['success']:
            print("\n   Failed Aggregations:")
            for agg_name, agg_result in result['aggregations'].items():
                if not agg_result['success']:
                    print(f"   - {agg_name}")
        
        return result['success']
        
    except Exception as e:
        print(f"❌ Error testing Gold validation: {e}")
        return False
    finally:
        spark.stop()


def main():
    """Run all validation tests."""
    print("\n" + "=" * 80)
    print("Great Expectations Validation Test Suite")
    print("=" * 80)
    
    results = {
        'bronze': test_bronze_validation(),
        'silver': test_silver_validation(),
        'gold': test_gold_validation()
    }
    
    # Summary
    print("\n" + "=" * 80)
    print("VALIDATION SUMMARY")
    print("=" * 80)
    
    for layer, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{layer.upper():10} {status}")
    
    overall_success = all(results.values())
    print(f"\n{'✅' if overall_success else '❌'} Overall Status: {'ALL PASSED' if overall_success else 'SOME FAILED'}")
    
    print("\n📄 View detailed reports in Data Docs:")
    print("   great_expectations/uncommitted/data_docs/local_site/index.html")
    print("=" * 80 + "\n")
    
    return 0 if overall_success else 1


if __name__ == "__main__":
    sys.exit(main())
