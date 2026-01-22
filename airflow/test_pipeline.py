#!/usr/bin/env python3
"""
Test the pipeline DAG tasks without Airflow.

This script simulates the DAG execution locally for testing purposes.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.layers import BronzeLayer, SilverLayer, GoldLayer
from src.config.settings import Settings


def test_bronze_task():
    """Test Bronze layer ingestion task."""
    print("\n" + "=" * 80)
    print("TESTING BRONZE LAYER TASK")
    print("=" * 80 + "\n")
    
    try:
        bronze = BronzeLayer(bronze_path=Settings.BRONZE_PATH)
        result = bronze.ingest_breweries()
        
        metadata = {
            'total_records': result['total_records'],
            'ingestion_id': result.get('ingestion_id', 'unknown'),
            'ingestion_path': result['file_path'],
            'status': result.get('status', 'success')
        }
        
        print(f"\n✅ Bronze task succeeded!")
        print(f"   Records: {metadata['total_records']:,}")
        print(f"   Ingestion ID: {metadata['ingestion_id'][:16]}...")
        
        return metadata
    except Exception as e:
        print(f"\n❌ Bronze task failed: {e}")
        return None


def test_silver_task(bronze_metadata):
    """Test Silver layer transformation task."""
    print("\n" + "=" * 80)
    print("TESTING SILVER LAYER TASK")
    print("=" * 80 + "\n")
    
    if not bronze_metadata:
        print("❌ Skipping - Bronze task failed")
        return None
    
    try:
        with SilverLayer() as silver:
            result = silver.transform_breweries()
            
            metadata = {
                'input_records': bronze_metadata['total_records'],
                'output_records': result['quality_metrics']['total_records'],
                'output_path': result['output_path'],
                'transformation_time': result['transformation_time_seconds'],
                'status': result['status']
            }
            
            print(f"\n✅ Silver task succeeded!")
            print(f"   Input: {metadata['input_records']:,}")
            print(f"   Output: {metadata['output_records']:,}")
            print(f"   Time: {metadata['transformation_time']}s")
            
            return metadata
    except Exception as e:
        print(f"\n❌ Silver task failed: {e}")
        return None


def test_gold_task(silver_metadata):
    """Test Gold layer aggregation task."""
    print("\n" + "=" * 80)
    print("TESTING GOLD LAYER TASK")
    print("=" * 80 + "\n")
    
    if not silver_metadata:
        print("❌ Skipping - Silver task failed")
        return None
    
    try:
        with GoldLayer() as gold:
            result = gold.create_aggregations()
            
            metadata = {
                'input_records': silver_metadata['output_records'],
                'total_aggregations': result['total_aggregations'],
                'status': 'success'
            }
            
            print(f"\n✅ Gold task succeeded!")
            print(f"   Aggregations: {metadata['total_aggregations']}")
            
            return metadata
    except Exception as e:
        print(f"\n❌ Gold task failed: {e}")
        return None


def main():
    """Run all tasks in sequence."""
    print("\n" + "=" * 80)
    print("BREWERIES PIPELINE - LOCAL TEST")
    print("=" * 80)
    
    start_time = datetime.now()
    
    # Execute tasks
    bronze_result = test_bronze_task()
    silver_result = test_silver_task(bronze_result)
    gold_result = test_gold_task(silver_result)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    # Summary
    print("\n" + "=" * 80)
    print("PIPELINE TEST SUMMARY")
    print("=" * 80)
    
    tasks_status = [
        ("Bronze", bronze_result is not None),
        ("Silver", silver_result is not None),
        ("Gold", gold_result is not None),
    ]
    
    for task_name, success in tasks_status:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status}: {task_name} Layer")
    
    success_count = sum(1 for _, success in tasks_status if success)
    print(f"\n📊 Results: {success_count}/3 tasks passed")
    print(f"⏱️  Duration: {duration:.2f}s")
    
    if success_count == 3:
        print("\n🎉 All tasks passed! DAG is ready to deploy.")
        sys.exit(0)
    else:
        print("\n⚠️  Some tasks failed. Fix issues before deploying.")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
