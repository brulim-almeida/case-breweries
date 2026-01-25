"""
Generate sample pipeline metadata for testing the Streamlit dashboard

This script creates sample metadata files to demonstrate the
Pipeline Metrics dashboard functionality.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import random

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.metadata_manager import PipelineMetadataManager


def generate_sample_metadata(num_runs: int = 10) -> None:
    """
    Generate sample pipeline metadata for testing.
    
    Args:
        num_runs: Number of historical runs to generate
    """
    print(f"Generating {num_runs} sample pipeline execution records...")
    
    metadata_mgr = PipelineMetadataManager()
    
    base_date = datetime.now() - timedelta(days=num_runs)
    
    for i in range(num_runs):
        # Generate realistic metadata
        execution_date = base_date + timedelta(days=i)
        
        # Random variations for realistic data
        records_base = 9000
        records_variation = random.randint(-200, 200)
        records_ingested = records_base + records_variation
        
        # Simulate some data loss
        data_loss = random.uniform(0.3, 2.5)
        records_transformed = int(records_ingested * (1 - data_loss / 100))
        
        # Execution times with some variation
        bronze_time = random.uniform(40, 60)
        silver_time = random.uniform(100, 150)
        gold_time = random.uniform(15, 25)
        total_time = bronze_time + silver_time + gold_time
        
        # Geocoding stats
        coord_coverage = random.uniform(0.84, 0.88)
        valid_coords = random.uniform(0.82, 0.86)
        geocoded_rate = random.uniform(0.10, 0.15)
        
        # Validation success rates
        bronze_success_rate = random.choice([100.0, 100.0, 100.0, 95.5])  # Usually 100%
        silver_success_rate = random.uniform(90, 100)
        gold_success_rate = random.choice([100.0, 100.0, 95.5])
        
        # Status (mostly success)
        status = 'success' if random.random() > 0.1 else 'failed'
        
        metadata = {
            'execution_date': execution_date.strftime('%Y-%m-%d'),
            'dag_run_id': f"scheduled__{execution_date.strftime('%Y-%m-%dT%H%M%S')}.000000+0000",
            'execution_timestamp': execution_date.isoformat(),
            'status': status,
            'execution_times': {
                'bronze_ingestion_time': round(bronze_time, 1),
                'silver_transformation_time': round(silver_time, 1),
                'gold_aggregation_time': round(gold_time, 1),
                'total_pipeline_time': round(total_time, 1)
            },
            'data_quality': {
                'records_ingested': records_ingested,
                'records_transformed': records_transformed,
                'aggregations_created': 6,
                'data_loss_rate': round(data_loss, 2)
            },
            'great_expectations': {
                'bronze_success': bronze_success_rate == 100.0,
                'silver_success': silver_success_rate >= 95.0,
                'gold_success': gold_success_rate == 100.0,
                'bronze_success_rate': bronze_success_rate,
                'silver_success_rate': round(silver_success_rate, 1),
                'gold_success_rate': gold_success_rate
            },
            'bronze_layer': {
                'records': records_ingested,
                'path': '/opt/airflow/lakehouse/bronze/breweries',
                'quality_success': bronze_success_rate == 100.0
            },
            'silver_layer': {
                'records': records_transformed,
                'countries': random.randint(43, 47),
                'types': 10,
                'path': '/opt/airflow/lakehouse/silver/breweries',
                'quality_success': silver_success_rate >= 95.0,
                'enrichment': {
                    'coordinate_coverage': round(coord_coverage, 4),
                    'valid_coordinates_rate': round(valid_coords, 4),
                    'geocoded_rate': round(geocoded_rate, 4),
                    'country_normalized_rate': 1.0
                }
            },
            'gold_layer': {
                'aggregations': 6,
                'processing_time': round(gold_time, 1),
                'quality_success': gold_success_rate == 100.0
            },
            'validation_results': {
                'bronze': {
                    'success': bronze_success_rate == 100.0,
                    'success_rate': bronze_success_rate,
                    'total_expectations': 8,
                    'passed': 8 if bronze_success_rate == 100.0 else 7,
                    'failed': 0 if bronze_success_rate == 100.0 else 1
                },
                'silver': {
                    'success': silver_success_rate >= 95.0,
                    'success_rate': round(silver_success_rate, 1),
                    'total_expectations': 6,
                    'passed': 6 if silver_success_rate >= 95.0 else 5,
                    'failed': 0 if silver_success_rate >= 95.0 else 1,
                    'enrichment_stats': {
                        'coordinate_coverage': round(coord_coverage, 4),
                        'valid_coordinates_rate': round(valid_coords, 4),
                        'geocoded_rate': round(geocoded_rate, 4),
                        'country_normalized_rate': 1.0
                    }
                },
                'gold': {
                    'success': gold_success_rate == 100.0,
                    'total_aggregations': 6,
                    'passed': 6,
                    'failed': 0
                }
            }
        }
        
        # Save metadata
        metadata_mgr.save_run_metadata(metadata)
        
        print(f"✓ Generated run {i+1}/{num_runs}: {execution_date.strftime('%Y-%m-%d')} - {status}")
    
    print(f"\n✅ Successfully generated {num_runs} sample pipeline runs")
    print(f"📄 Metadata saved to: {metadata_mgr.metadata_path}")
    print("\n🎯 Now you can:")
    print("   1. Open Streamlit dashboard")
    print("   2. Go to 'Pipeline Metrics' tab")
    print("   3. View the execution history and metrics")


if __name__ == "__main__":
    # Generate 20 sample runs
    generate_sample_metadata(num_runs=20)
