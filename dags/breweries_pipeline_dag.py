"""
Breweries Data Pipeline DAG

This DAG orchestrates the complete Medallion architecture pipeline:
Bronze (Ingestion) → Silver (Transformation) → Gold (Aggregation)

Schedule: Daily at 2 AM UTC
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.layers import BronzeLayer, SilverLayer, GoldLayer
from src.config.settings import Settings


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}


def on_failure_callback(context):
    """
    Callback function executed when a task fails.
    
    Args:
        context: Airflow context with task instance details
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context['execution_date']
    
    print(f"❌ TASK FAILED: {dag_id}.{task_id}")
    print(f"   Execution Date: {execution_date}")
    print(f"   Task Instance: {task_instance}")
    
    # Here you could integrate with alerting systems like:
    # - Slack notifications
    # - PagerDuty alerts
    # - Email with detailed logs
    # - Metrics to monitoring systems


def on_success_callback(context):
    """
    Callback function executed when the DAG succeeds.
    
    Args:
        context: Airflow context with DAG run details
    """
    dag_run = context['dag_run']
    print(f"✅ DAG SUCCESS: {dag_run.dag_id}")
    print(f"   Execution Date: {dag_run.execution_date}")


# Create the DAG
with DAG(
    dag_id='breweries_data_pipeline',
    default_args=default_args,
    description='End-to-end Breweries data pipeline (Bronze → Silver → Gold)',
    schedule_interval='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2026, 1, 21),  # Fixed start date
    catchup=False,
    max_active_runs=1,
    tags=['breweries', 'medallion', 'data-lake'],
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
) as dag:

    @task(
        task_id='bronze_ingestion',
        retries=3,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=30),
    )
    def ingest_bronze_data(**context):
        """
        Task 1: Ingest raw data from Open Brewery DB API to Bronze layer.
        
        Returns:
            dict: Metadata with ingestion statistics
        """
        print("=" * 80)
        print("STARTING BRONZE LAYER INGESTION")
        print("=" * 80)
        
        try:
            bronze = BronzeLayer(bronze_path=Settings.BRONZE_PATH)
            
            # Ingest all breweries
            result = bronze.ingest_breweries()
            
            # Push metadata to XCom for downstream tasks
            metadata = {
                'total_records': result['total_records'],
                'ingestion_id': result['ingestion_id'],
                'ingestion_path': result['file_path'],
                'pages_processed': result.get('pages_processed', 0),
                'status': 'success'
            }
            
            print(f"\n✅ Bronze ingestion completed successfully!")
            print(f"   Records ingested: {metadata['total_records']:,}")
            print(f"   Pages processed: {metadata['pages_processed']}")
            print(f"   Output path: {metadata['ingestion_path']}")
            
            return metadata
            
        except Exception as e:
            print(f"\n❌ Bronze ingestion failed: {e}")
            raise

    @task(
        task_id='silver_transformation',
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=60),  # Increased for geocoding
    )
    def transform_silver_data(bronze_metadata: dict, **context):
        """
        Task 2: Transform and curate Bronze data into Silver layer.
        Includes geocoding enrichment for missing coordinates.
        
        Args:
            bronze_metadata: Metadata from Bronze layer ingestion
            
        Returns:
            dict: Metadata with transformation and geocoding statistics
        """
        print("=" * 80)
        print("STARTING SILVER LAYER TRANSFORMATION (WITH GEOCODING)")
        print("=" * 80)
        
        # Validate Bronze layer data exists
        if bronze_metadata['status'] != 'success':
            raise ValueError("Bronze layer ingestion was not successful")
        
        print(f"Input records from Bronze: {bronze_metadata['total_records']:,}")
        
        try:
            with SilverLayer() as silver:
                # Transform Bronze to Silver with geocoding enabled
                result = silver.transform_breweries(
                    ingestion_path=bronze_metadata['ingestion_path'],
                    enable_geocoding=True,
                    max_geocoding_records=100  # Increased limit; set to None for all
                )
                
                # Validate transformation
                if result['quality_metrics']['total_records'] == 0:
                    raise ValueError("No records were written to Silver layer")
                
                # Extract geocoding metrics
                geocoding_data = result.get('geocoding_metrics', {})
                
                metadata = {
                    'input_records': bronze_metadata['total_records'],
                    'output_records': result['quality_metrics']['total_records'],
                    'completeness_rate': result['quality_metrics'].get('completeness_rate', 0),
                    'coordinate_coverage': result['quality_metrics'].get('coordinate_coverage', 0),
                    'contact_coverage': result['quality_metrics'].get('contact_coverage', 0),
                    'distinct_countries': len(result['quality_metrics'].get('by_country', {})),
                    'distinct_types': len(result['quality_metrics'].get('by_type', {})),
                    'output_path': result['output_path'],
                    'transformation_time': result['transformation_time_seconds'],
                    'status': result['status'],
                    # GEOCODING METRICS (NEW!)
                    'geocoding_enabled': geocoding_data.get('enabled', False),
                    'geocoding_stats': geocoding_data.get('enrichment_results', {}) if geocoding_data.get('enabled') else None
                }
                
                print(f"\n✅ Silver transformation completed successfully!")
                print(f"   Input records: {metadata['input_records']:,}")
                print(f"   Output records: {metadata['output_records']:,}")
                print(f"   Distinct countries: {metadata['distinct_countries']}")
                print(f"   Distinct brewery types: {metadata['distinct_types']}")
                print(f"   Completeness rate: {metadata['completeness_rate']}%")
                print(f"   Coordinate coverage: {metadata['coordinate_coverage']}%")
                print(f"   Contact coverage: {metadata['contact_coverage']}%")
                print(f"   Transformation time: {metadata['transformation_time']}s")
                
                # Display geocoding metrics
                if metadata['geocoding_enabled'] and metadata['geocoding_stats']:
                    print(f"\n🗺️  GEOCODING ENRICHMENT METRICS:")
                    print(f"   ✅ New coordinates added: {metadata['geocoding_stats'].get('new_coordinates_added', 0):,}")
                    print(f"   🎯 Success rate: {metadata['geocoding_stats'].get('success_rate_percentage', 0):.2f}%")
                    print(f"   📈 Coverage improvement: +{metadata['geocoding_stats'].get('improvement_percentage', 0):.2f}%")
                    print(f"   📊 Attempted: {metadata['geocoding_stats'].get('attempted_geocoding', 0):,}")
                    print(f"   ✔️  Successful: {metadata['geocoding_stats'].get('successful_geocoding', 0):,}")
                    print(f"   ❌ Failed: {metadata['geocoding_stats'].get('failed_geocoding', 0):,}")
                
                return metadata
                
        except Exception as e:
            print(f"\n❌ Silver transformation failed: {e}")
            raise

    @task(
        task_id='gold_aggregation',
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
    )
    def create_gold_aggregations(silver_metadata: dict, **context):
        """
        Task 3: Create business aggregations in Gold layer.
        
        Args:
            silver_metadata: Metadata from Silver layer transformation
            
        Returns:
            dict: Metadata with aggregation statistics
        """
        print("=" * 80)
        print("STARTING GOLD LAYER AGGREGATIONS")
        print("=" * 80)
        
        # Validate Silver layer data exists
        if silver_metadata['status'] != 'success':
            raise ValueError("Silver layer transformation was not successful")
        
        print(f"Input records from Silver: {silver_metadata['output_records']:,}")
        
        try:
            with GoldLayer() as gold:
                # Create all aggregations
                result = gold.create_aggregations()
                
                # Validate aggregations
                if result['status'] != 'success':
                    raise ValueError(f"Gold aggregation failed: {result.get('error')}")
                
                if result['total_aggregations'] == 0:
                    raise ValueError("No aggregations were created")
                
                metadata = {
                    'input_records': silver_metadata['output_records'],
                    'total_aggregations': result['total_aggregations'],
                    'aggregation_time': result['aggregation_time_seconds'],
                    'aggregations': result['aggregations'],
                    'status': 'success'
                }
                
                print(f"\n✅ Gold aggregations completed successfully!")
                print(f"   Input records: {metadata['input_records']:,}")
                print(f"   Total aggregations: {metadata['total_aggregations']}")
                print(f"   Processing time: {metadata['aggregation_time']:.2f}s")
                print("\n   Created aggregations:")
                for agg in metadata['aggregations']:
                    print(f"     • {agg['table_name']}: {agg['record_count']:,} records")
                
                return metadata
                
        except Exception as e:
            print(f"\n❌ Gold aggregation failed: {e}")
            raise

    @task(
        task_id='validate_pipeline',
        retries=1,
    )
    def validate_pipeline_execution(
        bronze_metadata: dict,
        silver_metadata: dict,
        gold_metadata: dict,
        **context
    ):
        """
        Task 4: Validate the complete pipeline execution and data quality.
        
        Args:
            bronze_metadata: Metadata from Bronze layer
            silver_metadata: Metadata from Silver layer
            gold_metadata: Metadata from Gold layer
            
        Returns:
            dict: Final pipeline execution report
        """
        print("=" * 80)
        print("PIPELINE VALIDATION")
        print("=" * 80)
        
        # Check if all metadata is available
        if not bronze_metadata or not silver_metadata or not gold_metadata:
            raise ValueError("Missing metadata from upstream tasks")
        
        # Validate each layer
        validations = {
            'bronze': bronze_metadata['status'] == 'success',
            'silver': silver_metadata['status'] == 'success',
            'gold': gold_metadata['status'] == 'success',
        }
        
        # Data quality checks
        data_quality = {
            'records_ingested': bronze_metadata['total_records'],
            'records_transformed': silver_metadata['output_records'],
            'aggregations_created': gold_metadata['total_aggregations'],
            'data_loss_rate': (
                (bronze_metadata['total_records'] - silver_metadata['output_records']) 
                / bronze_metadata['total_records'] * 100
            ) if bronze_metadata['total_records'] > 0 else 0,
        }
        
        # Generate report
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id,
            'validations': validations,
            'data_quality': data_quality,
            'bronze_layer': {
                'records': bronze_metadata['total_records'],
                'path': bronze_metadata['ingestion_path'],
            },
            'silver_layer': {
                'records': silver_metadata['output_records'],
                'countries': silver_metadata['distinct_countries'],
                'types': silver_metadata['distinct_types'],
                'path': silver_metadata['output_path'],
            },
            'gold_layer': {
                'aggregations': gold_metadata['total_aggregations'],
                'processing_time': gold_metadata['aggregation_time'],
            },
            'status': 'success' if all(validations.values()) else 'failed'
        }
        
        # Print summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"\n✅ Status: {report['status'].upper()}")
        print(f"\n📊 Data Flow:")
        print(f"   Bronze:  {data_quality['records_ingested']:>8,} records")
        print(f"   Silver:  {data_quality['records_transformed']:>8,} records")
        print(f"   Gold:    {data_quality['aggregations_created']:>8} aggregations")
        print(f"\n📉 Data Loss: {data_quality['data_loss_rate']:.2f}%")
        
        if data_quality['data_loss_rate'] > 5.0:
            print("   ⚠️  WARNING: Data loss exceeds 5% threshold")
        
        print("\n" + "=" * 80)
        
        return report

    # Define task dependencies using TaskFlow API
    bronze_result = ingest_bronze_data()
    silver_result = transform_silver_data(bronze_result)
    gold_result = create_gold_aggregations(silver_result)
    validation_result = validate_pipeline_execution(
        bronze_result,
        silver_result,
        gold_result
    )
    
    # Task flow: Bronze → Silver → Gold → Validation
    bronze_result >> silver_result >> gold_result >> validation_result
