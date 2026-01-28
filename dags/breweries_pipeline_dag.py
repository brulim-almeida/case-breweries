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
from src.validation import BreweriesDataValidator
from utils.metadata_manager import PipelineMetadataManager
from utils.delta_spark import initialize_spark, stop_spark


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
    params={
        'clean_before_run': False,  # Set to True during testing to avoid duplicates
    }
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
        
        # Get clean_before_run parameter
        clean_before_run = context['params'].get('clean_before_run', False)
        
        try:
            bronze = BronzeLayer(bronze_path=Settings.BRONZE_PATH)
            
            # Clean data if requested (useful for testing)
            if clean_before_run:
                print("\n🧹 CLEANING EXISTING DATA (clean_before_run=True)")
                bronze.clean_all_data()
                print("✅ Data cleaned successfully\n")
            
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
        task_id='validate_bronze_quality',
        execution_timeout=timedelta(minutes=15),
    )
    def validate_bronze_quality(bronze_metadata: dict, **context):
        """
        Task 2.5: Validate Bronze Layer data quality using Great Expectations.
        
        📊 VALIDAÇÕES REALIZADAS:
        - Schema da API completo e correto
        - IDs únicos (sem duplicatas)
        - Campos obrigatórios preenchidos
        - Volume de dados dentro do esperado (5k-50k)
        - Tipos de cervejaria conhecidos
        - Coordenadas em ranges válidos
        - Detecção de anomalias de volume
        
        Args:
            bronze_metadata: Metadata from Bronze ingestion
            
        Returns:
            dict: Validation results
        """
        print("=" * 80)
        print("🔍 VALIDATING BRONZE LAYER WITH GREAT EXPECTATIONS")
        print("=" * 80)
        
        try:
            # Check if bronze ingestion was successful
            if bronze_metadata.get('status') == 'failed':
                error_msg = bronze_metadata.get('error', 'Unknown error')
                print(f"⚠️  Skipping validation - Bronze ingestion failed: {error_msg}")
                return {
                    'success': False,
                    'error': f'Bronze ingestion failed: {error_msg}',
                    'skipped': True
                }

            if bronze_metadata.get('status') == 'warning' or bronze_metadata.get('total_records', 0) == 0:
                print(f"⚠️  Skipping validation - No data to validate")
                return {
                    'success': False,
                    'error': 'No data ingested',
                    'skipped': True
                }

            # Use the project's Spark session with Delta Lake support
            spark = initialize_spark(app_name="BreweriesValidation-Bronze")

            # Read Bronze data from the CURRENT ingestion only (not all historical files)
            # This prevents duplicate records when validating
            # Note: bronze_metadata uses 'ingestion_path' key (renamed from 'file_path' in bronze task)
            current_file_path = bronze_metadata.get('ingestion_path')
            if not current_file_path:
                raise ValueError(
                    "Bronze metadata does not contain 'ingestion_path'. "
                    "This usually means the bronze ingestion did not complete successfully. "
                    f"Bronze metadata keys: {list(bronze_metadata.keys())}"
                )

            # Check if file exists
            from pathlib import Path
            if not Path(current_file_path).exists():
                raise FileNotFoundError(
                    f"Bronze data file not found: {current_file_path}. "
                    "The file may have been deleted or the ingestion failed."
                )

            print(f"📂 Reading bronze data from: {current_file_path}")
            bronze_df = spark.read.option("multiLine", "true").json(current_file_path)
            
            # Get previous count for anomaly detection
            previous_count = context['ti'].xcom_pull(
                task_ids='bronze_ingestion',
                key='previous_count'
            ) if context else None
            
            # Initialize validator
            validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
            
            # Validate
            result = validator.validate_bronze_layer(
                df=bronze_df,
                execution_date=context['ds'],
                previous_count=previous_count
            )
            
            # Store current count for next execution
            context['ti'].xcom_push(key='previous_count', value=bronze_metadata['total_records'])
            
            print(f"\n📊 Validation Summary:")
            print(f"   Success: {'✅ YES' if result['success'] else '❌ NO'}")
            print(f"   Success Rate: {result['success_rate']:.1f}%")
            print(f"   Passed: {result['passed_expectations']}/{result['total_expectations']}")
            
            if not result['success']:
                print(f"\n⚠️  WARNING: {result['failed_expectations_count']} expectations failed")
                for failure in result['failed_details'][:5]:  # Show first 5
                    print(f"   - {failure['expectation']}: {failure['description']}")
            
            return result
            
        except Exception as e:
            print(f"❌ Bronze validation failed: {e}")
            # Don't fail the pipeline, just log
            return {'success': False, 'error': str(e)}
        finally:
            # Always stop Spark session
            if 'spark' in locals():
                stop_spark(spark)
    
    @task(
        task_id='validate_silver_quality',
        execution_timeout=timedelta(minutes=15),
    )
    def validate_silver_quality(silver_metadata: dict, bronze_metadata: dict, **context):
        """
        Task 3.5: Validate Silver Layer data quality using Great Expectations.
        
        📊 VALIDAÇÕES REALIZADAS:
        - Data Loss < 5% (Bronze → Silver)
        - País normalizado para 100% dos registros
        - 85%+ com coordenadas válidas (pós-geocoding)
        - Validação geográfica (coords batem com país)
        - Sem pontos em "Null Island" (0,0)
        - URLs e telefones normalizados
        
        Args:
            silver_metadata: Metadata from Silver transformation
            bronze_metadata: Metadata from Bronze (for loss calculation)
            
        Returns:
            dict: Validation results
        """
        print("=" * 80)
        print("🔍 VALIDATING SILVER LAYER WITH GREAT EXPECTATIONS")
        print("=" * 80)
        
        try:
            # Use the project's Spark session with Delta Lake support
            spark = initialize_spark(app_name="BreweriesValidation-Silver")
            
            # Read Silver data (breweries table)
            silver_path = f"{Settings.SILVER_PATH}/breweries"
            silver_df = spark.read.format("delta").load(silver_path)
            
            # Initialize validator
            validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
            
            # Validate
            result = validator.validate_silver_layer(
                df=silver_df,
                execution_date=context['ds'],
                bronze_count=bronze_metadata['total_records']
            )
            
            print(f"\n📊 Validation Summary:")
            print(f"   Success: {'✅ YES' if result['success'] else '❌ NO'}")
            print(f"   Success Rate: {result['success_rate']:.1f}%")
            print(f"   Passed: {result['passed_expectations']}/{result['total_expectations']}")
            
            print(f"\n🌍 Enrichment Stats:")
            print(f"   Coordinate Coverage: {result['enrichment_stats']['coordinate_coverage']:.1%}")
            print(f"   Valid Coordinates: {result['enrichment_stats']['valid_coordinates_rate']:.1%}")
            print(f"   Geocoded: {result['enrichment_stats']['geocoded_rate']:.1%}")
            
            if not result['success']:
                print(f"\n⚠️  WARNING: {result['failed_expectations_count']} expectations failed")
                for failure in result['failed_details'][:5]:
                    print(f"   - {failure['expectation']}: {failure['description']}")
            
            return result
            
        except Exception as e:
            print(f"❌ Silver validation failed: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            # Always stop Spark session
            if 'spark' in locals():
                stop_spark(spark)
    
    @task(
        task_id='validate_gold_quality',
        execution_timeout=timedelta(minutes=15),
    )
    def validate_gold_quality(gold_metadata: dict, silver_metadata: dict, **context):
        """
        Task 4.5: Validate Gold Layer aggregations using Great Expectations.
        
        📊 VALIDAÇÕES REALIZADAS:
        - Agregações não vazias
        - Counts positivos e <= total Silver
        - USA presente no top países
        - 'micro' presente nos tipos
        - Integridade matemática das agregações
        
        Args:
            gold_metadata: Metadata from Gold layer
            silver_metadata: Metadata from Silver (for consistency checks)
            
        Returns:
            dict: Validation results
        """
        print("=" * 80)
        print("🔍 VALIDATING GOLD LAYER WITH GREAT EXPECTATIONS")
        print("=" * 80)
        
        try:
            # Use the project's Spark session with Delta Lake support
            spark = initialize_spark(app_name="BreweriesValidation-Gold")
            
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
                    aggregations[agg_name] = spark.read.format("delta").load(agg_path)
                except Exception as e:
                    print(f"⚠️  Could not read {agg_name}: {e}")
            
            # Initialize validator
            validator = BreweriesDataValidator(spark=spark, enable_data_docs=True)
            
            # Validate
            result = validator.validate_gold_layer(
                aggregations=aggregations,
                execution_date=context['ds'],
                silver_count=silver_metadata['output_records']
            )
            
            print(f"\n📊 Validation Summary:")
            print(f"   Success: {'✅ YES' if result['success'] else '❌ NO'}")
            print(f"   Total Aggregations: {result['summary']['total_aggregations']}")
            print(f"   Passed: {result['summary']['passed_aggregations']}")
            print(f"   Failed: {result['summary']['failed_aggregations']}")
            
            if not result['success']:
                print("\n⚠️  WARNING: Some aggregations failed validation")
                for agg_name, agg_result in result['aggregations'].items():
                    if not agg_result['success']:
                        print(f"   ❌ {agg_name}")
            
            return result
            
        except Exception as e:
            print(f"❌ Gold validation failed: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            # Always stop Spark session
            if 'spark' in locals():
                stop_spark(spark)

    @task(
        task_id='validate_pipeline',
        retries=1,
    )
    def validate_pipeline_execution(
        bronze_metadata: dict,
        silver_metadata: dict,
        gold_metadata: dict,
        bronze_validation: dict,
        silver_validation: dict,
        gold_validation: dict,
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
        
        # Great Expectations validation summary
        ge_summary = {
            'bronze_success': bronze_validation.get('success', False),
            'silver_success': silver_validation.get('success', False),
            'gold_success': gold_validation.get('success', False),
            'bronze_success_rate': bronze_validation.get('success_rate', 0),
            'silver_success_rate': silver_validation.get('success_rate', 0),
            'gold_success_rate': gold_validation.get('success_rate', 0),
        }
        
        # Generate report
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id,
            'validations': validations,
            'data_quality': data_quality,
            'great_expectations': ge_summary,
            'bronze_layer': {
                'records': bronze_metadata['total_records'],
                'path': bronze_metadata['ingestion_path'],
                'quality_success': bronze_validation.get('success', False),
            },
            'silver_layer': {
                'records': silver_metadata['output_records'],
                'countries': silver_metadata['distinct_countries'],
                'types': silver_metadata['distinct_types'],
                'path': silver_metadata['output_path'],
                'quality_success': silver_validation.get('success', False),
                'enrichment': silver_validation.get('enrichment_stats', {}),
            },
            'gold_layer': {
                'aggregations': gold_metadata['total_aggregations'],
                'processing_time': gold_metadata['aggregation_time'],
                'quality_success': gold_validation.get('success', False),
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
        
        print(f"\n🔍 Great Expectations Quality Checks:")
        print(f"   Bronze: {'✅' if ge_summary['bronze_success'] else '❌'} ({ge_summary['bronze_success_rate']:.1f}%)")
        print(f"   Silver: {'✅' if ge_summary['silver_success'] else '❌'} ({ge_summary['silver_success_rate']:.1f}%)")
        print(f"   Gold:   {'✅' if ge_summary['gold_success'] else '❌'} ({ge_summary['gold_success_rate']:.1f}%)")
        
        print("\n" + "=" * 80)
        
        # Save metadata for Streamlit visualization
        try:
            metadata_manager = PipelineMetadataManager()
            
            # Prepare execution times
            execution_times = {
                'bronze_ingestion_time': bronze_metadata.get('pages_processed', 0) * 0.5,  # Approximate
                'silver_transformation_time': silver_metadata.get('transformation_time', 0),
                'gold_aggregation_time': gold_metadata.get('aggregation_time', 0),
                'total_pipeline_time': (
                    bronze_metadata.get('pages_processed', 0) * 0.5 +
                    silver_metadata.get('transformation_time', 0) +
                    gold_metadata.get('aggregation_time', 0)
                )
            }
            
            # Save complete metadata
            metadata_manager.save_run_metadata({
                **report,
                'execution_times': execution_times,
                'validation_results': {
                    'bronze': {
                        'success': bronze_validation.get('success', False),
                        'success_rate': bronze_validation.get('success_rate', 0),
                        'total_expectations': bronze_validation.get('total_expectations', 0),
                        'passed': bronze_validation.get('passed_expectations', 0),
                        'failed': bronze_validation.get('failed_expectations_count', 0)
                    },
                    'silver': {
                        'success': silver_validation.get('success', False),
                        'success_rate': silver_validation.get('success_rate', 0),
                        'total_expectations': silver_validation.get('total_expectations', 0),
                        'passed': silver_validation.get('passed_expectations', 0),
                        'failed': silver_validation.get('failed_expectations_count', 0),
                        'enrichment_stats': silver_validation.get('enrichment_stats', {})
                    },
                    'gold': {
                        'success': gold_validation.get('success', False),
                        'total_aggregations': gold_validation.get('summary', {}).get('total_aggregations', 0),
                        'passed': gold_validation.get('summary', {}).get('passed_aggregations', 0),
                        'failed': gold_validation.get('summary', {}).get('failed_aggregations', 0)
                    }
                }
            })
            
            print("💾 Pipeline metadata saved successfully")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not save metadata: {e}")
            # Don't fail the pipeline if metadata save fails
        
        return report

    # Define task dependencies using TaskFlow API
    bronze_result = ingest_bronze_data()
    bronze_quality = validate_bronze_quality(bronze_result)
    
    silver_result = transform_silver_data(bronze_result)
    silver_quality = validate_silver_quality(silver_result, bronze_result)
    
    gold_result = create_gold_aggregations(silver_result)
    gold_quality = validate_gold_quality(gold_result, silver_result)
    
    validation_result = validate_pipeline_execution(
        bronze_result,
        silver_result,
        gold_result,
        bronze_quality,
        silver_quality,
        gold_quality
    )
    
    # Task flow with quality checks:
    # Bronze → Bronze Quality → Silver → Silver Quality → Gold → Gold Quality → Final Validation
    bronze_result >> bronze_quality >> silver_result >> silver_quality >> gold_result >> gold_quality >> validation_result

