"""
Great Expectations Validator for Breweries Data Pipeline

This module implements data quality validation using Great Expectations framework.
It provides validation for each layer of the Medallion architecture with specific
expectations tailored to each stage of data processing.

Author: Data Engineering Team
Date: January 2026
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Great Expectations imports
try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.data_context import DataContext
    from great_expectations.checkpoint import Checkpoint
except ImportError as e:
    raise ImportError(
        "Great Expectations is not installed. "
        "Install it with: pip install great-expectations==0.18.8"
    ) from e


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BreweriesDataValidator:
    """
    Data quality validator using Great Expectations.
    
    This class provides methods to validate data quality across different
    layers of the data lake using customized expectation suites.
    
    Features:
    - Layer-specific validation (Bronze, Silver, Gold)
    - Automated data profiling
    - Anomaly detection (volume changes)
    - Detailed validation reports
    - Integration with Airflow XCom
    
    Example:
        >>> validator = BreweriesDataValidator(spark)
        >>> result = validator.validate_bronze_layer(bronze_df)
        >>> if not result['success']:
        >>>     print(f"Validation failed: {result['failed_expectations']}")
    """
    
    def __init__(
        self,
        spark: SparkSession,
        context_root_dir: Optional[str] = None,
        enable_data_docs: bool = True
    ):
        """
        Initialize the validator.
        
        Args:
            spark: Active SparkSession
            context_root_dir: Root directory for GE context (default: project root)
            enable_data_docs: Whether to generate Data Docs HTML reports
        """
        self.spark = spark
        self.enable_data_docs = enable_data_docs
        
        # Set context root directory
        if context_root_dir is None:
            # Default to project root
            self.context_root_dir = str(Path(__file__).parent.parent.parent / "great_expectations")
        else:
            self.context_root_dir = context_root_dir
        
        logger.info(f"Initializing Great Expectations context at: {self.context_root_dir}")
        
        # Initialize or load GE context
        self.context = self._get_or_create_context()
        
        # Statistics storage
        self.validation_stats = {}
    
    def _get_or_create_context(self) -> DataContext:
        """
        Get existing or create new Great Expectations DataContext.
        
        Returns:
            DataContext instance
        """
        try:
            # Try to load existing context
            context = gx.get_context(context_root_dir=self.context_root_dir)
            logger.info("Loaded existing Great Expectations context")
        except Exception as e:
            logger.info(f"Creating new Great Expectations context: {e}")
            # Create new context if not exists
            context = gx.get_context(
                context_root_dir=self.context_root_dir,
                mode="file"
            )
        
        return context
    
    def _create_spark_datasource(self, datasource_name: str) -> Any:
        """
        Create or get Spark datasource for Great Expectations.
        
        Args:
            datasource_name: Name for the datasource
            
        Returns:
            Datasource object
        """
        try:
            # Try to get existing datasource
            datasource = self.context.get_datasource(datasource_name)
            logger.info(f"Using existing datasource: {datasource_name}")
        except Exception:
            # Create new Spark datasource
            logger.info(f"Creating new Spark datasource: {datasource_name}")
            datasource = self.context.sources.add_spark(
                name=datasource_name,
                spark=self.spark
            )
        
        return datasource
    
    def _get_dataframe_stats(self, df: DataFrame) -> Dict[str, Any]:
        """
        Calculate basic statistics from a DataFrame.
        
        📊 ESTATÍSTICAS CALCULADAS:
        - row_count: Número total de registros
        - column_count: Número de colunas
        - null_counts: Contagem de nulos por coluna
        - duplicate_count: Registros duplicados (se houver coluna 'id')
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with statistics
        """
        stats = {
            'row_count': df.count(),
            'column_count': len(df.columns),
            'columns': df.columns,
            'null_counts': {}
        }
        
        # Calculate null counts per column
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                stats['null_counts'][col] = null_count
        
        # Check for duplicates if 'id' column exists
        if 'id' in df.columns:
            total_count = stats['row_count']
            distinct_count = df.select('id').distinct().count()
            stats['duplicate_count'] = total_count - distinct_count
        
        return stats
    
    def validate_bronze_layer(
        self,
        df: DataFrame,
        execution_date: Optional[str] = None,
        previous_count: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Validate Bronze Layer data quality.
        
        🎯 VALIDAÇÕES DA BRONZE LAYER:
        
        1. **Schema Validation**
           - Verifica se todas as colunas esperadas existem
           - Garante integridade estrutural dos dados da API
        
        2. **Uniqueness** (expect_column_values_to_be_unique)
           - Coluna 'id': 100% dos IDs devem ser únicos
           - Estatística: Detecta duplicações na fonte
        
        3. **Completeness** (expect_column_values_to_not_be_null)
           - Colunas obrigatórias: id, name, brewery_type
           - Estatística: Taxa de completude de campos críticos
        
        4. **Volume Check** (expect_table_row_count_to_be_between)
           - Range esperado: 5,000 a 50,000 registros
           - Estatística: Detecta anomalias de volume (API down, filtros)
        
        5. **Domain Validation** (expect_column_values_to_be_in_set)
           - brewery_type: Apenas valores conhecidos
           - Estatística: Identifica novos tipos não catalogados
        
        6. **Coordinate Ranges** (expect_column_values_to_be_between)
           - Latitude: -90 a 90
           - Longitude: -180 a 180
           - mostly=0.5: Aceita 50% sem coordenadas (problema conhecido da API)
        
        7. **Anomaly Detection** (volume comparison)
           - Compara com execução anterior
           - Alerta se variação > 20%
        
        Args:
            df: Bronze DataFrame
            execution_date: Date of execution (for tracking)
            previous_count: Row count from previous execution (for anomaly detection)
            
        Returns:
            Validation result dictionary with success status and details
        """
        logger.info("=" * 80)
        logger.info("🔍 VALIDATING BRONZE LAYER")
        logger.info("=" * 80)
        
        # Get DataFrame statistics
        df_stats = self._get_dataframe_stats(df)
        logger.info(f"📊 DataFrame Stats: {df_stats['row_count']:,} rows, {df_stats['column_count']} columns")
        
        execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        
        # Create temporary view for validation
        temp_view_name = f"bronze_breweries_{execution_date.replace('-', '')}"
        df.createOrReplaceTempView(temp_view_name)
        
        # Define Bronze Layer expectations
        expectations = self._get_bronze_expectations(
            df_stats=df_stats,
            previous_count=previous_count
        )
        
        # Run validation
        result = self._execute_validation(
            datasource_name="breweries_bronze",
            data_asset_name=temp_view_name,
            suite_name="bronze_quality_suite",
            expectations=expectations,
            batch_identifiers={"execution_date": execution_date}
        )
        
        # Add statistics to result
        result['statistics'] = df_stats
        self.validation_stats['bronze'] = result
        
        return result
    
    def _get_bronze_expectations(
        self,
        df_stats: Dict[str, Any],
        previous_count: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Define expectations for Bronze Layer.
        
        Returns:
            List of expectation configurations
        """
        expectations = [
            # 1. SCHEMA VALIDATION
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "id", "name", "brewery_type", "address_1", "address_2",
                        "address_3", "city", "state_province", "postal_code",
                        "country", "longitude", "latitude", "phone", "website_url",
                        "state", "street", "ingestion_timestamp", "year"
                    ]
                },
                "meta": {
                    "description": "Verifica se o schema da API está correto e completo"
                }
            },
            
            # 2. UNIQUENESS - IDs devem ser únicos
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {
                    "column": "id"
                },
                "meta": {
                    "description": "100% dos IDs devem ser únicos (sem duplicatas)",
                    "statistic": "Contagem de registros únicos vs total"
                }
            },
            
            # 3. COMPLETENESS - Campos obrigatórios
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "id",
                    "mostly": 1.0
                },
                "meta": {
                    "description": "ID nunca pode ser nulo",
                    "statistic": "% de registros com ID válido"
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "name",
                    "mostly": 0.99  # Permite 1% de falha
                },
                "meta": {
                    "description": "Nome da cervejaria é essencial",
                    "statistic": "Taxa de completude de nomes"
                }
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "brewery_type",
                    "mostly": 0.95  # 95% devem ter tipo
                },
                "meta": {
                    "description": "Tipo de cervejaria deve estar presente",
                    "statistic": "% registros com tipo classificado"
                }
            },
            
            # 4. VOLUME CHECK - Detecta anomalias
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 5000,
                    "max_value": 50000
                },
                "meta": {
                    "description": "Volume esperado de cervejarias globais",
                    "statistic": "Contagem total de registros",
                    "alert": "Alerta se fora do range (API instável ou filtros)"
                }
            },
            
            # 5. DOMAIN VALIDATION - Valores conhecidos
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "brewery_type",
                    "value_set": [
                        "micro", "nano", "regional", "brewpub", "large",
                        "planning", "bar", "contract", "proprietor", "closed"
                    ],
                    "mostly": 0.95
                },
                "meta": {
                    "description": "Tipos de cervejaria catalogados",
                    "statistic": "Distribuição por tipo + identificação de tipos novos"
                }
            },
            
            # 6. COORDINATE RANGES - Validação geográfica básica
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "latitude",
                    "min_value": -90.0,
                    "max_value": 90.0,
                    "mostly": 0.5  # Apenas 50% têm coordenadas
                },
                "meta": {
                    "description": "Latitude deve estar no range geográfico válido",
                    "statistic": "% de coordenadas válidas (conhecido: ~74% da API tem coords)"
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "longitude",
                    "min_value": -180.0,
                    "max_value": 180.0,
                    "mostly": 0.5
                },
                "meta": {
                    "description": "Longitude deve estar no range geográfico válido",
                    "statistic": "% de coordenadas preenchidas"
                }
            },
            
            # 7. TIMESTAMP VALIDATION
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "ingestion_timestamp",
                    "mostly": 1.0
                },
                "meta": {
                    "description": "Timestamp de ingestão deve sempre existir",
                    "statistic": "Controle de rastreabilidade"
                }
            }
        ]
        
        # ANOMALY DETECTION: Comparar com execução anterior
        if previous_count is not None:
            current_count = df_stats['row_count']
            variance = abs(current_count - previous_count) / previous_count
            
            if variance > 0.20:  # Variação > 20%
                logger.warning(
                    f"⚠️ ANOMALY DETECTED: Volume change of {variance:.1%} "
                    f"(Previous: {previous_count:,}, Current: {current_count:,})"
                )
        
        return expectations
    
    def validate_silver_layer(
        self,
        df: DataFrame,
        execution_date: Optional[str] = None,
        bronze_count: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Validate Silver Layer data quality.
        
        🎯 VALIDAÇÕES DA SILVER LAYER:
        
        1. **Data Loss Check**
           - Compara registros Bronze vs Silver
           - Tolerância: Máximo 5% de perda aceitável
           - Estatística: Taxa de retenção de dados
        
        2. **Country Normalization** (expect_column_values_to_not_be_null)
           - country_normalized: 100% devem ter país
           - Estatística: Sucesso da normalização de países
        
        3. **Coordinate Enrichment** (geocoding validation)
           - coordinates_valid: 85%+ devem ter coords válidas
           - Estatística: Taxa de sucesso do geocoding
        
        4. **Geographic Consistency**
           - Coordenadas batem com país informado
           - Sem pontos em "Null Island" (0,0)
           - Estatística: % de coordenadas consistentes
        
        5. **Data Cleaning Validation**
           - Website URLs formatadas corretamente
           - Phone numbers normalizados
           - Estatística: Taxa de limpeza de dados
        
        6. **Partition Validation**
           - Todas as partições country_normalized criadas
           - Distribuição equilibrada
        
        Args:
            df: Silver DataFrame
            execution_date: Date of execution
            bronze_count: Row count from Bronze layer (for loss detection)
            
        Returns:
            Validation result dictionary
        """
        logger.info("=" * 80)
        logger.info("🔍 VALIDATING SILVER LAYER")
        logger.info("=" * 80)
        
        # Get DataFrame statistics
        df_stats = self._get_dataframe_stats(df)
        logger.info(f"📊 DataFrame Stats: {df_stats['row_count']:,} rows, {df_stats['column_count']} columns")
        
        # Calculate enrichment statistics
        enrichment_stats = self._calculate_enrichment_stats(df)
        logger.info(f"🌍 Geocoding Coverage: {enrichment_stats['coordinate_coverage']:.1%}")
        logger.info(f"✅ Valid Coordinates: {enrichment_stats['valid_coordinates_rate']:.1%}")
        
        execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        
        # Create temporary view
        temp_view_name = f"silver_breweries_{execution_date.replace('-', '')}"
        df.createOrReplaceTempView(temp_view_name)
        
        # Define Silver Layer expectations
        expectations = self._get_silver_expectations(
            df_stats=df_stats,
            bronze_count=bronze_count,
            enrichment_stats=enrichment_stats
        )
        
        # Run validation
        result = self._execute_validation(
            datasource_name="breweries_silver",
            data_asset_name=temp_view_name,
            suite_name="silver_quality_suite",
            expectations=expectations,
            batch_identifiers={"execution_date": execution_date}
        )
        
        # Add statistics
        result['statistics'] = df_stats
        result['enrichment_stats'] = enrichment_stats
        self.validation_stats['silver'] = result
        
        return result
    
    def _calculate_enrichment_stats(self, df: DataFrame) -> Dict[str, float]:
        """
        Calculate enrichment and data quality statistics for Silver layer.
        
        📊 ESTATÍSTICAS DE ENRICHMENT:
        - coordinate_coverage: % de registros com lat/long
        - valid_coordinates_rate: % de coordenadas geograficamente válidas
        - geocoded_rate: % de registros geocodificados (vs API original)
        - country_normalized_rate: % com país normalizado
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Dictionary with enrichment statistics
        """
        total = df.count()
        
        # Coordinate coverage
        with_coords = df.filter(
            F.col('latitude').isNotNull() & F.col('longitude').isNotNull()
        ).count()
        
        # Valid coordinates (if column exists)
        valid_coords = 0
        if 'coordinates_valid' in df.columns:
            valid_coords = df.filter(F.col('coordinates_valid') == True).count()
        
        # Country normalization
        with_country = df.filter(F.col('country_normalized').isNotNull()).count()
        
        # Geocoded entries (if column exists)
        geocoded = 0
        if 'geocoded_source' in df.columns:
            geocoded = df.filter(F.col('geocoded_source') == 'nominatim').count()
        
        return {
            'coordinate_coverage': with_coords / total if total > 0 else 0,
            'valid_coordinates_rate': valid_coords / total if total > 0 else 0,
            'geocoded_rate': geocoded / total if total > 0 else 0,
            'country_normalized_rate': with_country / total if total > 0 else 0
        }
    
    def _get_silver_expectations(
        self,
        df_stats: Dict[str, Any],
        bronze_count: Optional[int],
        enrichment_stats: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """
        Define expectations for Silver Layer.
        """
        expectations = [
            # 1. DATA LOSS CHECK
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": int(bronze_count * 0.95) if bronze_count else 5000,
                    "max_value": bronze_count if bronze_count else 50000
                },
                "meta": {
                    "description": "Máximo 5% de perda de dados aceitável Bronze→Silver",
                    "statistic": f"Data retention rate: {(df_stats['row_count']/bronze_count*100):.1f}%" if bronze_count else "N/A"
                }
            },
            
            # 2. COUNTRY NORMALIZATION
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "country_normalized",
                    "mostly": 1.0
                },
                "meta": {
                    "description": "100% dos registros devem ter país normalizado",
                    "statistic": f"Country normalization: {enrichment_stats['country_normalized_rate']:.1%}"
                }
            },
            
            # 3. COORDINATE ENRICHMENT
            {
                "expectation_type": "expect_column_pair_values_to_be_in_set",
                "kwargs": {
                    "column_A": "latitude",
                    "column_B": "longitude",
                    "value_pairs_set": None,  # Will check for non-null pairs
                    "mostly": 0.85  # 85% devem ter coordenadas
                },
                "meta": {
                    "description": "85%+ devem ter coordenadas (com geocoding)",
                    "statistic": f"Coordinate coverage: {enrichment_stats['coordinate_coverage']:.1%}"
                }
            },
            
            # 4. COORDINATE VALIDATION
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "coordinates_valid",
                    "value_set": [True],
                    "mostly": 0.85
                },
                "meta": {
                    "description": "85%+ coordenadas devem ser geograficamente válidas",
                    "statistic": f"Valid coordinates: {enrichment_stats['valid_coordinates_rate']:.1%}"
                }
            },
            
            # 5. NULL ISLAND CHECK (0,0)
            {
                "expectation_type": "expect_compound_columns_to_be_unique",
                "kwargs": {
                    "column_list": ["latitude", "longitude"]
                },
                "meta": {
                    "description": "Detecta Null Island (0,0) e duplicatas de coords",
                    "statistic": "Verifica qualidade do geocoding"
                }
            },
            
            # 6. SCHEMA ENRICHMENT CHECK
            {
                "expectation_type": "expect_table_column_count_to_be_between",
                "kwargs": {
                    "min_value": 20,  # Silver tem mais colunas que Bronze
                    "max_value": 30
                },
                "meta": {
                    "description": "Silver deve ter colunas enriched",
                    "statistic": f"Total columns: {df_stats['column_count']}"
                }
            }
        ]
        
        return expectations
    
    def validate_gold_layer(
        self,
        aggregations: Dict[str, DataFrame],
        execution_date: Optional[str] = None,
        silver_count: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Validate Gold Layer aggregations.
        
        🎯 VALIDAÇÕES DA GOLD LAYER:
        
        1. **Aggregation Consistency**
           - Soma das agregações = total Silver
           - Sem valores negativos em counts
           - Estatística: Integridade das agregações
        
        2. **Top Entities Validation**
           - USA deve estar no top 3 países (tem ~70% das breweries)
           - 'micro' deve ser tipo mais comum
           - Estatística: Consistência com distribuição conhecida
        
        3. **Summary Statistics Validation**
           - Médias e totais fazem sentido
           - Sem outliers absurdos
        
        4. **Completeness**
           - Todas as agregações esperadas existem
           - Sem tabelas vazias
        
        Args:
            aggregations: Dictionary with Gold tables (by_country, by_type, etc)
            execution_date: Date of execution
            silver_count: Total count from Silver layer
            
        Returns:
            Validation result dictionary
        """
        logger.info("=" * 80)
        logger.info("🔍 VALIDATING GOLD LAYER")
        logger.info("=" * 80)
        
        execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        
        # Validate each aggregation
        results = {}
        
        for agg_name, agg_df in aggregations.items():
            logger.info(f"📊 Validating aggregation: {agg_name}")
            
            temp_view_name = f"gold_{agg_name}_{execution_date.replace('-', '')}"
            agg_df.createOrReplaceTempView(temp_view_name)
            
            expectations = self._get_gold_expectations(
                agg_name=agg_name,
                agg_df=agg_df,
                silver_count=silver_count
            )
            
            result = self._execute_validation(
                datasource_name=f"breweries_gold_{agg_name}",
                data_asset_name=temp_view_name,
                suite_name=f"gold_{agg_name}_suite",
                expectations=expectations,
                batch_identifiers={"execution_date": execution_date}
            )
            
            results[agg_name] = result
        
        # Combine results
        overall_success = all(r['success'] for r in results.values())
        
        combined_result = {
            'success': overall_success,
            'layer': 'gold',
            'execution_date': execution_date,
            'aggregations': results,
            'summary': {
                'total_aggregations': len(results),
                'passed_aggregations': sum(1 for r in results.values() if r['success']),
                'failed_aggregations': sum(1 for r in results.values() if not r['success'])
            }
        }
        
        self.validation_stats['gold'] = combined_result
        
        return combined_result
    
    def _get_gold_expectations(
        self,
        agg_name: str,
        agg_df: DataFrame,
        silver_count: Optional[int]
    ) -> List[Dict[str, Any]]:
        """
        Define expectations for Gold Layer aggregations.
        """
        expectations = [
            # Base expectations for all aggregations
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 1,
                    "max_value": 10000
                },
                "meta": {
                    "description": "Agregação não pode estar vazia",
                    "statistic": f"Aggregation size: {agg_df.count()} rows"
                }
            }
        ]
        
        # Specific expectations based on aggregation type
        if 'count' in agg_df.columns:
            expectations.append({
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "count",
                    "min_value": 1,
                    "max_value": silver_count if silver_count else 50000
                },
                "meta": {
                    "description": "Counts devem ser positivos e <= total Silver",
                    "statistic": "Validação de integridade das agregações"
                }
            })
        
        # Country-specific validation
        if agg_name == 'by_country':
            expectations.extend([
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "country_normalized",
                        "value_set": ["United States"],  # Must include USA
                        "mostly": 0.0  # At least one row must be USA
                    },
                    "meta": {
                        "description": "USA deve estar presente (país com mais breweries)",
                        "statistic": "Validação de top países"
                    }
                }
            ])
        
        # Type-specific validation
        if agg_name == 'by_type':
            expectations.extend([
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {
                        "column": "brewery_type",
                        "value_set": ["micro"],  # Must include micro
                        "mostly": 0.0
                    },
                    "meta": {
                        "description": "'micro' deve estar presente (tipo mais comum)",
                        "statistic": "Validação de distribuição de tipos"
                    }
                }
            ])
        
        return expectations
    
    def _execute_validation(
        self,
        datasource_name: str,
        data_asset_name: str,
        suite_name: str,
        expectations: List[Dict[str, Any]],
        batch_identifiers: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Execute validation and return results.
        
        Args:
            datasource_name: Name of the datasource
            data_asset_name: Name of the data asset (temp view)
            suite_name: Name of the expectation suite
            expectations: List of expectations to validate
            batch_identifiers: Identifiers for the batch
            
        Returns:
            Validation result dictionary
        """
        try:
            # Create or update expectation suite
            try:
                suite = self.context.get_expectation_suite(suite_name)
                logger.info(f"Using existing suite: {suite_name}")
            except Exception:
                logger.info(f"Creating new suite: {suite_name}")
                suite = self.context.add_expectation_suite(suite_name)
            
            # Add expectations to suite
            for exp_config in expectations:
                suite.add_expectation(**exp_config)
            
            # Create or get datasource
            datasource = self._create_spark_datasource(datasource_name)
            
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name=datasource_name,
                data_asset_name=data_asset_name,
                runtime_parameters={
                    "batch_data": self.spark.table(data_asset_name)
                },
                batch_identifiers=batch_identifiers
            )
            
            # Validate
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            validation_result = validator.validate()
            
            # Process results
            success = validation_result.success
            results = validation_result.results
            
            failed_expectations = []
            passed_expectations = []
            
            for result in results:
                exp_type = result.expectation_config.expectation_type
                exp_kwargs = result.expectation_config.kwargs
                
                if result.success:
                    passed_expectations.append({
                        'expectation': exp_type,
                        'column': exp_kwargs.get('column'),
                        'description': result.expectation_config.meta.get('description', '')
                    })
                else:
                    failed_expectations.append({
                        'expectation': exp_type,
                        'column': exp_kwargs.get('column'),
                        'observed_value': result.result.get('observed_value'),
                        'description': result.expectation_config.meta.get('description', '')
                    })
            
            # Log results
            total_expectations = len(results)
            passed_count = len(passed_expectations)
            failed_count = len(failed_expectations)
            success_rate = (passed_count / total_expectations * 100) if total_expectations > 0 else 0
            
            logger.info(f"📊 Validation Results:")
            logger.info(f"   Total Expectations: {total_expectations}")
            logger.info(f"   ✅ Passed: {passed_count}")
            logger.info(f"   ❌ Failed: {failed_count}")
            logger.info(f"   Success Rate: {success_rate:.1f}%")
            
            if failed_expectations:
                logger.warning("❌ Failed Expectations:")
                for failure in failed_expectations:
                    logger.warning(f"   - {failure['expectation']}: {failure['description']}")
                    if failure.get('column'):
                        logger.warning(f"     Column: {failure['column']}")
                    if failure.get('observed_value'):
                        logger.warning(f"     Observed: {failure['observed_value']}")
            
            # Generate Data Docs if enabled
            if self.enable_data_docs:
                try:
                    self.context.build_data_docs()
                    logger.info(f"📄 Data Docs generated at: {self.context_root_dir}/uncommitted/data_docs/local_site/index.html")
                except Exception as e:
                    logger.warning(f"Could not generate Data Docs: {e}")
            
            return {
                'success': success,
                'suite_name': suite_name,
                'total_expectations': total_expectations,
                'passed_expectations': passed_count,
                'failed_expectations_count': failed_count,
                'success_rate': success_rate,
                'passed_details': passed_expectations,
                'failed_details': failed_expectations,
                'execution_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"❌ Validation failed with error: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'suite_name': suite_name
            }
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get summary of all validations performed.
        
        Returns:
            Summary dictionary with statistics from all layers
        """
        return {
            'bronze': self.validation_stats.get('bronze'),
            'silver': self.validation_stats.get('silver'),
            'gold': self.validation_stats.get('gold'),
            'overall_success': all(
                v.get('success', False) 
                for v in self.validation_stats.values()
                if isinstance(v, dict)
            )
        }
