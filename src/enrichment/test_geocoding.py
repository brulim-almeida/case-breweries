"""
Script de teste para Geocoding Enrichment

Este script demonstra como usar o GeocodeEnricher para
completar coordenadas faltantes de cervejarias.

Execução:
    docker exec breweries_data_lake-airflow-worker-1 python /opt/airflow/src/enrichment/test_geocoding.py
"""

import sys
sys.path.insert(0, '/opt/airflow')

from utils.delta_spark import initialize_spark, stop_spark
from src.enrichment.geocoding import GeocodeEnricher
from pyspark.sql import functions as F

def main():
    print("=" * 80)
    print("🧪 TESTE DE GEOCODING ENRICHMENT")
    print("=" * 80)
    
    # 1. Initialize Spark
    print("\n📦 Inicializando Spark...")
    spark = initialize_spark()
    
    # 2. Load Silver data
    print("\n📂 Carregando dados da Silver layer...")
    silver_path = "/opt/airflow/lakehouse/silver/breweries"
    df = spark.read.format("delta").load(silver_path)
    
    total_count = df.count()
    print(f"Total de cervejarias: {total_count:,}")
    
    # 3. Analyze missing coordinates
    print("\n🔍 Analisando coordenadas faltantes...")
    missing_coords = df.filter(F.col("latitude").isNull() | F.col("longitude").isNull())
    missing_count = missing_coords.count()
    missing_pct = (missing_count / total_count * 100)
    
    print(f"Cervejarias sem coordenadas: {missing_count:,} ({missing_pct:.2f}%)")
    print(f"Cervejarias com coordenadas: {total_count - missing_count:,} ({100-missing_pct:.2f}%)")
    
    # 4. Show sample of missing coordinates
    print("\n📋 Exemplos de cervejarias sem coordenadas:")
    sample = missing_coords.select("name", "city", "state", "country_normalized").limit(5)
    sample.show(truncate=False)
    
    # 5. Test geocoding on a small sample
    print("\n🧪 Testando geocoding em 5 exemplos...")
    enricher = GeocodeEnricher(spark, rate_limit_delay=1.1)
    
    # Take only 5 for quick test
    test_df = missing_coords.limit(5)
    
    print("\nAntes do enrichment:")
    test_df.select("name", "city", "state", "latitude", "longitude").show(truncate=False)
    
    # Enrich
    enriched_df = enricher.enrich_coordinates(test_df, max_records=5)
    
    print("\nDepois do enrichment:")
    enriched_df.select("name", "city", "state", "latitude", "longitude").show(truncate=False)
    
    # 6. Show statistics
    print("\n📊 Estatísticas do teste:")
    stats = enricher.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # 7. Cleanup
    enricher.close()
    stop_spark(spark)
    
    print("\n✅ Teste concluído!")
    print("=" * 80)

if __name__ == "__main__":
    main()
