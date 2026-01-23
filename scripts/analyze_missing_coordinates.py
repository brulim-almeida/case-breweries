"""
Script to analyze missing coordinates in Silver layer
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

sys.path.insert(0, '/opt/airflow')
from utils.delta_spark import initialize_spark

# Initialize Spark
spark = initialize_spark()

# Read Silver data
silver_path = "/opt/airflow/lakehouse/silver/breweries"
df = spark.read.format("delta").load(silver_path)

# Analyze missing coordinates
total = df.count()
missing_coords = df.filter(F.col("latitude").isNull() | F.col("longitude").isNull())
missing_count = missing_coords.count()
missing_pct = (missing_count / total * 100)

print("=" * 80)
print("📊 ANÁLISE DE COORDENADAS FALTANTES")
print("=" * 80)
print(f"Total de cervejarias: {total:,}")
print(f"Sem coordenadas: {missing_count:,} ({missing_pct:.2f}%)")
print(f"Com coordenadas: {total - missing_count:,} ({100-missing_pct:.2f}%)")
print()

# Show countries with missing coordinates
print("🌍 Países com mais coordenadas faltantes:")
missing_by_country = (missing_coords
                      .groupBy("country_normalized")
                      .count()
                      .orderBy(F.desc("count"))
                      .limit(10))
missing_by_country.show(truncate=False)

# Show sample of missing coordinates
print("\n📋 Exemplos de cervejarias sem coordenadas:")
missing_coords.select("name", "city", "state", "country_normalized", "full_address").show(5, truncate=False)

spark.stop()
