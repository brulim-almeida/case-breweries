# Silver Layer - Curated Data Transformation

## Overview

The **Silver Layer** transforms raw data from the Bronze layer into **curated, validated, and structured** data ready for analytics. It applies schema enforcement, data cleaning, normalization, quality validation, and stores data in columnar format (Parquet or Delta Lake) for optimal query performance.

## Architecture

```
Bronze Layer (JSON)  →  Silver Layer (Parquet/Delta)
    Raw Data         →    Curated Data
```

### Key Responsibilities

1. **Data Reading**: Load raw JSON data from Bronze layer
2. **Schema Enforcement**: Apply strict schema with proper data types
3. **Data Cleaning**: Remove duplicates, trim whitespace, handle nulls
4. **Normalization**: Standardize values (countries, brewery types)
5. **Enrichment**: Add derived columns and metadata
6. **Quality Validation**: Calculate and log data quality metrics
7. **Partitioning**: Organize data by location (country, state)
8. **Format Conversion**: Transform JSON → Parquet/Delta

## Implementation

### Core Module: `src/layers/silver_layer.py`

```python
from src.layers.silver_layer import SilverLayer

# Transform Bronze to Silver
with SilverLayer() as silver:
    metadata = silver.transform_breweries()
    print(f"Transformed {metadata['quality_metrics']['total_records']:,} records")
```

### Data Transformations

#### 1. Schema Enforcement

```python
BREWERY_SCHEMA = StructType([
    StructField("id", StringType(), False),              # Required
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("longitude", DoubleType(), True),        # Numeric
    StructField("latitude", DoubleType(), True),
    # ... additional fields
])
```

#### 2. Data Cleaning

- **Whitespace Trimming**: Remove leading/trailing spaces
- **Empty String Handling**: Convert "" → NULL
- **Duplicate Removal**: Ensure unique brewery IDs

#### 3. Normalization

| Original | Normalized |
|----------|-----------|
| USA, US, United States | United States |
| England | United Kingdom |
| micro | micro |
| Brew Pub | brewpub |

#### 4. Derived Columns

- `country_normalized`: Standardized country names
- `brewery_type_normalized`: Cleaned brewery type
- `full_address`: Concatenated address string
- `location_key`: Partition key (country_state)
- `has_coordinates`: Boolean flag
- `has_contact`: Boolean flag
- `is_complete`: Completeness indicator
- `silver_processed_at`: Processing timestamp
- `processing_date`: Processing date (for partitioning)

### Data Quality Metrics

The transformation tracks comprehensive quality metrics:

```json
{
  "total_records": 9038,
  "unique_ids": 9038,
  "completeness_rate": 85.4,
  "coordinate_coverage": 92.1,
  "contact_coverage": 78.3,
  "by_type": {
    "micro": 4850,
    "brewpub": 2568,
    "planning": 672
  },
  "by_country": {
    "United States": 8080,
    "France": 450,
    "South Korea": 203
  }
}
```

## Usage

### 1. Basic Transformation

```python
from src.layers.silver_layer import SilverLayer

silver = SilverLayer()
metadata = silver.transform_breweries()

if metadata["status"] == "success":
    print(f"✓ Transformed {metadata['quality_metrics']['total_records']:,} records")
    print(f"  Output: {metadata['output_path']}")
    print(f"  Time: {metadata['transformation_time_seconds']:.2f}s")
```

### 2. With Custom Partitioning

```python
# Partition by country and state
metadata = silver.transform_breweries(
    partition_by=["country_normalized", "state"],
    format_type="parquet"
)
```

### 3. With Delta Lake Format

```python
# Use Delta format for ACID transactions and time travel
metadata = silver.transform_breweries(
    partition_by=["country_normalized", "state"],
    format_type="delta"
)
```

### 4. Reading Silver Data

```python
# Read all data
df = silver.read_silver_data("breweries")

# Read with filters
df = silver.read_silver_data(
    "breweries",
    filters={
        "country_normalized": "United States",
        "state": "California"
    }
)

# Show sample
df.select("id", "name", "city", "state").show(10)
```

### 5. Get Statistics

```python
stats = silver.get_statistics("breweries")

print(f"Total records: {stats['total_records']:,}")
print(f"\nTop 5 States:")
for state, count in list(stats['by_state'].items())[:5]:
    print(f"  {state}: {count:,}")
```

## File Structure

```
lakehouse/
└── silver/
    └── breweries/
        ├── country_normalized=France/
        │   └── state=unknown/
        │       └── *.parquet
        ├── country_normalized=United States/
        │   ├── state=California/
        │   │   └── *.parquet
        │   ├── state=Colorado/
        │   │   └── *.parquet
        │   └── state=Texas/
        │       └── *.parquet
        └── country_normalized=United Kingdom/
            └── state=unknown/
                └── *.parquet
```

## Schema

### Input Schema (Bronze)

```
root
 |-- id: string (nullable = false)
 |-- name: string (nullable = true)
 |-- brewery_type: string (nullable = true)
 |-- address_1: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- country: string (nullable = true)
 |-- longitude: double (nullable = true)
 |-- latitude: double (nullable = true)
 |-- phone: string (nullable = true)
 |-- website_url: string (nullable = true)
```

### Output Schema (Silver)

```
root
 |-- id: string (nullable = false)
 |-- name: string (nullable = true)
 |-- brewery_type: string (nullable = true)
 |-- brewery_type_normalized: string (nullable = true)
 |-- address_1: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- postal_code: string (nullable = true)
 |-- country: string (nullable = true)
 |-- country_normalized: string (nullable = true)
 |-- full_address: string (nullable = true)
 |-- location_key: string (nullable = true)
 |-- longitude: double (nullable = true)
 |-- latitude: double (nullable = true)
 |-- phone: string (nullable = true)
 |-- website_url: string (nullable = true)
 |-- has_coordinates: boolean (nullable = true)
 |-- has_contact: boolean (nullable = true)
 |-- is_complete: boolean (nullable = true)
 |-- silver_processed_at: string (nullable = true)
 |-- processing_date: date (nullable = true)
 |-- processing_year: integer (nullable = true)
 |-- processing_month: integer (nullable = true)
```

## Interactive Example

Run the interactive example script:

```bash
python3 example_silver_transformation.py
```

### Menu Options:

1. **Transform Bronze to Silver**: Full transformation pipeline
2. **Read and query Silver data**: Interactive querying with filters
3. **Get Silver layer statistics**: Comprehensive statistics and breakdowns
4. **Transform with Delta format**: Use Delta Lake features
5. **Exit**

## Testing

### Run Tests

```bash
# Run all tests
pytest tests/test_silver_layer.py -v

# Run specific test class
pytest tests/test_silver_layer.py::TestDataCleaning -v

# Run with coverage
pytest tests/test_silver_layer.py --cov=src.layers.silver_layer --cov-report=html

# Run integration tests only
pytest tests/test_silver_layer.py -m integration
```

### Test Coverage

- **18 tests total** covering:
  - Initialization and configuration
  - Bronze data reading
  - Data cleaning and normalization
  - Schema validation
  - Metadata addition
  - Quality validation
  - Parquet/Delta writing
  - Partitioning
  - Complete transformation pipeline
  - Data querying and statistics
  - Context manager functionality

## Performance Considerations

### Optimization Strategies

1. **Partitioning**: Data organized by `country_normalized` and `state` for partition pruning
2. **Columnar Format**: Parquet format enables column pruning and compression
3. **Schema Enforcement**: Early schema application reduces memory overhead
4. **Predicate Pushdown**: Filters applied at file scan level
5. **Broadcast Joins**: Small lookups use broadcast for efficiency

### Expected Performance

- **9,038 records**: ~2-3 seconds
- **100,000 records**: ~10-15 seconds
- **1,000,000 records**: ~60-90 seconds

Performance varies based on:
- Available memory
- Number of Spark partitions
- Disk I/O speed
- Complexity of transformations

## Data Quality Rules

### Completeness Rules

A record is considered **complete** if it has:
- ✓ Coordinates (latitude & longitude)
- ✓ Contact information (phone or website)
- ✓ City
- ✓ State

### Normalization Rules

1. **Countries**: Standardize to full names (USA → United States)
2. **States**: Preserve original state names
3. **Brewery Types**: Lowercase, underscore-separated
4. **Phone Numbers**: Preserve original format
5. **URLs**: Preserve original format

## Next Steps

After Silver Layer transformation:

1. ✅ **Bronze Layer**: Raw data ingestion complete
2. ✅ **Silver Layer**: Curated transformation complete
3. ⏳ **Gold Layer**: Build aggregations and analytics tables
4. ⏳ **Airflow DAGs**: Orchestrate the pipeline
5. ⏳ **Data Quality**: Implement Great Expectations checks
6. ⏳ **Monitoring**: Add metrics and alerting

## Configuration

### Environment Variables

```bash
# Paths
export SILVER_PATH="./lakehouse/silver"
export BRONZE_PATH="./lakehouse/bronze"

# Spark
export SPARK_APP_NAME="Breweries-Silver"
export SPARK_SHUFFLE_PARTITIONS="8"
export SPARK_LOG_LEVEL="ERROR"
```

### Settings in Code

```python
from src.config.settings import Settings

# Display current settings
Settings.display_settings()

# Get Silver path for a dataset
path = Settings.get_silver_path("breweries")
```

## Troubleshooting

### Common Issues

**Issue**: `FileNotFoundError: Bronze data not found`
- **Solution**: Run Bronze ingestion first: `python3 example_bronze_ingestion.py`

**Issue**: `PermissionError: Cannot write to Silver path`
- **Solution**: Check directory permissions or update `SILVER_PATH` environment variable

**Issue**: `OutOfMemoryError` during transformation
- **Solution**: Reduce `spark.sql.shuffle.partitions` or increase executor memory

**Issue**: Schema mismatch errors
- **Solution**: Verify Bronze data schema matches expected format

## API Reference

### SilverLayer Class

```python
class SilverLayer:
    def __init__(spark=None, bronze_path=None, silver_path=None)
    def transform_breweries(partition_date=None, partition_by=None, format_type="parquet")
    def read_silver_data(dataset_name="breweries", filters=None)
    def get_statistics(dataset_name="breweries")
    def close()
```

### Methods

- `transform_breweries()`: Complete transformation pipeline
- `read_silver_data()`: Read and optionally filter Silver data
- `get_statistics()`: Get comprehensive statistics
- `close()`: Stop Spark session

## Related Documentation

- [Bronze Layer README](./README_BRONZE.md)
- [Gold Layer README](./README_GOLD.md) _(coming soon)_
- [Project README](../../README.md)
- [Case Requirements](../../case.md)

---

**Silver Layer Implementation Complete** ✅  
18/18 tests passing | Full transformation pipeline | Comprehensive data quality validation
