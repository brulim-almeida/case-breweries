# Case Breweries Data Lake

Pipeline de dados para extrair, processar e armazenar informações de cervejarias da API Open Brewery DB, seguindo arquitetura Medallion (Bronze → Silver → Gold) com Apache Airflow, PySpark e Delta Lake.

<img width="935" height="418" alt="Architecture" src="https://github.com/user-attachments/assets/e75faccd-fc85-47a8-84f8-f6b73fb8ccbb" />

**Stack:** Python 3.11 | PySpark 3.5 | Delta Lake 3.1 | Airflow 2.8 | Docker | Great Expectations

---

## Quick Start

### Prerequisites
- Docker 20.10+ & Docker Compose 2.0+
- 8GB RAM | 20GB disk

### Setup & Run
```bash
# Clone and configure
git clone https://github.com/brulim-almeida/case-breweries.git
cd case-breweries
cp .env.example .env

# Permissions (Linux)
sudo chmod -R 777 ./logs ./lakehouse

# Build and start
docker build -t airflow-breweries:latest .
docker compose up -d

# Optional: Start with Streamlit dashboard
docker compose --profile streamlit up -d
```

### Access
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| Streamlit | http://localhost:8501 | - |
| Flower | http://localhost:5555 | - |

### Run Pipeline
1. Access Airflow UI → DAGs → `breweries_pipeline_dag`
2. Enable DAG (toggle on) → Trigger DAG (▶️)

> **Tip:** For testing, use "Trigger DAG w/ config" with `{"clean_before_run": true}` to avoid data duplication.

---

## Architecture - Medallion Layers

### Bronze Layer
- **Input:** Raw JSON from Open Brewery DB API (~9,000 breweries)
- **Storage:** JSON files partitioned by `year/month/day`
- **Location:** `lakehouse/bronze/breweries/`

### Silver Layer
- **Transformations:**
  - Data cleaning and normalization (trim, null handling)
  - Enrichment: `full_address`, `has_coordinates`, `has_contact`, `processed_at`
  - Geocoding for missing coordinates (via Nominatim API)
  - Geographic validation (bounding boxes, null island detection)
- **Storage:** Delta Lake, partitioned by `country_normalized/state`
- **Schema Evolution:** Enabled for new columns
- **Location:** `lakehouse/silver/breweries/`

### Gold Layer
Aggregated tables for analytics:

| Table | Description |
|-------|-------------|
| `breweries` | Complete brewery dataset |
| `by_country` | Count per country |
| `by_type` | Count per brewery type |
| `by_state` | Count per state (top 20) |
| `by_type_and_country` | Type × Country matrix |
| `by_type_and_state` | Type × State matrix |
| `summary_statistics` | Quality & coverage metrics |

**Location:** `lakehouse/gold/breweries/`

---

## Orchestration

<img width="297" height="313" alt="DAG" src="https://github.com/user-attachments/assets/0d298aac-51a8-4554-b186-54f322c1f7c0" />

**Engine:** Airflow with TaskFlow API + CeleryExecutor

### Pipeline Tasks
1. **bronze_ingestion** → Extract from API, save raw JSON
2. **silver_transformation** → Clean, enrich, geocode, validate
3. **gold_aggregation** → Create analytical tables
4. **validate_pipeline** → End-to-end integrity check

### Error Handling
- **Retries:** 3 attempts with exponential backoff (5min → 10min → 20min)
- **Callbacks:** `on_failure_callback` for alerting
- **Validation:** Data loss rate calculation (Bronze vs Silver)

### Scheduling
- Configurable via Airflow UI or `schedule_interval` in DAG
- XCom metadata tracking between tasks

---

## Data Quality

**Framework:** Great Expectations integrated at all layers

### Validations per Layer
| Layer | Checks |
|-------|--------|
| Bronze | IDs unique, required fields, volume 5k-50k, valid types, coordinate ranges |
| Silver | Data loss <5%, coordinate coverage 85%+, schema enrichment |
| Gold | Non-empty aggregations, positive counts, type consistency |

### Metrics Tracked
- Success rate per expectation
- Data retention rate (Bronze→Silver: 99.4%+)
- Coordinate coverage improvement (74% → 86%+)
- Geocoding success rate (~85%)

📄 **Docs:** [documents/GREAT_EXPECTATIONS_GUIDE.md](documents/GREAT_EXPECTATIONS_GUIDE.md)

---

## Observability

- **XCom Metadata:** Records processed, quality metrics, execution times per task
- **Pipeline Metrics Dashboard:** Streamlit tab with execution history, data flow visualization
- **Airflow Logs:** Structured logs with timestamps and context
- **Flower:** Celery worker monitoring at port 5555
- **Stored Metrics:** `lakehouse/metadata/pipeline_runs.json`

---

## Extra Features

Beyond case requirements:

- **Streamlit Dashboard** - 5 tabs: Maps, Geographic, Types, Quality, Cities with Plotly visualizations
- **Geocoding Enrichment** - Nominatim API integration, +26% coordinate coverage improvement
- **Coordinate Validation** - Bounding boxes for 13 countries, null island detection, `coordinates_valid` flag
- **Incremental Processing** - Silver layer processes only new ingestion files via `ingestion_path`
- **`clean_before_run` Parameter** - DAG config to clean Bronze data for testing environments
- **Pipeline Metrics Tab** - Execution times, data volumes, historical trends, Sankey diagrams

📄 **Geocoding Docs:** [documents/GEOCODING_INTEGRATION.md](documents/GEOCODING_INTEGRATION.md)

---

## Project Structure

```
case-breweries/
├── dags/                 # Airflow DAGs
│   └── breweries_pipeline_dag.py
├── src/                  # Source code
│   ├── api/              # API client (brewery_client.py)
│   ├── layers/           # Medallion layers (bronze, silver, gold)
│   ├── validation/       # Great Expectations (ge_validator.py)
│   └── enrichment/       # Geocoding (geocoding.py)
├── dashboards/           # Streamlit app
├── tests/                # Unit tests (pytest)
├── utils/                # Helpers (delta_spark, metadata_manager)
├── lakehouse/            # Data Lake
│   ├── bronze/           # Raw JSON
│   ├── silver/           # Delta Lake (curated)
│   ├── gold/             # Delta Lake (aggregations)
│   └── metadata/         # Pipeline run history
└── documents/            # Technical documentation
```

---

## Tests

```bash
# Run tests inside container
docker compose exec airflow-webserver pytest tests/ -v

# With coverage
docker compose exec airflow-webserver pytest tests/ --cov=src --cov-report=html
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No space left on device" | `docker system prune -a --volumes` |
| "Port 8080 already in use" | Edit port in `docker-compose.yaml` |
| DAG not visible | Check: `docker compose logs airflow-scheduler -f` |
| Memory errors | Reduce `spark.executor.memory` in `utils/delta_spark.py` |

---

## Stop Services

```bash
docker compose down           # Stop services
docker compose down -v        # Stop and remove volumes (full reset)
```

---

## Author

**Bruno Lima** - [LinkedIn](https://www.linkedin.com/in/brulimalmeida/) | [Repository](https://github.com/brulim-almeida/case-breweries)
