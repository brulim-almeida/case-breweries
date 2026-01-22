# Bronze Layer - Raw Data Ingestion

A camada **Bronze** é responsável por ingerir dados brutos da **Open Brewery DB API** e armazená-los em formato nativo (JSON) no Data Lake.

## 📋 Características

### Funcionalidades Principais

- ✅ **Ingestão completa de dados** da Open Brewery DB API
- ✅ **Paginação automática** para buscar todos os registros
- ✅ **Retry logic** com exponential backoff
- ✅ **Particionamento por data** (year/month/day)
- ✅ **Metadata tracking** para cada ingestão
- ✅ **Error handling** robusto
- ✅ **Logging detalhado** de todas as operações

### Estrutura de Dados

```
lakehouse/bronze/breweries/
├── year=2026/
│   └── month=01/
│       └── day=21/
│           └── breweries_20260121_123456_abc123.json
└── _metadata/
    └── ingestion_id=xyz789.json
```

## 🚀 Como Usar

### 1. Uso Básico

```python
from src.layers.bronze_layer import BronzeLayer

# Criar instância
bronze = BronzeLayer()

# Ingerir todos os dados
metadata = bronze.ingest_breweries()

print(f"Total ingerido: {metadata['total_records']} cervejarias")
```

### 2. Ingestão de Teste (1 página apenas)

```python
# Útil para testes
metadata = bronze.ingest_breweries(max_pages=1)
```

### 3. Consultar Dados Ingeridos

```python
# Obter última ingestão
latest = bronze.get_latest_ingestion()
print(f"Última ingestão: {latest['ingestion_timestamp']}")

# Listar todas as ingestões
ingestions = bronze.list_ingestions()
print(f"Total de ingestões: {len(ingestions)}")

# Ler dados brutos
data = bronze.read_bronze_data()
print(f"Total de registros: {len(data)}")
```

### 4. Context Manager

```python
# Recomendado: usar com context manager
with BronzeLayer() as bronze:
    metadata = bronze.ingest_breweries()
    # API client é fechado automaticamente
```

## 📦 Módulos

### `src/api/brewery_client.py`

Cliente HTTP para consumir a Open Brewery DB API.

**Principais métodos:**
- `get_breweries_page(page)` - Busca uma página específica
- `get_all_breweries()` - Busca todos os dados com paginação automática
- `get_brewery_by_id(id)` - Busca uma cervejaria específica
- `search_breweries(query)` - Busca por nome, cidade ou estado

**Características:**
- Retry automático em caso de falha
- Timeout configurável
- Rate limiting
- Session pooling

### `src/layers/bronze_layer.py`

Processador da camada Bronze.

**Principais métodos:**
- `ingest_breweries()` - Ingere dados da API e salva no Bronze
- `get_latest_ingestion()` - Retorna metadata da última ingestão
- `list_ingestions()` - Lista todas as ingestões
- `read_bronze_data()` - Lê dados brutos do Bronze

**Características:**
- Particionamento por data
- Metadata tracking
- Nomes de arquivo únicos (timestamp + hash)
- Error recovery

### `src/config/settings.py`

Configurações centralizadas do projeto.

**Principais configurações:**
- `BREWERY_API_BASE_URL` - URL da API
- `BRONZE_PATH` - Caminho da camada Bronze
- `SPARK_*` - Configurações do Spark
- `DATA_QUALITY_*` - Configurações de qualidade

## 🧪 Testes

### Executar Testes Unitários

```bash
# Todos os testes
pytest tests/test_bronze_layer.py -v

# Testes específicos
pytest tests/test_bronze_layer.py::TestBronzeLayer::test_ingest_breweries_success -v

# Com cobertura
pytest tests/test_bronze_layer.py --cov=src/layers --cov-report=html
```

### Executar Exemplo

```bash
# Script interativo de exemplo
python example_bronze_ingestion.py
```

## 📊 Formato dos Dados

### Dados Brutos (JSON)

Cada registro contém:

```json
{
  "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
  "name": "Sample Brewing Company",
  "brewery_type": "micro",
  "address_1": "123 Main St",
  "city": "San Francisco",
  "state": "California",
  "postal_code": "94102",
  "country": "United States",
  "longitude": "-122.419906",
  "latitude": "37.774929",
  "phone": "4155551234",
  "website_url": "http://www.samplebrewery.com"
}
```

### Metadata da Ingestão

```json
{
  "ingestion_id": "abc123...",
  "ingestion_timestamp": "2026-01-21T10:30:00",
  "source": "open_brewery_db_api",
  "total_records": 8000,
  "file_path": "/opt/airflow/lakehouse/bronze/breweries/...",
  "file_size_bytes": 5242880,
  "partition_date": {
    "year": 2026,
    "month": 1,
    "day": 21
  },
  "status": "success"
}
```

## 🔧 Configuração

### Variáveis de Ambiente

Defina no arquivo `.env`:

```bash
# API Configuration
BREWERY_API_BASE_URL=https://api.openbrewerydb.org/v1
BREWERY_API_TIMEOUT=30
BREWERY_API_RETRIES=3
BREWERY_API_RETRY_DELAY=5

# Data Lake Paths
BRONZE_PATH=/opt/airflow/lakehouse/bronze
```

## 📈 Monitoramento

A camada Bronze gera logs detalhados:

```
2026-01-21 10:30:00 - bronze_layer - INFO - STARTING BRONZE LAYER INGESTION
2026-01-21 10:30:01 - brewery_client - INFO - Fetching breweries page 1 (per_page=200)
2026-01-21 10:30:02 - brewery_client - INFO - Successfully fetched 200 breweries from page 1
2026-01-21 10:35:45 - bronze_layer - INFO - Saving 8000 breweries to Bronze layer...
2026-01-21 10:35:46 - bronze_layer - INFO - BRONZE LAYER INGESTION COMPLETED SUCCESSFULLY
2026-01-21 10:35:46 - bronze_layer - INFO - Total records ingested: 8,000
```

## ⚠️ Considerações

### Performance
- A ingestão completa pode levar alguns minutos dependendo do total de registros
- Use `max_pages` para testes rápidos
- Delay entre páginas: 0.5s (configurável)

### Armazenamento
- Dados brutos em JSON (facilita inspeção)
- Particionamento por data (facilita queries)
- Metadata separado (não polui dados)

### Idempotência
- Cada ingestão gera um arquivo novo
- Nomes únicos previnem conflitos
- Metadata permite rastreamento completo

## 🔗 Próximos Passos

Após a ingestão no Bronze, os dados seguem para:

1. **Silver Layer** - Transformação para formato colunar (Parquet/Delta)
2. **Gold Layer** - Agregações e métricas de negócio

---

**Documentação**: [Ver README principal](../README.md)
