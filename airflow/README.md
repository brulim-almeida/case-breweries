# Airflow - Orquestração do Pipeline

## 📋 Visão Geral

Este diretório contém a orquestração do pipeline de dados das cervejarias usando Apache Airflow 3.0.0 com a TaskFlow API.

## 🏗️ Arquitetura da DAG

### Pipeline: `breweries_data_pipeline`

```
┌─────────────────────┐
│  Bronze Ingestion   │  → Ingestão da API (Open Brewery DB)
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│ Silver Transform    │  → Transformação e curadoria (Delta Lake)
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Gold Aggregation   │  → Agregações de negócio (Delta Lake)
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Pipeline Validate  │  → Validação de qualidade e relatório
└─────────────────────┘
```

## 🎯 Tasks da DAG

### 1. **bronze_ingestion**
- **Função**: Ingere dados da API do Open Brewery DB
- **Output**: JSON bruto particionado por data
- **Retries**: 3
- **Timeout**: 30 minutos

### 2. **silver_transformation**
- **Função**: Transforma e limpa dados Bronze → Silver
- **Output**: Delta Lake com dados normalizados
- **Retries**: 2
- **Timeout**: 45 minutos

### 3. **gold_aggregation**
- **Função**: Cria agregações de negócio
- **Output**: 6 tabelas Delta Lake com métricas
- **Retries**: 2
- **Timeout**: 30 minutos

### 4. **validate_pipeline**
- **Função**: Valida execução e qualidade dos dados
- **Output**: Relatório de execução completo
- **Retries**: 1

## ⚙️ Configuração

### Schedule
```python
schedule_interval='0 2 * * *'  # Diariamente às 2h AM UTC
```

### Default Args
```python
{
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'email_on_failure': True,
}
```

## 🚀 Como Usar

### 1. Validar a DAG (sem Airflow)

```bash
# Validar sintaxe e configuração
python3 airflow/validate_dags.py
```

### 2. Iniciar o Airflow (Docker)

```bash
# Subir o ambiente completo (usar docker-compose da raiz do projeto)
cd /home/brunolima_driva/VSCode/case-breweries
docker-compose up -d

# Verificar status
docker-compose ps

# Acessar UI
# http://localhost:8080
# User: airflow
# Password: airflow
```

### 3. Executar a DAG

**Via UI:**
1. Acesse http://localhost:8080
2. Encontre `breweries_data_pipeline`
3. Toggle ON para ativar
4. Clique em "Trigger DAG" para execução manual

**Via CLI:**
```bash
# Trigger manual
docker exec -it airflow-scheduler airflow dags trigger breweries_data_pipeline

# Listar execuções
docker exec -it airflow-scheduler airflow dags list-runs -d breweries_data_pipeline

# Ver logs
docker exec -it airflow-scheduler airflow tasks logs breweries_data_pipeline bronze_ingestion <execution_date>
```

### 4. Monitorar Execução

**Logs em tempo real:**
```bash
# Todos os containers
docker-compose logs -f

# Scheduler específico
docker-compose logs -f scheduler

# Worker específico
docker-compose logs -f worker
```

**Verificar status:**
```bash
# Status da DAG
docker exec -it airflow-scheduler airflow dags state breweries_data_pipeline <execution_date>

# Status de uma task
docker exec -it airflow-scheduler airflow tasks state breweries_data_pipeline bronze_ingestion <execution_date>
```

## 📊 Metadata e XCom

A DAG usa XCom para passar metadata entre tasks:

### Bronze → Silver
```json
{
    "total_records": 9038,
    "pages_processed": 181,
    "ingestion_path": "./lakehouse/bronze/breweries/2026/01/21",
    "status": "success"
}
```

### Silver → Gold
```json
{
    "output_records": 9038,
    "distinct_countries": 58,
    "distinct_types": 7,
    "output_path": "./lakehouse/silver/breweries",
    "status": "success"
}
```

### Gold → Validation
```json
{
    "total_aggregations": 6,
    "aggregation_time": 12.5,
    "aggregations": [...],
    "status": "success"
}
```

## 🔔 Callbacks e Alertas

### On Failure
- Registra falha nos logs
- Envia email (se configurado)
- Pode integrar com:
  - Slack
  - PagerDuty
  - Teams
  - Telegram

### On Success
- Registra sucesso nos logs
- Pode enviar notificações de conclusão

## 📈 Métricas Monitoradas

### Data Quality
- **Records Ingested**: Total de registros da API
- **Records Transformed**: Registros salvos no Silver
- **Data Loss Rate**: Taxa de perda de dados (%)
- **Aggregations Created**: Número de tabelas Gold

### Performance
- **Ingestion Time**: Tempo de ingestão Bronze
- **Transformation Time**: Tempo de transformação Silver
- **Aggregation Time**: Tempo de agregação Gold
- **Total Pipeline Time**: Tempo total de execução

### Quality Thresholds
- ⚠️ **Data Loss > 5%**: Alerta de qualidade
- ⚠️ **Zero Aggregations**: Falha crítica
- ⚠️ **Execution > 2h**: Timeout warning

## 🔧 Troubleshooting

### DAG não aparece na UI

```bash
# Verificar logs do scheduler
docker-compose logs scheduler | grep ERROR

# Validar sintaxe
python3 airflow/validate_dags.py

# Listar DAGs reconhecidas
docker exec -it airflow-scheduler airflow dags list
```

### Task falhando

```bash
# Ver logs da task
docker exec -it airflow-scheduler airflow tasks logs \
    breweries_data_pipeline <task_id> <execution_date>

# Testar task localmente
docker exec -it airflow-scheduler airflow tasks test \
    breweries_data_pipeline <task_id> <execution_date>
```

### Problemas de import

```bash
# Verificar Python path
docker exec -it airflow-scheduler python -c "import sys; print('\n'.join(sys.path))"

# Testar imports
docker exec -it airflow-scheduler python -c "from src.layers import BronzeLayer"
```

### Conexão com banco de dados

```bash
# Verificar conexões
docker exec -it airflow-scheduler airflow connections list

# Resetar DB
docker-compose down -v
docker-compose up -d
```

## 📁 Estrutura de Arquivos

```
/                                      # Raiz do projeto
├── docker-compose.yaml                # ⭐ Configuração Docker principal
├── Dockerfile                         # Build customizado com Delta Lake
├── dags/
│   └── breweries_pipeline_dag.py     # DAG principal
├── airflow/
│   ├── dags/                          # DAGs de desenvolvimento
│   ├── logs/                          # Logs das execuções
│   ├── validate_dags.py               # Script de validação
│   ├── test_pipeline.py               # Teste local do pipeline
│   └── README.md                      # Esta documentação
├── src/                               # Código fonte do projeto
└── lakehouse/                         # Data Lake (Bronze/Silver/Gold)
```

## 🎓 Próximos Passos

1. **Sensors**: Adicionar FileSensor para verificar dados
2. **Data Quality**: Implementar Great Expectations
3. **Alertas**: Configurar Slack/email notifications
4. **Backfill**: Estratégia para reprocessamento histórico
5. **SLA**: Definir SLAs para cada task
6. **Variables**: Usar Airflow Variables para configurações
7. **Connections**: Configurar conexões externas (se necessário)

## 📚 Documentação Adicional

- [Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [XCom Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
