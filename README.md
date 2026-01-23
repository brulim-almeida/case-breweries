# Case Breweries Data Lake

O desafio deste case foi desenvolver um pipeline de dados robusto para extrair, processar e armazenar informações de cervejarias a partir da API Open Brewery DB. O pipeline segue a arquitetura Medallion (Bronze, Silver e Gold) implementada com Apache Airflow, PySpark e Delta Lake.

<img width="935" height="418" alt="image" src="https://github.com/user-attachments/assets/e75faccd-fc85-47a8-84f8-f6b73fb8ccbb" />


> 💾 Arquitetura da Pipeline de Dados

## Resumo dos Principais Conceitos e Tecnologias Utilizadas

* **Pipeline orquestrado no Apache Airflow** com TaskFlow API
* **Airflow rodando em containers Docker** com CeleryExecutor para processamento distribuído
* **Processamento via PySpark 3.5.0** com suporte a Delta Lake 3.1.0
* **Arquitetura Medallion** (Bronze → Silver → Gold) para organização e qualidade dos dados
* **Processamento incremental** na Silver layer para evitar reprocessamento de dados históricos
* **Delta Lake** para ACID transactions e versionamento de dados
* **Boas práticas de engenharia**: código modular, documentado e testável
* **Data Quality** integrado com validações e métricas de qualidade em cada camada
* **XCom** para comunicação entre tasks e rastreamento de metadados

---

## 🎯 Funcionalidades Extras Implementadas (Além do Case Original)

Este projeto vai além dos requisitos básicos do case, incluindo funcionalidades avançadas que agregam valor significativo ao pipeline:

### 1. 🎨 Dashboard Interativo com Streamlit
**Por que foi adicionado:** Demonstrar o valor dos dados processados através de visualizações interativas e acessíveis para stakeholders não-técnicos.

**Funcionalidades:**
- 5 abas de análise (Maps, Geographic, Types, Quality, Cities)
- Visualizações interativas com Plotly (mapas, treemaps, gauges)
- Métricas de qualidade em tempo real
- Filtros dinâmicos por país, tipo e região
- Leitura nativa de Delta Lake sem overhead Spark

### 2. 🌍 Geocoding Automático para Coordenadas Faltantes
**Por que foi adicionado:** ~26% das cervejarias não possuem coordenadas na API, limitando análises geográficas. A solução enriquece automaticamente esses dados.

**Funcionalidades:**
- Integração com API Nominatim (OpenStreetMap)
- Geocoding inteligente com estratégia de fallback
- Rate limiting respeitando limites da API (1 req/seg)
- Métricas detalhadas: taxa de sucesso, cobertura, performance
- Processamento batch configurável (100-1000 registros por execução)

**Resultados esperados:**
- Melhoria de cobertura geográfica de ~74% para ~85%+
- Viabilização de análises espaciais completas
- Logs detalhados de sucesso/falha para auditoria

### 3. ✅ Validação Geográfica de Coordenadas
**Por que foi adicionado:** Algumas coordenadas da API (e do geocoding) são incorretas, resultando em pontos no oceano ou países errados.

**Validações implementadas:**
- Range check: latitude (-90 a 90), longitude (-180 a 180)
- Detecção de "Null Island" (0,0) - erro comum de geocoding
- Consistência geográfica: verifica se coordenadas batem com país informado
- Bounding boxes para 13 países principais (USA, UK, Brasil, Alemanha, etc.)
- Coluna `coordinates_valid` para filtragem automática

**Benefícios:**
- Mapas limpos sem pontos suspeitos no oceano
- Maior confiabilidade em análises geográficas
- Identificação de problemas de qualidade para correção

### 4. 📊 Tabela Completa de Breweries na Gold Layer
**Por que foi adicionado:** O dashboard precisa acessar dados individuais de cervejarias (não apenas agregações).

**Implementação:**
- Tabela `breweries` completa (não particionada) na Gold layer
- Otimizada para consultas analíticas rápidas
- Inclui todas as colunas enriched da Silver + validações
- Base para análises exploratórias e drill-down

### 5. 📈 Métricas Avançadas de Data Quality
**Além das métricas básicas, foram implementadas:**
- Cobertura de coordenadas (antes/depois do geocoding)
- Taxa de melhoria de qualidade
- Breakdown de falhas de validação
- Performance de geocoding (registros/segundo)
- Taxa de perda de dados entre camadas (data loss rate)

---

## Estrutura de Diretórios e Arquivos

```
case-breweries/
├── .env                          # Variáveis de ambiente
├── .env.example                  # Template de configuração
├── .gitignore                    # Git ignore rules
├── Dockerfile                    # Imagem customizada Airflow + PySpark
├── docker-compose.yaml           # Orquestração completa (8 serviços)
├── pytest.ini                    # Configuração de testes
├── requirements.txt              # Dependências Python
│
├── airflow/                      # Configurações Airflow
│   └── README.md                 # Documentação de setup
│
├── dags/                         # DAGs do Airflow
│   └── breweries_pipeline_dag.py # Pipeline principal
│
├── dashboards/                   # 🎨 Dashboard Streamlit (EXTRA)
│   └── streamlit_app.py          # App interativo com visualizações
│
├── src/                          # Código-fonte principal
│   ├── api/                      # Cliente API
│   │   └── brewery_client.py    # Integração com Open Brewery DB
│   ├── config/                   # Configurações
│   │   └── settings.py           # Settings centralizados
│   ├── enrichment/               # Enriquecimento de dados (EXTRA)
│   │   ├── geocoding.py          # Geocoding com Nominatim API
│   │   └── test_geocoding.py    # Testes de geocoding
│   └── layers/                   # Camadas Medallion
│       ├── bronze_layer.py       # Ingestão de dados brutos
│       ├── silver_layer.py       # Transformação + Geocoding + Validação
│       └── gold_layer.py         # Agregações + Tabela completa
│
├── tests/                        # Testes unitários
│   ├── test_bronze_layer.py
│   ├── test_silver_layer.py
│   └── test_gold_layer.py
│
├── utils/                        # Utilitários
│   └── delta_spark.py            # Helper para Delta Lake
│
├── lakehouse/                    # Data Lake
│   ├── bronze/                   # Dados brutos (JSON particionado)
│   ├── silver/                   # Dados curados (Delta Lake)
│   └── gold/                     # Agregações (Delta Lake)
│
└── logs/                         # Logs do Airflow
```

## Descrição da DAG [breweries_pipeline_dag.py]

<img width="297" height="313" alt="image" src="https://github.com/user-attachments/assets/0d298aac-51a8-4554-b186-54f322c1f7c0" />

<img width="1151" height="302" alt="image" src="https://github.com/user-attachments/assets/d4eeab73-0879-4a18-bd7c-58cdcd223fa7" />

O pipeline é composto por 4 tasks principais encadeadas:

### 1️⃣ Bronze Ingestion (`bronze_ingestion`)
**Responsável por:**
- Extrair dados da API Open Brewery DB com paginação automática (~9,000 cervejarias)
- Persistir dados brutos em formato JSON no lakehouse/bronze
- Particionamento temporal: `year=YYYY/month=MM/day=DD/breweries_TIMESTAMP.json`
- Retornar metadados via XCom: `total_records`, `pages_processed`, `ingestion_path`, `status`

**Tratamento de erros:** Retry até 3x com backoff exponencial (5min → 10min → 20min)

### 2️⃣ Silver Transformation (`silver_transformation`)
**Responsável por:**
- **Processamento incremental**: consome APENAS o arquivo da ingestão atual (via `ingestion_path`)
- Limpeza e normalização de dados (trim, null handling, padronização)
- Enriquecimento: `full_address`, flags `has_coordinates`, `has_contact`, timestamp `processed_at`
- **🌍 Geocoding automático** (EXTRA): Enriquece coordenadas faltantes via API Nominatim
- **✅ Validação geográfica** (EXTRA): Valida coordenadas e marca pontos suspeitos/inválidos
- Escrita em Delta Lake com **schema evolution** habilitado para novas colunas
- Particionamento: `country_normalized`, `state`
- Cálculo de métricas de qualidade: completeness rate, coordinate coverage, contact coverage
- Retornar metadados: `output_records`, `distinct_countries`, `distinct_types`, `quality_metrics`, `geocoding_metrics`

**Inovações:**
- Processamento incremental evita reprocessamento de histórico
- Geocoding configurable: `max_geocoding_records` para controlar volume
- Logs detalhados de validação de coordenadas (null island, out of range, wrong country)

### 3️⃣ Gold Aggregation (`gold_aggregation`)
**Responsável por:**
- Consumir dados curados da Silver layer
- **📊 Criar tabela completa de breweries** (EXTRA): Dataset não-agregado para dashboard
- Criar 6 agregações estratégicas de negócio:
  - `by_country`: Total de cervejarias por país
  - `by_type`: Distribuição por tipo de cervejaria
  - `by_state`: Distribuição por estado (top 20)
  - `by_type_and_country`: Matriz tipo × país
  - `by_type_and_state`: Matriz tipo × estado
  - `summary_statistics`: Estatísticas consolidadas
- Persistir cada dataset como tabela Delta Lake separada
- Otimização para consumo analítico (baixa latência)

### 4️⃣ Validate Pipeline (`validate_pipeline`)
**Responsável por:**
- Validar integridade dos metadados de todas as camadas
- Verificar status de execução (SUCCESS vs FAILURE)
- Calcular `data_loss_rate` comparando registros Bronze vs Silver
- Gerar relatório consolidado com:
  - Status final: SUCCESS/FAILURE
  - Métricas de cada camada
  - Indicadores de qualidade (países, tipos, cobertura)
  - Taxa de perda de dados (esperado: 0%)

**Callback:** Em caso de falha, aciona `on_failure_callback` para notificação

## Mais Detalhes sobre a DAG

### Data Quality
A validação de qualidade foi implementada em múltiplas camadas:
- **Bronze**: Validação de schema JSON e total de registros
- **Silver**: Métricas de completeness, coordinate coverage, contact coverage, contagem de valores únicos
- **Gold**: Validação de agregações e consistência de totais
- **Pipeline**: Validação end-to-end com cálculo de data loss rate

Possíveis evoluções: Great Expectations, Soda Core, alertas via Slack/email

### Monitoramento
Implementação atual:
- **XCom**: Rastreamento de metadados entre tasks
- **Airflow Logs**: Logs estruturados com timestamps e contexto
- **Métricas de execução**: Duration, records processed, quality metrics

Evoluções possíveis:
- Integração com **Prometheus + Grafana** para dashboards em tempo real
- Alertas proativos via **PagerDuty** ou **Opsgenie**
- Observabilidade com **OpenTelemetry**

### Processamento Incremental
**Problema resolvido:** Silver layer inicialmente processava TODO o histórico Bronze a cada execução (multiplicação de dados).

**Solução implementada:**
```python
# DAG passa o caminho específico da ingestão
silver.transform_breweries(ingestion_path=bronze_metadata['ingestion_path'])

# Silver layer processa apenas o arquivo específico
if ingestion_path:
    df = spark.read.json(ingestion_path)  # ✅ Incremental
else:
    df = spark.read.json(f"{bronze_path}/year=*/month=*/day=*/*.json")  # Full load
```

**Resultado:** 0% data loss, processamento eficiente, sem duplicação

## Arquitetura de Deployment

### Docker Compose (7 serviços)
- **postgres**: Metadata database (Airflow backend)
- **redis**: Message broker (Celery)
- **scheduler**: Agendador de DAGs
- **worker**: Executor de tasks (CeleryExecutor)
- **webserver**: Interface web (porta 8080)
- **triggerer**: Deferrable operators
- **flower**: Monitoramento Celery (porta 5555)

### Volumes Persistentes
- `./dags` → `/opt/airflow/dags`
- `./logs` → `/opt/airflow/logs`
- `./lakehouse` → `/opt/airflow/lakehouse`
- `./src` → `/opt/airflow/src`
- `./utils` → `/opt/airflow/utils`

## Pontos de Melhoria e Próximos Passos

### 1. Escalabilidade e Cloud-Native
**Desafio atual:** Spark rodando em único worker do Airflow (limitação de recursos)

**Solução proposta:**
- Migrar para **Kubernetes** (EKS, GKE ou AKS)
- Implementar **Spark Operator** para auto-scaling de Spark executors
- Utilizar **Airflow on K8s** com KubernetesExecutor

**Benefícios:** Alta disponibilidade, elasticidade, melhor utilização de recursos

### 2. CI/CD e Infraestrutura como Código
**Implementações sugeridas:**
- **Terraform** para provisionamento de infra (AWS/GCP/Azure)
- **GitHub Actions** ou **GitLab CI** para pipelines CI/CD
- **ArgoCD** para GitOps e deployment automatizado
- **Testes automatizados** em múltiplos ambientes (dev, staging, prod)

**Benefícios:** FinOps, reprodutibilidade, rollback rápido, segregação de ambientes

### 3. Segurança e Governança
- **Secrets Management**: Migrar para AWS Secrets Manager ou HashiCorp Vault
- **IAM Roles**: Implementar least privilege access

### 4. Otimizações de Performance
- **Z-Ordering** no Delta Lake para queries otimizadas
- **Data Skipping** com estatísticas de partições
- **Compaction** automático de small files
- **Caching** de datasets frequentes

### 5. Advanced Analytics
- **Streaming**: Implementar ingestão em tempo real com Kafka + Spark Streaming
- **Data Quality**: Integrar Great Expectations com alertas automáticos

## 🎨 Dashboard Interativo com Streamlit (FUNCIONALIDADE EXTRA)

> **📌 IMPORTANTE:** Esta funcionalidade foi implementada como um **diferencial adicional**, não sendo parte dos requisitos originais do case. O objetivo é demonstrar o valor dos dados processados através de visualizações interativas e insights acionáveis.

Como demonstração adicional das capacidades do pipeline, foi implementado um **dashboard interativo com Streamlit** para visualização dos dados agregados na camada Gold.

### Características do Dashboard

**📊 5 Abas de Análise:**
1. **🗺️ Maps** (EXTRA): Visualização geográfica global com mapas interativos
   - Scatter plot mundial com filtros por país e tipo
   - Mapa de densidade por concentração geográfica
   - Filtros dinâmicos de coordenadas válidas
   - Alertas de coordenadas inválidas filtradas
   
2. **🌍 Geographic**: Distribuição global de cervejarias
   - Visualização comparativa (incluindo/excluindo EUA)
   - Top 10 países com treemap hierárquico
   
3. **🏷️ Types**: Análise por tipo de cervejaria
   - Gráficos de pizza e barras interativos
   - Distribuição percentual
   
4. **📈 Quality**: Métricas de qualidade dos dados
   - Gauges interativos para cobertura de coordenadas
   - Métricas de informações de contato
   - Impacto do geocoding (antes/depois)
   
5. **🏙️ Cities**: Análise por estados
   - Treemap hierárquico (país → estado → cidade)
   - Top 20 rankings dinâmicos

**🔧 Stack Técnico:**
- **Streamlit 1.31.0**: Framework web interativo
- **Plotly 5.18.0**: Visualizações interativas e responsivas (scatter_geo, treemap, gauges)
- **deltalake 0.15.0**: Leitura nativa de Delta Lake sem overhead Java/Spark
- **pandas 2.1.4**: Manipulação de dados

**🎯 Decisões Arquiteturais:**
- Utilização da biblioteca `deltalake` Python para leitura direta dos arquivos Delta, evitando a complexidade de inicializar Spark/JVM no container do Streamlit
- Dashboard consome diretamente as tabelas Gold agregadas pelo pipeline Airflow
- Deploy como serviço adicional no Docker Compose com profile dedicado (`--profile streamlit`)
- Filtros de coordenadas válidas aplicados automaticamente (remove pontos no oceano)

### Como Executar o Dashboard

```bash
# Iniciar todos os serviços incluindo Streamlit
docker compose --profile streamlit up -d

# Acessar o dashboard
# URL: http://localhost:8501
```

O dashboard se conecta automaticamente aos dados da camada Gold e oferece:
- ✅ Visualizações interativas com zoom, pan e export de imagens
- ✅ Filtros e drill-down para análise detalhada
- ✅ Métricas de qualidade em tempo real
- ✅ Insights automáticos sobre distribuição e cobertura de dados

**💡 Benefícios:**
- Demonstração visual do valor agregado pelo pipeline
- Interface amigável para stakeholders não-técnicos
- Validação imediata da qualidade das agregações Gold
- Base para desenvolvimento de analytics avançados

### Parar o Dashboard

```bash
# Parar apenas o Streamlit
docker compose stop streamlit

# Parar todos os serviços
docker compose --profile streamlit down
```

## Passos para Executar o Projeto

> [!NOTE]
> Projeto desenvolvido e testado em ambiente Linux (Ubuntu/Debian)

### Requisitos
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM mínimo
- 20GB espaço em disco

### 1. Clonar o Repositório
```bash
git clone https://github.com/brulim-almeida/case-breweries.git
cd case-breweries
```

### 2. Configurar Variáveis de Ambiente
```bash
cp .env.example .env
# Editar .env se necessário (configurações padrão já funcionam)
```

### 3. Ajustar Permissões (Linux)
```bash
sudo chmod -R 777 ./logs ./lakehouse
```

### 4. Build da Imagem Docker
```bash
docker build -t airflow-breweries:latest .
```

> [!TIP]
> A imagem já inclui OpenJDK 17, PySpark 3.5.0 e Delta Lake 3.1.0

### 5. Iniciar os Serviços
```bash
docker compose up -d
```

Aguarde ~2 minutos para inicialização completa. Verifique status:
```bash
docker compose ps
```

### 6. Acessar o Airflow Webserver
```
URL: http://localhost:8080
Login: airflow
Senha: airflow
```

### 7. Executar a DAG
1. Na interface web, vá em **DAGs**
2. Localize `breweries_pipeline_dag`
3. Ative a DAG (toggle on)
4. Clique em **Trigger DAG** (botão ▶️)

### 8. Monitorar Execução
- **Graph View**: Visualizar dependências entre tasks
- **Grid View**: Histórico de execuções
- **Logs**: Logs detalhados de cada task
- **Flower**: http://localhost:5555 (monitoramento Celery)

**Logs Esperados de Geocoding (Silver Layer):**
```
GEOCODING ENRICHMENT
================================================================================
BEFORE Geocoding:
  Total records: 9,038
  With coordinates: 6,685 (73.97%)
  Missing coordinates: 2,353 (26.03%)

Processing 1000 addresses...
⏱️  Estimated minimum time: ~16.7 minutes (at 1.0s per request)

Progress: 10/100 (10.0%) - Geocoded: 10, Failed: 0 - ETA: ~15.2 min
Progress: 100/1000 (10.0%) - Geocoded: 95, Failed: 5 - ETA: ~14.8 min
...

AFTER Geocoding:
  With coordinates: 7,580 (83.87%)
  Missing coordinates: 1,458 (16.13%)
  
Enrichment: +895 new coordinates (38.0% improvement)
Success rate: 89.5%
```

**Logs Esperados de Validação de Coordenadas:**
```
Validating geographic coordinates...
Coordinate validation results:
  Total with coordinates: 7,580
  Valid coordinates: 7,340 (96.83%)
  Invalid/Suspicious: 240 (3.17%)
  
Validation failures breakdown:
  - Null Island (0,0): 8
  - Out of range: 2
  - Wrong country/region: 230
```

### 9. Validar Resultados
```bash
# Verificar dados Bronze
ls -lh lakehouse/bronze/breweries/year=*/month=*/day=*/

# Verificar dados Silver (Delta Lake)
ls -lh lakehouse/silver/breweries/_delta_log/

# Verificar agregações Gold
ls -lh lakehouse/gold/breweries/
```

### 10. Parar os Serviços
```bash
docker compose down  # Para os serviços
docker compose down -v  # Para e remove volumes (reset completo)
```

## Estrutura de Dados

### Bronze Layer
```json
{
  "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
  "name": "Foo Bar Brewery",
  "brewery_type": "micro",
  "address_1": "1234 Main St",
  "city": "San Francisco",
  "state": "California",
  "postal_code": "94102",
  "country": "United States",
  "longitude": "-122.419906",
  "latitude": "37.7749",
  "phone": "4155551234",
  "website_url": "http://foobarbrewery.com"
}
```

### Silver Layer (Delta Lake)
Adiciona campos enriquecidos:
- `full_address`: String concatenada completa
- `country_normalized`: País normalizado
- `brewery_type_normalized`: Tipo normalizado
- `has_coordinates`: Boolean (latitude e longitude preenchidas)
- `has_contact`: Boolean (phone ou website preenchido)
- `is_complete`: Boolean (dados completos)
- `coordinates_valid`: Boolean (EXTRA - validação geográfica)
- `silver_processed_at`: Timestamp de processamento
- `processing_date`, `processing_year`, `processing_month`: Campos temporais

### Gold Layer Aggregations
- **breweries** (EXTRA): Tabela completa não-agregada para dashboard
- **by_country**: `country`, `total_breweries`
- **by_type**: `brewery_type`, `total_breweries`
- **by_state**: `country`, `state`, `total_breweries`
- **by_type_and_country**: `brewery_type`, `country`, `total_breweries`
- **by_type_and_state**: `brewery_type`, `state`, `total_breweries`
- **summary_statistics**: Estatísticas consolidadas de qualidade e cobertura

## Testes

Executar testes unitários:
```bash
# Dentro do container Airflow
docker compose exec airflow-webserver pytest tests/ -v

# Com coverage
docker compose exec airflow-webserver pytest tests/ --cov=src --cov-report=html
```

## Troubleshooting

### Erro: "No space left on device"
```bash
docker system prune -a --volumes
```

### Erro: "Port 8080 already in use"
Editar `docker-compose.yaml` e alterar porta do webserver

### DAG não aparece na interface
```bash
# Verificar logs do scheduler
docker compose logs airflow-scheduler -f

# Validar sintaxe da DAG
docker compose exec airflow-webserver python /opt/airflow/dags/breweries_pipeline_dag.py
```

### Tasks falhando com erro de memória
Aumentar recursos do Docker Desktop ou reduzir `spark.executor.memory` em `utils/delta_spark.py`

## Conclusão & Agradecimentos

Este projeto demonstra a implementação completa de um Data Lake moderno utilizando as melhores práticas de engenharia de dados:

**✅ Requisitos do Case (Implementados):**
- Arquitetura Medallion para organização e qualidade
- Processamento incremental para eficiência
- Orquestração robusta com Airflow
- ACID transactions com Delta Lake
- Código modular, testável e documentado
- Deploy containerizado e reprodutível

**🚀 Funcionalidades Extras (Além do Case):**
- **Dashboard Streamlit** com 5 abas de análise e visualizações interativas
- **Geocoding automático** para enriquecer 26% das cervejarias sem coordenadas
- **Validação geográfica** para identificar e filtrar coordenadas inválidas
- **Tabela completa na Gold** para análises exploratórias
- **Métricas avançadas** de data quality e performance

O case oferece uma base sólida para evoluções futuras em cloud, ML pipelines e analytics avançado, enquanto as funcionalidades extras demonstram capacidade de ir além dos requisitos e agregar valor ao produto final.

Agradeço pela oportunidade e estou disponível para qualquer esclarecimento!

---

**Autor:** [Bruno Lima](https://www.linkedin.com/in/brulimalmeida/)  
**Repositório:** [case-breweries](https://github.com/brulim-almeida/case-breweries)  
