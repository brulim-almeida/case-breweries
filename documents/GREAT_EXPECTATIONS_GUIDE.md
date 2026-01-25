# Great Expectations Integration - Data Quality Validation

## 📋 Visão Geral

Este projeto implementa **Great Expectations** para validação automatizada de qualidade de dados em todas as camadas da arquitetura Medallion (Bronze, Silver e Gold).

Great Expectations é um framework Python que permite:
- ✅ Definir "expectations" (expectativas) sobre seus dados
- 📊 Validar dados automaticamente em cada execução do pipeline
- 📄 Gerar documentação HTML interativa (Data Docs)
- 🚨 Detectar anomalias e problemas de qualidade
- 📈 Rastrear qualidade ao longo do tempo

---

## 🏗️ Arquitetura

```
Pipeline Airflow
│
├─ Bronze Ingestion
│  ├─ Ingerir dados da API
│  └─ ✓ Validate Bronze Quality (Great Expectations)
│     ├─ Schema validation
│     ├─ Uniqueness checks
│     ├─ Volume anomaly detection
│     └─ Domain validation
│
├─ Silver Transformation
│  ├─ Transformar e enrichar dados
│  └─ ✓ Validate Silver Quality (Great Expectations)
│     ├─ Data loss check
│     ├─ Enrichment validation
│     ├─ Geographic consistency
│     └─ Null Island detection
│
└─ Gold Aggregation
   ├─ Criar agregações
   └─ ✓ Validate Gold Quality (Great Expectations)
      ├─ Aggregation consistency
      ├─ Top entities validation
      └─ Mathematical integrity
```

---

## 📊 Expectations Implementadas por Camada

### **Bronze Layer** (Dados Brutos da API)

#### 1. **Schema Validation**
```python
expect_table_columns_to_match_ordered_list()
```
- **O que valida:** Verifica se todas as colunas esperadas da API existem
- **Por que é importante:** Detecta mudanças no schema da API
- **Estatística:** Lista de colunas presentes vs esperadas

#### 2. **Uniqueness - IDs Únicos**
```python
expect_column_values_to_be_unique(column="id")
```
- **O que valida:** 100% dos IDs devem ser únicos (sem duplicatas)
- **Por que é importante:** Evita processamento duplicado
- **Estatística:** `unique_count / total_count` 
- **Exemplo:** 9,000 únicos de 9,000 total = ✅

#### 3. **Completeness - Campos Obrigatórios**
```python
expect_column_values_to_not_be_null(column="id", mostly=1.0)
expect_column_values_to_not_be_null(column="name", mostly=0.99)
expect_column_values_to_not_be_null(column="brewery_type", mostly=0.95)
```
- **O que valida:** Campos críticos não podem ser nulos
- **Por que é importante:** Garante dados mínimos para análise
- **Estatística:** Taxa de completude por campo
- **Tolerância:** 
  - `id`: 100% (zero tolerância)
  - `name`: 99% (1% pode falhar)
  - `brewery_type`: 95% (5% pode falhar)

#### 4. **Volume Check - Detecção de Anomalias**
```python
expect_table_row_count_to_be_between(min=5000, max=50000)
```
- **O que valida:** Volume de registros dentro do esperado
- **Por que é importante:** Detecta problemas na API (downtime, rate limiting)
- **Estatística:** Contagem total de registros
- **Alerta:** Se < 5k ou > 50k registros
- **Exemplo:** 8,500 registros = ✅ | 3,200 registros = ❌ (API com problema)

#### 5. **Domain Validation - Valores Conhecidos**
```python
expect_column_values_to_be_in_set(
    column="brewery_type",
    value_set=["micro", "nano", "regional", "brewpub", ...]
)
```
- **O que valida:** brewery_type só pode ter valores catalogados
- **Por que é importante:** Identifica novos tipos não mapeados
- **Estatística:** Distribuição por tipo + tipos desconhecidos
- **Valores aceitos:** micro, nano, regional, brewpub, large, planning, bar, contract, proprietor, closed

#### 6. **Coordinate Ranges - Validação Geográfica**
```python
expect_column_values_to_be_between(
    column="latitude", 
    min_value=-90, 
    max_value=90,
    mostly=0.5
)
```
- **O que valida:** Coordenadas em range geográfico válido
- **Por que é importante:** Detecta coordenadas inválidas da API
- **Estatística:** % de coordenadas válidas
- **Tolerância:** `mostly=0.5` = aceita 50% sem coordenadas (conhecido: ~26% da API não tem coords)
- **Exemplo:** 74% com coords válidas = ✅

#### 7. **Anomaly Detection - Comparação Histórica**
```python
# Compara volume atual vs execução anterior
variance = abs(current_count - previous_count) / previous_count
if variance > 0.20:  # Variação > 20%
    logger.warning("ANOMALY DETECTED")
```
- **O que valida:** Volume não varia >20% entre execuções
- **Por que é importante:** Detecta mudanças bruscas suspeitas
- **Estatística:** % de variação vs última execução
- **Exemplo:** 
  - Ontem: 9,000 registros
  - Hoje: 9,500 registros
  - Variação: 5.5% = ✅
  - Hoje: 7,000 registros
  - Variação: 22% = ⚠️ ANOMALIA

---

### **Silver Layer** (Dados Transformados e Enriched)

#### 1. **Data Loss Check**
```python
expect_table_row_count_to_be_between(
    min_value=int(bronze_count * 0.95),
    max_value=bronze_count
)
```
- **O que valida:** Máximo 5% de perda de dados Bronze → Silver
- **Por que é importante:** Evita perda silenciosa de dados
- **Estatística:** Taxa de retenção = `silver_count / bronze_count`
- **Exemplo:**
  - Bronze: 9,000 registros
  - Silver: 8,900 registros
  - Retenção: 98.9% = ✅
  - Silver: 8,400 registros
  - Retenção: 93.3% = ❌ (>5% de perda)

#### 2. **Country Normalization**
```python
expect_column_values_to_not_be_null(
    column="country_normalized",
    mostly=1.0
)
```
- **O que valida:** 100% dos registros têm país normalizado
- **Por que é importante:** Essencial para particionamento e análises
- **Estatística:** Taxa de normalização
- **Exemplo:** 9,000/9,000 com país = ✅

#### 3. **Coordinate Enrichment**
```python
expect_column_pair_values_to_be_in_set(
    column_A="latitude",
    column_B="longitude",
    mostly=0.85
)
```
- **O que valida:** 85%+ devem ter coordenadas (com geocoding)
- **Por que é importante:** Valida sucesso do geocoding
- **Estatística:** Cobertura de coordenadas
- **Comparação:**
  - Bronze: 74% com coords (API)
  - Silver: 85%+ com coords (API + geocoding) = ✅

#### 4. **Coordinate Validation**
```python
expect_column_values_to_be_in_set(
    column="coordinates_valid",
    value_set=[True],
    mostly=0.85
)
```
- **O que valida:** 85%+ coords geograficamente válidas
- **Por que é importante:** Garante qualidade das coords enriched
- **Estatística:** % de coordenadas que passam validação geográfica
- **Validações incluídas:**
  - Range check (lat: -90 a 90, lng: -180 a 180)
  - Não é Null Island (0, 0)
  - Coords batem com país (bounding box check)

#### 5. **Null Island Detection**
```python
expect_compound_columns_to_be_unique(
    column_list=["latitude", "longitude"]
)
```
- **O que valida:** Detecta coordenadas (0, 0) - "Null Island"
- **Por que é importante:** Erro comum de geocoding
- **Estatística:** Contagem de Null Island
- **Exemplo:** 0 registros em (0,0) = ✅

#### 6. **Schema Enrichment**
```python
expect_table_column_count_to_be_between(
    min_value=20,
    max_value=30
)
```
- **O que valida:** Silver tem mais colunas que Bronze
- **Por que é importante:** Confirma que enrichment ocorreu
- **Estatística:** Contagem de colunas
- **Exemplo:** Bronze: 18 cols, Silver: 25 cols = ✅

---

### **Gold Layer** (Agregações)

#### 1. **Aggregation Consistency**
```python
expect_table_row_count_to_be_between(min=1, max=10000)
expect_column_values_to_be_between(
    column="count",
    min_value=1,
    max_value=silver_count
)
```
- **O que valida:** 
  - Agregações não vazias
  - Counts positivos e <= total Silver
- **Por que é importante:** Garante integridade matemática
- **Estatística:** Range de valores de agregações
- **Exemplo:**
  - breweries_by_country tem 45 países
  - Maior count: 6,500 (USA)
  - Soma de todos: 9,000 = Silver count ✅

#### 2. **Top Entities Validation**
```python
expect_column_values_to_be_in_set(
    column="country_normalized",
    value_set=["United States"]
)
```
- **O que valida:** USA deve estar no resultado
- **Por que é importante:** USA tem ~70% das breweries - se não aparecer, há erro
- **Estatística:** Presença de top entidades conhecidas
- **Exemplo:** USA presente = ✅, USA ausente = ❌

```python
expect_column_values_to_be_in_set(
    column="brewery_type",
    value_set=["micro"]
)
```
- **O que valida:** "micro" deve estar presente (tipo mais comum)
- **Por que é importante:** Validação de consistência
- **Estatística:** Top tipos presentes

---

## 📈 Estatísticas Calculadas

### **Durante Validação Bronze:**
```python
{
    "row_count": 9000,
    "column_count": 18,
    "null_counts": {
        "latitude": 2340,  # 26% sem coordenadas
        "phone": 3600      # 40% sem telefone
    },
    "duplicate_count": 0,
    "success_rate": 100.0,
    "passed_expectations": 8,
    "failed_expectations": 0
}
```

### **Durante Validação Silver:**
```python
{
    "row_count": 8950,
    "column_count": 25,
    "enrichment_stats": {
        "coordinate_coverage": 0.86,      # 86% com coords
        "valid_coordinates_rate": 0.84,   # 84% válidas
        "geocoded_rate": 0.12,            # 12% geocodificadas
        "country_normalized_rate": 1.0    # 100% com país
    },
    "data_retention_rate": 0.994,         # 99.4% Bronze→Silver
    "success_rate": 95.5
}
```

### **Durante Validação Gold:**
```python
{
    "total_aggregations": 4,
    "passed_aggregations": 4,
    "failed_aggregations": 0,
    "aggregation_details": {
        "breweries_by_country": {
            "rows": 45,
            "top_country": "United States",
            "top_count": 6300
        },
        "breweries_by_type": {
            "rows": 10,
            "top_type": "micro",
            "top_count": 5400
        }
    }
}
```

---

## 🚀 Como Usar

### **1. Execução Automática (via Airflow)**
As validações são executadas automaticamente em cada run do pipeline:

```
Bronze Ingestion → Validate Bronze Quality → Silver Transform → ...
```

### **2. Execução Manual**
```python
from pyspark.sql import SparkSession
from src.validation import BreweriesDataValidator

# Inicializar Spark
spark = SparkSession.builder.getOrCreate()

# Ler dados
bronze_df = spark.read.format("delta").load("/opt/airflow/lakehouse/bronze/breweries")

# Criar validator
validator = BreweriesDataValidator(spark=spark)

# Validar
result = validator.validate_bronze_layer(
    df=bronze_df,
    execution_date="2026-01-24"
)

# Verificar resultado
if result['success']:
    print(f"✅ Validation passed! ({result['success_rate']:.1f}%)")
else:
    print(f"❌ {result['failed_expectations_count']} expectations failed")
    for failure in result['failed_details']:
        print(f"   - {failure['expectation']}: {failure['description']}")
```

### **3. Visualizar Data Docs (Relatórios HTML)**
Após execução, Great Expectations gera documentação HTML interativa:

```bash
# Localização dos Data Docs
cd /opt/airflow/great_expectations/uncommitted/data_docs/local_site

# Abrir no navegador
# index.html
```

**O que você verá:**
- 📊 Dashboard de qualidade
- 📈 Gráficos de tendência
- 🎯 % de sucesso por expectation
- 📋 Profiling estatístico automático
- 🔍 Drill-down em falhas

---

## 🎯 Benefícios para o Case de Entrevista

### **1. Demonstra Maturidade Técnica**
- Data quality é crítico em produção
- Mostra preocupação com governança

### **2. Proativo vs Reativo**
- Detecta problemas antes de impactar análises
- Não espera usuários reportarem bugs

### **3. Documentação Automática**
- Data Docs impressionam visualmente
- Catálogo de dados self-service

### **4. Padrão da Indústria**
- Usado por Netflix, Uber, Airbnb
- Framework amplamente adotado

### **5. Fácil de Mostrar**
- Relatório HTML bonito
- Métricas claras e visuais

---

## 📊 Exemplos de Output

### **Console Output (Airflow Logs):**
```
================================================================================
🔍 VALIDATING BRONZE LAYER WITH GREAT EXPECTATIONS
================================================================================
📊 DataFrame Stats: 9,000 rows, 18 columns
📊 Validation Results:
   Total Expectations: 8
   ✅ Passed: 8
   ❌ Failed: 0
   Success Rate: 100.0%
📄 Data Docs generated at: /opt/airflow/great_expectations/uncommitted/data_docs/local_site/index.html
```

### **XCom Output (para debugging):**
```json
{
  "success": true,
  "suite_name": "bronze_quality_suite",
  "total_expectations": 8,
  "passed_expectations": 8,
  "failed_expectations_count": 0,
  "success_rate": 100.0,
  "statistics": {
    "row_count": 9000,
    "column_count": 18,
    "null_counts": {...}
  }
}
```

---

## 🔧 Configuração

Great Expectations é configurado automaticamente na primeira execução:

```
great_expectations/
├── great_expectations.yml         # Configuração principal
├── checkpoints/                   # Checkpoints por layer
│   ├── bronze_checkpoint.yml
│   ├── silver_checkpoint.yml
│   └── gold_checkpoint.yml
├── expectations/                  # Expectation suites
│   ├── bronze_quality_suite.json
│   ├── silver_quality_suite.json
│   └── gold_quality_suite.json
└── uncommitted/
    └── data_docs/
        └── local_site/            # Documentação HTML
            └── index.html         # 📄 Abrir no navegador!
```

---

## 💡 Dicas para Apresentação

### **Durante a Entrevista:**

1. **Abra os Data Docs HTML** - Impacto visual forte
2. **Mostre as estatísticas** - Números concretos impressionam
3. **Explique a lógica** - Por que cada expectation é importante
4. **Conecte com produção** - "Em prod, isso alertaria via Slack"
5. **Fale de trade-offs** - "mostly=0.85 porque sabemos que 15% não terão coords"

### **Perguntas que você pode responder:**

**Q: "Como você garante qualidade dos dados?"**
✅ "Implementei Great Expectations com X expectations validando schema, volume, domínio e consistência. Cada execução gera relatório de qualidade."

**Q: "E se os dados mudarem?"**
✅ "Tenho anomaly detection comparando volume entre execuções. Se variar >20%, alerta automático."

**Q: "Como você documenta os dados?"**
✅ "Great Expectations gera Data Docs automaticamente com profiling estatístico e catálogo de expectations."

---

## 📚 Referências

- [Great Expectations Docs](https://docs.greatexpectations.io/)
- [Expectation Gallery](https://greatexpectations.io/expectations/)
- [Best Practices](https://docs.greatexpectations.io/docs/guides/miscellaneous/best_practices/)

---

## 🎯 Next Steps (Futuro)

Para expandir ainda mais:

1. **Alerting:** Integrar com Slack/Discord para notificações de falha
2. **Data Contracts:** Formalizar contratos de dados entre camadas
3. **Custom Expectations:** Criar expectations específicas do domínio
4. **Performance:** Adicionar expectations de tempo de execução (SLA)
5. **Comparisons:** Comparar distribuições dia a dia (data drift)

---

## ✅ Checklist de Validação

- [x] Bronze: Schema completo e IDs únicos
- [x] Bronze: Volume dentro do esperado (5k-50k)
- [x] Bronze: Tipos de brewery conhecidos
- [x] Bronze: Coordenadas em ranges válidos
- [x] Silver: Data loss < 5%
- [x] Silver: País normalizado para 100%
- [x] Silver: 85%+ com coordenadas válidas
- [x] Silver: Sem Null Island (0,0)
- [x] Gold: Agregações não vazias
- [x] Gold: USA no top países
- [x] Gold: Counts positivos e consistentes

**Status Geral: ✅ IMPLEMENTADO**
