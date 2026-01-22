# Gold Layer - Business Aggregations

## Overview

A camada Gold é a camada final da arquitetura Medallion, focada em agregações de negócio otimizadas para análise e consumo por ferramentas de BI. Todas as tabelas são armazenadas em formato Delta Lake para garantir ACID compliance e suporte a time travel.

## 📊 Aggregações Disponíveis

### 1. Breweries by Type (`breweries_by_type`)
Agrupa cervejarias por tipo normalizado.

**Métricas:**
- `brewery_count`: Total de cervejarias
- `unique_breweries`: IDs únicos
- `with_coordinates`: Cervejarias com coordenadas geográficas
- `with_contact`: Cervejarias com informações de contato
- `complete_records`: Registros completos (completeness_score >= 4)
- `completeness_rate`: Taxa de completude (%)

**Uso:**
```python
from src.layers import GoldLayer

with GoldLayer() as gold:
    df = gold.read_aggregation("breweries_by_type")
    df.show()
```

### 2. Breweries by Country (`breweries_by_country`)
Agrupa cervejarias por país normalizado.

**Métricas:**
- `brewery_count`: Total de cervejarias no país
- `unique_breweries`: IDs únicos
- `distinct_types`: Quantidade de tipos diferentes de cervejarias
- `with_coordinates`: Com coordenadas
- `with_contact`: Com contato

**Caso de uso:** Análise de distribuição geográfica, identificação de mercados principais.

### 3. Breweries by State (`breweries_by_state`)
Agrupa cervejarias por estado/província e país.

**Métricas:**
- `brewery_count`: Total de cervejarias no estado
- `distinct_cities`: Número de cidades com cervejarias
- `distinct_types`: Tipos diferentes de cervejarias

**Caso de uso:** Análise regional detalhada, planejamento de expansão.

### 4. Breweries by Type and Country (`breweries_by_type_and_country`)
Cross-tabulation de tipos de cervejarias por país.

**Métricas:**
- `brewery_count`: Contagem para cada combinação tipo+país

**Caso de uso:** Análise de mercado por segmento, comparação internacional de tipos de cervejarias.

### 5. Breweries by Type and State (`breweries_by_type_and_state`)
Cross-tabulation de tipos de cervejarias por estado e país.

**Métricas:**
- `brewery_count`: Contagem para cada combinação tipo+estado+país

**Caso de uso:** Análise granular de mercado, identificação de nichos regionais.

### 6. Summary Statistics (`brewery_summary_statistics`)
Estatísticas gerais consolidadas de todo o dataset.

**Métricas:**
- `total_breweries`: Total de registros
- `unique_breweries`: Total de cervejarias únicas
- `distinct_types`: Tipos únicos de cervejarias
- `distinct_countries`: Países representados
- `distinct_states`: Estados/províncias representados
- `distinct_cities`: Cidades representadas
- `with_coordinates`: Total com coordenadas geográficas
- `with_contact`: Total com informações de contato
- `complete_records`: Total de registros completos

**Caso de uso:** Dashboards executivos, KPIs gerais, monitoramento de qualidade de dados.

## 🚀 Como Usar

### Criar Agregações

```python
from src.layers import GoldLayer

with GoldLayer() as gold:
    # Criar todas as agregações
    metadata = gold.create_aggregations()
    
    print(f"Status: {metadata['status']}")
    print(f"Total de tabelas criadas: {metadata['total_aggregations']}")
    print(f"Tempo de processamento: {metadata['aggregation_time_seconds']:.2f}s")
```

### Ler Agregações

```python
# Ler agregação específica
with GoldLayer() as gold:
    df = gold.read_aggregation("breweries_by_type")
    df.show()
```

### Aplicar Filtros

```python
# Ler com filtros
with GoldLayer() as gold:
    filters = {"brewery_type_normalized": "micro"}
    df = gold.read_aggregation("breweries_by_type_and_country", filters=filters)
    df.show()
```

### Listar Agregações Disponíveis

```python
with GoldLayer() as gold:
    tables = gold.list_aggregations()
    print(f"Tabelas disponíveis: {tables}")
```

## 📝 Script de Exemplo Interativo

Execute o script de demonstração para explorar as agregações:

```bash
python3 example_gold_aggregations.py
```

**Funcionalidades do script:**
1. Criar todas as agregações
2. Visualizar agregações por tipo
3. Visualizar agregações por país (Top 20)
4. Visualizar agregações por estado (Top 20)
5. Visualizar agregações tipo+país (com filtros)
6. Visualizar estatísticas gerais
7. Listar todas as agregações
8. Sair

## 🏗️ Arquitetura

```
Gold Layer
├── Input: Silver Layer (Delta Lake)
│   └── Dados curados e normalizados
│
├── Processing: Spark Aggregations
│   ├── GroupBy operations
│   ├── Distinct counts
│   ├── Conditional sums
│   └── Cross-tabulations
│
└── Output: Delta Lake Tables
    ├── breweries_by_type/
    ├── breweries_by_country/
    ├── breweries_by_state/
    ├── breweries_by_type_and_country/
    ├── breweries_by_type_and_state/
    └── brewery_summary_statistics/
```

## 📈 Métricas de Qualidade de Dados

A Gold Layer calcula métricas de qualidade que ajudam a avaliar a completude dos dados:

- **Completeness Score**: Pontuação de 0-5 baseada na presença de campos essenciais
- **Completeness Rate**: Percentual de registros completos (score >= 4)
- **Coordinate Coverage**: Percentual de registros com latitude/longitude
- **Contact Coverage**: Percentual de registros com telefone ou website

## 🔍 Casos de Uso de Negócio

### Análise de Mercado
```python
# Identificar países com maior concentração de microcervejarias
with GoldLayer() as gold:
    df = gold.read_aggregation("breweries_by_type_and_country",
                               filters={"brewery_type_normalized": "micro"})
    df.orderBy(F.desc("brewery_count")).limit(10).show()
```

### Planejamento de Expansão
```python
# Identificar estados com alta diversidade de tipos
with GoldLayer() as gold:
    df = gold.read_aggregation("breweries_by_state")
    df.filter(F.col("distinct_types") >= 3).orderBy(F.desc("brewery_count")).show()
```

### Monitoramento de Qualidade
```python
# Avaliar completude dos dados
with GoldLayer() as gold:
    df = gold.read_aggregation("brewery_summary_statistics")
    stats = df.collect()[0]
    
    coord_coverage = (stats['with_coordinates'] / stats['total_breweries']) * 100
    print(f"Cobertura de coordenadas: {coord_coverage:.1f}%")
```

## ⚙️ Configurações

As configurações da Gold Layer são gerenciadas pelo módulo `src.config.settings`:

```python
from src.config.settings import Settings

# Caminhos configurados
print(Settings.SILVER_PATH)  # ./lakehouse/silver
print(Settings.GOLD_PATH)    # ./lakehouse/gold
```

## 🧪 Testes

A Gold Layer possui 17 testes automatizados cobrindo:
- Inicialização e context manager
- Leitura de dados Silver
- Cada método de agregação individualmente
- Pipeline completo de criação
- Leitura com filtros
- Listagem de tabelas

Execute os testes:
```bash
pytest tests/test_gold_layer.py -v
```

## 📊 Formato de Saída

Todas as tabelas Gold incluem automaticamente:
- `aggregation_timestamp`: Data/hora da geração da agregação (ISO 8601)
- Formato Delta Lake para suporte a ACID e time travel
- Particionamento otimizado para consultas analíticas

## 🔄 Atualização das Agregações

Para atualizar as agregações com novos dados:

```python
with GoldLayer() as gold:
    # Recriar todas as agregações (sobrescreve)
    metadata = gold.create_aggregations()
```

**Nota:** Por padrão, o modo é `overwrite`, mas pode ser alterado no código para `append` se necessário.

## 📚 Documentação Adicional

- [Bronze Layer](README.md#bronze-layer): Ingestão de dados brutos
- [Silver Layer](README.md#silver-layer): Transformação e curadoria
- [Arquitetura Geral](README.md): Visão geral do projeto
