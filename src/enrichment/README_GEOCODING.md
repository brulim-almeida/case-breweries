# 🗺️ Geocoding Enrichment - Documentação Completa

## 📚 O que é Geocoding?

**Geocoding** é o processo de converter endereços (texto) em coordenadas geográficas (latitude/longitude).

Exemplo:
- **Entrada**: "123 Main St, Portland, Oregon, USA"
- **Saída**: `latitude: 45.5152, longitude: -122.6784`

## 🎯 Por que fazer isso?

1. **Completude dos dados**: Nem todas as cervejarias têm coordenadas na API
2. **Visualização em mapas**: Precisa de coordenadas para plotar no mapa
3. **Análises geográficas**: Densidade, proximidade, rotas, etc.
4. **Valor de negócio**: Identificar gaps geográficos, saturação de mercado

---

## 🏗️ Arquitetura da Solução

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Silver    │ ---> │  Geocoding   │ ---> │   Silver    │
│  (input)    │      │   Enricher   │      │  (output)   │
│             │      │              │      │             │
│ - sem coords│      │ 1. Identifica│      │ + com coords│
│ - endereços │      │ 2. Chama API │      │ + enriquecida│
└─────────────┘      │ 3. Atualiza  │      └─────────────┘
                     └──────────────┘
                            |
                            v
                    ┌──────────────┐
                    │  Nominatim   │
                    │     API      │
                    │ (OpenStreetMap)│
                    └──────────────┘
```

---

## 🔧 Como Funciona? (Passo a Passo)

### **Passo 1: Identificar Registros sem Coordenadas**

```python
# Filtrar cervejarias sem latitude OU longitude
missing_coords = df.filter(
    F.col("latitude").isNull() | F.col("longitude").isNull()
)
```

**O que acontece:**
- Spark varre todos os registros
- Marca os que têm `latitude` ou `longitude` = NULL
- Retorna apenas esses registros

---

### **Passo 2: Construir Endereço para API**

```python
# Combina componentes do endereço
address_parts = []
if street: address_parts.append(street)
if city: address_parts.append(city)
if state: address_parts.append(state)
if country: address_parts.append(country)

address = ", ".join(address_parts)
# Resultado: "123 Main St, Portland, Oregon, United States"
```

**Por que fazer assim:**
- Nem todo registro tem todos os campos
- API funciona melhor com endereço completo
- Fallback: se falhar, tenta só "cidade, país"

---

### **Passo 3: Chamar API de Geocoding (Nominatim)**

```python
# Request HTTP para Nominatim
response = requests.get(
    "https://nominatim.openstreetmap.org/search",
    params={
        'q': address,
        'format': 'json',
        'limit': 1
    },
    headers={'User-Agent': 'BreweriesDataLake/1.0'}
)

results = response.json()
lat = float(results[0]['lat'])   # 45.5152
lon = float(results[0]['lon'])   # -122.6784
```

**O que acontece:**
1. Envia endereço para API
2. API procura no banco do OpenStreetMap
3. Retorna coordenadas mais próximas
4. Parse do JSON response

**⚠️ Rate Limit:**
- Nominatim permite **1 request/segundo**
- Por isso há `time.sleep(1.1)` entre chamadas
- Se fizer > 1/seg, você é banido temporariamente

---

### **Passo 4: Estratégia de Fallback**

```python
# Tentativa 1: Endereço completo
lat, lon = geocode_address(
    street="123 Main St",
    city="Portland",
    state="Oregon",
    country="United States"
)

# Se falhar...
if not lat:
    # Tentativa 2: Só cidade + país
    lat, lon = geocode_address(
        city="Portland",
        country="United States"
    )
```

**Por que fazer isso:**
- Endereços incompletos/errados são comuns
- Cidade + país quase sempre funciona
- Aumenta taxa de sucesso de ~60% para ~85%

---

### **Passo 5: Join de Resultados**

```python
# Resultados do geocoding em DataFrame
results_df = spark.createDataFrame([
    {'id': '123', 'geocoded_lat': 45.52, 'geocoded_lon': -122.67},
    {'id': '456', 'geocoded_lat': 40.71, 'geocoded_lon': -74.00}
])

# Join com DataFrame original
enriched_df = df.join(results_df, on='id', how='left')

# Atualiza coordenadas onde foram geocodificadas
enriched_df = enriched_df.withColumn(
    'latitude',
    F.when(F.col('geocoded_lat').isNotNull(), F.col('geocoded_lat'))
     .otherwise(F.col('latitude'))  # mantém original se não geocodificou
)
```

**O que acontece:**
1. Resultados viram DataFrame Spark
2. JOIN por `id` (chave única)
3. Atualiza `latitude`/`longitude` apenas onde geocodificou
4. Mantém valores originais nos outros registros

---

### **Passo 6: Atualizar Flag `has_coordinates`**

```python
enriched_df = enriched_df.withColumn(
    'has_coordinates',
    F.when(
        (F.col('latitude').isNotNull()) & 
        (F.col('longitude').isNotNull()),
        True
    ).otherwise(False)
)
```

**Por que:**
- Flag usado para filtros e análises
- Dashboard precisa saber quais têm coordenadas
- Métricas de qualidade de dados

---

## 📊 Estatísticas e Tracking

```python
self.stats = {
    'total_missing': 0,      # Total sem coordenadas
    'geocoded_count': 0,     # Sucesso
    'failed_count': 0,       # Falhas
    'skipped_count': 0       # Pulados
}
```

**Monitoramento:**
- A cada 10 registros, loga progresso
- No final, mostra taxa de sucesso
- Importante para troubleshooting

---

## 🚀 Como Usar

### **Teste Rápido (5 registros)**

```bash
docker exec breweries_data_lake-airflow-worker-1 \\
    python /opt/airflow/src/enrichment/test_geocoding.py
```

**Saída esperada:**
```
🧪 TESTE DE GEOCODING ENRICHMENT
Total de cervejarias: 9,038
Cervejarias sem coordenadas: 543 (6.01%)

📋 Exemplos de cervejarias sem coordenadas:
+-------------------+---------+-------+------------------+
|name               |city     |state  |country_normalized|
+-------------------+---------+-------+------------------+
|Urban Chestnut     |St Louis |Missouri|United States    |
...

🧪 Testando geocoding em 5 exemplos...
✅ Geocoded: Urban Chestnut, St Louis, Missouri, United States → (38.62, -90.19)
...

📊 Estatísticas do teste:
  total_missing: 5
  geocoded_count: 4
  failed_count: 1
  Success rate: 80.00%
```

---

## 🔗 Integração com Silver Layer

Para integrar na pipeline, modifique `silver_layer.py`:

```python
from src.enrichment.geocoding import GeocodeEnricher

class SilverLayer:
    def transform(self, ...):
        # ... transformações existentes ...
        
        # NOVO: Enrich coordinates
        enricher = GeocodeEnricher(self.spark)
        df = enricher.enrich_coordinates(df, max_records=100)
        
        # Continue com o resto...
```

---

## ⚙️ Parâmetros Configuráveis

```python
enricher = GeocodeEnricher(
    spark=spark,
    rate_limit_delay=1.1,     # Segundos entre requests
    timeout=10,                # Timeout do request
    user_agent="MyApp/1.0"    # Identificação
)

enriched_df = enricher.enrich_coordinates(
    df=df,
    max_records=100,           # Limitar para teste
    batch_size=100             # Processar em lotes
)
```

---

## 🎓 Conceitos Importantes

### **1. Rate Limiting**
- APIs gratuitas têm limites
- Nominatim: 1 request/segundo
- Se violar: banimento temporário (1 hora+)
- Solução: `time.sleep()` entre requests

### **2. Fallback Strategy**
- Primeira tentativa: endereço completo
- Segunda tentativa: cidade + país
- Aumenta taxa de sucesso
- Reduz desperdício de requests

### **3. Spark DataFrames vs Pandas**
- Spark: processamento distribuído (grandes volumes)
- Pandas: API calls sequenciais (necessário para rate limit)
- Estratégia: collect para Pandas → geocode → convert back to Spark

### **4. Idempotência**
- Rodar 2x não duplica dados
- JOIN por `id` garante unicidade
- Importante para reprocessamento

---

## 📈 Performance

**Estimativa para 500 registros:**
- 1 request/segundo = 60 requests/minuto
- 500 registros ÷ 60 = ~8.3 minutos
- Taxa de sucesso: ~80%
- Resultado: ~400 coordenadas novas

**Para 5,000 registros:**
- ~83 minutos (~1.4 horas)
- Pode rodar em background na DAG

---

## 🐛 Troubleshooting

### Erro: "429 Too Many Requests"
- **Causa**: Violou rate limit
- **Solução**: Aumentar `rate_limit_delay` para 1.5

### Erro: "Timeout"
- **Causa**: API lenta ou down
- **Solução**: Aumentar `timeout` para 30

### Taxa de sucesso baixa (<50%)
- **Causa**: Endereços ruins/incompletos
- **Solução**: Melhorar limpeza de dados na Silver

---

## 🎯 Próximos Passos

1. ✅ **Testar** o script com 5 registros
2. ⏭️ **Testar** com 100 registros
3. ⏭️ **Integrar** na Silver Layer
4. ⏭️ **Adicionar** task na DAG
5. ⏭️ **Executar** pipeline completa
6. ⏭️ **Verificar** no dashboard

---

**Pronto para testar? Execute o teste agora! 🚀**
