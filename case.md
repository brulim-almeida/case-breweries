# BEES Data Engineering – Breweries Case (Resumo do PDF)

Você trabalha como **Data Engineer** em uma grande empresa de bebidas, e sua missão é construir um **Data Lake resiliente e escalável do zero**. Para isso, você deve consumir dados da **Open Brewery DB API**, que fornece informações sobre cervejarias e pubs ao redor do mundo.

---

## 1) Arquitetura (Solution Design)
Descrever como você desenharia a arquitetura de dados.  
Entregar um blueprint/diagrama ou documentação explicando:
- principais componentes
- fluxo de dados (data flow)

---

## 2) Data Lake Architecture (Medallion)
O Data Lake deve seguir arquitetura Medallion com **Bronze / Silver / Gold**:

### a) Bronze Layer (Raw)
- Persistir os dados brutos vindos da API
- Manter em formato nativo ou outro formato adequado

### b) Silver Layer (Curated)
- Transformar para formato colunar (**parquet** ou **delta**)
- Particionar por **brewery location**
- Explicar outras transformações realizadas

### c) Gold Layer (Business / Aggregated)
- Criar uma visão agregada com:
  - **quantidade de breweries por type e location**

---

## 3) Linguagem
- Pode usar a linguagem de sua preferência para:
  - requisições (consumo da API)
  - transformação de dados
- Incluir **test cases**
- Preferência: **Python e PySpark**

---

## 4) Orquestração
Escolher uma ferramenta de orquestração (ex: Airflow, Luigi, Mage etc.) para:
- agendamento contínuo (scheduling)
- retries
- error handling
- monitoramento da pipeline

---

## 5) Data Quality
Descrever como desenharia e implementaria checagens de qualidade de dados, incluindo:
- técnicas e regras aplicadas
- dimensões de qualidade garantidas (ex: completude, consistência, unicidade, validade etc.)

---

## 6) Observability / Alerts
Descrever como desenharia monitoramento e alertas para garantir confiabilidade, incluindo:
- rastreamento de performance
- detecção e diagnóstico de falhas
- notificação de stakeholders em incidentes

---

## Extras (Opcional)
### a) Deployment
Se possível, implementar deployment/automação usando:
- Docker
- Kubernetes

---

## Entrega
### Repositório
Criar repositório público no GitHub contendo:
- código
- documentação de decisões de design
- trade-offs
- instruções de execução

### Cloud Services (se usar)
- Instruir como configurar
- **não publicar credenciais no repo público**

---

## Critérios de Avaliação
1. Solution Design  
2. Efficiency  
3. Code Quality  
4. Completeness  
5. Documentation  
6. Error Handling  

---

## Prazo
Finalizar em **1 semana** e compartilhar o link do GitHub.

---

Fonte: PDF "Technical_Case_Data_Engineer_VII.pdf"
