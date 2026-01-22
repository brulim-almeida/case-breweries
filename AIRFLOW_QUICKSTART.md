# 🚀 Quick Start - Airflow

## Como Usar o Airflow

### Opção 1: Teste Rápido (Sem Docker)
```bash
# Teste o pipeline completo localmente
python3 airflow/test_pipeline.py
```

### Opção 2: Airflow Completo (Docker)

#### 1. Iniciar o Ambiente
```bash
# Subir todos os serviços
docker-compose up -d

# Ver logs
docker-compose logs -f

# Verificar status
docker-compose ps
```

#### 2. Acessar a UI
- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

#### 3. Executar a DAG
1. Na UI, encontre `breweries_data_pipeline`
2. Toggle ON para ativar
3. Clique em "Trigger DAG" para executar manualmente

#### 4. Monitorar
```bash
# Logs do scheduler
docker-compose logs -f airflow-scheduler

# Logs do worker
docker-compose logs -f airflow-worker

# Flower (monitoring)
# http://localhost:5555
```

#### 5. Parar o Ambiente
```bash
# Parar todos os serviços
docker-compose down

# Parar e remover volumes (limpar tudo)
docker-compose down -v
```

## 📋 Serviços Disponíveis

| Serviço | Porta | URL | Descrição |
|---------|-------|-----|-----------|
| **Airflow UI** | 8080 | http://localhost:8080 | Interface web principal |
| **Flower** | 5555 | http://localhost:5555 | Monitoring Celery workers |
| **PostgreSQL** | 5432 | localhost:5432 | Metadata database |

## 🔧 Comandos Úteis

```bash
# Listar DAGs
docker-compose exec airflow-scheduler airflow dags list

# Trigger manual
docker-compose exec airflow-scheduler airflow dags trigger breweries_data_pipeline

# Ver logs de uma task
docker-compose exec airflow-scheduler airflow tasks logs breweries_data_pipeline bronze_ingestion <date>

# Acessar shell
docker-compose exec airflow-scheduler bash

# Reiniciar apenas o scheduler
docker-compose restart airflow-scheduler
```

## 📚 Documentação Completa

Para mais detalhes, veja:
- **Airflow**: [airflow/README.md](airflow/README.md)
- **Pipeline**: [README.md](README.md)

## ⚠️ Troubleshooting

**DAG não aparece:**
```bash
# Verificar erros no scheduler
docker-compose logs airflow-scheduler | grep ERROR

# Validar sintaxe da DAG
python3 airflow/validate_dags.py
```

**Problemas de permissão:**
```bash
# Ajustar AIRFLOW_UID (Linux)
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose down -v
docker-compose up -d
```

**Limpar e reiniciar:**
```bash
docker-compose down -v
docker-compose up -d
```
