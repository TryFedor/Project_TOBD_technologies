# Аналитика социальных медиа

Проект по дисциплине «Технология обработки больших данных».

## Технологии  
- **Prefect 2.x** — оркестрация ETL-пайплайнов  
- **Dask** — параллельная обработка больших данных  
- **PostgreSQL** — реляционное хранилище аналитики (в Docker)  
- **Plotly** — интерактивная визуализация  
- **Docker** — изолированная инфраструктура

## Структура проекта  
- `data/` — исходные данные (CSV)  
- `flows/` — Prefect Flow с ETL-логикой  
- `dashboards/` — скрипты визуализации  
- `docker-compose.yml` — инфраструктура (Prefect Server + PostgreSQL)

## Запуск (production-режим)

> Требуется: Docker, Docker Compose, Python 3.10+

### 1. Настройка инфраструктуры
```bash
# Остановить и удалить старые контейнеры
docker compose down -v

# Запустить Prefect Server + PostgreSQL в фоне
docker compose up -d
```
### 2. Установка зависимостей
```bash
pip install -r requirements.txt
```
### 3.  Настройка переменных Prefect
```bash
prefect variable set db_type postgres
prefect variable set db_host localhost
prefect variable set raw_data_path data/Social_Media_Users.csv
prefect variable set table_name platform_analytics
```
### 4. Создание пула задач и деплой
```bash
prefect work-pool create "default" --type process
prefect deploy flows/etl_flow.py:etl_comparison_flow -n "social-media-etl" -p "default"
```

### 5. Запуск агента (в новом терминале — только для Windows)
```bash
$env:PYTHONIOENCODING="utf-8"
$env:PYTHONUTF8=1
prefect worker start --pool "default"
```

### 6. Запуск ETL через веб-интерфейс
#### Откройте http://localhost:4200
#### Перейдите в Deployments → social-media-etl
#### Нажмите Run

### 7. Визуализация результатов
```bash
python dashboards/visualize.py
```
