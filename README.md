# de_project

## Требования
- Docker Desktop (с включённым Docker Compose)
- Git

---

## Быстрый старт (рекомендуется): восстановление из дампа

### 1)  Склонировать репозиторий
```bash
git clone https://github.com/avgcring3/Project_ITDE.git
cd Project_ITDE

```
### 2) Собрать и запустить проект
```bash
docker compose up --build
```
### 3) Проверить, что контейнеры запущены
```bash
docker compose ps
```
### 4) Открыть интерфейсы
Airflow → http://localhost:8080
pgAdmin → http://localhost:5050

pgAdmin логин:
email: admin@example.com
password: admin

airflow логин:
email: airflow
password: airflow

### 5) Запустить витрины (обязательно)
```bash
docker compose exec airflow airflow dags test metrics_upd 2025-12-10
```
### 6) Проверить, что витрины создались
```bash
docker compose exec postgres psql -U de_user -d de_db -c "
SELECT 'product' mart, COUNT(*) rows
FROM dwh.product_performance_data_mart
WHERE load_date = DATE '2025-12-10'
UNION ALL
SELECT 'order' mart, COUNT(*) rows
FROM dwh.order_performance_data_mart
WHERE load_date = DATE '2025-12-10';
"
```
### 7) Если что то пошло не так*

```bash
docker compose down -v
docker compose up --build
```

