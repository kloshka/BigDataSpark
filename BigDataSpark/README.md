# Инструкция по запуску и проверке ETL-пайплайна

### 1. Подготовка инфраструктуры
Разверните стек (PostgreSQL, ClickHouse, Spark) с помощью Docker Compose:

```powershell
docker compose up -d
```

Инициализация схем в PostgreSQL происходит автоматически (скрипты в `./sql/init`). Сырые данные загружаются в таблицу `public.mock_data` при старте контейнера.

### 2. Запуск Spark-джоб
Запуск производится через `spark-submit` в контейнере Spark.

**Доступ к среде:** контейнер Spark поднимается вместе со стеком, дополнительных токенов Jupyter здесь не требуется.

**Порядок выполнения джоб:**

* **`etl_to_star.py`**: трансформация из `public.mock_data` в модель «Снежинка/Звезда» (PostgreSQL). Реализована логика построения измерений и fact-таблицы.
* **`star_to_clickhouse.py`**: агрегация данных из PostgreSQL и формирование 6 аналитических витрин в ClickHouse.

Команды запуска:

```powershell
docker exec -it bds2-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3 /opt/spark/work-dir/jobs/etl_to_star.py
```

```powershell
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "CREATE DATABASE IF NOT EXISTS analytics;"
```

```powershell
docker exec -it bds2-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.2.1 /opt/spark/work-dir/jobs/star_to_clickhouse.py
```

### 3. Верификация данных
Для проверки результатов используйте DBeaver или `clickhouse-client` / `psql`:

* **PostgreSQL:** `localhost:5432` (Database: `bds2`) — проверка таблиц фактов и измерений.
* **ClickHouse:** `localhost:8123` (Database: `analytics`) — проверка 6 таблиц `report_*`.

Проверочные запросы:

```powershell
docker exec -it bds2-postgres psql -U bds_user -d bds2 -c "SELECT COUNT(*) FROM public.mock_data;"
docker exec -it bds2-postgres psql -U bds_user -d bds2 -c "SELECT COUNT(*) FROM dwh.fact_sales;"
docker exec -it bds2-postgres psql -U bds_user -d bds2 -c "SELECT COUNT(*) FROM dwh.dim_product;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SHOW TABLES FROM analytics;"
```

```powershell
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_products;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_customers;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_time;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_stores;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_suppliers;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_product_quality;"
```
