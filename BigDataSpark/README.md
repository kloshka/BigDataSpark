# Отчет по лабораторной работе №2: Реализация ETL-пайплайна в Apache Spark и построение витрин в ClickHouse

## Введение
Данная лабораторная работа является продолжением первой части, где выполнялась нормализация исходных данных. Основная цель текущей работы - вынести трансформации из СУБД во внешний движок Apache Spark и построить аналитические витрины в ClickHouse на основе модели данных звезда в PostgreSQL.

---

## Что было сделано

### 1. Архитектура и инфраструктура
В проекте развернут стек PostgreSQL, Spark и ClickHouse через Docker Compose. PostgreSQL используется как источник сырых данных и хранилище DWH-слоя, Spark выполняет ETL и агрегации, ClickHouse используется как целевая аналитическая СУБД для витрин.

Инициализация структуры и загрузка сырых данных выполняется автоматически при старте PostgreSQL из SQL-скрипта. В исходную таблицу загружаются все 10 CSV-файлов, суммарно 10000 записей.

### 2. Трансформация в модель звезда через PySpark
Реализовано Spark-приложение etl_to_star.py, которое:
- читает данные из public.mock_data в PostgreSQL;
- приводит даты и типы к аналитически корректному виду;
- формирует измерения dwh.dim_customer, dwh.dim_seller, dwh.dim_store, dwh.dim_supplier, dwh.dim_product, dwh.dim_date;
- формирует факт-таблицу dwh.fact_sales с ключами измерений и показателями продаж.

Ключевые инженерные решения:
- использование row_number и dense_rank для дедупликации и стабильного построения ключей измерений;
- применение null-safe join через eqNullSafe для корректного связывания записей с пустыми значениями;
- разделение построения промежуточных сущностей и финального факта для повышения прозрачности пайплайна.

### 3. Формирование аналитических витрин в ClickHouse
Реализовано Spark-приложение star_to_clickhouse.py, которое читает DWH-слой из PostgreSQL и формирует 6 витрин в ClickHouse:

1. Витрина продаж по продуктам
Цель: анализ выручки, объема продаж и популярности продуктов.
- топ-10 самых продаваемых продуктов;
- выручка по категориям;
- рейтинг и количество отзывов по продуктам.

2. Витрина продаж по клиентам
Цель: анализ клиентского поведения.
- топ-10 клиентов по сумме покупок;
- распределение клиентов по странам;
- средний чек по клиентам.

3. Витрина продаж по времени
Цель: анализ динамики во времени.
- месячные и годовые тренды;
- сравнение выручки по периодам;
- средний размер заказа по месяцам.

4. Витрина продаж по магазинам
Цель: анализ эффективности магазинов.
- топ-5 магазинов по выручке;
- распределение продаж по городам и странам;
- средний чек по магазинам.

5. Витрина продаж по поставщикам
Цель: анализ эффективности поставщиков.
- топ-5 поставщиков по выручке;
- средняя цена товаров поставщика;
- распределение продаж по странам поставщиков.

6. Витрина качества продукции
Цель: анализ качества и отзывов.
- продукты с максимальным и минимальным рейтингом;
- корреляция рейтинга и объема продаж;
- продукты с наибольшим числом отзывов.

---

## Результаты
- Полный ETL-цикл автоматизирован двумя Spark-джобами: из сырого слоя в DWH и из DWH в ClickHouse витрины.
- Подтверждена загрузка 10000 строк в raw-слой PostgreSQL и 10000 строк в dwh.fact_sales.
- В ClickHouse создаются все 6 обязательных витрин report_* и заполняются данными.
- Проект воспроизводим: после поднятия контейнеров и запуска двух spark-submit команд получаем готовую аналитическую модель и витрины.

## Вывод
В рамках лабораторной работы реализована полноценная цепочка обработки данных: хранение источника в PostgreSQL, трансформации средствами Apache Spark и публикация аналитических витрин в ClickHouse. Такой подход отделяет транзакционное хранение от аналитической нагрузки и позволяет масштабировать ETL-процесс на большие объемы данных.

---

## Инструкция по запуску

1. Склонировать репозиторий и перейти в папку проекта.

2. Поднять инфраструктуру:

```powershell
docker compose up -d
```

3. Запустить джобу построения модели звезда в PostgreSQL:

```powershell
docker exec -it bds2-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3 /opt/spark/work-dir/jobs/etl_to_star.py
```

4. ClickHouse инициализируется автоматически (база analytics и 6 таблиц витрин создаются init-скриптом в sql/clickhouse-init).

При необходимости можно выполнить команду вручную:

```powershell
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "CREATE DATABASE IF NOT EXISTS analytics;"
```

5. Запустить джобу построения витрин в ClickHouse:

```powershell
docker exec -it bds2-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.postgresql:postgresql:42.7.3,com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.2.1 /opt/spark/work-dir/jobs/star_to_clickhouse.py
```

6. Проверить результат:

```powershell
docker exec -it bds2-postgres psql -U bds_user -d bds2 -c "SELECT COUNT(*) FROM public.mock_data;"
docker exec -it bds2-postgres psql -U bds_user -d bds2 -c "SELECT COUNT(*) FROM dwh.fact_sales;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SHOW TABLES FROM analytics;"
docker exec -it bds2-clickhouse clickhouse-client --user ch_user --password ch_pass --query "SELECT count() FROM analytics.report_sales_products;"
```
