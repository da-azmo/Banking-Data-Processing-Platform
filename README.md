# Banking-Data-Processing-Platform
Production-like pipeline, который ежедневно собирает данные о клиентах и банковских операциях, централизованно хранит их и предоставляет аналитикам данные.

* Банковский домен - [requirements](/docs/requirements.md)
* Архитектура системы - [architecture](/docs/architecture.md)
* Схемы данных (ODS, DDS и DM) - [data_schema](/docs/data_schemas.md)

# Источники
Данные предоставляются из:
* CSV
* JSON
* API
* OLTP (PostgreSQL)

# Технологии
* S3: Minio
* Базы данных: PostgreSQL, ClickHouse
* Броккеры: Kafka
* CDC: Debezium
* Инструменты обработки и трансформации данных: pySpark, dbt
* Оркестраторы: Apache Aiflow
* Инфраструктура: Docker (Kafka, Debezium, S3, ClickHouse)

# Как поднять Docker
* Перейти в папку проекта: *cd banking_data_processing_platform*
* Ввести в консоль: *sudo docker compose up/start -d*

P.S: на данный момент проект всё ещё находится **в разработке**.
Планы на ближайшее время:
* обновить схемы в документации;
* добавить оркестрацию Airflow;
* перейти на использование Avro для сериализации и десериализации CDC-сообщений Debezium вместо JSON при загрузке данных из MinIO в PostgreSQL.
