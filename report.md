# Отчет по проекту "Разработка прототипа платформы данных для интеллектуальной рекомендательной системы"

**Студент:** Осина Виктория  
**Дата:** 21 февраля 2026  
**ОС:** Rocky Linux 9.5  
**GitHub:** https://github.com/urocean/data-platform-workshop

---

## 1. Облачное хранилище

**Требование:** Создайте датасет в Google BigQuery. Спроектируйте схему сырых данных (raw_clicks, raw_orders) и витрины (top_products).

**Решение:** В связи с отсутствием доступа к платному аккаунту Google Cloud Platform, работа с BigQuery была заменена на DuckDB - легковесную встраиваемую базу данных, полностью имитирующую функциональность BigQuery для учебных целей. Все SQL-запросы и трансформации сохранены.

### 1.1 Установка DuckDB

![Установка DuckDB](Screenshot%20from%202026-02-21%2016-33-33.png)

### 1.2 Создание таблиц

![Создание таблиц в DuckDB](Screenshot%20from%202026-02-21%2018-33-04.png)

### 1.3 Схемы созданных таблиц

**Таблица `raw_clicks`** (сырые данные о кликах):
- `product_id` - идентификатор товара
- `click_time` - время клика

**Таблица `raw_orders`** (исторические заказы):
- `product_id` - идентификатор товара
- `quantity` - количество заказанных единиц
- `price` - цена товара

**Таблица `top_products`** (витрина топ-товаров):
- `product_id` - идентификатор товара
- `score` - рейтинг товара

### 1.4 Результат работы ETL

![Результат ETL](Screenshot%20from%202026-02-21%2019-09-43.png)

### 1.5 Данные в Redis

![Данные в Redis](Screenshot%20from%202026-02-21%2019-11-02.png)


## 2. Потоковая обработка

**Требование:** Разверните Kafka (в Docker) и запустите генератор тестовых кликов (Python-скрипт). Реализуйте оконную агрегацию: для каждого товара подсчитывайте количество кликов за последние 5 минут. Результат агрегации публикуйте в отдельный топик Kafka.

---

### 2.1 Развертывание Kafka в Docker

![Запущенные контейнеры](Screenshot%20from%202026-02-21%2012-11-08.png)

*На скриншоте видно, что контейнеры Kafka (версия 6.2.0), Redis, Zookeeper и Airflow успешно запущены.*

---

### 2.2 Создание топиков Kafka

![Создание топиков](Screenshot%20from%202026-02-21%2012-16-32.png)

*Созданы топики `clicks` для входящих событий и `aggregated_clicks` для агрегированных данных.*

---

### 2.3 Генератор тестовых кликов

![Генератор кликов](Screenshot%20from%202026-02-21%2012-17-47.png)

*Установлена библиотека kafka-python и запущен генератор, который отправляет события с product_id, категорией, timestamp и user_id.*

---

### 2.4 Подготовка к Flink (JAR-коннектор)

![JAR коннектор](Screenshot%20from%202026-02-21%2012-34-42.png)

*Для работы PyFlink с Kafka скачан JAR-файл коннектора версии 3.0.1-1.18.*

---

### 2.5 Реализация агрегатора

В связи со сложностями настройки PyFlink, был реализован упрощенный агрегатор на чистом Python, выполняющий ту же логику оконной агрегации.

![Код агрегатора](Screenshot%20from%202026-02-21%2012-59-39.png)

*Агрегатор читает сообщения из топика `clicks`, накапливает их в 5-минутные окна и публикует результаты в топик `aggregated_clicks`.*

---

### 2.6 Запуск агрегатора

![Запуск агрегатора](Screenshot%20from%202026-02-21%2013-00-03.png)

*Агрегатор успешно запущен и обрабатывает входящие клики.*

---

### 2.7 Результаты агрегации

![Агрегированные данные](Screenshot%20from%202026-02-21%2018-52-53.png)

*В топике `aggregated_clicks` присутствуют сообщения с агрегированными данными за 5-минутные окна, например: `{"product_id": 84, "count": 10, "window_end": 1771688771215}`.*

## 3. Оркестрация пакетной части

**Требование:** Напишите DAG в Apache Airflow, который запускается каждые 10 минут и выполняет:
- Чтение агрегатов из Kafka
- Джойн с историческими заказами из BigQuery (таблица orders)
- Запись финальных записей в BigQuery (витрина) и в Redis
- DAG должен учитывать возможные сбои и уметь перезапускаться

---

### 3.1 Запуск Airflow и инициализация базы данных

![Запуск Airflow](Screenshot%20from%202026-02-21%2013-39-46.png)

*Выполнен ручной запуск контейнера Airflow с инициализацией базы данных через `airflow db init`.*

---

### 3.2 Веб-интерфейс Airflow

![Вход в Airflow](Screenshot%20from%202026-02-21%2013-44-09.png)

*Веб-интерфейс Airflow доступен по адресу http://localhost:8080, вход выполнен под учетной записью admin.*

---

### 3.3 DAG recommendation_pipeline

![DAG в Airflow](Screenshot%20from%202026-02-21%2016-10-04.png)

*DAG `recommendation_pipeline` загружен и отображается в списке. Владелец: student, расписание: каждые 10 минут, последний запуск: 12:50.*

---

### 3.4 Результат работы ETL

![Результат ETL](Screenshot%20from%202026-02-21%2019-09-43.png)

*ETL-скрипт успешно выполнен: прочитаны данные из Kafka, выполнен джойн с заказами, записано 10 товаров.*

---

### 3.5 Данные в Redis

![Данные в Redis](Screenshot%20from%202026-02-21%2019-11-02.png)

*В Redis появились ключи с ID товаров и значениями в формате JSON, содержащими click_count, total_orders, avg_price и score.*

Пример записи:
```
"40" -> {"product_id": 40, "click_count": 13, "total_orders": 91, "avg_price": 712.03, "score": 36.4}
```

---

### 3.6 Обработка сбоев

В DAG настроены повторные попытки в случае сбоев:
```python
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
```
## 4. Data Mesh (доменный data product)

**Требование:** Выделите один data product, например, «Данные кликов домена clickstream». Оформите его как отдельный набор данных в BigQuery, добавьте описание (теги, владелец, SLA). В репозитории создайте структуру папок, соответствующую доменам.

---

### 4.1 Создание структуры папок для домена

![Data Mesh структура](Screenshot%20from%202026-02-21%2019-13-47.png)

*Создана папка `domains/clickstream/` в корне проекта.*

---

### 4.2 Описание data product

**Файл `domains/clickstream/README.md`:**

```
# Data Product: Clickstream

| Параметр | Значение |
|----------|----------|
| Владелец | Студент |
| SLA | 99.9% |
| Теги | clicks, user-activity, realtime |
| Дата создания | 2026-02-21 |

## Описание
Данные о кликах пользователей в реальном времени.
Содержит события кликов по товарам с метаданными.

## Схема
- event_id: уникальный идентификатор события
- product_id: ID товара
- category: категория товара
- timestamp: время клика
- user_id: ID пользователя
- session_id: ID сессии
```

---

### 4.3 Структура в репозитории

![Репозиторий с файлами](Screenshot%20from%202026-02-21%2019-29-56.png)

*В репозитории присутствует папка `domains/clickstream` с файлом README.md, что соответствует требованиям Data Mesh.*
## 5. Семантический слой / Feature Store

**Требование:** Используйте dbt для создания моделей, которые преобразуют сырые данные в бизнес-показатели: Модель popular_products (считает скользящую популярность на основе кликов), Модель orders_facts (агрегирует заказы по товарам).

---

### 5.1 Структура dbt-проекта

![Структура dbt](Screenshot%20from%202026-02-21%2011-45-01.png)

*В проекте создана папка `dbt/` с необходимыми поддиректориями `models/`.*

---

### 5.2 Конфигурация dbt-проекта

**Файл `dbt/dbt_project.yml`:**
```yaml
name: 'recommendations'
version: '1.0.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  recommendations:
    +materialized: table
    +schema: dwh
```

---

### 5.3 Описание источников и моделей

**Файл `dbt/models/schema.yml`:**
```yaml
version: 2

sources:
  - name: raw
    database: your-project
    schema: raw
    tables:
      - name: clicks
      - name: orders

models:
  - name: popular_products
    description: "Агрегация кликов по товарам за последний час"
    columns:
      - name: product_id
        tests:
          - not_null
      - name: click_count
  - name: top_products
    description: "Итоговая витрина с объединением кликов и заказов"
    columns:
      - name: product_id
      - name: score
```

---

### 5.4 Модель popular_products

**Файл `dbt/models/popular_products.sql`:**
```sql
{{ config(materialized='table') }}

SELECT
    product_id,
    COUNT(*) AS click_count,
    MAX(timestamp) AS last_click
FROM {{ source('raw', 'clicks') }}
WHERE timestamp >= UNIX_MILLIS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
GROUP BY product_id
```

---

### 5.5 Модель top_products

**Файл `dbt/models/top_products.sql`:**
```sql
{{ config(materialized='table') }}

WITH orders_agg AS (
    SELECT
        product_id,
        SUM(quantity) AS total_orders,
        AVG(price) AS avg_price
    FROM {{ source('raw', 'orders') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.click_count,
    o.total_orders,
    o.avg_price,
    (p.click_count * 0.7 + o.total_orders * 0.3) AS score
FROM {{ ref('popular_products') }} p
LEFT JOIN orders_agg o ON p.product_id = o.product_id
ORDER BY score DESC
LIMIT 10
```

---

### 5.6 Интеграция с DuckDB

Для работы dbt с локальным хранилищем был установлен адаптер dbt-duckdb:

![Установка dbt-duckdb](Screenshot%20from%202026-02-21%2016-33-33.png)

## 6. CI/CD

**Требование:** Настройте репозиторий на GitHub. Напишите конфигурацию Terraform для создания облачных ресурсов (датасет BigQuery, bucket для хранения логов). В GitHub Actions создайте workflow, который при пуше в main запускает линтеры (Flake8 для Python, SQLFluff для SQL), выполняет terraform plan и (при мерже) terraform apply.

---

### 6.1 Создание репозитория на GitHub

![Создание репозитория](Screenshot%20from%202026-02-21%2019-16-04.png)

*Создан репозиторий `urocean/data-platform-workshop` с поддержкой HTTPS и SSH.*

---

### 6.2 Репозиторий с файлами проекта

![Репозиторий с файлами](Screenshot%20from%202026-02-21%2019-29-56.png)

*Все файлы проекта успешно загружены в репозиторий. Статистика языков: Python 91.8%, HCL 8.2%.*

---

### 6.3 Конфигурация Terraform

**Файл `terraform/main.tf`:**
```hcl
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.region
  description = "Сырые данные"
}

resource "google_bigquery_dataset" "dwh" {
  dataset_id = "dwh"
  location   = var.region
  description = "Витрины данных"
}

resource "google_bigquery_table" "orders" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "orders"
  schema = <<EOF
[
  {
    "name": "order_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "product_id",
    "type": "INTEGER",
    "mode": "REQUIRED"
  },
  {
    "name": "quantity",
    "type": "INTEGER"
  },
  {
    "name": "price",
    "type": "FLOAT"
  },
  {
    "name": "order_date",
    "type": "TIMESTAMP"
  }
]
EOF
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "EU"
}
```

---

### 6.4 Конфигурация GitHub Actions

**Файл `.github/workflows/ci.yml`:**
```yaml
name: CI Pipeline

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  lint-and-validate:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        pip install flake8 black
        pip install -r requirements.txt
    
    - name: Lint Python with flake8
      run: flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
    
    - name: Lint SQL with sqlfluff
      run: |
        pip install sqlfluff
        sqlfluff lint dbt/models/ --dialect bigquery
    
    - name: Terraform Init
      run: |
        cd terraform
        terraform init
    
    - name: Terraform Plan
      run: |
        cd terraform
        terraform plan -var-file="variables.tfvars"
      env:
        GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY }}
```

---

### 6.5 Структура GitHub Actions в репозитории

![GitHub структура](Screenshot%20from%202026-02-21%2019-29-56.png)

*В репозитории присутствует папка `.github/workflows/` с файлом `ci.yml`.*

---

### 6.6 Примечание

В связи с отсутствием доступа к платному аккаунту Google Cloud Platform, фактическое выполнение `terraform apply` не производилось. Однако конфигурационные файлы полностью подготовлены и при наличии сервисного аккаунта и секретов в GitHub могут быть использованы для развертывания инфраструктуры.


