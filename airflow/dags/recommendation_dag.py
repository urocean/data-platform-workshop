from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import duckdb
from kafka import KafkaConsumer, KafkaProducer
import redis

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def read_aggregates_from_kafka(**context):
    consumer = KafkaConsumer(
        'aggregated_clicks',
        bootstrap_servers='data-platform-workshop-kafka-1:9093',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    aggregates = {}
    for msg in consumer:
        pid = msg.value['product_id']
        cnt = msg.value['count']
        aggregates[pid] = aggregates.get(pid, 0) + cnt
    consumer.close()
    context['ti'].xcom_push(key='aggregates', value=aggregates)

def init_duckdb():
    """Создаем тестовые данные в DuckDB"""
    conn = duckdb.connect('/home/vaosina/data-platform-workshop/data/warehouse.duckdb')
    
    # Создаем таблицу заказов
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price FLOAT,
            order_date TIMESTAMP
        )
    """)
    
    # Добавляем тестовые данные (100 товаров с разными продажами)
    conn.execute("DELETE FROM orders")
    
    import random
    for i in range(1, 101):
        conn.execute("""
            INSERT INTO orders VALUES 
                (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (i, i, random.randint(1, 100), random.uniform(10, 1000)))
    
    print(f"Добавлено 100 тестовых заказов в DuckDB")
    conn.close()

def join_with_orders(**context):
    aggregates = context['ti'].xcom_pull(key='aggregates', task_ids='read_kafka')
    if not aggregates:
        aggregates = {}
    
    # Инициализируем DuckDB с тестовыми данными
    init_duckdb()
    
    # Подключаемся к DuckDB
    conn = duckdb.connect('/home/vaosina/data-platform-workshop/data/warehouse.duckdb')
    
    # Получаем агрегаты заказов
    orders_df = conn.execute("""
        SELECT 
            product_id, 
            SUM(quantity) as total_orders, 
            AVG(price) as avg_price
        FROM orders 
        GROUP BY product_id
    """).fetchall()
    
    orders_data = {row[0]: {'total_orders': row[1], 'avg_price': row[2]} for row in orders_df}
    
    # Объединение
    final = []
    for pid, click_count in aggregates.items():
        orders = orders_data.get(pid, {'total_orders': 0, 'avg_price': 0})
        final.append({
            'product_id': pid,
            'click_count': click_count,
            'total_orders': orders['total_orders'],
            'avg_price': orders['avg_price'],
            'score': click_count * 0.7 + orders['total_orders'] * 0.3
        })
    
    final.sort(key=lambda x: x['score'], reverse=True)
    top10 = final[:10]
    
    conn.close()
    context['ti'].xcom_push(key='top10', value=top10)

def write_to_redis_bigquery(**context):
    top10 = context['ti'].xcom_pull(key='top10', task_ids='join_orders')
    
    # Запись в Redis
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    redis_client.delete('top_products')
    for item in top10:
        redis_client.hset('top_products', str(item['product_id']), json.dumps(item))
    
    # Запись в DuckDB (вместо BigQuery)
    conn = duckdb.connect('/home/vaosina/data-platform-workshop/data/warehouse.duckdb')
    
    # Создаем таблицу для топ-продуктов
    conn.execute("""
        CREATE TABLE IF NOT EXISTS top_products (
            product_id INTEGER,
            click_count INTEGER,
            total_orders INTEGER,
            avg_price FLOAT,
            score FLOAT,
            updated_at TIMESTAMP
        )
    """)
    
    # Очищаем старые записи
    conn.execute("DELETE FROM top_products")
    
    # Вставляем новые
    for item in top10:
        conn.execute("""
            INSERT INTO top_products VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (item['product_id'], item['click_count'], 
              item['total_orders'], item['avg_price'], item['score']))
    
    conn.close()
    print(f"Записано {len(top10)} товаров в DuckDB и Redis")

with DAG(
    'recommendation_pipeline',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['example'],
) as dag:
    t1 = PythonOperator(
        task_id='read_kafka',
        python_callable=read_aggregates_from_kafka,
    )
    t2 = PythonOperator(
        task_id='join_orders',
        python_callable=join_with_orders,
    )
    t3 = PythonOperator(
        task_id='write_targets',
        python_callable=write_to_redis_bigquery,
    )
    t1 >> t2 >> t3
