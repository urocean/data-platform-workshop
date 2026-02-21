#!/usr/bin/env python3
import json
import time
from kafka import KafkaConsumer
import duckdb
import redis
from datetime import datetime

def run_etl():
    print(f"[{datetime.now()}] Запуск ETL...")
    
    # Читаем из Kafka
    consumer = KafkaConsumer(
        'aggregated_clicks',
        bootstrap_servers='localhost:9092',
        group_id='etl-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    
    aggregates = {}
    for msg in consumer:
        pid = msg.value['product_id']
        cnt = msg.value['count']
        aggregates[pid] = aggregates.get(pid, 0) + cnt
    consumer.close()
    
    if not aggregates:
        print("Нет данных в Kafka")
        return
    
    # Работа с DuckDB
    conn = duckdb.connect('/home/vaosina/data-platform-workshop/data/warehouse.duckdb')
    
    # Создаем тестовые заказы если нет
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            product_id INTEGER,
            quantity INTEGER,
            price FLOAT,
            order_date TIMESTAMP
        )
    """)
    
    # Добавляем тестовые данные
    import random
    conn.execute("DELETE FROM orders")
    for i in range(1, 101):
        conn.execute("INSERT INTO orders VALUES (?, ?, ?, CURRENT_TIMESTAMP)", 
                    (i, random.randint(1, 100), random.uniform(10, 1000)))
    
    # Получаем агрегаты заказов
    orders_data = {}
    result = conn.execute("""
        SELECT product_id, SUM(quantity) as total_orders, AVG(price) as avg_price
        FROM orders GROUP BY product_id
    """).fetchall()
    
    for row in result:
        orders_data[row[0]] = {'total_orders': row[1], 'avg_price': row[2]}
    
    # Объединяем
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
    
    # Запись в Redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.delete('top_products')
    for item in top10:
        r.hset('top_products', str(item['product_id']), json.dumps(item))
    
    # Запись в DuckDB
    conn.execute("CREATE TABLE IF NOT EXISTS top_products AS SELECT * FROM (VALUES (1,1,1,1,1)) WHERE 1=0")
    conn.execute("DELETE FROM top_products")
    for item in top10:
        conn.execute("INSERT INTO top_products VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)", 
                    (item['product_id'], item['click_count'], 
                     item['total_orders'], item['avg_price'], item['score']))
    
    conn.close()
    print(f"[{datetime.now()}] Готово! Записано {len(top10)} товаров")

if __name__ == "__main__":
    run_etl()
