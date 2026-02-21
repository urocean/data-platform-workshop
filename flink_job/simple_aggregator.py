import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

def aggregate_clicks():
    print("Запуск простого агрегатора (без Flink)...")
    
    consumer = KafkaConsumer(
        'clicks',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    window_data = defaultdict(int)
    last_window = time.time()
    
    for message in consumer:
        click = message.value
        product_id = click['product_id']
        window_data[product_id] += 1
        print(f"Получен клик: товар {product_id}")
        
        # Каждые 5 минут выгружаем агрегаты
        current_time = time.time()
        if current_time - last_window >= 300:  # 300 секунд = 5 минут
            print(f"Окно закрыто. Агрегация {len(window_data)} товаров...")
            
            for pid, count in window_data.items():
                result = {
                    'product_id': pid,
                    'count': count,
                    'window_end': int(current_time * 1000)
                }
                producer.send('aggregated_clicks', value=result)
                print(f"Отправлено: товар {pid}, кликов {count}")
            
            window_data.clear()
            last_window = current_time

if __name__ == "__main__":
    try:
        aggregate_clicks()
    except KeyboardInterrupt:
        print("Остановлено")
