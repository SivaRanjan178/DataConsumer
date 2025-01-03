import redis
from kafka import KafkaConsumer
import json

# Connect to Redis
redis_client = redis.StrictRedis(
    host='redis.finvedic.in',
    port=6379,
    db=0
)

def consume_from_kafka():
    consumer = KafkaConsumer(
        'market_data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Kafka consumer initialized")  # Debug: Consumer initialized

    for message in consumer:
        print("Received a message from Kafka")  # Debug: Message received
        data = message.value
        print(f"Received data: {data}")  # Debug: Print received data

        try:
            # Use default key and serialize value
            key = data.get('key', f"auto_key_{message.offset}")
            value = json.dumps(data.get('value', data))  # Serialize value to JSON

            # Save to Redis
            redis_client.set(key, value)
            print(f"Data saved to Redis: key={key}, value={value}")  # Debug: Data saved to Redis
        except Exception as e:
            print(f"Failed to save data to Redis: {e}")

if __name__ == "__main__":
    consume_from_kafka()
