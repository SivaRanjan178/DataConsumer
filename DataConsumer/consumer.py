from kafka import KafkaConsumer
from redis import StrictRedis
import json

# Connect to Redis
redis_client = StrictRedis(
    host='localhost',
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

        if 'key' in data and 'value' in data:
            redis_client.set(data['key'], data['value'])
            print(f"Data saved to Redis: key={data['key']}, value={data['value']}")  # Debug: Data saved to Redis
        else:
            print("Received data does not contain 'key' and 'value' fields")

if __name__ == "__main__":
    consume_from_kafka()