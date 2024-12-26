from flask import Flask, jsonify
from kafka import KafkaConsumer
import redis
import json
import threading

app = Flask(__name__)

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Store consumed messages for response
consumed_messages = []

def process_message(message):
    """
    Processes a Kafka message and inserts it into Redis.
    """
    try:
        # Flatten the nested structure
        if isinstance(message, dict) and 'value' in message:
            data = message['value']  # Extract the nested dictionary
            if isinstance(data, dict) and 'stock_price' in data and 'bid_price' in data:
                # Create a unique Redis key based on stock details
                key = f"market_data:{message.get('key')}"
                redis_client.set(key, json.dumps(data))  # Store the JSON message in Redis
                print(f"Inserted into Redis: {key} -> {data}")
                consumed_messages.append(data)
            else:
                raise ValueError("Invalid nested message format")
        else:
            raise ValueError("Invalid message format")
    except Exception as e:
        print(f"Error processing message: {message}, Error: {e}")

def start_kafka_consumer(topic):
    """
    Consume messages from Kafka, print simulator messages, and insert them into Redis.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='market-data-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserialize JSON messages
    )
    print(f"Connected to Kafka topic '{topic}'. Waiting for messages...")

    for message in consumer:
        try:
            value = message.value  # Extract the message value
            print(f"Received message: {value}")  # Log the received message

            if isinstance(value, dict):
                # Process the message
                process_message(value)

        except Exception as e:
            # Log unexpected message formats or processing errors
            print(f"Unexpected message format or error: {value}, Error: {e}")

@app.route('/consume', methods=['GET'])
def consume():
    """
    Start consuming messages from Kafka and storing them in Redis.
    """
    topic = "market_data"  # Kafka topic name
    consumer_thread = threading.Thread(target=start_kafka_consumer, args=(topic,))
    consumer_thread.start()
    return jsonify({
        "status": "Started consuming Kafka messages and storing them in Redis."
    }), 200

if __name__ == '__main__':
    app.run(debug=True, port=5001)
