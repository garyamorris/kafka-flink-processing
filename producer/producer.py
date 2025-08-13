import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

def create_producer():
    kafka_broker = os.environ.get('KAFKA_BROKER', 'localhost:9092')
    max_retries = 30
    retry_delay = 2
    
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[kafka_broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"Connected to Kafka broker at {kafka_broker}")
            return producer
        except Exception as e:
            print(f"Attempt {i+1}/{max_retries}: Failed to connect to Kafka: {e}")
            time.sleep(retry_delay)
    
    raise Exception("Failed to connect to Kafka after maximum retries")

def main():
    print("Starting Kafka producer...")
    
    # Wait for Kafka to be ready
    time.sleep(10)
    
    producer = create_producer()
    message_count = 0
    
    try:
        while True:
            message_count += 1
            
            # Generate random data
            event = {
                "id": message_count,
                "ts": datetime.now().isoformat(),
                "value": round(random.uniform(0, 100), 2)
            }
            
            # Send to Kafka
            future = producer.send('events', value=event)
            
            try:
                record_metadata = future.get(timeout=10)
                print(f"Sent message {message_count}: {event}")
                print(f"  Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except KafkaError as e:
                print(f"Failed to send message: {e}")
            
            # Wait 200ms before next message
            time.sleep(0.2)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    main()