from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_random_temperature():
    return {
        "sensor_id": random.randint(1, 100),
        "temperature": round(random.uniform(20.0, 30.0), 1)
    }

try:
    while True:
        temperature_reading = generate_random_temperature()
        producer.send('task1-topic', temperature_reading)
        print(f"Sent: {temperature_reading}")
        time.sleep(2)  
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()
