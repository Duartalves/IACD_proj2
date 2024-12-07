from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'task2-topic',
    bootstrap_servers='localhost:9092',
    group_id='activity-group',  # Grupo de consumidores
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages...")
for message in consumer:
    print(f"Consumer {consumer.config['client_id']} received: {message.value}")
