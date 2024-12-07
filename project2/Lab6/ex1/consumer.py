from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'task1-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  # Começa a ler a partir das novas mensagens
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for messages...")
for message in consumer:
    print(f"Received: {message.value}")
