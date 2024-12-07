from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'purchase-topic',
    bootstrap_servers='localhost:9092',
    group_id='purchase-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dicionário para armazenar o total de compras por utilizador
total_spent = defaultdict(float)

print("Listening for purchase messages...")
for message in consumer:
    data = message.value
    user_id = data['user_id']
    amount = data['amount']
    total_spent[user_id] += amount
    print(f"User {user_id} has spent a total of: {total_spent[user_id]}")
