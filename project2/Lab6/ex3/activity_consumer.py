from kafka import KafkaConsumer
import json
from collections import defaultdict

consumer = KafkaConsumer(
    'user-activity-topic',
    bootstrap_servers='localhost:9092',
    group_id='activity-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dicionário para armazenar o número de atividades por utilizador
activity_count = defaultdict(int)

print("Listening for activity messages...")
for message in consumer:
    data = message.value
    user_id = data['user_id']
    activity_count[user_id] += 1
    print(f"User {user_id} has performed {activity_count[user_id]} activities")
