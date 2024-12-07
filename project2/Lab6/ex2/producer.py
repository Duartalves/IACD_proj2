from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ["user1", "user2", "user3", "user4", "user5"]
activities = ["login", "logout", "purchase", "view", "search"]

def generate_user_activity():
    return {
        "user_id": random.choice(users),
        "activity": random.choice(activities)
    }

try:
    while True:
        user_activity = generate_user_activity()
        producer.send('task2-topic', user_activity)
        print(f"Sent: {user_activity}")
        time.sleep(1)  # Enviar uma mensagem de atividade de utilizador a cada segundo
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()
