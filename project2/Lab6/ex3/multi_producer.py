from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ["user1", "user2", "user3", "user4"]
items = ["book", "pen", "notebook", "tablet"]
activities = ["page_view", "click", "scroll", "search"]

def generate_purchase():
    return {
        "user_id": random.choice(users),
        "amount": round(random.uniform(5.0, 50.0), 2),
        "item": random.choice(items)
    }

def generate_user_activity():
    return {
        "user_id": random.choice(users),
        "activity": random.choice(activities)
    }

try:
    while True:
        # Enviar uma compra para `purchase-topic`
        purchase = generate_purchase()
        producer.send('purchase-topic', purchase)
        print(f"Sent to purchase-topic: {purchase}")

        # Enviar uma atividade de utilizador para `user-activity-topic`
        activity = generate_user_activity()
        producer.send('user-activity-topic', activity)
        print(f"Sent to user-activity-topic: {activity}")

        time.sleep(1)  # Espera de 1 segundo entre cada envio
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()
