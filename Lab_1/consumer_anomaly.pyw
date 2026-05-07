from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

print("Nasłuchuję anomalii prędkości (więcej niż 3 transakcje / 60s)...")

for message in consumer:
    data = message.value
    user_id = data["user_id"]
    timestamp = datetime.fromisoformat(data["timestamp"])
    
    user_transactions[user_id].append(timestamp)
    
    user_transactions[user_id] = [
        t for t in user_transactions[user_id]
        if (timestamp - t).total_seconds() <= 60
    ]
    
    if len(user_transactions[user_id]) > 3:
        print(f"ALERT: user {user_id} - więcej niż 3 transakcje w ciągu 60 sekund")
