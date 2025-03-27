# consumer.py
import json
import csv
from kafka import KafkaConsumer

# Setup Kafka Consumer
consumer = KafkaConsumer(
    'f1-driver-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Save to CSV
with open('driver_summary.csv', mode='w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=[
        'year', 'race', 'circuit', 'driver', 'team', 'fastest_lap_time', 'position', 'compound', 'avg_speed'
    ])
    writer.writeheader()
    for message in consumer:
        data = message.value
        print(f"Received: {data}")
        writer.writerow(data)
