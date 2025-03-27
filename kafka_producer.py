# producer.py
import json
from kafka import KafkaProducer
import fastf1
from fastf1 import plotting

# Setup Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Enable cache
fastf1.Cache.enable_cache('cache')

# Fetch driver data for last 5 seasons
for year in range(2019, 2024):
    schedule = fastf1.get_event_schedule(year)
    for rnd in schedule.index:
        try:
            session = fastf1.get_session(year, rnd, 'R')
            session.load()
            for drv in session.drivers:
                drv_data = session.laps.pick_driver(drv).pick_fastest()
                data = {
                    'year': year,
                    'race': session.event['EventName'],
                    'circuit': session.event['Location'],
                    'driver': drv,
                    'team': session.get_driver(drv)['TeamName'],
                    'fastest_lap_time': str(drv_data['LapTime']),
                    'position': int(drv_data['Position']),
                    'compound': str(drv_data['Compound']),
                    'avg_speed': drv_data['SpeedI1'],
                }
                producer.send('f1-driver-data', value=data)
                print(f"Sent: {data}")
        except Exception as e:
            print(f"Skipping {year} Round {rnd} due to error: {e}")

producer.flush()
