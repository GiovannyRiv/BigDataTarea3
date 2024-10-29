import time
import json
import random
from kafka import KafkaProducer
def generate_sensor_data():
        return {
                "sensor_id": random.randint(1, 10),
                "carril_escolar": round(random.uniform(10, 20), 0),
                "servicio_publico": round(random.uniform(30, 70), 0),
                "mixto": round(random.uniform(30,79),0),
                "timestamp": int(time.time())
        }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
value_serializer=lambda x: json.dumps(x).encode('utf-8'))
while True:
        sensor_data = generate_sensor_data()
        producer.send('sensor_data', value=sensor_data)
        print(f"Sent: {sensor_data}")
        time.sleep(1)
