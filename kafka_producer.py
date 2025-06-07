import pandas as pd
import time
import random
from kafka import KafkaProducer
import json

# Đọc dữ liệu CSV
df = pd.read_csv("data.csv")

# Khởi tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Gửi từng dòng đến Kafka topic
for index, row in df.iterrows():
    message = row.to_dict()
    print(f"Gửi: {message}")
    producer.send("credit-card-transactions", value=message)
    time.sleep(random.uniform(1, 3))

producer.flush()
