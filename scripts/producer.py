from kafka import KafkaProducer
import re
import csv
import json
import time
from datetime import datetime


brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
producer = KafkaProducer(
    bootstrap_servers=brokers,
)

topicName = 'stream_sample_data_topic'

with open("../data/u.data", "rb") as file:
    for i in file:
        row = i.decode()
        data = re.split('\t|\n', row)[:5]
        data[4] = datetime.fromtimestamp(int(data[4]))\
            .strftime('%Y-%m-%d %H:%M:%S')
        producer.send(topicName, json.dumps(data).encode('utf-8'))
        print(data)
        time.sleep(1)
