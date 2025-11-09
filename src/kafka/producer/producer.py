from confluent_kafka import Producer
import time
import os

# Lấy địa chỉ Kafka từ biến môi trường (dễ cấu hình khi deploy)
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "test")

conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

file_path = os.getenv("DATA_PATH", "merged_data.json")

with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            producer.produce(topic, value=line, callback=delivery_report)
        except BufferError:
            producer.poll(1)
            continue
        producer.poll(0)
        time.sleep(1)

producer.flush()
