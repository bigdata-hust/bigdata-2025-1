from kafka import KafkaProducer
import time
import json
import threading
import os

# === Cấu hình Kafka ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")  
DATA_DIR = os.getenv("DATA_DIR", "/app/data")            

# === Khởi tạo Kafka Producer ===
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(msg):
    print(f"✅ Delivered to {msg.topic} [{msg.partition}] offset={msg.offset}")

def on_send_error(err):
    print(f"❌ Delivery failed: {err}")

# === Hàm đọc file và gửi dữ liệu ===
def stream_file_to_topic(filename, topic):
    file_path = os.path.join(DATA_DIR, filename)
    print(f"Streaming {file_path} -> topic {topic}")

    with open(file_path, "r", encoding="utf-8") as f:
        batch = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            batch.append(json.loads(line))

            if len(batch) >= 100:
                send_batch(batch, topic)
                batch = []
                time.sleep(1)

        if batch:
            send_batch(batch, topic)

def send_batch(batch, topic):
    future = producer.send(topic, batch)
    future.add_callback(on_send_success)
    future.add_errback(on_send_error)
    print(f"Sent {len(batch)} records to topic '{topic}'")


topics = {
    "business.json": "business",
    "review.json": "review",
    "user.json": "user"
}

threads = []
for filename, topic in topics.items():
    t = threading.Thread(target=stream_file_to_topic, args=(filename, topic))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

producer.flush()
producer.close()
print("✅ All data sent successfully.")
