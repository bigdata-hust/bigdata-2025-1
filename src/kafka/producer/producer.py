from confluent_kafka import Producer
import time

# Cấu hình Kafka broker
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

topic = "test"

# Hàm callback khi gửi xong
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Đọc file JSON và gửi từng dòng
with open(r"D:\HUAN\PROJECT\Big data\data\unlabeled\merged_data.json", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        producer.produce(topic, key=None, value=line, callback=delivery_report)
        producer.poll(0)  # Xử lý event callback
        time.sleep(1)     # delay 1 giây (mô phỏng streaming)

producer.flush()
