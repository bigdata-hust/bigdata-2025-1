from kafka import KafkaProducer
import time
import json
import threading

# Cấu hình Kafka 
producer = KafkaProducer(
    bootstrap_servers = ["172.23.152.231:9092"]
)

# === Callback khi gửi thành công ===
def on_send_success(msg):
    print(f"✅ Delivered to {msg.topic} [{msg.partition}] offset={msg.offset}")

# === Callback khi gửi lỗi ===
def on_send_error(err):
    print(f"❌ Delivery failed: {err}")

# Đọc file JSON và gửi 100 dòng một lần
def stream_file_to_topic(path , topic) :
    with open(path, "r", encoding="utf-8") as f:
        batch = []
        for line in f:
            if line.strip():
                batch.append(json.loads(line.strip()))
            if len(batch) >= 100 :
                #for b in batch : 
                    future = producer.send(topic , key=None, value=bytes(str((json.dumps(batch))) , 'utf-8'))
                    future.add_callback(on_send_success)
                    future.add_errback(on_send_error)
                    print(f"Sent {len(batch)} records to topic {topic}")
                    batch = []
                    time.sleep(1)     # delay 1 giây (mô phỏng streaming)
        if batch :
            future = producer.send(topic , key=None, value=bytes(str((json.dumps(batch))) , 'utf-8'))
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            print(f"Sent {len(batch)} records to topic {topic}")

threads = [
    threading.Thread(target = stream_file_to_topic , args = ("./data/business.json" , "business")) ,
    threading.Thread(target = stream_file_to_topic , args = ("./data/review.json" , "review")) ,
    threading.Thread(target = stream_file_to_topic , args = ("./data/user.json" , "user"))
]

for t in threads :
    t.start() 

for t in threads :
    t.join()

producer.flush()
producer.close()

print("All data sent successfully .")
