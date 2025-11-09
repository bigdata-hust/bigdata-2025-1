from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import time

# Lấy cấu hình từ biến môi trường (dễ dùng khi chạy docker-compose)
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "test")
group_id = os.getenv("KAFKA_GROUP_ID", "json-consumer-group")

conf = {
    "bootstrap.servers": bootstrap_servers,
    "group.id": group_id,
    "auto.offset.reset": "earliest",  # đọc từ đầu nếu chưa có offset
}

consumer = Consumer(conf)
consumer.subscribe([topic])

print(f"Consumer connected to Kafka at {bootstrap_servers}")
print(f"Subscribed to topic: {topic}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            # Khi Kafka broker ngắt tạm thời, đợi reconnect
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            elif msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

        value = msg.value().decode("utf-8")
        print(f"eceived message: {value}")

except KeyboardInterrupt:
    print("Stopped by user")

except KafkaException as e:
    print(f"Kafka exception: {e}")

finally:
    consumer.close()
    print("Consumer closed cleanly")
