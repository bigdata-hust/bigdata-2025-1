from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': "localhost:9092",
    'group.id': "json-consumer-group",
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(["test"])

print("👀 Listening for messages...")
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"❌ Consumer error: {msg.error()}")
        continue
    print(f"📥 Received: {msg.value().decode('utf-8')}")
