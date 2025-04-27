from confluent_kafka import Consumer, Producer
import json

# Kafka Consumer config
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'view-consumers',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['views'])

# Kafka Producer config
producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

print("Listening to 'views' topic...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        data = json.loads(msg.value().decode('utf-8'))
        views = data.get("views", [])
        avg_views = sum(views) / len(views) if views else 0
        print(f"Average views: {avg_views:.2f}")

        # Send to `views_avg` topic
        producer.produce("views_avg", value=json.dumps({"avg_views": avg_views}))
        producer.flush()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
