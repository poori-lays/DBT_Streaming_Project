import json
import time
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'raw-questions',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='view-processor-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Starting view processor...")

try:
    for message in consumer:
        questions_data = message.value
        questions = questions_data.get('items', [])
        views = [q.get('view_count', 0) for q in questions]
        producer.send('views', value={"views": views})
        print(f"Processed and sent views: {views}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Shutting down...")

