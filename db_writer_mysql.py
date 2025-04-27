from confluent_kafka import Consumer
import json
import mysql.connector

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="shreyas",
    password="#shreyas1507",
    database="dbt"
)
cursor = conn.cursor()

# Kafka Consumer config
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mysql-writer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['views_avg'])

print("⏳ Listening to 'views_avg' topic and inserting into MySQL...")

try:
    slno = 1  # Manual counter since your table requires Slno

    # Get the latest Slno from the table to avoid duplicate primary keys
    cursor.execute("SELECT MAX(Slno) FROM Avg_Views;")
    result = cursor.fetchone()
    if result and result[0]:
        slno = result[0] + 1

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Consumer error:", msg.error())
            continue

        data = json.loads(msg.value().decode('utf-8'))
        avg_views = int(data.get("avg_views", 0))

        cursor.execute(
            "INSERT INTO Avg_Views (Slno, view_count) VALUES (%s, %s);",
            (slno, avg_views)
        )
        conn.commit()
        print(f"✅ Inserted [Slno={slno}, view_count={avg_views}]")
        slno += 1

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    cursor.close()
    conn.close()
