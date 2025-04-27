import json
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

def fetch_latest_questions(site="stackoverflow", fromdate=None):
    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "site": site,
        "sort": "creation",
        "order": "desc",
        "pagesize": 15,
        "filter": "default"
    }
    if fromdate:
        params["fromdate"] = fromdate

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("items", [])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StackOverflowFetcher") \
        .getOrCreate()

    last_time = int(time.time()) - 60 * 15  # 15 minutes ago
    polling_interval = 60 * 15

    while True:
        print("Fetching new questions...")
        items = fetch_latest_questions(fromdate=last_time)
        last_time = int(time.time())

        if not items:
            print("No new questions found.")
            time.sleep(polling_interval)
            continue

        # Wrap in the same format your original code used
        wrapped_data = json.dumps({"items": items})

        # Create a DataFrame with a single row and a 'value' column
        df = spark.createDataFrame([(wrapped_data,)], ["value"])

        # Write to Kafka
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "raw-questions") \
            .save()

        print(f"Sent {len(items)} questions to Kafka topic 'raw-questions'")
        time.sleep(polling_interval)
ss
