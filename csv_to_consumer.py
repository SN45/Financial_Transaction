from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'financial_transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Process transactions
if __name__ == "__main__":
    print("Listening for messages...")
    for message in consumer:
        transaction = message.value
        print(f"Received transaction: {transaction}")
        if transaction['amount'] > 500:
            print(f"ALERT: Large transaction detected! {transaction}")