from kafka import KafkaProducer  # type: ignore
import json
import time
import random
import requests

producer=KafkaProducer( bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_transactions():
    URL = "hhttps://www.alphavantage.co/support/#MKVIHIBVPAB8UDYM"  # Use production URL if needed
    api="MKVIHIBVPAB8UDYM"
    headers = {
        "Content-Type": "application/json",
    }
    

    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 200:
        data = response.json()
        return data.get("transactions", [])  # Returns the list of transactions
    else:
        print(f"Error fetching data from Plaid: {response.status_code}, {response.text}")
        return None
    if __name__ == "__main__":
        topic = "fin_transasc"
        printf(f"Producing messages to topic: {topic}")
        while True:
            transac_data=fetch_transactions_data()
            if transac_data:
                for transaction in transac_data:
                    producer.send(topic,transac)
                printf(f"Transaction sent: {transaction}")
                
            time.sleep(10)



    
    

