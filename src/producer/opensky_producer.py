import requests
from kafka import KafkaProducer
import json
import time
import os

# Environment variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '100'))  # default size

# Kafka Producer definitions
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_PLAINTEXT",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000
)

# OpenSky API data fetch
def fetch_opensky_data():
    try:
        print("Trying to fetch data from OpenSky API...")
        url = "https://opensky-network.org/api/states/all"
        response = requests.get(url, auth=(USERNAME, PASSWORD), timeout=5)
        
        print(f"API Response Status Code: {response.status_code}")
        
        if response.status_code == 429:
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(300)
            return fetch_opensky_data()
        elif response.status_code == 401:
            print("Authentication failed. Please check your credentials.")
            return None
        elif response.status_code != 200:
            print(f"API error with status code: {response.status_code}")
            return None

        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"API error details: {str(e)}")
        return None 


def chunk_data(data, chunk_size=CHUNK_SIZE):
    """Big data to smaller pieces for writing efficiently"""
    all_states = data.get('states', [])
    time_val = data.get('time')
    
    if not all_states:
        return []
    
    chunks = []
    for i in range(0, len(all_states), chunk_size):
        chunk = {
            'time': time_val,
            'states': all_states[i:i+chunk_size]
        }
        chunks.append(chunk)
    
    return chunks


while True:
    data = fetch_opensky_data()
    if data:
        try:
            chunks = chunk_data(data)
            total_chunks = len(chunks)
            
            print(f"Data split into {total_chunks} chunks")
            
            for i, chunk in enumerate(chunks):
                # Send every piece to the Kafka
                chunk_size_bytes = len(json.dumps(chunk).encode('utf-8'))
                print(f"Sending chunk {i+1}/{total_chunks} to Kafka, size: {chunk_size_bytes} bytes")
                
                future = producer.send(KAFKA_TOPIC, chunk)
                record_metadata = future.get(timeout=10)  
                
                print(f"Successfully sent chunk {i+1}/{total_chunks} to Kafka: "
                      f"Topic={record_metadata.topic}, "
                      f"Partition={record_metadata.partition}, "
                      f"Offset={record_metadata.offset}")
            
            producer.flush()
            print(f"All {total_chunks} chunks successfully sent to Kafka")
            
        except Exception as e:
            print(f"Failed to send data to Kafka: {type(e).__name__}: {str(e)}")
            # Error details
            import traceback
            print(traceback.format_exc())
    
    time.sleep(60)