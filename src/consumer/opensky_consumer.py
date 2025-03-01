import os
import json
import time
import logging
from kafka import KafkaConsumer
from google.cloud import bigquery

# Simple logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Environment variables 
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'opensky')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka.kafka.svc.cluster.local:9092')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'user1')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'flight-streaming-project')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'opensky_dataset')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'flights')

def setup_bigquery():
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
    logger.info(f"BigQuery table: {table_id}")
    return client, table_id

def setup_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id="opensky-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=False
    )
    logger.info(f"Kafka connected succesfully: {KAFKA_BROKER}")
    return consumer

def transform_data(data):
    """Transforms OpenSky API data into BigQuery format"""
    # Timestamp
    timestamp = data.get('time', int(time.time()))
    
    # aircraft_list
    aircraft_list = []
    for state in data.get('states', []):
        if not state or len(state) < 17:
            continue
            
        # aircraft info
        aircraft = {
            "icao24": state[0],
            "callsign": state[1].strip() if state[1] else None,
            "origin_country": state[2],
            "longitude": float(state[5]) if state[5] is not None else None,
            "latitude": float(state[6]) if state[6] is not None else None,
            "altitude": float(state[7]) if state[7] is not None else None,
            "velocity": float(state[9]) if state[9] is not None else None,
            "track": float(state[10]) if state[10] is not None else None
        }
        aircraft_list.append(aircraft)
    
    
    record = {
        "timestamp": timestamp,
        "aircraft_count": len(aircraft_list),
        "aircraft": aircraft_list
    }
    
    return record

def main():
    logger.info("OpenSky Consumer starting...")
    
    # Connections
    client, table_id = setup_bigquery()
    consumer = setup_kafka_consumer()
    
    try:
        for message in consumer:
            try:
                # return the message coming from Kafka
                record = transform_data(message.value)
                
                # Write to BigQuery
                errors = client.insert_rows_json(table_id, [record])
                
                if errors:
                    logger.error(f"BigQuery writing error: {errors}")
                else:
                    logger.info(f"Data written to BigQuery: {record['timestamp']} - aircraft count: {record['aircraft_count']}")
                
                
                consumer.commit()
                
            except Exception as e:
                logger.error(f"error: {str(e)}")
                
    
    except KeyboardInterrupt:
        logger.info("Program stopped")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        consumer.close()
        logger.info("Program ended")

if __name__ == "__main__":
    main()