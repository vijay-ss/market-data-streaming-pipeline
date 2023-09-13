import os
import json
import yaml
from kafka import KafkaProducer


def load_env_variables():
    try:
        with open('.env.yml') as file:
            payload = yaml.safe_load(file)
        for item in payload:
            os.environ[item] = payload.get(item)           
        print("Loaded environment variables.")
    except Exception as error:
        print(error)


def load_producer(kafka_server: str, kafka_port: str):

    return KafkaProducer(
            bootstrap_servers=[f'{kafka_server}:{kafka_port}'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )

def on_send_success(record_metadata):
    success_message = (
        "Successfully sent message to broker. "
        f"topic: {record_metadata.topic}, "
        f"partition: {record_metadata.partition}, "
        f"message offset: {record_metadata.offset}, "
        f"timestamp: {record_metadata.timestamp}"
    )
    print(success_message)
    

def on_send_error(excp):
    print('Error sending message.', exc_info=excp)