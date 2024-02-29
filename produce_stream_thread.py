import requests
from time import sleep
import json
import threading

from kafka import KafkaProducer

# Configuration du producteur Kafka
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "IA"
MASTODON_URL = "https://mastodon.social/api/v1/timelines/tag/IA"


def fetch_data():
    while True:
        response = requests.get(MASTODON_URL)
        if response.status_code == 200:
            data = response.json()
            if data:
                yield data


def produce_message(producer: KafkaProducer):
    while True:
        for data in fetch_data():
            for message in data:
                thread_id: str = message["id"]
                date = message["created_at"]
                message = {'id': thread_id, 'date': date}
                message = json.dumps(message).encode("utf-8")
                producer.send(TOPIC_NAME, value=message, key=thread_id.encode("utf-8"))
            print('message sent', len(data))
            sleep(30)


def debug_produce(producer: KafkaProducer):
    d = fetch_data()
    while True:
        for data in d:
            for message in data:
                thread_id: str = message["id"]
                date = message["created_at"]
                message = {'id': thread_id, 'date': date}
                message = json.dumps(message).encode("utf-8")
                producer.send(TOPIC_NAME, value=message, key=thread_id.encode("utf-8"))
            print('message sent', len(data))
            sleep(10)


if __name__ == "__main__":
    # Démarrage du producteur Kafka
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

    # Démarrage d'un thread pour la production de messages
    thread = threading.Thread(target=debug_produce, args=(producer,))
    thread.start()

    # Attente de la fin du thread
    thread.join()
