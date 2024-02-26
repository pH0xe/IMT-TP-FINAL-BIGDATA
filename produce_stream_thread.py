import threading
import time

from kafka import KafkaProducer

# Configuration du producteur Kafka
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "IA"
MASTODON_URL = "https://mastodon.social/api/v1/timelines/tag/IA"

import json
from datetime import datetime
from time import sleep

# On fetch les données de Mastodon
import requests


def fetch_data():
    while True:
        response = requests.get(MASTODON_URL)
        if response.status_code == 200:
            data = response.json()
            if data:
                last_id = data[0]["id"]
                yield data

# Fonction pour produire un message Kafka
def produce_message(producer):
    while True:
      print("Fetching data...")
      for data in fetch_data():
        for message in data:
            id = message["id"]
            date = message["created_at"]
            message = {'id': id, 'date': date}
            message = json.dumps(message).encode("utf-8")
            producer.send(TOPIC_NAME, message)
        print('message sent', len(data))
        sleep(30)

# Démarrage du producteur Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

# Démarrage d'un thread pour la production de messages
thread = threading.Thread(target=produce_message, args=(producer,))
thread.start()

# Attente de la fin du thread
thread.join()




