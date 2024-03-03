import requests
from time import sleep
import json
import threading

from kafka import KafkaProducer

# Configuration du producteur Kafka
KAFKA_BROKER_URL = "localhost:9092"
TOPIC_NAME = "IA"
MASTODON_URL = "https://mastodon.social/api/v1/timelines/tag/IA"

# Fetch et retourne les données de l'API pour le tag IA
def fetch_data():
    while True:
        response = requests.get(MASTODON_URL)
        if response.status_code == 200:
            data = response.json()
            if data:
                yield data

# Envoie un message contenant les données de fetch_data() en boucle à Kafka
def produce_message(producer: KafkaProducer):
    while True:
        for data in fetch_data():
            for message in data:
                thread_id: str = message["id"]
                date = message["created_at"]
                message = {'id': thread_id, 'date': date}
                message = json.dumps(message).encode("utf-8")
                # Envoie le message pour chaque ligne
                producer.send(TOPIC_NAME, value=message, key=thread_id.encode("utf-8"))
            print('message sent', len(data))
            # Temps de pause pour chaque message puis recommence après
            sleep(30)

# Envoie un message contenant les données de fetch_data() fetchée qu'une seule fois en boucle à Kafka
def debug_produce(producer: KafkaProducer):
    # On le met en dehors du while pour qu'elles ne soient pas modifiées par la suite
    d = fetch_data()
    while True:
        for data in d:
            for message in data:
                thread_id: str = message["id"]
                date = message["created_at"]
                message = {'id': thread_id, 'date': date}
                message = json.dumps(message).encode("utf-8")
                # Envoie le message pour chaque ligne
                producer.send(TOPIC_NAME, value=message, key=thread_id.encode("utf-8"))
            print('message sent', len(data))
            # Temps de pause pour chaque message puis recommence après
            sleep(10)


if __name__ == "__main__":
    # Démarrage du producteur Kafka
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

    # Démarrage d'un thread pour la production de messages
    thread = threading.Thread(target=produce_message, args=(producer,))
    thread.start()

    # Attente de la fin du thread
    thread.join()
