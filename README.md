# TP noté de BigData

**Julien REIG - Justine BARTHELME**


## Lancez le programme

1. Lancer le broker kafka: `docker-compose up -d`
2. Créer un environnement virtuel: `python3 -m venv venv`
3. Activer l'environnement virtuel: `source venv/bin/activate`
4. Installer les dépendances: `pip install -r requirements.txt`
5. Lancer le producer: `python3 producer.py`
6. Dans un autre terminal, lancer le consumer: `python3 consumer.py`

## Le projet

Le programme permet de streamer l'API Mastodon, une API pour le réseau social Mastodon,
avec Kafka.
Le fichier producer-stream_thread.py sert à fetcher les données pour le TAG donné pour les envoyer en boucle à Kafka.
Le fichier consume_thread.py permet de lire dans les threads pour pouvoir traiter les données. Le programme ici permet de trouver les threads produits par 6 d'intervalles avec un chevauchement sur 30 minutes, ensuite, on sauvegarde ces données dans un fichier csv (dans /unique_output/).


## Question bonus

1) Comment pourriez-vous faire pour n'avoir qu'un seul fichier ?
Pour cette question nous avons fait en sorte de stocker les données en mémoire dans une table temporaire. Lorsque l'utilisateur stop le programme, on récupère ces données, on fait un coallesce pour n'avoir qu'un seul dataframe, et on l'écrit dans un fichier csv.

2) Peut-on trier les résultats ?
Il n'est pas possible de trier les résultats sur des streams de données. En effet, le tri nécessite de connaître l'ensemble des données, ce qui n'est pas possible avec un stream.
Cependant, comme nous stockons les données dans une table temporaire, il est possible de trier les données après le coallesce.