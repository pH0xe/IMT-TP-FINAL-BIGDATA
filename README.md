# TP noté de BigData

**Julien REIG - Justine BARTHELME**



## Lancez le programme

1. Lancer le broker kafka: `docker-compose up -d`
2. Créer un environnement virtuel: `python3 -m venv venv`
3. Activer l'environnement virtuel: `source venv/bin/activate`
4. Installer les dépendances: `pip install -r requirements.txt`
5. Lancer le producer: `python3 producer.py`
6. Dans un autre terminal, lancer le consumer: `python3 consumer.py`
