from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'IA',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True)

i = 0
for message in consumer:
    print('\n\n\n',i, message)
    i += 1