from kafka import KafkaProducer
from time import sleep
import requests
import json


# Coinbase API endpoint
url = 'https://api.coinbase.com/v2/exchange-rates'
#'https://api.coinbase.com/v2/exchange-rates'  #all
#'https://api.coinbase.com/v2/prices/btc-usd/spot' #BTC only

# Producing as JSON
kafka_topic = 'kafka-crypto-rates'
kafka_bootstrap_servers = ['cnt7-naya-cdh63:9092']
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers,
                        value_serializer=lambda m: json.dumps(m).encode('ascii'))

while(True):
    sleep(5)
    price = ((requests.get(url)).json())
    print("Prices fetched")
    producer.send(kafka_topic, price['data']['rates'])
    print("Prices sent to consumer")
    print(price['data']['rates'])