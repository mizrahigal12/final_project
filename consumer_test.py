

















from kafka import KafkaConsumer
import json
from project_config import cnx
from time import sleep


# Insert function
insert_statement = """
    INSERT INTO crypto_coins.crypto_coins_raw_data(currency, rate) VALUES ('{}', '{}');"""

def mysql_event_insert(mysql_conn, currency, rate):
    sql = insert_statement.format(currency, rate)
    mysql_cursor.execute(sql)



# Getting the data as JSON
topic = 'kafka-crypto-rates'
brokers = ['cnt7-naya-cdh63:9092']
consumer = KafkaConsumer(topic,
bootstrap_servers=brokers,
#auto_offset_reset='earliest', # auto_offset_reset='latest',
enable_auto_commit=True,
auto_commit_interval_ms=1000,
value_deserializer=lambda m: json.loads(m.decode('ascii')))

mysql_cursor = cnx.cursor()
for message in consumer:
    # Write to MySQL
    data = (message.value)['data']#['amount']
    rates = data['rates']
    #amount = data['amount']
    print(rates)
    '''currency = data['base']
    rate = data['amount']
    mysql_event_insert(cnx, currency, rate)
    print('The', currency, 'currency was inserted with the rate -', rate)'''
    sleep(10)
mysql_cursor.close()    


