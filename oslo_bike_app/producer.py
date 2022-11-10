# In this section we will create a producer in python that sends message to our kafka broker.
# This message will later be consumed by the consumer and stored into our postgresql DataWarehouse
from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

for i in range(120):  # every 30 seconds
    headers = {
        'Client-Identifier': 'korea-citymonitor',
    }

    response = requests.get('https://gbfs.urbansharing.com/oslobysykkel.no/station_status.json', headers=headers)
    res = response.json()
    producer.send('test', value=res)
    print("We sended new data: ", i)
    sleep(30)
