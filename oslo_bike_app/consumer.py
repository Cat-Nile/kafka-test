# Now that the producer is created and is sending the data to Kafka broker, and that we have database ready,
# we will create a consumer named consumer.py that will retrieve the data from Kafka and store it into Postgres
# database. First, we import the library that will be used.

from kafka import KafkaConsumer
from json import loads
from datetime import datetime

import collection as collection
import psycopg2
import time

# We initialize the consumer
consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

conn = psycopg2.connect(database='oslo_city_bike',
                        user='postgres',
                        password='postgres',
                        host='localhost',
                        port='5432'
                        )
cursor = conn.cursor()

for message in consumer:
    message = message.value
    timestamp = message['last_updated']
    dt_object = datetime.fromtimestamp(timestamp)
    message['Date'] = dt_object.strftime("%b %d %Y %H:%M:%S")
    datehrs = dt_object.strftime("%b %d %Y %H:%M:%S")
    start = time.time()
    j = 0
    for station in message['data']['stations']:
        print(station)
        j = j + 1
        cursor.execute(
            "INSERT INTO Station_Status (Station_id, is_installed, is_renting, is_returning,last_reported,num_bikes_available,num_docks_available) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (station['station_id'], station['is_installed'], station['is_renting'], station['is_returning'],
             station['last_reported'], station['num_bikes_available'], station['num_docks_available']))
        conn.commit()
    end = time.time()
    elapsed = end - start
    print('\n\n총 처리시간: ', elapsed)
    print('초당 처리량: ', j / elapsed)

    print('\nData at {} added to POSTGRESQL'.format(dt_object, collection))
    print('-------------------------------------')
conn.close()
