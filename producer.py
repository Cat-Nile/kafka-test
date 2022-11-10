from kafka import KafkaProducer
import json
import time
from csv import reader


class MessageProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker,
                                      value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                      acks=0,
                                      retries=3
                                      )

    def send_message(self, msg):
        # print("sending messages...")
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()
            future.get(timeout=60)
            print(msg)
            return {'status_code': 200, 'error': None}
        except Exception as e:
            return e


broker = 'localhost:9092'
topic = 'test-fire'
message_producer = MessageProducer(broker, topic)

with open('fire.txt', 'r', encoding='utf-8') as file:
    for data in file:
        res = message_producer.send_message(data)

# with open('fire.csv', 'r', encoding='euc-kr') as obj:
#     csv_reader = reader(obj)
#     header = next(csv_reader)
#     print(header)
