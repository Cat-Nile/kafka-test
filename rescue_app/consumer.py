from kafka import KafkaConsumer
import time
import json


class MessageConsumer:
    broker = ""
    topic = ""
    group_id = ""
    logger = None

    def __init__(self, broker, topic, group_id):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id

    def activate_listener(self):
        consumer = KafkaConsumer(
            bootstrap_servers=self.broker,
            group_id="my-group",
            consumer_timeout_ms=2000,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )

        consumer.subscribe(self.topic)
        start=time.time()
        i=0
        for message in consumer:
            message=message.value
            print(i, message)
            i=i+1
            consumer.commit()

        end=time.time()
        elapsed=end-start
        per_time_value=i/elapsed

        print("\n\n총 처리시간: ", elapsed)
        print("초당 처리건수: ", per_time_value)
        print("----------------------------")


broker = 'localhost:9092'
topic = 'test-fire'
group_id = 'consumer-1'

consumer1 = MessageConsumer(broker, topic, group_id)
consumer1.activate_listener()

consumer2 = MessageConsumer(broker, topic, group_id)
consumer2.activate_listener()

