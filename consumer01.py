from kafka import KafkaConsumer
from json import loads
from datetime import datetime
import psycopg2
import time

consumer = KafkaConsumer(
    'test-fire',
    bootstrap_servers=['test-broker01:9092'],
    group_id="consumer1",
    consumer_timeout_ms=60000,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    database='fire_station',
    user='postgres',
    password='postgres',
    host='test-broker01',
    port='5432'
)

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS rescue_stat")
sql = '''CREATE TABLE rescue_stat(
    no INT NOT NULL primary key,
    call_date CHAR(30),
    call_time CHAR(30),
    going_date CHAR(30),
    going_time CHAR(30),
    accident_city CHAR(15),
    accident_gu CHAR(15),
    accident_dong CHAR(15),
    accident_cause CHAR(25),
    accident_cause_code CHAR(20)
)'''

cursor.execute(sql)
conn.commit()

i = 0

for message in consumer:
    i = i + 1
    mv = message.value
    json_msg = loads(mv)
    num = json_msg['No']
    start = time.time()
    try:
        cursor.execute(
            "INSERT INTO rescue_stat(no, call_date, call_time, going_date, going_time) VALUES (%s, %s, %s, %s, %s)",
            (num, json_msg['calldate'], json_msg['calltime'], json_msg['rescue_date'], json_msg['rescue_time']))
        # print('Data added to PostgreSQL: ', json_msg)
    except Exception:
        pass

    conn.commit()

conn.close()
