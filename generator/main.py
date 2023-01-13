# Libraries needed
from kafka import KafkaProducer
import time
from json import dumps
import random

producer = None
while producer==None:
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka0:29092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
        print("Connection Established")
        time.sleep(0.1)
    except Exception as err:
        print(f"Waiting for broker")


# Methods
def send_data():
    data_json="""{'test':"""+random.randint(0,100)+"""}"""
    print(data_json)
    producer.send("mocked_data",data_json)
    producer.flush()


# Main
while True:
    send_data()
    time.sleep(5)

