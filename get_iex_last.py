import signal
import requests
import pika
from time import sleep
from sys import exit

URL = 'https://api.iextrading.com/1.0/tops/last'
RABBITMQ_HOST = 'localhost'
EXCHANGE = 'tops-last'
INTERVAL = 5

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)


def sigterm_handler():
    connection.close()
    exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)

channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')

while True:
    req = requests.get(URL)
    serial_json = req.text
    channel.basic_publish(exchange=EXCHANGE, routing_key='', body=serial_json)
    sleep(INTERVAL)
