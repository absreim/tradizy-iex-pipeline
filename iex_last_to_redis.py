import redis
import json
import pika

RABBITMQ_HOST = 'localhost'
EXCHANGE = 'tops-last'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)
channel = connection.channel()

channel.exchange_declare(exchange=EXCHANGE, exchange_type='fanout')

queue_dec_result = channel.queue_declare(queue='', exclusive=True)
queue_name = queue_dec_result.method.queue

channel.queue_bind(exchange=EXCHANGE, queue=queue_name)

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def callback(_, __, ___, body):
    securities_list = json.loads(body)
    pipe = redis_client.pipeline(transaction=False)
    for security_dict in securities_list:
        symbol = security_dict['symbol']
        pipe.set(symbol, json.dumps(security_dict))
    pipe.execute()

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
