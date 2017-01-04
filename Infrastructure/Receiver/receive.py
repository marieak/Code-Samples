#!/usr/bin/env python

"""Receives URL from RabbitMQ and Responds with the URL's contents"""

__author__  = "Oliver Rice"

import requests
import logging
import json
import pika
import sys
from collections import namedtuple

# Simple URL Downloader
def callback(ch, method, properties, body):
    # Sanitize String
    if isinstance(body, str):
        pass
    else:
        body = body.decode("utf-8")

    # Load message from RMQ
    msg = json.loads(body)
    logging.debug('Recieved download request %s' % msg)
    msg['Response'] = requests.get(msg['url']).text
    logging.info('Downloaded %s' % body)
    channel.basic_publish(exchange='',
        routing_key='response',
        body=json.dumps(msg))
    logging.debug('Published results of %s' % body)

    # Acknowlege that work has been completed
    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    # Configure Logging
    lg = logging.getLogger()
    lg.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    lg_format = logging.Formatter('%(asctime)s %(message)s')
    ch.setFormatter(lg_format)
    lg.addHandler(ch)

    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='rabbitmq', 
        port=5672,
        connection_attempts=15,
        retry_delay=5,
        heartbeat_interval=0
        ))

    # Create 'response' and 'url' queues
    channel = connection.channel()
    channel.basic_qos(prefetch_count=1)
    channel.queue_declare(queue='response')
    channel.queue_declare(queue='url')

    # Accept work on 'url' queue and use 'callback' to process it
    channel.basic_consume(callback,
            queue='url',
            no_ack=False)

    # Wait for messages
    logging.debug(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
