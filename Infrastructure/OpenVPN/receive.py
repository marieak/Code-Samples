#!/usr/bin/env python

"""Receives URL from RabbitMQ and Responds with the URL's contents"""

__author__  = "Oliver Rice"

import requests
import logging
import json
import pika
import sys
from collections import namedtuple

# Configure Logging
lg = logging.getLogger()
lg.setLevel(logging.INFO)
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

# Simple URL Downloader
def callback(ch, method, properties, body):
    # Sanitize String
    if isinstance(body, str):
        pass
    else:
        body = body.decode("utf-8")

    msg = json.loads(body)
    
    logging.info('Recieved download request %s' % msg)
    msg['Response'] = requests.get(msg['url']).text
    logging.info('Downloaded %s' % msg)
    channel.basic_publish(exchange='',
            routing_key='response',
            body=json.dumps(msg))
            
    logging.info('Published results of %s' % body)
    #print('Published results of %s' % body)

    # Acknowlege that work has been completed
    ch.basic_ack(delivery_tag=method.delivery_tag)
    
# Accept work on 'url' queue and use 'callback' to process it
channel.basic_consume(callback,
        queue='url',
        no_ack=False)

# Waiting for messages
logging.info(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

