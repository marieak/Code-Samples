#!/usr/bin/env python

"""Extract minutely common stock trading data from Google Finance

The list of URLs is handed to RabbitMQ on the channel 'url' and the process begins listening on 'response' channel

**An external process is expected to download the body of provided URLs and respond on the 'response' channel.**

Responses, containing the website body, are parsed to extract minutely stock trading data 

"""

__author__  = "Oliver Rice"


import pika
import logging
from bs4 import BeautifulSoup
from collections import namedtuple 
import json
from datetime import datetime, timedelta
import sys

# Asset (Stock) Data Template
Asset = namedtuple('Asset', ['Symbol', 'Name', 'Exchange', 'url', 'Response'])

# Bar of a Single Asset Template
Bar = namedtuple('Bar', ['Symbol', 'Exchange', 'Duration', 'Datetime', 'Close', 'High', 'Low', 'Open', 'Volume']) 


def get_bars_from_response(ch, method, properties, body):
    """Convert google finance response to Bars""" 
    asset = json.loads(body)

    logging.debug("Processing %s : %s" % (asset['Exchange'], asset['Symbol']))
    html = asset['Response']


    keywords =  ['EXCHANGE=','MARKET_OPEN_MINUTE=',
            'MARKET_CLOSE_MINUTE=','INTERVAL=',
            'COLUMNS=','DATA=','TIMEZONE_OFFSET',
            'DATA_SESSIONS']

    bars = []
    # Parse HTML into a 'Bar'
    for row in html.split():
        if any(keyword in row for keyword in keywords):
            continue
        r = row.split(',')
        if len(r)==6:
            if 'a' in row[0]:
                base_date = datetime.utcfromtimestamp(float(r[0].strip('a')))
                incr_date = timedelta(minutes = 0)
            else:
                incr_date = timedelta(minutes = int(r[0]))
                bar = Bar(
                    Symbol = asset['Symbol'], 
                    Exchange = asset['Exchange'], 
                    Duration = '1m',
                    Datetime = base_date + incr_date,
                    Close = float(r[1]),
                    High = float(r[2]),
                    Low = float(r[3]),
                    Open = float(r[4]),
                    Volume = int(r[5]),
                    )
                bars.append(bar._asdict())

    logging.debug('Available bar count for %s : %s = %d' % (asset['Exchange'], asset['Symbol'], len(bars)))
    if len(bars) > 0:
        logging.debug('Example record %s' % bars[0])


    # Acknowlege Work Completion. Ready for next task
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
    connection = pika.BlockingConnection(
            pika.ConnectionParameters( 
                host='rabbitmq', 
                port=5672, 
                connection_attempts=15, 
                retry_delay=5)) 
    
    # Setup RabbitMQ Queues 
    channel = connection.channel() 
    channel.queue_declare(queue='url') 
    channel.queue_declare(queue='response') 

    # Hard coded placeholder assets for sake of simplicity in this demo
    assets= [
            Asset('A','Agilent Technologies', 'NYSE', 'https://www.google.com/finance/getprices?e=NYSE&q=A&i=60s&p=2d&f=d,o,h,l,c,v', '')
            ,Asset('ABEV','Ambev S.A.', 'NYSE', 'https://www.google.com/finance/getprices?e=NYSE&q=ABEV&i=60s&p=2d&f=d,o,h,l,c,v', '')
            ,Asset('ACN','Accenture Plc', 'NYSE', 'https://www.google.com/finance/getprices?e=NYSE&q=ACN&i=60s&p=2d&f=d,o,h,l,c,v', '')
            ,Asset('CAT','Caterpillar Inc', 'NYSE', 'https://www.google.com/finance/getprices?e=NYSE&q=CAT&i=60s&p=2d&f=d,o,h,l,c,v', '')
            ,Asset('CBS','CBS Corp', 'NYSE', 'https://www.google.com/finance/getprices?e=NYSE&q=CBS&i=60s&p=2d&f=d,o,h,l,c,v', '')
        ] 
    
    # Post assets to RabbitMQ
    for asset in assets:
        channel.basic_publish(
                exchange='', 
                routing_key='url', 
                body=json.dumps(asset._asdict()),) 
         
    # When responses are received, extract 'Bars' from 'response' queue
    channel.basic_consume(get_bars_from_response,
            queue='response')

    logging.debug(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()
