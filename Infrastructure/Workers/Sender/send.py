#!/usr/bin/env python

"""Extract minutely common stock trading data from Google Finance

This process scrapes Google Finance for:
	- The financial exchanges they publish
	- The assets (common stock) contained in each of those exchanges

From this information it computes the URL where trading data can be found for each asset
e.g. 
	https://www.google.com/finance/getprices?e=NASDAQ&q=GOOG&i=60s&p=2d&f=d,o,h,l,c,v

The list of URLs is handed to RabbitMQ on the channel 'url' and the process begins listening on 'response' channel

*An external process is expected to download the body of provided URLs and respond on the 'response' channel.*

Responses, containing the website body, are parsed to extract minutely stock trading data 

Results are stored in MongoDB.


Possible Improvements:
	1. Fault tolerance around MongoDB connection
	2. Fault tolerance in 'Bar' processing
	3. Confirm IP address of remote process is different from IP address of (this) process
	4. Async download of exchange & asset lists
	5. Async inserts into MongoDB using Motor library
"""

__author__  = "Oliver Rice"


import pika
import requests
from bs4 import BeautifulSoup
from collections import namedtuple 
import json
from datetime import datetime, timedelta
import csv
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient(host='mongodb')

# Create/Connect DB
db = client['google_finance']

# Create/Connect to Collection
collection = db['bars']

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', 
    port=5672,
    connection_attempts=15,
    retry_delay=5
    ))


# Setup RabbitMQ Queues
channel = connection.channel()
channel.queue_declare(queue='url')
channel.queue_declare(queue='response')

# Stock Exchange Data Template
Exchange = namedtuple('Exchange', ['Region', 'Code', 'Name'])

# Download a List of Stock Exchanges Available on Google Finance
def get_exchanges():
    url = 'https://www.google.com/googlefinance/disclaimer/' 
    try:
        html = requests.get(url).text
        print('fetched %s' % url)
        soup = BeautifulSoup(html, 'html.parser')
        # Get Exchange Codes
        exchanges = []
        for row in soup.find('table').findAll('tr'):
            parsed_row = [v.text.strip() for v in row.findAll('td')]
            if len(parsed_row) == 4:
                if parsed_row[0]:
                    region = parsed_row[0]
                for code in parsed_row[1].split(','):
                    exchanges.append(
                        Exchange(Region=region,Code=code,Name=parsed_row[2]))

    except Exception as e:
        print('Exception: %s %s' % (e, url))
        return []

    return exchanges


# Asset (Stock) Data Template
Asset = namedtuple('Asset', ['Symbol', 'Name', 'Exchange', 'url', 'Response'])

def get_assets_from_exchange(exchange):
    url = 'http://www.google.com/finance?q=%5B(exchange%20%3D%3D%20%22'+exchange.Code+'%22)%5D&restype=company&noIL=1&num=50000' 
    try:
        html = requests.get(url).text
        print('fetched %s' % url)

        assets = []
        soup = BeautifulSoup(html, 'html.parser')
        for row in soup.findAll('tr', {'class' : 'snippet'}):
            asset = Asset(
                Symbol = row.find('td', {'class' : 'symbol'}).text.strip(),
                Name = row.find('td', {'class' : 'localName nwp'}).find('nobr').text.strip(),
                Exchange = row.find('td', {'class' : 'exch'}).text.split()[0].strip(),
                url = '',
                Response = ''
                )
            asset = asset._replace(
                    url=''.join(['https://www.google.com/finance/getprices?e=',asset.Exchange,'&q=',asset.Symbol,'&i=60s&p=2d&f=d,o,h,l,c,v']))

            assets.append(asset)
    except Exception as e:
        print('Exception: %s %s' % (e, url))
        return []

    return assets

# Bar of a Single Asset Template
Bar = namedtuple('Bar', ['Symbol', 'Exchange', 'Duration', 'Datetime', 'Close', 'High', 'Low', 'Open', 'Volume']) 


processed_assets = 0

def get_bars_from_response(ch, method, properties, body):
    """Convert response

    """
    asset = json.loads(body)

    print("Processing %s : %s" % (asset['Exchange'], asset['Symbol']))
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

    # Insert Bars into MongoDB Collection
    collection.insert_many(bars)

    # Acknowlege Work Completion. Ready for next task
    ch.basic_ack(delivery_tag=method.delivery_tag)

    # Teardown on completion
    if processed_assets == len(assets):
        print("All Work Completed")
        connection.close

# List of Exchanges -- Restricted to subset for purposes of code sample
exchanges = get_exchanges()[0:2]

# List of Assets on All Exchanges
assets = []

# Iterage Exchanges & Extract List of Assets on the Exchange
for exchange in (ex for ex in exchanges if ex.Code != 'OTCMKTS'):
    for asset in get_assets_from_exchange(exchange):
        assets.append(asset)
        channel.basic_publish(exchange='',
            routing_key='url',
            body=json.dumps(asset._asdict()),
            )
            
# When responses are received, extract 'Bars' from 'response' queue
channel.basic_consume(get_bars_from_response,
        queue='response')


print(' [*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()

connection.close()

