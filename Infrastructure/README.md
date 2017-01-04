How to execute
---------------
docker-compose > 1.8

docker > 1.12.5

`$ docker-compose up`

Objective
--------------------
Sender : 
 - Pass a list of Google Finance urls from a main worker process to a message queue (RabbitMQ) 
 - Wait for responses
 - Parse responses into minutely bars & provide basic info for each asset (number of bars received & an example record)
 
Receiver : 
  - Receive arbitrary message from RabbitMQ containing a 'url' field
  - Download the url body from the provided url
  - Add the url's body to the original message and post back to 'Sender' on the 'response' channel
 
Multiple Receivers (currently 3) process the work provided by a single Sender.


Reason for including
--------------------
Demonstrates understanding of:
  - Docker
  - Docker-compose
  - Distributed systems
  - Scatter/gather
  - Message queues

 
Note
--------------------
This is just a toy example. Asynchronous requests via 'requests.async' would be a (much) better solution to this task.
