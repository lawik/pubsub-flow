import socket
import uuid
import zmq
import time

import threadpool
import pubsub
from cStringIO import StringIO

import logging

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

pub_endpoint = 'tcp://127.0.0.1:30000'

outstanding_requests = {}

log.info("Run only once?")

pool = threadpool.ThreadPool(pub_endpoint)

publisher = pubsub.ThreadSafePublisher()

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.bind(('127.0.0.1', '8300'))
serversocket.listen(5)

while True:
    (clientsocket, address) = serversocket.accept()
    pool.task(dispatch_request, clientsocket, address)

def dispatch_client(self, clientsocket, address):
    request_id = str(uuid.uuid1())
    outstanding_requests[request_id] = {clientsocket
