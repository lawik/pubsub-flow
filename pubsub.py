import zmq
import logging
import threading
import time

log = logging.getLogger(__name__)

EVENT_LOOP_DELAY = 0.01
TOPIC = 'global'

class Publisher(object):
    def __init__(self, endpoint):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(endpoint)
        log.info("Setting up publisher...")

    def send(self, data):
        self.socket.send_multipart([TOPIC].extend(data))

class ThreadSafePublisher(object):
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.context = zmq.Context()
        self.queue = []
        self.finish = False
        self.finished = False
        self.started = False
        log.info("Setting up thread-safe publisher...")
        thread = threading.Thread(target=self._event_thread)
        thread.daemon = True
        thread.start()

    def send(self, data):
        self.queue.append(data)

    def end(self):
        self.finish = True

    def _event_thread(self):
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(self.endpoint)
        while not self.finish:
            self.started = True
            data = self.queue.popleft()
            if data:
                self.socket.send_multipart([TOPIC].extend(data))
            else:
                time.sleep(EVENT_LOOP_DELAY)
        self.finished = True

class Subscriber(object):
    def __init__(self, endpoint):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, TOPIC)
        self.socket.connect(endpoint)

    def run(self, blocking=True):
        log.info("Running subscriber...")
        if blocking:
            while True:
                yield self.socket.recv_multipart()
        else:
            while True:
                try:
                    yield self.socket.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.Again:
                    yield


if __name__ = '__main__':
    import itertools
    import uuid
    log.addHandler(logging.StreamHandler())
    log.setLevel(logging.INFO)

    publisher = Publisher('tcp://127.0.0.1:30000')
    for i in itertools.count():
        log.info("Sending %d" % i)
        publisher.send([str(i),str(uuid.uuid1())])
