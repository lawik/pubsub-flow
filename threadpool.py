import zmq
import logging
import uuid
import threading

log = logging.getLogger(__name__)

THREADPOOL_TASK_ERROR = 'THREADPOOL_TASK_ERROR'


class ThreadPool(object):
    def __init__(self, starting_threads=3):
        self.socket_name = str(uuid.uuid1())
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.bind('inproc://%s' % self.socket_name)
        self.tasks = {}
        self.add_threads(starting_threads)

    def add_threads(self, count):
        for thread_number in range(1,count):
            self.add_thread()

    def add_thread(self):
        thread_args = [
            self.context,
            self.socket_name
        ]
        thread = threading.Thread(target=self._work, args=thread_args)
        thread.daemon = True
        thread.start()

    # Medium
    def drop_thread(self):
        pass

    # Harder
    def kill_thread(self, thread_name):
        pass

    def task(self, function, *args, **kwargs):
        task_id = str(uuid.uuid1())
        self.tasks[task_id] = Task(function, *args, **kwargs)
        self.socket.send_multipart([task_id])

    def _work(self):
        socket = self.context.socket(zmq.REP)
        socket.connect(self.worker_endpoint)

        while True:
            frames = socket.recv_multipart()
            start_time = timeit.default_timer()
            log.info("Task received...")
            if not frames:
                log.error("Empty message.")
                break
            else:
                log.info("Task: %s" % str(frames[0]))
                try:
                    result = self.tasks[frames[0]].run()
                    del self.tasks[frames[0]]
                except Exception as e:
                    log.exception("Caught an uncaught exception in threadpool task. This should have been handled by the task function. Sloppy...")

                stop_time = timeit.default_timer() - start_time

                log.info("Task %(task_id)s processed in %(time)s seconds." %
                    {
                        'task_id': frames[0],
                        'time': stop_time,
                    }
                )


class Task(object):
    def __init__(self, function, *args, **kwargs):
        self.function = task_function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.function(*args, **kwargs)
