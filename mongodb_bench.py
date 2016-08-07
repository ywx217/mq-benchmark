import time
import gevent
import bench_base
import pymongo
from bson import Binary
import queue_data


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--host', '-H', type=str, default='localhost', help='mongodb host')
    p.add_argument('--port', '-p', type=int, default=32771, help='mongodb port')
    p.add_argument('--receivers', '-r', type=int, default=10, help='receiver count')
    p.add_argument('--senders', '-s', type=int, default=100, help='sender count')
    p.add_argument('--clear', action='store_true', help='clear db')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--bin', action='store_true', help='use compressed binary test data')
    g.add_argument('--obj', action='store_true', help='use uncompressed python object data')
    return p.parse_args()


class MongodbQueue(object):
    """Simple Queue with MongoDB Backend"""
    def __init__(self, name, namespace='queue', **mongodb_kwargs):
        conn = pymongo.Connection(**mongodb_kwargs)
        self.__db = conn[namespace][name]
        self.__db.create_index([('time', pymongo.ASCENDING)])

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.count()

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def clear(self):
        self.__db.remove({})

    def put(self, item):
        """Put item into the queue."""
        self.__db.insert({
            'time': time.time(),
            'payload': item,
        })

    def get(self):
        """Remove and return an item from the queue(blocked)."""
        doc = self.__db.find_and_modify({}, sort=[('time', pymongo.ASCENDING)], remove=True)
        if doc:
            return doc.get('payload')
        return doc


class MongodbBench(bench_base.BenchBase):
    def __init__(self, host, port, test_data, clear=False):
        super(MongodbBench, self).__init__()
        self._q = MongodbQueue('test', host=host, port=port)
        if clear:
            self._q.clear()
        if isinstance(test_data, str):
            self._data = Binary(test_data)
        else:
            self._data = test_data

    def send(self, worker_idx):
        self.record_uniq('n_sender', worker_idx)
        try:
            while self._running_flag:
                self._q.put(self._data)
                self.record('send')
                gevent.sleep(1.0)
        finally:
            self.record_uniq('n_sender', worker_idx, remove=True)

    def recv(self, worker_idx):
        self.record_uniq('n_receiver', worker_idx)
        try:
            while self._running_flag:
                if self._q.get():
                    self.record('recv')
                else:
                    gevent.sleep(0.1)
        finally:
            self.record_uniq('n_receiver', worker_idx, remove=True)

    def check_queue_size(self):
        while self._running_flag:
            self.set_record('qsize', self._q.qsize())
            gevent.sleep(0.5)

    def _addition_worker_on_start(self):
        return (self.check_queue_size, )


if __name__ == '__main__':
    args = parse_args()
    if args.bin:
        test_data = queue_data.PACKED_DATA
    else:
        test_data = queue_data.STRUCTURE_DATA
    MongodbBench(args.host, args.port, test_data, args.clear).start(
        bench_base.BenchCurve(args.senders, 0, 1),
        bench_base.BenchCurve(args.receivers, 0, 0.01),
    )

