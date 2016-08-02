import time
import gevent
import bench_base
import pymongo

TEST_DATA = "test content"


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--host', '-h', type=str, default='192.168.99.100', help='mongodb host')
    p.add_argument('--port', '-p', type=int, default=32771, help='mongodb port')
    p.add_argument('--receivers', '-r', type=int, default=10, help='receiver count')
    p.add_argument('--senders', '-s', type=int, default=10, help='sender count')
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
    def __init__(self, host, port):
        super(MongodbBench, self).__init__()
        self._q = MongodbQueue('test', host=host, port=port)

    def send(self, worker_idx):
        while self._running_flag:
            self._q.put(TEST_DATA)
            self.record('send')
            gevent.sleep(0.1)

    def recv(self, worker_idx):
        while self._running_flag:
            if self._q.get():
                self.record('recv')


if __name__ == '__main__':
    args = parse_args()
    MongodbBench(args.host, args.port).start(
        bench_base.BenchCurve(args.senders, 0, 1),
        bench_base.BenchCurve(args.receivers, 0, 0),
    )

