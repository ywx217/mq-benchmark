import gevent
import bench_base
import redis

# RedisQueue from blog: http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html
REDIS_HOST = '192.168.99.100'
TEST_DATA = "test content"


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--port', '-p', type=int, default=32771, help='redis port')
    p.add_argument('--receivers', '-r', type=int, default=10, help='receiver count')
    p.add_argument('--senders', '-s', type=int, default=10, help='sender count')
    return p.parse_args()


class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, name, namespace='queue', **redis_kwargs):
        """The default connection parameters are: host='localhost', port=6379, db=0"""
        self.__db = redis.Redis(**redis_kwargs)
        self.key = '%s:%s' %(namespace, name)

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        self.__db.rpush(self.key, item)

    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. 

        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available."""
        if block:
            item = self.__db.blpop(self.key, timeout=timeout)
        else:
            item = self.__db.lpop(self.key)

        if item:
            item = item[1]
        return item

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)


class RedisBench(bench_base.BenchBase):
    def __init__(self, host, port):
        super(RedisBench, self).__init__()
        self._q = RedisQueue('test', host=host, port=port)

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
    RedisBench(REDIS_HOST, args.port).start(
        bench_base.BenchCurve(args.senders, 0, 1),
        bench_base.BenchCurve(args.receivers, 0, 0),
    )

