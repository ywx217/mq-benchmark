import gevent
import bench_base
import redis
import queue_data

# RedisQueue from blog: http://peter-hoffmann.com/2012/python-simple-queue-redis-queue.html


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--host', '-H', type=str, default='localhost', help='redis host')
    p.add_argument('--port', '-p', type=int, default=32771, help='redis port')
    p.add_argument('--receivers', '-r', type=int, default=10, help='receiver count')
    p.add_argument('--senders', '-s', type=int, default=100, help='sender count')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--bin', action='store_true', help='use compressed binary test data')
    g.add_argument('--obj', action='store_true', help='use uncompressed python object data')
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
    def __init__(self, host, port, test_data):
        super(RedisBench, self).__init__()
        self._q = RedisQueue('test', host=host, port=port)
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
    print args.bin, args.obj
    if args.bin:
        test_data = queue_data.PACKED_DATA
    else:
        test_data = queue_data.STRUCTURE_DATA
    RedisBench(args.host, args.port, test_data).start(
        bench_base.BenchCurve(args.senders, 0, 1),
        bench_base.BenchCurve(args.receivers, 0, 0.01),
    )

