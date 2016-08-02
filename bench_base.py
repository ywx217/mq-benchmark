import collections
import time
import gevent
import gevent.monkey
import gevent.coros
gevent.monkey.patch_all()


class BenchCurve(object):
    def __init__(self, n_workers, init_delay=0.0, worker_interval=0.0):
        self._init_delay = init_delay
        self._worker_interval = worker_interval
        self._worker_count = n_workers

    def get_init_sleep(self, worker_idx):
        return self._init_delay + worker_idx * self._worker_interval

    def count(self):
        return self._worker_count


class BenchBase(object):
    def __init__(self):
        super(BenchBase, self).__init__()
        self._counter_lock = gevent.coros.BoundedSemaphore(1)
        self._counter = collections.Counter()
        self._print_interval = 1.0
        self._running_flag = True

    def stop(self):
        self._running_flag = False

    def send(self, worker_idx):
        raise NotImplementedError

    def recv(self, worker_idx):
        raise NotImplementedError

    def record(self, key, value=1):
        # record benchmark infos
        self._counter_lock.acquire()
        self._counter[key] += value
        self._counter_lock.release()
        return self._counter[key]

    def get_record(self, key):
        return self._counter[key]

    def print_record(self):
        c = sorted(self._counter.items())
        print '[%s] %s' % (
            time.strftime('%Y-%m-%d %H:%M:%S'),
            ' '.join(map(lambda x: '='.join(map(str, x)), c))
        )

    def _print_loop(self):
        while self._running_flag:
            gevent.sleep(self._print_interval)
            self.print_record()

    def _make_worker(self, curve, worker_idx, work_func):
        delay = curve.get_init_sleep(worker_idx)

        def _worker(*args, **kwargs):
            gevent.sleep(delay)
            work_func(*args, **kwargs)

        return _worker

    def _add_signal_support(self):
        pass

    def start(self, send_curve=None, recv_curve=None, print_interval=1.0):
        workers = []
        if send_curve:
            for idx in xrange(send_curve.count()):
                workers.append(gevent.spawn(self._make_worker(send_curve, idx, self.send), idx))
        if recv_curve:
            for idx in xrange(recv_curve.count()):
                workers.append(gevent.spawn(self._make_worker(recv_curve, idx, self.recv), idx))
        if print_interval > 0:
            self._print_interval = print_interval
            workers.append(gevent.spawn(self._print_loop))
        self._add_signal_support()
        gevent.joinall(workers)

