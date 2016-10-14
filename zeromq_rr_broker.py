import collections
import random
import time
import gevent
import gevent.pool

import msgpack

from zeroactor import gate


# Use ZeroMQ ROUTER-ROUTER for server-server connection


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--ip', type=str, default=get_my_ip(), help='self host name')
    p.add_argument('--port', '-p', type=int, default=55666, help='broker port')
    p.add_argument('--life-time', '-l', type=int, default=0, help='lifetime in seconds, 0 is live forever')
    p.add_argument('--profile', action='store_true', help='open profile')
    return p.parse_args()


def get_my_ip():
    import socket
    return socket.gethostbyname(socket.gethostname())


class Broker(gate.RouterRouterGate):
    # send workload to a random receiver
    def __init__(self, my_ip, port, life_time=0):
        super(Broker, self).__init__(my_ip, port)
        self._registered_receivers = {}
        self._pending_queue = collections.deque()
        if life_time > 0:
            self._death_time = time.time() + life_time
        else:
            self._death_time = 0
        self._pool = gevent.pool.Pool()

    def _dispatch_rpc(self, source_addr, payload):
        method, data = msgpack.unpackb(payload)
        if method == 'reg':
            if source_addr not in self._registered_receivers:
                print '[reg] %s' % source_addr
            self._registered_receivers[source_addr] = time.time()
        elif method == 'work':
            self._pending_queue.append(payload)
        # dispatch work
        if self._registered_receivers and self._pending_queue:
            recv_list = list(self._registered_receivers)
            for workload in self._pending_queue:
                self.send(random.choice(recv_list), workload)
            self._pending_queue.clear()

    @property
    def is_alive(self):
        return not self._death_time or time.time() < self._death_time

    def _io_loop(self):
        while self.is_alive:
            self.poll(0)
            gevent.sleep(0.001)

    def _queue_monitor(self):
        while self.is_alive:
            gevent.sleep(5)
            print '[%s] recv=%s queue=%s' % (
                time.strftime('%Y-%m-%d %H:%M:%S'),
                len(self._registered_receivers),
                len(self._pending_queue)
            )

    def _remove_timeout_brokers(self):
        while self.is_alive:
            gevent.sleep(2)
            now = time.time()
            remove_set = [r for r, t in self._registered_receivers.iteritems() if now - t > 8]
            if not remove_set:
                continue
            for r in remove_set:
                del self._registered_receivers[r]
            print 'remove dead receiver: %s' % remove_set

    def run(self):
        self._pool.spawn(self._io_loop)
        self._pool.spawn(self._queue_monitor)
        self._pool.spawn(self._remove_timeout_brokers)
        self._pool.join()


def prof_run(func):
    import cProfile
    p = cProfile.Profile()
    p.runcall(func)
    p.dump_stats('prof.prof')


if __name__ == '__main__':
    args = parse_args()
    broker = Broker(args.ip, args.port, args.life_time)
    if args.profile:
        prof_run(broker.run)
    else:
        broker.run()
