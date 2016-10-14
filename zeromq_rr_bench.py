import gevent
import gevent.queue
import random
import msgpack
import bench_base
import queue_data
from zeroactor import gate

# Use ZeroMQ ROUTER-ROUTER for server-server connection


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--ip', type=str, default=get_my_ip(), help='self host name')
    p.add_argument('--my-port', type=int, default=0, help='self port')
    p.add_argument('--host', '-H', type=str, default=get_my_ip(), help='zmq rpc broker host')
    p.add_argument('--port', '-p', type=int, default=55666, help='zmq rpc broker port')
    p.add_argument('--receivers', '-r', type=int, default=1, help='receiver count')
    p.add_argument('--senders', '-s', type=int, default=100, help='sender count')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--bin', action='store_true', help='use compressed binary test data')
    g.add_argument('--obj', action='store_true', help='use uncompressed python object data')
    return p.parse_args()


def get_my_ip():
    import socket
    return socket.gethostbyname(socket.gethostname())


class RRGate(gate.RouterRouterGate):
    def __init__(self, ip, port, benchmark, broker_addr):
        super(RRGate, self).__init__(ip, port)
        self._benchmark = benchmark
        self._broker_addr = broker_addr
        self._queue = gevent.queue.Queue(10)
        self.connect(broker_addr)

    def recv(self):
        return self._queue.get()

    def _dispatch_rpc(self, source_addr, payload):
        self._queue.put(1)

    def _ping(self):
        while self._benchmark.is_running():
            gevent.sleep(2.0)
            self.send(self._broker_addr, msgpack.packb(('reg', '')))

    def _run(self):
        while self._benchmark.is_running():
            self.poll(1)
            gevent.sleep(0.01)

    def spawn_all(self):
        return gevent.spawn(self._ping), gevent.spawn(self._run)


class ZeroMQBench(bench_base.BenchBase):
    def __init__(self, my_ip, my_port, broker_ip, broker_port, test_data):
        super(ZeroMQBench, self).__init__()
        self._broker_addr = 'tcp://%s:%d' % (broker_ip, broker_port)
        self._data = test_data
        self._gate = RRGate(my_ip, my_port or random.randint(10000, 30000), self, self._broker_addr)
        self._pool.spawn(self._gate._ping)
        self._pool.spawn(self._gate._run)

    def is_running(self):
        return self._running_flag

    def send(self, worker_idx):
        self.record_uniq('n_sender', worker_idx)
        try:
            gevent.sleep(1.0)
            while self._running_flag:
                self._gate.send(self._broker_addr, msgpack.packb(('work', self._data)))
                self.record('send')
                gevent.sleep(0.1)
        finally:
            self.record_uniq('n_sender', worker_idx, remove=True)

    def recv(self, worker_idx):
        self.record_uniq('n_receiver', worker_idx)
        try:
            while self._running_flag:
                if self._gate.recv():
                    self.record('recv')
        finally:
            self.record_uniq('n_receiver', worker_idx, remove=True)


if __name__ == '__main__':
    args = parse_args()
    print args.bin, args.obj
    if args.bin:
        test_data = queue_data.PACKED_DATA
    else:
        test_data = queue_data.STRUCTURE_DATA
    ZeroMQBench(args.ip, args.my_port, args.host, args.port, test_data).start(
        bench_base.BenchCurve(args.senders, 0, 1),
        bench_base.BenchCurve(args.receivers, 0, 0.01),
    )
