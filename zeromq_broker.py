import collections
import random

import msgpack

from zeroactor import gate


# Use ZeroMQ DEALER-ROUTER for server-server connection


def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--ip', type=str, default=get_my_ip(), help='self host name')
    p.add_argument('--port', '-p', type=int, default=55666, help='broker port')
    return p.parse_args()


def get_my_ip():
    import socket
    return socket.gethostbyname(socket.gethostname())


class Broker(gate.DealerRouterGate):
    # send workload to a random receiver
    def __init__(self, my_ip, port):
        super(Broker, self).__init__(my_ip, port)
        self._registered_receivers = set()
        self._pending_queue = collections.deque()

    def _dispatch_rpc(self, source_addr, payload):
        method, data = msgpack.unpackb(payload)
        if method == 'reg':
            self._registered_receivers.add(source_addr)
        elif method == 'work':
            self._pending_queue.append(payload)
        # dispatch work
        if self._registered_receivers:
            rs = list(self._registered_receivers)
            for work_load in self._pending_queue:
                recver = random.choice(rs)
                self.send(recver, work_load)
            self._pending_queue.clear()

    def run(self):
        while 1:
            self.poll(1)


if __name__ == '__main__':
    args = parse_args()
    Broker(args.ip, args.port).run()
