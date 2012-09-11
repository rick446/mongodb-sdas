import time
import struct

import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent_zeromq import zmq

context = zmq.Context.instance()
sock = context.socket(zmq.PUB)
sock.bind('tcp://*:5555')

def main():
    ts = int(time.time())
    while True:
        sock.send(struct.pack('l', ts))
        gevent.sleep(1)
        ts += 60

if __name__ == '__main__':
    main()
