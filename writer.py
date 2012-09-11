import struct
import random
from datetime import datetime, time

import gevent.monkey
gevent.monkey.patch_all()
import pymongo

from gevent_zeromq import zmq

measures = [ 'measure-%d' % i for i in xrange(1000) ]

context = zmq.Context.instance()
sock = context.socket(zmq.SUB)
sock.connect('tcp://localhost:5555')
sock.setsockopt(zmq.SUBSCRIBE, '')

conn = pymongo.Connection(
    'mongodb://ip-10-190-131-134.ec2.internal:27017')

writes = 0

def main():
    global writes
    gl = None
    fp = open('times.csv', 'a')
    while True:
        ts, = struct.unpack('l', sock.recv())
        if gl is not None:
            gl.kill()
            if True or dt.minute == 0:
                line = '%d,%d\n' % (dt.hour * 60 + dt.minute, writes)
                fp.write(line)
                fp.flush()
                print line,
                writes = 0
        dt = datetime.utcfromtimestamp(ts)
        gl = gevent.spawn_link_exception(fast_writer, dt)

def fast_writer(dt):
    global writes
    coll = conn.test.hits
    while True:
        record_hit(coll, dt, random.choice(measures))
        writes += 1
        if random.random() < 0.1:
            gevent.sleep(0)
        if random.random() < 0.05:
            conn.test.command('getLastError')

def record_hit(coll, dt, measure):
    sdate = dt.strftime('%Y%m%d')
    metadata = dict(
        date=datetime.combine(
            dt.date(),
            time.min),
        measure=measure)
    id='%s/%s' % (sdate, measure)
    coll.update(
        { '_id': id, 'metadata': metadata },
        { '$inc': {
                'daily': 1,
                'hourly.%d' % dt.hour: 1,
                'minute.%d' % dt.minute: 1 } },
        upsert=True)

if __name__ == '__main__':
    main()
