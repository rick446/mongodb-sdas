import sys
import struct
import random
from datetime import datetime, time, timedelta

import gevent.monkey
gevent.monkey.patch_all()
import pymongo

from gevent_zeromq import zmq

measures = [ 'measure-%d' % i for i in xrange(100) ]

context = zmq.Context.instance()
sock = context.socket(zmq.SUB)
sock.connect('tcp://localhost:5555')
sock.setsockopt(zmq.SUBSCRIBE, '')

conn = pymongo.Connection(
    'mongodb://ip-10-190-131-134.ec2.internal:27017')

writes = 0
PREALLOC=True

def main():
    global writes
    gl = None
    conn.test.hits.drop()
    fp = open('times.csv', 'w')
    while True:
        ts, = struct.unpack('l', sock.recv())
        if gl is not None:
            gl.kill()
            minute = dt.hour * 60 + dt.minute
            if True or dt.minute == 0:
                csv_line = '%d,%d\n' % (
                    minute, writes)
                line = '%d,%d %s\n' % (
                    minute, writes,
                    '*'*(writes/10))
                fp.write(csv_line)
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

def preallocate_hier(coll, dt, measure):
    sdate = dt.strftime('%Y%m%d')
    metadata = dict(
        date=datetime.combine(
            dt.date(),
            time.min),
        measure=measure)
    id='%s/%s' % (sdate, measure)
    hourly_doc = dict(
        ('hourly.%.2d' % i, 0)
        for i in range(24))
    minute_doc = dict(
        ('minute.%.2d.%.2d' % (h,m), 0)
        for h in range(24)
        for m in range(60))
    update = {
        '$set': { 'metadata': metadata },
        '$inc': { 'daily': 0 } }
    update['$inc'].update(hourly_doc)
    update['$inc'].update(minute_doc)
    coll.update(
        { '_id': id },
        update,
        upsert=True,
        safe=True)

def record_hit_hier(coll, dt, measure):
    if PREALLOC and random.random() < (1.0/1500.0):
        preallocate_hier(coll, dt + timedelta(days=1), measure)
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
                'hourly.%.2d' % dt.hour: 1,
                ('minute.%.2d.%.2d' % (dt.hour, dt.minute)): 1 } },
        upsert=True)

if __name__ == '__main__':
    main()
