import sys
from itertools import cycle
import random
import time as tm_time
from datetime import datetime, time, timedelta

import pymongo

measures = [ 'measure-%d' % i for i in xrange(100) ]

conn = pymongo.Connection(
    'mongodb://ip-10-190-131-134.ec2.internal:27017')

PREALLOC=eval(sys.argv[1])
DILATION=300

def main():
    coll = conn.test.hits
    coll.drop()
    fp = open('times.csv', 'w')
    rt_begin = sim_now = tm_time.time()
    measure_iter = cycle(measures)
    while True:
        # Simulate a minute
        writes = 0
        sim_min_begin = sim_now
        rt_min_begin = tm_time.time()
        while sim_now - sim_min_begin < 60.0:
            rt_elapsed = tm_time.time() - rt_begin
            sim_now = rt_begin + (rt_elapsed * DILATION)
            sim_dt = datetime.utcfromtimestamp(sim_now)
            record_hit(coll, sim_dt, measure_iter.next())
            writes += 1
            if writes % 100 == 0:
                conn.test.command('getLastError')
        conn.test.command('getLastError')
        rt_min_elapsed = tm_time.time() - rt_min_begin

        # Write that minute's results
        wps = writes / float(rt_min_elapsed)
        minute = sim_dt.hour * 60 + sim_dt.minute
        csv_line = '%d,%f\n' % (
            minute, wps)
        line = '%d,%f %s' % (minute, wps,'*'*(writes/10))
        fp.write(csv_line)
        if minute % 10 == 0:
            fp.flush()
            print line

def preallocate(coll, dt, measure):
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
        ('minute.%.4d' % i, 0)
        for i in range(1440))
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

def record_hit(coll, dt, measure):
    if PREALLOC and random.random() < (1.0/1500.0):
        preallocate(coll, dt + timedelta(days=1), measure)
    sdate = dt.strftime('%Y%m%d')
    metadata = dict(
        date=datetime.combine(
            dt.date(),
            time.min),
        measure=measure)
    id='%s/%s' % (sdate, measure)
    minute = dt.hour * 60 + dt.minute
    coll.update(
        { '_id': id, 'metadata': metadata },
        { '$inc': {
                'daily': 1,
                'hourly.%.2d' % dt.hour: 1,
                'minute.%.4d' % minute: 1 } },
        upsert=True)

if __name__ == '__main__':
    main()
