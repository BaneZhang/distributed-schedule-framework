# -*- coding:utf-8 -*-

import redis
import sys

sys.path.append('../src/')
import mutexscheduler

scheduler = mutexscheduler.MutexScheduler(daemonic=False)
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
rdc = redis.StrictRedis(connection_pool=pool)


def get_lock_record():
    return rdc.hgetall('h:lock:scheduler')


def generate_tick(ip, timestamp, **kwargs):
    kwargs['ip'] = ip
    kwargs['timestamp'] = timestamp
    rdc.hmset('h:lock:scheduler', kwargs)


def do_something_for_no_lock(lock_record):
    if lock_record:
        print lock_record['ip'] + ' holding lock.'


@scheduler.mutex(lock=get_lock_record, tick=generate_tick, no_lock=do_something_for_no_lock)
@scheduler.cron_schedule(second='*')
def job(**kwargs):
    if not 'data' in kwargs:
        kwargs['data'] = 0
    print kwargs['data']

    kwargs['data'] = int(kwargs['data']) + 1
    return kwargs


scheduler.start()