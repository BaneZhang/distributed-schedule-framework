# -*- coding:utf-8 -*-

import time
import socket
from logging import NullHandler

from apscheduler.scheduler import Scheduler, logger

default_timeout = 5


class MutexScheduler(Scheduler):
    def __init__(self, config={}, **kwargs):
        Scheduler.__init__(self, config, **kwargs)
        logger.addHandler(NullHandler())
        self.mutex_function_generator = None
        self.ip = socket.gethostbyname(socket.gethostname())

    def mutex(self, lock=None, tick=None, no_lock=None, timeout=default_timeout):
        def mutex_function_generator(function):
            def mutex_function():
                if lock:
                    lock_record = lock()
                    now = int(time.time())

                    if not lock_record or lock_record['ip'] == self.ip or (
                            lock_record['timestamp'] and (now - int(lock_record['timestamp']) >= timeout)):

                        if lock_record:
                            del lock_record['ip']
                            del lock_record['timestamp']

                        if not lock_record:
                            lock_record = {}

                        kwargs = function(**lock_record)
                        if not kwargs:
                            kwargs = {}

                        tick(self.ip, now, **kwargs)
                    else:
                        no_lock(lock_record)
                else:
                    function()

            return mutex_function

        self.mutex_function_generator = mutex_function_generator

        def decorator(function):
            return function

        return decorator

    def cron_schedule(self, **kwargs):
        def decorator(function):
            if hasattr(self, 'mutex_function_generator'):
                function = self.mutex_function_generator(function)

            function.job = self.add_cron_job(function, **kwargs)
            return function

        return decorator
