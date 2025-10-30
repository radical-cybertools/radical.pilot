#!/usr/bin/env python3

import sys
import time
import threading     as mt

import radical.utils as ru
import radical.pilot as rp


N = 1024
t_mode = 'func'

# ------------------------------------------------------------------------------
#
def watch(pout_url):

    pout = ru.zmq.Pipe(ru.zmq.MODE_PULL, url=pout_url)

    for _ in range(N):

        msg = pout.get()
        print(msg)


@rp.pythontask
def sleep(t_sleep):
    import time
    time.sleep(t_sleep)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    try:
        with ru.ru_open('/tmp/dragon.endpoints', 'r') as fin:
            pin_url  = fin.readline().strip()
            pout_url = fin.readline().strip()
    except:
        print('Cannot open /tmp/dragon.endpoints')

    try:
        pin_url  = sys.argv[1]
        pout_url = sys.argv[2]
    except IndexError:
        print('Usage: %s <pin_url> <pout_url>' % sys.argv[0])
        sys.exit(1)

    pin = ru.zmq.Pipe(ru.zmq.MODE_PUSH, url=pin_url)

    watcher = mt.Thread(target=watch, args=[pout_url])
    watcher.daemon = True
    watcher.start()

    for i in range(N):

        if t_mode == 'exec':
            task = {'uid'              : 'task.%04d' % i,
                    'task_sandbox_path': '/tmp',
                    'exec_path'        : '/bin/date',
                    'description'      : {'executable': '/bin/date',
                                          'ranks': 1,
                                          'mode': rp.TASK_EXECUTABLE}}

        elif t_mode == 'func':
            task = {'uid'              : 'task.%04d' % i,
                    'task_sandbox_path': '/tmp',
                    'description'      : {'function': sleep(0),
                                          'ranks': 1,
                                          'args': [],
                                          'kwargs': {},
                                          'mode': rp.TASK_FUNCTION}}

        pin.put({'cmd': 'run', 'task': task})
        print('push %s' % task)

    watcher.join()


# ------------------------------------------------------------------------------

