#!/usr/bin/env python3

import sys
import time

import radical.utils as ru


if __name__ == '__main__':

    def res_cb(tasks):
        for task in ru.as_list(tasks):
            print('%s: %s' % (task.uid, task.result))


    putter = ru.zmq.Putter(channel='req_queue', url=sys.argv[1])
    getter = ru.zmq.Getter(channel='res_queue', url=sys.argv[2], cb=res_cb)

    # uid  : duh!
    # ranks: how many workers to use in the task's communicator
    # work : what method to call
    # args : what arguments to pass to the method
    tasks = [{'uid': 0, 'ranks': 3, 'work' : 'test_1', 'args': ['foo']},
             {'uid': 1, 'ranks': 4, 'work' : 'test_2', 'args': ['bar']},
             {'uid': 2, 'ranks': 4, 'work' : 'test_1', 'args': ['buz']},
             {'uid': 3, 'ranks': 3, 'work' : 'test_2', 'args': ['biz']},
            ]

    putter.put(tasks)

    time.sleep(30)





