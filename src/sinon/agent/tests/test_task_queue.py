#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import uuid
import Queue
import unittest
import threading
import multiprocessing

from radical.agent import Task, TaskQueue


#-----------------------------------------------------------------------------
#
class Test_TaskQueue(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test_task_queue_put_get_threading(self):
        num_worker_threads = 4

        # create a new queue
        tq = TaskQueue()
        for i in range(0, 100):
            tq.put(Task(uuid.uuid4(), '/bin/sleep', ['60'], 1, None, None))

        results = multiprocessing.Queue()

        _thread_terminate = False

        # define the worker functions
        def _worker():
            while _thread_terminate is not True:
                item = tq.get()
                results.put(item)
                tq.task_done()

        threads = list()
        for i in range(num_worker_threads):
            t = threading.Thread(target=_worker)
            t.daemon = True
            t.start()
            threads.append(t)

        tq.join()
        assert tq.empty() is True
        assert results.empty() is False

    #-------------------------------------------------------------------------
    #
    def test_task_queue_put_get_multiprocessing(self):
        # create a new queue
        tq = TaskQueue()
        for i in range(0, 100):
            tq.put(Task(uuid.uuid4(), '/bin/sleep', ['60'], 1, None, None))

        results = multiprocessing.Queue()

        # define the worker class
        class _Worker(multiprocessing.Process):
            def __init__(self, task_queue):
                # init superclass
                super(_Worker, self).__init__()

                self.task_queue = task_queue
                self.kill_received = False

            def run(self):
                while not self.kill_received:
                    try:
                        job = self.task_queue.get()
                        results.put(job)
                        self.task_queue.task_done()
                    except Queue.Empty:
                        break

        worker = _Worker(tq)
        worker.start()

        tq.join()
        worker.terminate()

        assert results.empty() is False
