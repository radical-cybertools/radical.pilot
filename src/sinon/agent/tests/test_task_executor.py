#!/usr/bin/env python
# encoding: utf-8

__author__    = "Ole Weidner"
__copyright__ = "Copyright 2013, Ole Weidner"
__license__   = "MIT"

import os
import uuid
import time
import Queue
import tempfile
import unittest

from radical.agent import Task, TaskQueue, ResultQueue, TaskExecutor, ExecutionEnvironment


#-----------------------------------------------------------------------------
#
class Test_TaskExecutor(unittest.TestCase):

    def setUp(self):
        self.tmp_pbsnodes_file = tempfile.NamedTemporaryFile(delete=False)
        self.tmp_pbsnodes_file.write('hostA\n')
        self.tmp_pbsnodes_file.write('hostB\n')
        self.tmp_pbsnodes_file.write('hostC\n')
        self.tmp_pbsnodes_file.write('hostD\n')
        self.tmp_pbsnodes_file.flush()

    def tearDown(self):
        os.remove(self.tmp_pbsnodes_file.name)

    def failUnless(self, expr):
        # St00pid speling.
        return self.assertTrue(expr)

    def failIf(self, expr):
        # St00pid speling.
        return self.assertFalse(expr)

    #-------------------------------------------------------------------------
    #
    def test_task_executor_emtpy_queue_without_pbs_nodefile(self):
        ee = ExecutionEnvironment.discover()

        # create a new set of queue
        rq = ResultQueue()
        tq = TaskQueue()

        te = TaskExecutor(task_queue=tq,
                          result_queue=rq,
                          execution_environment=ee
                          )

        te.start()
        assert te.num_workers == 1
        te.terminate()

    #-------------------------------------------------------------------------
    #
    def test_task_executor_populated_queue_without_pbs_nodefile(self):
        ee = ExecutionEnvironment.discover()

        rq = ResultQueue()
        tq = TaskQueue()
        for i in range(0, 20):
            tq.put(Task(uid=str(uuid.uuid4()),
                        executable='/bin/sleep',
                        arguments=['10'],
                        numcores=1,
                        stdout=None,
                        stderr=None))

        te = TaskExecutor(task_queue=tq,
                          result_queue=rq,
                          execution_environment=ee,
                          terminate_on_emtpy_queue=True,
                          )

        te.start()
        assert te.num_workers == 1
        te.join()

        while True:
            try:
                result = rq.get_nowait()
                print result
            except Queue.Empty:
                return

    #-------------------------------------------------------------------------
    #
    def test_task_executor_emtpy_queue_with_pbs_nodefile(self):
        os.environ['PBS_NODEFILE'] = self.tmp_pbsnodes_file.name
        ee = ExecutionEnvironment.discover()

        rq = ResultQueue()
        tq = TaskQueue()

        te = TaskExecutor(task_queue=tq,
                          result_queue=rq,
                          execution_environment=ee,
                          terminate_on_emtpy_queue=True,
                          )

        te.start()
        assert te.num_workers == 4
        te.join()


        # restore environment and delete tempfile
        if os.environ.get('PBS_NODEFILE') is not None:
            del os.environ['PBS_NODEFILE']
