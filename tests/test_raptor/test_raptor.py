#!/usr/bin/env python3

import time

from   unittest import mock
from   unittest import TestCase

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestWorker(TestCase):

    def test_alloc(self):

        cfg = {'uid': 'worker.0000',
               'cores': 8,
               'gpus' : 2,
               'sid'  : str(time.time())}

        rp.utils.Component.register_subscriber = mock.Mock()
        rp.utils.Component.register_publisher  = mock.Mock()

        ru.zmq.Putter = mock.Mock()
        ru.zmq.Getter = mock.Mock()

        worker = rp.task_overlay.Worker(cfg, session=mock.Mock())

        task = {'cores': 1,
                'gpus' : 1}

        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])

        worker._alloc_task(task)
        self.assertEqual(worker._resources['cores'], [1, 0, 0, 0, 0, 0, 0, 0])

        worker._dealloc_task(task)
        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestWorker()
    tc.test_alloc()


# ------------------------------------------------------------------------------

