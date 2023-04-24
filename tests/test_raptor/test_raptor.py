#!/usr/bin/env python3

# pylint: disable=unused-argument, no-value-for-parameter

import os
import time

from   unittest import mock
from   unittest import TestCase

import radical.utils as ru
import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestWorker(TestCase):

    def read_json_side_effect(self, fname=None):
        return {'sub': '', 'pub': '', 'cores_per_rank': 8, 'gpus_per_rank': 2}


    @mock.patch('radical.utils.zmq.Subscriber')
    @mock.patch('radical.utils.zmq.Publisher')
    @mock.patch('radical.utils.zmq.Putter')
    @mock.patch('radical.utils.read_json', side_effect=read_json_side_effect)
    @mock.patch('threading.Event')
    @mock.patch('threading.Thread')
    def test_alloc(self, mock_1, mock_2, mock_3, mock_4, mock_5, mock_6):

        cfg = ru.Config(cfg={'uid'           : 'worker.0000',
                             'sid'           : str(time.time()),
                             'info'          : {},
                             'cores_per_rank': 8,
                             'gpus_per_rank' : 2})

        ru.zmq.Subscriber = mock.Mock()
        ru.zmq.Publisher  = mock.Mock()
        ru.zmq.Putter     = mock.Mock()

        rp.utils.Component.register_subscriber = mock.Mock()
        rp.utils.Component.register_publisher  = mock.Mock()

        ru.zmq.Putter = mock.Mock()
        ru.zmq.Getter = mock.Mock()

        rp.raptor.Worker.publish       = mock.Mock()
        rp.raptor.Worker._ts_addr      = 'tcp://localhost:1'
        rp.raptor.Worker._res_addr_put = 'tcp://localhost:2'
        rp.raptor.Worker._req_addr_get = 'tcp://localhost:3'

        os.environ['RP_TASK_ID']       = 'task.000000'
        os.environ['RP_TASK_SANDBOX']  = '/tmp'
        os.environ['RP_PILOT_SANDBOX'] = '/tmp'
        os.environ['RP_RANKS']         = str(8)

        with ru.ru_open('/tmp/control_pubsub.cfg', 'w') as fout:
            fout.write('{"sub": "tcp://localhost:10000", '
                       ' "pub": "tcp://localhost:10001"}\n')

        worker = rp.raptor.DefaultWorker(cfg)

        task_1 = {'uid': 'task.0000', 'cores': 1, 'gpus' : 1}
        task_2 = {'uid': 'task.0001', 'cores': 2, 'gpus' : 1}
        task_3 = {'uid': 'task.0002', 'cores': 3, 'gpus' : 1}


        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])

        worker._alloc(task_1)
        self.assertEqual(worker._resources['cores'], [1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 0])
        self.assertEqual(task_1['slots'], {'cores': [0], 'gpus': [0]})

        worker._alloc(task_2)
        self.assertEqual(worker._resources['cores'], [1, 1, 1, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 1])
        self.assertEqual(task_2['slots'], {'cores': [1, 2], 'gpus': [1]})

        worker._alloc(task_3)
        self.assertEqual(worker._resources['cores'], [1, 1, 1, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 1])
        self.assertEqual(task_3.get('slots'), None)

        worker._dealloc(task_1)
        self.assertEqual(worker._resources['cores'], [0, 1, 1, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [0, 1])

        worker._alloc(task_3)
        self.assertEqual(worker._resources['cores'], [1, 1, 1, 1, 1, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 1])
        self.assertEqual(task_3['slots'], {'cores': [0, 3, 4], 'gpus': [0]})

        worker._dealloc(task_2)
        self.assertEqual(worker._resources['cores'], [1, 0, 0, 1, 1, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 0])

        worker._dealloc(task_3)
        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [0, 0])

        os.unlink('/tmp/control_pubsub.cfg')


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestWorker()
    tc.test_alloc()


# ------------------------------------------------------------------------------

