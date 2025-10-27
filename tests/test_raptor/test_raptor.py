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
        return {'addr_sub': '', 'addr_pub': '', 'cores_per_rank': 8, 'gpus_per_rank': 2}

    def dict_merge_side_effect(self, fname=None):
        return {'addr_sub': '', 'addr_pub': '', 'cores_per_rank': 8, 'gpus_per_rank': 2}

    class MyConfig(ru.TypedDict):
        def __init__(self, cfg=None, from_dict=None):
            if cfg: super().__init__(from_dict=cfg)
            else  : super().__init__(from_dict=from_dict)

    class MyRegistry(ru.TypedDict):

        def __init__(self, url):

            data = {
                       'cfg': {},
                       'bridges.state_pubsub': {
                           'addr_sub': 'tcp://localhost:10000',
                           'addr_pub': 'tcp://localhost:10001'
                       },
                       'bridges.control_pubsub': {
                           'addr_sub': 'tcp://localhost:10000',
                           'addr_pub': 'tcp://localhost:10001'
                       },
                       'raptor.task.000000.cfg': {
                           'cores_per_rank': 8,
                           'gpus_per_rank' : 2
                       },
                       'rcfg.raptor.hb_delay': 5

                   }

            super().__init__(from_dict=data)

        def dump(self, *args, **kwargs): pass


    @mock.patch('radical.utils.zmq.RegistryClient', MyRegistry)
    @mock.patch('radical.utils.zmq.Subscriber')
    @mock.patch('radical.utils.zmq.Publisher')
    @mock.patch('radical.utils.zmq.Putter')
    @mock.patch('radical.utils.read_json', side_effect=read_json_side_effect)
    @mock.patch('radical.utils.Config', MyConfig)
    @mock.patch('threading.Event')
    @mock.patch('threading.Thread')
    def test_alloc(self, *args):

        cfg = ru.Config(from_dict={'uid'           : 'worker.0000',
                                   'sid'           : str(time.time()),
                                   'info'          : {},
                                   'cores_per_rank': 8,
                                   'gpus_per_rank' : 2})

        ru.zmq.Subscriber = mock.Mock()
        ru.zmq.Publisher  = mock.Mock()
        ru.zmq.Putter     = mock.Mock()

        rp.utils.BaseComponent.register_subscriber = mock.Mock()
        rp.utils.BaseComponent.register_publisher  = mock.Mock()

        ru.zmq.Putter = mock.Mock()
        ru.zmq.Getter = mock.Mock()

        rp.raptor.Worker.publish          = mock.Mock()
        rp.raptor.Worker._ts_addr         = 'tcp://localhost:1'
        rp.raptor.Worker._res_addr_put    = 'tcp://localhost:2'
        rp.raptor.Worker._req_addr_get    = 'tcp://localhost:3'

        os.environ['cores_per_rank']      = '8'
        os.environ['gpus_per_rank']       = '2'
        os.environ['RP_TASK_ID']          = 'task.000000'
        os.environ['RP_TASK_SANDBOX']     = '/tmp'
        os.environ['RP_TASK_RUNDIR']      = '/tmp'
        os.environ['RP_PILOT_SANDBOX']    = '/tmp'
        os.environ['RP_RANKS']            = str(8)
        os.environ['RP_SESSION_ID']       = 'foo'
        os.environ['RP_REGISTRY_ADDRESS'] = 'tcp://localhost:10001'

        worker = rp.raptor.DefaultWorker('master.0000')

        task_1 = {'uid': 'task.0000', 'cores': 1, 'gpus' : 1}
        task_2 = {'uid': 'task.0001', 'cores': 2, 'gpus' : 1}
        task_3 = {'uid': 'task.0002', 'cores': 3, 'gpus' : 1}


        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])

        worker._alloc(task_1)
        self.assertEqual(worker._resources['cores'], [1, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 0])
        self.assertEqual(task_1['slots'], [{'cores': [0], 'gpus': [0]}])

        worker._alloc(task_2)
        self.assertEqual(worker._resources['cores'], [1, 1, 1, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 1])
        self.assertEqual(task_2['slots'], [{'cores': [1, 2], 'gpus': [1]}])

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
        self.assertEqual(task_3['slots'], [{'cores': [0, 3, 4], 'gpus': [0]}])

        worker._dealloc(task_2)
        self.assertEqual(worker._resources['cores'], [1, 0, 0, 1, 1, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [1, 0])

        worker._dealloc(task_3)
        self.assertEqual(worker._resources['cores'], [0, 0, 0, 0, 0, 0, 0, 0])
        self.assertEqual(worker._resources['gpus' ], [0, 0])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestWorker()
    tc.test_alloc()


# ------------------------------------------------------------------------------

