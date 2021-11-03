#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager import ResourceManager, RMInfo


# ------------------------------------------------------------------------------
#
class RMBaseTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '_prepare_launch_methods')
    @mock.patch('radical.utils.zmq.server.Logger')
    @mock.patch('radical.utils.zmq.server.Profiler')
    def test_init_from_registry(self, mocked_prof, mocked_log, mocked_lm):

        # check initialization from registry data only,
        # initialization from scratch will be tested by each method separately

        reg = ru.zmq.Registry()
        reg.start()

        rm_info = RMInfo({'requested_nodes': 1,
                          'requested_cores': 16,
                          'requested_gpus' : 2,
                          'node_list'      : [['node00', '1']],
                          'cores_per_node' : 16,
                          'gpus_per_node'  : 2})

        c = ru.zmq.RegistryClient(url=reg.addr)
        c.put('rm.resourcemanager', rm_info)
        c.close()

        rm = ResourceManager(cfg=ru.Munch({'reg_addr': reg.addr}),
                             log=mock.Mock(), prof=mock.Mock())

        self.assertIsInstance(rm.info, RMInfo)
        # check some attributes
        self.assertEqual(rm.info.requested_cores, rm_info.requested_cores)
        self.assertEqual(rm.info.node_list, rm_info.node_list)

        reg.stop()
        reg.wait()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_set_info(self, mocked_init):

        rm = ResourceManager(cfg=None, log=None, prof=None)

        with self.assertRaises(KeyError):
            # required attributes are missed
            rm._set_info(RMInfo())

        rm_info = RMInfo({'requested_nodes': 1,
                          'requested_cores': 16,
                          'requested_gpus' : 2,
                          'node_list'      : [['node00', '1']],
                          'cores_per_node' : 16,
                          'gpus_per_node'  : 2})

        rm._set_info(rm_info)
        self.assertIsInstance(rm.info, RMInfo)

        rm_info.requested_nodes = None
        with self.assertRaises(AssertionError):
            # required attribute is not set
            rm._set_info(RMInfo(rm_info))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_prepare_info(self, mocked_init):

        os.environ['LOCAL'] = '/tmp/local_folder/'

        cfg = ru.Munch({'cores': 16,
                        'gpus' : 2,
                        'resource_cfg': {'cores_per_node'   : 16,
                                         'gpus_per_node'    : 2,
                                         'lfs_path_per_node': '${LOCAL}',
                                         'lfs_size_per_node': 100}})

        rm = ResourceManager(cfg=None, log=None, prof=None)
        rm._cfg = cfg
        rm_info = rm._prepare_info()

        self.assertIsInstance(rm_info, RMInfo)
        # check some attributes
        self.assertEqual(rm_info.requested_cores, cfg.cores)
        self.assertEqual(rm_info.requested_gpus,  cfg.gpus)
        self.assertEqual(rm_info.cores_per_node,
                         cfg.resource_cfg.cores_per_node)
        self.assertEqual(rm_info.gpus_per_node,
                         cfg.resource_cfg.gpus_per_node)
        # lfs size and lfs path
        self.assertEqual(rm_info.lfs_per_node, 100)
        self.assertEqual(rm_info.lfs_path,     '/tmp/local_folder/')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.launch_method.base.LaunchMethod')
    def test_find_launcher(self, mocked_lm, mocked_init):

        os.environ['LOCAL'] = '/tmp/local_folder/'

        cfg = ru.Munch({
            'cores'        : 16,
            'gpus'         : 2,
            'resource_cfg' : {
                'cores_per_node'   : 16,
                'gpus_per_node'    : 2,
                'lfs_path_per_node': '${LOCAL}',
                'lfs_size_per_node': 100,
                'launch_methods'   : {
                    'order': ['SRUN'],
                    'SRUN' : {}
                }}})

        rm = ResourceManager.create('FORK', cfg=cfg, log=mock.Mock(),
                                    prof=mock.Mock())

        rm._launch_order = ['SRUN']
        rm._launchers    = {'SRUN': mocked_lm}

        def mocked_can_launch(task):
            return False, ''
        mocked_lm.can_launch = mocked_can_launch

        self.assertIsNone(rm.find_launcher(task=None))

        def mocked_can_launch(task):
            return True, ''
        mocked_lm.can_launch = mocked_can_launch
        self.assertIs(rm.find_launcher(task=None), mocked_lm)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_init_from_scratch(self, mocked_init):

        rm = ResourceManager(cfg=None, log=None, prof=None)
        rm._log = mock.Mock()
        rm._cfg = ru.Munch({'agents': {'agent.0000': {'target': 'node'}}})

        with self.assertRaises(RuntimeError):
            # `node_list` became empty b/c of `agent_node_list`
            rm._init_from_scratch(RMInfo({'node_list': [['node00', '1']]}))

        with self.assertRaises(AssertionError):
            # `node_list` should not be empty or not being set
            # this attribute is set by an overwritten method `_update_info`
            rm._init_from_scratch(RMInfo({'node_list': []}))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.resource_manager.base.rpa.LaunchMethod')
    def test_prepare_launch_methods(self, mocked_lm, mocked_init):

        mocked_lm.create.return_value = mocked_lm

        rm = ResourceManager(cfg=None, log=None, prof=None)
        rm._log = rm._prof = mock.Mock()
        rm._cfg = ru.Munch({'pid'     : None,
                            'reg_addr': None,
                            'resource_cfg': {'launch_methods': {'SRUN': {}}}})

        rm._prepare_launch_methods(None)

        self.assertEqual(rm._launchers['SRUN'], mocked_lm)

        rm._cfg.resource_cfg.launch_methods = {}
        with self.assertRaises(RuntimeError):
            rm._prepare_launch_methods(None)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = RMBaseTestCase()
    tc.test_init_from_registry()
    tc.test_set_info()
    tc.test_prepare_info()
    tc.test_find_launcher()
    tc.test_init_from_scratch()
    tc.test_prepare_launch_methods()


# ------------------------------------------------------------------------------

