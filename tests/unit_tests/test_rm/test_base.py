# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager import ResourceManager, RMInfo


# ------------------------------------------------------------------------------
#
class RMBaseTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_init_from_registry(self):

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
        c.put('rm.ResourceManager', rm_info)
        c.close()

        rm = ResourceManager(cfg=ru.Munch({'reg_addr': reg.addr}),
                             log=mock.Mock(), prof=mock.Mock())

        self.assertIsInstance(rm.info, RMInfo)
        # check some attributes
        self.assertEqual(rm.info.requested_cores, rm_info.requested_cores)
        self.assertEqual(rm.info.node_list,       rm_info.node_list)

        rm._reg.close()
        reg.stop()

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

        cfg = ru.Munch({'cores': 16,
                        'gpus' : 2,
                        'resource_cfg': {'cores_per_node': 16,
                                         'gpus_per_node' : 2}})

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

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.resource_manager.base.rpa.LaunchMethod')
    def test_init_from_scratch(self, mocked_lm, mocked_init):

        rm_info = RMInfo({'requested_nodes': 1,
                          'requested_cores': 16,
                          'requested_gpus': 2,
                          'node_list': [['node00', '1']],
                          'cores_per_node': 16,
                          'gpus_per_node': 2})

        rm = ResourceManager(cfg=None, log=None, prof=None)
        rm._log = rm._prof = mock.Mock()
        rm._cfg = ru.Munch({'pid'     : None,
                            'reg_addr': None,
                            'resource_cfg': {'launch_methods': {'SRUN': {}}}})
        mocked_lm.create.return_value = mocked_lm

        rm._init_from_scratch(rm_info)

        self.assertEqual(rm._launchers['SRUN'], mocked_lm)

        rm._cfg.resource_cfg.launch_methods = {}
        with self.assertRaises(RuntimeError):
            # no launch methods
            rm._init_from_scratch(rm_info)

        rm_info.node_list = []
        with self.assertRaises(AssertionError):
            # `node_list` should not be empty or not being set
            # this attribute is set by an overwritten method `_update_info`
            rm._init_from_scratch(rm_info)

# ------------------------------------------------------------------------------

