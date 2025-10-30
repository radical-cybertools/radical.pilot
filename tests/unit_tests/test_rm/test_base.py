#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2021-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager import ResourceManager, RMInfo

base = os.path.abspath(os.path.dirname(__file__))


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
        # initialization from scratch will be tested separately

        path = '/tmp/rp_tests_%s' % os.getuid()
        ru.rec_makedir(path)
        reg = ru.zmq.Registry(path='/tmp/rp_tests_%s' % os.getuid())
        reg.start()

        rm_info = RMInfo({'requested_nodes': 1,
                          'requested_cores': 16,
                          'requested_gpus' : 2,
                          'node_list'      : [['node00', '1']],
                          'cores_per_node' : 16,
                          'gpus_per_node'  : 2})

        c = ru.zmq.RegistryClient(url=reg.addr)
        c.put('rm.resourcemanager', rm_info.as_dict())
        c.close()

        rm = ResourceManager(cfg=ru.TypedDict({'reg_addr': reg.addr}),
                             rcfg=ru.TypedDict(),
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
    def test_init_from_scratch(self, mocked_init):

        os.environ['LOCAL'] = '/tmp/local_folder/'

        cfg = ru.Config(cfg={'nodes'            : 1,
                             'cores'            : 16,
                             'gpus'             : 2,
                             'cores_per_node'   : 16,
                             'gpus_per_node'    : 2,
                             'lfs_path_per_node': '${LOCAL}',
                             'lfs_size_per_node': 100,
                             'resource_cfg'     : {}})

        rm = ResourceManager(cfg=None, rcfg=None, log=None, prof=None)
        rm._cfg  = cfg
        rm._rcfg = ru.Config(cfg={})

        rm._log  = mock.Mock()
        rm._prof = mock.Mock()

        def _init_from_scratch(rm_info):
            rm_info.node_list = rm._get_node_list([('node00', 16)], rm_info)
            rm_info.cores_per_node = rm_info['cores_per_node']
            return rm_info

        # RM specific method (to update node_list and cores_per_node if needed)
        rm.init_from_scratch = _init_from_scratch

        rm_info_output = rm._init_from_scratch()

        self.assertIsInstance(rm_info_output, RMInfo)
        # check some attributes
        self.assertEqual(rm_info_output.requested_nodes, cfg.nodes)
        self.assertEqual(rm_info_output.requested_cores, cfg.cores)
        self.assertEqual(rm_info_output.requested_gpus,  cfg.gpus)
        self.assertEqual(rm_info_output.cores_per_node,  cfg.cores_per_node)
        self.assertEqual(rm_info_output.gpus_per_node,   cfg.gpus_per_node)
        # lfs size and lfs path
        self.assertEqual(rm_info_output.lfs_per_node,    100)
        self.assertEqual(rm_info_output.lfs_path,        '/tmp/local_folder/')

        rm._cfg.update({'agents': {'agent.0000': {'target': 'node'}}})
        with self.assertRaises(RuntimeError):
            # `node_list` became empty b/c of `agent_node_list`
            rm._init_from_scratch()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_cores_cpus_map(self, mocked_init):

        tc_map = ru.read_json('%s/test_cases/test_cores_gpus_map.json' % base)

        rm = ResourceManager(cfg=None, rcfg=None, log=None, prof=None)
        rm._log  = mock.Mock()
        rm._prof = mock.Mock()

        for rm_info, rm_cfg, result in zip(tc_map['rm_info'],
                                           tc_map['rm_cfg'],
                                           tc_map['result']):

            def _init_from_scratch(rm_info_tc, rm_info_input):

                _rm_info = ru.TypedDict(rm_info_input)
                _rm_info.update(rm_info_tc)

                return _rm_info

            from functools import partial

            rm._rcfg = ru.TypedDict(rm_cfg['rcfg'])
            del rm_cfg['rcfg']
            rm._cfg  = ru.TypedDict(rm_cfg)
            rm.init_from_scratch = partial(_init_from_scratch, rm_info)

            if result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    rm._init_from_scratch()
            else:
                rm_info_output = rm._init_from_scratch()
                self.assertEqual(rm_info_output.node_list, result)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_set_info(self, mocked_init):

        rm = ResourceManager(cfg=None, rcfg=None, log=None, prof=None)

        with self.assertRaises(AssertionError):
            # required attributes are not set
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
    @mock.patch('radical.pilot.agent.launch_method.base.LaunchMethod')
    def test_find_launcher(self, mocked_lm, mocked_init):

        os.environ['LOCAL'] = '/tmp/local_folder/'

        cfg = ru.TypedDict({
            'cores'        : 16,
            'gpus'         : 2,
            })
        rcfg = ru.TypedDict({
                'cores_per_node'   : 16,
                'gpus_per_node'    : 2,
                'lfs_path_per_node': '${LOCAL}',
                'lfs_size_per_node': 100,
                'launch_methods'   : {
                    'order': ['SRUN'],
                    'SRUN' : {}
                }})

        rm = ResourceManager.create('FORK', cfg, rcfg, None, None)

        rm._launch_order = ['SRUN']
        rm._launchers    = {'SRUN': mocked_lm}
        rm._log          = mock.Mock()
        rm._prof         = mock.Mock()

        def mocked_can_launch_false(task):
            return False, 'error'
        mocked_lm.can_launch = mocked_can_launch_false

        self.assertEqual((None, None), rm.find_launcher(task={'uid': 'task0000'}))

        def mocked_can_launch_true(task):
            return True, ''
        mocked_lm.can_launch = mocked_can_launch_true
        self.assertEqual(rm.find_launcher(task={'uid': 'task0000'}),
                         (mocked_lm, 'SRUN'))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    @mock.patch('radical.pilot.agent.resource_manager.base.rpa.LaunchMethod')
    def test_prepare_launch_methods(self, mocked_lm, mocked_init):

        mocked_lm.create.return_value = mocked_lm

        rm = ResourceManager(cfg=None, log=None, prof=None)
        rm._log     = rm._prof = mock.Mock()
        rm._cfg     = ru.TypedDict({'pid'     : None,
                                    'reg_addr': None})
        rm._rm_info = ru.TypedDict({
            'launch_methods': {
                'SRUN': {'pre_exec_cached': []}
            }
        })

        # launching order not provided

        rm._prepare_launch_methods()
        self.assertEqual(rm._launchers['SRUN'], mocked_lm)
        self.assertEqual(rm._launch_order, ['SRUN'])

        # launching order provided

        rm._rm_info.launch_methods = {'order': ['SSH'],
                                      'SRUN' : {},
                                      'SSH'  : {}}
        rm._prepare_launch_methods()
        self.assertEqual(rm._launch_order, ['SSH'])

        # launching methods not provided

        rm._rm_info.launch_methods = {}
        with self.assertRaises(RuntimeError):
            rm._prepare_launch_methods()

        # raise exception for every launch method

        def lm_raise_exception(*args, **kwargs):
            raise Exception('LM Error')

        rm._rm_info.launch_methods = {'SRUN': {}, 'SSH': {}}
        mocked_lm.create = mock.MagicMock(side_effect=lm_raise_exception)
        # all LMs will be skipped, thus RuntimeError raised
        with self.assertRaises(RuntimeError):
            rm._prepare_launch_methods()
        # check that exception was logged (sign that LM exception was raised)
        self.assertTrue(rm._log.exception.called)

        # raise exception for one method and adjust launching order accordingly

        exception_raised = False

        def lm_raise_exception_once(*args, **kwargs):
            nonlocal exception_raised
            if not exception_raised:
                exception_raised = True
                raise Exception('LM Error')
            return mocked_lm

        rm._rm_info.launch_methods = {'SRUN': {}, 'SSH': {}}
        mocked_lm.create = mock.MagicMock(side_effect=lm_raise_exception_once)
        rm._prepare_launch_methods()
        # only second LM is considered successful
        self.assertEqual(rm._launch_order, ['SSH'])
        self.assertEqual(len(rm._launchers), 1)
        self.assertEqual(rm._launchers['SSH'], mocked_lm)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(ResourceManager, '__init__', return_value=None)
    def test_create_errors(self, mocked_init):

        with self.assertRaises(RuntimeError):
            ResourceManager.create('UNKNOWN_RM', None, None, None, None)

        from radical.pilot.agent.resource_manager.slurm import Slurm
        with self.assertRaises(TypeError):
            # ResourceManager Factory only available to base class
            Slurm.create('SLURM', None, None, None, None)

    # --------------------------------------------------------------------------
    #
    def test_batch_started(self):

        self.assertFalse(ResourceManager.batch_started())

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = RMBaseTestCase()
    tc.test_init_from_registry()
    tc.test_init_from_scratch()
    tc.test_cores_cpus_map()
    tc.test_set_info()
    tc.test_find_launcher()
    tc.test_prepare_launch_methods()
    tc.test_create_errors()
    tc.test_batch_started()

# ------------------------------------------------------------------------------

