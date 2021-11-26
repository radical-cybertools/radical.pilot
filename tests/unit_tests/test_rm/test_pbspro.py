#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager        import RMInfo
from radical.pilot.agent.resource_manager.pbspro import PBSPro

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class PBSProTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        os.environ['SAGA_PPN']     = '0'
        os.environ['NUM_PPN']      = '4'
        os.environ['PBS_JOBID']    = '482125'

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch.object(PBSPro, '_parse_pbspro_vnodes',
                       return_value=['nodes1', 'nodes2'])
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_vnodes, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mocked_logger

        rm_info = rm_pbspro._init_from_scratch(RMInfo({'cores_per_node': None,
                                                       'gpus_per_node': 1}))

        node_names = sorted([n['node_name'] for n in rm_info.node_list])
        self.assertEqual(node_names, ['nodes1', 'nodes2'])
        self.assertEqual(rm_info.cores_per_node, 4)
        self.assertEqual(rm_info.gpus_per_node,  1)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch_error(self, mocked_logger, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mocked_logger

        for ppn_env_var in ['NUM_PPN', 'SAGA_PPN']:
            if ppn_env_var in os.environ:
                del os.environ[ppn_env_var]
        with self.assertRaises(RuntimeError):
            # both $NUM_PPN and $SAGA_PPN were not set
            rm_pbspro._init_from_scratch(RMInfo())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('subprocess.check_output',
                return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
    @mock.patch('radical.utils.Logger')
    def test_parse_pbspro_vnodes(self, mocked_logger, mocked_sp, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mocked_logger

        vnodes = rm_pbspro._parse_pbspro_vnodes()

        self.assertEqual(vnodes, ['vnode1', 'vnode2'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    def test_parse_pbspro_vnodes_error(self, mocked_init):

        if 'PBS_JOBID' in os.environ:
            del os.environ['PBS_JOBID']

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)

        with self.assertRaises(RuntimeError):
            # $PBS_JOBID not set
            rm_pbspro._parse_pbspro_vnodes()

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = PBSProTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch()
    tc.test_parse_pbspro_vnodes()
    tc.test_parse_pbspro_vnodes_error()


# ------------------------------------------------------------------------------

