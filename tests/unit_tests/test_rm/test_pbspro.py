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

        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.pbs' % base
        os.environ['SAGA_PPN']     = '0'
        os.environ['NODE_COUNT']   = '2'
        os.environ['NUM_PPN']      = '4'
        os.environ['NUM_PES']      = '1'
        os.environ['PBS_JOBID']    = '482125'

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch.object(PBSPro, '_parse_pbspro_vnodes',
                       return_value=['nodes1', 'nodes2'])
    @mock.patch('radical.utils.Logger')
    def test_update_info(self, mocked_logger, mocked_vnodes, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mocked_logger

        rm_info = rm_pbspro._update_info(RMInfo({'gpus_per_node': 1}))

        self.assertEqual(rm_info.node_list, [['nodes1', '1'], ['nodes2', '2']])
        self.assertEqual(rm_info.cores_per_node, 4)
        self.assertEqual(rm_info.gpus_per_node,  1)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info_error(self, mocked_logger, mocked_init):

        pbs_nodefile = os.environ.get('PBS_NODEFILE')
        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mocked_logger

        with self.assertRaises(RuntimeError):
            rm_pbspro._update_info(None)
        os.environ['PBS_NODEFILE'] = pbs_nodefile

        for ppn_env_var in ['NUM_PPN', 'SAGA_PPN']:
            if ppn_env_var in os.environ:
                del os.environ[ppn_env_var]

        with self.assertRaises(RuntimeError):
            # both $NUM_PPN and $SAGA_PPN were not set
            rm_pbspro._update_info(None)

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
    tc.test_update_info()
    tc.test_update_info_error()
    tc.test_parse_pbspro_vnodes()
    tc.test_parse_pbspro_vnodes_error()


# ------------------------------------------------------------------------------

