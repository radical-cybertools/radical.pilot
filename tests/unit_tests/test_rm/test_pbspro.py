#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

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
    def setUp(self) -> None:

        os.environ['PBS_JOBID']    = '12345'
        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.pbs' % base

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    def test_init_from_scratch(self, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mock.Mock()

        with mock.patch('radical.pilot.agent.resource_manager.pbspro.'
                        'ru.sh_callout') as mocked_callout:
            mocked_callout.return_value = [
                'exec_vnode = (vnode1:cpu=10)+(vnode2:cpu=10)', '', 0]

            rm_info = rm_pbspro.init_from_scratch(RMInfo())

            self.assertEqual(rm_info.cores_per_node, 10)
            self.assertEqual(len(rm_info.node_list), 2)

        rm_pbspro._parse_pbspro_vnodes = mock.Mock(
            side_effect=RuntimeError('qstat failed: exception message'))

        rm_info = rm_pbspro.init_from_scratch(RMInfo({'cores_per_node': 15}))

        # will be used "nodelist.pbs" ($PBS_NODEFILE) with node defined there

        self.assertEqual(rm_info.cores_per_node, 15)
        self.assertEqual(len(rm_info.node_list), 1)
        self.assertEqual(rm_info.node_list[0]['name'], 'nodes1')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    def test_init_from_scratch_error(self, mocked_init):

        rm_info = RMInfo()

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mock.Mock()

        rm_pbspro._parse_pbspro_vnodes = mock.Mock(side_effect=RuntimeError)
        with self.assertRaises(RuntimeError):
            rm_pbspro.init_from_scratch(rm_info)

        rm_pbspro._parse_pbspro_vnodes.side_effect = ValueError

        rm_info.cores_per_node = 0
        with self.assertRaises(RuntimeError):
            # no vnodes parsing and no cores_per_node from config
            rm_pbspro.init_from_scratch(rm_info)

        rm_info.cores_per_node = 10
        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']
        with self.assertRaises(RuntimeError):
            # no vnodes parsing and no $PBS_NODEFILE
            rm_pbspro.init_from_scratch(rm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    def test_parse_pbspro_vnodes(self, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mock.Mock()

        with mock.patch('radical.pilot.agent.resource_manager.pbspro.'
                        'ru.sh_callout') as mocked_callout:
            mocked_callout.return_value = [
                'exec_vnode = (vnode1:cpu=10)+(vnode2:cpu=10)+\n'
                '(vnode3:cpu=10)\n'
                'other_attr = some_value', '', 0]

            nodes, cores_per_node = rm_pbspro._parse_pbspro_vnodes()

            self.assertEqual(nodes, ['vnode1', 'vnode2', 'vnode3'])
            self.assertEqual(cores_per_node, 10)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(PBSPro, '__init__', return_value=None)
    def test_parse_pbspro_vnodes_error(self, mocked_init):

        rm_pbspro = PBSPro(cfg=None, log=None, prof=None)
        rm_pbspro._log = mock.Mock()

        with mock.patch('radical.pilot.agent.resource_manager.pbspro.'
                        'ru.sh_callout') as mocked_callout:

            mocked_callout.return_value = [
                'exec_vnode = (vnode1:cpu=1)+(vnode2:cpu=10)', '', 0]

            # different sizes of allocated nodes
            with self.assertRaises(RuntimeError):
                rm_pbspro._parse_pbspro_vnodes()

            mocked_callout.return_value = [
                'exec_vnode = (vnode1:cpu=1)', 'some error', 1]

            # qstat failed (error comes from `ru.sh_callout`)
            with self.assertRaises(RuntimeError):
                rm_pbspro._parse_pbspro_vnodes()

        if 'PBS_JOBID' in os.environ:
            del os.environ['PBS_JOBID']

        with self.assertRaises(RuntimeError):
            # $PBS_JOBID not set
            rm_pbspro._parse_pbspro_vnodes()

    # --------------------------------------------------------------------------
    #
    def test_batch_started(self):

        saved_batch_id = os.getenv('PBS_JOBID')

        os.environ['PBS_JOBID'] = '12345'
        self.assertTrue(PBSPro.batch_started())

        del os.environ['PBS_JOBID']
        self.assertFalse(PBSPro.batch_started())

        if saved_batch_id is not None:
            os.environ['PBS_JOBID'] = saved_batch_id

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = PBSProTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_error()
    tc.test_parse_pbspro_vnodes()
    tc.test_parse_pbspro_vnodes_error()
    tc.test_batch_started()

# ------------------------------------------------------------------------------

