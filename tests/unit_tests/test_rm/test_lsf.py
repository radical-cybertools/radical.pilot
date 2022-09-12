#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager     import RMInfo
from radical.pilot.agent.resource_manager.lsf import LSF

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class LSFTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._host_file_path = '%s/test_cases/nodelist.lsf' % base

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_init):

        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

        rm_lsf = LSF(cfg=None, log=None, prof=None)
        rm_lsf._log = mocked_logger
        rm_lsf._cfg = ru.TypedDict({'resource_cfg': {'launch_methods': {}}})

        rm_info = rm_lsf._init_from_scratch(RMInfo({'cores_per_node': None}))

        node_names = sorted([n['node_name'] for n in rm_info.node_list])
        self.assertEqual(node_names, ['nodes1', 'nodes2'])
        self.assertEqual(rm_info.cores_per_node,   20)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch_error(self, mocked_logger, mocked_init):

        rm_lsf = LSF(cfg=None, log=None, prof=None)
        rm_lsf._log = mocked_logger

        if 'LSB_DJOB_HOSTFILE' in os.environ:
            del os.environ['LSB_DJOB_HOSTFILE']
        with self.assertRaises(RuntimeError):
            rm_lsf._init_from_scratch(RMInfo())

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = LSFTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_error()


# ------------------------------------------------------------------------------
