#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager        import RMInfo
from radical.pilot.agent.resource_manager.torque import Torque

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TorqueTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.torque' % base

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_init):

        rm_torque = Torque(cfg=None, log=None, prof=None)
        rm_torque._log = mocked_logger

        rm_info = rm_torque._init_from_scratch(RMInfo({'cores_per_node': None}))

        node_names = sorted([n['node_name'] for n in rm_info.node_list])
        self.assertEqual(node_names, ['nodes1'])
        self.assertEqual(rm_info.cores_per_node, 2)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch_error(self, mocked_logger, mocked_init):

        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']

        rm_torque = Torque(cfg=None, log=None, prof=None)
        rm_torque._log = mocked_logger

        with self.assertRaises(RuntimeError):
            rm_torque._init_from_scratch(RMInfo())

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TorqueTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_error()


# ------------------------------------------------------------------------------

