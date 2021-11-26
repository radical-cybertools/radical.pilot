#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager        import RMInfo
from radical.pilot.agent.resource_manager.cobalt import Cobalt


# ------------------------------------------------------------------------------
#
class CobaltTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    def test_init_from_scratch(self, mocked_init):

        os.environ['COBALT_PARTNAME'] = '1'  # node id -> node name: 'nid00001'

        rm_cobalt = Cobalt(cfg=None, log=None, prof=None)

        rm_info = rm_cobalt._init_from_scratch(RMInfo({'cores_per_node': 1}))

        self.assertEqual(rm_info.node_list[0]['node_name'], 'nid00001')
        self.assertEqual(rm_info.node_list[0]['cores'], [0])  # list of cores

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Cobalt, '__init__', return_value=None)
    def test_init_from_scratch_error(self, mocked_init):

        rm_cobalt = Cobalt(cfg=None, log=None, prof=None)

        with self.assertRaises(RuntimeError):
            # `cores_per_node` not defined
            rm_cobalt._init_from_scratch(RMInfo({'cores_per_node': None}))

        for cobalt_env_var in ['COBALT_NODEFILE', 'COBALT_PARTNAME']:
            if cobalt_env_var in os.environ:
                del os.environ[cobalt_env_var]
        with self.assertRaises(RuntimeError):
            # both $COBALT_NODEFILE and $COBALT_PARTNAME are not set
            rm_cobalt._init_from_scratch(RMInfo({'cores_per_node': 1}))

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = CobaltTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_error()


# ------------------------------------------------------------------------------
