#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager      import RMInfo
from radical.pilot.agent.resource_manager.fork import Fork


# ------------------------------------------------------------------------------
#
class ForkTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__', return_value=None)
    @mock.patch('multiprocessing.cpu_count', return_value=24)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_mp_cpu_count,
                               mocked_init):

        rm_info = RMInfo({'requested_nodes': 1,
                          'requested_cores': 16,
                          'cores_per_node' : 16})

        rm_fork = Fork(cfg=None, log=None, prof=None)
        rm_fork._cfg = ru.TypedDict({'resource_cfg': {'fake_resources': False}})
        rm_fork._log = mocked_logger

        rm_info = rm_fork._init_from_scratch(rm_info)
        self.assertEqual(len(rm_info.node_list), 1)

        # not fake resource, but request more cores than available
        rm_info.requested_cores = 36
        with self.assertRaises(RuntimeError):
            rm_fork._init_from_scratch(rm_info)

        # fake resource, request more cores than available
        rm_fork._cfg.resource_cfg.fake_resources = True
        rm_info = rm_fork._init_from_scratch(rm_info)
        self.assertGreater(rm_info.requested_cores, mocked_mp_cpu_count())

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = ForkTestCase()
    tc.test_init_from_scratch()


# ------------------------------------------------------------------------------
