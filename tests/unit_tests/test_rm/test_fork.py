#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021-2022, The RADICAL-Cybertools Team'
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

        rm_fork = Fork(cfg=None, log=None, prof=None)
        rm_fork._cfg  = ru.TypedDict({'resource_cfg': {}})
        rm_fork._rcfg = ru.TypedDict()
        rm_fork._log = mocked_logger

        rm_fork._cfg.resource_cfg.fake_resources = False

        rm_info = RMInfo({'requested_cores': 1,
                          'cores_per_node' : 0})

        # `rm_info.cores_per_node` will be set by detected cores
        rm_info = rm_fork.init_from_scratch(rm_info)
        self.assertEqual(rm_info.cores_per_node,  mocked_mp_cpu_count())
        self.assertEqual(rm_info.requested_nodes, 1)

        # request less cores than available/detected
        rm_info.requested_cores = int(mocked_mp_cpu_count() / 2)

        rm_info.cores_per_node  = mocked_mp_cpu_count() + 1
        self.assertFalse(rm_fork._log.warn.called)
        rm_info = rm_fork.init_from_scratch(rm_info)
        self.assertTrue(rm_fork._log.warn.called)

        rm_info.cores_per_node = rm_info.requested_cores - 1
        with self.assertRaises(RuntimeError):
            # inconsistency with defined (`cores_per_node`) and available cores
            rm_fork.init_from_scratch(rm_info)

        # real resource, request more cores than available/detected
        rm_info.requested_cores = mocked_mp_cpu_count() * 10
        with self.assertRaises(RuntimeError):
            rm_fork.init_from_scratch(rm_info)

        # fake/virtual resource, request more cores than available/detected
        rm_fork._rcfg.fake_resources = True

        rm_info.requested_nodes = 0  # will be calculated during init
        rm_info.requested_cores = mocked_mp_cpu_count() * 10

        rm_info = rm_fork.init_from_scratch(rm_info)
        self.assertGreater(rm_info.requested_cores, mocked_mp_cpu_count())
        self.assertGreater(rm_info.requested_nodes, 1)
        self.assertEqual(len(rm_info.node_list), rm_info.requested_nodes)


# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = ForkTestCase()
    tc.test_init_from_scratch()


# ------------------------------------------------------------------------------
