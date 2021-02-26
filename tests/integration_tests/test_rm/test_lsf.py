#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import socket

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.lsf_summit import LSF_SUMMIT


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    def setUp(self) -> dict:
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = 'summit'

        for host in resources.keys():
            if host in hostname:
                return resources[host]

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        cfg = self.setUp()
        component = LSF_SUMMIT(cfg=None, session=None)
        component._log    = mocked_Logger
        component._cfg    = {}
        component._configure()

        self.assertEqual(component.sockets_per_node, 1)
        self.assertEqual(component.cores_per_socket, cfg['cores_per_node'])
        self.assertEqual(component.gpus_per_socket, 0)
        self.assertEqual(component.cores_per_node, cfg['cores_per_node'])
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None,
                                                 'size': 0})
        self.assertEqual(component.mem_per_node, 0)

# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tt = TestTask()
    tt.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

