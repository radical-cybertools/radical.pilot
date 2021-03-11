#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.lsf_summit import LSF_SUMMIT


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        cls.host = 'summit'
        cls.resource = resources[cls.host]

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        component = LSF_SUMMIT(cfg=None, session=None)
        component._log    = mocked_Logger
        component._cfg    = {}
        component._configure()

        self.assertEqual(component.sockets_per_node, 1)
        self.assertEqual(component.cores_per_socket, self.resource['cores_per_node'])
        self.assertEqual(component.gpus_per_socket, 0)
        self.assertEqual(component.cores_per_node, self.resource['cores_per_node'])
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

