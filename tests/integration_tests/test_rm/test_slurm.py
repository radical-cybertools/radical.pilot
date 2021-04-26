#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import socket

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.slurm import Slurm


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    def __init__(self):

        TestCase.__init__(self)
        self.setUpClass()


    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = socket.gethostname()

        for host in resources.keys():
            if host in hostname:
                cls.host = host
                cls.resource = resources[host]
                break


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_configure(self, mocked_init, mocked_Logger):

        if not self.host:
            return

        component = Slurm(cfg=None, session=None)
        component._log    = mocked_Logger
        component._cfg    = ru.Munch({'resource': self.host})
        component.lm_info = {'cores_per_node': None}
        component._configure()

        node = os.environ['SLURM_NODELIST']

        self.assertEqual(component.node_list, [[node, node]])
        self.assertEqual(component.cores_per_node, self.resource['cores_per_node'])
        self.assertEqual(component.gpus_per_node, self.resource['gpus_per_node'])
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tt = TestTask()
    tt.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
