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

    # --------------------------------------------------------------------------
    #
    def setUp(self) -> dict:
        path = os.path.dirname(__file__) + '../test_config/resources.json'
        resources = ru.read_json(path)
        hostname = socket.gethostname()

        for host in resources.keys():
            if host in hostname:
                return resources[host]

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    def test_configure(self, mocked_init):

        os.environ['SLURM_NODELIST']     = 'node_1'
        os.environ['SLURM_NPROCS']       = '48'
        os.environ['SLURM_NNODES']       = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        cfg = self.setUp()
        component = Slurm(cfg=None, session=None)
        component._log    = ru.Logger('dummy')
        component._cfg    = {}
        component.lm_info = {'cores_per_node': None}
        component._configure()

        node = os.environ['SLURM_NODELIST']

        self.assertEqual(component.node_list, [[node, node]])
        self.assertEqual(component.cores_per_node, cfg['cores_per_node'])
        self.assertEqual(component.gpus_per_node, ['gpus_per_node'])
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tt = TestTask()
    tt.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
