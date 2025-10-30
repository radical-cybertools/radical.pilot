#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pytest

import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager       import RMInfo
from radical.pilot.agent.resource_manager.slurm import Slurm


# ------------------------------------------------------------------------------
#
class SlurmTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):

        path = os.path.dirname(__file__) + '/../test_config/resources.json'
        resources = ru.read_json(path)
        hostname  = ru.get_hostname()

        for host in resources.keys():
            if host in hostname:
                cls.host = host
                cls.resource = resources[host]
                break

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    @pytest.mark.skipif(
        'SLURM_NODELIST' not in os.environ,
        reason='test needs to run in Slurm allocation')
    def test_update_info(self, mocked_init):

        if not self.host:
            return

        rm_slurm = Slurm(cfg=None, log=None, prof=None)
        rm_slurm._log = mock.Mock()

        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': 0,
                                                      'gpus_per_node' : 0}))

        node = os.environ['SLURM_NODELIST'].split(',')[0]
        self.assertEqual(rm_info.node_list[0]['name'], node)

        self.assertEqual(rm_info.cores_per_node,
                         self.resource['cores_per_node'])
        self.assertEqual(rm_info.gpus_per_node,
                         self.resource['gpus_per_node'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = SlurmTestCase()
    tc.setUpClass()
    tc.test_update_info()


# ------------------------------------------------------------------------------

