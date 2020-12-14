# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import pytest

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.slurm import Slurm


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    def test_configure(self, mocked_init):

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {'cores_per_node': None}
        component._configure()
        node = os.environ['SLURM_NODELIST']
        nodes = os.environ['SLURM_NNODES']
        cores_per_node = os.environ['SLURM_CPUS_ON_NODE']

        assert component.node_list == [[node, node]]
        assert component.cores_per_node == cores_per_node
        assert component.gpus_per_node == 0
        assert component.lfs_per_node == {'path': None, 'size': 0}
    # ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
