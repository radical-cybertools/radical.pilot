#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager       import RMInfo
from radical.pilot.agent.resource_manager.slurm import Slurm


# ------------------------------------------------------------------------------
#
class SlurmTestCase(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        os.environ['SLURM_NODELIST']     = 'node-[1-2]'
        os.environ['SLURM_NPROCS']       = '48'
        os.environ['SLURM_NNODES']       = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info(self, mocked_logger, mocked_init):

        rm_slurm = Slurm(cfg=None, log=None, prof=None)
        rm_slurm._log = mocked_logger

        rm_info = rm_slurm._update_info(RMInfo({'cores_per_node': 0,
                                                'gpus_per_node' : 1}))

        self.assertEqual(rm_info.node_list, [['node-1', '1'], ['node-2', '2']])
        self.assertEqual(rm_info.cores_per_node, 24)
        self.assertEqual(rm_info.gpus_per_node,  1)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_update_info_error(self, mocked_logger, mocked_init):

        rm_slurm = Slurm(cfg=None, log=None, prof=None)
        rm_slurm._log = mocked_logger

        if 'SLURM_CPUS_ON_NODE' in os.environ:
            del os.environ['SLURM_CPUS_ON_NODE']
        with self.assertRaises(RuntimeError):
            rm_slurm._update_info(None)
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        if 'SLURM_NNODES' in os.environ:
            del os.environ['SLURM_NNODES']
        with self.assertRaises(RuntimeError):
            rm_slurm._update_info(None)
        os.environ['SLURM_NNODES'] = '2'

        if 'SLURM_NPROCS' in os.environ:
            del os.environ['SLURM_NPROCS']
        with self.assertRaises(RuntimeError):
            rm_slurm._update_info(None)
        os.environ['SLURM_NPROCS'] = '48'

        if 'SLURM_NODELIST' in os.environ:
            del os.environ['SLURM_NODELIST']
        with self.assertRaises(RuntimeError):
            rm_slurm._update_info(None)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = SlurmTestCase()
    tc.test_update_info()
    tc.test_update_info_error()


# ------------------------------------------------------------------------------
