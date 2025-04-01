#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2021-2023, The RADICAL-Cybertools Team'
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
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch(self, mocked_logger, mocked_init):

        rm_slurm = Slurm(cfg=None, log=None, prof=None)
        rm_slurm._log = mocked_logger

        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None,
                                                     'gpus_per_node' : 1}))

        node_names = sorted([n['name'] for n in rm_info.node_list])
        self.assertEqual(node_names, ['node-1', 'node-2'])
        self.assertEqual(rm_info.cores_per_node, 24)
        self.assertEqual(rm_info.gpus_per_node,  1)

        # test `gpus_per_node` discovery

        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None,
                                                     'gpus_per_node' : 0}))
        self.assertEqual(rm_info.gpus_per_node, 0)

        os.environ['SLURM_GPUS_ON_NODE'] = '10'
        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None,
                                                     'gpus_per_node' : 0}))
        self.assertEqual(rm_info.gpus_per_node, 10)

        del os.environ['SLURM_GPUS_ON_NODE']
        os.environ['SLURM_JOB_GPUS'] = '1,2,3,4'  # 4 allocated GPUs per node
        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None,
                                                     'gpus_per_node' : 0}))
        self.assertEqual(rm_info.gpus_per_node, 4)

        del os.environ['SLURM_JOB_GPUS']
        os.environ['GPU_DEVICE_ORDINAL'] = '0,1,2'  # 3 allocated GPUs per node
        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None,
                                                     'gpus_per_node': 0}))
        self.assertEqual(rm_info.gpus_per_node, 3)

        # test env variable "SLURM_JOB_NODELIST"
        if 'SLURM_NODELIST' in os.environ:
            del os.environ['SLURM_NODELIST']
        os.environ['SLURM_JOB_NODELIST'] = 'node-[5-7]'
        rm_info = rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None}))
        node_names = sorted([n['name'] for n in rm_info.node_list])
        self.assertEqual(node_names, ['node-5', 'node-6', 'node-7'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch_error(self, mocked_logger, mocked_init):

        rm_slurm = Slurm(cfg=None, log=None, prof=None)
        rm_slurm._log = mocked_logger

        if 'SLURM_CPUS_ON_NODE' in os.environ:
            del os.environ['SLURM_CPUS_ON_NODE']
        with self.assertRaises(RuntimeError):
            rm_slurm.init_from_scratch(RMInfo({'cores_per_node': None}))
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        if 'SLURM_NODELIST' in os.environ:
            del os.environ['SLURM_NODELIST']
        if 'SLURM_JOB_NODELIST' in os.environ:
            del os.environ['SLURM_JOB_NODELIST']
        with self.assertRaises(RuntimeError):
            rm_slurm.init_from_scratch(RMInfo())

    # --------------------------------------------------------------------------
    #
    def test_batch_started(self):

        saved_batch_id = os.getenv('SLURM_JOB_ID')

        os.environ['SLURM_JOB_ID'] = '12345'
        self.assertTrue(Slurm.batch_started())

        del os.environ['SLURM_JOB_ID']
        self.assertFalse(Slurm.batch_started())

        if saved_batch_id is not None:
            os.environ['SLURM_JOB_ID'] = saved_batch_id

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = SlurmTestCase()
    tc.test_init_from_scratch()
    tc.test_init_from_scratch_error()
    tc.test_batch_started()

# ------------------------------------------------------------------------------
