#!/usr/bin/env python3
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.slurm import Slurm


class TestSlurm(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        # Test 1 no config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        os.environ['SLURM_NPROCS'] = '48'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {'cores_per_node': None}
        import sys
        sys.stderr.write('%s' % os.environ.get('SLURM_NODELIST'))
        sys.stderr.flush()
        component._configure()

        self.assertEqual(component.node_list,
                         [['nodes-1', 'nodes-1'], ['nodes-2', 'nodes-2']])
        self.assertEqual(component.cores_per_node, 24)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

        # Test 2 config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        os.environ['SLURM_NPROCS'] = '48'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'cores_per_node': 24,
                          'gpus_per_node': 1,
                          'lfs_path_per_node': 'test/',
                          'lfs_size_per_node': 100}
        component.lm_info = {'cores_per_node': None}
        component._configure()
        self.assertEqual(component.node_list,
                         [['nodes-1', 'nodes-1'], ['nodes-2', 'nodes-2']])
        self.assertEqual(component.cores_per_node, 24)
        self.assertEqual(component.gpus_per_node, 1)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})

        # Test 3 config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        os.environ['SLURM_NPROCS'] = '48'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'
        os.environ['LOCAL'] = '/local_folder/'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'cores_per_node': 24,
                          'gpus_per_node': 1,
                          'lfs_path_per_node': '${LOCAL}',
                          'lfs_size_per_node': 100}
        component.lm_info = {'cores_per_node': None}
        component._configure()
        self.assertEqual(component.node_list,
                         [['nodes-1', 'nodes-1'], ['nodes-2', 'nodes-2']])
        self.assertEqual(component.cores_per_node, 24)
        self.assertEqual(component.gpus_per_node, 1)
        self.assertEqual(component.lfs_per_node, {'path': '/local_folder/', 'size': 100})

    # ------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Slurm, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure_error(self, mocked_init, mocked_raise_on):

        # Test 1 no config file
        if 'SLURM_NODELIST' in os.environ:
            del os.environ['SLURM_NODELIST']

        os.environ['SLURM_NPROCS'] = '48'
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        del os.environ['SLURM_NPROCS']
        os.environ['SLURM_NNODES'] = '2'
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        os.environ['SLURM_NPROCS'] = '48'
        del os.environ['SLURM_NNODES']
        os.environ['SLURM_CPUS_ON_NODE'] = '24'

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 config file
        os.environ['SLURM_NODELIST'] = 'nodes-[1-2]'
        os.environ['SLURM_NPROCS'] = '48'
        os.environ['SLURM_NNODES'] = '2'
        del os.environ['SLURM_CPUS_ON_NODE']

        component = Slurm(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertRaises(RuntimeError):
            component._configure()


if __name__ == '__main__':

    tc = TestSlurm()
    tc.test_configure()
    tc.test_configure_error()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
