#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import warnings

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.torque import Torque

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestTorque(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        # Test 1 no config file
        os.environ['PBS_NODEFILE']  = '%s/test_cases/nodelist.torque' % base
        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {}
        component._log  = mock.Mock()
        component._prof = mock.Mock()
        component._init_from_scratch()

        self.assertEqual(component.node_list, [['nodes1', 'nodes1']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

        # Test 2 config file
        os.environ['PBS_NODEFILE']  = '%s/test_cases/nodelist.torque' % base
        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {'cores_per_node'    : 4,
                           'gpus_per_node'     : 1,
                           'lfs_path_per_node' : 'test/',
                           'lfs_size_per_node' : 100}
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        component._init_from_scratch()

        self.assertEqual(component.node_list, [['nodes1', 'nodes1']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 1)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure_error(self, mocked_init, mocked_raise_on):

        # Test 1 no config file check nodefile
        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']

        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {}
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        with self.assertRaises(RuntimeError):
            component._init_from_scratch()

        # Test 2 no config file check Number of CPUS
        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.torque' % base

        if 'PBS_NCPUS' in os.environ:
            del os.environ['PBS_NCPUS']

        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {}
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        with self.assertWarns(RuntimeWarning):
            component._init_from_scratch()
            warnings.warn("PBS_NCPUS not set!", RuntimeWarning)

        # Test 3 no config file check Number of Processes per Node
        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.torque' % base
        os.environ['PBS_NCPUS']    = '2'

        if 'PBS_NUM_PPN' in os.environ:
            del os.environ['PBS_NUM_PPN']

        os.environ['PBS_NUM_NODES'] = '2'
        os.environ['SAGA_PPN']      = '0'

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {}
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        with self.assertWarns(RuntimeWarning):
            component._init_from_scratch()
            warnings.warn("PBS_PPN not set!", RuntimeWarning)

        # Test 4 no config file check Number of Nodes
        os.environ['PBS_NODEFILE'] = '%s/test_cases/nodelist.torque' % base
        os.environ['PBS_NCPUS']    = '2'
        os.environ['PBS_NUM_PPN']  = '4'

        if 'PBS_NUM_NODES' in os.environ:
            del os.environ['PBS_NUM_NODES']

        component = Torque(None, None, None)
        component.name  = 'Torque'
        component._cfg  = {}
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        with self.assertWarns(RuntimeWarning):
            component._init_from_scratch()
            warnings.warn("PBS_NUM_NODES not set!", RuntimeWarning)



# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTorque()
    tc.test_configure()
    tc.test_configure_error()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter

