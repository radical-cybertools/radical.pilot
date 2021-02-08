#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import warnings

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.torque import Torque


class TestTorque(TestCase):
    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes1'])
    def test_configure(self, mocked_init, mocked_raise_on, mocked_expand_hoslist):

        # Test 1 no config file
        base = os.path.dirname(__file__) + '/../'
        os.environ['PBS_NODEFILE']  = '%s/test_cases/rm/nodelist.torque' % base
        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1', 'nodes1']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

        # Test 2 config file
        os.environ['PBS_NODEFILE']  = '%s/test_cases/rm/nodelist.torque' % base
        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {'cores_per_node'    : 4,
                          'gpus_per_node'     : 1,
                          'lfs_path_per_node' : 'test/',
                          'lfs_size_per_node' : 100}
        component.lm_info = {'cores_per_node' : None}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1', 'nodes1']])
        self.assertEqual(component.cores_per_node, 4)
        self.assertEqual(component.gpus_per_node, 1)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Torque, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes1'])
    def test_configure_error(self, mocked_init, mocked_raise_on, mocked_expand_hoslist):

        # Test 1 no config file check nodefile
        base = os.path.dirname(__file__) + '/../'
        if 'PBS_NODEFILE' in os.environ:
            del os.environ['PBS_NODEFILE']

        os.environ['PBS_NCPUS']     = '2'
        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 no config file check Number of CPUS
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.torque' % base

        if 'PBS_NCPUS' in os.environ:
            del os.environ['PBS_NCPUS']

        os.environ['PBS_NUM_PPN']   = '4'
        os.environ['PBS_NUM_NODES'] = '2'

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertWarns(RuntimeWarning):
            component._configure()
            warnings.warn("PBS_NCPUS not set!", RuntimeWarning)

        # Test 3 no config file check Number of Processes per Node
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.torque' % base
        os.environ['PBS_NCPUS']    = '2'

        if 'PBS_NUM_PPN' in os.environ:
            del os.environ['PBS_NUM_PPN']

        os.environ['PBS_NUM_NODES'] = '2'
        os.environ['SAGA_PPN']      = '0'

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertWarns(RuntimeWarning):
            component._configure()
            warnings.warn("PBS_PPN not set!", RuntimeWarning)

        # Test 4 no config file check Number of Nodes
        os.environ['PBS_NODEFILE'] = '%s/test_cases/rm/nodelist.torque' % base
        os.environ['PBS_NCPUS']    = '2'
        os.environ['PBS_NUM_PPN']  = '4'

        if 'PBS_NUM_NODES' in os.environ:
            del os.environ['PBS_NUM_NODES']

        component = Torque(cfg=None, session=None)
        component.name = 'Torque'
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component.lm_info = {}

        with self.assertWarns(RuntimeWarning):
            component._configure()
            warnings.warn("PBS_NUM_NODES not set!", RuntimeWarning)


if __name__ == '__main__':

    tc = TestTorque()
    tc.test_configure()
    tc.test_configure_error()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
