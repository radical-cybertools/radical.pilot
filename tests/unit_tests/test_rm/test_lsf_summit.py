# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.lsf_summit import LSF_SUMMIT


class TestLSF_SUMMIT(TestCase):


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        base = os.path.dirname(__file__) + '/../'
        os.environ['LSB_DJOB_HOSTFILE'] = '%s/test_cases/rm/nodelist.lsf' % base

        # Test 1 no config file
        component = LSF_SUMMIT(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1', '1'],['nodes2', '2']])
        self.assertEqual(component.sockets_per_node, 1)
        self.assertEqual(component.cores_per_socket, 20)
        self.assertEqual(component.gpus_per_socket, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})
        self.assertEqual(component.mem_per_node, 0)

        # Test 2 config file
        os.environ['LSB_DJOB_HOSTFILE'] = '%s/test_cases/rm/nodelist.lsf' % base

        component = LSF_SUMMIT(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'sockets_per_node': 2,
                          'gpus_per_node': 2,
                          'lfs_path_per_node': 'test/',
                          'lfs_size_per_node': 100}
        component._configure()

        self.assertEqual(component.node_list, [['nodes1', '1'],['nodes2', '2']])
        self.assertEqual(component.sockets_per_node, 2)
        self.assertEqual(component.cores_per_socket, 10)
        self.assertEqual(component.gpus_per_socket, 1)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})
        self.assertEqual(component.mem_per_node, 0)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure_error(self, mocked_init, mocked_raise_on):

        base = os.path.dirname(__file__) + '/../'
        # Test 1 no config file
        if 'LSB_DJOB_HOSTFILE' in os.environ:
            del os.environ['LSB_DJOB_HOSTFILE']

        component = LSF_SUMMIT(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}

        with self.assertRaises(RuntimeError):
            component._configure()
    #
    #    # Test 2 config file
        os.environ['LSB_DJOB_HOSTFILE'] = '%s/test_cases/rm/nodelist.lsf' % base

        component = LSF_SUMMIT(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'sockets_per_node': 2,
                          'gpus_per_node': 1,
                          'lfs_path_per_node': 'test/',
                          'lfs_size_per_node': 100}

        with self.assertRaises(AssertionError):
            component._configure()


if __name__ == '__main__':

    tc = TestLSF_SUMMIT()
    tc.test_configure()
    tc.test_configure_error()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
