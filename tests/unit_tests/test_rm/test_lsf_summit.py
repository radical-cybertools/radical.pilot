# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.lsf_summit import LSF_SUMMIT


class TestLSF_SUMMIT(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls):
        cls._base_path = os.path.dirname(__file__) + '/../'
        cls._host_file_path = '%s/test_cases/rm/nodelist.lsf' % cls._base_path

    # --------------------------------------------------------------------------
    #
    def test_init(self):

        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

        default_cfg = {'cores'              : 1,
                       'agent_launch_method': None,
                       'task_launch_method' : None}

        component = LSF_SUMMIT(cfg=default_cfg, session=mock.Mock())

        self.assertEqual(component.name, 'LSF_SUMMIT')
        self.assertEqual(component._cfg, default_cfg)
        self.assertIsInstance(component.partitions, dict)

        # check that `self.rm_info` has all necessary keys
        rm_info_keys = ['name', 'lm_info', 'node_list', 'partitions',
                        'agent_nodes', 'sockets_per_node', 'cores_per_socket',
                        'gpus_per_socket', 'cores_per_node', 'gpus_per_node',
                        'lfs_per_node', 'mem_per_node', 'smt']
        for rm_info_key in component.rm_info.keys():
            self.assertIn(rm_info_key, rm_info_keys)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

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
        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

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

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(LSF_SUMMIT, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure_error(self, mocked_init, mocked_raise_on):

        # Test 1 no config file
        if 'LSB_DJOB_HOSTFILE' in os.environ:
            del os.environ['LSB_DJOB_HOSTFILE']

        component = LSF_SUMMIT(cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {}

        with self.assertRaises(RuntimeError):
            component._configure()

        # Test 2 config file
        os.environ['LSB_DJOB_HOSTFILE'] = self._host_file_path

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
    tc.test_init()
    tc.test_configure()
    tc.test_configure_error()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
