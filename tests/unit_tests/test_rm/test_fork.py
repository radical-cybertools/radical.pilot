# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from radical.pilot.agent.resource_manager.fork import Fork


class TestFork(TestCase):
    # ------------------------------------------------------------------------------
    # Test 1 config file
    @mock.patch.object(Fork, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('multiprocessing.cpu_count', return_value=24)
    def test_configure(self, mocked_init, mocked_raise_on,
                       mocked_multiprocessing_cpu_count):

        # configuration #1
        component = Fork(cfg=None, session=None)
        component.requested_cores = 2
        component.requested_gpus  = 0
        component._log = ru.Logger('dummy')
        component._cfg = ru.Config(cfg={'resource_cfg': {'fake_resources': 1}})
        component._configure()

        self.assertEqual(component.cores_per_node, 2)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.mem_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': None, 'size': 0})

        # configuration #2
        component = Fork(cfg=None, session=None)
        component.requested_cores = 48
        component.requested_gpus  = 0
        component._log = ru.Logger('dummy')
        component._cfg = ru.Config(cfg={'cores_per_node': 24,
                                        'gpus_per_node': 0,
                                        'lfs_path_per_node': 'test/',
                                        'lfs_size_per_node': 100,
                                        'resource_cfg': {'fake_resources': 1}})
        component._configure()

        self.assertEqual(component.cores_per_node, 24)
        self.assertEqual(component.gpus_per_node, 0)
        self.assertEqual(component.mem_per_node, 0)
        self.assertEqual(component.lfs_per_node, {'path': 'test/', 'size': 100})
        # number of nodes is calculated based on number of requested CPUs
        self.assertEqual(len(component.node_list), 2)

        # configuration #3
        component = Fork(cfg=None, session=None)
        component.requested_cores = 48
        component.requested_gpus  = 8
        component._log = ru.Logger('dummy')
        component._cfg = ru.Config(cfg={'cores_per_node': 24,
                                        'gpus_per_node': 2,
                                        'resource_cfg': {'fake_resources': 1}})
        component._configure()

        # number of nodes is calculated based on number of requested GPUs
        self.assertEqual(len(component.node_list), 4)


if __name__ == '__main__':

    tc = TestFork()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
