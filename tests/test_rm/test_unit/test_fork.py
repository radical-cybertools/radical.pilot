#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

import radical.utils as ru
from radical.pilot.agent.resource_manager.fork import Fork

try:
    import mock
except ImportError:
    from unittest import mock


# --------------------------------------------------------------------------------
# Test 1 config file
@mock.patch.object(Fork, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('multiprocessing.cpu_count',return_value = 24)
def test_configure(mocked_init, mocked_raise_on,
                   mocked_multiprocessing_cpu_count):
    component = Fork(cfg=None, session=None)
    component.requested_cores = 48
    component._log = ru.Logger('dummy')
    component._cfg = ru.Config(cfg={'resource_cfg': {'fake_resources': 1}})
    component._configure()

    assert component.cores_per_node == 48
    assert component.gpus_per_node == 0
    assert component.mem_per_node ==  0
    assert component.lfs_per_node == {'path': None, 'size': 0}

    component = Fork(cfg=None, session=None)
    component.requested_cores = 48
    component._log = ru.Logger('dummy')
    component._cfg = ru.Config(cfg={'cores_per_node': 48  ,
                                    'gpus_per_node': 0,
                                    'lfs_path_per_node': 'test/',
                                    'lfs_size_per_node': 100,
                                    'resource_cfg': {'fake_resources': 1}})
    component._configure()

    assert component.cores_per_node == 48
    assert component.gpus_per_node == 0
    assert component.mem_per_node ==  0
    assert component.lfs_per_node == {'path': 'test/', 'size': 100}


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    test_configure()


# ------------------------------------------------------------------------------

