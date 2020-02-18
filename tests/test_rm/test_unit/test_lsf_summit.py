
# pylint: disable=protected-access, unused-argument
import os
import pytest
import radical.utils as ru
from radical.pilot.agent.resource_manager.lsf_summit import LSF_SUMMIT

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
def test_configure(mocked_init, mocked_raise_on):

    # Test 1 no config file
    os.environ['LSB_DJOB_HOSTFILE'] = 'tests/test_cases/rm/nodelist.lsf'

    component = LSF_SUMMIT(cfg=None, session=None)
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component._configure()

    assert component.node_list == [['nodes1', '1'],['nodes2', '2']]
    assert component.sockets_per_node == 1
    assert component.cores_per_socket == 20
    assert component.gpus_per_socket  == 0
    assert component.lfs_per_node     == {'path': None, 'size': 0}
    assert component.mem_per_node     == 0

    # Test 2 config file
    os.environ['LSB_DJOB_HOSTFILE'] = 'tests/test_cases/rm/nodelist.lsf'

    component = LSF_SUMMIT(cfg=None, session=None)
    component._log = ru.Logger('dummy')
    component._cfg = {'sockets_per_node': 2,
                      'gpus_per_node': 2,
                      'lfs_path_per_node': 'test/',
                      'lfs_size_per_node': 100}
    component._configure()

    assert component.node_list == [['nodes1', '1'],['nodes2', '2']]
    assert component.sockets_per_node == 2
    assert component.cores_per_socket == 10
    assert component.gpus_per_socket  == 1
    assert component.lfs_per_node     == {'path': 'test/', 'size': 100}
    assert component.mem_per_node     == 0
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
#
@mock.patch.object(LSF_SUMMIT, '__init__',   return_value=None)
@mock.patch('radical.utils.raise_on')
def test_configure_error(mocked_init, mocked_raise_on):

    # Test 1 no config file
    del os.environ['LSB_DJOB_HOSTFILE']

    component = LSF_SUMMIT(cfg=None, session=None)
    component._log = ru.Logger('dummy')
    component._cfg = {}

    with pytest.raises(RuntimeError):
        component._configure()
#
#    # Test 2 config file
    os.environ['LSB_DJOB_HOSTFILE'] = 'tests/test_cases/rm/nodelist.lsf'

    component = LSF_SUMMIT(cfg=None, session=None)
    component._log = ru.Logger('dummy')
    component._cfg = {'sockets_per_node': 2,
                      'gpus_per_node': 1,
                      'lfs_path_per_node': 'test/',
                      'lfs_size_per_node': 100}

    with pytest.raises(AssertionError):
        component._configure()

# ------------------------------------------------------------------------------
