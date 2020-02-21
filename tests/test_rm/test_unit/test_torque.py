'''
Unit test for torque_pbs scheduler
'''
# pylint: disable=protected-access, unused-argument
import os
import warnings
import pytest
import radical.utils as ru
from radical.pilot.agent.resource_manager.torque import Torque

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(Torque, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes1'])
def test_configure(mocked_init, mocked_raise_on, mocked_expand_hoslist):
    # Test 1 no config file
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    os.environ['PBS_NCPUS'] = '2'
    os.environ['PBS_NUM_PPN'] = '4'
    os.environ['PBS_NUM_NODES'] = '2'

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component._configure()

    assert component.node_list == [['nodes1', 'nodes1']]
    assert component.cores_per_node == 4
    assert component.gpus_per_node == 0
    assert component.lfs_per_node == {'path': None, 'size': 0}

    # Test 2 config file
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    os.environ['PBS_NCPUS'] = '2'
    os.environ['PBS_NUM_PPN'] = '4'
    os.environ['PBS_NUM_NODES'] = '2'

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {'cores_per_node': 4,
                      'gpus_per_node': 1,
                      'lfs_path_per_node': 'test/',
                      'lfs_size_per_node': 100}
    component.lm_info = {'cores_per_node': None}
    component._configure()
    assert component.node_list == [['nodes1', 'nodes1']]
    assert component.cores_per_node == 4
    assert component.gpus_per_node == 1
    assert component.lfs_per_node == {'path': 'test/', 'size': 100}


# ------------------------------------------------------------------------------
#
@mock.patch.object(Torque, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch('hostlist.expand_hostlist', return_value=['nodes1', 'nodes1'])
def test_configure_error(mocked_init, mocked_raise_on, mocked_expand_hoslist):

    # Test 1 no config file check nodefile
    if 'PBS_NODEFILE' in os.environ:
        del os.environ['PBS_NODEFILE']
    os.environ['PBS_NCPUS'] = '2'
    os.environ['PBS_NUM_PPN'] = '4'
    os.environ['PBS_NUM_NODES'] = '2'

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component.lm_info = {}

    with pytest.raises(RuntimeError):
        component._configure()

    # Test 2 no config file check Number of CPUS
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    if 'PBS_NCPUS' in os.environ:
        del os.environ['PBS_NCPUS']
    os.environ['PBS_NUM_PPN'] = '4'
    os.environ['PBS_NUM_NODES'] = '2'

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component.lm_info = {}

    with pytest.warns(RuntimeWarning):
        component._configure()
        warnings.warn("PBS_NCPUS not set!", RuntimeWarning)

    # Test 3 no config file check Number of Processes per Node
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    os.environ['PBS_NCPUS'] = '2'
    if 'PBS_NUM_PPN' in os.environ:
        del os.environ['PBS_NUM_PPN']
    os.environ['PBS_NUM_NODES'] = '2'
    os.environ['SAGA_PPN'] = '0'

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component.lm_info = {}

    with pytest.warns(RuntimeWarning):
        component._configure()
        warnings.warn("PBS_PPN not set!", RuntimeWarning)

    # Test 4 no config file check Number of Nodes
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.torque'
    os.environ['PBS_NCPUS'] = '2'
    os.environ['PBS_NUM_PPN'] = '4'
    if 'PBS_NUM_NODES' in os.environ:
        del os.environ['PBS_NUM_NODES']

    component = Torque(cfg=None, session=None)
    component.name = 'Torque'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component.lm_info = {}

    with pytest.warns(RuntimeWarning):
        component._configure()
        warnings.warn("PBS_NUM_NODES not set!", RuntimeWarning)

