# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
import os
import pytest
import warnings
import radical.utils as ru
from radical.pilot.agent.rm.pbspro import PBSPro

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(PBSPro, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch.object(PBSPro, '_parse_pbspro_vnodes', return_value=['nodes1', 'nodes2'])
def test_configure(mocked_init, mocked_raise_on, mocked_parse_pbspro_vnodes):
    # Test 1 no config file
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    os.environ['SAGA_PPN'] = '0'
    os.environ['NODE_COUNT'] = '2'
    os.environ['NUM_PPN'] = '4'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    component = PBSPro()
    component.name = 'PBSPro'
    component._log = ru.Logger('dummy')
    component._cfg = {}
    component._configure()

    assert component.node_list == [['nodes1', 'nodes1'],['nodes2','nodes2']]
    assert component.cores_per_node == 4
    assert component.gpus_per_node == 0
    assert component.lfs_per_node == {'path': None, 'size': 0}

    # Test 2 no config file
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    os.environ['SAGA_PPN'] = '0'
    os.environ['NODE_COUNT'] = '2'
    os.environ['NUM_PPN'] = '4'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    component = PBSPro()
    component.name = 'PBSPro'
    component._log = ru.Logger('dummy')
    component._cfg = {'cores_per_node': 4,
                      'gpus_per_node': 1,
                      'lfs_path_per_node': 'test/',
                      'lfs_size_per_node': 100}
    component.lm_info = {'cores_per_node': None}
    component._configure()

    assert component.node_list == [['nodes1', 'nodes1'],['nodes2','nodes2']]    
    assert component.cores_per_node == 4
    assert component.gpus_per_node == 1
    assert component.lfs_per_node == {'path': 'test/', 'size': 100}

# ------------------------------------------------------------------------------
#
@mock.patch.object(PBSPro, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch.object(PBSPro, '_parse_pbspro_vnodes', return_value=['nodes1', 'nodes2'])
def test_configure_error(mocked_init, mocked_raise_on, mocked_parse_pbspro_vnodes):

    # Test 1 no config file check nodefile
    if 'PBS_NODEFILE' in os.environ:
        del os.environ['PBS_NODEFILE']
    os.environ['SAGA_PPN'] = '0'
    os.environ['NODE_COUNT'] = '2'
    os.environ['NUM_PPN'] = '4'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    component = PBSPro()
    component.name = 'PBSPro'
    component._log = ru.Logger('dummy')
    component._cfg = {}

    with pytest.raises(RuntimeError):
        component._configure()

    # Test 2 check Number of Processors per Node
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    if 'NUM_PPN' in os.environ:
        del os.environ['NUM_PPN']
    if 'SAGA_PPN' in os.environ:
        del os.environ['SAGA_PPN'] 
    os.environ['NODE_COUNT'] = '2'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    with pytest.raises(RuntimeError):
        component._configure()

    # Test 3 check Number of Nodes allocated
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    os.environ['SAGA_PPN'] = '0'
    if 'NODE_COUNT' in os.environ:
        del os.environ['NODE_COUNT']
    os.environ['NUM_PPN'] = '4'
    os.environ['NUM_PES'] = '1'
    os.environ['PBS_JOBID'] = '482125'

    with pytest.warns(RuntimeWarning):
        component._configure()
        warnings.warn("NODE_COUNT not set!", RuntimeWarning)

    # Test 4 check Number of Parallel Environments
    os.environ['PBS_NODEFILE'] = 'tests/test_cases/rm/nodelist.pbs'
    os.environ['NUM_PPN'] = '4'
    os.environ['SAGA_PPN'] = '0'
    os.environ['NODE_COUNT'] = '2'
    if 'NUM_PES' in os.environ:
        del os.environ['NUM_PES']
    os.environ['PBS_JOBID'] = '482125'

    with pytest.warns(RuntimeWarning):
        component._configure()
        warnings.warn("NUM_PES not set!", RuntimeWarning)


# ------------------------------------------------------------------------------
#
@mock.patch.object(PBSPro, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch.object(PBSPro, '_configure', return_value=None)
@mock.patch('subprocess.check_output', return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
def test_parse_pbspro_vnodes(mocked_init, mocked_raise_on, mocked_configure, mocked_subproc):
    # Test 1 no config file JOB_ID
    os.environ['PBS_JOBID'] = '482125'
    component = PBSPro()
    component.name = 'PBSPro'
    component._log = ru.Logger('dummy')
    component._parse_pbspro_vnodes()

# ------------------------------------------------------------------------------
#
@mock.patch.object(PBSPro, '__init__', return_value=None)
@mock.patch('radical.utils.raise_on')
@mock.patch.object(PBSPro, '_configure', return_value=None)
@mock.patch('subprocess.check_output', return_value='exec_vnode = (vnode1:cpu=3)+(vnode2:cpu=2)')
def test_parse_pbspro_vnodes_error(mocked_init, mocked_raise_on, mocked_configure, mocked_subproc):
    # Test 1 check JOB_ID
    if 'PBS_JOBID' in os.environ:
        del os.environ['PBS_JOBID']
    component = PBSPro()
    component._cfg = {}
    component.name = 'PBSPro'
    component._log = ru.Logger('dummy')
    with pytest.raises(RuntimeError):
        component._parse_pbspro_vnodes()




