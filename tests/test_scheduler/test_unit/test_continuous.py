# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
"""This is a unit test for the continuous"""
import pytest
import radical.utils as ru
from radical.pilot.agent.scheduler.continuous import Continuous

try:
    import mock
except ImportError:
    from unittest import mock


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
def test_configure(mocked_init, mocked_agent):
    '''
    Test 1 check configuration setup
    '''
    component = Continuous()
    component.__oversubscribe = True
    component._cfg = {}
    component._lrms_cores_per_node = 4
    component._lrms_gpus_per_node = 2
    component._lrms_lfs_per_node = 128
    component._lrms_mem_per_node = 128
    component._lrms_node_list = [['a', 1], ['b', 2], ['c', 3],
                                 ['d', 4], ['e', 5]]
    assert component._lrms_cores_per_node == 4
    assert component._lrms_gpus_per_node == 2
    assert component._lrms_lfs_per_node == 128
    assert component._lrms_mem_per_node == 128

    if component.__oversubscribe:
        component._configure()


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
def test_configure_err(mocked_init, mocked_agent):
    '''
    Test 2 check configuration setup `oversubscribe`
    is set to False (which is the default for now)
    '''
    component = Continuous()
    component._cfg = {}
    component.__oversubscribe = True
    component._lrms_cores_per_node = 2
    component._lrms_gpus_per_node = 8
    component._lrms_lfs_per_node = 128
    component._lrms_mem_per_node = 128
    assert component._lrms_cores_per_node == 2
    assert component._lrms_gpus_per_node == 8
    assert component._lrms_lfs_per_node == 128
    assert component._lrms_mem_per_node == 128

    if not component.__oversubscribe:
        with pytest.raises(RuntimeError):
            component._configure()


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
def test_pass_find_resources(mocked_init, mocked_agent):
    '''
    Test 1 check functionality
    '''
    component = Continuous()
    component.node = {
        'name': 'node_1',
        'uid': 1,
        'cores': [1, 2, 4, 5],
        'gpus': [1, 2],
        'lfs': {'size': 128},
        'mem': 128
    }
    component.requested_cores = 4
    component.requested_gpus = 4
    component.requested_lfs = 2
    component.requested_mem = 2
    component.core_chunk = 0
    component.lfs_chunk = 0
    component.gpu_chunk = 0
    component.mem_chunk = 0
    component._find_resources(component.node, component.requested_cores,
                              component.requested_gpus,
                              component.requested_lfs,
                              component.requested_mem, component.core_chunk,
                              component.lfs_chunk, component.gpu_chunk,
                              component.mem_chunk)


# ------------------------------------------------------------------------------
#
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch('radical.pilot.agent.scheduler.base.AgentSchedulingComponent')
def test_pass_find_resources_err(mocked_init, mocked_agent):

    '''
    Test 2 check division error rasie (Div by zero)
    '''
    component = Continuous()
    component.node = {
        'name': 'node_1',
        'uid': 1,
        'cores': [1, 2, 4, 5],
        'gpus': [1, 2],
        'lfs': {'size': 128},
        'mem': 128
    }
    component.requested_cores = None
    component.requested_gpus = None
    component.requested_lfs = 2
    component.requested_mem = 2
    component.core_chunk = 0
    component.lfs_chunk = 0
    component.gpu_chunk = 0
    component.mem_chunk = 0
    with pytest.raises(ZeroDivisionError):
        component._find_resources(component.node, component.requested_cores,
                                  component.requested_gpus,
                                  component.requested_lfs,
                                  component.requested_mem,
                                  component.core_chunk,
                                  component.lfs_chunk, component.gpu_chunk,
                                  component.mem_chunk)


# ------------------------------------------------------------------------------
#
@mock.patch.object(Continuous, '__init__', return_value=None)
def test_get_node_maps(mocked_init):

    '''
    Test 1 unit_test for the structure of
    the returned cores and gpus map
    '''
    component = Continuous()
    cores = [0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0]
    gpus = [1, 1, 1]
    tpp = 16
    expected_map = ([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
                    [[1], [1], [1]])
    component._log = ru.Logger('dummy')
    assert  component._get_node_maps(cores, gpus, tpp) == expected_map


@mock.patch.object(Continuous, '__init__', return_value=None)
def test_get_node_maps_err(mocked_init):

    '''
    Test 2 unit_test for raising error if make sure the
    core sets can host the requested number of threads
    '''
    component = Continuous()
    cores = [0, 0, 0, 0, 0, 0, 0, 0,
             0, 0, 0, 0, 0, 0, 0, 0]
    gpus = [1, 1, 1]
    expected_map = ([[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]],
                    [[1], [1], [1]])
    tpp = 24
    with pytest.raises(Exception):
        assert component._get_node_maps(cores, gpus, tpp) == expected_map


# ------------------------------------------------------------------------------
#
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, '_configure', return_value=None)
def test_alloc_nompi_fit(mocked_init, mocked_configure):

    '''
    Test 1 make sure that the requested
    allocation fits on a single node
    '''
    component = Continuous()
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._log = ru.Logger('dummy')
    component.nodes = []
    unit = {
        "uid"        : "unit.000001",
        "description":
            {
                "executable"    : "/bin/sleep",
                "arguments"     : ["10"],
                "gpu_processes" : 1,
                "cpu_processes" : 1,
                "cpu_threads"   : 1,
                "gpu_threads"   : 1,
                "mem_per_process": 128,
                "lfs_per_process":2
            },
    }

    component._alloc_nompi(unit)


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, '_configure', return_value=None)
def test_alloc_nompi_no_fit(mocked_init, mocked_configure):

    '''
    Test 2 umake sure that the requested
    allocation fits on a single node (cpu_processes)and rasis error
    '''
    component = Continuous()
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._log = ru.Logger('dummy')
    component.nodes = []
    unit = {
        "uid"        : "unit.000002",
        "description":
        {
            "executable"    : "/bin/sleep",
            "arguments"     : ["10"],
            "gpu_processes" : 1,
            "cpu_processes" : 16,
            "cpu_threads"   : 1,
            "gpu_threads"   : 1,
            "mem_per_process": 128,
            "lfs_per_process":2
        },
    }

    with pytest.raises(ValueError):
        component._alloc_nompi(unit)

    # Test 3 umake sure that the requested
    # allocation fits on a single node (gpu_processes)and raise error

    component = Continuous()
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._log = ru.Logger('dummy')
    component.nodes = []
    unit = {
        "uid"        : "unit.000003",
        "description":
        {
            "executable"    : "/bin/sleep",
            "arguments"     : ["10"],
            "gpu_processes" : 2,
            "cpu_processes" : 1,
            "cpu_threads"   : 1,
            "gpu_threads"   : 1,
            "mem_per_process": 128,
            "lfs_per_process":2
        },
    }

    with pytest.raises(ValueError):
        component._alloc_nompi(unit)


    # Test 4 umake sure that the requested
    # allocation fits on a single node (requested mem)and raise error
    component = Continuous()
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._log = ru.Logger('dummy')
    component.nodes = []
    unit = {
        "uid"        : "unit.000004",
        "description":
        {
            "executable"    : "/bin/sleep",
            "arguments"     : ["10"],
            "gpu_processes" : 2,
            "cpu_processes" : 1,
            "cpu_threads"   : 1,
            "gpu_threads"   : 1,
            "mem_per_process": 1024,
            "lfs_per_process":2
        },
    }

    with pytest.raises(ValueError):
        component._alloc_nompi(unit)


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, '_configure', return_value=None)
def test_alloc_mpi(mocked_init, mocked_configure):
    '''
    Test 1 check MPI unit fitting into node
    '''
    component = Continuous()
    component.nodes = []
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._lrms_info = ''
    component._lrms_lm_info = ''
    component._log = ru.Logger('dummy')

    unit = {
        "uid"        : "unit.000005",
        "description":
        {
            "executable"    : "/bin/sleep",
            "arguments"     : ["10"],
            "gpu_processes" : 1,
            'cpu_process_type': 'MPI',
            "cpu_processes" : 1,
            "cpu_threads"   : 1,
            "gpu_threads"   : 1,
            "mem_per_process": 128,
            "lfs_per_process":2
        },
    }
    component._alloc_mpi(unit)


@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, '_configure', return_value=None)
def test_alloc_mpi_no_fit(mocked_init, mocked_configure):
    '''
    Test 1 Too many threads requested
    '''
    component = Continuous()
    component.nodes = []
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._lrms_info = ''
    component._lrms_lm_info = ''
    component._log = ru.Logger('dummy')

    unit = {
        "uid"        : "unit.000006",
        "description":
            {
                "executable"    : "/bin/sleep",
                "arguments"     : ["10"],
                "gpu_processes" : 1,
                'cpu_process_type': 'MPI',
                "cpu_processes" : 1,
                "cpu_threads"   : 16,
                "gpu_threads"   : 1,
                "mem_per_process": 128,
                "lfs_per_process":2
            },
    }
    with pytest.raises(ValueError):
        component._alloc_mpi(unit)

    # Test 2 Too much LFS requeted

    component = Continuous()
    component.nodes = []
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._lrms_info = ''
    component._lrms_lm_info = ''
    component._log = ru.Logger('dummy')

    unit = {
        "uid"        : "unit.000006",
        "description":
            {
                "executable"    : "/bin/sleep",
                "arguments"     : ["10"],
                "gpu_processes" : 1,
                'cpu_process_type': 'MPI',
                "cpu_processes" : 1,
                "cpu_threads"   : 1,
                "gpu_threads"   : 1,
                "mem_per_process": 128,
                "lfs_per_process":3047
            },
    }
    with pytest.raises(ValueError):
        component._alloc_mpi(unit)

    # Test 3 Too much MEM requeted

    component = Continuous()
    component.nodes = []
    component._lrms_cores_per_node = 1
    component._lrms_gpus_per_node = 1
    component._lrms_lfs_per_node = {'size': 128}
    component._lrms_mem_per_node = 128
    component._lrms_info = ''
    component._lrms_lm_info = ''
    component._log = ru.Logger('dummy')

    unit = {
        "uid"        : "unit.000006",
        "description":
            {
                "executable"    : "/bin/sleep",
                "arguments"     : ["10"],
                "gpu_processes" : 1,
                'cpu_process_type': 'MPI',
                "cpu_processes" : 1,
                "cpu_threads"   : 1,
                "gpu_threads"   : 1,
                "mem_per_process": 1024,
                "lfs_per_process":2
            },
    }
    with pytest.raises(ValueError):
        component._alloc_mpi(unit)
