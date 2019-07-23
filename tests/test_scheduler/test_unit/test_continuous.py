# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
"""This is a unit test for the continuous"""
import pytest
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
