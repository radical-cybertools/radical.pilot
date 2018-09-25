import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.scheduler.continuous_summit import ContinuousSummit
import pytest
import radical.pilot.constants as rpc
import glob
import os
import shutil
import copy
from pprint import pprint

try:
    import mock
except ImportError:
    from unittest import mock

# User Input for test
#-----------------------------------------------------------------------------------------------------------------------
resource_name = 'local.localhost'
access_schema = 'ssh'
#-----------------------------------------------------------------------------------------------------------------------


# Setup for all tests
#-----------------------------------------------------------------------------------------------------------------------
# Stating session id
session_id = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))
# local_sample_data = os.path.join(cur_dir, 'sample_data')
# sample_data = [
#     'single_file.txt',
#     'single_folder',
#     'multi_folder'
# ]
#-----------------------------------------------------------------------------------------------------------------------


# Setup to be done for every test
#-----------------------------------------------------------------------------------------------------------------------
def setUp():
    # Add SAGA method to only create directories on remote - don't transfer yet!
    session = rp.Session()

    cfg = dict()
    cfg['lrms_info'] = dict()
    # cfg['oversubscribe'] = False
    cfg['cross_socket_threads'] = False
    cfg['lrms_info']['lm_info'] = 'INFO'
    cfg['lrms_info']['node_list'] = [['a', 1],['b', 2],['c', 3]]
    cfg['lrms_info']['sockets_per_node'] = 2
    cfg['lrms_info']['cores_per_socket'] = 7
    cfg['lrms_info']['gpus_per_socket'] = 3
    cfg['lrms_info']['lfs_per_node'] = {'size': 0, 'path': '/dev/null'}

    return cfg, session
#-----------------------------------------------------------------------------------------------------------------------


def nompi():
    cud = dict()
    cud['environment'] = dict()
    cud['cpu_process_type'] = None
    cud['gpu_process_type'] = None
    cud['cpu_processes'] = 1
    cud['cpu_threads'] = 1
    cud['gpu_processes'] = 0
    cud['lfs_per_process'] = 0

    return cud

# Cleanup any folders and files to leave the system state
# as prior to the test
#-----------------------------------------------------------------------------------------------------------------------


def tearDown():
    rp = glob.glob('%s/rp.session.*' % os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr input staging of a single file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(ContinuousSummit, '__init__', return_value=None)
@mock.patch.object(ContinuousSummit, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_unit(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    cfg, session = setUp()

    component = ContinuousSummit(cfg=cfg, session=session)
    component._cfg = cfg
    component._lrms_info = cfg['lrms_info']
    component._lrms_lm_info = cfg['lrms_info']['lm_info']
    component._lrms_node_list = cfg['lrms_info']['node_list']
    component._lrms_sockets_per_node = cfg['lrms_info']['sockets_per_node']
    component._lrms_cores_per_socket = cfg['lrms_info']['cores_per_socket']
    component._lrms_gpus_per_socket = cfg['lrms_info']['gpus_per_socket']
    component._lrms_lfs_per_node = cfg['lrms_info']['lfs_per_node']
    component._tag_history = dict()
    component._configure()

    # pprint(component.nodes)

    # Allocate first CUD -- should land on first node
    cud = nompi()
    slot = component._allocate_slot(cud)

    assert slot == {'cores_per_node': 14,
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{'lfs': {'size': 0, 'path': '/dev/null'},
                               'core_map': [[0]],
                               'name': 'a',
                               'gpu_map': [],
                               'uid': 1}],
                    'lm_info': 'INFO',
                    'gpus_per_node': 6}

    # Assert resulting node list values after first CUD
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]

    # Allocate second CUD -- should land on first node, fill up first socket
    cud = nompi()
    cud['cpu_threads'] = 6
    slot = component._allocate_slot(cud)
    assert slot == {'cores_per_node': 14,
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{'lfs': {'path': '/dev/null', 'size': 0},
                               'core_map': [[1,2,3,4,5,6]],
                               'name': 'a',
                               'gpu_map': [],
                               'uid': 1}],
                    'lm_info': 'INFO',
                    'gpus_per_node': 6}

    # Assert resulting node list values after second CUD
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]

    # Allocate third CUD -- should land on first node, get 3 cores on 2nd socket
    cud = nompi()
    cud['cpu_threads'] = 3
    slot = component._allocate_slot(cud)
    assert slot == {'cores_per_node': 14,
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{'lfs': {'path': '/dev/null', 'size': 0},
                               'core_map': [[7,8,9]],
                               'name': 'a',
                               'gpu_map': [],
                               'uid': 1}],
                    'lm_info': 'INFO',
                    'gpus_per_node': 6}
    
    # Assert resulting node list values after third CUD
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]},
                                            {'cores': [1,1,1,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]

    # Allocate four CUD -- should land on second node, first socket - no partial socket allocations
    cud = nompi()
    cud['cpu_threads'] = 6
    slot = component._allocate_slot(cud)
    assert slot == {'cores_per_node': 14,
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{'lfs': {'path': '/dev/null', 'size': 0},
                               'core_map': [[0,1,2,3,4,5]],
                               'name': 'b',
                               'gpu_map': [],
                               'uid': 2}],
                    'lm_info': 'INFO',
                    'gpus_per_node': 6}


    # Assert resulting node list values after fourth CUD
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]},
                                            {'cores': [1,1,1,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]


    # Allocate fifth CUD -- should land on first node, second socket -- search from first node for cores
    cud = nompi()
    cud['cpu_threads'] = 4
    slot = component._allocate_slot(cud)
    assert slot == {'cores_per_node': 14,
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{'lfs': {'path': '/dev/null', 'size': 0},
                               'core_map': [[10,11,12,13]],
                               'name': 'a',
                               'gpu_map': [],
                               'uid': 1}],
                    'lm_info': 'INFO',
                    'gpus_per_node': 6}


    # Assert resulting node list values after fifth CUD
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]},
                                            {'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]

    # Fail with ValueError if number of threads is greater than cores per socket
    with pytest.raises(ValueError):

        cud = nompi()
        cud['cpu_threads'] = 8
        slot = component._allocate_slot(cud)


    # Release cores of second node, first socket
    slot = {    'cores_per_node': 14,
                'lfs_per_node': component._lrms_lfs_per_node,
                'nodes': [{'lfs': {'path': '/dev/null', 'size': 0},
                            'core_map': [[0,1,2,3,4,5]],
                            'name': 'b',
                            'gpu_map': [],
                            'uid': 2}],
                'lm_info': 'INFO',
                'gpus_per_node': 6}
    component._release_slot(slot)

    # Assert resulting node list after release
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]},
                                            {'cores': [1,1,1,1,1,1,1],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]
                                
    # Release all cores of first node
    slot = {    'cores_per_node': 14,
                'lfs_per_node': component._lrms_lfs_per_node,
                'nodes': [  {'lfs': {'path': '/dev/null', 'size': 0},
                            'core_map': [[0,1,2,3,4,5,6]],
                            'name': 'a',
                            'gpu_map': [],
                            'uid': 1},
                            {'lfs': {'path': '/dev/null', 'size': 0},
                            'core_map': [[7,8,9,10,11,12,13]],
                            'name': 'a',
                            'gpu_map': [],
                            'uid': 1}],
                'lm_info': 'INFO',
                'gpus_per_node': 6}
    component._release_slot(slot)

    # Assert resulting node list after release
    assert component.nodes == [{'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'a',
                                'uid': 1},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'b',
                                'uid': 2},
                                {'lfs': {'size': 0, 'path': '/dev/null'},
                                'sockets': [{'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]},
                                            {'cores': [0,0,0,0,0,0,0],
                                            'gpus': [0,0,0]}],
                                'name': 'c',
                                'uid': 3}]

    tearDown()
#-----------------------------------------------------------------------------------------------------------------------
