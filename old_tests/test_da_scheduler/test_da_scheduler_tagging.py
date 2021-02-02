import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.scheduler.continuous import Continuous
import pytest
import radical.pilot.constants as rpc
import glob
import os
import shutil
import copy
import threading

try:
    import mock
except ImportError:
    from tasktest import mock

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
    cfg['lrms_info']['lm_info'] = 'INFO'
    cfg['lrms_info']['node_list'] = [['a', 1], ['b', 2], ['c', 3], ['d', 4], ['e', 5]]
    cfg['lrms_info']['cores_per_node'] = 2
    cfg['lrms_info']['gpus_per_node'] = 1
    cfg['lrms_info']['lfs_per_node'] = {'size': 5120, 'path': 'abc'}

    return cfg, session
#-----------------------------------------------------------------------------------------------------------------------


def nompi():
    t = dict()
    t['description'] = {
        'environment': dict(),
        'cpu_process_type': None,
        'gpu_process_type': None,
        'cpu_processes': 1,
        'cpu_threads': 1,
        'gpu_processes': 0,
        'lfs_per_process': 1024
    }
    
    return t
#-----------------------------------------------------------------------------------------------------------------------


def mpi():
    t = dict()
    t['description'] = {

        'environment': dict(),
        'cpu_process_type': 'MPI',
        'gpu_process_type': None,
        'cpu_processes': 1,
        'cpu_threads': 1,
        'gpu_processes': 0,
        'lfs_per_process': 1024

    }
    
    return t
#-----------------------------------------------------------------------------------------------------------------------
# Cleanup any folders and files to leave the system state
# as prior to the test


def tearDown():
    rp = glob.glob('%s/rp.session.*' % os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)
#-----------------------------------------------------------------------------------------------------------------------


# Test tmgr input staging of a single file
#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_task_with_tagging(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    cfg, session = setUp()

    component = Continuous(cfg=dict(), session=session)
    component._lrms_info = cfg['lrms_info']
    component._lrms_lm_info = cfg['lrms_info']['lm_info']
    component._lrms_node_list = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node = cfg['lrms_info']['gpus_per_node']
    component._lrms_lfs_per_node = cfg['lrms_info']['lfs_per_node']
    component._log = ru.Logger('test.component')
    component._slot_lock = threading.RLock()
    component._prof = ru.Profiler('test')
    component._tag_history = dict()


    component.nodes = []
    for node, node_uid in component._lrms_node_list:
        component.nodes.append(copy.deepcopy({
            'name': node,
            'uid': node_uid,
            'cores': [rpc.FREE] * component._lrms_cores_per_node,
            'gpus': [rpc.FREE] * component._lrms_gpus_per_node,
            'lfs': component._lrms_lfs_per_node
        }))

    # Allocate first TD -- should land on first node
    t = nompi()
    t['uid'] = 'task.000000'
    component._try_allocation(t)
    slot1 = t['slots']
    assert component._tag_history == {'task.000000': [1]}
    assert slot1 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[0]],
                                'name': 'a',
                                'gpu_map': [],
                                'uid': 1}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}

    # Assert resulting node list values after first TD
    assert component.nodes == [{'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 0],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    # Allocate second TD -- should land on first node
    t = nompi()
    t['uid'] = 'task.000001'
    t['description']['tag'] = 'task.000000'
    component._try_allocation(t)
    slot2 = t['slots']
    assert component._tag_history == {'task.000000': [1],
                                      'task.000001': [1]}
    assert slot2 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[1]],
                                'name': 'a',
                                'gpu_map': [],
                                'uid': 1}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}

    # Assert resulting node list values after second TD
    assert component.nodes == [{'lfs': {'size': 3072, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    # Allocate third TD -- should return None as first node is not released
    t = nompi()    
    t['uid'] = 'task.000002'
    t['description']['cpu_threads'] = 1
    t['description']['tag'] = 'task.000000'

    component._try_allocation(t)
    slot3 = t['slots']
    assert slot3 == None
    assert component._tag_history == {'task.000000': [1],
                                      'task.000001': [1]}

    #
    component._release_slot(slot2)
    # Assert resulting node list values after second CUDslot release
    assert component.nodes == [{'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 0],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    # Allocate fourth TD -- should land on first node
    t = nompi()    
    t['uid'] = 'task.000002'
    t['description']['cpu_threads'] = 1
    t['description']['tag'] = 'task.000000'

    component._try_allocation(t)
    slot4 = t['slots']
    assert slot4 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[1]],
                                'name': 'a',
                                'gpu_map': [],
                                'uid': 1}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}
    assert component._tag_history == {'task.000000': [1],
                                      'task.000001': [1],
                                      'task.000002': [1]}

    assert component.nodes == [{'lfs': {'size': 3072, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]


#-----------------------------------------------------------------------------------------------------------------------
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_mpi_task_with_tagging(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    cfg, session = setUp()

    component = Continuous(cfg=dict(), session=session)
    component._lrms_info = cfg['lrms_info']
    component._lrms_lm_info = cfg['lrms_info']['lm_info']
    component._lrms_node_list = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node = cfg['lrms_info']['gpus_per_node']
    component._lrms_lfs_per_node = cfg['lrms_info']['lfs_per_node']
    component._slot_lock = threading.RLock()
    component._scattered = True
    component._log = ru.Logger('test.component')
    component._prof = ru.Profiler('test')
    component._tag_history = dict()

    component.nodes = []
    for node, node_uid in component._lrms_node_list:
        component.nodes.append(copy.deepcopy({
            'name': node,
            'uid': node_uid,
            'cores': [rpc.FREE] * component._lrms_cores_per_node,
            'gpus': [rpc.FREE] * component._lrms_gpus_per_node,
            'lfs': component._lrms_lfs_per_node
        }))

    # Allocate first TD -- should land on first node
    t = mpi()
    t['uid'] = 'task.000000'
    t['description']['cpu_processes'] = 2
    t['description']['cpu_threads'] = 1
    t['description']['lfs_per_process'] = 1024
    component._try_allocation(t)
    slot1 = t['slots']
    assert component._tag_history == {'task.000000': [1]}
    assert slot1 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 2048, 'path': 'abc'},
                                'core_map': [[0], [1]],
                                'name': 'a',
                                'gpu_map': [],
                                'uid': 1}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}

    # Assert resulting node list values after first TD
    assert component.nodes == [{'lfs': {'size': 3072, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    # Allocate second TD -- should return None as the first node is
    # not yet released
    t = mpi()
    t['uid'] = 'task.000001'
    t['description']['tag'] = 'task.000000'
    component._try_allocation(t)
    slot2 = t['slots']
    assert slot2 == None
    assert component._tag_history == {'task.000000': [1]}

    # Allocate third TD -- should land on second and third node
    t = mpi()
    t['uid'] = 'task.000002'
    t['description']['cpu_processes'] = 2
    t['description']['cpu_threads'] = 2    
    component._try_allocation(t)
    slot3 = t['slots']
    assert slot3 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[0, 1]],
                                'name': 'b',
                                'gpu_map': [],
                                'uid': 2},
                               {'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[0, 1]],
                                'name': 'c',
                                'gpu_map': [],
                                'uid': 3}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}
    assert component._tag_history == {'task.000000': [1],
                                      'task.000002': [2, 3]}

    # Assert resulting node list values after second CUDslot release
    assert component.nodes == [{'lfs': {'size': 3072, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    # Allocate fourth TD -- should return None as the second node is not
    # yet released
    t = mpi()
    t['uid'] = 'task.000003'
    t['description']['cpu_threads'] = 2    
    t['description']['tag'] = 'task.000002'
    component._try_allocation(t)
    slot4 = t['slots']
    assert slot4 == None
    assert component._tag_history == {'task.000000': [1],
                                      'task.000002': [2, 3]}

    # Release first node and allocate second TD again
    component._release_slot(slot1)

    assert component.nodes == [{'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    t = mpi()
    t['uid'] = 'task.000001'
    t['description']['tag'] = 'task.000000'
    component._try_allocation(t)
    slot2 = t['slots']
    assert slot2 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[0]],
                                'name': 'a',
                                'gpu_map': [],
                                'uid': 1}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}
    assert component._tag_history == {'task.000000': [1],
                                      'task.000001': [1],
                                      'task.000002': [2, 3]}

    # Release second and third nodes and allocate fourth TD again
    component._release_slot(slot3)

    assert component.nodes == [{'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 0],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    t = mpi()    
    t['uid'] = 'task.000003'
    t['description']['tag'] = 'task.000002'
    t['description']['cpu_threads'] = 2
    component._try_allocation(t)
    slot4 = t['slots']
    assert slot4 == {'cores_per_node': 2,
                     'lfs_per_node': component._lrms_lfs_per_node,
                     'nodes': [{'lfs': {'size': 1024, 'path': 'abc'},
                                'core_map': [[0, 1]],
                                'name': 'b',
                                'gpu_map': [],
                                'uid': 2}],
                     'lm_info': 'INFO',
                     'gpus_per_node': 1}
    assert component._tag_history == {'task.000000': [1],
                                      'task.000001': [1],
                                      'task.000002': [2, 3],
                                      'task.000003': [2]}

    assert component.nodes == [{'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 0],
                                'name': 'a',
                                'gpus': [0],
                                'uid': 1},
                               {'lfs': {'size': 4096, 'path': 'abc'},
                                'cores': [1, 1],
                                'name': 'b',
                                'gpus': [0],
                                'uid': 2},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'c',
                                'gpus': [0],
                                'uid': 3},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'd',
                                'gpus': [0],
                                'uid': 4},
                               {'lfs': {'size': 5120, 'path': 'abc'},
                                'cores': [0, 0],
                                'name': 'e',
                                'gpus': [0],
                                'uid': 5}]

    tearDown()
