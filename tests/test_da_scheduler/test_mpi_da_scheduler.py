import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.scheduler.continuous import Continuous
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
    cfg['lrms_info']['lm_info'] = 'INFO'
    cfg['lrms_info']['node_list'] = [['a',1],['b',2],['c',3],['d',4],['e',5]]
    cfg['lrms_info']['cores_per_node'] = 4
    cfg['lrms_info']['gpus_per_node'] = 1
    cfg['lrms_info']['lfs_per_node'] = {'size': 5120, 'path': 'abc'}

    return cfg, session
#-----------------------------------------------------------------------------------------------------------------------

def mpi():
    cud = dict()
    cud['cpu_process_type'] = 'MPI'
    cud['gpu_process_type'] = None
    cud['cpu_processes'] = 1
    cud['cpu_threads'] = 1
    cud['gpu_processes'] = 0
    cud['lfs_per_process'] = 1024

    return cud

# Cleanup any folders and files to leave the system state
# as prior to the test
#-----------------------------------------------------------------------------------------------------------------------
def tearDown():
    rp = glob.glob('%s/rp.session.*'%os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)
#-----------------------------------------------------------------------------------------------------------------------


# Test umgr input staging of a single file
#-----------------------------------------------------------------------------------------------------------------------
# @mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_mpi_unit_with_continuous_scheduler(
        # mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    cfg, session = setUp()   
    component = Continuous(cfg={'owner': 'parent'}, session=session)
    component._scattered = True
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']
    component._lrms_lfs_per_node   = cfg['lrms_info']['lfs_per_node']

    component.nodes = []
    for node, node_uid in component._lrms_node_list:
        component.nodes.append(copy.deepcopy({
                'name' : node,
                'uid'  : node_uid,
                'cores': [rpc.FREE] * component._lrms_cores_per_node,
                'gpus' : [rpc.FREE] * component._lrms_gpus_per_node,
                'lfs'  : component._lrms_lfs_per_node
            }))


    # Allocate first CUD -- should land on first node
    print '--------------------------------------------------------'
    cud = mpi()
    cud['cpu_processes'] = 2
    cud['cpu_threads'] = 1
    cud['lfs_per_process'] = 1024
    slot =  component._allocate_slot(cud)
    assert slot == {'cores_per_node': component._lrms_cores_per_node, 
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{ 'lfs': 2048, 
                                'core_map': [[0],[1]], 
                                'name': 'a', 
                                'gpu_map': [], 
                                'uid': 1}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': component._lrms_gpus_per_node}

    # Assert resulting node list values after first CUD
    assert component.nodes == [ {   'lfs': {'size': 3072, 'path': 'abc'},
                                    'cores': [1, 1, 0, 0], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]


    # Allocate second CUD -- should land on first node
    print '--------------------------------------------------------'
    cud = mpi()
    cud['cpu_processes'] = 1
    cud['cpu_threads'] = 2
    cud['lfs_per_process'] = 1024
    slot =  component._allocate_slot(cud)
    assert slot == {'cores_per_node': component._lrms_cores_per_node, 
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[2,3]], 
                                'name': 'a', 
                                'gpu_map': [], 
                                'uid': 1}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': component._lrms_gpus_per_node}


    # Assert resulting node list values after second CUD
    assert component.nodes == [ {   'lfs': {'size': 2048, 'path': 'abc'},
                                    'cores': [1, 1, 1, 1], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]


    # Allocate third CUD -- should land on second node since no cores are
    # available on the first
    print '--------------------------------------------------------'
    cud = mpi()
    cud['cpu_processes'] = 1
    cud['cpu_threads'] = 1
    cud['lfs_per_process'] = 1024
    slot =  component._allocate_slot(cud)
    # pprint(slot)
    assert slot == {'cores_per_node': component._lrms_cores_per_node, 
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[0]], 
                                'name': 'b', 
                                'gpu_map': [], 
                                'uid': 2}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': component._lrms_gpus_per_node}

   
    # Assert resulting node list values after third CUD
    assert component.nodes == [ {   'lfs': {'size': 2048, 'path': 'abc'},
                                    'cores': [1, 1, 1, 1], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': {'size': 4096, 'path': 'abc'},
                                    'cores': [1, 0, 0, 0], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]


    # Allocate fourth CUD -- should land on second and third nodes
    print '--------------------------------------------------------'
    cud = mpi()
    cud['cpu_processes'] = 2
    cud['cpu_threads'] = 2
    cud['lfs_per_process'] = 1024
    slot =  component._allocate_slot(cud)
    # print slot
    assert slot == {'cores_per_node': component._lrms_cores_per_node, 
                    'lfs_per_node': component._lrms_lfs_per_node,
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[1,2]], 
                                'name': 'b', 
                                'gpu_map': [], 
                                'uid': 2},
                                { 'lfs': 1024, 
                                'core_map': [[0,1]], 
                                'name': 'c', 
                                'gpu_map': [], 
                                'uid': 3}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': component._lrms_gpus_per_node}

    # Assert resulting node list values after fourth CUD
    assert component.nodes == [ {   'lfs': {'size': 2048, 'path': 'abc'},
                                    'cores': [1, 1, 1, 1], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': {'size': 3072, 'path': 'abc'},
                                    'cores': [1, 1, 1, 0], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': {'size': 4096, 'path': 'abc'},
                                    'cores': [1, 1, 0, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]


    # Deallocate slot
    component._release_slot(slot)
    assert component.nodes == [ {   'lfs': {'size': 2048, 'path': 'abc'},
                                    'cores': [1, 1, 1, 1], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': {'size': 4096, 'path': 'abc'},
                                    'cores': [1, 0, 0, 0], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': {'size': 5120, 'path': 'abc'},
                                    'cores': [0, 0, 0, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]



    tearDown()
#-----------------------------------------------------------------------------------------------------------------------


