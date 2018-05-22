import radical.utils as ru
import radical.pilot as rp
from radical.pilot.agent.scheduler.continuous import Continuous
import pytest
import radical.pilot.constants as rpc
import glob
import os
import shutil


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
    cfg['lrms_info']['cores_per_node'] = 2
    cfg['lrms_info']['gpus_per_node'] = 1
    cfg['lrms_info']['lfs_per_node'] = {'size': 5120, 'path': 'abc'}

    return cfg, session
#-----------------------------------------------------------------------------------------------------------------------

def nompi():
    cud = dict()
    cud['cpu_process_type'] = None
    cud['gpu_process_type'] = None
    cud['cpu_processes'] = 1
    cud['cpu_threads'] = 1
    cud['gpu_processes'] = 0
    cud['lfs'] = 1024

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
@mock.patch.object(Continuous, '__init__', return_value=None)
@mock.patch.object(Continuous, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_unit_with_continuous_scheduler(
        mocked_init,
        mocked_method,
        mocked_profiler,
        mocked_raise_on):

    cfg, session = setUp()   
    
    component = Continuous(cfg=dict(), session=session)
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']
    component._lrms_lfs_per_node   = cfg['lrms_info']['lfs_per_node']

    component.nodes = []
    import copy
    for node, node_uid in component._lrms_node_list:
        component.nodes.append(copy.deepcopy({
                'name' : node,
                'uid'  : node_uid,
                'cores': [rpc.FREE] * component._lrms_cores_per_node,
                'gpus' : [rpc.FREE] * component._lrms_gpus_per_node,
                'lfs'  : component._lrms_lfs_per_node
            }))


    # Allocate first CUD -- should land on first node
    cud = nompi()
    slot =  component._allocate_slot(cud)
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[0]], 
                                'name': 'a', 
                                'gpu_map': [], 
                                'uid': 1}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # print component.nodes
    import sys
    sys.exit(1)

    '''
    # Allocate second CUD -- should land on first node
    cud = nompi()
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[1]], 
                                'name': 'a', 
                                'gpu_map': [], 
                                'uid': 1}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

   
    # Allocate third CUD -- should land on second node
    cud = nompi()
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 1024, 
                                'core_map': [[0]], 
                                'name': 'b', 
                                'gpu_map': [], 
                                'uid': 2}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # Allocate four CUD -- should land on third node
    cud = nompi()
    cud['lfs'] = 5120
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 5120, 
                                'core_map': [[0]], 
                                'name': 'c', 
                                'gpu_map': [], 
                                'uid': 3}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # Fail with ValueError if  lfs required by cud is more than available
    with pytest.raises(ValueError):

        cud = nompi()
        cud['lfs'] = 6000
        slot =  component._allocate_slot(cud)    
    

    # Max out available resources
    # Allocate two CUDs -- should land on fourth and fifth node
    cud = nompi()
    cud['lfs'] = 5120
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 5120, 
                                'core_map': [[0]], 
                                'name': 'd', 
                                'gpu_map': [], 
                                'uid': 4}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 5120, 
                                'core_map': [[0]], 
                                'name': 'e', 
                                'gpu_map': [], 
                                'uid': 5}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # Allocate CUD with to land on second node
    cud = nompi()
    cud['lfs'] = 4096
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 4096, 
                                'core_map': [[1]], 
                                'name': 'b', 
                                'gpu_map': [], 
                                'uid': 2}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # Allocate CUD with no lfs requirement
    cud = nompi()
    cud['lfs'] = 0
    slot =  component._allocate_slot(cud)    
    assert slot == {'cores_per_node': 2, 
                    'nodes': [{ 'lfs': 0, 
                                'core_map': [[1]], 
                                'name': 'c', 
                                'gpu_map': [], 
                                'uid': 3}], 
                    'lm_info': 'INFO', 
                    'gpus_per_node': 1}

    # Deallocate slot
    component._release_slot(slot)
    assert component.nodes == [ {   'lfs': 3072, 
                                    'cores': [1, 1], 
                                    'name': 'a', 
                                    'gpus': [0], 
                                    'uid': 1}, 
                                {   'lfs': 0, 
                                    'cores': [1, 1], 
                                    'name': 'b', 
                                    'gpus': [0], 
                                    'uid': 2}, 
                                {   'lfs': 0, 
                                    'cores': [1, 0], 
                                    'name': 'c', 
                                    'gpus': [0], 
                                    'uid': 3}, 
                                {   'lfs': 0, 
                                    'cores': [1, 0], 
                                    'name': 'd', 
                                    'gpus': [0], 
                                    'uid': 4}, 
                                {   'lfs': 0, 
                                    'cores': [1, 0], 
                                    'name': 'e', 
                                    'gpus': [0], 
                                    'uid': 5}]


    # Allocate CUD which cannot fit on available resources
    cud = nompi()
    cud['lfs'] = 5120
    slot =  component._allocate_slot(cud)
    assert slot == None
    '''

    tearDown()
#-----------------------------------------------------------------------------------------------------------------------


