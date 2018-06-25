

import os
import copy
import pprint
import pytest

import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.constants as rpc

from radical.pilot.agent.scheduler.hombre import Hombre


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
# User Input for test
resource_name = 'local.localhost'
access_schema = 'ssh'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------------------
# Setup for every test
def setUp():

    session = rp.Session()
    config  = {'lrms_info' : {'lm_info'        : 'INFO',
                              'n_nodes'        : 2,
                              'cores_per_node' : 4,
                              'gpus_per_node'  : 2,
                              'node_list'      : [['0', 0], ['1', 1]]}}
    return config, session


# ------------------------------------------------------------------------------
#
def cud_nonmpi():

    return {'cpu_process_type' : None,
            'cpu_thread_type'  : None,
            'cpu_processes'    : 1,
            'cpu_threads'      : 2,

            'gpu_process_type' : None,
            'gpu_thread_type'  : None,
            'gpu_processes'    : 1,
            'gpu_threads'      : 1}


# ------------------------------------------------------------------------------
#
def cud_mpi():

    return {'cpu_process_type' : rpc.MPI,
            'cpu_thread_type'  : None,
            'cpu_processes'    : 3,
            'cpu_threads'      : 1,

            'gpu_process_type' : rpc.MPI,
            'gpu_thread_type'  : None,
            'gpu_processes'    : 1,
            'gpu_threads'      : 1}


# ------------------------------------------------------------------------------
# Cleanup any folders and files to leave the system state
# as prior to the test
def tearDown():

    pass


# ------------------------------------------------------------------------------
# Test non mpi units
@mock.patch.object(Hombre, '__init__', return_value=None)
@mock.patch.object(Hombre, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_unit_withhombre_scheduler(mocked_init,
                                          mocked_method,
                                          mocked_profiler,
                                          mocked_raise_on):
    cfg, session = setUp()

    component = Hombre(cfg=dict(), session=session)
    component._configured          = False
    component._cfg                 = cfg
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_n_nodes        = cfg['lrms_info']['n_nodes']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']

    component.nodes = list()
    for node in component._lrms_node_list:
        component.nodes.append({'uid'  : node[0], 
                                'name' : node[1]})

    # populate component attributes
    component._configure()
    component._oversubscribe = True

    # we expect these slots to be available
    all_slots = list()
    for n in range(component._lrms_n_nodes):
        all_slots.append({'lm_info'        : 'INFO',
                          'cores_per_node' : 4,
                          'gpus_per_node'  : 2,
                          'nodes'          : [[n, str(n), [[0, 1]], [[0]]]]
                         })
        all_slots.append({'lm_info'        : 'INFO',
                          'cores_per_node' : 4,
                          'gpus_per_node'  : 2,
                          'nodes'          : [[n, str(n), [[2, 3]], [[1]]]]
                         })

    # Allocate first CUD -- should land on second node
    cud  = cud_nonmpi()
    slot = component._allocate_slot(cud)
    chk  = all_slots[-1]
    print '---------------'

    assert(slot == chk)

    # Allocate second CUD -- should also land on second node
    cud  = cud_nonmpi()
    slot = component._allocate_slot(cud)
    assert slot == all_slots[-2]

    # Allocate third CUD -- should land on first node
    cud  = cud_nonmpi()
    slot = component._allocate_slot(cud)
    assert slot == all_slots[-3]

  # print '---------------'
  # print 'free'
  # pprint.pprint(component.free)
  # print 'found'
  # pprint.pprint(slot)
  # print 'expect'
  # pprint.pprint(chk)

    # Allocate fourth CUD -- should also land on tecond node
    cud  = cud_nonmpi()
    slot = component._allocate_slot(cud)
    assert slot == all_slots[-4]

    # Fail with ValueError if heterogeneous  CUs are scheduled
    with pytest.raises(ValueError):

        cud = cud_nonmpi()
        cud['gpu_processes'] = 2
        slot = component._allocate_slot(cud)


    # expext no slots now, as all resources are used
    cud    = cud_nonmpi()
    noslot = component._allocate_slot(cud)
    assert(noslot is None)

    # Deallocate last filled slot
    component._release_slot(slot)

    # we should get a new slot now, which is the same as the one just freed
    cud     = cud_nonmpi()
    newslot = component._allocate_slot(cud)
    assert(newslot == slot)

    tearDown()


# ------------------------------------------------------------------------------
# Test mpi units
@mock.patch.object(Hombre, '__init__', return_value=None)
@mock.patch.object(Hombre, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_nonmpi_unit_withhombre_scheduler(mocked_init,
                                          mocked_method,
                                          mocked_profiler,
                                          mocked_raise_on):
    cfg, session = setUp()

    component = Hombre(cfg=dict(), session=session)
    component._configured          = False
    component._cfg                 = cfg
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_n_nodes        = cfg['lrms_info']['n_nodes']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']

    component.nodes = list()
    for node in component._lrms_node_list:
        component.nodes.append({'uid'  : node[0], 
                                'name' : node[1]})

    # populate component attributes
    component._configure()
    component._oversubscribe = True

    # we expect these slots to be available
    all_slots = [{
                      'lm_info'        : 'INFO',
                      'cores_per_node' : 4,
                      'gpus_per_node'  : 2,
                      'nodes'          : [[0, '0', [[0], [1], [2]], [[0]]]]
                 },
                 {
                      'lm_info'        : 'INFO',
                      'cores_per_node' : 4,
                      'gpus_per_node'  : 2,
                      'nodes'          : [[1, '1', [[0], [1], [2]], [[0]]]]
                 }]

    # Allocate first CUD -- should land on second node
    cud  = cud_mpi()
    slot = component._allocate_slot(cud)
    chk  = all_slots[-1]
    assert(slot == chk)

  # print '---------------'
  # print 'free'
  # pprint.pprint(component.free)
  # print 'found'
  # pprint.pprint(slot)
  # print 'expect'
  # pprint.pprint(chk)
  # print '---------------'

    # Allocate second CUD -- should land on first node
    cud  = cud_mpi()
    slot = component._allocate_slot(cud)
    assert slot == all_slots[-2]

    # Fail with ValueError if heterogeneous  CUs are scheduled
    with pytest.raises(ValueError):

        cud = cud_mpi()
        cud['gpu_processes'] = 2
        slot = component._allocate_slot(cud)

    # expext no slots now, as all resources are used
    cud    = cud_mpi()
    noslot = component._allocate_slot(cud)
    assert(noslot is None)

    # Deallocate last filled slot
    component._release_slot(slot)

    # we should get a new slot now, which is the same as the one just freed
    cud     = cud_mpi()
    newslot = component._allocate_slot(cud)
    assert(newslot == slot)

    tearDown()


# ------------------------------------------------------------------------------

