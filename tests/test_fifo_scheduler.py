

import os
import pprint
import pytest

import threading as mt

import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.constants as rpc

from radical.pilot.agent.scheduler.continuous_fifo import ContinuousFifo


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
    config  = {'lrms_info' : {'name'           : 'dummy',
                              'lm_info'        : 'INFO',
                              'n_nodes'        : 2,
                              'cores_per_node' : 4,
                              'gpus_per_node'  : 0,
                              'node_list'      : [['0', 0], ['1', 1]]}}
    return config, session


# ------------------------------------------------------------------------------
#
def get_cuds(uids):

    if not isinstance(uids, list):
        uids = [uids]

    return [{'uid'          : uid, 
             'description' : {'cpu_process_type' : None,
                              'cpu_thread_type'  : None,
                              'cpu_processes'    : 1,
                              'cpu_threads'      : 0,
                              'gpu_process_type' : None,
                              'gpu_thread_type'  : None,
                              'gpu_processes'    : 0,
                              'gpu_threads'      : 0}}
            for uid in uids]


# ------------------------------------------------------------------------------
# Cleanup any folders and files to leave the system state
# as prior to the test
def tearDown():

    pass


# ------------------------------------------------------------------------------
# Test non mpi units
@mock.patch.object(ContinuousFifo, '__init__', return_value=None)
@mock.patch.object(ContinuousFifo, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_fifo_scheduler(mocked_init,
                        mocked_method,
                        mocked_profiler,
                        mocked_raise_on):

    cfg, session = setUp()

    component = ContinuousFifo(cfg=dict(), session=session)
    component._configured          = False
    component._cfg                 = cfg
    component._lrms_info           = cfg['lrms_info']
    component._lrms_lm_info        = cfg['lrms_info']['lm_info']
    component._lrms_n_nodes        = cfg['lrms_info']['n_nodes']
    component._lrms_node_list      = cfg['lrms_info']['node_list']
    component._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    component._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']
    component._slot_lock           = mt.RLock()
    component._prof                = mocked_profiler
    component._log                 = ru.Logger('x')

    component.nodes = []
    for node, node_uid in component._lrms_node_list:
        component.nodes.append({
            'name' : node,
            'uid'  : node_uid,
            'cores': [rpc.FREE] * component._lrms_cores_per_node,
            'gpus' : [rpc.FREE] * component._lrms_gpus_per_node
        })

    # populate component attributes
    component._configure()
    component._oversubscribe = True

    def assert_pool(n):
        assert(len(component._ordered_wait_pool) == n)
        assert(len(component._ordered_wait_pool) == n)

    # pool is initially empty
    assert_pool(0)

    # unit 1 should wait
    component.work(get_cuds('unit.00001'))
    assert_pool(1)

    # unit 3 and 4 should also wait
    component.work(get_cuds(['unit.3', 'unit.4']))
    assert_pool(3)

    # unit 0 gets scheduled, and triggers unit 1 - unit 3 and 4 remain
    component.work(get_cuds('unit.0'))
    assert_pool(2)

    # unit 6 and 7 should also wait
    component.work(get_cuds(['unit.6', 'unit.7']))
    assert_pool(4)

    # units 2 and 5 flush the whole pool
    component.work(get_cuds(['unit.5', 'unit.2']))
    assert_pool(0)

    # repeat of unit 1 triggers an error
    with pytest.raises(AssertionError):
        component.work(get_cuds(['unit.1']))

    tearDown()


# ------------------------------------------------------------------------------

