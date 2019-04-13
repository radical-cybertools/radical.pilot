

import os
import pprint
import pytest

import threading as mt

import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.constants as rpc

from radical.pilot.agent.scheduler.continuous_ordered import ContinuousOrdered


try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
# User Input for test
resource_name = 'local.localhost'
access_schema = 'local'

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
                              'lfs_per_node'   : {'size': 10, 'path': '/'},
                              'mem_per_node'   : 10,
                              'node_list'      : [['0', 0], ['1', 1]]}, 
                'bridges' : []}
    ns      = {0: {'current': 0,
                   0        : {'size'    : 4, 
                               'uids'    : ['0.0.5.0', '0.0.5.1'],
                               'done'    : ['0.0.5.2', '0.0.5.3']},
                   1        : {'size'    : 0, 
                               'uids'    : ['0.1.5.4'],
                               'done'    : []}},
               1: {'current': 0,
                   0        : {'size'    : 4, 
                               'uids'    : ['1.0.5.0'],
                               'done'    : ['1.0.5.2', '1.0.5.3']},
                   1        : {'size'    : 0, 
                               'uids'    : ['1.1.5.4'],
                               'done'    : []}}}

    us      = ['u1', 'u2', 'u3', 'u4']

    units   = dict()
    uids    = ['0.0.5.0', 
               '0.0.5.1',
               '0.0.5.2',
               '0.0.5.3',
               '0.1.5.4',
               '1.0.5.0',
               '1.0.5.2',
               '1.0.5.3',
               '1.1.5.4', 
               'u1', 'u2', 'u2', 'u4']
    for uid in uids:
        units[uid] = get_unit(uid)


    return config, session, ns, us, units


# ------------------------------------------------------------------------------
#
def get_unit(uid):

    elems = uid.split('.')
    if len(elems) == 1:
        tags = None
    else:
        tags = {'order' : {'ns'   : elems[0], 
                           'order': elems[1], 
                           'size' : elems[2]}}

    unit = {'uid'        :  uid, 
            'description': {'cpu_process_type': None,
                            'cpu_thread_type' : None,
                            'cpu_processes'   : 1,
                            'cpu_threads'     : 0,
                            'gpu_process_type': None,
                            'gpu_thread_type' : None,
                            'gpu_processes'   : 0,
                            'gpu_threads'     : 0,
                            'lfs_per_process' : 1,
                            'mem_per_process' : 1,
                            'tags'            : tags
                           }
           }
    return unit


# ------------------------------------------------------------------------------
#
def put_unit(s):
    '''
    return a unit from above as `DONE`
    '''


# ------------------------------------------------------------------------------
#
def check_state(s):
    '''
    return tuples of [ns1, [[done, wait], [done, wait], ...],
                      ns2, [[done, wait], [done, wait], ...]]
    for all name spaces and all orders in them, where `done` and `wait` are
    lists of uids which are done or are waiting for execution, respectively.
    '''


# ------------------------------------------------------------------------------
# Cleanup any folders and files to leave the system state
# as prior to the test
def tearDown():

    pass


# ------------------------------------------------------------------------------
# Test non mpi units
@mock.patch.object(ContinuousOrdered, '__init__', return_value=None)
@mock.patch.object(ContinuousOrdered, 'advance')
@mock.patch.object(ContinuousOrdered, 'stop')
@mock.patch.object(ContinuousOrdered, 'register_subscriber')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_try_schedule(mocked_init,
                      mocked_method1,
                      mocked_method2,
                      mocked_method3,
                      mocked_profiler,
                      mocked_raise_on):

    cfg, session, ns, us, units = setUp()

    co = ContinuousOrdered(cfg=dict(), session=session)
    co._configured          = False
    co._cfg                 = cfg
    co._lrms_info           = cfg['lrms_info']
    co._lrms_lm_info        = cfg['lrms_info']['lm_info']
    co._lrms_n_nodes        = cfg['lrms_info']['n_nodes']
    co._lrms_node_list      = cfg['lrms_info']['node_list']
    co._lrms_cores_per_node = cfg['lrms_info']['cores_per_node']
    co._lrms_gpus_per_node  = cfg['lrms_info']['gpus_per_node']
    co._lrms_lfs_per_node   = cfg['lrms_info']['lfs_per_node']
    co._lrms_mem_per_node   = cfg['lrms_info']['mem_per_node']
    co._slot_lock           = mt.RLock()
    co._prof                = mocked_profiler
    co._ru_term             = None
    co._ru_spawned          = True
    co._ru_is_child         = True
    co._ru_initialized      = True
    co._ru_terminating      = False
    co._log                 = ru.Logger('radical.test', level='DEBUG')
    co._ru_log              = co._log
    co._uid                 = 'co'
    co._popen               = None
    co._ru_name             = 'CO'
    co._ru_is_parent        = False
    co._publishers          = list()
    co._cb_lock             = mt.RLock()
    co._bridges             = list()
    co._components          = list()
    co._parent_pid          = 123


    co.nodes = list()
    for node, node_uid in co._lrms_node_list:
        co.nodes.append({'name' : node,
                         'uid'  : node_uid,
                         'cores': [rpc.FREE] * co._lrms_cores_per_node,
                         'gpus' : [rpc.FREE] * co._lrms_gpus_per_node,
                         'lfs'  : {'size': 10, 'path': '/'},
                         'mem'  : 10,
                            })

    # populate co attributes
    co._configure()
    co._oversubscribe = True

    co._units         = units
    co._ns            = ns
    co._unscheduled   = us

  # pprint.pprint(co._units)
    pprint.pprint(co._ns)

    # we submit no units, and expect `try_schedule` to kick in.  It should
    # schedule all remaining units for order '0' in both ns '0' and '1', but
    # none of the units from order '1'.
    co._schedule_units([])

    pprint.pprint(co._ns)

    assert not co._ns[0][0]['uids']
    assert not co._ns[1][0]['uids']

    assert     co._ns[0][1]['uids']
    assert     co._ns[1][1]['uids']

    tearDown()


# ------------------------------------------------------------------------------

