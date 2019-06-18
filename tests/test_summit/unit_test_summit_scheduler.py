
import os
import json
import glob
import shutil
import pytest

import radical.utils as ru

from radical.pilot.agent.scheduler.continuous_summit import ContinuousSummit

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
resource_name = 'local.localhost'
access_schema = 'ssh'
session_id    = 'rp.session.testing.local.0000'

# Sample data to be staged -- available in cwd
cur_dir = os.path.dirname(os.path.abspath(__file__))


# ------------------------------------------------------------------------------
# 
def setUp():

    test_cases = json.load(open('test_cases_summit_scheduler.json'))

    return test_cases.pop('cfg'),test_cases['allocate'],test_cases['release']


# ------------------------------------------------------------------------------
#
def tearDown():
    rp = glob.glob('%s/rp.session.*' % os.getcwd())
    for fold in rp:
        shutil.rmtree(fold)



# ------------------------------------------------------------------------------
#
@mock.patch.object(ContinuousSummit, '__init__', return_value=None)
@mock.patch.object(ContinuousSummit, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_allocate_slot(mocked_init,
                       mocked_method,
                       mocked_profiler,
                       mocked_raise_on):

    cfg, test_cases, _  = setUp()

    component = ContinuousSummit(cfg=cfg, session=None)

    component._cfg = cfg
    component._lrms_info             = cfg['lrms_info']
    component._lrms_lm_info          = cfg['lrms_info']['lm_info']
    component._lrms_node_list        = cfg['lrms_info']['node_list']
    component._lrms_sockets_per_node = cfg['lrms_info']['sockets_per_node']
    component._lrms_cores_per_socket = cfg['lrms_info']['cores_per_socket']
    component._lrms_gpus_per_socket  = cfg['lrms_info']['gpus_per_socket']
    component._lrms_lfs_per_node     = cfg['lrms_info']['lfs_per_node']
    component._tag_history           = dict()
    component._log                   = ru.get_logger('dummy')

    component._configure()

    for i in range(len(test_cases['trigger'])):

        if test_cases['final_state'][i] == "Error":

            with pytest.raises(ValueError):
                component.nodes = test_cases['init_state'][i]
                component._allocate_slot(test_cases['trigger'][i])
        else:

            component.nodes = test_cases['init_state'][i]
            slot = component._allocate_slot(test_cases['trigger'][i])

            assert slot            == test_cases['slot'][i]
            assert component.nodes == test_cases['final_state'][i]

    tearDown()


# ------------------------------------------------------------------------------
#
@mock.patch.object(ContinuousSummit, '__init__', return_value=None)
@mock.patch.object(ContinuousSummit, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
def test_release_slot(mocked_init,
                      mocked_method,
                      mocked_profiler,
                      mocked_raise_on):

    cfg, _, test_cases = setUp()

    component = ContinuousSummit(cfg=cfg, session=None)

    component._cfg                   = cfg
    component._lrms_info             = cfg['lrms_info']
    component._lrms_lm_info          = cfg['lrms_info']['lm_info']
    component._lrms_node_list        = cfg['lrms_info']['node_list']
    component._lrms_sockets_per_node = cfg['lrms_info']['sockets_per_node']
    component._lrms_cores_per_socket = cfg['lrms_info']['cores_per_socket']
    component._lrms_gpus_per_socket  = cfg['lrms_info']['gpus_per_socket']
    component._lrms_lfs_per_node     = cfg['lrms_info']['lfs_per_node']
    component._tag_history           = dict()
    component._log                   = ru.Logger('dummy')

    component._configure()

    for i in range(len(test_cases['trigger'])):

        component.nodes = test_cases['init_state'][i]
        component._release_slot(test_cases['trigger'][i])

        assert component.nodes ==  test_cases['final_state'][i]

    tearDown()    


# ------------------------------------------------------------------------------
#
@mock.patch.object(ContinuousSummit, '__init__', return_value=None)
@mock.patch.object(ContinuousSummit, 'advance')
@mock.patch.object(ru.Profiler, 'prof')
@mock.patch('radical.utils.raise_on')
@mock.patch.object(ContinuousSummit, 'register_input')
@mock.patch.object(ContinuousSummit, 'register_output')
@mock.patch.object(ContinuousSummit, 'register_subscriber')
@mock.patch.object(ContinuousSummit, 'register_publisher')
def test_initialize_child(mocked_init,
                          mocked_method,
                          mocked_profiler,
                          mocked_raise_on,
                          mocked_input,
                          mocked_output,
                          mocked_subscriber,
                          mocked_publisher):

    cfg, _, test_cases = setUp()

    component      = ContinuousSummit(cfg=cfg, session=None)
    component._log = ru.Logger('dummy')
    component._cfg = cfg

    component.initialize_child()


# ------------------------------------------------------------------------------

