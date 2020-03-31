# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

import pytest
import threading
import radical.utils as ru
from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

try:
    import mock
except ImportError:
    from unittest import mock


# ------------------------------------------------------------------------------
#
@mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
def test_change_slot_states(mocked_init):

    component = AgentSchedulingComponent()
    component.nodes = [{'uid': 1,
                        'cores': [0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0],
                        'lfs': {'size': 100, 'path': 'test'},
                        'mem': 1024}]

    slots = {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [],
                        "uid": 1,"mem": None}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    component._change_slot_states(slots=slots, new_state=1)
    assert component.nodes == [{'mem': 1024,
                                'cores': [1, 0, 0, 0, 0, 0, 0, 0, 
                                          0, 0, 0, 0, 0, 0, 0, 0],
                                'uid': 1,
                                'lfs': {'path': 'test', 'size': 100}}]

    component.nodes = [{'uid': 2,
                        'cores': [0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0],
                        'lfs': {'size': 100, 'path': 'test'},
                        'mem': 1024,
                        'gpus': [0, 0, 0]}]

    slots = {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [[1]],
                        "uid": 2,"mem": 10}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    component._change_slot_states(slots=slots, new_state=1)
    assert component.nodes == [{'mem': 1014,
                                'cores': [1, 0, 0, 0, 0, 0, 0, 0, 
                                          0, 0, 0, 0, 0, 0, 0, 0],
                                'uid': 2,
                                'lfs': {'path': 'test', 'size': 100},
                                'gpus':[0,1,0]}]

    component.nodes = [{'uid': 2,
                        'cores': [0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0],
                        'lfs': {'size': 100, 'path': 'test'},
                        'mem': 1024,
                        'gpus': [0, 0, 0]}]

    slots = {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [[1]],
                        "uid": 2,"mem": 10}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    component._change_slot_states(slots=slots, new_state=1)
    assert component.nodes == [{'mem': 1014,
                                'cores': [1, 0, 0, 0, 0, 0, 0, 0, 
                                          0, 0, 0, 0, 0, 0, 0, 0],
                                'uid': 2,
                                'lfs': {'path': 'test', 'size': 100},
                                'gpus':[0,1,0]}]

    slots = {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [[1]],
                        "uid": 2,"mem": 10}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    component._change_slot_states(slots=slots, new_state=0)
    assert component.nodes == [{'mem': 1024,
                                'cores': [0, 0, 0, 0, 0, 0, 0, 0, 
                                          0, 0, 0, 0, 0, 0, 0, 0],
                                'uid': 2,
                                'lfs': {'path': 'test', 'size': 100},
                                'gpus':[0,0,0]}]


    component.nodes = [{'uid': 2,
                        'cores': [0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0],
                        'lfs': {'size': 100, 'path': 'test'},
                        'mem': 1024,
                        'gpus': [0, 0, 0]}]

    slots = {"cores_per_node": 16,
             "lfs_per_node": {"size": 0, "path": "/dev/null"},
             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                        "core_map": [[0]],
                        "name": "a",
                        "gpu_map": [],
                        "uid": 1,"mem": None}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    with pytest.raises(RuntimeError):
        component._change_slot_states(slots=slots, new_state=1)


# ------------------------------------------------------------------------------
#
@mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
@mock.patch.object(AgentSchedulingComponent, '_handle_cuda', return_value=True)
@mock.patch.object(AgentSchedulingComponent, 'schedule_unit',
                   return_value={"cores_per_node": 16,
                                 "lfs_per_node": {"size": 0, "path": "/dev/null"},
                                 "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                            "core_map": [[0]],
                                            "name": "a",
                                            "gpu_map": None,
                                            "uid": 1,"mem": None}],
                                 "lm_info": "INFO",
                                 "gpus_per_node": 6,
                                 })
@mock.patch.object(AgentSchedulingComponent, '_change_slot_states',
                   return_value=True)
def test_try_allocation(mocked_init, mocked_schedule_unit, mocked_handle_cuda,
                        mocked_change_slot_states):

    component = AgentSchedulingComponent()
    component._log = ru.Logger('dummy')
    component._allocate_slot = mock.Mock(side_effect=[None, {'slot':'test_slot'}])
    component._prof = mock.Mock()
    component._prof.prof = mock.Mock(return_value=True)
    component._wait_pool = list()
    component._wait_lock = threading.RLock()
    component._slot_lock = threading.RLock()
    unit = {'description':'this is a unit', 'uid': 'test'}
    component._try_allocation(unit=unit)
    assert unit['slots'] == {"cores_per_node": 16,
                             "lfs_per_node": {"size": 0, "path": "/dev/null"},
                             "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                        "core_map": [[0]],
                                        "name": "a",
                                        "gpu_map": None,
                                        "uid": 1,"mem": None}],
                             "lm_info": "INFO",
                             "gpus_per_node": 6,
                             }


# ------------------------------------------------------------------------------
#
@mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
def test_handle_cuda(mocked_init):

    component = AgentSchedulingComponent()
    component._log = ru.Logger('dummy')
    component._cfg = {'rm_info':{'lm_info':{}}}

    unit = dict()
    unit['description'] = {'environment': {}}
    unit['slots'] = {"cores_per_node": 16,
                     "lfs_per_node": {"size": 0, "path": "/dev/null"},
                     "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "core_map": [[0]],
                                "name": "a",
                                "gpu_map": None,
                                "uid": 1,"mem": None}],
                     "lm_info": "INFO",
                     "gpus_per_node": 6,
                     }

    component._handle_cuda(unit)
    assert unit['description']['environment']['CUDA_VISIBLE_DEVICES'] == ''

    unit = dict()
    unit['description'] = {'environment': {}}
    unit['slots'] = {"cores_per_node": 16,
                     "lfs_per_node": {"size": 0, "path": "/dev/null"},
                     "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "core_map": [[0]],
                                "name": "a",
                                "gpu_map": [[1]],
                                "uid": 1,"mem": None}],
                     "lm_info": "INFO",
                     "gpus_per_node": 6,
                     }

    component._handle_cuda(unit)
    assert unit['description']['environment']['CUDA_VISIBLE_DEVICES'] == '1'

    component._cfg = {'rm_info':{'lm_info':{'cvd_id_mode':'physical'}}}
    unit = dict()
    unit['description'] = {'environment': {}}
    unit['slots'] = {"cores_per_node": 16,
                     "lfs_per_node": {"size": 0, "path": "/dev/null"},
                     "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "core_map": [[0]],
                                "name": "a",
                                "gpu_map": [[0],[1]],
                                "uid": 1,"mem": None}],
                     "lm_info": "INFO",
                     "gpus_per_node": 6,
                     }

    component._handle_cuda(unit)
    assert unit['description']['environment']['CUDA_VISIBLE_DEVICES'] == '0,1'

    component._cfg = {'rm_info':{'lm_info':{'cvd_id_mode':'logical'}}}
    unit = dict()
    unit['description'] = {'environment': {}}
    unit['slots'] = {"cores_per_node": 16,
                     "lfs_per_node": {"size": 0, "path": "/dev/null"},
                     "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "core_map": [[0]],
                                "name": "a",
                                "gpu_map": [[]],
                                "uid": 1,"mem": None}],
                     "lm_info": "INFO",
                     "gpus_per_node": 6,
                     }

    component._handle_cuda(unit)
    assert unit['description']['environment']['CUDA_VISIBLE_DEVICES'] == ''

    component._cfg = {'rm_info':{'lm_info':{'cvd_id_mode':'something_else'}}}
    unit = dict()
    unit['description'] = {'environment': {}}
    unit['slots'] = {"cores_per_node": 16,
                     "lfs_per_node": {"size": 0, "path": "/dev/null"},
                     "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                "core_map": [[0]],
                                "name": "a",
                                "gpu_map": [[0]],
                                "uid": 1,"mem": None}],
                     "lm_info": "INFO",
                     "gpus_per_node": 6,
                     }

    with pytest.raises(ValueError):
        component._handle_cuda(unit)


# ------------------------------------------------------------------------------
#
@mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
def test_get_node_maps(mocked_init):
    component = AgentSchedulingComponent()

    cores = [1, 2, 3, 4, 5, 6, 7, 8] 
    gpus  = [1, 2] 
    tpp   = 4 
    core_map, gpu_map = component._get_node_maps(cores, gpus, tpp)
    assert core_map == [[1, 2, 3, 4], [5, 6, 7, 8]]
    assert gpu_map ==  [[1], [2]]

