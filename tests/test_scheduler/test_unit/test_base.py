# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

import os
import pytest
import radical.pilot
from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#
@mock.patch.object(radical.pilot.utils.Component, '__init__', return_value=None)
@mock.patch('radical.utils.generate_id', return_value='test')
def test_init(mocked_init, mocked_generate_id):
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert not component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = 'true'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = 'YES'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl

    os.environ['RP_UNIFORM_WORKLOAD'] = '1'
    component = AgentSchedulingComponent(cfg={'owner': 'test'}, session=None)

    assert component.nodes == None
    assert component._lrms == None
    assert component._uid == 'test'
    assert component._uniform_wl


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
                        "gpu_map": [],
                        "uid": 1,"mem": None}],
             "lm_info": "INFO",
             "gpus_per_node": 6,
             }

    with pytest.raises(RuntimeError):
        component._change_slot_states(slots=slots, new_state=1)

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

    #with pytest.raises(RuntimeError):
    #    component._change_slot_states(slots=slots, new_state=1)

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