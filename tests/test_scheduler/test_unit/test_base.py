# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter

from unittest import TestCase
import pytest
import threading
import radical.utils as ru
from radical.pilot.agent.scheduler.base import AgentSchedulingComponent

try:
    import mock
except ImportError:
    from unittest import mock


class TestBase(TestCase):


    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        tc = ru.read_json('tests/test_scheduler/test_unit/test_cases/test_base.json')

        return tc

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_change_slot_states(self, mocked_init):

        tests      = self.setUp()
        nodes      = tests['change_slots']['nodes']
        slots      = tests['change_slots']['slots']
        new_states = tests['change_slots']['new_state']
        results    = tests['change_slots']['results']

        component = AgentSchedulingComponent()

        for node, slot, new_state, result in zip(nodes, slots, new_states, results):
            component.nodes = node
            if result == 'RuntimeError':
                with pytest.raises(RuntimeError):
                    component._change_slot_states(slots=slot, new_state=new_state)
            else:
                component._change_slot_states(slots=slot, new_state=new_state)
                self.assertEqual(component.nodes, result)


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
    def test_try_allocation(self, mocked_init, mocked_schedule_unit, mocked_handle_cuda,
                            mocked_change_slot_states):

        component = AgentSchedulingComponent()
        component._log = ru.Logger('dummy')
        component._allocate_slot = mock.Mock(side_effect=[None, {'slot':'test_slot'}])
        component._prof = mock.Mock()
        component._prof.prof = mock.Mock(return_value=True)
        component._wait_pool = list()
        component._wait_lock = threading.RLock()
        component._slot_lock = threading.RLock()
        unit = {'description':{'note':'this is a unit'}, 'uid': 'test'}
        component._try_allocation(unit=unit)
        self.assertEqual(unit['slots'], {"cores_per_node": 16,
                                 "lfs_per_node": {"size": 0, "path": "/dev/null"},
                                 "nodes": [{"lfs": {"path": "/dev/null", "size": 0},
                                            "core_map": [[0]],
                                            "name": "a",
                                            "gpu_map": None,
                                            "uid": 1,"mem": None}],
                                 "lm_info": "INFO",
                                 "gpus_per_node": 6,
                                 })


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_handle_cuda(self,mocked_init):

        tests     = self.setUp()
        setups    = tests['handle_cuda']['setup']
        units     = tests['handle_cuda']['unit']
        results   = tests['handle_cuda']['results']
        component = AgentSchedulingComponent()
        component._log = ru.Logger('dummy')

        for setup, unit, result in zip(setups, units, results):
            component._cfg = setup
            if result == 'ValueError':
                with pytest.raises(ValueError):
                    component._handle_cuda(unit)
            else:
                component._handle_cuda(unit)
                self.assertEqual(unit['description']['environment']['CUDA_VISIBLE_DEVICES'],
                                 result)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_get_node_maps(self,mocked_init):
        component = AgentSchedulingComponent()

        cores = [1, 2, 3, 4, 5, 6, 7, 8] 
        gpus  = [1, 2] 
        tpp   = 4 
        core_map, gpu_map = component._get_node_maps(cores, gpus, tpp)
        self.assertEqual(core_map, [[1, 2, 3, 4], [5, 6, 7, 8]])
        self.assertEqual(gpu_map, [[1], [2]])

