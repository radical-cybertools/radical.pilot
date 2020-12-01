
# pylint: disable=protected-access, no-value-for-parameter, unused-argument

import threading

from unittest import mock
from unittest import TestCase

import radical.utils as ru

from radical.pilot.agent.scheduler.base import AgentSchedulingComponent


# ------------------------------------------------------------------------------
#
class TestBase(TestCase):


    # --------------------------------------------------------------------------
    #
    def setUp(self):

        fname = 'tests/test_scheduler/test_unit/test_cases/test_base.json'
        tc    = ru.read_json(fname)

        return tc

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_change_slot_states(self, mocked_init):

        tests      = self.setUp()
        nodes      = tests['change_slots']['nodes']
        slots      = tests['change_slots']['slots']
        new_states = tests['change_slots']['new_state']
        results    = tests['change_slots']['results']

        component = AgentSchedulingComponent()

        for node, slot, new_state, result \
                in zip(nodes, slots, new_states, results):
            component.nodes = node
            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    component._change_slot_states(slots=slot,
                                                  new_state=new_state)
            else:
                component._change_slot_states(slots=slot, new_state=new_state)
                self.assertEqual(component.nodes, result)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__',
                       return_value=None)
    @mock.patch.object(AgentSchedulingComponent, '_handle_cuda',
                       return_value=True)
    @mock.patch.object(AgentSchedulingComponent, '_change_slot_states',
                       return_value=True)
    def test_try_allocation(self, mocked_init, mocked_handle_cuda,
                            mocked_change_slot_states):

        component = AgentSchedulingComponent()
        component._log           = ru.Logger('dummy')
        component._allocate_slot = mock.Mock(side_effect=[None,
                                                          {'slot':'test_slot'}])
        component._prof          = mock.Mock()
        component._prof.prof     = mock.Mock(return_value=True)
        component._wait_pool     = list()
        component._wait_lock     = threading.RLock()
        component._slot_lock     = threading.RLock()

        tests = self.setUp()['try_allocation']
        for input_data, result in zip(tests['setup'], tests['results']):
            component.schedule_unit = mock.Mock(
                return_value=input_data['scheduled_unit_slots'])

            unit = input_data['unit']
            component._try_allocation(unit=unit)

            # test unit's slots
            self.assertEqual(unit['slots'], result['slots'])

            # test environment variable(s)
            self.assertEqual(unit['description']['environment'],
                             result['description']['environment'])


    # --------------------------------------------------------------------------
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
                with self.assertRaises(ValueError):
                    component._handle_cuda(unit)
            else:
                component._handle_cuda(unit)
                unit_env = unit['description']['environment']
                if result == 'KeyError':
                    with self.assertRaises(KeyError):
                        self.assertIsNone(unit_env['CUDA_VISIBLE_DEVICES'])
                else:
                    self.assertEqual(unit_env['CUDA_VISIBLE_DEVICES'], result)


    # --------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
