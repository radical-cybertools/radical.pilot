
# pylint: disable=protected-access, no-value-for-parameter, unused-argument

import threading
import os

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

        fname = os.path.dirname(__file__) + '/test_cases/test_base.json'
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
            component.schedule_task = mock.Mock(
                return_value=input_data['scheduled_task_slots'])

            task = input_data['task']
            component._try_allocation(task=task)

            # test task's slots
            self.assertEqual(task['slots'], result['slots'])

            # test environment variable(s)
            self.assertEqual(task['description']['environment'],
                             result['description']['environment'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(AgentSchedulingComponent, '__init__', return_value=None)
    def test_handle_cuda(self,mocked_init):

        tests     = self.setUp()
        setups    = tests['handle_cuda']['setup']
        tasks     = tests['handle_cuda']['task']
        results   = tests['handle_cuda']['results']
        component = AgentSchedulingComponent()
        component._log = ru.Logger('dummy')

        for setup, task, result in zip(setups, tasks, results):
            component._cfg = setup
            if result == 'ValueError':
                with self.assertRaises(ValueError):
                    component._handle_cuda(task)
            else:
                component._handle_cuda(task)
                task_env = task['description']['environment']
                if result == 'KeyError':
                    with self.assertRaises(KeyError):
                        self.assertIsNone(task_env['CUDA_VISIBLE_DEVICES'])
                else:
                    self.assertEqual(task_env['CUDA_VISIBLE_DEVICES'], result)


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


if __name__ == '__main__':

    tc = TestBase()
    tc.test_get_node_maps()
    tc.test_handle_cuda()
    tc.test_try_allocation()
    tc.test_change_slot_states()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
