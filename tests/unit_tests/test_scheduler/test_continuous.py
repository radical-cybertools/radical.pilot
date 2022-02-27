#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import copy
import glob

from unittest import mock, TestCase

import radical.utils           as ru
import radical.pilot.constants as rpc

from radical.pilot.agent.scheduler.continuous import Continuous

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestContinuous(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._test_cases = []
        for f in glob.glob('%s/test_cases/task.*.json' % base):
            cls._test_cases.append(ru.read_json(f))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_find_resources(self, mocked_init):

        component = Continuous(cfg=None, session=None)
        component._uid  = 'agent_scheduling.0001'
        component._log  = mock.Mock()
        component._prof = mock.Mock()

        for test_case in self._test_cases:

          # component._rm.info.lfs_per_node = 1024

            td = test_case['task']['description']
            alc_slots = component._find_resources(
                node=test_case['setup']['nodes'][0],
                find_slots=td['cpu_processes'],
                cores_per_slot=td['cpu_threads'],
                gpus_per_slot=td['gpu_processes'],
                lfs_per_slot=td['lfs_per_process'],
                mem_per_slot=td['mem_per_process'],
                partial=False)

            if alc_slots is None:
                # tests should be defined in a way, so task would fit 1 node
                raise AssertionError('Test should be redefined (%s)' %
                                     test_case['task']['uid'])

            self.assertEqual(alc_slots, test_case['result']['slots']['ranks'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_schedule_task(self, mocked_logger, mocked_configure, mocked_init):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0002'
        component._log = mocked_logger

        for test_case in self._test_cases:

            nodes = test_case['setup']['nodes']

            component._rm      = mock.Mock()
            component._rm.info = ru.TypedDict(from_dict={
                'cores_per_node': len(nodes[0]['cores']),
                'gpus_per_node' : len(nodes[0]['gpus']),
                'lfs_per_node'  : nodes[0]['lfs'],
                'mem_per_node'  : nodes[0]['mem']})

            component._colo_history      = {}
            component._tagged_nodes      = set()
            component._scattered         = None
            component._node_offset       = 0
            component._partitions        = {}
            component.nodes              = nodes

            task  = test_case['task']
            slots = component.schedule_task(task)

            self.assertEqual(slots, test_case['result']['slots'])
            self.assertEqual(component._colo_history,
                             test_case['result']['colo_history'])

            # check tags exclusiveness (default: exclusive=False)

            task['description'].update({'tags': {'colocate': 'exclusive_tag',
                                                 'exclusive': True},
                                        'cpu_processes': 1,
                                        'cpu_threads': 1,
                                        'gpu_processes': 0})
            pre_sched_n_tagged_nodes = len(component._tagged_nodes)
            component.schedule_task(task)
            post_sched_n_tagged_nodes = len(component._tagged_nodes)
            if pre_sched_n_tagged_nodes < len(component.nodes):
                # new task got exclusive node(s)
                self.assertEqual(post_sched_n_tagged_nodes,
                                 pre_sched_n_tagged_nodes + 1)
            else:
                # new task shared allocated nodes
                self.assertEqual(post_sched_n_tagged_nodes,
                                 pre_sched_n_tagged_nodes)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_unschedule_task(self, mocked_logger, mocked_init):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0003'
        component._log = mocked_logger

        for test_case in self._test_cases:

            component.nodes = copy.deepcopy(test_case['setup']['nodes'])

            task = {'description': test_case['task']['description'],
                    'slots'      : test_case['result']['slots']}

            # set corresponding cores/gpus as busy
            component._change_slot_states(task['slots'], rpc.BUSY)
            # check that nodes got changed
            self.assertNotEqual(component.nodes, test_case['setup']['nodes'])

            component.unschedule_task(task)

            # nodes are back to the initial state
            self.assertEqual(component.nodes, test_case['setup']['nodes'])


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestContinuous()
    tc.setUpClass()
    tc.test_find_resources()
    tc.test_schedule_task()
    tc.test_unschedule_task()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
