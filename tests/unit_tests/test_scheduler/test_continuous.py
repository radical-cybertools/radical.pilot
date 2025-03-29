#!/usr/bin/env python3

# pylint: disable=protected-access, no-value-for-parameter, unused-argument

import os
import copy
import glob

import multiprocessing as mp

from unittest import mock, TestCase

import radical.utils           as ru
import radical.pilot           as rp
import radical.pilot.constants as rpc

from radical.pilot.agent.resource_manager     import RMInfo

from radical.pilot.agent.scheduler.continuous import Continuous

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestContinuous(TestCase):

    maxDiff = None

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

            td = test_case['task']['description']
            sd_options = {'n_slots'       : td['ranks'],
                          'cores_per_slot': td['cores_per_rank'],
                          'gpus_per_slot' : td['gpus_per_rank'],
                          'lfs_per_slot'  : td['lfs_per_rank'],
                          'mem_per_slot'  : td['mem_per_rank']}

            alc_slots = component._find_resources(
                node=test_case['setup']['nodes'][0],
                partial=True,
                **sd_options
            )

            # number of ranks to run on a single node
            ranks = len(alc_slots)
            alc = rp.utils.convert_slots_to_new(alc_slots)
            cmp = rp.utils.convert_slots_to_new(test_case['result']['slots'][:ranks])
            self.assertEqual(alc, cmp)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_scheduling(self, mocked_init):

        component = Continuous(cfg=None, session=None)
        component._uid  = 'agent_scheduling.0002'
        component._log  = mock.Mock()
        component._log  = ru.Logger('radical.pilot', targets='/tmp/t.log',
                                    level='DEBUG_9')
        component._prof = mock.Mock()

        component._log._debug_level = 0

        # test `_schedule_incoming` and `_schedule_waitpool`

        for test_case in self._test_cases:

            task  = copy.deepcopy(test_case['task'])
            nodes = copy.deepcopy(test_case['setup']['nodes'])

            component.nodes    = nodes

            component._rm      = mock.Mock()
            component._rm.info = RMInfo({
                'cores_per_node': len(nodes[0]['cores']),
                'gpus_per_node' : len(nodes[0]['gpus']),
                'lfs_per_node'  : nodes[0]['lfs'],
                'mem_per_node'  : nodes[0]['mem']})

            component._active_cnt    = 0
            component._colo_history  = {}
            component._tagged_nodes  = set()
            component._node_offset   = 0
            component._scattered     = None
            component._partition_ids = {}
            component._term          = mp.Event()
            component._queue_sched   = mp.Queue()
            component._waitpool      = {}

            def advance(tasks, *args, **kwargs):
                tasks = ru.as_list(tasks)
                for t in tasks:
                    td = t['description']
                    self.assertEqual(
                        t['resources'], {'cpu': td['ranks'] *
                                                td['cores_per_rank'],
                                         'gpu': td['ranks'] *
                                                td['gpus_per_rank']})

            component.advance = advance

            self.assertIsNone(task.get('resources'))
            component._queue_sched.put(([task], Continuous._SCHEDULE))
            component._schedule_incoming()

            slots = test_case['result']['slots']
            slots = rp.utils.convert_slots_to_new(slots)
            component._change_slot_states(slots, rpc.FREE)

            component._set_tuple_size(task)
            component._waitpool = {0: {task['uid']: task}}

            component._schedule_waitpool()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Continuous, '__init__', return_value=None)
    def test_schedule_task(self, mocked_init):

        component = Continuous(cfg=None, session=None)
        component._uid = 'agent_scheduling.0003'
        component._log  = mock.Mock()
      # component._log  = ru.Logger('radical.pilot', targets='/tmp/t.log',
      #                             level='DEBUG_9')

        for test_case in self._test_cases:

            task  = copy.deepcopy(test_case['task'])
            nodes = copy.deepcopy(test_case['setup']['nodes'])

            component._rm      = mock.Mock()
            component._rm.info = RMInfo({
                'cores_per_node': len(nodes[0]['cores']),
                'gpus_per_node' : len(nodes[0]['gpus']),
                'lfs_per_node'  : nodes[0]['lfs'],
                'mem_per_node'  : nodes[0]['mem']})

            component._colo_history  = {}
            component._tagged_nodes  = set()
            component._scattered     = None
            component._node_offset   = 0
            component._partition_ids = {}
            component.nodes          = nodes

            slots, partition = component.schedule_task(task)

            alc = rp.utils.convert_slots_to_new(slots)
            cmp = rp.utils.convert_slots_to_new(test_case['result']['slots'])
            self.assertEqual(alc[0], cmp[0])
            self.assertEqual(component._colo_history,
                             test_case['result']['colo_history'])

            # check tags exclusiveness (default: exclusive=False)

            task['description'].update({'tags': {'colocate' : 'exclusive_tag',
                                                 'exclusive': True},
                                        'ranks'         : 1,
                                        'cores_per_rank': 1,
                                        'gpus_per_rank' : 0})

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
        component._uid = 'agent_scheduling.0004'
        component._log = mocked_logger

        for test_case in self._test_cases:

            component.nodes = copy.deepcopy(test_case['setup']['nodes'])

            task = {'description': test_case['task']['description'],
                    'slots'      : test_case['result']['slots'],
                    'uid'        : 'task.000000'}

            task['slots'] = rp.utils.convert_slots_to_new(task['slots'])

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
    tc.test_scheduling()
    tc.test_schedule_task()
    tc.test_unschedule_task()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
