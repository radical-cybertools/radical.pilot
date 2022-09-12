
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import threading     as mt
import radical.utils as ru

from radical.pilot import states as rps

from radical.pilot.agent.scheduler.continuous         import Continuous
from radical.pilot.agent.scheduler.continuous_ordered import ContinuousOrdered


class TestContinuousOrdered(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(ContinuousOrdered, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_configure', return_value=None)
    @mock.patch.object(ContinuousOrdered, 'register_subscriber')
    def test_configure(self, mocked_init, mocked_init_continuous, mocked_subscriber):

        '''
        Test 1 check configuration setup
        '''
        cfg = dict()

        component = ContinuousOrdered(cfg=None, session=None)
        component._trigger_state  = rps.TMGR_STAGING_OUTPUT_PENDING
        component._lock           = mt.RLock()
        component._cfg            = cfg
        component._ru_terminating = True
        component._uid            = None
        component._log            = ru.Logger('dummy')
        component._tasks          = dict()
        component._unordered      = list()
        component._ns             = dict()
        component._ns_init        = {'current' : 0}
        component._order_init     = {'size'    : 0,
                                 'uids'    : list(),
                                 'done'    : list()}

        component._configure()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(ContinuousOrdered, '__init__', return_value=None)
    @mock.patch.object(ContinuousOrdered, '_try_schedule', return_value=None)
    @mock.patch.object(Continuous, 'advance')
    def test_schedule_tasks(self, mocked_init, mocked_try_schedule, mocked_advance):

        '''
        Test 2 check schedule_tasks
        '''
        component = ContinuousOrdered(cfg=None, session=None)
        component._ru_terminating = True
        component._lock           = mt.RLock()
        component._log            = ru.Logger('dummy')
        component._unordered      = list()
        tasks = [
                {"uid"        : "task.000001",
                 "description": {"executable"     : "/bin/sleep",
                                 "arguments"      : ["10"],
                                 "gpu_processes"  : 1,
                                 "cpu_processes"  : 1,
                                 "cpu_threads"    : 1,
                                 "gpu_threads"    : 1,
                                 "mem_per_process": 128,
                                 "lfs_per_process": 2
                                }
                },
                {"uid"        : "task.000002",
                 "description": {"executable"     : "/bin/sleep",
                                 "arguments"      : ["20"],
                                 "gpu_processes"  : 1,
                                 "cpu_processes"  : 1,
                                 "cpu_threads"    : 1,
                                 "gpu_threads"    : 1,
                                 "mem_per_process": 128,
                                 "lfs_per_process": 2
                                },
                }]

        for task in tasks:
            component._schedule_task(task)


    # ------------------------------------------------------------------------------
    #


    @mock.patch.object(ContinuousOrdered, '__init__', return_value=None)
    @mock.patch.object(Continuous, '_try_allocation', return_value=None)
    @mock.patch.object(Continuous, 'advance')
    def test_try_schedule(self, mocked_init, mocked_try_allocation, mocked_advance):
        '''
        Test 3 check try_schedule
        '''
        component = ContinuousOrdered(cfg=None, session=None)
        component._lock      = mt.RLock()
        component._log       = ru.Logger('dummy')
        component._unordered = list()
        component._ns        = dict()
        component._try_schedule()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(ContinuousOrdered, '__init__', return_value=None)
    @mock.patch.object(ContinuousOrdered, '_try_schedule', return_value=None)
    @mock.patch.object(Continuous, 'advance')
    def test_state_cb(self, mocked_init, mocked_try_schedule, mocked_advance):
        '''
        Test 4 check state_cb
        '''
        component = ContinuousOrdered(cfg=None, session=None)
        component._lock = mt.RLock()
        component._log  = ru.Logger('dummy')

        msg   = {'cmd':'', 'arg':''}
        topic = None

        component._state_cb(topic, msg)


if __name__ == '__main__':

    tc = TestContinuousOrdered()
    tc.test_configure()
    tc.test_state_cb()
    tc.test_try_schedule()
    tc.test_schedule_tasks()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
