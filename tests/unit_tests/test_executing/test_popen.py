
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"

import unittest
import os

from unittest import mock

import radical.utils as ru

from radical.pilot.agent.launch_method.base import LaunchMethod
from radical.pilot.agent.executing.popen    import Popen


# ------------------------------------------------------------------------------
#


class TestBase(unittest.TestCase):

    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        fname = os.path.dirname(__file__) + '/test_cases/test_base.json'

        return ru.read_json(fname)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'initialize', return_value=None)
    def test_handle_task(self, mocked_init, mocked_initialize):

        global_launcher = []
        global_tasks    = []

        def spawn_side_effect(launcher, task):
            nonlocal global_launcher
            nonlocal global_tasks
            global_launcher.append(launcher)
            global_tasks.append(task)

        tests = self.setUp()
        task  = dict()

        task['uid']         = tests['task']['uid']
        task['description'] = tests['task']['description']

        component = Popen()
        component._mpi_launcher          = mock.Mock()
        component._mpi_launcher.name     = 'mpiexec'
        component._mpi_launcher.command  = 'mpiexec'
        component._task_launcher         = mock.Mock()
        component._task_launcher.name    = 'ssh'
        component._task_launcher.command = 'ssh'

        component.spawn = mock.MagicMock(side_effect=spawn_side_effect
                               (launcher=component._mpi_launcher, task=task))

        component._log = ru.Logger('dummy')
        component._handle_task(task)
        self.assertEqual(task, global_tasks[0])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'initialize', return_value=None)
    def test_check_running(self, mocked_init, mocked_initialize):

        global_tasks   = []
        global_state   = None
        global_publish = None
        global_push    = None

        def _advance_side_effect(task, state, publish, push):
            nonlocal global_tasks
            nonlocal global_state
            nonlocal global_publish
            nonlocal global_push

            global_tasks.append(task)
            global_state   = 'FAILED'
            global_publish = True
            global_push    = True

        tests = self.setUp()
        task = dict()
        task = tests['task']
        task['target_state'] = None
        task['proc']         = mock.Mock()
        task['proc'].poll    = mock.Mock(return_value=1)
        task['proc'].wait    = mock.Mock(return_value=1)

        component = Popen()
        component._tasks_to_watch = list()
        component._tasks_to_cancel = list()
        component._tasks_to_watch.append(task)
        component.advance = mock.MagicMock(side_effect=_advance_side_effect)
        component._prof = mock.Mock()
        component.publish = mock.Mock()
        component._log = ru.Logger('dummy')
        component._check_running()
        self.assertEqual(task['target_state'], global_state)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'initialize', return_value=None)
    @mock.patch.object(LaunchMethod, '__init__', return_value=None)
    @mock.patch.object(LaunchMethod, 'construct_command',
                       return_value=('mpiexec echo hello',None))
    def test_spawn(self, mocked_init, mocked_initialize,
                   mocked_launchmethod, mocked_construct_command):
        tests = self.setUp()
        _pids = []
        task = dict()
        task = tests['task']
        task['slots'] = tests['setup']['lm']['slots']
        task['task_sandbox_path'] = tests['setup']['lm']['task_sandbox']

        launcher  = LaunchMethod()
        component = Popen()
        component._cfg = dict()

        component._cfg['sid']  = 'sid0'
        component._cfg['pid']  = 'pid0'
        component._cfg['aid']  = 'aid0'
        component._uid         = mock.Mock()
        component.gtod         = mock.Mock()
        component._pwd         = mock.Mock()
        component._prof        = mock.Mock()
        component._task_tmp    = mock.Mock()
        component._watch_queue = mock.Mock()
        component._log         = ru.Logger('dummy')

        component.spawn(launcher=launcher, task=task)
        self.assertEqual(len(_pids), 0)


if __name__ == '__main__':

    tc = TestBase()
    tc.test_spawn()
    tc.test_check_running()
    tc.test_handle_task()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
