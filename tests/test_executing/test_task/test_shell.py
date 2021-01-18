
# pylint: disable=protected-access, unused-argument, no-value-for-parameter
#
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"

import unittest

from unittest import mock

import radical.utils as ru

from radical.pilot.agent.executing.shell import Shell


# ------------------------------------------------------------------------------
#
class TestBase(unittest.TestCase):

    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        fname = 'tests/test_executing/test_task/test_cases/test_base.json'

        return ru.read_json(fname)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Shell, '__init__', return_value=None)
    @mock.patch.object(Shell, 'initialize', return_value=None)
    def test_handle_task(self, mocked_init, mocked_initialize):

        global_launcher = []
        global_tasks = []

        def spawn_side_effect(launcher, t):
            nonlocal global_launcher
            nonlocal global_tasks
            global_launcher.append(launcher)
            global_tasks.append(t)

        tests = self.setUp()
        t = dict()
        t['uid']         = tests['task']['uid']
        t['description'] = tests['task']['description']
        t['stderr']      = 'tests/test_executing/test_task/test_cases/'

        component = Shell()
        component._tasks_to_cancel       = []
        component._prof                  = mock.Mock()
        component.publish                = mock.Mock()
        component._mpi_launcher          = mock.Mock()
        component._mpi_launcher.name     = 'mpiexec'
        component._mpi_launcher.command  = 'mpiexec'
        component._task_launcher         = mock.Mock()
        component._task_launcher.name    = 'ssh'
        component._task_launcher.command = 'ssh'
        component._log                   = ru.Logger('dummy')

        component.spawn = mock.MagicMock(side_effect=spawn_side_effect
                (launcher=component._mpi_launcher, t=t))

        component._handle_task(t)
        self.assertEqual(t, global_tasks[0])

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
