
# pylint: disable=protected-access, unused-argument, no-value-for-parameter
#
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"

import unittest
import os

from unittest import mock

import radical.utils as ru

from radical.pilot.agent.executing.shell import Shell


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
    @mock.patch.object(Shell, '__init__', return_value=None)
    @mock.patch.object(Shell, 'initialize', return_value=None)
    def test_handle_unit(self, mocked_init, mocked_initialize):

        global_launcher = []
        global_cu = []

        def spawn_side_effect(launcher, cu):
            nonlocal global_launcher
            nonlocal global_cu
            global_launcher.append(launcher)
            global_cu.append(cu)

        tests = self.setUp()
        cu = dict()
        cu['uid']         = tests['unit']['uid']
        cu['description'] = tests['unit']['description']
        cu['stderr']      = '/test_cases/'

        component = Shell()
        component._cus_to_cancel         = []
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
                (launcher=component._mpi_launcher, cu=cu))

        component._handle_unit(cu)
        self.assertEqual(cu, global_cu[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Shell, '__init__', return_value=None)
    @mock.patch.object(Shell, 'initialize', return_value=None)
    def test_cu_to_cmd(self, mocked_init, mocked_initialize):
        self.maxDiff = None
        tests = self.setUp()
        cu = dict()
        cu['uid']         = tests['unit']['uid']
        cu['description'] = tests['unit']['description']
        cu['unit_sandbox_path'] = tests['unit']['unit_sandbox_path']
        cu['gtod'] = tests['unit']['gtod']

        component = Shell()
        component._log                   = ru.Logger('dummy')
        component._cfg = {'sid': 'test_sid', 'pid': 'test_pid', 'aid': 'test_aid'}
        component._uid = 'exec.0000'
        component._pwd = './'
        component._deactivate = ''
        component._env_cu_export = {}

        component._prof = mock.MagicMock()
        component._prof.enabled = True

        launcher = mock.MagicMock()
        launcher.construct_command = mock.MagicMock(return_value=('some_exe', None))

        script = component._cu_to_cmd(cu, launcher)
        self.assertEqual(script, tests['results']['script'])

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
