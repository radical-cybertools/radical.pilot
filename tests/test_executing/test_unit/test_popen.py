# pylint: disable=protected-access, unused-argument
# pylint: disable=no-value-for-parameter
__copyright__ = "Copyright 2013-2016, http://radical.rutgers.edu"
__license__   = "MIT"
import os
import unittest
import subprocess
import radical.utils as ru
from radical.pilot.agent.executing.popen import Popen

try:
    import mock
except ImportError:
    from unittest import mock

# ------------------------------------------------------------------------------
#


class TestBase(unittest.TestCase):



    # ------------------------------------------------------------------------------
    #
    def setUp(self):

        tc = ru.read_json('tests/test_executing/test_unit/test_cases/test_base.json')

        return tc


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'initialize', return_value=None)
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
        cu['uid'] = tests['unit']['uid'] 
        cu['description'] = tests['unit']['description'] 
        component = Popen()
        component._mpi_launcher  = mock.Mock()
        component._mpi_launcher.name = 'mpiexec'
        component._mpi_launcher.command = 'mpiexec'
        component._task_launcher = mock.Mock()
        component._task_launcher.name = 'ssh'
        component._task_launcher.command = 'ssh'

        component.spawn   = mock.MagicMock(side_effect=spawn_side_effect)
        component._log = ru.Logger('dummy')
        component._handle_unit(cu)
        self.assertEqual(cu, global_cu[0])



    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_check_running(self, mocked_init):
        tests = self.setUp()
        cu = dict()
        cu = tests['unit']
        cu['proc'] = subprocess.Popen(args       = '10',
                                      executable = '/bin/sleep',
                                      stdin      = None,
                                      stdout     = None,
                                      stderr     = None,
                                      preexec_fn = os.setsid,
                                      close_fds  = True,
                                      shell      = True,
                                      cwd        = None)

        component = Popen()
        component._cus_to_watch = list()
        component._cus_to_cancel = list()
        component._cus_to_watch.append(cu)
        action = component._check_running()
        self.assertEqual(action,0)
