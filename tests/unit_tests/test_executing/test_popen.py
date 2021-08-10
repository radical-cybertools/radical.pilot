#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2013-2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import threading as mt

import radical.pilot.states as rps
import radical.utils as ru

from unittest import mock, TestCase

from radical.pilot.agent.launch_method.fork import Fork
from radical.pilot.agent.executing.popen    import Popen

TEST_CASES_DIR = 'tests/unit_tests/test_executing/test_cases'


# ------------------------------------------------------------------------------
#
class TestPopen(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:
        cls._test_case = ru.read_json('%s/test_base.json' % TEST_CASES_DIR)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_command_cb(self, mocked_logger, mocked_init):

        pex = Popen(cfg=None, session=None)
        pex._log             = mocked_logger()
        pex._cancel_lock     = mt.RLock()
        pex._tasks_to_cancel = []

        msg = {'cmd': '', 'arg': {'uids': ['task.0000', 'task.0001']}}
        self.assertTrue(pex.command_cb(topic=None, msg=msg))
        # tasks were not added to the list `_tasks_to_cancel`
        self.assertFalse(pex._tasks_to_cancel)

        msg['cmd'] = 'cancel_tasks'
        self.assertTrue(pex.command_cb(topic=None, msg=msg))
        # tasks were added to the list `_tasks_to_cancel`
        self.assertEqual(pex._tasks_to_cancel, msg['arg']['uids'])


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(Popen, 'find_launcher', return_value=None)
    @mock.patch.object(Fork, '__init__', return_value=None)
    @mock.patch('subprocess.Popen')
    def test_handle_task(self, mocked_sp_popen, mocked_lm_init,
                         mocked_find_launcher, mocked_init):

        task = dict(self._test_case['task'])
        task['slots'] = self._test_case['setup']['slots']

        pex = Popen(cfg=None, session=None)

        with self.assertRaises(RuntimeError):
            # no launcher
            pex._handle_task(task)

        pex._log = pex._prof = pex._watch_queue = mock.Mock()
        pex._pwd     = ''
        pex._pid     = 'pilot.0000'
        pex.sid      = 'session.0000'
        pex.resource = 'resource_label'
        pex.rsbox    = ''
        pex.ssbox    = ''
        pex.psbox    = ''
        pex.lfs      = '/tmp'
        pex.gtod     = ''
        pex.prof     = ''

        launcher = Fork(name=None, lm_cfg={}, cfg={}, log=None, prof=None)
        launcher.name    = 'FORK'
        launcher._env_sh = 'env/lm_fork.sh'
        mocked_find_launcher.return_value = launcher

        pex._handle_task(task)

        for prefix in ['.launch.sh', '.exec.sh', '.sl']:
            path = '%s/%s%s' % (task['task_sandbox_path'], task['uid'], prefix)
            self.assertTrue(os.path.isfile(path))
            try   : os.remove(path)
            except: pass


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch('os.killpg')
    def test_check_running(self, mocked_killpg, mocked_init):

        task = dict(self._test_case['task'])
        task['target_state'] = None

        pex = Popen(cfg=None, session=None)
        pex._tasks_to_watch  = []
        pex._tasks_to_cancel = []
        pex._cancel_lock     = mt.RLock()
        pex._log    = pex._prof   = mock.Mock()
        pex.advance = pex.publish = mock.Mock()

        # case 1: exit_code is None, task to be cancelled
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = None
        pex._tasks_to_watch.append(task)
        pex._tasks_to_cancel.append(task['uid'])
        pex._check_running()
        self.assertFalse(pex._tasks_to_cancel)

        # case 2: exit_code == 0
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = 0
        pex._tasks_to_watch.append(task)
        pex._check_running()
        self.assertEqual(task['target_state'], rps.DONE)

        # case 3: exit_code == 1
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = 1
        pex._tasks_to_watch.append(task)
        pex._check_running()
        self.assertEqual(task['target_state'], rps.FAILED)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestPopen()
    tc.test_command_cb()
    tc.test_check_running()
    tc.test_handle_task()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
