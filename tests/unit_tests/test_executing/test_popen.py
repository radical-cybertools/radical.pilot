#!/usr/bin/env python3

# pylint: disable=protected-access, unused-argument, no-value-for-parameter

__copyright__ = 'Copyright 2013-2023, The RADICAL-Cybertools Team'
__license__   = 'MIT'

import os
import queue

import threading as mt

import radical.pilot.constants as rpc
import radical.pilot.states    as rps
import radical.utils           as ru

from unittest import mock, TestCase

from radical.pilot.agent.resource_manager.base import ResourceManager
from radical.pilot.agent.launch_method.aprun   import APRun
from radical.pilot.agent.executing.popen       import Popen

base = os.path.abspath(os.path.dirname(__file__))


# ------------------------------------------------------------------------------
#
class TestPopen(TestCase):

    # --------------------------------------------------------------------------
    #
    @classmethod
    def setUpClass(cls) -> None:

        cls._test_case = ru.read_json('%s/test_cases/test_base.json' % base)
        assert cls._test_case, 'how is this test supposed to work???'

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_control_cb(self, mocked_logger, mocked_init):

        mocked_logger._debug_level = 1

        check = list()
        def cancel_task(task):
            check.append(task['uid'])

        pex = Popen(cfg=None, session=None)
        pex._log             = mocked_logger()
        pex._cancel_lock     = mt.RLock()
        pex.cancel_task      = cancel_task
        pex._tasks           = {'task.0000': {'uid': 'task.0000'},
                                'task.0001': {'uid': 'task.0001'}}

        msg = {'cmd': '', 'arg': {'uids': ['task.0000', 'task.0001']}}
        self.assertIsNone(pex.control_cb(topic=None, msg=msg))

        msg['cmd'] = 'cancel_tasks'
        self.assertIsNone(pex.control_cb(topic=None, msg=msg))
        for uid in msg['arg']['uids']:
            self.assertIn(uid, check)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch.object(ResourceManager, 'find_launcher', return_value=None)
    @mock.patch.object(APRun, '__init__', return_value=None)
    @mock.patch('subprocess.Popen')
    def test_handle_task(self, mocked_sp_popen, mocked_lm_init,
                         mocked_find_launcher, mocked_init):

        launcher = APRun(name=None, lm_cfg={}, rm_info={}, log=None, prof=None)
        launcher.name     = 'APRUN'
        launcher._command = '/bin/aprun'
        launcher._env_sh  = 'env/lm_aprun.sh'
        mocked_find_launcher.return_value = (launcher, 'APRUN')

        task = dict(self._test_case['task'])
        task['slots'] = self._test_case['setup']['slots']
        task['partition'] = 0

        pex = Popen(cfg=None, session=None)

        pex._log = pex._prof  = pex._watch_queue = mock.Mock()
        pex._log._debug_level = 1

        pex._pwd     = ''
        pex.pid      = 'pilot.0000'
        pex.sid      = 'session.0000'
        pex.resource = 'resource_label'
        pex.rsbox    = ''
        pex.ssbox    = ''
        pex.psbox    = ''
        pex.gtod     = ''
        pex.prof     = ''
        pex.rp_ctrl  = ''

        pex._session      = mock.Mock()
        pex._session.rcfg = ru.Config(from_dict={'new_session_per_task': False})
        pex.is_canceled   = mock.Mock(return_value=False)

        pex._rm      = mock.Mock()
        pex._rm.find_launcher = mocked_find_launcher

        pex._reg = ru.TypedDict(
                {'bridges.control_pubsub': {'addr_pub': 'addr_pub',
                                            'addr_sub': 'addr_sub'}})
        pex._reg.dump = mock.Mock()

        pex._handle_task(task)

        popen_input_kwargs = mocked_sp_popen.call_args_list[0][1]
        self.assertFalse(popen_input_kwargs['start_new_session'])

        for prefix in ['.launch.sh', '.exec.sh']:
            path = '%s/%s%s' % (task['task_rundir_path'], task['uid'], prefix)
            self.assertTrue(os.path.isfile(path))

            with ru.ru_open(path) as fd:
                content = fd.read()

            if 'launch' in prefix:
                self.assertIn('$RP_PROF launch_start', content)
                self.assertIn('$RP_LAUNCH_PID',        content)

            elif 'exec' in prefix:
                self.assertIn('$RP_PROF exec_start',   content)
                self.assertIn('$RP_EXEC_PID',          content)
                self.assertIn('$RP_RANK_PID',          content)
                for pre_exec_cmd in task['description']['pre_exec']:

                    if isinstance(pre_exec_cmd, str):
                        self.assertIn('%s' % pre_exec_cmd, content)

                    elif isinstance(pre_exec_cmd, dict):
                        self.assertIn('case "$RP_RANK" in', content)
                        for rank_id, cmds in pre_exec_cmd.items():
                            self.assertIn('%s)\n' % rank_id, content)
                            for cmd in ru.as_list(cmds):
                                self.assertIn('%s' % cmd, content)

            try   : os.remove(path)
            except: pass

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_extend_pre_exec(self, mocked_init):

        pex = Popen(cfg=None, session=None)

        pex._session      = mock.Mock()
        pex._session.rcfg = {}

        td    = {'cores_per_rank': 2,
                 'threading_type': '',
                 'gpus_per_rank' : 1,
                 'gpu_type'      : '',
                 'pre_exec'      : []}
        slots = [{'cores': [{'index': 0, 'occupation': 1.0},
                            {'index': 1, 'occupation': 1.0}],
                  'gpus' : [{'index': 5, 'occupation': 1.0}]}]

        pex._extend_pre_exec(td, slots)
        self.assertNotIn('export OMP_NUM_THREADS=2', td['pre_exec'])
        self.assertFalse(bool(td['pre_exec']))

        td.update({'threading_type': rpc.OpenMP,
                   'gpu_type'      : rpc.CUDA})

        # we target attribute "task_pre_exec"
        pex._session.rcfg = {'task_pre_exec': ['export TEST_ENV=test']}

        pex._extend_pre_exec(td, slots)
        self.assertIn('export OMP_NUM_THREADS=2',             td['pre_exec'])
        self.assertIn({'0': 'export CUDA_VISIBLE_DEVICES=5'}, td['pre_exec'])
        self.assertIn('export TEST_ENV=test', td['pre_exec'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    @mock.patch('os.killpg')
    def test_check_running(self, mocked_killpg, mocked_init):

        class Launcher(object):
            def cancel_task(self, task):
                pass

        task = dict(self._test_case['task'])
        task['target_state']  = None
        task['launcher_name'] = None

        pex = Popen(cfg=None, session=None)
        pex._log    = pex._prof   = mock.Mock()
        pex.advance = pex.publish = mock.Mock()
        pex._rm     = mock.Mock()
        pex._rm._get_launcher = mock.Mock(return_value=Launcher())
        pex._tasks  = {task['uid']: task}
        pex._check_lock = mt.Lock()

        os.getpgid = mock.Mock()
        os.killpg  = mock.Mock()

        to_watch  = list()

        # case 1: exit_code is None, task to be cancelled
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = None
        task['proc'].pid = os.getpid()
        pex.cancel_task(task)
        self.assertNotIn(task['uid'], pex._tasks)

        # case 2: exit_code == 0
        pex._tasks   = {task['uid']: task}
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = 0
        to_watch.append(task)
        pex._check_running(to_watch)
        self.assertEqual(task['target_state'], rps.DONE)

        # case 3: exit_code == 1
        pex._tasks   = {task['uid']: task}
        task['proc'] = mock.Mock()
        task['proc'].poll.return_value = 1
        to_watch.append(task)
        pex._check_running(to_watch)
        self.assertEqual(task['target_state'], rps.FAILED)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Popen, '__init__', return_value=None)
    def test_get_rank_ids(self, mocked_init):

        pex = Popen(cfg=None, session=None)

        launcher = mock.Mock()
        launcher.get_rank_cmd = mock.Mock(
            return_value='test -z "$MPI_RANK"  || export RP_RANK=$MPI_RANK\n')

        for n_ranks in [1, 5]:
            ranks_str = pex._get_rank_ids(n_ranks=n_ranks, launcher=launcher)
            self.assertTrue(launcher.get_rank_cmd.called)
            self.assertIn('RP_RANKS=%s' % n_ranks, ranks_str)

        launcher = mock.Mock()
        launcher.get_rank_cmd = mock.Mock(
            return_value='test -z "$MPI_RANK" || echo "who cares"\n')

        with self.assertRaises(RuntimeError):
            ranks_str = pex._get_rank_ids(n_ranks=2, launcher=launcher)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestPopen()
    tc.setUpClass()
    tc.test_control_cb()
    tc.test_check_running()
    tc.test_handle_task()
    tc.test_extend_pre_exec()


# ------------------------------------------------------------------------------

