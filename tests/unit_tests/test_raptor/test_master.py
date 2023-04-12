# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import glob
import os
import shutil
import time

import threading            as mt

import radical.pilot        as rp
import radical.pilot.states as rps

from unittest import mock, TestCase

from radical.pilot.raptor.master import Master


# ------------------------------------------------------------------------------
#
class RaptorMasterTC(TestCase):

    _cleanup_files = []

    # --------------------------------------------------------------------------
    #
    @classmethod
    @mock.patch.object(rp.Session, '_initialize_primary', return_value=None)
    @mock.patch.object(rp.Session, '_get_logger')
    @mock.patch.object(rp.Session, '_get_profiler')
    @mock.patch.object(rp.Session, '_get_reporter')
    def setUpClass(cls, *args, **kwargs) -> None:

        cls._session = rp.Session()
        cls._cleanup_files.append(cls._session.uid)

    # --------------------------------------------------------------------------
    #
    @classmethod
    def tearDownClass(cls) -> None:

        for p in cls._cleanup_files:
            for f in glob.glob(p):
                if os.path.isdir(f):
                    try:
                        shutil.rmtree(f)
                    except OSError as e:
                        print('[ERROR] %s - %s' % (e.filename, e.strerror))
                else:
                    os.unlink(f)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Master, '__init__', return_value=None)
    def test_wait(self, mocked_init):

        log_debug_messages = ''

        def _log_debug(*args):
            nonlocal log_debug_messages
            log_debug_messages += args[0]
            if len(args) > 1:
                log_debug_messages = log_debug_messages % args[1:]

        raptor_master = Master(cfg=None)

        raptor_master._log = mock.Mock()
        raptor_master._log.debug.side_effect = _log_debug

        raptor_master._lock    = mt.Lock()
        raptor_master._workers = {
            'w.0000': {'status': Master.NEW},
            'w.0001': {'status': Master.NEW}
        }

        def _set_workers_done(master_obj):
            while True:
                time.sleep(3)
                with master_obj._lock:
                    for uid in master_obj._workers:
                        master_obj._workers[uid]['status'] = Master.DONE
                break

        _thread = mt.Thread(target=_set_workers_done, args=(raptor_master,))
        _thread.start()

        raptor_master.wait()
        self.assertTrue(log_debug_messages.endswith('wait ok'))

        _thread.join()

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Master, '__init__', return_value=None)
    def test_submit_tasks(self, mocked_init):

        executable_tasks_count = 0
        raptor_tasks_count     = 0
        advanced_tasks         = []

        def advance_side_effect(*args, **kwargs):
            nonlocal executable_tasks_count
            nonlocal raptor_tasks_count

            state = kwargs.get('state')
            if state == rps.AGENT_STAGING_INPUT_PENDING:
                executable_tasks_count += len(args[0])
            elif state == rps.AGENT_SCHEDULING:
                raptor_tasks_count += len(args[0])
            else:
                return

            advanced_tasks.extend(args[0])

        raptor_master = Master(cfg=None)
        raptor_master._uid          = 'master.0000'
        raptor_master._cfg          = {}
        raptor_master._info         = mock.Mock()
        raptor_master._task_service = mock.Mock()
        raptor_master._req_put      = mock.Mock()
        raptor_master._log          = mock.Mock()

        raptor_master._session      = self._session
        raptor_master._psbox        = '/tmp/pilot.0000'
        raptor_master._ssbox        = '/tmp'
        raptor_master._rsbox        = '/tmp'
        raptor_master._pid          = 'pilot.0000'

        raptor_master.publish       = mock.Mock()
        raptor_master.advance       = mock.Mock(side_effect=advance_side_effect)

        # 1 executable task and 2 raptor tasks
        tasks = [
            {
                'uid'        : 'task.0000',
                'description': {'uid'       : 'task.0000',
                                'ranks'     : 1,
                                'mode'      : rp.TASK_EXECUTABLE,
                                'executable': '/bin/date',
                                'sandbox'   : '/tmp'}
            },
            {
                'uid'        : 'task.0001',
                'description': {'uid'       : 'task.0001',
                                'ranks'     : 1,
                                'mode'      : rp.TASK_FUNCTION,
                                'function'  : 'test',
                                'sandbox'   : ''}
            },
            {
                'uid'        : 'task.0002',
                'description': {'uid'       : 'task.0002',
                                'ranks'     : 1,
                                'mode'      : rp.TASK_FUNCTION,
                                'function'  : 'test',
                                'sandbox'   : 'task_in_psbox'}
            }
        ]

        raptor_master.submit_tasks(tasks)

        self.assertEqual(executable_tasks_count, 1)
        self.assertEqual(raptor_tasks_count,     2)

        for task in advanced_tasks:
            td = task['description']
            if td['sandbox']:
                if td['sandbox'].startswith('/'):
                    self.assertEqual(task['task_sandbox_path'], td['sandbox'])
                else:
                    self.assertTrue(task['task_sandbox_path'].
                                    startswith(raptor_master._psbox))
                    self.assertTrue(task['task_sandbox_path'].
                                    endswith(td['sandbox'] + '/'))
            else:
                self.assertTrue(task['task_sandbox_path'].
                                startswith(raptor_master._psbox))
                self.assertTrue(task['task_sandbox_path'].
                                endswith(td['uid'] + '/'))

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Master, '__init__', return_value=None)
    def test_submit_workers_err(self, mocked_init):

        raptor_master = Master(cfg=None)
        raptor_master._uid          = 'master.0000'
        raptor_master._cfg          = {}
        raptor_master._info         = mock.Mock()
        raptor_master._task_service = mock.Mock()

        with self.assertRaises(RuntimeError):
            # raise an exception in case of shared GPU(s)
            raptor_master.submit_workers({'gpus_per_rank': 1.5}, 1)


# ------------------------------------------------------------------------------

if __name__ == '__main__':

    tc = RaptorMasterTC()
    tc.test_wait()
    tc.test_submit_workers_err()

# ------------------------------------------------------------------------------
