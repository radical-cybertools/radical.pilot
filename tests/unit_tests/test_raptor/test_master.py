# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import time

import threading as mt

from unittest import mock, TestCase

from radical.pilot.raptor.master import Master


# ------------------------------------------------------------------------------
#
class RaptorMasterTC(TestCase):

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
    def test_submit_workers_err(self, mocked_init):

        raptor_master = Master(cfg=None)
        raptor_master._uid          = 'master.0000'
        raptor_master._cfg          = {}
        raptor_master._info         = mock.Mock()
        raptor_master._task_service = mock.Mock()

        with self.assertRaises(RuntimeError):
            # raise an exception in case of shared GPU(s)
            raptor_master.submit_workers({'gpus_per_rank': 1.5, }, 1)


# ------------------------------------------------------------------------------

if __name__ == '__main__':

    tc = RaptorMasterTC()
    tc.test_wait()
    tc.test_submit_workers_err()

# ------------------------------------------------------------------------------
