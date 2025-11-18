# pylint: disable=unused-argument, no-value-for-parameter

import time

from unittest import TestCase, mock

import radical.pilot as rp


# ------------------------------------------------------------------------------
#
class TestTask(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(rp.TaskManager, '__init__', return_value=None)
    def test_task_uid(self, mocked_init):

        tmgr = rp.TaskManager(None)
        tmgr._uid     = 'tmgr.0000'
        tmgr._log     = mock.Mock()
        tmgr._prof    = mock.Mock()
        tmgr._session = mock.Mock(uid=str(time.time()))  # restart uid counter
        tmgr.advance  = mock.Mock()

        descr = rp.TaskDescription({})
        with self.assertRaises(ValueError):
            # no either `executable` or `kernel`
            rp.Task(tmgr, descr, 'test')

        descr = rp.TaskDescription({'uid'       : 'foo.1',
                                    'name'      : 'bar',
                                    'executable': './exec'})
        task = rp.Task(tmgr, descr, 'test')
        self.assertEqual(task.uid,  'foo.1')
        self.assertEqual(task.name, 'bar')
        self.assertEqual(task.state, rp.NEW)
        self.assertIsInstance(task.as_dict(), dict)


    # --------------------------------------------------------------------------
    #
    def test_task_description(self):

        td = rp.TaskDescription({'executable'      : 'true',
                                 'cpu_processes'   : 2,
                                 'cpu_threads'     : 3,
                                 'cpu_process_type': rp.MPI,
                                 'gpu_processes'   : 4,
                                 'gpu_threads'     : 5,
                                 'gpu_process_type': rp.CUDA})

        td.verify()

        self.assertEqual(td.ranks,          2)
        self.assertEqual(td.cores_per_rank, 3)
        self.assertEqual(td.gpus_per_rank,  4)
        self.assertEqual(td.gpu_type,       rp.CUDA)

        self.assertFalse(td.pre_exec_sync)

        td = rp.TaskDescription({'executable': 'true'})
        td.verify()
        self.assertFalse(td.use_mpi)

        td = rp.TaskDescription({'ranks': 2, 'executable': 'true'})
        td.verify()
        self.assertTrue(td.use_mpi)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestTask()
    tc.test_task_uid()
    tc.test_task_description()


# ------------------------------------------------------------------------------

