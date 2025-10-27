#!/usr/bin/env python3

# pylint: disable=unused-argument, no-value-for-parameter

import glob
import os
import shutil
import asyncio

import multiprocessing as mp
import radical.pilot   as rp

from unittest import mock, TestCase

from radical.pilot.raptor.worker_default import DefaultWorker


# ------------------------------------------------------------------------------
#
def calculate_area(x):
    ret = x * x
    print(ret)
    return ret


# ------------------------------------------------------------------------------
#
class TestRaptorWorker(TestCase):

    _cleanup_files = []

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
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    def test_register_mode(self, mocked_init):
        component = DefaultWorker()
        component._modes = dict()
        component.register_mode('test', 'test_call')

        self.assertEqual(component._modes, {'test': 'test_call'})

        with self.assertRaises(ValueError):
            component.register_mode('test', 'test_call')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_eval(self, mocked_init, mocked_Logger):

        component = DefaultWorker()
        component._log  = mocked_Logger
        component._prof = mock.Mock()
        task = {'uid'        : 'task.0000',
                'description': {'code': '2 + 5'}}
        out, err, ret, val, exc = component._dispatch_eval(task)

        self.assertEqual(ret, 0)
        self.assertEqual(val, 7)
        self.assertEqual(out, '')
        self.assertEqual(err, '')
        self.assertEqual(exc, (None, None))

        task = {'uid'        : 'task.0001',
                'description': {'code': 'math.add(2, 5)'}}
        out, err, ret, _, _ = component._dispatch_eval(task)
        self.assertEqual(out, '')
        self.assertEqual(err, "\neval failed: name 'math' is not defined")
        self.assertEqual(ret, 1)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_exec(self, mocked_init, mocked_Logger):

        pass

      # component = DefaultWorker()
      # component._log = mocked_Logger
      # data = {'code': '2 + 5'}
      # out, err, ret, val = component._exec(data)
      #
      # self.assertEqual(ret, 0)
      # self.assertEqual(val, {7})
      # self.assertEqual(out, '')
      # self.assertEqual(err, '')
      #
      # data = {'code': 'math.log10(1)',
      #         'pre_exec': 'import math'}
      # out, err, ret, val = component._exec(data)
      # self.assertEqual(out, '')
      # self.assertEqual(err, "")
      # self.assertEqual(val, {float(0)})
      # self.assertEqual(ret, 0)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_call(self, mocked_init, mocked_Logger):

        component = DefaultWorker()
        component.calculate_area = calculate_area
        component._prof = mock.Mock()
        component._log  = mocked_Logger
        task = {'uid'        : 'task.0000',
                'description': {
                        'function': 'calculate_area',
                        'args'    : [2],
                        'kwargs'  : {}}}
        out, err, ret, val, exc = asyncio.run(component._dispatch_func(task))

        self.assertEqual(ret, 0)
        self.assertEqual(val, 4)
        self.assertEqual(out, '4\n')
        self.assertEqual(err, '')
        self.assertEqual(exc, (None, None))


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    @mock.patch('multiprocessing.Process')
    def test_sandbox(self, mocked_mp_process, mocked_init):

        component = DefaultWorker()
        component.check_pwd     = os.getcwd
        component._log          = mock.Mock()
        component._prof         = mock.Mock()
        component._result_queue = mp.Queue()
        component._sbox         = '/tmp'
        component._modes        = {rp.TASK_FUNCTION: component._dispatch_func}

        task_sbox = 'task_sandbox'
        task_sbox_path = os.path.join(os.getcwd(), task_sbox)
        self.assertFalse(os.path.isdir(task_sbox_path))

        task = {'uid'              : 'task.0000',
                'slots'            : [{'gpus': []}],
                'description'      : {'mode'    : rp.TASK_FUNCTION,
                                      'function': 'check_pwd',
                                      'args'    : [],
                                      'kwargs'  : {},
                                      'timeout' : 0},
                'task_rundir_path' : task_sbox,
                'task_sandbox_path': task_sbox}

        def _target_process(target, args):
            nonlocal component
            target(args[0])
            res = component._result_queue.get(timeout=0.1)
            # out
            self.assertEqual(res[1], '')
            # err
            self.assertEqual(res[2], '')
            # return code
            self.assertEqual(res[3], 0)
            # result from executed function
            self.assertEqual(res[4], task_sbox_path)
            mp_process = mock.Mock()
            mp_process.is_alive.return_value = False
            return mp_process

        mocked_mp_process.side_effect = _target_process
        with mock.patch('sys.exit'):
            component._dispatch(task, {})

        self.assertTrue(os.path.isdir(task_sbox_path))
        self._cleanup_files.append(task_sbox_path)


# ------------------------------------------------------------------------------
#
if __name__ == '__main__':

    tc = TestRaptorWorker()
    tc.test_sandbox()


# ------------------------------------------------------------------------------

