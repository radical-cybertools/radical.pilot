# pylint: disable=unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from radical.pilot.raptor.worker_default import DefaultWorker

from unittest import mock, TestCase


# ------------------------------------------------------------------------------
#
def calculate_area(x):
    ret = x * x
    print(ret)
    return ret


# ------------------------------------------------------------------------------
#
class TestRaptorWorker(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    def test_register_mode(self, mocked_init):
        component = DefaultWorker()
        component._modes = dict()
        component.register_mode('test', 'test_call')

        self.assertEqual(component._modes, {'test': 'test_call'})

        with self.assertRaises(AssertionError):
            component.register_mode('test', 'test_call')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(DefaultWorker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_eval(self, mocked_init, mocked_Logger):

        component = DefaultWorker()
        component._log = mocked_Logger
        data = {'code': '2 + 5'}
        out, err, ret, val = component._eval(data)

        self.assertEqual(ret, 0)
        self.assertEqual(val, 7)
        self.assertEqual(out, '')
        self.assertEqual(err, '')

        data = {'code': 'math.add(2,5)'}
        out, err, ret, _ = component._eval(data)
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
      # print('===', [out, err, ret, val])
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
        component._log = mocked_Logger
        data = {'method': 'calculate_area',
                'args': [2]}
        out, err, ret, val = component._call(data)

        self.assertEqual(ret, 0)
        self.assertEqual(val, 4)
        self.assertEqual(out, '4\n')
        self.assertEqual(err, '')


# ------------------------------------------------------------------------------

