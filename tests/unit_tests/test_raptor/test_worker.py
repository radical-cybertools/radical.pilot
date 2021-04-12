# pylint: disable=unused-argument

__copyright__ = 'Copyright 2021, The RADICAL-Cybertools Team'
__license__   = 'MIT'

from radical.pilot.raptor.worker import Worker

from unittest import mock, TestCase


# Area of Square
def calculateArea(l):
    return l * l

# ------------------------------------------------------------------------------
#
class TestRaptorWorker(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Worker, '__init__', return_value=None)
    def test_register_mode(self, mocked_init):
        component = Worker()
        component._modes = dict()
        component.register_mode('test', 'test_call')

        self.assertEqual(component._modes, {'test': 'test_call'})

        with self.assertRaises(AssertionError):
            component.register_mode('test', 'test_call')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Worker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_eval(self, mocked_init, mocked_Logger):

        component = Worker()
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
    @mock.patch.object(Worker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_exec(self, mocked_init, mocked_Logger):

        component = Worker()
        component._log = mocked_Logger
        data = {'code': '2 + 5'}
        out, err, ret, val = component._exec(data)

        self.assertEqual(ret, 0)
        self.assertEqual(val, {7})
        self.assertEqual(out, '')
        self.assertEqual(err, '')

        data = {'code': 'math.log10(1)',
                'pre_exec': 'import math'}
        out, err, ret, val = component._exec(data)
        self.assertEqual(out, '')
        self.assertEqual(err, "")
        self.assertEqual(val, {float(0)})
        self.assertEqual(ret, 0)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Worker, '__init__', return_value=None)
    @mock.patch('radical.utils.Logger')
    def test_call(self, mocked_init, mocked_Logger):

        component = Worker()
        component._log = mocked_Logger
        data = {'function': 'calculateArea',
                'args': 2}
        out, err, ret, val = component._call(data)

        self.assertEqual(ret, 0)
        self.assertEqual(val, {7})
        self.assertEqual(out, '')
        self.assertEqual(err, '')

        data = {'code': 'math.log10(1)',
                'pre_exec': 'import math'}
        out, err, ret, val = component._exec(data)
        self.assertEqual(out, '')
        self.assertEqual(err, "")
        self.assertEqual(val, {float(0)})
        self.assertEqual(ret, 0)
