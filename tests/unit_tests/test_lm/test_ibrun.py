
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from .test_common                            import setUp
from radical.pilot.agent.launch_method.ibrun import IBRun


class TestIBRun(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__', return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='/usr/bin/ibrun')
    def test_configure(self, mocked_init, mocked_raise_on, mocked_which):

        component = IBRun(name=None, cfg=None, session=None)
        component._configure()
        self.assertEqual('/usr/bin/ibrun', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(IBRun, '__init__',   return_value=None)
    @mock.patch.object(IBRun, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init, mocked_configure, mocked_raise_on):

        component = IBRun(name=None, cfg=None, session=None)

        component._log = ru.Logger('dummy')
        component._cfg = {'cores_per_node': 0}
        component._node_list = [['node1'], ['node2']]

        component.name = 'IBRun'
        component.launch_command = 'ibrun'

        test_cases = setUp('lm', 'ibrun')
        for task, result in test_cases:
            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    _, _ = component.construct_command(task, None)
            elif result == 'AssertionError':
                with self.assertRaises(AssertionError):
                    _, _ = component.construct_command(task, None)
            else:
                command, hop = component.construct_command(task, None)
                self.assertEqual([command, hop], result, msg=task['uid'])


if __name__ == '__main__':

    tc = TestIBRun()
    tc.test_configure()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
