
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from   .test_common                           import setUp
from   radical.pilot.agent.launch_method.fork import Fork


class TestFork(TestCase):


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        component = Fork(name=None, cfg=None, session=None)
        component._configure()
        self.assertEqual('', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Fork, '__init__',   return_value=None)
    @mock.patch.object(Fork, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init, mocked_configure, mocked_raise_on):

        test_cases     = setUp('lm', 'fork')
        component      = Fork(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')

        for task, result in test_cases:
            command, hop = component.construct_command(task, None)
            self.assertEqual([command, hop], result)


if __name__ == '__main__':

    tc = TestFork()
    tc.test_construct_command()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
