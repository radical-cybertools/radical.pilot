# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from .test_common                            import setUp
from radical.pilot.agent.launch_method.aprun import APRun


class TestAPRun(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='/usr/bin/aprun')
    def test_configure(self, mocked_init, mocked_raise_on, mocked_which):

        component = APRun(name=None, cfg=None, session=None)
        component._configure()
        self.assertEqual('/usr/bin/aprun', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(APRun, '__init__',   return_value=None)
    @mock.patch.object(APRun, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init, mocked_configure, mocked_raise_on):

        test_cases = setUp('lm', 'aprun')
        component  = APRun(name=None, cfg=None, session=None)

        component.launch_command = 'aprun'
        component.name           = 'aprun'
        component._log           = ru.Logger('dummy')

        for task, result in test_cases:
            command, hop = component.construct_command(task, None)
            self.assertEqual([command, hop], result)


if __name__ == '__main__':

    tc = TestAPRun()
    tc.test_configure()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
