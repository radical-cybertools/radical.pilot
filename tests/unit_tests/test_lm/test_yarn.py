# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from   .test_common                           import setUp
from   radical.pilot.agent.launch_method.yarn import Yarn


class TestYarn(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Yarn, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        component = Yarn(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'rm_info':{'lm_info':{'launch_command':'yarn'}}}
        component._configure()
        self.assertEqual('yarn', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Yarn, '__init__',   return_value=None)
    @mock.patch.object(Yarn, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init,
                               mocked_configure,
                               mocked_raise_on):

        test_cases = setUp('lm', 'yarn')
        component  = Yarn(cfg=None, session=None, name=None)

        component._log           = ru.Logger('dummy')
        component.launch_command = 'yarn'
        component.name           = "YARN"

        for task, result in test_cases:
            if result == "RuntimeError":
                with self.assertRaises(RuntimeError):
                    _, _ = component.construct_command(task, None)
            else:
                command, hop = component.construct_command(task, None)
                self.assertEqual([command, hop], result)


if __name__ == '__main__':

    tc = TestYarn()
    tc.test_configure()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
