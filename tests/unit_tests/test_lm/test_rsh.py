
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os

from   unittest     import mock, TestCase
from   .test_common import setUp

import radical.utils as ru

from   radical.pilot.agent.launch_method.rsh import RSH


class TestRSH(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value='/usr/bin/rsh')
    def test_configure(self, mocked_init, mocked_raise_on, mocked_which):

        component = RSH(name=None, cfg=None, session=None)
        component._configure()
        self.assertEqual('/usr/bin/rsh', component.launch_command)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    @mock.patch('radical.utils.which', return_value=None)
    def test_configure_fail(self, mocked_init, mocked_raise_on, mocked_which):

        component = RSH(name=None, cfg=None, session=None)
        with self.assertRaises(RuntimeError):
            component._configure()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__',   return_value=None)
    @mock.patch.object(RSH, '_configure', return_value=None)
    @mock.patch.dict(os.environ,
                     {'PATH': 'test_path', 'LD_LIBRARY_PATH': '/usr/local/lib/'},
                     clear=True)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init,
                               mocked_configure,
                               mocked_raise_on):

        test_cases = setUp('lm', 'rsh')
        component  = RSH(name=None, cfg=None, session=None)
        component._log           = ru.Logger('dummy')
        component.name           = 'RSH'
        component.mpi_flavor     = None
        component.launch_command = 'rsh'

        for task, result in test_cases:
            if result == "ValueError":
                with self.assertRaises(ValueError):
                    _, _ = component.construct_command(task, None)
            elif result == "RuntimeError":
                with self.assertRaises(RuntimeError):
                    _, _ = component.construct_command(task, 1)
            else:
                command, hop = component.construct_command(task, 1)
                self.assertEqual([command, hop], result)


if __name__ == '__main__':

    tc = TestRSH()
    tc.test_configure()
    tc.test_configure_fail()
    tc.test_construct_command()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
