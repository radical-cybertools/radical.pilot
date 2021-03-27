# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

import radical.utils as ru

from .test_common import setUp
from radical.pilot.agent.launch_method.srun import Srun


class TestSrun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/bin/srun')
    @mock.patch('radical.utils.sh_callout', return_value=['19.05.2', '', 0])
    def test_configure(self, mocked_init, mocked_which, mocked_sh_callout):

        component = Srun(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._configure()
        self.assertEqual('/bin/srun', component.launch_command)
        self.assertEqual('19.05.2', component._version)


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__',   return_value=None)
    @mock.patch('radical.utils.which', return_value=None)
    @mock.patch('radical.utils.sh_callout', return_value=['', '', 1])
    @mock.patch('radical.utils.raise_on')
    def test_configure_fail(self, mocked_init, mocked_which, mocked_sh_callout,
                            mocked_raise_on):

        component = Srun(name=None, cfg=None, session=None)
        with self.assertRaises(RuntimeError):
            component._configure()


    # --------------------------------------------------------------------------
    #
    @mock.patch.object(Srun, '__init__', return_value=None)
    @mock.patch.object(Srun, '_configure', return_value=None)
    def test_construct_command(self, mocked_init, mocked_configure):

        component = Srun(name=None, cfg=None, session=None)

        component._log = ru.Logger('dummy')
        component._cfg = {}

        component.name = 'srun'
        component.launch_command = '/bin/srun'

        test_cases = setUp('lm', 'srun')
        for task, result in test_cases:
            if result != "RuntimeError":
                command, hop = component.construct_command(task, None)
                self.assertEqual([command, hop], result, msg=task['uid'])


if __name__ == '__main__':

    tc = TestSrun()
    tc.test_configure()
    tc.test_configure_fail()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
