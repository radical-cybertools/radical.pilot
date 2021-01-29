
# pylint: disable=protected-access, unused-argument, no-value-for-parameter

import os
import glob

from unittest import mock, TestCase

import radical.utils as ru

from .test_common                            import setUp
from radical.pilot.agent.launch_method.jsrun import JSRUN


class TestJSRun(TestCase):

    # ------------------------------------------------------------------------------
    #
    def tearDown(self):
        rs = glob.glob('%s/*.rs' % os.getcwd())
        for fold in rs:
            os.remove(fold)


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch.object(JSRUN, '_configure',return_value='jsrun')
    @mock.patch('radical.utils.raise_on')
    def test_create_resource_set_file(self, mocked_init, mocked_configure,
                                      mocked_raise_on):

        test_cases = setUp('lm', 'jsrun')
        component  = JSRUN(name=None, cfg=None, session=None)

        for task, _, resource_file, _ in test_cases:

            slot   = task['slots']
            uid    = task['uid']

            component._create_resource_set_file(slots=slot, uid=uid, sandbox='.')
            print(uid)
            with open('%s.rs' % uid) as rs_layout:
                self.assertEqual(rs_layout.readlines(),  resource_file)

        self.tearDown()


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(JSRUN, '__init__', return_value=None)
    @mock.patch.object(JSRUN, '_configure', return_value='jsrun')
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init, mocked_configure, mocked_raise_on):

        test_cases = setUp('lm', 'jsrun')
        component  = JSRUN(name=None, cfg=None, session=None)

        component._create_resource_set_file = mock.Mock()
        component.launch_command            = 'jsrun'
        component._log                      = ru.Logger('dummy')

        for task, result, _ , resource_filename  in test_cases:
            component._create_resource_set_file.return_value = resource_filename
            command, hop = component.construct_command(task, None)
            self.assertEqual([command, hop] == result)


if __name__ == '__main__':

    tc = TestJSRun()
    tc.test_construct_command()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
