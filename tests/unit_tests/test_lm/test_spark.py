# pylint: disable=protected-access, unused-argument, no-value-for-parameter
from unittest import mock, TestCase

import radical.utils as ru

from   .test_common                            import setUp
from   radical.pilot.agent.launch_method.spark import Spark


class TestSpark(TestCase):

    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Spark, '__init__',   return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_configure(self, mocked_init, mocked_raise_on):

        component      = Spark(name=None, cfg=None, session=None)
        component._log = ru.Logger('dummy')
        component._cfg = {'rm_info':{'lm_info':{'launch_command':'spark-submit'}}}
        component._configure()
        self.assertEqual('spark-submit', component.launch_command)

    # ------------------------------------------------------------------------------


    # ------------------------------------------------------------------------------
    #
    @mock.patch.object(Spark, '__init__',   return_value=None)
    @mock.patch.object(Spark, '_configure', return_value=None)
    @mock.patch('radical.utils.raise_on')
    def test_construct_command(self, mocked_init,
                               mocked_configure,
                               mocked_raise_on):

        test_cases = setUp('lm', 'spark')
        component  = Spark(cfg=None, session=None, name=None)

        component._log           = ru.Logger('dummy')
        component.launch_command = ''
        component.name           = 'Spark'

        for task, result in test_cases:
            if result == "RuntimeError":
                with self.assertRaises(RuntimeError):
                    _, _ = component.construct_command(task, None)
            else:
                command, hop = component.construct_command(task, None)
                self.assertEqual([command, hop], result)


if __name__ == '__main__':

    tc = TestSpark()
    tc.test_configure()
    tc.test_configure()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
