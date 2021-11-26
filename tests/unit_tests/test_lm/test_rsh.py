# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.rsh import RSH


# ------------------------------------------------------------------------------
#
class TestRSH(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/rsh')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_rsh = RSH('', {}, None, None, None)

        lm_info = lm_rsh._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_rsh = RSH('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_rsh.sh',
                   'command': '/usr/bin/rsh'}
        lm_rsh._init_from_info(lm_info)
        self.assertEqual(lm_rsh._env,     lm_info['env'])
        self.assertEqual(lm_rsh._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_rsh._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_rsh._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        # ensure single rank
        # (NOTE: full task and rank descriptions are NOT provided)

        lm_rsh = RSH('', {}, None, None, None)
        self.assertTrue(lm_rsh.can_launch(
            task={'slots': {'ranks': [{'node_id': '00001'}]}})[0])
        self.assertFalse(lm_rsh.can_launch(
            task={'slots': {'ranks': [{'node_id': '00001'},
                                      {'node_id': '00002'}]}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_rsh = RSH('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ssh.sh',
                   'command': '/usr/bin/ssh'}
        lm_rsh._init_from_info(lm_info)
        lm_env = lm_rsh.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(RSH, '__init__', return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_rsh = RSH('', {}, None, None, None)
        lm_rsh._command = 'rsh'

        test_cases = setUp('lm', 'rsh')
        for task, result in test_cases:

            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    lm_rsh.get_launch_cmds(task, '')

            else:
                command = lm_rsh.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                command = lm_rsh.get_rank_exec(task, None, None)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestRSH()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
