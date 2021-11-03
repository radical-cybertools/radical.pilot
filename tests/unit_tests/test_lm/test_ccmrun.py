# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.ccmrun import CCMRun


# ------------------------------------------------------------------------------
#
class TestCCMRun(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(CCMRun, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/ccmrun')
    def test_init_from_scratch(self, mocked_which, mocked_init):

        lm_ccmrun = CCMRun('', {}, None, None, None)

        lm_info = lm_ccmrun._init_from_scratch({}, '')
        self.assertEqual(lm_info['command'], mocked_which())

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(CCMRun, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_ccmrun = CCMRun('', {}, None, None, None)

        lm_info = {'env': {'test_env': 'test_value'},
                   'env_sh': 'env/lm_ccmrun.sh',
                   'command': '/usr/bin/ccmrun'}
        lm_ccmrun._init_from_info(lm_info)
        self.assertEqual(lm_ccmrun._env,     lm_info['env'])
        self.assertEqual(lm_ccmrun._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_ccmrun._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_ccmrun._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(CCMRun, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        lm_ccmrun = CCMRun('', {}, None, None, None)
        self.assertTrue(lm_ccmrun.can_launch(
            task={'description': {'executable': 'script'}})[0])
        self.assertFalse(lm_ccmrun.can_launch(
            task={'description': {'executable': None}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(CCMRun, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_ccmrun = CCMRun('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ccmrun.sh',
                   'command': '/usr/bin/ccmrun'}
        lm_ccmrun._init_from_info(lm_info)
        lm_env = lm_ccmrun.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(CCMRun, '__init__',   return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_ccmrun  = CCMRun('', {}, None, None, None)
        lm_ccmrun._command = 'ccmrun'

        test_cases = setUp('lm', 'ccmrun')
        for task, result in test_cases:

            command = lm_ccmrun.get_launch_cmds(task, '')
            self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

            command = lm_ccmrun.get_rank_exec(task, None, None)
            self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestCCMRun()
    tc.test_init_from_scratch()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()


# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
