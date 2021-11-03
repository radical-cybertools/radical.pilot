# pylint: disable=protected-access, unused-argument, no-value-for-parameter

from unittest import mock, TestCase

from .test_common import setUp
from radical.pilot.agent.launch_method.ssh import SSH


# ------------------------------------------------------------------------------
#
class TestSSH(TestCase):

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/ssh')
    @mock.patch('os.path.islink', return_value=False)
    def test_init_from_scratch_not_rsh(self, mocked_islink, mocked_which,
                                       mocked_init):

        lm_ssh = SSH('', {}, None, None, None)

        lm_info = lm_ssh._init_from_scratch({}, '')
        self.assertIn(mocked_which(), lm_info['command'])
        self.assertIn('StrictHostKeyChecking', lm_info['command'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value='/usr/bin/ssh')
    @mock.patch('os.path.islink', return_value=True)
    @mock.patch('os.path.realpath', return_value='/usr/bin/rsh')
    @mock.patch('os.path.basename', return_value='rsh')
    @mock.patch('radical.utils.Logger')
    def test_init_from_scratch_is_rsh(self, mocked_logger, mocked_basename,
                                      mocked_realpath, mocked_islink,
                                      mocked_which, mocked_init):

        lm_ssh = SSH('', {}, None, None, None)
        lm_ssh._log = mocked_logger

        lm_info = lm_ssh._init_from_scratch({}, '')
        self.assertNotEqual(lm_info['command'], mocked_which())
        self.assertEqual(lm_info['command'], mocked_realpath())
        self.assertNotIn('StrictHostKeyChecking', lm_info['command'])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    @mock.patch('radical.utils.which', return_value=None)
    def test_init_from_scratch_fail(self, mocked_which, mocked_init):

        lm_ssh = SSH('', {}, None, None, None)
        with self.assertRaises(RuntimeError):
            # error while getting `ssh` command
            lm_ssh._init_from_scratch({}, '')

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    def test_init_from_info(self, mocked_init):

        lm_ssh = SSH('', {}, None, None, None)

        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ssh.sh',
                   'command': '/usr/bin/ssh'}
        lm_ssh._init_from_info(lm_info)
        self.assertEqual(lm_ssh._env,     lm_info['env'])
        self.assertEqual(lm_ssh._env_sh,  lm_info['env_sh'])
        self.assertEqual(lm_ssh._command, lm_info['command'])

        lm_info['command'] = ''
        with self.assertRaises(AssertionError):
            lm_ssh._init_from_info(lm_info)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    def test_can_launch(self, mocked_init):

        # ensure single rank
        # (NOTE: full task and rank descriptions are NOT provided)

        lm_ssh = SSH('', {}, None, None, None)
        self.assertFalse(lm_ssh.can_launch(
            task={'slots': {'ranks': [{'node_id': '00001'},
                                      {'node_id': '00002'}]}})[0])
        self.assertFalse(lm_ssh.can_launch(
            task={'description': {'executable': None},
                  'slots': {'ranks': [{'node_id': '00001'}]}})[0])
        self.assertTrue(lm_ssh.can_launch(
            task={'description': {'executable': 'script'},
                  'slots': {'ranks': [{'node_id': '00001'}]}})[0])

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    def test_get_launcher_env(self, mocked_init):

        lm_ssh = SSH('', {}, None, None, None)
        lm_info = {'env'    : {'test_env': 'test_value'},
                   'env_sh' : 'env/lm_ssh.sh',
                   'command': '/usr/bin/ssh'}
        lm_ssh._init_from_info(lm_info)
        lm_env = lm_ssh.get_launcher_env()

        self.assertIn('. $RP_PILOT_SANDBOX/%s' % lm_info['env_sh'], lm_env)

    # --------------------------------------------------------------------------
    #
    @mock.patch.object(SSH, '__init__', return_value=None)
    def test_get_launch_rank_cmds(self, mocked_init):

        lm_ssh = SSH('', {}, None, None, None)
        lm_ssh._command = 'ssh'

        test_cases = setUp('lm', 'ssh')
        for task, result in test_cases:

            if result == 'RuntimeError':
                with self.assertRaises(RuntimeError):
                    lm_ssh.get_launch_cmds(task, '')

            else:
                command = lm_ssh.get_launch_cmds(task, '')
                self.assertEqual(command, result['launch_cmd'], msg=task['uid'])

                command = lm_ssh.get_rank_exec(task, None, None)
                self.assertEqual(command, result['rank_exec'], msg=task['uid'])

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    tc = TestSSH()
    tc.test_init_from_scratch_not_rsh()
    tc.test_init_from_scratch_is_rsh()
    tc.test_init_from_scratch_fail()
    tc.test_init_from_info()
    tc.test_can_launch()
    tc.test_get_launcher_env()
    tc.test_get_launch_rank_cmds()

# ------------------------------------------------------------------------------
# pylint: enable=protected-access, unused-argument, no-value-for-parameter
